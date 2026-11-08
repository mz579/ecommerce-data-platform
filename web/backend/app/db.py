"""只读数据库访问层（PyMySQL + 简单连接池）。

设计约束（SPEC §2/§8）：

* 运行时账号只有 ``SELECT``；连接建立后额外执行
  ``SET SESSION TRANSACTION READ ONLY`` 做第二道防线；
* 连接池按线程安全队列复用连接，取用前 ``ping``，坏连接直接丢弃；
* 连接失败抛 :class:`DatabaseConnectionError`（接口层映射 503），
  查询失败抛 :class:`DatabaseQueryError`（映射 500），日志保留完整堆栈，
  响应体只返回通用说明，不把栈或 SQL 暴露给客户端。
"""

from __future__ import annotations

import logging
import queue
import re
import threading
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from typing import Any

import pymysql
from pymysql.cursors import DictCursor

from .config import Settings, settings

logger = logging.getLogger(__name__)

_IDENT_RE = re.compile(r"^[A-Za-z0-9_]+$")


class DatabaseError(RuntimeError):
    """数据库层错误基类。"""


class DatabaseConnectionError(DatabaseError):
    """连接池耗尽、连不上 MySQL、认证失败等可重试错误（HTTP 503）。"""


class DatabaseQueryError(DatabaseError):
    """SQL 执行失败（HTTP 500）。"""


def quote_identifier(name: str) -> str:
    """校验并反引号包裹库/表名，杜绝拼接注入。"""
    if not _IDENT_RE.match(name):
        raise ValueError(f"unsafe SQL identifier: {name!r}")
    return f"`{name}`"


def table_ref(schema: str, table: str) -> str:
    return f"{quote_identifier(schema)}.{quote_identifier(table)}"


class ConnectionPool:
    """极简 LIFO 连接池。

    ``maxsize`` 个连接；池空且未达上限时现场建连，达上限则阻塞等待
    ``pool_timeout`` 秒。所有连接 ``autocommit=True`` 且会话只读。
    """

    def __init__(self, cfg: Settings) -> None:
        self._cfg = cfg
        self._queue: queue.LifoQueue[pymysql.connections.Connection] = queue.LifoQueue(
            maxsize=cfg.pool_size
        )
        self._lock = threading.Lock()
        self._created = 0
        self._closed = False

    def _connect(self) -> pymysql.connections.Connection:
        try:
            return pymysql.connect(
                host=self._cfg.mysql_host,
                port=self._cfg.mysql_port,
                user=self._cfg.mysql_user,
                password=self._cfg.mysql_password,
                database=self._cfg.dw_database,
                charset="utf8mb4",
                cursorclass=DictCursor,
                autocommit=True,
                connect_timeout=self._cfg.connect_timeout,
                read_timeout=self._cfg.read_timeout,
                write_timeout=self._cfg.read_timeout,
                init_command="SET SESSION TRANSACTION READ ONLY",
            )
        except pymysql.MySQLError as exc:
            logger.error(
                "MySQL connection failed host=%s port=%s user=%s db=%s: %s",
                self._cfg.mysql_host,
                self._cfg.mysql_port,
                self._cfg.mysql_user,
                self._cfg.dw_database,
                exc,
            )
            raise DatabaseConnectionError(
                f"cannot connect to MySQL at {self._cfg.mysql_host}:{self._cfg.mysql_port}"
            ) from exc

    def _discard(self, conn: pymysql.connections.Connection | None) -> None:
        if conn is not None:
            try:
                conn.close()
            except Exception:  # pragma: no cover - 关闭失败不影响主流程
                logger.debug("failed to close a dead MySQL connection", exc_info=True)
        with self._lock:
            self._created = max(0, self._created - 1)

    def acquire(self) -> pymysql.connections.Connection:
        if self._closed:
            raise DatabaseConnectionError("connection pool is closed")
        for _ in range(2):
            conn: pymysql.connections.Connection | None = None
            try:
                conn = self._queue.get_nowait()
            except queue.Empty:
                create = False
                with self._lock:
                    if self._created < self._cfg.pool_size:
                        self._created += 1
                        create = True
                if create:
                    try:
                        return self._connect()
                    except Exception:
                        with self._lock:
                            self._created = max(0, self._created - 1)
                        raise
                try:
                    conn = self._queue.get(timeout=self._cfg.pool_timeout)
                except queue.Empty as exc:
                    raise DatabaseConnectionError("MySQL connection pool exhausted") from exc
            try:
                conn.ping(reconnect=False)
                return conn
            except pymysql.MySQLError:
                logger.warning("discarding unhealthy MySQL connection", exc_info=True)
                self._discard(conn)
        raise DatabaseConnectionError("cannot obtain a healthy MySQL connection")

    def release(self, conn: pymysql.connections.Connection) -> None:
        if not getattr(conn, "open", False):
            self._discard(conn)
            return
        try:
            self._queue.put_nowait(conn)
        except queue.Full:
            self._discard(conn)

    @contextmanager
    def connection(self) -> Iterator[pymysql.connections.Connection]:
        conn = self.acquire()
        try:
            yield conn
        except Exception:
            # 无法判断连接是否仍健康（网络中断 / 服务端断开），直接丢弃，
            # 避免把半死连接放回池里污染后续请求。
            self._discard(conn)
            raise
        else:
            self.release(conn)

    def fetch_all(
        self,
        sql: str,
        params: Sequence[Any] | None = None,
    ) -> list[dict[str, Any]]:
        with self.connection() as conn:
            try:
                with conn.cursor() as cur:
                    cur.execute(sql, tuple(params or ()))
                    return list(cur.fetchall())
            except pymysql.MySQLError as exc:
                logger.error("MySQL query failed: %s", exc)
                raise DatabaseQueryError(f"query failed with code {exc.args[0]}") from exc

    def fetch_one(
        self,
        sql: str,
        params: Sequence[Any] | None = None,
    ) -> dict[str, Any] | None:
        rows = self.fetch_all(sql, params)
        return rows[0] if rows else None

    def close_all(self) -> None:
        with self._lock:
            self._closed = True
        while True:
            try:
                conn = self._queue.get_nowait()
            except queue.Empty:
                break
            try:
                conn.close()
            except Exception:  # pragma: no cover - 关闭失败不影响退出
                logger.debug("failed to close pooled MySQL connection", exc_info=True)
        with self._lock:
            self._created = 0


POOL = ConnectionPool(settings)
