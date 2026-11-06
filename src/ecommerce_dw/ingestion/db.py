"""MySQL connection factory for ODS ingestion.

All credentials come from environment variables (see ``config.py``); nothing is
hard-coded, and no password is ever logged or embedded in DSN strings.
"""

from __future__ import annotations

import pymysql

from ecommerce_dw.config import DbConfig


def connect(
    cfg: DbConfig,
    *,
    connect_timeout: int = 10,
    read_timeout: int = 600,
) -> pymysql.connections.Connection:
    """Open a pymysql connection against the configured MySQL database."""
    return pymysql.connect(
        host=cfg.host,
        port=cfg.port,
        user=cfg.user,
        password=cfg.password,
        database=cfg.database,
        charset="utf8mb4",
        connect_timeout=connect_timeout,
        read_timeout=read_timeout,
        autocommit=False,
        cursorclass=pymysql.cursors.Cursor,
    )
