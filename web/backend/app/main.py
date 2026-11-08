"""FastAPI 入口：注册 /api 路由 + 托管前端静态资源（SPA fallback）。

部署形态（SPEC §6）：单容器 ``ecommerce-web``，Uvicorn 监听 8090；同一进程同时
提供 API 与 ``web/frontend/dist`` 构建产物。前端尚未构建时返回一个极简占位页，
保证后端镜像可独立验证。

注意：本模块与全部路由只调用 ``SELECT``；写连接权限在 ``webapp_ro`` 账号层被
拒绝，连接会话也设置为 READ ONLY。
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse

from . import db
from .config import settings
from .routers import items, meta, ops, overview, quality, realtime, retention

logging.basicConfig(
    level=getattr(logging, settings.log_level, logging.INFO),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("ecommerce_web")

PLACEHOLDER_HTML = """<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>电商数仓 Web UI</title>
  <style>
    body { margin: 0; background: #f9f9f7; color: #0b0b0b;
           font: 14px/1.6 system-ui, -apple-system, "Segoe UI", "Microsoft YaHei", sans-serif; }
    main { max-width: 720px; margin: 96px auto; padding: 0 24px; }
    h1 { font-size: 24px; font-weight: 600; }
    p { color: #52514e; }
    code { background: #fcfcfb; border: 1px solid rgba(11, 11, 11, 0.10);
           border-radius: 6px; padding: 1px 6px; }
  </style>
</head>
<body>
  <main>
    <h1>电商数仓 Web UI</h1>
    <p>后端已启动，但前端构建产物（<code>web/frontend/dist</code>）尚未就位。</p>
    <p>API 可用：<code>/api/health</code>、<code>/api/meta</code>、<code>/api/overview</code>。</p>
  </main>
</body>
</html>
"""


@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncIterator[None]:
    logger.info(
        "web backend starting: mysql=%s:%s user=%s dw=%s rt=%s pool=%s static=%s",
        settings.mysql_host,
        settings.mysql_port,
        settings.mysql_user,
        settings.dw_database,
        settings.rt_database,
        settings.pool_size,
        settings.static_dir,
    )
    yield
    db.POOL.close_all()
    logger.info("web backend stopped")


app = FastAPI(
    title="电商数仓 Web UI API",
    version="1.0.0",
    description="只读展示层：ADS 报表、实时结果、质量门禁与运维信息。",
    lifespan=lifespan,
)

for api_router in (
    meta.router,
    overview.router,
    items.router,
    retention.router,
    realtime.router,
    quality.router,
    ops.router,
):
    app.include_router(api_router, prefix="/api")


@app.exception_handler(db.DatabaseConnectionError)
async def database_connection_handler(
    request: Request, exc: db.DatabaseConnectionError
) -> JSONResponse:
    logger.error(
        "database connection error on %s %s: %s",
        request.method,
        request.url.path,
        exc,
        exc_info=(type(exc), exc, exc.__traceback__),
    )
    return JSONResponse(
        status_code=503,
        content={"detail": "database temporarily unavailable"},
    )


@app.exception_handler(db.DatabaseQueryError)
async def database_query_handler(request: Request, exc: db.DatabaseQueryError) -> JSONResponse:
    logger.error(
        "database query error on %s %s: %s",
        request.method,
        request.url.path,
        exc,
        exc_info=(type(exc), exc, exc.__traceback__),
    )
    return JSONResponse(status_code=500, content={"detail": "database query failed"})


@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    logger.error(
        "unhandled error on %s %s: %s",
        request.method,
        request.url.path,
        exc,
        exc_info=(type(exc), exc, exc.__traceback__),
    )
    return JSONResponse(status_code=500, content={"detail": "internal server error"})


def _index_html(static_dir: Path) -> HTMLResponse | FileResponse:
    index = static_dir / "index.html"
    if index.is_file():
        return FileResponse(index)
    return HTMLResponse(PLACEHOLDER_HTML)


@app.get("/{full_path:path}", include_in_schema=False, response_model=None)
def spa(full_path: str) -> HTMLResponse | FileResponse:
    """静态资源优先；未知页面回落到 index.html（SPA 路由）。"""
    if full_path == "api" or full_path.startswith("api/"):
        raise HTTPException(status_code=404, detail="Not Found")

    static_root = settings.static_dir.resolve()
    if full_path:
        candidate = (static_root / full_path).resolve()
        try:
            candidate.relative_to(static_root)
        except ValueError:
            raise HTTPException(status_code=404, detail="Not Found") from None
        if candidate.is_file():
            return FileResponse(candidate)
    return _index_html(settings.static_dir)
