# ============================================================================
# Phase 11 — Web UI 单容器镜像（多阶段）
#
#   node:20-alpine     构建 React/Vite 前端 → dist
#   python:3.11-slim   安装 FastAPI 后端，同进程托管 /api 与 SPA 静态资源
#
# 前端构建模式（build arg WEB_FRONTEND_MODE）：
#   auto（默认）：有前端源码时容器内 npm ci && npm run build；若构建失败且
#                 存在预构建 dist，则回退复用（SPEC §6 网络退化路径）；
#   build       ：强制容器内构建，失败即失败；
#   prebuilt    ：直接使用宿主机预构建的 web/frontend/dist（离线构建）；
#   skip        ：写占位 index.html，仅验证后端镜像（前端尚未就绪时用）。
#
# 运行时不包含 .env（.dockerignore 已排除），凭据由 docker-compose 注入。
# ============================================================================

FROM node:20-alpine AS frontend-build
WORKDIR /build

# 只复制 web/ 目录：backend 用于让上下文在 frontend 尚未创建时也可构建；
# node_modules 已由仓库根 .dockerignore 排除。
COPY web ./web

ARG WEB_FRONTEND_MODE=auto
RUN set -eux; \
    PLACEHOLDER='<!doctype html><html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><title>电商数仓 Web UI</title></head><body><main><h1>电商数仓 Web UI</h1><p>前端构建产物尚未就位；后端 API 可用。</p></main></body></html>'; \
    mkdir -p web/frontend/dist; \
    if [ "$WEB_FRONTEND_MODE" = "skip" ]; then \
        echo "WEB_FRONTEND_MODE=skip: writing placeholder index.html"; \
        printf '%s' "$PLACEHOLDER" > web/frontend/dist/index.html; \
    elif [ "$WEB_FRONTEND_MODE" = "prebuilt" ]; then \
        echo "WEB_FRONTEND_MODE=prebuilt: requiring web/frontend/dist/index.html"; \
        test -f web/frontend/dist/index.html; \
    elif [ -f web/frontend/package.json ]; then \
        if ! ( cd web/frontend; \
               if [ -f package-lock.json ]; then npm ci --no-audit --no-fund; \
               else npm install --no-audit --no-fund; fi; \
               npm run build ); then \
            if [ "$WEB_FRONTEND_MODE" = "auto" ] && [ -f web/frontend/dist/index.html ]; then \
                echo "npm build failed; falling back to prebuilt web/frontend/dist" >&2; \
            else \
                echo "npm build failed and no prebuilt dist fallback available" >&2; \
                exit 1; \
            fi; \
        fi; \
    elif [ "$WEB_FRONTEND_MODE" = "build" ]; then \
        echo "WEB_FRONTEND_MODE=build but web/frontend/package.json is missing" >&2; \
        exit 1; \
    elif [ -f web/frontend/dist/index.html ]; then \
        echo "frontend sources absent: using prebuilt web/frontend/dist"; \
    else \
        echo "frontend sources and dist absent: writing placeholder index.html"; \
        printf '%s' "$PLACEHOLDER" > web/frontend/dist/index.html; \
    fi; \
    test -f web/frontend/dist/index.html

FROM python:3.11-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_INDEX_URL=https://pypi.tuna.tsinghua.edu.cn/simple \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    WEB_STATIC_DIR=/app/static

WORKDIR /app

COPY web/backend/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY web/backend/app ./app
COPY --from=frontend-build /build/web/frontend/dist ./static

RUN useradd --create-home --uid 10001 appuser \
    && mkdir -p /app/analysis \
    && chown -R appuser:appuser /app
USER appuser

# 上传分析沙箱：compose 会把命名卷挂到这里；目录必须对 appuser 可写。
VOLUME ["/app/analysis"]

EXPOSE 8090

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8090"]
