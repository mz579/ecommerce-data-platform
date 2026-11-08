"""运行时配置：只从环境变量 / 项目根 .env 读取，不在代码里写死凭据。

容器内由 docker-compose 注入 ``MYSQL_HOST=mysql``、``WEB_DB_USER`` 等；
宿主机开发时自动加载仓库根目录 ``.env``（已 gitignore）。环境变量优先级高于
.env（``load_dotenv(override=False)``）。
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


def _detect_project_root() -> Path:
    """宿主机 = 含 pyproject.toml/.env 的祖先目录；容器内回退到 /app。"""
    here = Path(__file__).resolve()
    for parent in here.parents:
        if (parent / "pyproject.toml").exists() or (parent / ".env").exists():
            return parent
    return here.parents[1] if len(here.parents) > 1 else here.parent


# 宿主机：web/backend/app/config.py -> 仓库根；容器内：/app/app/config.py -> /app
PROJECT_ROOT = _detect_project_root()
load_dotenv(PROJECT_ROOT / ".env")


def _env_int(env: Mapping[str, str], name: str, default: int) -> int:
    raw = env.get(name)
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer, got {raw!r}") from exc


def _env_path(env: Mapping[str, str], name: str, default: Path) -> Path:
    raw = env.get(name)
    if not raw:
        return default
    path = Path(raw).expanduser()
    return path if path.is_absolute() else (PROJECT_ROOT / path).resolve()


@dataclass(frozen=True)
class Settings:
    """Web 后端运行参数（不含任何写库账号）。"""

    mysql_host: str
    mysql_port: int
    mysql_user: str
    mysql_password: str
    dw_database: str
    rt_database: str
    pool_size: int
    pool_timeout: float
    connect_timeout: int
    read_timeout: int
    static_dir: Path
    log_level: str


def load_settings(environ: Mapping[str, str] | None = None) -> Settings:
    """从环境变量组装 Settings；测试可传入自定义 mapping。"""
    env = os.environ if environ is None else environ
    # 运行时只使用独立的只读账号；绝不回退到 ecom_app / MYSQL_PASSWORD。
    user = env.get("WEB_DB_USER") or "webapp_ro"
    password = env.get("WEB_DB_PASSWORD", "")
    return Settings(
        mysql_host=env.get("MYSQL_HOST") or "127.0.0.1",
        mysql_port=_env_int(env, "MYSQL_PORT", 3306),
        mysql_user=user,
        mysql_password=password,
        dw_database=env.get("MYSQL_DATABASE") or "ecommerce_dw",
        rt_database=env.get("RT_DATABASE") or "ecommerce_rt",
        pool_size=_env_int(env, "WEB_DB_POOL_SIZE", 5),
        pool_timeout=float(env.get("WEB_DB_POOL_TIMEOUT", "5")),
        connect_timeout=_env_int(env, "WEB_DB_CONNECT_TIMEOUT", 5),
        read_timeout=_env_int(env, "WEB_DB_READ_TIMEOUT", 30),
        static_dir=_env_path(env, "WEB_STATIC_DIR", PROJECT_ROOT / "web" / "frontend" / "dist"),
        log_level=(env.get("WEB_LOG_LEVEL") or "INFO").upper(),
    )


settings = load_settings()
