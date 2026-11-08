"""Web 后端测试夹具：使用真实本地 MySQL（只读账号）与 FastAPI TestClient。"""

from __future__ import annotations

import sys
from collections.abc import Iterator
from pathlib import Path

import pytest

BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from fastapi.testclient import TestClient  # noqa: E402

from app import db  # noqa: E402
from app.main import app  # noqa: E402


@pytest.fixture(scope="session", autouse=True)
def require_readonly_database() -> None:
    """库不可达时整组跳过并给出明确原因（与现有 integration 测试风格一致）。"""
    try:
        db.POOL.fetch_one("SELECT 1 AS ok")
    except db.DatabaseConnectionError as exc:
        pytest.skip(f"cannot reach MySQL with the webapp_ro account: {exc}")


@pytest.fixture(scope="session")
def client() -> Iterator[TestClient]:
    with TestClient(app) as test_client:
        yield test_client
