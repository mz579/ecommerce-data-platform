"""上传分析 API：上传 → 映射 → 开始 → 轮询 → 结果 → 删除。"""

from __future__ import annotations

import time
from collections.abc import Iterator
from dataclasses import replace
from pathlib import Path

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("duckdb")
pytest.importorskip("openpyxl")
pytest.importorskip("httpx")

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.analysis.router import AnalysisRuntime, router
from app.analysis.store import STATUS_CANCELLED, STATUS_FAILED, STATUS_QUEUED
from app.config import settings

CSV_CONTENT = b"""timestamp,visitorid,event,itemid,transactionid
1433116800,101,view,888,
1433116900,102,view,888,
1433117000,101,addtocart,888,
1433117100,101,transaction,888,T1
1433203200,101,view,888,
1433203300,102,view,999,
1433203400,102,transaction,999,T2
"""


@pytest.fixture()
def api(tmp_path: Path) -> Iterator[tuple[TestClient, AnalysisRuntime]]:
    cfg = replace(
        settings,
        analysis_data_dir=tmp_path / "analysis",
        analysis_max_upload_mb=10,
        analysis_max_rows=10_000,
        analysis_job_ttl_hours=1,
        analysis_workers=1,
        analysis_queue_max=5,
        analysis_sample_enabled=False,
    )
    runtime = AnalysisRuntime.create(cfg)
    app = FastAPI()
    app.state.analysis = runtime
    app.include_router(router, prefix="/api")
    runtime.startup()
    client = TestClient(app)
    try:
        yield client, runtime
    finally:
        runtime.shutdown()


def _mapping(preview: dict) -> dict[str, str | None]:
    detected = preview["detected"]
    mapping = {
        field: detected[field]["column"]
        for field in ("user_id", "item_id", "event_type", "event_time")
    }
    mapping.update(
        {
            "transaction_id": detected["transaction_id"]["column"],
            "category_id": None,
            "amount": None,
            "product_name": None,
        }
    )
    return mapping


def test_upload_start_result_delete(
    api: tuple[TestClient, AnalysisRuntime],
) -> None:
    client, _ = api
    response = client.post(
        "/api/analysis/uploads",
        files={"file": ("events.csv", CSV_CONTENT, "text/csv")},
    )
    assert response.status_code == 201
    job = response.json()
    job_id = job["job_id"]
    assert job["status"] == "awaiting_mapping"
    assert job["preview"]["detected"]["user_id"]["column"] == "visitorid"

    response = client.post(
        f"/api/analysis/jobs/{job_id}/start",
        json={"mapping": _mapping(job["preview"]), "timezone": "UTC"},
    )
    assert response.status_code == 202

    status = ""
    for _ in range(200):
        time.sleep(0.05)
        status = client.get(f"/api/analysis/jobs/{job_id}").json()["status"]
        if status in {"done", "failed", "cancelled"}:
            break
    assert status == "done", client.get(f"/api/analysis/jobs/{job_id}").json()

    result = client.get(f"/api/analysis/jobs/{job_id}/result")
    assert result.status_code == 200
    payload = result.json()
    assert payload["job_id"] == job_id
    assert payload["totals"]["pv"] == 4
    assert payload["totals"]["orders"] == 2
    assert payload["quality"]["rows_kept"] == 7

    listing = client.get("/api/analysis/jobs").json()["jobs"]
    assert [item["job_id"] for item in listing] == [job_id]

    assert client.delete(f"/api/analysis/jobs/{job_id}").status_code == 204
    assert client.get(f"/api/analysis/jobs/{job_id}").status_code == 404


def test_start_requires_required_fields(
    api: tuple[TestClient, AnalysisRuntime],
) -> None:
    client, _ = api
    job = client.post(
        "/api/analysis/uploads",
        files={"file": ("events.csv", CSV_CONTENT, "text/csv")},
    ).json()
    response = client.post(
        f"/api/analysis/jobs/{job['job_id']}/start",
        json={"mapping": {"user_id": "visitorid"}, "timezone": "UTC"},
    )
    assert response.status_code == 400
    assert "缺少必填字段" in response.json()["detail"]


def test_result_before_done_is_conflict(
    api: tuple[TestClient, AnalysisRuntime],
) -> None:
    client, _ = api
    job = client.post(
        "/api/analysis/uploads",
        files={"file": ("events.csv", CSV_CONTENT, "text/csv")},
    ).json()
    response = client.get(f"/api/analysis/jobs/{job['job_id']}/result")
    assert response.status_code == 409


def test_rejects_unsupported_extension(
    api: tuple[TestClient, AnalysisRuntime],
) -> None:
    client, _ = api
    response = client.post(
        "/api/analysis/uploads",
        files={"file": ("events.exe", b"MZ", "application/octet-stream")},
    )
    assert response.status_code == 400


def _wait_terminal(client: TestClient, job_id: str, timeout: float = 10.0) -> str:
    deadline = time.monotonic() + timeout
    status = ""
    while time.monotonic() < deadline:
        status = client.get(f"/api/analysis/jobs/{job_id}").json()["status"]
        if status in {"done", "failed", "cancelled"}:
            return status
        time.sleep(0.05)
    return status


@pytest.mark.parametrize("terminal_status", [STATUS_FAILED, STATUS_CANCELLED])
def test_start_allows_retry_after_failed_or_cancelled(
    api: tuple[TestClient, AnalysisRuntime], terminal_status: str
) -> None:
    client, runtime = api
    job = client.post(
        "/api/analysis/uploads",
        files={"file": ("events.csv", CSV_CONTENT, "text/csv")},
    ).json()
    job_id = job["job_id"]
    runtime.store.set_status(job_id, terminal_status, error="forced for test")

    response = client.post(
        f"/api/analysis/jobs/{job_id}/start",
        json={"mapping": _mapping(job["preview"]), "timezone": "UTC"},
    )
    assert response.status_code == 202
    assert response.json()["status"] == "queued"
    assert response.json()["error"] is None

    assert _wait_terminal(client, job_id) == "done"
    # 完成后的任务不能再次启动。
    retry = client.post(
        f"/api/analysis/jobs/{job_id}/start",
        json={"mapping": _mapping(job["preview"]), "timezone": "UTC"},
    )
    assert retry.status_code == 409


def test_start_rejects_queued_job(api: tuple[TestClient, AnalysisRuntime]) -> None:
    client, runtime = api
    job = client.post(
        "/api/analysis/uploads",
        files={"file": ("events.csv", CSV_CONTENT, "text/csv")},
    ).json()
    job_id = job["job_id"]
    runtime.store.set_status(job_id, STATUS_QUEUED)

    response = client.post(
        f"/api/analysis/jobs/{job_id}/start",
        json={"mapping": _mapping(job["preview"]), "timezone": "UTC"},
    )
    assert response.status_code == 409
    assert "不可开始" in response.json()["detail"]
