"""上传分析 API。

路由只操作 ``ANALYSIS_DATA_DIR``：上传落盘 → 探测 → 用户确认映射 → 单 worker
队列分析 → 结果快照。不导入 ``app.db``，不接触 MySQL。
"""

from __future__ import annotations

import json
import logging
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Annotated, Any

from fastapi import APIRouter, Depends, File, HTTPException, Request, Response, UploadFile
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from ..config import Settings
from . import detect, sample
from .detect import ALL_FIELDS, REQUIRED_FIELDS, DetectionError
from .jobs import JobManager
from .normalize import validate_timezone
from .store import (
    RUNNING_STATUSES,
    STATUS_AWAITING_MAPPING,
    STATUS_CANCELLED,
    STATUS_DONE,
    STATUS_FAILED,
    InvalidJobIdError,
    JobManagerError,
    JobNotFoundError,
    JobStore,
    JobStoreError,
    QueueFullError,
    UploadTooLargeError,
    safe_display_filename,
    safe_extension,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/analysis", tags=["analysis"])


@dataclass
class AnalysisRuntime:
    cfg: Settings
    store: JobStore
    jobs: JobManager

    @classmethod
    def create(cls, cfg: Settings) -> AnalysisRuntime:
        store = JobStore(cfg.analysis_data_dir, cfg.analysis_job_ttl_hours)
        jobs = JobManager(
            store,
            max_workers=cfg.analysis_workers,
            queue_max=cfg.analysis_queue_max,
            max_rows=cfg.analysis_max_rows,
        )
        return cls(cfg=cfg, store=store, jobs=jobs)

    def startup(self) -> None:
        self.jobs.startup()

    def shutdown(self) -> None:
        self.jobs.shutdown()


def get_runtime(request: Request) -> AnalysisRuntime:
    runtime = getattr(request.app.state, "analysis", None)
    if runtime is None:
        raise HTTPException(status_code=503, detail="analysis service not ready")
    return runtime


RuntimeDep = Annotated[AnalysisRuntime, Depends(get_runtime)]


class StartRequest(BaseModel):
    mapping: dict[str, str | None]
    timezone: str = "UTC"
    has_header: bool | None = None


def _read_manifest(runtime: AnalysisRuntime, job_id: str) -> dict[str, Any]:
    try:
        return runtime.store.read_manifest(job_id)
    except InvalidJobIdError as exc:
        raise HTTPException(status_code=400, detail="非法任务 ID") from exc
    except JobNotFoundError as exc:
        raise HTTPException(status_code=404, detail="任务不存在") from exc
    except JobStoreError as exc:
        logger.error("cannot read analysis manifest %s: %s", job_id, exc)
        raise HTTPException(status_code=500, detail="任务状态读取失败") from exc


def _public(runtime: AnalysisRuntime, job_id: str) -> dict[str, Any]:
    return runtime.store.public_job(_read_manifest(runtime, job_id))


def _prepare_preview(
    runtime: AnalysisRuntime, job_id: str, target: Path, filename: str
) -> dict[str, Any]:
    job_dir = runtime.store.job_dir(job_id, must_exist=True)
    preview = detect.inspect_file(target, filename, job_dir)
    runtime.store.update_preview(job_id, preview)
    runtime.store.set_status(job_id, STATUS_AWAITING_MAPPING)
    runtime.store.set_progress(
        job_id,
        stage=STATUS_AWAITING_MAPPING,
        percent=8,
        rows_total=preview.get("rows_estimated"),
        eta_seconds=None,
        message="请确认字段映射",
    )
    return preview


@router.post("/uploads", status_code=201)
async def upload(
    runtime: RuntimeDep,
    file: Annotated[UploadFile, File(...)],
) -> dict[str, Any]:
    if not file.filename:
        raise HTTPException(status_code=400, detail="缺少文件名")
    try:
        extension = safe_extension(file.filename)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    filename = safe_display_filename(file.filename)
    manifest = runtime.store.create(filename, 0, extension.lstrip("."))
    job_id = str(manifest["job_id"])
    job_dir = runtime.store.job_dir(job_id, must_exist=True)
    target = job_dir / str(manifest["upload_name"])
    max_bytes = max(1, runtime.cfg.analysis_max_upload_mb) * 1024 * 1024
    size = 0

    try:
        with target.open("wb") as output:
            while True:
                chunk = await file.read(1024 * 1024)
                if not chunk:
                    break
                size += len(chunk)
                if size > max_bytes:
                    raise UploadTooLargeError(
                        f"文件超过 {runtime.cfg.analysis_max_upload_mb}MB 上限"
                    )
                output.write(chunk)
        if size == 0:
            raise ValueError("文件为空")
        runtime.store.mutate(job_id, lambda data: data.update(size_bytes=size))
        await run_in_threadpool(_prepare_preview, runtime, job_id, target, filename)
        return _public(runtime, job_id)
    except UploadTooLargeError as exc:
        runtime.store.delete(job_id, ignore_errors=True)
        raise HTTPException(status_code=413, detail=str(exc)) from exc
    except DetectionError as exc:
        runtime.store.delete(job_id, ignore_errors=True)
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except ValueError as exc:
        runtime.store.delete(job_id, ignore_errors=True)
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except HTTPException:
        runtime.store.delete(job_id, ignore_errors=True)
        raise
    except Exception as exc:
        logger.exception("analysis upload failed for job %s", job_id)
        runtime.store.delete(job_id, ignore_errors=True)
        raise HTTPException(status_code=500, detail="上传处理失败") from exc
    finally:
        await file.close()


@router.post("/sample", status_code=201)
def create_sample(runtime: RuntimeDep) -> dict[str, Any]:
    if not runtime.cfg.analysis_sample_enabled:
        raise HTTPException(status_code=404, detail="示例数据未启用")
    source = sample.sample_path()
    if not source.is_file():
        raise HTTPException(status_code=404, detail="示例数据文件不存在")

    manifest = runtime.store.create(source.name, source.stat().st_size, "csv", sample=True)
    job_id = str(manifest["job_id"])
    try:
        job_dir = runtime.store.job_dir(job_id, must_exist=True)
        target = job_dir / str(manifest["upload_name"])
        shutil.copyfile(source, target)
        _prepare_preview(runtime, job_id, target, source.name)
        return _public(runtime, job_id)
    except DetectionError as exc:
        runtime.store.delete(job_id, ignore_errors=True)
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except Exception as exc:
        logger.exception("cannot create sample analysis job")
        runtime.store.delete(job_id, ignore_errors=True)
        raise HTTPException(status_code=500, detail="示例任务创建失败") from exc


@router.get("/jobs")
def list_jobs(runtime: RuntimeDep, limit: int = 10) -> dict[str, Any]:
    safe_limit = max(1, min(int(limit), 50))
    return {"jobs": runtime.store.list_jobs(safe_limit)}


@router.get("/jobs/{job_id}")
def get_job(job_id: str, runtime: RuntimeDep) -> dict[str, Any]:
    return _public(runtime, job_id)


@router.post("/jobs/{job_id}/start", status_code=202)
def start_job(
    job_id: str,
    payload: StartRequest,
    runtime: RuntimeDep,
) -> dict[str, Any]:
    manifest = _read_manifest(runtime, job_id)
    if manifest.get("status") not in {
        STATUS_AWAITING_MAPPING,
        STATUS_FAILED,
        STATUS_CANCELLED,
    }:
        raise HTTPException(status_code=409, detail="任务当前状态不可开始分析")

    preview = manifest.get("preview") or {}
    columns = {column["name"] for column in preview.get("columns", [])}
    clean_mapping = {field: (payload.mapping.get(field) or None) for field in ALL_FIELDS}
    missing = [field for field in REQUIRED_FIELDS if not clean_mapping.get(field)]
    if missing:
        raise HTTPException(status_code=400, detail=f"缺少必填字段映射：{', '.join(missing)}")
    for field, column in clean_mapping.items():
        if column and column not in columns:
            raise HTTPException(status_code=400, detail=f"字段 {field} 映射的列不存在：{column}")

    try:
        timezone = validate_timezone(payload.timezone)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    rows_estimated = preview.get("rows_estimated")
    if isinstance(rows_estimated, int) and rows_estimated > runtime.cfg.analysis_max_rows:
        raise HTTPException(
            status_code=413,
            detail=f"文件约 {rows_estimated:,} 行，超过上限 {runtime.cfg.analysis_max_rows:,} 行",
        )

    stored_mapping: dict[str, Any] = {
        **clean_mapping,
        "timezone": timezone,
        "has_header": (
            payload.has_header
            if payload.has_header is not None
            else bool(preview.get("has_header", True))
        ),
    }
    runtime.store.mutate(job_id, lambda data: data.update(mapping=stored_mapping))
    try:
        runtime.jobs.submit(job_id)
    except QueueFullError as exc:
        raise HTTPException(status_code=429, detail=str(exc)) from exc
    except JobManagerError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    return _public(runtime, job_id)


@router.get("/jobs/{job_id}/result")
def get_result(job_id: str, runtime: RuntimeDep) -> dict[str, Any]:
    manifest = _read_manifest(runtime, job_id)
    if manifest.get("status") != STATUS_DONE:
        raise HTTPException(status_code=409, detail="分析尚未完成")
    path = runtime.store.result_path(job_id)
    try:
        with path.open("r", encoding="utf-8") as handle:
            result = json.load(handle)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=409, detail="结果文件尚未生成") from exc
    except json.JSONDecodeError as exc:
        logger.error("analysis result %s is corrupt", job_id)
        raise HTTPException(status_code=500, detail="结果文件损坏") from exc
    return result


@router.delete("/jobs/{job_id}", status_code=204)
def delete_job(job_id: str, runtime: RuntimeDep) -> Response:
    manifest = _read_manifest(runtime, job_id)
    status = manifest.get("status")
    if status in RUNNING_STATUSES:
        runtime.jobs.request_cancel(job_id)
        return JSONResponse(status_code=202, content={"status": "cancelling"})
    runtime.store.delete(job_id)
    logger.info("analysis job %s deleted (status=%s)", job_id, status)
    return Response(status_code=204)
