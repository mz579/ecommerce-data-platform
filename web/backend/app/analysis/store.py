"""任务目录、manifest 与生命周期的唯一入口。

所有路径都从服务端生成的 32 位 job_id 推导，绝不拼接用户提供的文件名；
manifest 采用临时文件 + ``os.replace`` 原子替换，避免轮询读到半个 JSON。
"""

from __future__ import annotations

import json
import logging
import os
import re
import shutil
import threading
import uuid
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

JOB_ID_RE = re.compile(r"^[a-f0-9]{32}$")
ALLOWED_UPLOAD_EXTENSIONS = {".csv", ".tsv", ".txt", ".xlsx", ".xlsm", ".zip"}

STATUS_RECEIVED = "received"
STATUS_DETECTING = "detecting"
STATUS_AWAITING_MAPPING = "awaiting_mapping"
STATUS_QUEUED = "queued"
STATUS_PARSING = "parsing"
STATUS_NORMALIZING = "normalizing"
STATUS_AGGREGATING = "aggregating"
STATUS_DONE = "done"
STATUS_FAILED = "failed"
STATUS_CANCELLED = "cancelled"

TERMINAL_STATUSES = {STATUS_DONE, STATUS_FAILED, STATUS_CANCELLED}
RUNNING_STATUSES = {
    STATUS_QUEUED,
    STATUS_PARSING,
    STATUS_NORMALIZING,
    STATUS_AGGREGATING,
}


class JobStoreError(RuntimeError):
    """任务存储层错误基类。"""


class JobManagerError(JobStoreError):
    """任务队列不可用（例如服务正在关闭）。"""


class JobNotFoundError(JobStoreError):
    pass


class InvalidJobIdError(JobStoreError):
    pass


class UploadTooLargeError(JobStoreError):
    pass


class QueueFullError(JobStoreError):
    pass


def utcnow() -> datetime:
    return datetime.now(UTC)


def iso(value: datetime) -> str:
    return value.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def safe_display_filename(filename: str | None) -> str:
    """只保留 basename，去掉路径与控制字符；仅用于展示与扩展名判断。"""
    raw = (filename or "").replace("\\", "/").split("/")[-1].strip()
    cleaned = "".join(ch for ch in raw if ch.isprintable() and ch not in {"\x00"})
    return (cleaned or "upload")[:200]


def safe_extension(filename: str) -> str:
    suffix = Path(safe_display_filename(filename)).suffix.lower()
    if suffix not in ALLOWED_UPLOAD_EXTENSIONS:
        allowed = " / ".join(sorted(ALLOWED_UPLOAD_EXTENSIONS))
        raise ValueError(f"unsupported file type {suffix or '(none)'}; allowed: {allowed}")
    return suffix


def _new_manifest(
    job_id: str, filename: str, size_bytes: int, file_type: str, sample: bool
) -> dict[str, Any]:
    now = iso(utcnow())
    return {
        "job_id": job_id,
        "status": STATUS_RECEIVED,
        "created_at": now,
        "updated_at": now,
        "filename": filename,
        "upload_name": f"upload{Path(filename).suffix.lower()}",
        "size_bytes": size_bytes,
        "file_type": file_type,
        "sample": sample,
        "preview": None,
        "mapping": None,
        "progress": {
            "stage": STATUS_RECEIVED,
            "percent": 1,
            "rows_processed": 0,
            "rows_total": None,
            "eta_seconds": None,
            "elapsed_seconds": 0,
            "message": "文件已接收",
        },
        "error": None,
        "cancel_requested": False,
        "started_at": None,
        "finished_at": None,
        "elapsed_seconds": None,
    }


class JobStore:
    """一个 ``ANALYSIS_DATA_DIR`` 对应一个 Store；测试可指向临时目录。"""

    def __init__(self, root: Path, ttl_hours: int = 168) -> None:
        self.root = Path(root)
        self.ttl_hours = ttl_hours
        self._locks: dict[str, threading.RLock] = {}
        self._locks_guard = threading.Lock()

    # ---- 路径 ----
    def ensure_root(self) -> None:
        self.root.mkdir(parents=True, exist_ok=True)

    def _lock(self, job_id: str) -> threading.RLock:
        with self._locks_guard:
            lock = self._locks.get(job_id)
            if lock is None:
                lock = threading.RLock()
                self._locks[job_id] = lock
            return lock

    def job_dir(self, job_id: str, *, must_exist: bool = False) -> Path:
        if not JOB_ID_RE.match(job_id or ""):
            raise InvalidJobIdError(f"invalid job id: {job_id!r}")
        root = self.root.resolve()
        path = (root / job_id).resolve()
        try:
            path.relative_to(root)
        except ValueError as exc:  # 防御性：正常正则下不会发生
            raise InvalidJobIdError("job path escapes analysis root") from exc
        if must_exist and not path.is_dir():
            raise JobNotFoundError(job_id)
        return path

    def manifest_path(self, job_id: str) -> Path:
        return self.job_dir(job_id, must_exist=True) / "manifest.json"

    def duckdb_path(self, job_id: str) -> Path:
        return self.job_dir(job_id, must_exist=True) / "analysis.duckdb"

    def result_path(self, job_id: str) -> Path:
        return self.job_dir(job_id, must_exist=True) / "result.json"

    # ---- 创建 / 读写 ----
    def create(
        self,
        filename: str,
        size_bytes: int,
        file_type: str,
        *,
        sample: bool = False,
    ) -> dict[str, Any]:
        self.ensure_root()
        job_id = uuid.uuid4().hex
        directory = self.job_dir(job_id)
        directory.mkdir(parents=True, exist_ok=False)
        manifest = _new_manifest(
            job_id, safe_display_filename(filename), size_bytes, file_type, sample
        )
        self._write_manifest(job_id, manifest)
        return manifest

    def _write_manifest(self, job_id: str, manifest: dict[str, Any]) -> None:
        directory = self.job_dir(job_id)
        target = directory / "manifest.json"
        tmp = directory / f".manifest.{os.getpid()}.{threading.get_ident()}.tmp"
        with tmp.open("w", encoding="utf-8") as handle:
            json.dump(manifest, handle, ensure_ascii=False, indent=2)
        os.replace(tmp, target)

    def read_manifest(self, job_id: str) -> dict[str, Any]:
        path = self.manifest_path(job_id)
        try:
            with path.open("r", encoding="utf-8") as handle:
                data = json.load(handle)
        except json.JSONDecodeError as exc:
            raise JobStoreError(f"manifest for {job_id} is corrupt") from exc
        if not isinstance(data, dict):
            raise JobStoreError(f"manifest for {job_id} is not an object")
        return data

    def write_manifest(self, job_id: str, manifest: dict[str, Any]) -> None:
        with self._lock(job_id):
            manifest["updated_at"] = iso(utcnow())
            self._write_manifest(job_id, manifest)

    def mutate(self, job_id: str, fn: Callable[[dict[str, Any]], None]) -> dict[str, Any]:
        """在任务锁内读改写 manifest，避免并发轮询/worker 更新互相覆盖。"""
        with self._lock(job_id):
            manifest = self.read_manifest(job_id)
            fn(manifest)
            manifest["updated_at"] = iso(utcnow())
            self._write_manifest(job_id, manifest)
            return manifest

    def set_status(self, job_id: str, status: str, *, error: str | None = None) -> dict[str, Any]:
        def apply(manifest: dict[str, Any]) -> None:
            manifest["status"] = status
            manifest["error"] = error
            if status in {STATUS_DONE, STATUS_FAILED, STATUS_CANCELLED}:
                manifest["finished_at"] = iso(utcnow())
            if status == STATUS_PARSING and not manifest.get("started_at"):
                manifest["started_at"] = iso(utcnow())

        return self.mutate(job_id, apply)

    def set_progress(self, job_id: str, **fields: Any) -> dict[str, Any]:
        def apply(manifest: dict[str, Any]) -> None:
            progress = manifest.setdefault("progress", {})
            progress.update(fields)
            percent = progress.get("percent")
            if isinstance(percent, (int, float)):
                progress["percent"] = max(0, min(100, round(float(percent), 1)))
            eta = progress.get("eta_seconds")
            if isinstance(eta, (int, float)):
                progress["eta_seconds"] = max(0, int(eta))

        return self.mutate(job_id, apply)

    def update_preview(self, job_id: str, preview: dict[str, Any]) -> dict[str, Any]:
        def apply(manifest: dict[str, Any]) -> None:
            manifest["preview"] = preview

        return self.mutate(job_id, apply)

    # ---- 查询 ----
    def list_jobs(self, limit: int = 10) -> list[dict[str, Any]]:
        if not self.root.is_dir():
            return []
        items: list[dict[str, Any]] = []
        for child in self.root.iterdir():
            if not child.is_dir() or not JOB_ID_RE.match(child.name):
                continue
            try:
                manifest = self.read_manifest(child.name)
            except (JobStoreError, OSError):
                logger.warning("skip unreadable analysis job %s", child.name, exc_info=True)
                continue
            items.append(self.public_job(manifest, summary=True))
        items.sort(key=lambda item: item.get("created_at") or "", reverse=True)
        return items[: max(0, limit)]

    @staticmethod
    def public_job(manifest: dict[str, Any], *, summary: bool = False) -> dict[str, Any]:
        """返回给前端的字段白名单；不含服务器路径。"""
        data: dict[str, Any] = {
            "job_id": manifest.get("job_id"),
            "status": manifest.get("status"),
            "created_at": manifest.get("created_at"),
            "updated_at": manifest.get("updated_at"),
            "filename": manifest.get("filename"),
            "size_bytes": manifest.get("size_bytes"),
            "file_type": manifest.get("file_type"),
            "sample": bool(manifest.get("sample")),
            "progress": manifest.get("progress"),
            "error": manifest.get("error"),
            "started_at": manifest.get("started_at"),
            "finished_at": manifest.get("finished_at"),
            "elapsed_seconds": manifest.get("elapsed_seconds"),
        }
        if not summary:
            data["preview"] = manifest.get("preview")
            data["mapping"] = manifest.get("mapping")
        return data

    # ---- 清理 / 恢复 ----
    def cleanup_expired(self) -> int:
        """删除超过 TTL 的任务目录；返回删除数量。"""
        if not self.root.is_dir():
            return 0
        cutoff = utcnow() - timedelta(hours=self.ttl_hours)
        removed = 0
        for child in list(self.root.iterdir()):
            if not child.is_dir() or not JOB_ID_RE.match(child.name):
                continue
            try:
                manifest = self.read_manifest(child.name)
                created = datetime.fromisoformat(
                    str(manifest.get("created_at", "")).replace("Z", "+00:00")
                )
            except (JobStoreError, OSError, ValueError):
                created = datetime.fromtimestamp(child.stat().st_mtime, tz=UTC)
            if created < cutoff:
                if self.delete(child.name, ignore_errors=True):
                    removed += 1
        return removed

    def fail_interrupted(self) -> int:
        """服务重启后，把没有 worker 的进行中任务标记为 failed。"""
        count = 0
        if not self.root.is_dir():
            return count
        for child in list(self.root.iterdir()):
            if not child.is_dir() or not JOB_ID_RE.match(child.name):
                continue
            try:
                manifest = self.read_manifest(child.name)
            except (JobStoreError, OSError):
                continue
            if manifest.get("status") in RUNNING_STATUSES:
                self.set_status(
                    child.name,
                    STATUS_FAILED,
                    error="服务重启，任务中断；可在任务页调整映射后重试",
                )
                count += 1
        return count

    def delete(self, job_id: str, *, ignore_errors: bool = False) -> bool:
        try:
            directory = self.job_dir(job_id, must_exist=True)
        except JobNotFoundError:
            return False
        with self._lock(job_id):
            try:
                shutil.rmtree(directory)
            except OSError:
                if not ignore_errors:
                    raise
                shutil.rmtree(directory, ignore_errors=True)
        with self._locks_guard:
            self._locks.pop(job_id, None)
        return True
