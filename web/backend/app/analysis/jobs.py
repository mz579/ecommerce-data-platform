"""单 worker 任务队列：解析 → 建库 → 聚合 → 结果快照。"""

from __future__ import annotations

import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from . import ingest, metrics
from .errors import AnalysisLimitError, JobCancelled
from .store import (
    STATUS_AGGREGATING,
    STATUS_CANCELLED,
    STATUS_DONE,
    STATUS_FAILED,
    STATUS_PARSING,
    STATUS_QUEUED,
    JobManagerError,
    JobStore,
    QueueFullError,
    iso,
    utcnow,
)

logger = logging.getLogger(__name__)


class JobManager:
    def __init__(
        self,
        store: JobStore,
        *,
        max_workers: int,
        queue_max: int,
        max_rows: int,
    ) -> None:
        self.store = store
        self.queue_max = max(1, queue_max)
        self.max_rows = max_rows
        self._executor = ThreadPoolExecutor(
            max_workers=max(1, max_workers), thread_name_prefix="analysis"
        )
        self._active: set[str] = set()
        self._lock = threading.Lock()
        self._closed = False

    def startup(self) -> None:
        self.store.ensure_root()
        removed = self.store.cleanup_expired()
        interrupted = self.store.fail_interrupted()
        logger.info(
            "analysis runtime ready: root=%s removed_expired=%s interrupted=%s",
            self.store.root,
            removed,
            interrupted,
        )

    def shutdown(self) -> None:
        with self._lock:
            self._closed = True
        self._executor.shutdown(wait=False, cancel_futures=True)

    def submit(self, job_id: str) -> None:
        with self._lock:
            if self._closed:
                raise JobManagerError("analysis worker is shutting down")
            if len(self._active) >= self.queue_max:
                raise QueueFullError("分析队列已满，请稍后重试")
            self._active.add(job_id)
        try:
            # 允许 failed / cancelled 任务重新提交：清掉上一轮的取消标记与终态时间。
            self.store.mutate(
                job_id,
                lambda manifest: manifest.update(
                    cancel_requested=False,
                    error=None,
                    started_at=None,
                    finished_at=None,
                    elapsed_seconds=None,
                ),
            )
            self.store.result_path(job_id).unlink(missing_ok=True)
            self.store.set_status(job_id, STATUS_QUEUED)
            self.store.set_progress(
                job_id,
                stage=STATUS_QUEUED,
                percent=10,
                rows_processed=0,
                eta_seconds=None,
                message="已进入分析队列",
            )
            self._executor.submit(self._run, job_id)
        except Exception:
            with self._lock:
                self._active.discard(job_id)
            raise

    def request_cancel(self, job_id: str) -> None:
        self.store.mutate(job_id, lambda manifest: manifest.update(cancel_requested=True))

    def _cancel_requested(self, job_id: str) -> bool:
        try:
            return bool(self.store.read_manifest(job_id).get("cancel_requested"))
        except Exception:  # 任务被删除时按取消处理
            return True

    def _progress(self, job_id: str, **fields: Any) -> None:
        self.store.set_progress(job_id, **fields)
        if self._cancel_requested(job_id):
            raise JobCancelled("用户取消了分析")

    def _run(self, job_id: str) -> None:
        started = time.monotonic()
        try:
            manifest = self.store.read_manifest(job_id)
            if manifest.get("cancel_requested"):
                raise JobCancelled("用户取消了分析")

            self.store.set_status(job_id, STATUS_PARSING)
            self.store.set_progress(
                job_id,
                stage=STATUS_PARSING,
                percent=10,
                eta_seconds=None,
                message="开始解析文件",
            )
            job_dir = self.store.job_dir(job_id, must_exist=True)

            ingest_stats = ingest.ingest(
                job_dir,
                manifest,
                on_progress=lambda **fields: self._progress(job_id, **fields),
                should_cancel=lambda: self._cancel_requested(job_id),
                max_rows=self.max_rows,
            )
            build_stats = ingest.build_fact_table(
                job_dir,
                on_progress=lambda **fields: self._progress(job_id, **fields),
            )

            if self._cancel_requested(job_id):
                raise JobCancelled("用户取消了分析")
            self.store.set_status(job_id, STATUS_AGGREGATING)
            self.store.set_progress(
                job_id,
                stage=STATUS_AGGREGATING,
                percent=85,
                eta_seconds=3,
                message="正在汇总指标",
            )
            manifest = self.store.read_manifest(job_id)
            preview = manifest.get("preview") or {}
            result = metrics.aggregate(
                self.store.duckdb_path(job_id),
                mapping=manifest.get("mapping") or {},
                ingest_stats=ingest_stats,
                build_stats=build_stats,
                filename=manifest.get("filename") or "",
                size_bytes=int(manifest.get("size_bytes") or 0),
                file_type=manifest.get("file_type") or "",
                encoding=preview.get("encoding") or "",
                elapsed_seconds=time.monotonic() - started,
            )
            if self._cancel_requested(job_id):
                raise JobCancelled("用户取消了分析")
            result["job_id"] = job_id
            result["provenance"]["sample"] = bool(manifest.get("sample"))
            metrics.write_result(self.store.result_path(job_id), result)

            elapsed = round(time.monotonic() - started, 2)
            self.store.set_status(job_id, STATUS_DONE)
            self.store.set_progress(
                job_id,
                stage=STATUS_DONE,
                percent=100,
                rows_processed=ingest_stats.get("rows_read", 0),
                rows_total=ingest_stats.get("rows_total"),
                eta_seconds=0,
                elapsed_seconds=elapsed,
                message="分析完成",
            )
            self.store.mutate(
                job_id,
                lambda data: data.update(elapsed_seconds=elapsed, finished_at=iso(utcnow())),
            )
            logger.info("analysis job %s done in %.2fs", job_id, elapsed)
        except JobCancelled:
            self.store.set_status(job_id, STATUS_CANCELLED)
            self.store.set_progress(
                job_id,
                stage=STATUS_CANCELLED,
                percent=0,
                eta_seconds=None,
                message="已取消",
            )
            logger.info("analysis job %s cancelled", job_id)
        except AnalysisLimitError as exc:
            self.store.set_status(job_id, STATUS_FAILED, error=str(exc))
            self.store.set_progress(
                job_id,
                stage=STATUS_FAILED,
                percent=0,
                eta_seconds=None,
                message="超过分析上限",
            )
            logger.warning("analysis job %s exceeded limits: %s", job_id, exc)
        except Exception:
            logger.exception("analysis job %s failed", job_id)
            self.store.set_status(
                job_id,
                STATUS_FAILED,
                error="分析失败，请检查文件格式或字段映射后重试",
            )
            self.store.set_progress(
                job_id,
                stage=STATUS_FAILED,
                percent=0,
                eta_seconds=None,
                message="分析失败",
            )
        finally:
            elapsed = round(time.monotonic() - started, 2)
            try:
                self.store.mutate(job_id, lambda data: data.update(elapsed_seconds=elapsed))
            except Exception:  # 任务可能已被删除
                logger.debug("cannot record elapsed for %s", job_id, exc_info=True)
            with self._lock:
                self._active.discard(job_id)
