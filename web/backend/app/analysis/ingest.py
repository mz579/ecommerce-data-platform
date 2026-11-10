"""解析上传文件 → 规范化 CSV → DuckDB 两层模型。

解析阶段流式读写，不把全量数据放进 Python 内存；去重交给 DuckDB 的
``GROUP BY``。进度回调由 jobs 层注入，便于更新 manifest / ETA。
"""

from __future__ import annotations

import csv
import io
import logging
import os
import time
import zipfile
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import date, datetime
from pathlib import Path
from typing import Any

import duckdb
from openpyxl import load_workbook

from .detect import REQUIRED_FIELDS
from .errors import AnalysisError, AnalysisLimitError, JobCancelled
from .normalize import normalize_event, parse_amount, parse_timestamp

logger = logging.getLogger(__name__)

NORMALIZED_HEADER = (
    "event_time",
    "event_date",
    "user_id",
    "item_id",
    "event_type",
    "transaction_id",
    "category_id",
    "amount",
    "product_name",
    "source_row",
)
PROGRESS_ROWS = 20_000
PROGRESS_SECONDS = 0.8
MAX_PRODUCT_NAME_CHARS = 500


def _cell(row: list[Any], index: int | None) -> Any:
    if index is None or index >= len(row):
        return ""
    value = row[index]
    if value is None:
        return ""
    if isinstance(value, datetime):
        return value
    return str(value).strip()


def _cell_text(row: list[Any], index: int | None) -> str:
    value = _cell(row, index)
    if isinstance(value, datetime):
        return value.isoformat(sep=" ")
    return str(value).strip()


@contextmanager
def _csv_iterator(job_dir: Path, manifest: dict[str, Any]) -> Iterator[Iterator[list[str]]]:
    preview = manifest["preview"]
    upload = job_dir / manifest["upload_name"]
    encoding = preview.get("encoding") or "utf-8"
    delimiter = preview.get("delimiter") or ","
    archive: zipfile.ZipFile | None = None
    text: io.TextIOWrapper | None = None
    try:
        if preview.get("container") == "zip":
            archive = zipfile.ZipFile(upload)
            member = preview.get("selected_member")
            if not member:
                raise AnalysisError("ZIP 未选择表格成员")
            raw = archive.open(member)
            text = io.TextIOWrapper(raw, encoding=encoding, errors="replace", newline="")
        else:
            text = upload.open("r", encoding=encoding, errors="replace", newline="")
        assert text is not None
        yield csv.reader(text, delimiter=delimiter)
    finally:
        if text is not None:
            text.close()
        if archive is not None:
            archive.close()


@contextmanager
def _xlsx_iterator(job_dir: Path, manifest: dict[str, Any]) -> Iterator[Iterator[tuple[Any, ...]]]:
    preview = manifest["preview"]
    source_name = preview.get("extracted_name") or manifest["upload_name"]
    source = job_dir / source_name
    workbook = load_workbook(filename=source, read_only=True, data_only=True)
    try:
        sheet_name = preview.get("sheet")
        worksheet = workbook[sheet_name] if sheet_name in workbook.sheetnames else workbook.active
        if worksheet is None:
            raise AnalysisError("Excel 没有工作表")
        yield worksheet.iter_rows(values_only=True)
    finally:
        workbook.close()


def _data_rows(job_dir: Path, manifest: dict[str, Any]) -> Iterator[tuple[int, list[Any]]]:
    """逐行产出 ``(source_row, cells)``；source_row 保留原文件行号。"""
    preview = manifest["preview"]
    mapping = manifest.get("mapping") or {}
    has_header = mapping.get("has_header", preview.get("has_header", True))

    if preview.get("kind") == "xlsx":
        with _xlsx_iterator(job_dir, manifest) as rows:
            for row_number, row in enumerate(rows, start=1):
                if has_header and row_number == 1:
                    continue
                yield row_number, list(row)
    else:
        with _csv_iterator(job_dir, manifest) as reader:
            for row_number, row in enumerate(reader, start=1):
                if has_header and row_number == 1:
                    continue
                yield row_number, list(row)


def _index_map(manifest: dict[str, Any]) -> dict[str, int | None]:
    preview = manifest["preview"]
    columns = [column["name"] for column in preview["columns"]]
    index_by_name = {name: index for index, name in enumerate(columns)}
    mapping = manifest.get("mapping") or {}

    result: dict[str, int | None] = {}
    for field in REQUIRED_FIELDS:
        column = mapping.get(field)
        if not column:
            raise AnalysisError(f"必填字段未映射：{field}")
        if column not in index_by_name:
            raise AnalysisError(f"映射列不存在：{column}")
        result[field] = index_by_name[column]
    for field in ("transaction_id", "category_id", "amount", "product_name"):
        column = mapping.get(field)
        if column and column in index_by_name:
            result[field] = index_by_name[column]
        else:
            result[field] = None
    return result


def ingest(
    job_dir: Path,
    manifest: dict[str, Any],
    *,
    on_progress: Callable[..., None],
    should_cancel: Callable[[], bool],
    max_rows: int,
) -> dict[str, Any]:
    """流式解析并写出 ``normalized.csv``；返回解析统计。"""
    preview = manifest["preview"]
    mapping = manifest.get("mapping") or {}
    tz_name = mapping.get("timezone") or "UTC"
    indexes = _index_map(manifest)
    rows_total = preview.get("rows_estimated") or None
    if isinstance(rows_total, int) and rows_total > max_rows:
        raise AnalysisLimitError(f"文件约 {rows_total:,} 行，超过上限 {max_rows:,} 行")

    normalized_path = job_dir / "normalized.csv"
    temp_path = job_dir / "normalized.csv.tmp"
    rejected: dict[str, int] = {}
    unknown_events = 0
    rows_read = 0
    rows_written = 0
    started = time.monotonic()
    last_emit = 0.0

    def reject(reason: str) -> None:
        rejected[reason] = rejected.get(reason, 0) + 1

    try:
        with temp_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.writer(handle)
            for source_row, row in _data_rows(job_dir, manifest):
                rows_read += 1
                if rows_read > max_rows:
                    raise AnalysisLimitError(f"数据行数超过上限 {max_rows:,}")

                user_id = _cell_text(row, indexes["user_id"])
                item_id = _cell_text(row, indexes["item_id"])
                raw_event = _cell_text(row, indexes["event_type"])
                raw_time = _cell(row, indexes["event_time"])

                missing = [
                    name
                    for name, value in (
                        ("user_id", user_id),
                        ("item_id", item_id),
                        ("event_type", raw_event),
                        ("event_time", raw_time),
                    )
                    if value is None or (isinstance(value, str) and not value)
                ]
                if missing:
                    reject("缺少必填字段：" + ",".join(missing))
                    continue

                event_time = parse_timestamp(raw_time, tz_name)
                if event_time is None:
                    reject("时间无法解析")
                    continue

                event_type = normalize_event(raw_event)
                if event_type == "other":
                    unknown_events += 1

                amount = (
                    parse_amount(_cell(row, indexes["amount"]))
                    if indexes.get("amount") is not None
                    else None
                )
                transaction_id = _cell_text(row, indexes["transaction_id"])
                category_id = _cell_text(row, indexes["category_id"])
                product_name = _cell_text(row, indexes["product_name"])[:MAX_PRODUCT_NAME_CHARS]

                writer.writerow(
                    (
                        event_time.strftime("%Y-%m-%d %H:%M:%S"),
                        event_time.date().isoformat(),
                        user_id,
                        item_id,
                        event_type,
                        transaction_id,
                        category_id,
                        "" if amount is None else repr(amount),
                        product_name,
                        source_row,
                    )
                )
                rows_written += 1

                now = time.monotonic()
                if rows_read % PROGRESS_ROWS == 0 or now - last_emit >= PROGRESS_SECONDS:
                    last_emit = now
                    if should_cancel():
                        raise JobCancelled("用户取消了分析")
                    elapsed = max(0.001, now - started)
                    rate = rows_read / elapsed
                    eta = None
                    if rows_total and rate > 0:
                        eta = max(0, rows_total - rows_read) / rate + 2.0
                    percent = 10 + 55 * min(1.0, rows_read / rows_total) if rows_total else 12
                    on_progress(
                        stage="parsing",
                        percent=percent,
                        rows_processed=rows_read,
                        rows_total=rows_total,
                        eta_seconds=eta,
                        elapsed_seconds=elapsed,
                        message=f"正在解析 {rows_read:,} 行",
                    )

        os.replace(temp_path, normalized_path)
    except Exception:
        temp_path.unlink(missing_ok=True)
        raise
    finally:
        elapsed = time.monotonic() - started

    on_progress(
        stage="normalizing",
        percent=65,
        rows_processed=rows_read,
        rows_total=rows_total,
        eta_seconds=4,
        elapsed_seconds=elapsed,
        message="解析完成，正在写入分析库",
    )
    return {
        "rows_read": rows_read,
        "rows_written": rows_written,
        "rejected": sum(rejected.values()),
        "rejected_reasons": rejected,
        "unknown_events": unknown_events,
        "parse_seconds": round(elapsed, 2),
        "rows_total": rows_total,
    }


def _sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def build_fact_table(
    job_dir: Path,
    *,
    on_progress: Callable[..., None],
) -> dict[str, int]:
    """把 normalized.csv 装入 DuckDB，去重后生成 fact_events。"""
    db_path = job_dir / "analysis.duckdb"
    normalized = job_dir / "normalized.csv"
    if not normalized.is_file():
        raise AnalysisError("规范化文件不存在")
    db_path.unlink(missing_ok=True)

    temp_dir = job_dir / "duckdb_tmp"
    temp_dir.mkdir(exist_ok=True)

    on_progress(
        stage="normalizing",
        percent=68,
        eta_seconds=4,
        message="正在写入 DuckDB",
    )

    connection = duckdb.connect(str(db_path))
    try:
        connection.execute("PRAGMA threads=2")
        connection.execute("PRAGMA memory_limit='512MB'")
        connection.execute(f"SET temp_directory={_sql_string(str(temp_dir))}")
        connection.execute(
            """
            CREATE TABLE raw_events (
                event_time VARCHAR,
                event_date VARCHAR,
                user_id VARCHAR,
                item_id VARCHAR,
                event_type VARCHAR,
                transaction_id VARCHAR,
                category_id VARCHAR,
                amount VARCHAR,
                product_name VARCHAR,
                source_row VARCHAR
            )
            """
        )
        connection.execute(
            f"COPY raw_events FROM {_sql_string(str(normalized))} "
            f"(FORMAT CSV, HEADER FALSE, NULL '')"
        )
        raw_count = int(connection.execute("SELECT COUNT(*) FROM raw_events").fetchone()[0])

        on_progress(
            stage="normalizing",
            percent=76,
            eta_seconds=2,
            message="正在去重并生成事件事实表",
        )
        connection.execute(
            """
            CREATE TABLE fact_events AS
            WITH typed AS (
                SELECT
                    CAST(event_time AS TIMESTAMP) AS event_time,
                    CAST(event_date AS DATE) AS event_date,
                    user_id,
                    item_id,
                    event_type,
                    transaction_id,
                    category_id,
                    TRY_CAST(amount AS DOUBLE) AS amount,
                    product_name,
                    CAST(source_row AS BIGINT) AS source_row
                FROM raw_events
            )
            SELECT
                event_time,
                event_date,
                user_id,
                item_id,
                event_type,
                transaction_id,
                category_id,
                amount,
                product_name,
                MIN(source_row) AS source_row
            FROM typed
            WHERE event_time IS NOT NULL
            GROUP BY
                event_time, event_date, user_id, item_id, event_type,
                transaction_id, category_id, amount, product_name
            """
        )
        fact_count = int(connection.execute("SELECT COUNT(*) FROM fact_events").fetchone()[0])
        connection.execute("DROP TABLE raw_events")
        connection.execute("CHECKPOINT")
    finally:
        connection.close()

    if fact_count == 0:
        raise AnalysisError("没有可用的事件行（必填字段或时间均无法解析）")

    return {
        "raw_count": raw_count,
        "fact_count": fact_count,
        "duplicates": max(0, raw_count - fact_count),
    }


def file_date_range(job_dir: Path) -> tuple[str | None, str | None]:
    """小工具：读取 fact_events 的日期范围（测试/日志用）。"""
    db_path = job_dir / "analysis.duckdb"
    if not db_path.is_file():
        return None, None
    connection = duckdb.connect(str(db_path), read_only=True)
    try:
        row = connection.execute(
            "SELECT MIN(event_date), MAX(event_date) FROM fact_events"
        ).fetchone()
        if not row:
            return None, None
        return (
            row[0].isoformat() if isinstance(row[0], date) else None,
            row[1].isoformat() if isinstance(row[1], date) else None,
        )
    finally:
        connection.close()
