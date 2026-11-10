"""上传分析：解析 → DuckDB 两层模型 → 指标。"""

from __future__ import annotations

import csv
from pathlib import Path

import pytest

pytest.importorskip("duckdb")
pytest.importorskip("openpyxl")

from app.analysis import detect, ingest, metrics
from app.analysis.errors import AnalysisLimitError


def _prepare(tmp_path: Path, rows: list[list[str]]) -> tuple[Path, dict]:
    job_dir = tmp_path / "job"
    job_dir.mkdir()
    upload = job_dir / "upload.csv"
    with upload.open("w", newline="", encoding="utf-8") as handle:
        csv.writer(handle).writerows(rows)
    preview = detect.inspect_file(upload, upload.name, job_dir)
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
            "timezone": "UTC",
            "has_header": preview["has_header"],
        }
    )
    manifest = {
        "upload_name": upload.name,
        "filename": upload.name,
        "preview": preview,
        "mapping": mapping,
        "sample": False,
    }
    return job_dir, manifest


def _run(job_dir: Path, manifest: dict) -> tuple[dict, dict, dict]:
    stats = ingest.ingest(
        job_dir,
        manifest,
        on_progress=lambda **_: None,
        should_cancel=lambda: False,
        max_rows=1000,
    )
    build = ingest.build_fact_table(job_dir, on_progress=lambda **_: None)
    result = metrics.aggregate(
        job_dir / "analysis.duckdb",
        mapping=manifest["mapping"],
        ingest_stats=stats,
        build_stats=build,
        filename=manifest["filename"],
        size_bytes=1,
        file_type="csv",
        encoding=manifest["preview"]["encoding"],
        elapsed_seconds=1.0,
    )
    return stats, build, result


def test_pipeline_core_metrics(tmp_path: Path) -> None:
    job_dir, manifest = _prepare(
        tmp_path,
        [
            ["timestamp", "visitorid", "event", "itemid", "transactionid"],
            ["1433116800", "101", "view", "888", ""],
            ["1433116900", "102", "view", "888", ""],
            ["1433117000", "101", "addtocart", "888", ""],
            ["1433117100", "101", "transaction", "888", "T1"],
            ["1433203200", "101", "view", "888", ""],
            ["1433203300", "102", "view", "999", ""],
            ["1433203400", "102", "transaction", "999", "T2"],
            ["1433203500", "103", "view", "888", ""],
            ["1433203500", "103", "view", "888", ""],  # 重复
            ["badtime", "104", "view", "888", ""],  # 时间不可解析
            ["1433203600", "", "view", "888", ""],  # 缺少 user
            ["1433203700", "105", "wat", "888", ""],  # 未知事件
        ],
    )

    stats, build, result = _run(job_dir, manifest)

    assert stats["rows_read"] == 12
    assert stats["rows_written"] == 10
    assert stats["rejected"] == 2
    assert stats["unknown_events"] == 1
    assert build == {"raw_count": 10, "fact_count": 9, "duplicates": 1}

    totals = result["totals"]
    assert totals["events"] == 9
    assert totals["pv"] == 5
    assert totals["uv"] == 3
    assert totals["view_users"] == 3
    assert totals["cart_users"] == 1
    assert totals["buyer_users"] == 2
    assert totals["orders"] == 2
    assert totals["d1_retention"] == 1.0
    assert result["range"] == {
        "date_min": "2015-06-01",
        "date_max": "2015-06-02",
        "days": 2,
    }
    assert len(result["traffic"]) == 2
    assert result["d1_series"][-1]["window_status"] == "partial"
    assert result["quality"]["duplicates"] == 1
    assert result["quality"]["rows_rejected"] == 2
    assert result["quality"]["orders_basis"] == "transaction_id"


def test_pipeline_optional_amount_category_and_name(tmp_path: Path) -> None:
    job_dir, manifest = _prepare(
        tmp_path,
        [
            [
                "timestamp",
                "visitorid",
                "event",
                "itemid",
                "transactionid",
                "categoryid",
                "amount",
                "productname",
            ],
            ["1433116800", "101", "view", "888", "", "c1", "", "Red Mug"],
            ["1433116900", "101", "transaction", "888", "T1", "c1", "19.90", "Red Mug"],
            ["1433203200", "102", "view", "999", "", "c2", "", "Blue Cup"],
            ["1433203300", "102", "transaction", "999", "T2", "c2", "9.50", "Blue Cup"],
        ],
    )
    # 可选字段由探测自动识别；这里显式补进 mapping。
    for field in ("category_id", "amount", "product_name"):
        manifest["mapping"][field] = manifest["preview"]["detected"][field]["column"]

    _, _, result = _run(job_dir, manifest)

    assert result["totals"]["revenue"] == pytest.approx(29.4)
    assert result["has_amount"] is True
    assert result["has_category_id"] is True
    assert result["has_product_name"] is True
    assert {row["item_id"] for row in result["top_items"]} == {"888", "999"}
    assert {row["product_name"] for row in result["top_items"]} == {"Red Mug", "Blue Cup"}
    assert len(result["categories"]) == 2


def test_pipeline_enforces_row_limit(tmp_path: Path) -> None:
    job_dir, manifest = _prepare(
        tmp_path,
        [
            ["timestamp", "visitorid", "event", "itemid"],
            ["1433116800", "101", "view", "888"],
            ["1433116900", "102", "view", "999"],
            ["1433117000", "103", "view", "777"],
        ],
    )
    with pytest.raises(AnalysisLimitError):
        ingest.ingest(
            job_dir,
            manifest,
            on_progress=lambda **_: None,
            should_cancel=lambda: False,
            max_rows=2,
        )
