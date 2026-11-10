"""上传分析：文件探测与字段评分。"""

from __future__ import annotations

import csv
import zipfile
from pathlib import Path

import pytest

pytest.importorskip("openpyxl")
pytest.importorskip("duckdb")

from openpyxl import Workbook

from app.analysis.detect import DetectionError, inspect_file


def _write_csv(path: Path, rows: list[list[str]], *, encoding: str = "utf-8") -> None:
    with path.open("w", newline="", encoding=encoding) as handle:
        csv.writer(handle).writerows(rows)


def test_detects_retailrocket_columns(tmp_path: Path) -> None:
    path = tmp_path / "events.csv"
    _write_csv(
        path,
        [
            ["timestamp", "visitorid", "event", "itemid", "transactionid"],
            ["1433116800", "101", "view", "888", ""],
            ["1433116900", "102", "view", "999", ""],
            ["1433117000", "101", "add_to_cart", "888", ""],
            ["1433117100", "101", "transaction", "888", "T1"],
        ],
    )

    preview = inspect_file(path, path.name, tmp_path)

    assert preview["kind"] == "csv"
    assert preview["has_header"] is True
    assert preview["rows_estimated"] == 4
    assert preview["detected"]["user_id"]["column"] == "visitorid"
    assert preview["detected"]["item_id"]["column"] == "itemid"
    assert preview["detected"]["event_type"]["column"] == "event"
    assert preview["detected"]["event_time"]["column"] == "timestamp"
    assert preview["detected"]["transaction_id"]["column"] == "transactionid"
    for field in ("user_id", "item_id", "event_type", "event_time"):
        assert preview["detected"][field]["confidence"] >= 0.75


def test_detects_chinese_headers(tmp_path: Path) -> None:
    path = tmp_path / "中文.csv"
    _write_csv(
        path,
        [
            ["用户ID", "商品ID", "事件类型", "事件时间", "订单号"],
            ["u1", "p1", "浏览", "2015-06-01 10:00:00", ""],
            ["u2", "p1", "加购", "2015-06-01 10:01:00", ""],
            ["u1", "p1", "购买", "2015-06-01 10:02:00", "o1"],
        ],
        encoding="utf-8-sig",
    )

    preview = inspect_file(path, path.name, tmp_path)

    assert preview["detected"]["user_id"]["column"] == "用户ID"
    assert preview["detected"]["item_id"]["column"] == "商品ID"
    assert preview["detected"]["event_type"]["column"] == "事件类型"
    assert preview["detected"]["event_time"]["column"] == "事件时间"


def test_headerless_file_gets_generated_column_names(tmp_path: Path) -> None:
    path = tmp_path / "headerless.csv"
    _write_csv(
        path,
        [
            ["1433116800", "101", "view", "888", ""],
            ["1433116900", "102", "view", "999", ""],
        ],
    )

    preview = inspect_file(path, path.name, tmp_path)

    assert preview["has_header"] is False
    assert [column["name"] for column in preview["columns"]] == [
        "col_1",
        "col_2",
        "col_3",
        "col_4",
        "col_5",
    ]


def test_detects_xlsx_and_sheet(tmp_path: Path) -> None:
    path = tmp_path / "events.xlsx"
    workbook = Workbook()
    sheet = workbook.active
    sheet.title = "events"
    sheet.append(["timestamp", "visitorid", "event", "itemid"])
    sheet.append([1433116800, 101, "view", 888])
    sheet.append([1433116900, 102, "view", 999])
    workbook.save(path)

    preview = inspect_file(path, path.name, tmp_path)

    assert preview["kind"] == "xlsx"
    assert preview["sheet"] == "events"
    assert preview["detected"]["event_time"]["column"] == "timestamp"
    assert preview["detected"]["user_id"]["column"] == "visitorid"


def test_detects_csv_inside_zip(tmp_path: Path) -> None:
    source = tmp_path / "events.csv"
    _write_csv(
        source,
        [
            ["timestamp", "visitorid", "event", "itemid"],
            ["1433116800", "101", "view", "888"],
            ["1433116900", "102", "view", "999"],
        ],
    )
    archive = tmp_path / "events.zip"
    with zipfile.ZipFile(archive, "w") as handle:
        handle.write(source, "nested/events.csv")

    preview = inspect_file(archive, archive.name, tmp_path)

    assert preview["container"] == "zip"
    assert preview["selected_member"] == "nested/events.csv"
    assert preview["detected"]["user_id"]["column"] == "visitorid"


def test_rejects_zip_path_traversal(tmp_path: Path) -> None:
    archive = tmp_path / "evil.zip"
    with zipfile.ZipFile(archive, "w") as handle:
        handle.writestr("../evil.csv", "timestamp,visitorid,event,itemid\n1,2,view,3\n")

    with pytest.raises(DetectionError):
        inspect_file(archive, archive.name, tmp_path)
