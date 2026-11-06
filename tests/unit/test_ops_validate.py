"""Unit tests for the ODS source/contract gates (no DB, filesystem only)."""

from __future__ import annotations

from pathlib import Path

from ecommerce_dw.ops.validate import SOURCE_FILES, check_source_files, dataset_dir


def _make_dataset(tmp_path: Path, files: dict[str, str]) -> Path:
    """Write the given filename->content map into tmp_path/data/raw/retailrocket."""
    root = tmp_path
    d = dataset_dir(root)
    d.mkdir(parents=True, exist_ok=True)
    for name, content in files.items():
        (d / name).write_text(content, encoding="utf-8")
    return root


def _ok_files() -> dict[str, str]:
    return {
        "category_tree.csv": "categoryid,parentid\n1,0\n",
        "events.csv": "timestamp,visitorid,event,itemid,transactionid\n1,2,view,3,\n",
        "item_properties_part1.csv": "timestamp,itemid,property,value\n1,2,3,4\n",
        "item_properties_part2.csv": "timestamp,itemid,property,value\n1,2,3,4\n",
    }


def test_expected_sources_constant():
    assert SOURCE_FILES == (
        "category_tree.csv",
        "events.csv",
        "item_properties_part1.csv",
        "item_properties_part2.csv",
    )


def test_dataset_dir_layout():
    assert dataset_dir(Path("/x")) == Path("/x") / "data" / "raw" / "retailrocket"


def test_check_source_files_all_ok(tmp_path):
    findings = check_source_files(_make_dataset(tmp_path, _ok_files()))
    assert len(findings) == 4
    assert all(f.ok for f in findings)
    assert all("size=" in f.message for f in findings)


def test_missing_file_fails(tmp_path):
    files = _ok_files()
    del files["events.csv"]
    findings = check_source_files(_make_dataset(tmp_path, files))
    by_name = {f.file: f for f in findings}
    assert not by_name["events.csv"].ok
    assert "not found" in by_name["events.csv"].message
    assert sum(1 for f in findings if f.ok) == 3


def test_empty_file_fails(tmp_path):
    files = _ok_files()
    files["category_tree.csv"] = ""
    findings = check_source_files(_make_dataset(tmp_path, files))
    by_name = {f.file: f for f in findings}
    assert not by_name["category_tree.csv"].ok
    assert "empty" in by_name["category_tree.csv"].message


def test_header_mismatch_fails(tmp_path):
    files = _ok_files()
    files["events.csv"] = "wrong,header,here,now\n"
    findings = check_source_files(_make_dataset(tmp_path, files))
    by_name = {f.file: f for f in findings}
    assert not by_name["events.csv"].ok
    assert "header mismatch" in by_name["events.csv"].message
    assert "got" in by_name["events.csv"].message  # shows both sides


def test_unreadable_header_fails(tmp_path):
    files = _ok_files()
    files["item_properties_part1.csv"] = ",,,\n"
    findings = check_source_files(_make_dataset(tmp_path, files))
    by_name = {f.file: f for f in findings}
    assert not by_name["item_properties_part1.csv"].ok
    assert "header" in by_name["item_properties_part1.csv"].message
