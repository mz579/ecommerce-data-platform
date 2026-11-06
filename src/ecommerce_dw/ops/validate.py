"""源文件与 ODS 契约校验（Phase 6，check_source_files / validate_ods）。

两条闸与 Phase 5 门禁分工：

- ``check_source_files``：只读文件系统——4 个 ODS 源 CSV 必须存在、非空、
  表头与 ``ingestion.specs`` 契约一致。在打开数据库之前跑，漏文件尽早 fail-fast。
- ``validate_ods``：文件 + ``etl_batch_log`` 契约——每个源文件的最新批次必须
  LOADED、行数 > 0、且**当前磁盘文件 sha256** 与入库时记录的 sha256 一致
  （文件被改动过但还没 reload 会在这里被拦下，而不是让后续层静默重算）。

两者都不做全表扫描（``validate_ods`` 只读 ``etl_batch_log`` 最近一批 + 磁盘 sha256），
运行时开销在秒级。
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from ecommerce_dw.config import DbConfig
from ecommerce_dw.incremental.runner import connect as runner_connect
from ecommerce_dw.ingestion.batch_log import BatchLogRepo
from ecommerce_dw.ingestion.chunk import read_csv_header
from ecommerce_dw.ingestion.keys import sha256_file
from ecommerce_dw.ingestion.specs import infer_table_for_file, table_spec

SOURCE_FILES = (
    "category_tree.csv",
    "events.csv",
    "item_properties_part1.csv",
    "item_properties_part2.csv",
)


@dataclass(frozen=True)
class CheckFinding:
    """单文件校验结果。"""

    file: str
    ok: bool
    message: str


def dataset_dir(root: Path) -> Path:
    return root / "data" / "raw" / "retailrocket"


def _expected_header(filename: str) -> tuple[str, ...] | None:
    table = infer_table_for_file(filename)
    return table_spec(table).raw_columns if table is not None else None


def check_source_files(root: Path) -> list[CheckFinding]:
    """文件系统闸：存在、非空、表头符合契约。"""
    findings: list[CheckFinding] = []
    for name in SOURCE_FILES:
        path = dataset_dir(root) / name
        if not path.is_file():
            findings.append(CheckFinding(name, False, f"file not found: {path}"))
            continue
        size = path.stat().st_size
        if size <= 0:
            findings.append(CheckFinding(name, False, f"file is empty ({path})"))
            continue
        try:
            header = tuple(read_csv_header(path))
        except ValueError as exc:
            findings.append(CheckFinding(name, False, f"header unreadable: {exc}"))
            continue
        expected = _expected_header(name)
        if expected is not None and header != expected:
            findings.append(
                CheckFinding(
                    name,
                    False,
                    f"header mismatch: got {header!r}, expected {expected!r}",
                )
            )
            continue
        findings.append(CheckFinding(name, True, f"ok size={size:,} header={','.join(header)}"))
    return findings


def validate_ods(cfg: DbConfig, root: Path) -> list[CheckFinding]:
    """DB + 磁盘契约闸：最新批次 LOADED、行数 > 0、sha256 与磁盘一致。"""
    findings: list[CheckFinding] = []
    conn = runner_connect(cfg)
    try:
        repo = BatchLogRepo(conn)
        for name in SOURCE_FILES:
            path = dataset_dir(root) / name
            if not path.is_file():
                findings.append(CheckFinding(name, False, f"file not found: {path}"))
                continue
            entries = repo.find_for_file(name)
            if not entries:
                findings.append(CheckFinding(name, False, "no etl_batch_log entry for this file"))
                continue
            newest = entries[0]
            if newest.status != "LOADED":
                findings.append(
                    CheckFinding(
                        name,
                        False,
                        f"newest batch #{newest.batch_id} status={newest.status} (not LOADED)",
                    )
                )
                continue
            if newest.row_count <= 0:
                findings.append(CheckFinding(name, False, f"batch #{newest.batch_id} row_count=0"))
                continue
            sha = sha256_file(path)
            if sha != newest.file_sha256:
                findings.append(
                    CheckFinding(
                        name,
                        False,
                        f"file changed since batch #{newest.batch_id}: sha256 "
                        f"{sha[:16]}... != logged {newest.file_sha256[:16]}... "
                        "(reload with `ingestion.cli load` first)",
                    )
                )
                continue
            findings.append(
                CheckFinding(
                    name,
                    True,
                    f"batch #{newest.batch_id} rows={newest.row_count:,} sha256 matches "
                    f"on disk ({sha[:16]}...)",
                )
            )
        return findings
    finally:
        conn.close()
