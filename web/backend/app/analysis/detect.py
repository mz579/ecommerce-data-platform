"""文件探测：编码 / 分隔符 / 表头 / 采样 / 字段语义评分。

探测只读前 200 行与文件行数，不做全量解析；真正的解析在 ingest 阶段。
输出结构直接作为 ``preview`` 返回给前端，因此所有值都必须是 JSON 安全的。
"""

from __future__ import annotations

import csv
import io
import logging
import re
import shutil
import zipfile
from dataclasses import dataclass
from dataclasses import field as dc_field
from datetime import date, datetime
from pathlib import Path, PurePosixPath
from typing import IO, Any

from openpyxl import load_workbook

from .normalize import is_known_event, parse_timestamp

logger = logging.getLogger(__name__)

SAMPLE_ROWS = 200
PREVIEW_ROWS = 20
SAMPLE_BYTES = 256 * 1024
MAX_ZIP_MEMBERS = 50
MAX_ZIP_UNCOMPRESSED = 500 * 1024 * 1024
MAX_CELL_CHARS = 120

REQUIRED_FIELDS = ("user_id", "item_id", "event_type", "event_time")
OPTIONAL_FIELDS = ("transaction_id", "category_id", "amount", "product_name")
ALL_FIELDS = REQUIRED_FIELDS + OPTIONAL_FIELDS

FIELD_ALIASES: dict[str, tuple[str, ...]] = {
    "user_id": (
        "userid",
        "visitorid",
        "visitor",
        "uid",
        "customerid",
        "customer",
        "user",
        "用户id",
        "访客id",
        "用户",
        "客户id",
    ),
    "item_id": (
        "itemid",
        "productid",
        "product",
        "skuid",
        "sku",
        "item",
        "商品id",
        "产品id",
        "商品",
    ),
    "event_type": (
        "event",
        "eventtype",
        "eventname",
        "action",
        "behavior",
        "事件",
        "行为",
        "事件类型",
    ),
    "event_time": (
        "timestamp",
        "time",
        "eventtime",
        "datetime",
        "date",
        "createdat",
        "时间",
        "事件时间",
        "日期",
    ),
    "transaction_id": (
        "transactionid",
        "transaction",
        "orderid",
        "order",
        "订单号",
        "交易号",
        "订单",
    ),
    "category_id": ("categoryid", "category", "catid", "类目", "类目id", "品类"),
    "amount": ("amount", "price", "revenue", "sales", "金额", "价格", "销售额"),
    "product_name": (
        "productname",
        "itemname",
        "title",
        "name",
        "商品名",
        "商品名称",
        "名称",
        "标题",
    ),
}

DELIMITER_FALLBACK = {".csv": ",", ".tsv": "\t", ".txt": ","}


class DetectionError(ValueError):
    """文件无法识别（空文件、损坏、ZIP 不安全等），接口层映射 422。"""


@dataclass
class TableSample:
    kind: str  # "csv" | "xlsx"
    encoding: str
    delimiter: str
    has_header: bool
    header: list[str]
    data_rows: list[list[str]]
    rows_estimated: int
    container: str  # "file" | "zip"
    members: list[str] = dc_field(default_factory=list)
    selected_member: str | None = None
    sheet: str | None = None
    extracted_name: str | None = None


def inspect_file(path: Path, filename: str, work_dir: Path) -> dict[str, Any]:
    """探测上传文件并返回 preview。``work_dir`` 用于解压 ZIP 内的 Excel。"""
    work_dir.mkdir(parents=True, exist_ok=True)
    suffix = Path(filename).suffix.lower()
    if suffix == ".zip":
        sample = _inspect_zip(path, work_dir)
    elif suffix in {".xlsx", ".xlsm"}:
        sample = _inspect_xlsx(path, kind="xlsx", container="file")
    else:
        sample = _inspect_delimited(path, filename, container="file")

    if not sample.header and not sample.data_rows:
        raise DetectionError("文件中没有可读取的表格数据")

    width = max(
        [len(sample.header)] + [len(row) for row in sample.data_rows] or [0],
    )
    width = max(width, 1)
    header = _dedupe_names(sample.header, width)
    rows = [_pad_row(row, width) for row in sample.data_rows[:SAMPLE_ROWS]]

    columns = []
    for index, name in enumerate(header):
        values = [row[index] for row in rows if index < len(row)]
        columns.append(
            {
                "name": name,
                "dtype": _infer_dtype(values),
                "sample": [_short(value) for value in values if value.strip()][:3],
            }
        )

    detected = detect_mapping(header, rows)
    event_column = detected.get("event_type", {}).get("column")
    event_values: list[str] = []
    if event_column and event_column in header:
        index = header.index(event_column)
        seen: set[str] = set()
        for row in rows:
            value = row[index].strip()
            if value and value not in seen:
                seen.add(value)
                event_values.append(_short(value))
            if len(event_values) >= 20:
                break

    return {
        "kind": sample.kind,
        "container": sample.container,
        "encoding": sample.encoding,
        "delimiter": sample.delimiter,
        "has_header": sample.has_header,
        "sheet": sample.sheet,
        "members": sample.members,
        "selected_member": sample.selected_member,
        "extracted_name": sample.extracted_name,
        "rows_estimated": sample.rows_estimated,
        "columns": columns,
        "sample_rows": [[_short(cell) for cell in row] for row in rows[:PREVIEW_ROWS]],
        "detected": detected,
        "event_values": event_values,
    }


# ---------------------------------------------------------------------------
# 文件读取
# ---------------------------------------------------------------------------


def _detect_encoding(sample: bytes) -> str:
    # 末尾可能是被截断的多字节字符，先去掉最后 4 字节再试。
    probe = sample[:-4] if len(sample) > 8 else sample
    for encoding in ("utf-8-sig", "gb18030"):
        try:
            probe.decode(encoding)
            return encoding
        except UnicodeDecodeError:
            continue
    return "latin-1"


def _detect_delimiter(sample: bytes, encoding: str, filename: str) -> str:
    text = sample.decode(encoding, errors="replace")
    lines = [line for line in text.splitlines() if line.strip()][:20]
    if lines:
        try:
            dialect = csv.Sniffer().sniff("\n".join(lines), delimiters=",\t;|")
            return dialect.delimiter
        except csv.Error:
            pass
    return DELIMITER_FALLBACK.get(Path(filename).suffix.lower(), ",")


def _count_newlines(stream: IO[bytes]) -> int:
    count = 0
    last = b"\n"
    while True:
        chunk = stream.read(1024 * 1024)
        if not chunk:
            break
        count += chunk.count(b"\n")
        last = chunk[-1:]
    if last != b"\n":
        count += 1
    return count


def _read_text_sample(raw: IO[bytes], encoding: str, delimiter: str) -> list[list[str]]:
    text = io.TextIOWrapper(raw, encoding=encoding, errors="replace", newline="")
    try:
        reader = csv.reader(text, delimiter=delimiter)
        return [list(row) for row in _take(reader, SAMPLE_ROWS + 1)]
    finally:
        # TextIOWrapper 会关闭底层流；这里只需要释放引用。
        text.detach()


def _take(iterator, count: int) -> list[Any]:
    result: list[Any] = []
    for item in iterator:
        result.append(item)
        if len(result) >= count:
            break
    return result


def _inspect_delimited(path: Path, filename: str, *, container: str) -> TableSample:
    with path.open("rb") as handle:
        sample_bytes = handle.read(SAMPLE_BYTES)
    if not sample_bytes:
        raise DetectionError("文件为空")
    encoding = _detect_encoding(sample_bytes)
    delimiter = _detect_delimiter(sample_bytes, encoding, filename)

    with path.open("rb") as handle:
        rows = _read_text_sample(handle, encoding, delimiter)
    if not rows:
        raise DetectionError("文件没有可读取的行")

    has_header = _looks_like_header(rows[0])
    header = rows[0] if has_header else []
    data_rows = rows[1:] if has_header else rows
    with path.open("rb") as handle:
        line_count = _count_newlines(handle)
    rows_estimated = max(0, line_count - (1 if has_header else 0))
    if rows_estimated == 0:
        rows_estimated = len(data_rows)

    return TableSample(
        kind="csv",
        encoding=encoding,
        delimiter=delimiter,
        has_header=has_header,
        header=header,
        data_rows=data_rows,
        rows_estimated=rows_estimated,
        container=container,
    )


def _inspect_xlsx(
    path: Path,
    *,
    kind: str,
    container: str,
    members: list[str] | None = None,
    selected_member: str | None = None,
    extracted_name: str | None = None,
) -> TableSample:
    try:
        workbook = load_workbook(filename=path, read_only=True, data_only=True)
    except Exception as exc:  # openpyxl 对损坏文件抛多种异常
        raise DetectionError("Excel 文件无法读取或已损坏") from exc
    try:
        sheet_names = list(workbook.sheetnames)
        worksheet = None
        for name in sheet_names:
            candidate = workbook[name]
            if candidate.max_row and candidate.max_row > 1 and candidate.max_column >= 2:
                worksheet = candidate
                break
        if worksheet is None and sheet_names:
            worksheet = workbook[sheet_names[0]]
        if worksheet is None:
            raise DetectionError("Excel 没有工作表")

        raw_rows: list[list[str]] = []
        for row in worksheet.iter_rows(values_only=True):
            raw_rows.append([_cell_text(value) for value in row])
            if len(raw_rows) >= SAMPLE_ROWS + 1:
                break
        if not raw_rows:
            raise DetectionError("工作表为空")

        has_header = _looks_like_header(raw_rows[0])
        header = raw_rows[0] if has_header else []
        data_rows = raw_rows[1:] if has_header else raw_rows
        rows_estimated = max(0, int(worksheet.max_row or 0) - (1 if has_header else 0))
        if rows_estimated == 0:
            rows_estimated = len(data_rows)
        return TableSample(
            kind=kind,
            encoding="xlsx",
            delimiter="",
            has_header=has_header,
            header=header,
            data_rows=data_rows,
            rows_estimated=rows_estimated,
            container=container,
            members=members or sheet_names,
            selected_member=selected_member or worksheet.title,
            sheet=worksheet.title,
            extracted_name=extracted_name,
        )
    finally:
        workbook.close()


def _inspect_zip(path: Path, work_dir: Path) -> TableSample:
    try:
        archive = zipfile.ZipFile(path)
    except zipfile.BadZipFile as exc:
        raise DetectionError("ZIP 文件无法读取或已损坏") from exc

    with archive:
        infos = [info for info in archive.infolist() if not info.is_dir()]
        if len(infos) > MAX_ZIP_MEMBERS:
            raise DetectionError(f"ZIP 内文件数超过 {MAX_ZIP_MEMBERS} 个")
        total_size = 0
        candidates: list[zipfile.ZipInfo] = []
        for info in infos:
            name = info.filename
            pure = PurePosixPath(name.replace("\\", "/"))
            if name.startswith("/") or ".." in pure.parts or pure.is_absolute():
                raise DetectionError("ZIP 包含不安全的路径")
            if info.flag_bits & 0x1:
                raise DetectionError("不支持加密 ZIP")
            total_size += int(info.file_size or 0)
            if total_size > MAX_ZIP_UNCOMPRESSED:
                raise DetectionError("ZIP 解压后体积超过 500MB")
            if pure.name.startswith(".") or "__MACOSX" in pure.parts:
                continue
            if pure.suffix.lower() in {".csv", ".tsv", ".txt", ".xlsx", ".xlsm"}:
                candidates.append(info)

        if not candidates:
            raise DetectionError("ZIP 内没有 CSV / TSV / Excel 文件")
        selected = max(candidates, key=lambda info: int(info.file_size or 0))
        members = [info.filename for info in infos]
        member_name = selected.filename
        suffix = PurePosixPath(member_name).suffix.lower()

        if suffix in {".xlsx", ".xlsm"}:
            extracted = work_dir / "_source.xlsx"
            with archive.open(selected) as source, extracted.open("wb") as target:
                shutil.copyfileobj(source, target, length=1024 * 1024)
            sample = _inspect_xlsx(
                extracted,
                kind="xlsx",
                container="zip",
                members=members,
                selected_member=member_name,
                extracted_name="_source.xlsx",
            )
        else:
            with archive.open(selected) as raw:
                sample_bytes = raw.read(SAMPLE_BYTES)
            if not sample_bytes:
                raise DetectionError("ZIP 内的表格文件为空")
            encoding = _detect_encoding(sample_bytes)
            delimiter = _detect_delimiter(sample_bytes, encoding, PurePosixPath(member_name).name)
            with archive.open(selected) as raw:
                rows = _read_text_sample(raw, encoding, delimiter)
            if not rows:
                raise DetectionError("ZIP 内的表格文件没有可读取的行")
            has_header = _looks_like_header(rows[0])
            header = rows[0] if has_header else []
            data_rows = rows[1:] if has_header else rows
            with archive.open(selected) as raw:
                line_count = _count_newlines(raw)
            sample = TableSample(
                kind="csv",
                encoding=encoding,
                delimiter=delimiter,
                has_header=has_header,
                header=header,
                data_rows=data_rows,
                rows_estimated=max(0, line_count - (1 if has_header else 0)),
                container="zip",
                members=members,
                selected_member=member_name,
            )
        return sample


# ---------------------------------------------------------------------------
# 表头 / 列 / 语义
# ---------------------------------------------------------------------------


def _cell_text(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, datetime):
        return value.isoformat(sep=" ", timespec="seconds")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return str(value)


def _looks_like_header(row: list[str]) -> bool:
    cells = [str(cell).strip() for cell in row if str(cell).strip()]
    if len(cells) < 2:
        return False
    if len(set(cells)) != len(cells):
        return False
    data_like = sum(
        1 for cell in cells if _NUMERIC_RE.fullmatch(cell) or parse_timestamp(cell) is not None
    )
    return data_like < max(1, len(cells) / 2)


_NUMERIC_RE = re.compile(r"^[+-]?(\d+\.?\d*|\.\d+)$")


def _dedupe_names(header: list[str], width: int) -> list[str]:
    names: list[str] = []
    seen: dict[str, int] = {}
    for index in range(width):
        raw = header[index] if index < len(header) else ""
        base = str(raw).strip().lstrip("﻿") or f"col_{index + 1}"
        base = re.sub(r"\s+", " ", base)[:80]
        count = seen.get(base, 0) + 1
        seen[base] = count
        names.append(base if count == 1 else f"{base}_{count}")
    return names


def _pad_row(row: list[str], width: int) -> list[str]:
    values = [str(cell) for cell in row]
    return values + [""] * (width - len(values))


def _short(value: object) -> str:
    text = str(value).replace("\r", " ").replace("\n", " ").strip()
    return text[:MAX_CELL_CHARS]


def _infer_dtype(values: list[str]) -> str:
    clean = [str(value).strip() for value in values if str(value).strip()]
    if not clean:
        return "empty"
    if all(re.fullmatch(r"[+-]?\d+", value) for value in clean):
        return "int"
    if all(_NUMERIC_RE.fullmatch(value.replace(",", "")) for value in clean):
        return "float"
    parsed = sum(1 for value in clean if parse_timestamp(value) is not None)
    if parsed >= max(1, int(len(clean) * 0.8)):
        return "datetime"
    return "string"


# ---------------------------------------------------------------------------
# 字段映射
# ---------------------------------------------------------------------------


def _normalize_name(name: str) -> str:
    return re.sub(r"[\s_\-./]+", "", name.strip().lower())


def _name_score(name: str, aliases: tuple[str, ...]) -> float:
    normalized = _normalize_name(name)
    if not normalized:
        return 0.0
    best = 0.0
    for alias in aliases:
        candidate = _normalize_name(alias)
        if not candidate:
            continue
        if normalized == candidate:
            best = max(best, 1.0)
        elif len(candidate) >= 3 and (candidate in normalized or normalized in candidate):
            best = max(best, 0.82)
    return best


def _value_score(field: str, values: list[str]) -> tuple[float, str]:
    clean = [value.strip() for value in values if value is not None and str(value).strip()]
    if not clean:
        return 0.0, ""
    distinct = {value for value in clean}
    distinct_ratio = len(distinct) / len(clean)
    nonempty_ratio = len(clean) / max(1, len(values))

    if field == "event_type":
        known = sum(1 for value in clean if is_known_event(value))
        ratio = known / len(clean)
        if len(distinct) <= 20 and ratio >= 0.5:
            return min(1.0, 0.5 + ratio * 0.5), "值域匹配事件词表"
        return ratio * 0.4, ""

    if field == "event_time":
        parsed = sum(1 for value in clean if parse_timestamp(value) is not None)
        ratio = parsed / len(clean)
        if ratio >= 0.8:
            return 1.0, "样本可解析为时间"
        if ratio >= 0.5:
            return 0.6, "部分样本可解析为时间"
        return 0.0, ""

    if field in {"user_id", "item_id"}:
        if distinct_ratio >= 0.5 and nonempty_ratio >= 0.8:
            return 0.9, "高基数、非空"
        if distinct_ratio >= 0.2:
            return 0.45, "中等基数"
        return 0.0, ""

    if field == "transaction_id":
        if distinct_ratio >= 0.5:
            return 0.85, "高基数标识"
        if distinct_ratio >= 0.2:
            return 0.4, "中等基数"
        return 0.0, ""

    if field == "category_id":
        if 1 < len(distinct) <= 100:
            return 0.8, "低基数类别"
        return 0.0, ""

    if field == "amount":
        numeric = sum(1 for value in clean if _NUMERIC_RE.fullmatch(value.replace(",", "")))
        ratio = numeric / len(clean)
        if ratio >= 0.9:
            has_decimal = any("." in value for value in clean)
            return (0.9 if has_decimal else 0.55), "数值列"
        return 0.0, ""

    if field == "product_name":
        long_values = sum(1 for value in clean if len(value) >= 4)
        ratio = long_values / len(clean)
        if ratio >= 0.7 and distinct_ratio >= 0.3:
            return 0.75, "文本名称列"
        return 0.0, ""

    return 0.0, ""


def _score_field(field: str, column: str, values: list[str]) -> tuple[float, str]:
    name_score = _name_score(column, FIELD_ALIASES[field])
    value_score, value_reason = _value_score(field, values)
    if name_score > 0:
        score = min(1.0, name_score * 0.75 + value_score * 0.35)
    else:
        # 没有列名证据时最高 0.35，落在“待人工确认”阈值以下，避免把时间列误判成 user_id。
        score = value_score * 0.35
    reason_parts = []
    if name_score >= 0.95:
        reason_parts.append("列名精确命中")
    elif name_score > 0:
        reason_parts.append("列名接近")
    if value_reason:
        reason_parts.append(value_reason)
    return score, "；".join(reason_parts) or "未识别"


def detect_mapping(columns: list[str], rows: list[list[str]]) -> dict[str, dict[str, Any]]:
    """返回每个字段的最佳列、置信度与原因；未识别时 column=None。"""
    profiles: list[list[str]] = []
    for index in range(len(columns)):
        profiles.append([row[index] if index < len(row) else "" for row in rows])

    candidates: dict[str, list[tuple[float, int, str]]] = {}
    for field in ALL_FIELDS:
        scored: list[tuple[float, int, str]] = []
        for index, column in enumerate(columns):
            score, reason = _score_field(field, column, profiles[index])
            scored.append((score, index, reason))
        scored.sort(key=lambda item: item[0], reverse=True)
        candidates[field] = scored

    assigned: dict[str, dict[str, Any]] = {}
    used_columns: set[int] = set()

    # 必填字段优先，避免可选的 product_name 抢走 item_id 的列。
    for field_group in (REQUIRED_FIELDS, OPTIONAL_FIELDS):
        pool: list[tuple[float, int, str, str]] = []
        for field in field_group:
            for score, index, reason in candidates[field]:
                pool.append((score, index, reason, field))
        pool.sort(key=lambda item: item[0], reverse=True)
        for score, index, reason, field in pool:
            if field in assigned or index in used_columns or score < 0.4:
                continue
            assigned[field] = {
                "column": columns[index],
                "confidence": round(min(1.0, score), 2),
                "reason": reason,
            }
            used_columns.add(index)

    for field in ALL_FIELDS:
        assigned.setdefault(field, {"column": None, "confidence": 0.0, "reason": "未识别"})
    return assigned
