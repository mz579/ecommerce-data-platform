"""Table specifications for the ODS raw tables.

Every spec maps a target table to the exact ordered list of raw CSV columns
(honoring the physical header order of the source files). The loader builds the
INSERT statement and the header validation from these specs only, so adding a
new ODS table is a one-line change here.
"""

from __future__ import annotations

from dataclasses import dataclass

_ODS_TABLES = (
    "ods_events_raw",
    "ods_item_properties_raw",
    "ods_category_tree_raw",
)


@dataclass(frozen=True)
class TableSpec:
    """Target ODS table and its raw CSV columns, in physical header order."""

    table_name: str
    raw_columns: tuple[str, ...]


TABLE_SPECS: dict[str, TableSpec] = {
    "ods_events_raw": TableSpec(
        "ods_events_raw",
        ("timestamp", "visitorid", "event", "itemid", "transactionid"),
    ),
    "ods_item_properties_raw": TableSpec(
        "ods_item_properties_raw",
        ("timestamp", "itemid", "property", "value"),
    ),
    "ods_category_tree_raw": TableSpec(
        "ods_category_tree_raw",
        ("categoryid", "parentid"),
    ),
}

# Filename -> table hint used to default the CLI --table argument.
_FILE_TABLE_HINTS = {
    "events.csv": "ods_events_raw",
    "item_properties_part1.csv": "ods_item_properties_raw",
    "item_properties_part2.csv": "ods_item_properties_raw",
    "category_tree.csv": "ods_category_tree_raw",
}


def table_spec(table_name: str) -> TableSpec:
    try:
        return TABLE_SPECS[table_name]
    except KeyError as exc:
        raise ValueError(f"unknown ODS table {table_name!r}; choose one of {_ODS_TABLES}") from exc


def infer_table_for_file(filename: str) -> str | None:
    """Return the ODS table hinted by a source filename, if any."""
    return _FILE_TABLE_HINTS.get(filename)


def ods_table_names() -> tuple[str, ...]:
    return _ODS_TABLES
