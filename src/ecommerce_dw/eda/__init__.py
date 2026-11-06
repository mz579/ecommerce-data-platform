"""EDA / data profiling helpers for the Retailrocket CSV dataset (Phase 1).

Everything here streams from local CSV files and never touches a database.
Public entry points:

- ``scan_csv`` — generic CSV scan (type inference, missing / distinct counts,
  full-row duplicates)
- ``summarize_events`` / ``daily_event_counts`` — ``events.csv`` statistics
- ``summarize_item_properties`` — combined stats over the two parts of
  ``item_properties_*.csv``
- ``summarize_category_tree`` — ``category_tree.csv`` structure stats
"""

from ecommerce_dw.eda._core import DigestStats
from ecommerce_dw.eda.category_tree import (
    CategoryTreeStats,
    category_depth_distribution,
    summarize_category_tree,
)
from ecommerce_dw.eda.events import DailyEvents, EventsStats, daily_event_counts, summarize_events
from ecommerce_dw.eda.item_properties import ItemPropertyStats, summarize_item_properties
from ecommerce_dw.eda.scan import ColumnProfile, CsvScanResult, scan_csv

__all__ = [
    "CategoryTreeStats",
    "ColumnProfile",
    "CsvScanResult",
    "DailyEvents",
    "DigestStats",
    "EventsStats",
    "ItemPropertyStats",
    "category_depth_distribution",
    "daily_event_counts",
    "scan_csv",
    "summarize_category_tree",
    "summarize_events",
    "summarize_item_properties",
]
