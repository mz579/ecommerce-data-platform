"""内置示例数据：Retailrocket 风格的 events CSV，用于一键体验。"""

from __future__ import annotations

from pathlib import Path

SAMPLE_DIR = Path(__file__).resolve().parent / "sample"
SAMPLE_FILENAME = "events_sample.csv"
SAMPLE_COLUMNS = ("timestamp", "visitorid", "event", "itemid", "transactionid")


def sample_path() -> Path:
    return SAMPLE_DIR / SAMPLE_FILENAME
