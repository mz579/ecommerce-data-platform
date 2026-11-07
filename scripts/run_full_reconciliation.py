"""CLI wrapper: batched full-range replay + optional reconciliation (Phase 9.4).

Implementation lives in ``ecommerce_dw.streaming.batch_replay``. See ``--help``.

Typical acceptance run (from the repo root, project venv):

    python scripts/run_full_reconciliation.py \
        --evidence-dir C:/Zproject/ZClaude/streaming-9.4 \
        --reset-rt --batch-days 7 --lookahead-days 2 --rate 5000 \
        --high-rate 10000 --high-rate-batch 2 --reconcile

This script never writes ``ecommerce_dw``; only ``ecommerce_rt`` is modified.
"""

from __future__ import annotations

import sys

from ecommerce_dw.streaming.batch_replay import main

if __name__ == "__main__":
    sys.exit(main())
