"""CLI wrapper: full-range RT-vs-ADS/DWD reconciliation (Phase 9.4).

Implementation lives in ``ecommerce_dw.streaming.reconcile_all`` so it can be
unit tested without a database. See ``--help`` for options.

Examples:

    python scripts/reconcile_all.py --dry-run
    python scripts/reconcile_all.py --write \
        --json-out C:/Zproject/ZClaude/streaming-9.4/reconciliation.json
"""

from __future__ import annotations

import sys

from ecommerce_dw.streaming.reconcile_all import main

if __name__ == "__main__":
    sys.exit(main())
