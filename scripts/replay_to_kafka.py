"""CLI wrapper: replay ecommerce_dw.dwd_events into Kafka (Phase 9.2).

Implementation lives in ``ecommerce_dw.streaming.replay`` so it can be unit
tested without a broker. See ``--help`` for all options.

Example:

    python scripts/replay_to_kafka.py --from 2015-07-14 --rate 5000
"""

from __future__ import annotations

import sys

from ecommerce_dw.streaming.replay import main

if __name__ == "__main__":
    sys.exit(main())
