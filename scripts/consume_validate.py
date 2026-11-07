"""CLI wrapper: consume Kafka events, validate and route bad payloads to DLQ.

Implementation lives in ``ecommerce_dw.streaming.validate_consumer`` so it can
be unit tested without a broker. See ``--help`` for all options.

Example:

    python scripts/consume_validate.py --from-beginning --idle-timeout 15
"""

from __future__ import annotations

import sys

from ecommerce_dw.streaming.validate_consumer import main

if __name__ == "__main__":
    sys.exit(main())
