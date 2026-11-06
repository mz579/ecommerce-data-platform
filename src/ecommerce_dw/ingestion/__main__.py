"""Allow ``python -m ecommerce_dw.ingestion`` as an alias for the CLI."""

import sys

from ecommerce_dw.ingestion.cli import main

if __name__ == "__main__":
    sys.exit(main())
