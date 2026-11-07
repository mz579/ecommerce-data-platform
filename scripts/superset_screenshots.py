"""Phase 7 — real-render dashboard screenshots via headless Chrome (Playwright).

Drives the logged-in Superset UI and captures full-page PNGs into
``dashboards/screenshots/``. Uses the locally installed Chrome
(channel="chrome"), so no browser download is needed.

Phase 7 R16: this script reuses the HARD-ASSERTION helpers from
``superset_ui_evidence`` (module-level ``_FAILURES`` + ``wait_capture`` with
bounded polling), so the baseline captures here are subject to the same rule
as the interactive evidence: any chart panel that stays ``Waiting on`` /
``No results`` / under a stuck spinner at the bounded deadline is a hard
failure and the script exits 1.

Baseline set captured here: ops default, user default, product *before*
publish.  The filter/refresh *after* states are captured by
``superset_ui_evidence.py`` between and after its interactions.
"""

from __future__ import annotations

import sys
from pathlib import Path

from dotenv import load_dotenv
from playwright.sync_api import sync_playwright

from superset_ui_evidence import (
    _FAILURES,
    assert_markdown_panels,
    login,
    open_dashboard,
    wait_capture,
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SHOTS = PROJECT_ROOT / "dashboards" / "screenshots"


def main() -> int:
    load_dotenv(PROJECT_ROOT / ".env")
    SHOTS.mkdir(parents=True, exist_ok=True)

    with sync_playwright() as p:
        browser = p.chromium.launch(channel="chrome", headless=True)
        # widen viewport so charts have room; full_page captures the rest
        ctx = browser.new_context(viewport={"width": 1680, "height": 3000})
        page = ctx.new_page()
        login(page)

        # ---- ops dashboard: before-filter baseline ----
        open_dashboard(page, "ops")
        wait_capture(page, "ops default")
        assert_markdown_panels(page, "ops default markdown")
        page.screenshot(path=SHOTS / "ops_overview_default.png", full_page=True)
        print("saved ops_overview_default.png")

        # ---- user dashboard ----
        open_dashboard(page, "user")
        wait_capture(page, "user default")
        assert_markdown_panels(page, "user default markdown")
        page.screenshot(path=SHOTS / "user_behavior_default.png", full_page=True)
        print("saved user_behavior_default.png")

        # ---- product dashboard (before publish) ----
        open_dashboard(page, "product")
        wait_capture(page, "product before publish")
        assert_markdown_panels(page, "product markdown")
        page.screenshot(path=SHOTS / "product_analysis_before_publish.png", full_page=True)
        print("saved product_analysis_before_publish.png")

        browser.close()

    print()
    if _FAILURES:
        print(f"SCREENSHOTS FAILED: {len(_FAILURES)} hard-assertion failure(s)")
        for f in _FAILURES:
            print(f"  - {f}")
        return 1
    print("SCREENSHOTS OK: every chart rendered values (no 'Waiting on' / 'No results')")
    return 0


if __name__ == "__main__":
    sys.exit(main())
