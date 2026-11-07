"""Phase 7 — real-render dashboard screenshots via headless Chrome (Playwright).

Drives the logged-in Superset UI, waits for every chart to finish rendering
(no .ant-spin-spinning), then captures full-page PNGs into
``dashboards/screenshots/``. Uses the locally installed Chrome
(channel="chrome"), so no browser download is needed.

Baseline set captured here: ops default, user default, product *before*
publish.  The filter/refresh *after* states are captured by
``superset_ui_evidence.py`` between and after its interactions.
"""

from __future__ import annotations

import os
import sys
import time
from pathlib import Path

from dotenv import load_dotenv
from playwright.sync_api import sync_playwright

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SHOTS = PROJECT_ROOT / "dashboards" / "screenshots"

BASE = "http://localhost:8088"
LOGIN_URL = f"{BASE}/login/"
DASH_URL = {
    "ops": f"{BASE}/superset/dashboard/1/",
    "user": f"{BASE}/superset/dashboard/2/",
    "product": f"{BASE}/superset/dashboard/3/",
}


def wait_charts(page, timeout_ms: int = 120_000) -> None:
    page.wait_for_selector(".dashboard-component-chart-holder", timeout=timeout_ms)
    page.wait_for_function(
        "() => !document.querySelector('.ant-spin-spinning')", timeout=timeout_ms
    )
    # ECharts needs a beat to paint after the spinner disappears
    page.wait_for_timeout(2500)


def login(page, attempts: int = 4) -> None:
    """Login is flaky in headless Chrome; retry a few times before giving up."""
    for _ in range(attempts):
        try:
            page.goto(LOGIN_URL, wait_until="domcontentloaded", timeout=120_000)
            page.fill('input[name="username"]', os.environ["SUPERSET_ADMIN_USERNAME"])
            page.fill('input[name="password"]', os.environ["SUPERSET_ADMIN_PASSWORD"])
            page.click('input[type="submit"]')
            page.wait_for_load_state("networkidle")
            page.wait_for_selector("text=Dashboards", timeout=45_000)
            return
        except Exception:
            if _ == attempts - 1:
                raise
            time.sleep(2)


def open_dashboard(page, url: str) -> None:
    """Navigate to a dashboard; retry once if the heavy page stalls on load."""
    for attempt in (1, 2):
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=120_000)
            return
        except Exception:
            if attempt == 2:
                raise
            page.wait_for_timeout(2000)


def main() -> int:
    load_dotenv(PROJECT_ROOT / ".env")
    SHOTS.mkdir(parents=True, exist_ok=True)

    with sync_playwright() as p:
        browser = p.chromium.launch(channel="chrome", headless=True)
        # widen viewport so charts have room; full_page captures the rest
        ctx = browser.new_context(viewport={"width": 1680, "height": 3000})
        page = ctx.new_page()
        login(page)

        # ---- ops dashboard: before-filter baseline + filtered after ----
        open_dashboard(page, DASH_URL["ops"])
        wait_charts(page)
        page.screenshot(path=SHOTS / "ops_overview_default.png", full_page=True)
        print("saved ops_overview_default.png")

        page.wait_for_timeout(200)

        # ---- user dashboard ----
        open_dashboard(page, DASH_URL["user"])
        wait_charts(page)
        page.screenshot(path=SHOTS / "user_behavior_default.png", full_page=True)
        print("saved user_behavior_default.png")

        # ---- product dashboard (before publish) ----
        open_dashboard(page, DASH_URL["product"])
        wait_charts(page)
        page.screenshot(path=SHOTS / "product_analysis_before_publish.png", full_page=True)
        print("saved product_analysis_before_publish.png")

        browser.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
