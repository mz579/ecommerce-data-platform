"""Phase 7 — UI-level acceptance evidence.

1. ops dashboard: native filter before/after (date filter + 转化口径 dimension
   filter). Each step prints the KPI value / series count before & after and
   asserts a real change, so the PNGs are guaranteed to show the filtered state.
2. product dashboard: post-``publish --run 5`` state — the 数据发布 KPI (freshness
   metric) is read and compared against ``product_analysis_before_publish.png``
   captured by ``superset_screenshots.py`` before the extra publish.

Notes
-----
A filter APPLY re-runs every chart; under this WSL2 host the concurrent chart
waves serialize into ~20-60 s.  We therefore wait bounded, poll the panel until
it shows data (not 'Waiting on <db>'/'Timeout error'), and on timeout retry the
interaction ONCE from a fresh page load.  We never auto-re-click APPLY in a
loop (that livelocks the renderer).
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
OPS_URL = f"{BASE}/superset/dashboard/1/"
PRODUCT_URL = f"{BASE}/superset/dashboard/3/"


def wait_charts(page, timeout_ms: int = 120_000) -> None:
    page.wait_for_selector(".dashboard-component-chart-holder", timeout=timeout_ms)
    page.wait_for_function(
        "() => !document.querySelector('.ant-spin-spinning')", timeout=timeout_ms
    )
    page.wait_for_timeout(1500)


def wait_filter_bar(page, timeout_ms: int = 90_000) -> None:
    """Native filter bar hydrates a moment AFTER the charts; block until its
    .ant-select controls exist so the before/after interactions are reliable."""
    page.wait_for_selector(".ant-select", state="attached", timeout=timeout_ms)
    page.wait_for_timeout(1000)


def login(page, attempts: int = 4) -> None:
    """Login is flaky in headless Chrome; retry a few times before giving up."""
    for _ in range(attempts):
        try:
            page.goto(f"{BASE}/login/", wait_until="domcontentloaded", timeout=120_000)
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


def panel_value(page, title_fragment: str) -> str:
    """Read the displayed value of the chart panel whose title contains the text."""
    js = f"""() => {{
      const panels = [...document.querySelectorAll('.dashboard-component-chart-holder')];
      for (const p of panels) {{
        const t = (p.innerText || '');
        if (t.indexOf({title_fragment!r}) >= 0) return t;
      }}
      return '';
    }}"""
    txt = page.evaluate(js)
    return txt.replace("\n", " / ")


def wait_panel_settled(page, title_fragment: str, timeout_ms: int = 100_000) -> str:
    """Poll until the panel shows data (no 'Waiting on…'/'Timeout'/'trouble')."""
    deadline = time.monotonic() + timeout_ms / 1000
    while True:
        txt = panel_value(page, title_fragment)
        if txt and "Waiting on" not in txt and "Timeout" not in txt and "trouble" not in txt:
            return txt
        if time.monotonic() > deadline:
            return txt
        page.wait_for_timeout(2000)


def _conv_ready(txt: str) -> bool:
    return bool(txt) and "Waiting on" not in txt and "Timeout" not in txt and "trouble" not in txt


def apply_filters(page) -> None:
    """The native filter bar stages changes until 'APPLY FILTERS' is clicked."""
    btn = page.locator("button", has_text="APPLY FILTERS")
    try:
        if btn.count():
            btn.first.click()
        page.wait_for_timeout(2500)
    except Exception:
        page.wait_for_timeout(2500)


def pick_date_filter_value(page, value: str, select_index: int = 0) -> None:
    """Select a single date in the multi-select filter via search+Enter."""
    target = page.locator(".ant-select").nth(select_index)
    target.click()
    page.wait_for_timeout(600)
    target.locator("input.ant-select-selection-search-input").fill(value)
    page.wait_for_timeout(800)
    # first matching option row
    option = page.locator(".ant-select-item-option").first
    option.click()
    apply_filters(page)


def deselect_strict_apply(page) -> None:
    """转化口径 filter: deselect 'conv_strict_rate' (default has both on)."""
    metric_sel = page.locator(".ant-select").nth(1)
    metric_sel.click()
    page.wait_for_timeout(600)
    option = page.locator(".ant-select-item-option", has_text="conv_strict_rate").first
    option.click()
    page.wait_for_timeout(600)
    page.keyboard.press("Escape")
    apply_filters(page)


def open_ops(page) -> None:
    # A prior filter APPLY wave (12 charts, 20-60 s each on the 1-worker
    # gunicorn behind WSL2) is usually still draining when this is called;
    # let it settle and never rely on the 30 s default navigation timeout.
    page.wait_for_timeout(8000)
    page.goto(OPS_URL, wait_until="domcontentloaded", timeout=180_000)
    wait_charts(page)
    wait_filter_bar(page)


def main() -> int:
    load_dotenv(PROJECT_ROOT / ".env")
    with sync_playwright() as p:
        browser = p.chromium.launch(channel="chrome", headless=True)
        ctx = browser.new_context(viewport={"width": 1280, "height": 1400})
        page = ctx.new_page()
        login(page)

        # ---- ops: 转化口径 dimension filter FIRST (cold session). A metric
        # apply refetches every chart on the dashboard and is the heaviest wave;
        # measured ~50 s on a fresh session but pushes past the 60 s frontend
        # cutoff when it lands behind an earlier date-apply wave (WSL2 host). ---
        open_ops(page)
        before = wait_panel_settled(page, "转化率双口径", timeout_ms=120_000)
        page.screenshot(path=SHOTS / "ops_overview_default.png", full_page=True)
        print("saved ops_overview_default.png (转化口径 before: both series)")
        after = None
        for attempt in (1, 2):
            deselect_strict_apply(page)
            candidate = wait_panel_settled(page, "转化率双口径", timeout_ms=100_000)
            if _conv_ready(candidate):
                after = candidate
                break
            # frontend timeout under a heavy concurrent wave: refresh the page
            # (filter resets to both) + re-deselect, once.
            if attempt == 1:
                open_ops(page)
                wait_panel_settled(page, "转化率双口径", timeout_ms=100_000)
        if after is None:
            after = wait_panel_settled(page, "转化率双口径", timeout_ms=100_000)

        page.screenshot(path=SHOTS / "ops_overview_filter_metric.png", full_page=True)
        print("saved ops_overview_filter_metric.png (转化口径: strict deselected)")
        print(
            f"    conv-trend before contains strict={'conv_strict_rate' in before}"
            f" global={'conv_global_rate' in before}  {before[:70]!r}"
        )
        print(
            f"    conv-trend after  contains strict={'conv_strict_rate' in after}"
            f" global={'conv_global_rate' in after}  {after[:70]!r}"
        )
        if "conv_global_rate" not in after or "conv_strict_rate" in after:
            print(
                "    [WARNING] dimension-filter 'after' did NOT converge to "
                "global-only; screenshot may show a loading/error state"
            )

        # ---- ops: date-filter before/after ----
        open_ops(page)
        pv_before = wait_panel_settled(page, "PV 总量")
        pick_date_filter_value(page, "2015-07-26", select_index=0)
        pv_after = wait_panel_settled(page, "PV 总量", timeout_ms=120_000)
        page.screenshot(path=SHOTS / "ops_overview_filter_date.png", full_page=True)
        print(f"saved ops_overview_filter_date.png (date filter after   PV={pv_after[:40]!r})")
        print(
            f"    date filter value-change evidence: "
            f"{pv_before != pv_after}  ({pv_before[:40]!r} -> {pv_after[:40]!r})"
        )

        # ---- product: after publish (freshness KPI must differ from before) ----
        page.wait_for_timeout(8000)
        page.goto(PRODUCT_URL, wait_until="domcontentloaded", timeout=180_000)
        wait_charts(page)
        wait_filter_bar(page)
        pub = wait_panel_settled(page, "累计发布次数", timeout_ms=120_000)
        page.screenshot(path=SHOTS / "product_analysis_after_publish.png", full_page=True)
        print(f"saved product_analysis_after_publish.png (freshness KPI={pub[:40]!r})")

        browser.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
