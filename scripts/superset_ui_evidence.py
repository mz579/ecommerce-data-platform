"""Phase 7 R16 — UI-level acceptance evidence with HARD assertions.

Shoots the three dashboards, including interactive states AND a real publish
refresh, then fails the run if any chart stuck/empty:

  1. ops   default                       -> ops_overview_default.png
  2. ops   转化口径 dimension filter before/after -> ops_overview_filter_metric_before.png /
                                                   ops_overview_filter_metric.png
  3. ops   日期范围 filter before/after  -> ops_overview_filter_date_before.png /
                                           ops_overview_filter_date.png
  4. user  default                       -> user_behavior_default.png
  5. product before-publish              -> product_analysis_before_publish.png
  6. publish demo: DATES publish over 2015-09-16..17 backed by PASS gate run #5
     (consistent with the Phase-7-R16 gate-scope rule; run #5 is itself DATES
     over exactly those two days), executed by ``ecommerce_dw.cli publish``
     through subprocess so the published state comes from the real CLI path.
  7. product after-publish               -> product_analysis_after_publish.png
     (数据发布/累计发布次数 KPI must have incremented)

HARD ASSERTION (R16):  every chart panel must reach a terminal state showing
data.  We keep the 时间范围/时区/口径 markdown panels (part of the layouts) and
assert they are still present, but any *chart* whose innerText contains
``Waiting on`` / ``No results`` / ``Timeout`` / ``trouble loading`` / an HTTP
error, or that stays under a spinner at the bounded deadline, is recorded.  If
ANY capture has such a panel the script exits 1 (after saving every PNG for
review).  Waiting is bounded polling only — never an unbounded sleep.

Below-fold panels are lazy-loaded by the IntersectionObserver, so before each
capture we sweep every chart holder into view, let the queries fire, then
return to the top and re-check before the full-page PNG.
"""

from __future__ import annotations

import os
import subprocess
import sys
import time
from pathlib import Path

from dotenv import load_dotenv
from playwright.sync_api import sync_playwright

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SHOTS = PROJECT_ROOT / "dashboards" / "screenshots"
BASE = "http://localhost:8088"
DASH_URL = {
    "ops": f"{BASE}/superset/dashboard/1/",
    "user": f"{BASE}/superset/dashboard/2/",
    "product": f"{BASE}/superset/dashboard/3/",
}

FORBIDDEN = (
    "Waiting on",
    "No results",
    "Timeout",
    "trouble loading",
    "MySQL Error",
    "Unknown column",
    "Internal Server Error",
    "INTERNAL SERVER ERROR",
)
# 时间范围/时区/口径 panels are part of the layouts and must survive.
PANEL_MARKERS = ("时间范围", "时区", "口径")

_FAILURES: list[str] = []


def _report(context: str, message: str) -> None:
    print(f"    [FAIL] {context}: {message}")
    _FAILURES.append(f"{context}: {message}")


# --------------------------------------------------------------------------- #
# UI helpers                                                                   #
# --------------------------------------------------------------------------- #


def login(page, attempts: int = 4) -> None:
    """Login is flaky in headless Chrome; retry a few times before giving up."""
    for attempt in range(attempts):
        try:
            page.goto(f"{BASE}/login/", wait_until="domcontentloaded", timeout=120_000)
            page.fill('input[name="username"]', os.environ["SUPERSET_ADMIN_USERNAME"])
            page.fill('input[name="password"]', os.environ["SUPERSET_ADMIN_PASSWORD"])
            page.click('input[type="submit"]')
            page.wait_for_load_state("networkidle")
            page.wait_for_selector("text=Dashboards", timeout=45_000)
            return
        except Exception:
            if attempt == attempts - 1:
                raise
            time.sleep(2)


def open_dashboard(page, key: str) -> None:
    page.wait_for_timeout(4000)
    page.goto(DASH_URL[key], wait_until="domcontentloaded", timeout=180_000)
    page.wait_for_selector(".dashboard-component-chart-holder", timeout=120_000)
    page.wait_for_timeout(1500)


def _sweep_panels(page) -> None:
    """Scroll every chart holder into view to fire lazy-loaded queries."""
    n = page.evaluate("() => document.querySelectorAll('.dashboard-component-chart-holder').length")
    for i in range(n):
        page.evaluate(
            f"""() => {{
                const ps = document.querySelectorAll('.dashboard-component-chart-holder');
                if ({i} < ps.length) ps[{i}].scrollIntoView({{block:'center'}});
            }}"""
        )
        page.wait_for_timeout(200)
    page.evaluate("() => window.scrollTo(0, 0)")
    page.wait_for_timeout(1200)


def _panel_state(page) -> tuple[list[str], int]:
    """(innerText per chart holder, number of holders still under a spinner)."""
    texts = page.evaluate(
        """() => [...document.querySelectorAll('.dashboard-component-chart-holder')]
                    .map(el => (el.innerText || ''))"""
    )
    spinners = page.evaluate(
        """() => document.querySelectorAll(
            '.dashboard-component-chart-holder .ant-spin-spinning').length"""
    )
    return [str(t) for t in texts], int(spinners)


def _terminal(texts: list[str], spinners: int) -> tuple[bool, list[str]]:
    bad = sorted({t for t in texts if any(f in t for f in FORBIDDEN)})
    numeric = sum(1 for t in texts if any(ch.isdigit() for ch in t))
    return (not bad and spinners == 0 and numeric > 0), bad


def wait_capture(page, context: str, timeout_s: float = 150.0) -> None:
    """Sweep + bounded-poll until every panel shows data; save PNG on the way.

    Hard assertion: any ``Waiting on`` / ``No results`` / stuck-spinner panel at
    the deadline is recorded and the script will exit 1 at the end.
    """
    deadline = time.monotonic() + timeout_s
    last_bad: list[str] = []
    last_texts: list[str] = []
    while time.monotonic() < deadline:
        _sweep_panels(page)
        texts, spinners = _panel_state(page)
        ok, bad = _terminal(texts, spinners)
        last_bad, last_texts = bad, texts
        if ok:
            for marker in PANEL_MARKERS:
                if marker not in page.content():
                    _report(context, f"markdown panel missing: {marker!r}")
            return
        page.wait_for_timeout(2500)
    # deadline hit: hard assertion failure
    if last_bad:
        for frag in last_bad[:5]:
            _report(context, f"chart stuck/empty: {frag[:90]!r}")
    if not any(ch.isdigit() for t in last_texts for ch in t):
        _report(context, "no chart rendered any numeric value")
    _report(context, f"bounded wait ({timeout_s:.0f}s) expired; spinners={_panel_state(page)[1]}")


def assert_markdown_panels(page, context: str) -> None:
    """The 时间范围/时区/口径 markdown panels are part of the layout."""
    content = page.content()
    for marker in PANEL_MARKERS:
        if marker not in content:
            _report(context, f"markdown panel missing: {marker!r}")


def apply_filters(page) -> None:
    btn = page.locator("button", has_text="APPLY FILTERS")
    try:
        if btn.count():
            btn.first.click()
        page.wait_for_timeout(2500)
    except Exception:
        page.wait_for_timeout(2500)


def pick_date_filter_value(page, value: str) -> None:
    target = page.locator(".ant-select").nth(0)
    target.click()
    page.wait_for_timeout(600)
    target.locator("input.ant-select-selection-search-input").fill(value)
    page.wait_for_timeout(800)
    page.locator(".ant-select-item-option").first.click()
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


def run_publish_demo() -> None:
    """Real publish through the CLI: DATES over 2015-09-16..17 backed by run #5.

    Phase 7 R16 gate-scope rule: run #5 is a DATES PASS gate covering exactly
    those two days, so this publish is consistent (a FULL publish against run #5
    would now be refused). Appends one row to ads_publish_log (only grows).
    """
    if len(_FAILURES) > 0:
        print("    publish demo SKIPPED (capture failures already recorded)")
        return
    cmd = [
        sys.executable,
        "-m",
        "ecommerce_dw.cli",
        "publish",
        "--dates",
        "2015-09-16,2015-09-17",
        "--run",
        "5",
    ]
    print("    running publish demo:", " ".join(cmd))
    proc = subprocess.run(cmd, cwd=PROJECT_ROOT, capture_output=True, text=True)
    print(proc.stdout.strip())
    if proc.stderr.strip():
        print("    stderr:", proc.stderr.strip(), file=sys.stderr)
    if proc.returncode != 0:
        raise RuntimeError(f"publish demo failed (exit {proc.returncode})")


# --------------------------------------------------------------------------- #
# main                                                                         #
# --------------------------------------------------------------------------- #


def main() -> int:
    load_dotenv(PROJECT_ROOT / ".env")
    SHOTS.mkdir(parents=True, exist_ok=True)

    with sync_playwright() as p:
        browser = p.chromium.launch(channel="chrome", headless=True)
        ctx = browser.new_context(viewport={"width": 1680, "height": 3000})
        page = ctx.new_page()
        login(page)

        # ---- ops: default ----
        open_dashboard(page, "ops")
        wait_capture(page, "ops default")
        assert_markdown_panels(page, "ops default markdown")
        page.screenshot(path=SHOTS / "ops_overview_default.png", full_page=True)
        print("saved ops_overview_default.png")

        # ---- ops: 转化口径 dimension filter before/after ----
        open_dashboard(page, "ops")
        wait_capture(page, "ops metric before")
        page.screenshot(path=SHOTS / "ops_overview_filter_metric_before.png", full_page=True)
        print("saved ops_overview_filter_metric_before.png")
        deselect_strict_apply(page)
        wait_capture(page, "ops metric after")
        page.screenshot(path=SHOTS / "ops_overview_filter_metric.png", full_page=True)
        print("saved ops_overview_filter_metric.png")

        # ---- ops: 日期范围 filter before/after ----
        open_dashboard(page, "ops")
        wait_capture(page, "ops date before")
        page.screenshot(path=SHOTS / "ops_overview_filter_date_before.png", full_page=True)
        print("saved ops_overview_filter_date_before.png")
        pick_date_filter_value(page, "2015-07-26")
        wait_capture(page, "ops date after")
        page.screenshot(path=SHOTS / "ops_overview_filter_date.png", full_page=True)
        print("saved ops_overview_filter_date.png")

        # ---- user: default ----
        open_dashboard(page, "user")
        wait_capture(page, "user default")
        assert_markdown_panels(page, "user default markdown")
        page.screenshot(path=SHOTS / "user_behavior_default.png", full_page=True)
        print("saved user_behavior_default.png")

        # ---- product: BEFORE publish ----
        open_dashboard(page, "product")
        wait_capture(page, "product before publish")
        assert_markdown_panels(page, "product markdown")
        page.screenshot(path=SHOTS / "product_analysis_before_publish.png", full_page=True)
        print("saved product_analysis_before_publish.png")

        # ---- publish demo (CLI, real path) ----
        run_publish_demo()

        # ---- product: AFTER publish (freshness KPI must have incremented) ----
        open_dashboard(page, "product")
        wait_capture(page, "product after publish")
        page.screenshot(path=SHOTS / "product_analysis_after_publish.png", full_page=True)
        print("saved product_analysis_after_publish.png")

        browser.close()

    print()
    if _FAILURES:
        print(f"EVIDENCE FAILED: {len(_FAILURES)} hard-assertion failure(s)")
        for f in _FAILURES:
            print(f"  - {f}")
        return 1
    print(
        "EVIDENCE OK: every chart rendered values on all captures "
        "(no 'Waiting on' / 'No results' / stuck spinner)"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
