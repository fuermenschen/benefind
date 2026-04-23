"""Open a headed Playwright window for manual page inspection.

Flow:
1) Open google.com
2) Navigate to target URL
3) Keep browser open until user presses Enter
"""

from __future__ import annotations

import argparse

from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright

DEFAULT_TARGET_URL = "https://radelnohnealter.ch/winterthur/"


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Open Playwright debug browser and navigate")
    parser.add_argument(
        "--target",
        default=DEFAULT_TARGET_URL,
        help=f"Target URL to open after google.com (default: {DEFAULT_TARGET_URL})",
    )
    parser.add_argument(
        "--slow-mo-ms",
        type=int,
        default=250,
        help="Delay between Playwright actions in milliseconds (default: 250)",
    )
    parser.add_argument(
        "--timeout-ms",
        type=int,
        default=60_000,
        help="Navigation timeout in milliseconds (default: 60000)",
    )
    return parser


def main() -> None:
    args = _build_parser().parse_args()

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=False, slow_mo=max(0, args.slow_mo_ms))
        context = browser.new_context()
        page = context.new_page()

        print("Opening https://www.google.com ...")
        page.goto("https://www.google.com", wait_until="domcontentloaded", timeout=args.timeout_ms)

        print(f"Navigating to {args.target} ...")
        try:
            page.goto(args.target, wait_until="domcontentloaded", timeout=args.timeout_ms)
        except PlaywrightTimeoutError:
            print("Navigation timed out; page may still be loading. Check the window manually.")

        print("Browser left open for inspection. Press Enter in this terminal to close.")
        input()
        context.close()
        browser.close()


if __name__ == "__main__":
    main()
