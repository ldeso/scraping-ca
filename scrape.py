#!/usr/bin/env python3
"""Scrape the Climate Active certified-brands directory into a CSV.

The script loads https://www.climateactive.org.au/certified-brands, collects
every certified-member link exposed on that page (scrolling and clicking any
"Load more" control the site may render), then visits each brand's detail page
to extract the organisation's external website.

Results are merged with an existing CSV (if one exists) so that ``date_added``
is preserved for previously seen companies and set to today's date for any
newcomers. The CSV has three columns: date_added, company_name, company_website.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import re
import sys
from pathlib import Path
from urllib.parse import urljoin, urlparse

from playwright.sync_api import TimeoutError as PWTimeout
from playwright.sync_api import sync_playwright

BASE_URL = "https://www.climateactive.org.au"
LISTING_URL = f"{BASE_URL}/certified-brands"
MEMBER_PATH_RE = re.compile(r"^/buy-climate-active/certified-members/[^/]+/?$")
USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)
SOCIAL_DOMAINS = (
    "facebook.com", "twitter.com", "x.com", "linkedin.com", "instagram.com",
    "youtube.com", "youtu.be", "tiktok.com", "pinterest.com", "vimeo.com",
)


def collect_member_links(page) -> list[tuple[str, str]]:
    """Return a list of ``(detail_url, company_name)`` pairs from the listing."""
    selector = "a[href*='/buy-climate-active/certified-members/']"

    prev_count = -1
    for _ in range(60):
        count = page.locator(selector).count()
        if count == prev_count:
            break
        prev_count = count

        page.evaluate("window.scrollBy(0, document.body.scrollHeight)")
        page.wait_for_timeout(600)

        for label in ("Load more", "Show more", "See more", "View more"):
            button = page.get_by_role("button", name=re.compile(label, re.I))
            if button.count() and button.first.is_visible():
                try:
                    button.first.click()
                    page.wait_for_timeout(800)
                except Exception:
                    pass

    seen: dict[str, str] = {}
    for handle in page.locator(selector).element_handles():
        href = handle.get_attribute("href") or ""
        path = urlparse(href).path
        if not MEMBER_PATH_RE.match(path):
            continue
        detail_url = urljoin(BASE_URL, path)
        name = (handle.inner_text() or "").strip()
        if not name:
            name = (handle.get_attribute("aria-label") or "").strip()
        if not name:
            name = path.rstrip("/").rsplit("/", 1)[-1].replace("-", " ").title()
        # Prefer the longer rendered text (name > logo alt-text) when the same
        # detail URL appears more than once on the listing.
        if len(name) > len(seen.get(detail_url, "")):
            seen[detail_url] = name

    return sorted(seen.items(), key=lambda kv: kv[1].lower())


def looks_external(href: str) -> bool:
    if not href.startswith(("http://", "https://")):
        return False
    host = urlparse(href).netloc.lower().removeprefix("www.")
    if not host or host.endswith("climateactive.org.au"):
        return False
    return not any(host == d or host.endswith("." + d) for d in SOCIAL_DOMAINS)


def extract_company_website(page, detail_url: str) -> str:
    """Return the first external URL that looks like the organisation's site."""
    page.goto(detail_url, wait_until="domcontentloaded", timeout=60_000)
    try:
        page.wait_for_load_state("networkidle", timeout=15_000)
    except PWTimeout:
        pass

    labelled = page.locator(
        "xpath=//*[self::dt or self::strong or self::span or self::p or self::h3]"
        "[contains(translate(normalize-space(.), "
        "'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'website')]"
        "/following::a[starts-with(@href,'http')][1]"
    )
    if labelled.count():
        href = labelled.first.get_attribute("href") or ""
        if looks_external(href):
            return href

    for handle in page.locator(
        "main a[href^='http'], article a[href^='http'], "
        ".field a[href^='http'], .content a[href^='http']"
    ).element_handles():
        href = handle.get_attribute("href") or ""
        if looks_external(href):
            return href
    return ""


def load_existing(csv_path: Path) -> dict[str, dict[str, str]]:
    if not csv_path.exists():
        return {}
    with csv_path.open(newline="", encoding="utf-8") as f:
        return {row["company_name"]: row for row in csv.DictReader(f)}


def write_csv(csv_path: Path, rows: list[dict[str, str]]) -> None:
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    with csv_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f, fieldnames=["date_added", "company_name", "company_website"]
        )
        writer.writeheader()
        writer.writerows(sorted(rows, key=lambda r: r["company_name"].lower()))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output", default="certified_brands.csv",
        help="CSV file to read/write (default: certified_brands.csv).",
    )
    parser.add_argument(
        "--skip-details", action="store_true",
        help="Skip detail-page visits; company_website will be left blank.",
    )
    args = parser.parse_args()

    csv_path = Path(args.output)
    today = dt.date.today().isoformat()
    existing = load_existing(csv_path)

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent=USER_AGENT,
            locale="en-AU",
            viewport={"width": 1400, "height": 2000},
        )
        page = context.new_page()

        page.goto(LISTING_URL, wait_until="domcontentloaded", timeout=90_000)
        try:
            page.wait_for_load_state("networkidle", timeout=30_000)
        except PWTimeout:
            pass

        members = collect_member_links(page)
        if not members:
            print("No certified members found on the listing page.", file=sys.stderr)
            browser.close()
            return 1
        print(f"Found {len(members)} certified brands.", file=sys.stderr)

        merged = {name: dict(row) for name, row in existing.items()}
        for detail_url, name in members:
            row = merged.get(name)
            if row and row.get("company_website"):
                continue

            website = ""
            if not args.skip_details:
                try:
                    website = extract_company_website(page, detail_url)
                except Exception as exc:
                    print(f"WARN: {name} ({detail_url}): {exc}", file=sys.stderr)

            if row:
                if website:
                    row["company_website"] = website
            else:
                merged[name] = {
                    "date_added": today,
                    "company_name": name,
                    "company_website": website,
                }

        browser.close()

    write_csv(csv_path, list(merged.values()))
    print(f"Wrote {len(merged)} rows to {csv_path}.", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
