#!/usr/bin/env python3
"""Scrape certified brands from Climate Active using Playwright."""

import csv
import datetime
import re
import sys

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

BASE_URL = "https://www.climateactive.org.au"
LISTING_URL = f"{BASE_URL}/certified-brands"
OUTPUT_FILE = "companies.csv"

SOCIAL_MEDIA_DOMAINS = [
    "facebook.com",
    "twitter.com",
    "instagram.com",
    "linkedin.com",
    "youtube.com",
    "tiktok.com",
    "pinterest.com",
    "x.com",
    "threads.net",
]

BRAND_LINK_SELECTOR = 'a[href*="/buy-climate-active/certified-members/"]'
BRAND_PATH_RE = re.compile(r"^/buy-climate-active/certified-members/[^/?#]+/?$")


def _normalise_href(href):
    if not href:
        return None
    path = re.sub(r"[?#].*$", "", href)
    if path.startswith(BASE_URL):
        path = path[len(BASE_URL):]
    if not BRAND_PATH_RE.match(path):
        return None
    return path.rstrip("/")


def collect_brand_urls(page):
    """Load the directory and paginate through all pages to collect brand URLs."""
    page.goto(LISTING_URL, wait_until="domcontentloaded", timeout=60000)
    page.wait_for_selector(BRAND_LINK_SELECTOR, timeout=30000)

    all_paths = set()

    for _ in range(60):
        hrefs = page.eval_on_selector_all(
            BRAND_LINK_SELECTOR,
            'els => els.map(a => a.getAttribute("href"))',
        )
        for href in hrefs:
            path = _normalise_href(href)
            if path:
                all_paths.add(path)

        # Snapshot current brand links so we can detect when the page updates.
        old_links = page.evaluate(
            """() => Array.from(document.querySelectorAll(
                     'a[href*=\"/buy-climate-active/certified-members/\"]'))
                     .map(a => a.getAttribute('href')).join(',')"""
        )

        advanced = False

        # Prefer a visible "Load more" / "Show more" / "Next" button.
        for label in ("Load more", "Show more", "See more", "Next"):
            btn = page.query_selector(f'button:has-text("{label}")') \
                or page.query_selector(f'a:has-text("{label}")')
            if btn:
                try:
                    if btn.is_visible() and not btn.is_disabled():
                        btn.click()
                        advanced = True
                        break
                except Exception:
                    pass

        # Fall back to the Drupal-style rel="next" pager link.
        if not advanced:
            nxt = page.query_selector('a[rel="next"], li.pager__item--next a')
            if nxt:
                try:
                    nxt.click()
                    advanced = True
                except Exception:
                    pass

        if not advanced:
            break

        # Wait until the brand links change, indicating the new page loaded.
        try:
            page.wait_for_function(
                """(old) => Array.from(document.querySelectorAll(
                             'a[href*=\"/buy-climate-active/certified-members/\"]'))
                           .map(a => a.getAttribute('href')).join(',') !== old""",
                arg=old_links,
                timeout=10000,
            )
        except PlaywrightTimeoutError:
            break

        page.wait_for_timeout(500)

    return sorted(all_paths)


def scrape_brand_page(page, brand_path):
    """Visit a brand page and extract company name and website."""
    page.goto(f"{BASE_URL}{brand_path}", wait_until="domcontentloaded", timeout=30000)
    page.wait_for_selector("h1", timeout=15000)

    company_name = page.eval_on_selector("h1", "el => el.innerText.trim()")

    # The website link is an external <a> whose visible text looks like a domain
    # (e.g. "www.example.com.au"), excluding climateactive.org.au and social media.
    company_website = ""
    links = page.eval_on_selector_all(
        "a[href^='http']",
        "els => els.map(a => ({href: a.getAttribute('href'), text: a.innerText.trim()}))",
    )

    def is_external(href):
        if not href:
            return False
        if "climateactive.org.au" in href:
            return False
        return not any(domain in href for domain in SOCIAL_MEDIA_DOMAINS)

    for link in links:
        href = link["href"] or ""
        text = link["text"]
        if is_external(href) and text and "." in text and " " not in text:
            company_website = href
            break

    if not company_website:
        # Fallback: first external link regardless of visible text.
        for link in links:
            href = link["href"] or ""
            if is_external(href):
                company_website = href
                break

    return company_name, company_website


def load_existing_dates(filepath):
    """Load existing CSV to preserve original date_added values."""
    dates = {}
    try:
        with open(filepath, newline="") as f:
            for row in csv.DictReader(f):
                dates[row["company_name"]] = row["date_added"]
    except FileNotFoundError:
        pass
    return dates


def main():
    today = datetime.date.today().isoformat()

    with sync_playwright() as p:
        # Climate Active's CDN resets HTTP/2 connections coming from headless
        # Chromium (net::ERR_HTTP2_PROTOCOL_ERROR). Forcing HTTP/1.1 and using a
        # real-browser user agent keeps the session alive.
        browser = p.chromium.launch(
            headless=True,
            args=["--disable-http2"],
        )
        context = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
            ),
            locale="en-AU",
            viewport={"width": 1400, "height": 900},
        )
        page = context.new_page()

        print("Loading directory page...")
        brand_paths = collect_brand_urls(page)
        print(f"Found {len(brand_paths)} certified brands")

        if not brand_paths:
            print("ERROR: No brands found on the directory page.", file=sys.stderr)
            browser.close()
            sys.exit(1)

        companies = []
        for i, brand_path in enumerate(brand_paths, 1):
            slug = brand_path.rsplit("/", 1)[-1]
            print(f"[{i}/{len(brand_paths)}] {slug}")
            try:
                name, website = scrape_brand_page(page, brand_path)
                if name:
                    companies.append(
                        {
                            "date_added": today,
                            "company_name": name,
                            "company_website": website,
                        }
                    )
            except PlaywrightTimeoutError:
                print(f"  Timeout — skipping")
            except Exception as e:
                print(f"  Error: {e}")

        browser.close()

    companies.sort(key=lambda c: c["company_name"].lower())

    # Preserve the original date_added for companies already in the CSV so
    # the field reflects when the company was *first* recorded, not the last
    # time the scraper ran.
    existing_dates = load_existing_dates(OUTPUT_FILE)
    for company in companies:
        original = existing_dates.get(company["company_name"])
        if original:
            company["date_added"] = original

    with open(OUTPUT_FILE, "w", newline="") as f:
        writer = csv.DictWriter(
            f, fieldnames=["date_added", "company_name", "company_website"]
        )
        writer.writeheader()
        writer.writerows(companies)

    print(f"\nSaved {len(companies)} companies to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
