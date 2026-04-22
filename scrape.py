#!/usr/bin/env python3
"""Scrape certified brands from Climate Active.

Uses curl_cffi to impersonate a real Chrome TLS/JA3 fingerprint, since the
site sits behind Akamai and rejects vanilla Python HTTP clients (and at
times headless browsers from cloud IP ranges).
"""

import csv
import datetime
import re
import sys
import time
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup
from curl_cffi import requests as cc_requests

BASE_URL = "https://www.climateactive.org.au"
LISTING_URL = f"{BASE_URL}/certified-brands"
OUTPUT_FILE = "companies.csv"

IMPERSONATE = "chrome"
REQUEST_TIMEOUT = 60
MAX_PAGES = 60
RETRY_DELAYS = (2, 4, 8, 16)

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

BRAND_PATH_RE = re.compile(r"^/buy-climate-active/certified-members/[^/?#]+/?$")


def _log(msg):
    print(msg, flush=True)


def _summarise_response(resp):
    """Return a one-line summary of a curl_cffi response for logging."""
    server = resp.headers.get("server", "?")
    ctype = resp.headers.get("content-type", "?")
    clen = resp.headers.get("content-length", str(len(resp.content)))
    return f"status={resp.status_code} server={server} type={ctype} bytes={clen}"


def _dump_failure(resp, context):
    """Print everything we know about a failed response."""
    _log(f"  FAIL [{context}] {_summarise_response(resp)}")
    _log(f"  url       : {resp.url}")
    if getattr(resp, "history", None):
        chain = " -> ".join(f"{r.status_code} {r.url}" for r in resp.history)
        _log(f"  redirects : {chain}")
    _log("  headers   :")
    for k, v in resp.headers.items():
        _log(f"    {k}: {v}")
    body = resp.text or ""
    snippet = body[:2000].replace("\n", " ")
    _log(f"  body[:2k] : {snippet}")


def fetch(session, url, *, context):
    """GET a URL with retries; log diagnostics on every failure."""
    last_exc = None
    for attempt in range(len(RETRY_DELAYS) + 1):
        try:
            resp = session.get(url, timeout=REQUEST_TIMEOUT, allow_redirects=True)
        except Exception as e:
            last_exc = e
            _log(f"  exception [{context}] attempt {attempt + 1}: {e!r}")
        else:
            if resp.status_code == 200:
                if attempt > 0:
                    _log(f"  recovered [{context}] after {attempt} retr{'y' if attempt == 1 else 'ies'}")
                return resp
            _dump_failure(resp, context)
            last_exc = RuntimeError(f"HTTP {resp.status_code} for {url}")

        if attempt < len(RETRY_DELAYS):
            delay = RETRY_DELAYS[attempt]
            _log(f"  retrying in {delay}s...")
            time.sleep(delay)

    raise RuntimeError(f"GET {url} failed after retries: {last_exc!r}")


def _normalise_href(href):
    if not href:
        return None
    parsed = urlparse(href)
    if parsed.netloc and parsed.netloc not in (urlparse(BASE_URL).netloc, ""):
        return None
    path = parsed.path
    if not BRAND_PATH_RE.match(path):
        return None
    return path.rstrip("/")


def _extract_brand_paths(soup):
    paths = set()
    for a in soup.select('a[href*="/buy-climate-active/certified-members/"]'):
        path = _normalise_href(a.get("href"))
        if path:
            paths.add(path)
    return paths


def _find_next_url(soup, current_url):
    """Find the next page URL using Drupal-style rel=next pager links."""
    link = soup.find("a", attrs={"rel": "next"})
    if not link:
        link = soup.select_one("li.pager__item--next a")
    if not link or not link.get("href"):
        return None
    return urljoin(current_url, link["href"])


def collect_brand_urls(session):
    """Walk the directory pager and collect every brand URL."""
    all_paths = set()
    url = LISTING_URL

    for page_num in range(1, MAX_PAGES + 1):
        _log(f"Fetching directory page {page_num}: {url}")
        resp = fetch(session, url, context=f"directory page {page_num}")
        soup = BeautifulSoup(resp.text, "html.parser")

        new_paths = _extract_brand_paths(soup)
        if not new_paths and page_num == 1:
            _log("  WARNING: no brand links on first page — selector may be stale.")
            _log(f"  body[:2k] : {resp.text[:2000]}")
        added = len(new_paths - all_paths)
        all_paths |= new_paths
        _log(f"  found {len(new_paths)} brand links on this page ({added} new, {len(all_paths)} total)")

        next_url = _find_next_url(soup, resp.url)
        if not next_url or next_url == url:
            break
        url = next_url

    return sorted(all_paths)


def _looks_like_domain(text):
    return bool(text) and "." in text and " " not in text


def _is_external(href):
    if not href:
        return False
    if "climateactive.org.au" in href:
        return False
    return not any(domain in href for domain in SOCIAL_MEDIA_DOMAINS)


def scrape_brand_page(session, brand_path):
    url = f"{BASE_URL}{brand_path}"
    resp = fetch(session, url, context=f"brand {brand_path}")
    soup = BeautifulSoup(resp.text, "html.parser")

    h1 = soup.find("h1")
    company_name = h1.get_text(strip=True) if h1 else ""

    company_website = ""
    candidates = [
        (a.get("href", ""), a.get_text(strip=True))
        for a in soup.select("a[href^='http']")
    ]

    for href, text in candidates:
        if _is_external(href) and _looks_like_domain(text):
            company_website = href
            break

    if not company_website:
        for href, _text in candidates:
            if _is_external(href):
                company_website = href
                break

    return company_name, company_website


def load_existing_dates(filepath):
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

    session = cc_requests.Session(impersonate=IMPERSONATE)
    # Prime cookies (some Akamai deployments hand out a session cookie on the
    # landing page that they then check on subsequent requests).
    _log(f"Priming session against {BASE_URL} (impersonate={IMPERSONATE})...")
    try:
        prime = session.get(BASE_URL, timeout=REQUEST_TIMEOUT, allow_redirects=True)
        _log(f"  prime: {_summarise_response(prime)}")
    except Exception as e:
        _log(f"  prime failed (continuing anyway): {e!r}")

    _log("Loading directory page...")
    brand_paths = collect_brand_urls(session)
    _log(f"Found {len(brand_paths)} certified brands")

    if not brand_paths:
        print("ERROR: No brands found on the directory page.", file=sys.stderr)
        sys.exit(1)

    companies = []
    for i, brand_path in enumerate(brand_paths, 1):
        slug = brand_path.rsplit("/", 1)[-1]
        _log(f"[{i}/{len(brand_paths)}] {slug}")
        try:
            name, website = scrape_brand_page(session, brand_path)
            if name:
                companies.append(
                    {
                        "date_added": today,
                        "company_name": name,
                        "company_website": website,
                    }
                )
            else:
                _log("  no <h1> found — skipping")
        except Exception as e:
            _log(f"  Error: {e}")

    companies.sort(key=lambda c: c["company_name"].lower())

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

    _log(f"\nSaved {len(companies)} companies to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
