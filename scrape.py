#!/usr/bin/env python3
"""Scrape certified brands from Climate Active.

Uses curl_cffi to impersonate a real Chrome TLS/JA3 fingerprint, since the
site sits behind Akamai and rejects vanilla Python HTTP clients (and at
times headless browsers from cloud IP ranges).
"""

import csv
import datetime
import html
import json
import sys
import time
from urllib.parse import urlparse

from bs4 import BeautifulSoup
from curl_cffi import requests as cc_requests

BASE_URL = "https://www.climateactive.org.au"
LISTING_URL = f"{BASE_URL}/certified-brands"
OUTPUT_FILE = "companies.csv"

IMPERSONATE = "chrome"
REQUEST_TIMEOUT = 60
RETRY_DELAYS = (2, 4, 8, 16)
# Polite gap between brand-page requests. The CDN will silently start
# rejecting HTTP/2 streams on a connection once it sees too many rapid
# requests; this slows us down enough to stay under that threshold.
BRAND_REQUEST_DELAY = 0.5

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


def _new_session():
    """Create a fresh curl_cffi session primed against the landing page."""
    session = cc_requests.Session(impersonate=IMPERSONATE)
    _log(f"Priming session against {BASE_URL} (impersonate={IMPERSONATE})...")
    try:
        prime = session.get(BASE_URL, timeout=REQUEST_TIMEOUT, allow_redirects=True)
        _log(f"  prime: {_summarise_response(prime)}")
    except Exception as e:
        _log(f"  prime failed (continuing anyway): {e!r}")
    return session


class SessionHolder:
    """Owns the curl_cffi session so `fetch` can swap it on connection failure.

    Once the CDN returns a stream-level HTTP/2 error (curl code 92, INTERNAL_ERROR)
    every later request on the same connection fails the same way — retries
    against the live session can't recover. Replacing the session forces a new
    TLS handshake and a fresh HTTP/2 connection.
    """

    def __init__(self):
        self.session = _new_session()

    def reset(self):
        try:
            self.session.close()
        except Exception:
            pass
        self.session = _new_session()


def fetch(holder, url, *, context):
    """GET a URL with retries; log diagnostics on every failure."""
    last_exc = None
    for attempt in range(len(RETRY_DELAYS) + 1):
        try:
            resp = holder.session.get(
                url, timeout=REQUEST_TIMEOUT, allow_redirects=True
            )
        except Exception as e:
            last_exc = e
            _log(f"  exception [{context}] attempt {attempt + 1}: {e!r}")
            # Connection-level failure (e.g. curl 92 HTTP/2 INTERNAL_ERROR) —
            # the HTTP/2 connection is poisoned; rebuild before retrying.
            holder.reset()
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


def collect_brand_urls(holder):
    """Fetch the directory page and extract brand paths from its JSON payload.

    The listing page renders its cards client-side from a JSON blob on
    `div.certified-brands-list[data-model]`; the server HTML has no
    per-brand anchors, so we parse that JSON directly.
    """
    _log(f"Fetching directory page: {LISTING_URL}")
    resp = fetch(holder, LISTING_URL, context="directory page")
    soup = BeautifulSoup(resp.text, "html.parser")

    container = soup.select_one("div.certified-brands-list[data-model]")
    if not container:
        raise RuntimeError(
            "certified-brands-list[data-model] not found on directory page"
        )

    data = json.loads(html.unescape(container["data-model"]))
    brands = data.get("certifiedBrands", [])

    paths = set()
    for brand in brands:
        url = (brand.get("link") or {}).get("url") or ""
        path = urlparse(url).path.rstrip("/")
        if path:
            paths.add(path)

    _log(f"  parsed {len(brands)} brand entries, {len(paths)} unique paths")
    return sorted(paths)


def _looks_like_domain(text):
    return bool(text) and "." in text and " " not in text


def _is_external(href):
    if not href:
        return False
    if "climateactive.org.au" in href:
        return False
    return not any(domain in href for domain in SOCIAL_MEDIA_DOMAINS)


def scrape_brand_page(holder, brand_path):
    url = f"{BASE_URL}{brand_path}"
    resp = fetch(holder, url, context=f"brand {brand_path}")
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

    holder = SessionHolder()

    _log("Loading directory page...")
    brand_paths = collect_brand_urls(holder)
    _log(f"Found {len(brand_paths)} certified brands")

    if not brand_paths:
        print("ERROR: No brands found on the directory page.", file=sys.stderr)
        sys.exit(1)

    companies = []
    for i, brand_path in enumerate(brand_paths, 1):
        if i > 1:
            time.sleep(BRAND_REQUEST_DELAY)
        slug = brand_path.rsplit("/", 1)[-1]
        _log(f"[{i}/{len(brand_paths)}] {slug}")
        try:
            name, website = scrape_brand_page(holder, brand_path)
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
