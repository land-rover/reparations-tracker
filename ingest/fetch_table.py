"""
Approach: ArcGIS REST API probe first, then Playwright headless browser fallback.

reparationsresources.com/table is built on ArcGIS Experience Builder, which typically
exposes a FeatureServer REST endpoint. We probe likely REST patterns first (fastest, most
reliable). If that fails, we fall back to Playwright (headless Chromium) to fully render
the JavaScript app and extract table rows from the DOM.

Entry point: run(settings)
"""

import json
import logging
import os
import re
import time
from pathlib import Path

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

# Known US state abbreviations for normalisation
STATE_ABBR = {
    "alabama": "AL", "alaska": "AK", "arizona": "AZ", "arkansas": "AR",
    "california": "CA", "colorado": "CO", "connecticut": "CT", "delaware": "DE",
    "florida": "FL", "georgia": "GA", "hawaii": "HI", "idaho": "ID",
    "illinois": "IL", "indiana": "IN", "iowa": "IA", "kansas": "KS",
    "kentucky": "KY", "louisiana": "LA", "maine": "ME", "maryland": "MD",
    "massachusetts": "MA", "michigan": "MI", "minnesota": "MN", "mississippi": "MS",
    "missouri": "MO", "montana": "MT", "nebraska": "NE", "nevada": "NV",
    "new hampshire": "NH", "new jersey": "NJ", "new mexico": "NM", "new york": "NY",
    "north carolina": "NC", "north dakota": "ND", "ohio": "OH", "oklahoma": "OK",
    "oregon": "OR", "pennsylvania": "PA", "rhode island": "RI", "south carolina": "SC",
    "south dakota": "SD", "tennessee": "TN", "texas": "TX", "utah": "UT",
    "vermont": "VT", "virginia": "VA", "washington": "WA", "west virginia": "WV",
    "wisconsin": "WI", "wyoming": "WY", "district of columbia": "DC",
}

VALID_ABBRS = set(STATE_ABBR.values())


def _normalise_state(raw: str) -> str:
    """Return 2-letter state abbreviation. Returns raw value if unrecognised."""
    if not raw:
        return ""
    s = raw.strip()
    if s.upper() in VALID_ABBRS:
        return s.upper()
    return STATE_ABBR.get(s.lower(), s.upper()[:2])


# ---------------------------------------------------------------------------
# Strategy 1: probe ArcGIS REST endpoints
# ---------------------------------------------------------------------------

# Candidate base URLs derived from common ArcGIS Experience Builder patterns
_REST_CANDIDATES = [
    "https://services.arcgis.com/",
    "https://reparationsresources.com/arcgis/rest/services/",
    "https://www.reparationsresources.com/arcgis/rest/services/",
]

_QUERY_PARAMS = {
    "where": "1=1",
    "outFields": "*",
    "resultOffset": 0,
    "resultRecordCount": 2000,
    "f": "json",
}


def _probe_page_for_rest_url(source_url: str) -> str | None:
    """Scan page source for ArcGIS FeatureServer URLs."""
    try:
        resp = requests.get(source_url, timeout=20)
        resp.raise_for_status()
        # Look for FeatureServer patterns embedded in JS bundles / HTML
        matches = re.findall(
            r'https?://[^"\']+?/FeatureServer(?:/\d+)?',
            resp.text,
        )
        if matches:
            url = matches[0]
            # Normalise: ensure we have /0 layer
            if not url.endswith("/0"):
                url = url.rstrip("/") + "/0"
            return url
    except Exception as exc:  # noqa: BLE001
        logger.debug("REST probe page scan failed: %s", exc)
    return None


def _fetch_via_rest(feature_server_layer_url: str) -> list[dict] | None:
    """Download all records from an ArcGIS FeatureServer layer."""
    records = []
    offset = 0
    batch = 2000
    while True:
        params = {**_QUERY_PARAMS, "resultOffset": offset, "resultRecordCount": batch}
        try:
            resp = requests.get(
                f"{feature_server_layer_url}/query", params=params, timeout=30
            )
            resp.raise_for_status()
            data = resp.json()
        except Exception as exc:  # noqa: BLE001
            logger.debug("REST query failed at offset %d: %s", offset, exc)
            return None

        if "error" in data:
            logger.debug("REST error response: %s", data["error"])
            return None

        features = data.get("features", [])
        if not features:
            break

        for f in features:
            attrs = f.get("attributes", {})
            records.append(attrs)

        if len(features) < batch:
            break
        offset += batch

    return records if records else None


def _normalise_rest_record(attrs: dict) -> dict:
    """Map raw ArcGIS field names to canonical schema."""
    # Try to find meaningful fields by inspecting keys case-insensitively
    def _get(*keys):
        for k in keys:
            for ak, av in attrs.items():
                if ak.lower() == k.lower():
                    return av or ""
        return ""

    return {
        "initiative_name": _get("initiative_name", "name", "title", "program_name"),
        "city": _get("city", "municipality", "locality"),
        "state": _normalise_state(_get("state", "state_name", "state_abbr")),
        "initiative_type": _get("initiative_type", "type", "category"),
        "status": _get("status", "program_status"),
        "year": _get("year", "year_initiated", "year_passed", "start_year"),
        "description": _get("description", "summary", "notes"),
        "source_url": _get("source_url", "url", "link", "website"),
        "_raw": attrs,
    }


# ---------------------------------------------------------------------------
# Strategy 2: Playwright headless browser
# ---------------------------------------------------------------------------

def _fetch_via_playwright(source_url: str) -> list[dict] | None:
    """Use Playwright + headless Chromium to render the page and extract the table."""
    try:
        from playwright.sync_api import sync_playwright  # noqa: PLC0415
    except ImportError:
        logger.warning("Playwright not available; skipping headless browser strategy")
        return None

    records = []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(source_url, wait_until="networkidle", timeout=60_000)

        # Try to intercept XHR/fetch calls to capture JSON data
        captured_json: list[dict] = []

        def _handle_response(response):
            if "FeatureServer" in response.url and "/query" in response.url:
                try:
                    body = response.json()
                    if "features" in body:
                        captured_json.extend(body["features"])
                except Exception:  # noqa: BLE001
                    pass

        page.on("response", _handle_response)

        # Wait up to 15 s for any table-like element
        try:
            page.wait_for_selector("table, [role='grid'], [role='table']", timeout=15_000)
        except Exception:  # noqa: BLE001
            logger.debug("No table selector found; will parse full DOM")

        # Give dynamic content extra time
        time.sleep(3)

        # If we captured FeatureServer JSON responses, use those
        if captured_json:
            browser.close()
            return [_normalise_rest_record(f.get("attributes", {})) for f in captured_json]

        html = page.content()
        browser.close()

    # Parse extracted HTML
    soup = BeautifulSoup(html, "html.parser")
    records = _parse_html_table(soup)
    return records if records else None


def _parse_html_table(soup: BeautifulSoup) -> list[dict]:
    """Extract records from the first meaningful HTML table."""
    records = []
    table = soup.find("table")
    if not table:
        return records

    headers = [th.get_text(strip=True).lower().replace(" ", "_") for th in table.find_all("th")]
    if not headers:
        # Try first row as header
        first_row = table.find("tr")
        if first_row:
            headers = [td.get_text(strip=True).lower().replace(" ", "_") for td in first_row.find_all("td")]

    for row in table.find_all("tr")[1:]:
        cells = row.find_all("td")
        if not cells:
            continue
        raw = {headers[i]: cells[i].get_text(strip=True) if i < len(headers) else ""
               for i in range(len(cells))}
        records.append({
            "initiative_name": raw.get("initiative_name", raw.get("name", raw.get("program", ""))),
            "city": raw.get("city", raw.get("municipality", "")),
            "state": _normalise_state(raw.get("state", "")),
            "initiative_type": raw.get("initiative_type", raw.get("type", raw.get("category", ""))),
            "status": raw.get("status", ""),
            "year": raw.get("year", raw.get("year_initiated", raw.get("year_passed", ""))),
            "description": raw.get("description", raw.get("summary", raw.get("notes", ""))),
            "source_url": raw.get("source_url", raw.get("url", raw.get("link", ""))),
            "_raw": raw,
        })
    return records


# ---------------------------------------------------------------------------
# Strategy 3: plain requests + BeautifulSoup
# ---------------------------------------------------------------------------

def _fetch_via_bs4(source_url: str) -> list[dict] | None:
    """Fetch page with requests and parse with BeautifulSoup."""
    try:
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            )
        }
        resp = requests.get(source_url, headers=headers, timeout=30)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        records = _parse_html_table(soup)
        return records if records else None
    except Exception as exc:  # noqa: BLE001
        logger.debug("BS4 fetch failed: %s", exc)
        return None


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def run(settings) -> list[dict]:
    """
    Fetch the initiatives table from SOURCE_URL and write to RAW_DATA_PATH.

    Tries three strategies in order:
      1. ArcGIS REST API (direct JSON, fastest)
      2. Playwright headless browser (full JS rendering)
      3. requests + BeautifulSoup (plain HTML)

    Returns the list of initiative records written.
    """
    source_url = settings.SOURCE_URL
    out_path = settings.RAW_DATA_PATH
    os.makedirs(os.path.dirname(out_path), exist_ok=True)

    logger.info("Fetching initiatives table from %s", source_url)

    # --- Strategy 1: Probe for ArcGIS REST endpoint in page source ---
    records = None
    rest_url = _probe_page_for_rest_url(source_url)
    if rest_url:
        logger.info("Found FeatureServer URL in page source: %s", rest_url)
        records = _fetch_via_rest(rest_url)
        if records:
            logger.info("Strategy 1 (REST API): fetched %d records", len(records))

    # --- Strategy 2: Playwright ---
    if not records:
        logger.info("Strategy 1 failed or returned no records; trying Playwright")
        records = _fetch_via_playwright(source_url)
        if records:
            logger.info("Strategy 2 (Playwright): fetched %d records", len(records))

    # --- Strategy 3: Plain BS4 ---
    if not records:
        logger.info("Strategy 2 failed; trying plain requests + BeautifulSoup")
        records = _fetch_via_bs4(source_url)
        if records:
            logger.info("Strategy 3 (BS4): fetched %d records", len(records))

    if not records:
        raise RuntimeError(
            f"All three fetch strategies failed for {source_url}. "
            "Check network connectivity and that the source site is reachable."
        )

    # Strip internal _raw field from final output (keep for debugging during run)
    output = [{k: v for k, v in r.items() if k != "_raw"} for r in records]

    with open(out_path, "w", encoding="utf-8") as fh:
        json.dump(output, fh, indent=2, ensure_ascii=False)

    logger.info("Wrote %d initiative records to %s", len(output), out_path)
    return output


if __name__ == "__main__":
    import config.settings as settings

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    run(settings)
