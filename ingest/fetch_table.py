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
    """
    Scan page source and linked JS bundles for ArcGIS FeatureServer URLs.

    ArcGIS Experience Builder apps embed the service URL inside compiled JS
    chunks, not the initial HTML. We fetch the page, collect <script src>
    bundle URLs, then search each bundle for a FeatureServer pattern.
    """
    _FS_PATTERN = re.compile(r'https?://[^"\'<>\s]+?/FeatureServer(?:/\d+)?')

    def _first_match(text: str) -> str | None:
        m = _FS_PATTERN.search(text)
        if not m:
            return None
        url = m.group(0)
        if not url.endswith("/0"):
            url = url.rstrip("/") + "/0"
        return url

    try:
        headers = {"User-Agent": "Mozilla/5.0"}
        resp = requests.get(source_url, headers=headers, timeout=20)
        resp.raise_for_status()
        html = resp.text

        # Check initial HTML first
        found = _first_match(html)
        if found:
            return found

        # Parse <script src="..."> bundle URLs and scan each
        from bs4 import BeautifulSoup as _BS  # noqa: PLC0415
        soup = _BS(html, "html.parser")
        base = source_url.rstrip("/")
        checked = 0
        for tag in soup.find_all("script", src=True):
            src = tag["src"]
            if not src.startswith("http"):
                src = base + "/" + src.lstrip("/")
            try:
                js_resp = requests.get(src, headers=headers, timeout=15)
                js_resp.raise_for_status()
                found = _first_match(js_resp.text)
                if found:
                    return found
            except Exception:  # noqa: BLE001
                pass
            checked += 1
            if checked >= 20:  # don't scan every bundle
                break

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
# Strategy 2: Playwright async (Colab — requires libatk system libs from Cell 1)
# ---------------------------------------------------------------------------

async def _playwright_async(source_url: str) -> list[dict]:
    """Async Playwright coroutine — fallback when Selenium is unavailable."""
    from playwright.async_api import async_playwright  # noqa: PLC0415

    captured_json: list[dict] = []
    html = ""

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        async def _handle_response(response):
            if "FeatureServer" in response.url and "/query" in response.url:
                try:
                    body = await response.json()
                    if "features" in body:
                        captured_json.extend(body["features"])
                except Exception:  # noqa: BLE001
                    pass

        page.on("response", _handle_response)
        await page.goto(source_url, wait_until="networkidle", timeout=60_000)

        try:
            await page.wait_for_selector(
                "table, [role='grid'], [role='table']", timeout=15_000
            )
        except Exception:  # noqa: BLE001
            logger.debug("No table selector found; will parse full DOM")

        await page.wait_for_timeout(3000)

        if not captured_json:
            html = await page.content()

        await browser.close()

    if captured_json:
        return [_normalise_rest_record(f.get("attributes", {})) for f in captured_json]

    soup = BeautifulSoup(html, "html.parser")
    return _parse_html_table(soup)


def _fetch_via_playwright(source_url: str) -> list[dict] | None:
    """Playwright fallback — used when Selenium is not available."""
    try:
        from playwright.async_api import async_playwright  # noqa: F401, PLC0415
    except ImportError:
        logger.warning("Playwright not available; skipping")
        return None

    import asyncio  # noqa: PLC0415

    try:
        import nest_asyncio  # noqa: PLC0415
        nest_asyncio.apply()
    except ImportError:
        pass

    try:
        loop = asyncio.get_event_loop()
        records = loop.run_until_complete(_playwright_async(source_url))
        return records if records else None
    except Exception as exc:  # noqa: BLE001
        logger.warning("Playwright async fetch failed: %s", exc)
        return None


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
        logger.info("Strategy 1: found FeatureServer URL: %s", rest_url)
        records = _fetch_via_rest(rest_url)
        if records:
            logger.info("Strategy 1 (REST API): fetched %d records", len(records))
        else:
            logger.warning("Strategy 1: FeatureServer URL found but returned no records")
    else:
        logger.warning("Strategy 1: no FeatureServer URL found in page source")

    # --- Strategy 2: Playwright headless Chromium ---
    if not records:
        logger.info("Trying Strategy 2 (Playwright headless Chromium)")
        records = _fetch_via_playwright(source_url)
        if records:
            logger.info("Strategy 2 (Playwright): fetched %d records", len(records))
        else:
            logger.warning("Strategy 2 (Playwright): returned no records")

    # --- Strategy 3: Plain BS4 ---
    if not records:
        logger.info("Trying Strategy 3 (requests + BeautifulSoup)")
        records = _fetch_via_bs4(source_url)
        if records:
            logger.info("Strategy 3 (BS4): fetched %d records", len(records))
        else:
            logger.warning("Strategy 3 (BS4): returned no records")

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
