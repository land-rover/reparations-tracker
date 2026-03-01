"""
Step 2: Enrich initiatives with geocoordinates, congressional representatives,
and H.R. 40 co-sponsorship data.

Pipeline:
  2a. Load city boundaries from TIGER/Line Place shapefiles (cached on Drive).
      Intersect city polygons with 119th Congress district polygons (also cached).
      Place a point at each city×district intersection centroid.
  2b. Resolve House representative and both Senators via Congress.gov API,
      falling back to Google Civic Information API on failure.
  2c. Look up H.R. 40 co-sponsorship for each legislator.
  2d. Write initiatives_enriched.geojson.
  2e. Build and write legislators_summary.json.
  2f. Derive delegation_alignment per state.

Entry point: run(settings)
"""

import json
import logging
import os
import re
import time
from datetime import datetime, timezone
from pathlib import Path

import geopandas as gpd
import pandas as pd
import requests
from shapely.geometry import Point, mapping

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# State FIPS helpers
# ---------------------------------------------------------------------------

STATE_FIPS = {
    "AL": "01", "AK": "02", "AZ": "04", "AR": "05", "CA": "06", "CO": "08",
    "CT": "09", "DE": "10", "DC": "11", "FL": "12", "GA": "13", "HI": "15",
    "ID": "16", "IL": "17", "IN": "18", "IA": "19", "KS": "20", "KY": "21",
    "LA": "22", "ME": "23", "MD": "24", "MA": "25", "MI": "26", "MN": "27",
    "MS": "28", "MO": "29", "MT": "30", "NE": "31", "NV": "32", "NH": "33",
    "NJ": "34", "NM": "35", "NY": "36", "NC": "37", "ND": "38", "OH": "39",
    "OK": "40", "OR": "41", "PA": "42", "RI": "44", "SC": "45", "SD": "46",
    "TN": "47", "TX": "48", "UT": "49", "VT": "50", "VA": "51", "WA": "53",
    "WV": "54", "WI": "55", "WY": "56",
}
FIPS_TO_STATE = {v: k for k, v in STATE_FIPS.items()}

STATE_NAMES = {
    "AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas",
    "CA": "California", "CO": "Colorado", "CT": "Connecticut", "DE": "Delaware",
    "DC": "District of Columbia", "FL": "Florida", "GA": "Georgia", "HI": "Hawaii",
    "ID": "Idaho", "IL": "Illinois", "IN": "Indiana", "IA": "Iowa", "KS": "Kansas",
    "KY": "Kentucky", "LA": "Louisiana", "ME": "Maine", "MD": "Maryland",
    "MA": "Massachusetts", "MI": "Michigan", "MN": "Minnesota", "MS": "Mississippi",
    "MO": "Missouri", "MT": "Montana", "NE": "Nebraska", "NV": "Nevada",
    "NH": "New Hampshire", "NJ": "New Jersey", "NM": "New Mexico", "NY": "New York",
    "NC": "North Carolina", "ND": "North Dakota", "OH": "Ohio", "OK": "Oklahoma",
    "OR": "Oregon", "PA": "Pennsylvania", "RI": "Rhode Island", "SC": "South Carolina",
    "SD": "South Dakota", "TN": "Tennessee", "TX": "Texas", "UT": "Utah",
    "VT": "Vermont", "VA": "Virginia", "WA": "Washington", "WV": "West Virginia",
    "WI": "Wisconsin", "WY": "Wyoming",
}


def _slug(city: str, state: str, name: str) -> str:
    """Generate a stable initiative_id slug."""
    parts = f"{city}-{state}-{name}"
    slug = re.sub(r"[^a-z0-9]+", "-", parts.lower()).strip("-")
    return slug[:120]


# ---------------------------------------------------------------------------
# 2a-i: TIGER/Line Place shapefiles
# ---------------------------------------------------------------------------

def _ensure_tiger_place(settings) -> gpd.GeoDataFrame:
    """Download (once) and load all TIGER/Line Place shapefiles."""
    cache_dir = Path(settings.TIGER_PLACE_CACHE_DIR)
    cache_dir.mkdir(parents=True, exist_ok=True)
    complete_sentinel = Path(settings.TIGER_PLACE_COMPLETE)

    if complete_sentinel.exists():
        logger.info("Loading TIGER Place from cache (%s)", cache_dir)
    else:
        logger.info("Downloading TIGER/Line Place shapefiles (~600 MB) — this happens once")
        _download_tiger_place(settings.TIGER_PLACE_URL, cache_dir)
        complete_sentinel.touch()

    gdfs = []
    for zip_path in sorted(cache_dir.glob("tl_2024_*_place.zip")):
        gdf = gpd.read_file(f"zip://{zip_path}")
        gdfs.append(gdf)

    if not gdfs:
        raise RuntimeError(f"No TIGER Place zip files found in {cache_dir}")

    places = pd.concat(gdfs, ignore_index=True)
    places = places.to_crs(epsg=4326)
    logger.info("Loaded %d place boundaries", len(places))
    return places


def _download_tiger_place(base_url: str, cache_dir: Path) -> None:
    """Download all 50-state Place zip files from Census TIGER."""
    import re as _re

    resp = requests.get(base_url, timeout=30)
    resp.raise_for_status()
    links = _re.findall(r'href="(tl_2024_\d{2}_place\.zip)"', resp.text)
    if not links:
        raise RuntimeError(f"Could not find Place zip links at {base_url}")

    for filename in links:
        dest = cache_dir / filename
        if dest.exists():
            logger.debug("Already cached: %s", filename)
            continue
        url = base_url.rstrip("/") + "/" + filename
        logger.info("Downloading %s", url)
        with requests.get(url, stream=True, timeout=120) as r:
            r.raise_for_status()
            with open(dest, "wb") as fh:
                for chunk in r.iter_content(chunk_size=1 << 20):
                    fh.write(chunk)


def _ensure_tiger_cd(settings) -> gpd.GeoDataFrame:
    """Download (once) and load the 119th Congress district shapefile."""
    cache_path = Path(settings.TIGER_CD_CACHE_PATH)
    cache_path.parent.mkdir(parents=True, exist_ok=True)

    if not cache_path.exists():
        logger.info("Downloading congressional district boundaries")
        with requests.get(settings.TIGER_CD_URL, stream=True, timeout=120) as r:
            r.raise_for_status()
            with open(cache_path, "wb") as fh:
                for chunk in r.iter_content(chunk_size=1 << 20):
                    fh.write(chunk)

    gdf = gpd.read_file(f"zip://{cache_path}")
    gdf = gdf.to_crs(epsg=4326)
    logger.info("Loaded %d congressional districts", len(gdf))
    return gdf


def _ensure_tiger_state(settings) -> gpd.GeoDataFrame:
    """Download (once) and load state boundary shapefile."""
    cache_path = Path(settings.TIGER_STATE_CACHE_PATH)
    cache_path.parent.mkdir(parents=True, exist_ok=True)

    if not cache_path.exists():
        logger.info("Downloading state boundary shapefile")
        with requests.get(settings.TIGER_STATE_URL, stream=True, timeout=120) as r:
            r.raise_for_status()
            with open(cache_path, "wb") as fh:
                for chunk in r.iter_content(chunk_size=1 << 20):
                    fh.write(chunk)

    gdf = gpd.read_file(f"zip://{cache_path}")
    gdf = gdf.to_crs(epsg=4326)
    return gdf


# ---------------------------------------------------------------------------
# 2a-ii: City → boundary lookup with fallback geocoding
# ---------------------------------------------------------------------------

def _find_city_boundary(city: str, state: str, places_gdf: gpd.GeoDataFrame) -> tuple:
    """
    Return (geometry, geometry_source).

    Tries TIGER Place match first; falls back to Census geocoder, then Nominatim.
    """
    state_fips = STATE_FIPS.get(state.upper(), "")
    # Exact name + state FIPS match
    mask = (places_gdf["NAME"].str.lower() == city.lower()) & (places_gdf["STATEFP"] == state_fips)
    matches = places_gdf[mask]

    if not matches.empty:
        geom = matches.iloc[0].geometry
        return geom, "place_boundary"

    # Fuzzy: contains match (e.g. "City of X" vs "X")
    mask2 = (places_gdf["NAME"].str.lower().str.contains(city.lower(), regex=False)) & \
            (places_gdf["STATEFP"] == state_fips)
    matches2 = places_gdf[mask2]
    if not matches2.empty:
        geom = matches2.iloc[0].geometry
        return geom, "place_boundary"

    logger.warning("'%s, %s' not in TIGER Place; falling back to Census geocoder", city, state)
    pt = _geocode_census(city, state)
    if pt:
        return pt, "census_centroid"

    logger.warning("Census geocoder failed for '%s, %s'; falling back to Nominatim", city, state)
    pt = _geocode_nominatim(city, state)
    if pt:
        return pt, "nominatim_centroid"

    return None, None


def _geocode_census(city: str, state: str) -> Point | None:
    try:
        resp = requests.get(
            "https://geocoding.geo.census.gov/geocoder/locations/onelineaddress",
            params={"address": f"{city}, {state}", "benchmark": "Public_AR_Current", "format": "json"},
            timeout=15,
        )
        data = resp.json()
        matches = data.get("result", {}).get("addressMatches", [])
        if matches:
            coords = matches[0]["coordinates"]
            return Point(coords["x"], coords["y"])
    except Exception as exc:  # noqa: BLE001
        logger.debug("Census geocoder error for '%s, %s': %s", city, state, exc)
    return None


def _geocode_nominatim(city: str, state: str) -> Point | None:
    time.sleep(1)  # Nominatim usage policy
    try:
        resp = requests.get(
            "https://nominatim.openstreetmap.org/search",
            params={"q": f"{city}, {state}, USA", "format": "json", "limit": 1},
            headers={"User-Agent": "ReparationsTrackerPipeline/1.0"},
            timeout=15,
        )
        results = resp.json()
        if results:
            return Point(float(results[0]["lon"]), float(results[0]["lat"]))
    except Exception as exc:  # noqa: BLE001
        logger.debug("Nominatim error for '%s, %s': %s", city, state, exc)
    return None


# ---------------------------------------------------------------------------
# 2a-iii: District intersection
# ---------------------------------------------------------------------------

def _intersect_with_districts(
    city: str,
    state: str,
    city_geom,
    districts_gdf: gpd.GeoDataFrame,
    geometry_source: str,
) -> list[dict]:
    """
    Return list of dicts — one per overlapping congressional district.
    Each dict has keys: geometry (Point), statefp, cd119fp, geometry_source.
    """
    if city_geom is None:
        return []

    # Centroid fallback: degenerate polygon — find containing district
    if geometry_source in ("census_centroid", "nominatim_centroid"):
        pt = city_geom  # already a Point
        hits = districts_gdf[districts_gdf.contains(pt)]
        if hits.empty:
            hits = districts_gdf[districts_gdf.distance(pt) < 0.01]
        if hits.empty:
            logger.warning("No district found for %s, %s centroid", city, state)
            return []
        row = hits.iloc[0]
        return [{
            "geometry": pt,
            "statefp": row["STATEFP"],
            "cd119fp": row.get("CD119FP", row.get("CD119", "")),
            "geometry_source": geometry_source,
        }]

    # Full boundary: overlay intersection
    city_gdf = gpd.GeoDataFrame(
        [{"geometry": city_geom}], crs="EPSG:4326"
    )
    try:
        inter = gpd.overlay(city_gdf, districts_gdf, how="intersection")
    except Exception as exc:  # noqa: BLE001
        logger.warning("Overlay failed for %s, %s: %s", city, state, exc)
        return []

    if inter.empty:
        # Fallback: centroid-in-district
        centroid = city_geom.centroid
        hits = districts_gdf[districts_gdf.contains(centroid)]
        if hits.empty:
            logger.warning("No district overlap found for %s, %s", city, state)
            return []
        row = hits.iloc[0]
        return [{
            "geometry": centroid,
            "statefp": row["STATEFP"],
            "cd119fp": row.get("CD119FP", row.get("CD119", "")),
            "geometry_source": "place_boundary",
        }]

    results = []
    for _, row in inter.iterrows():
        centroid = row.geometry.centroid
        results.append({
            "geometry": centroid,
            "statefp": row.get("STATEFP", ""),
            "cd119fp": row.get("CD119FP", row.get("CD119", "")),
            "geometry_source": "place_boundary",
        })
    return results


# ---------------------------------------------------------------------------
# 2b: Congress.gov lookups
# ---------------------------------------------------------------------------

def _congress_api_key() -> str:
    key = os.environ.get("CONGRESS_API_KEY", "")
    if not key:
        raise ValueError("CONGRESS_API_KEY environment variable is not set")
    return key


def _congress_get(url: str, params: dict | None = None, retries: int = 3) -> dict:
    """GET from Congress.gov with exponential backoff."""
    key = _congress_api_key()
    p = {"api_key": key, "format": "json", **(params or {})}
    delay = 1.0
    for attempt in range(retries):
        try:
            resp = requests.get(url, params=p, timeout=15)
            if resp.status_code == 429:
                logger.debug("Congress.gov rate limit; waiting %.1fs", delay)
                time.sleep(delay)
                delay *= 2
                continue
            resp.raise_for_status()
            return resp.json()
        except Exception as exc:  # noqa: BLE001
            if attempt == retries - 1:
                raise
            logger.debug("Congress.gov request failed (%s); retrying", exc)
            time.sleep(delay)
            delay *= 2
    return {}


_member_cache: dict[str, dict] = {}
_senate_cache: dict[str, list] = {}


def _lookup_house_member(state: str, district: str | int) -> dict:
    """Fetch current House member for state + district from Congress.gov."""
    key = f"{state}-{district}"
    if key in _member_cache:
        return _member_cache[key]

    url = f"https://api.congress.gov/v3/member/{state}/{int(district)}"
    try:
        data = _congress_get(url)
        members = data.get("members", [])
        # Filter to current members
        current = [m for m in members if m.get("currentMember", False)]
        if not current:
            current = members  # take whatever is returned
        if current:
            m = current[0]
            result = {
                "rep_bioguide_id": m.get("bioguideId", ""),
                "rep_name": m.get("name", ""),
                "rep_party": m.get("partyName", m.get("party", "")),
                "rep_district": int(district),
                "rep_phone": (m.get("officeTelephone") or {}).get("phoneNumber", ""),
                "rep_url": m.get("officialWebsiteUrl", ""),
            }
            _member_cache[key] = result
            return result
    except Exception as exc:  # noqa: BLE001
        logger.warning("Congress.gov House lookup failed for %s-%s: %s", state, district, exc)

    _member_cache[key] = {}
    return {}


def _lookup_senators(state: str) -> list[dict]:
    """Fetch current Senators for state from Congress.gov."""
    if state in _senate_cache:
        return _senate_cache[state]

    url = f"https://api.congress.gov/v3/member/{state}"
    try:
        data = _congress_get(url, {"chamber": "senate", "currentMember": "true"})
        members = data.get("members", [])
        senators = []
        for m in members:
            senators.append({
                "bioguide_id": m.get("bioguideId", ""),
                "name": m.get("name", ""),
                "party": m.get("partyName", m.get("party", "")),
                "phone": (m.get("officeTelephone") or {}).get("phoneNumber", ""),
                "url": m.get("officialWebsiteUrl", ""),
            })
        _senate_cache[state] = senators
        return senators
    except Exception as exc:  # noqa: BLE001
        logger.warning("Congress.gov Senate lookup failed for %s: %s", state, exc)

    _senate_cache[state] = []
    return []


# ---------------------------------------------------------------------------
# 2b fallback: Google Civic Information API
# ---------------------------------------------------------------------------

def _civic_lookup(city: str, state: str) -> dict:
    """
    Fallback: use Google Civic Information API to find House + Senate members.
    Returns dict with rep + sen1 + sen2 keys (partial — no Bioguide ID directly).
    """
    api_key = os.environ.get("GOOGLE_CIVIC_API_KEY", "")
    if not api_key:
        logger.warning("GOOGLE_CIVIC_API_KEY not set; cannot use Civic API fallback")
        return {}

    logger.warning("Invoking Google Civic API fallback for '%s, %s'", city, state)
    try:
        resp = requests.get(
            "https://www.googleapis.com/civicinfo/v2/representatives",
            params={"key": api_key, "address": f"{city}, {state}"},
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()
    except Exception as exc:  # noqa: BLE001
        logger.error("Google Civic API call failed for '%s, %s': %s", city, state, exc)
        return {}

    offices = data.get("offices", [])
    officials = data.get("officials", [])

    def _extract_official(idx: int) -> dict:
        if idx >= len(officials):
            return {}
        o = officials[idx]
        return {
            "name": o.get("name", ""),
            "party": o.get("party", ""),
            "phone": (o.get("phones") or [""])[0],
            "url": (o.get("urls") or [""])[0],
            "photo_url": o.get("photoUrl", ""),
        }

    result: dict = {}
    for office in offices:
        title = office.get("name", "")
        indices = office.get("officialIndices", [])
        if "U.S. Representative" in title and indices:
            official = _extract_official(indices[0])
            bioguide = _resolve_bioguide_by_name(official.get("name", ""), state, "House")
            result["rep"] = {**official, "bioguide_id": bioguide}
        elif "U.S. Senator" in title:
            for i, idx in enumerate(indices[:2], start=1):
                official = _extract_official(idx)
                bioguide = _resolve_bioguide_by_name(official.get("name", ""), state, "Senate")
                result[f"sen{i}"] = {**official, "bioguide_id": bioguide}

    return result


def _resolve_bioguide_by_name(name: str, state: str, chamber: str) -> str:
    """Attempt to find a Bioguide ID by searching Congress.gov member list."""
    if not name:
        return ""
    try:
        url = "https://api.congress.gov/v3/member"
        data = _congress_get(url, {"stateCode": state, "currentMember": "true"})
        members = data.get("members", [])
        name_lower = name.lower()
        for m in members:
            m_chamber = m.get("chamber", "")
            if chamber == "House" and m_chamber.lower() != "house of representatives":
                continue
            if chamber == "Senate" and m_chamber.lower() != "senate":
                continue
            if name_lower in m.get("name", "").lower():
                return m.get("bioguideId", "")
    except Exception as exc:  # noqa: BLE001
        logger.debug("Bioguide name resolution failed for '%s': %s", name, exc)
    return ""


# ---------------------------------------------------------------------------
# 2c: H.R. 40 co-sponsorship
# ---------------------------------------------------------------------------

_hr40_cosponsors: set[str] | None = None


def _get_hr40_cosponsors() -> set[str]:
    global _hr40_cosponsors
    if _hr40_cosponsors is not None:
        return _hr40_cosponsors

    cosponsors: set[str] = set()
    # Check 119th Congress (current)
    for congress in [119, 118, 117]:
        try:
            url = f"https://api.congress.gov/v3/bill/{congress}/hr/40/cosponsors"
            data = _congress_get(url)
            for c in data.get("cosponsors", []):
                bid = c.get("bioguideId", "")
                if bid:
                    cosponsors.add(bid)
        except Exception as exc:  # noqa: BLE001
            logger.debug("H.R.40 cosponsor fetch failed for Congress %d: %s", congress, exc)

    _hr40_cosponsors = cosponsors
    logger.info("H.R. 40 co-sponsors loaded: %d members", len(cosponsors))
    return cosponsors


def _hr40_position(bioguide_id: str) -> str:
    if not bioguide_id:
        return "No co-sponsorship recorded"
    cosponsors = _get_hr40_cosponsors()
    return "Co-sponsor" if bioguide_id in cosponsors else "No co-sponsorship recorded"


# ---------------------------------------------------------------------------
# 2f: delegation alignment
# ---------------------------------------------------------------------------

_DEM = {"Democrat", "Democratic"}
_REP = {"Republican"}


def _delegation_alignment(party1: str, party2: str) -> str:
    p1 = party1.strip()
    p2 = party2.strip()
    if p1 in _DEM and p2 in _DEM:
        return "Both Democratic"
    if p1 in _REP and p2 in _REP:
        return "Both Republican"
    if p1 not in _DEM and p1 not in _REP and p2 not in _DEM and p2 not in _REP:
        return "Both Independent"
    return "Split"


# ---------------------------------------------------------------------------
# Geocode cache helpers
# ---------------------------------------------------------------------------

def _load_geocode_cache(path: str) -> dict:
    if os.path.exists(path):
        try:
            with open(path, encoding="utf-8") as fh:
                return json.load(fh)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Could not load geocode cache (%s); starting fresh", exc)
    return {}


def _save_geocode_cache(path: str, cache: dict) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(cache, fh, indent=2)


# ---------------------------------------------------------------------------
# Main enrichment logic
# ---------------------------------------------------------------------------

def _log_error(errors_log: str, initiative: dict, exc: Exception) -> None:
    os.makedirs(os.path.dirname(errors_log), exist_ok=True)
    with open(errors_log, "a", encoding="utf-8") as fh:
        ts = datetime.now(timezone.utc).isoformat()
        name = initiative.get("initiative_name", "?")
        city = initiative.get("city", "?")
        state = initiative.get("state", "?")
        fh.write(f"[{ts}] ERROR enriching '{name}' ({city}, {state}): {exc}\n")


def run(settings) -> None:
    """
    Enrich initiatives:
      - Load/download TIGER shapefiles
      - Intersect city boundaries with CD119 polygons
      - Look up House members and Senators via Congress.gov
      - Fetch H.R. 40 co-sponsorship data
      - Write initiatives_enriched.geojson and legislators_summary.json
    """
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")

    # Load raw initiatives
    with open(settings.RAW_DATA_PATH, encoding="utf-8") as fh:
        initiatives = json.load(fh)
    logger.info("Loaded %d raw initiatives", len(initiatives))

    # Load TIGER data
    places_gdf = _ensure_tiger_place(settings)
    districts_gdf = _ensure_tiger_cd(settings)

    # Load geocode cache
    geocode_cache: dict = _load_geocode_cache(settings.GEOCODE_CACHE_PATH)

    # Pre-fetch H.R. 40 cosponsor list
    _get_hr40_cosponsors()

    now_iso = datetime.now(timezone.utc).isoformat()
    features = []

    for initiative in initiatives:
        city = (initiative.get("city") or "").strip()
        state = (initiative.get("state") or "").strip().upper()
        name = (initiative.get("initiative_name") or "").strip()

        if not city or not state:
            logger.warning("Skipping initiative with missing city/state: %s", initiative)
            continue

        initiative_id = _slug(city, state, name)
        cache_key = f"{city}, {state}"

        try:
            # --- Boundary / geocode ---
            if cache_key in geocode_cache:
                cached = geocode_cache[cache_key]
                geom_source = cached["geometry_source"]
                # Reconstruct geometry from cache
                if geom_source == "place_boundary":
                    from shapely.geometry import shape  # noqa: PLC0415
                    city_geom = shape(cached["geometry"])
                else:
                    pt = cached["centroid"]
                    city_geom = Point(pt[0], pt[1])
            else:
                city_geom, geom_source = _find_city_boundary(city, state, places_gdf)
                if city_geom is not None:
                    if geom_source == "place_boundary":
                        geocode_cache[cache_key] = {
                            "geometry_source": geom_source,
                            "geometry": mapping(city_geom),
                            "centroid": [city_geom.centroid.x, city_geom.centroid.y],
                        }
                    else:
                        geocode_cache[cache_key] = {
                            "geometry_source": geom_source,
                            "centroid": [city_geom.x, city_geom.y],
                        }

            if city_geom is None:
                logger.error("Could not geocode '%s, %s'; skipping initiative '%s'", city, state, name)
                continue

            # --- District intersection ---
            intersections = _intersect_with_districts(city, state, city_geom, districts_gdf, geom_source)
            if not intersections:
                logger.warning("No district intersections for '%s, %s'; skipping", city, state)
                continue

            # --- Congressional lookups ---
            senators = _lookup_senators(state)
            # Fallback for senators
            if not senators:
                civic = _civic_lookup(city, state)
                sen1_info = civic.get("sen1", {})
                sen2_info = civic.get("sen2", {})
                senators = [
                    {
                        "bioguide_id": sen1_info.get("bioguide_id", ""),
                        "name": sen1_info.get("name", ""),
                        "party": sen1_info.get("party", ""),
                        "phone": sen1_info.get("phone", ""),
                        "url": sen1_info.get("url", ""),
                    },
                    {
                        "bioguide_id": sen2_info.get("bioguide_id", ""),
                        "name": sen2_info.get("name", ""),
                        "party": sen2_info.get("party", ""),
                        "phone": sen2_info.get("phone", ""),
                        "url": sen2_info.get("url", ""),
                    },
                ]

            sen1 = senators[0] if len(senators) > 0 else {}
            sen2 = senators[1] if len(senators) > 1 else {}

            for inter in intersections:
                pt: Point = inter["geometry"]
                statefp = inter["statefp"]
                cd119fp = inter["cd119fp"]

                try:
                    district_num = int(cd119fp.lstrip("0") or "0")
                except (ValueError, AttributeError):
                    district_num = 0

                rep_info = _lookup_house_member(state, district_num)

                # Civic fallback for rep if needed
                if not rep_info or not rep_info.get("rep_bioguide_id"):
                    civic = _civic_lookup(city, state)
                    civic_rep = civic.get("rep", {})
                    rep_info = {
                        "rep_bioguide_id": civic_rep.get("bioguide_id", ""),
                        "rep_name": civic_rep.get("name", ""),
                        "rep_party": civic_rep.get("party", ""),
                        "rep_district": district_num,
                        "rep_phone": civic_rep.get("phone", ""),
                        "rep_url": civic_rep.get("url", ""),
                    }

                # H.R. 40 positions
                hr40_rep = _hr40_position(rep_info.get("rep_bioguide_id", ""))
                hr40_s1 = _hr40_position(sen1.get("bioguide_id", ""))
                hr40_s2 = _hr40_position(sen2.get("bioguide_id", ""))

                props = {
                    **{k: v for k, v in initiative.items()},
                    "initiative_id": initiative_id,
                    # Rep
                    "rep_bioguide_id": rep_info.get("rep_bioguide_id", ""),
                    "rep_name": rep_info.get("rep_name", ""),
                    "rep_party": rep_info.get("rep_party", ""),
                    "rep_district": rep_info.get("rep_district", district_num),
                    "rep_phone": rep_info.get("rep_phone", ""),
                    "rep_url": rep_info.get("rep_url", ""),
                    # Sen1
                    "sen1_bioguide_id": sen1.get("bioguide_id", ""),
                    "sen1_name": sen1.get("name", ""),
                    "sen1_party": sen1.get("party", ""),
                    "sen1_phone": sen1.get("phone", ""),
                    "sen1_url": sen1.get("url", ""),
                    # Sen2
                    "sen2_bioguide_id": sen2.get("bioguide_id", ""),
                    "sen2_name": sen2.get("name", ""),
                    "sen2_party": sen2.get("party", ""),
                    "sen2_phone": sen2.get("phone", ""),
                    "sen2_url": sen2.get("url", ""),
                    # H.R. 40
                    "hr40_rep_position": hr40_rep,
                    "hr40_sen1_position": hr40_s1,
                    "hr40_sen2_position": hr40_s2,
                    # Meta
                    "geometry_source": inter["geometry_source"],
                    "last_updated": now_iso,
                }

                features.append({
                    "type": "Feature",
                    "geometry": {"type": "Point", "coordinates": [pt.x, pt.y]},
                    "properties": props,
                })

        except Exception as exc:  # noqa: BLE001
            logger.error("Failed to enrich '%s, %s - %s': %s", city, state, name, exc)
            _log_error(settings.ERRORS_LOG_PATH, initiative, exc)

    # Always write geocode cache back
    _save_geocode_cache(settings.GEOCODE_CACHE_PATH, geocode_cache)

    # Write enriched GeoJSON
    geojson = {"type": "FeatureCollection", "features": features}
    os.makedirs(os.path.dirname(settings.ENRICHED_DATA_PATH), exist_ok=True)
    with open(settings.ENRICHED_DATA_PATH, "w", encoding="utf-8") as fh:
        json.dump(geojson, fh, indent=2, ensure_ascii=False)
    logger.info("Wrote %d enriched features to %s", len(features), settings.ENRICHED_DATA_PATH)

    # Build legislators summary
    legislators = _build_legislators_summary(features)
    os.makedirs(os.path.dirname(settings.LEGISLATORS_PATH), exist_ok=True)
    with open(settings.LEGISLATORS_PATH, "w", encoding="utf-8") as fh:
        json.dump(legislators, fh, indent=2, ensure_ascii=False)
    logger.info("Wrote %d legislators to %s", len(legislators), settings.LEGISLATORS_PATH)


# ---------------------------------------------------------------------------
# 2e + 2f: Build legislators summary
# ---------------------------------------------------------------------------

def _build_legislators_summary(features: list[dict]) -> list[dict]:
    """
    Deduplicate all legislators from enriched features.
    Compute initiative_count (distinct initiative_id values) per legislator.
    Derive delegation_alignment per state.
    """
    # Aggregate per legislator
    leg_map: dict[str, dict] = {}  # bioguide_id -> info

    def _ensure(bioguide_id: str, name: str, chamber: str, party: str,
                state: str, district, phone: str, url: str, photo_url: str = "") -> None:
        if not bioguide_id:
            return
        if bioguide_id not in leg_map:
            leg_map[bioguide_id] = {
                "bioguide_id": bioguide_id,
                "name": name,
                "chamber": chamber,
                "party": party,
                "state": state,
                "district": district,
                "initiative_ids": set(),
                "hr40_position": _hr40_position(bioguide_id),
                "phone": phone,
                "url": url,
                "photo_url": photo_url,
            }

    for feat in features:
        p = feat["properties"]
        init_id = p.get("initiative_id", "")
        state = p.get("state", "")

        rep_bid = p.get("rep_bioguide_id", "")
        _ensure(rep_bid, p.get("rep_name", ""), "House",
                p.get("rep_party", ""), state, p.get("rep_district"),
                p.get("rep_phone", ""), p.get("rep_url", ""))
        if rep_bid in leg_map:
            leg_map[rep_bid]["initiative_ids"].add(init_id)

        s1_bid = p.get("sen1_bioguide_id", "")
        _ensure(s1_bid, p.get("sen1_name", ""), "Senate",
                p.get("sen1_party", ""), state, None,
                p.get("sen1_phone", ""), p.get("sen1_url", ""))
        if s1_bid in leg_map:
            leg_map[s1_bid]["initiative_ids"].add(init_id)

        s2_bid = p.get("sen2_bioguide_id", "")
        _ensure(s2_bid, p.get("sen2_name", ""), "Senate",
                p.get("sen2_party", ""), state, None,
                p.get("sen2_phone", ""), p.get("sen2_url", ""))
        if s2_bid in leg_map:
            leg_map[s2_bid]["initiative_ids"].add(init_id)

    # Compute delegation_alignment per state
    state_senators: dict[str, list[str]] = {}  # state -> [party1, party2]
    for leg in leg_map.values():
        if leg["chamber"] == "Senate" and leg["state"]:
            s = leg["state"]
            state_senators.setdefault(s, []).append(leg["party"])

    state_alignment: dict[str, str] = {}
    for state, parties in state_senators.items():
        if len(parties) >= 2:
            state_alignment[state] = _delegation_alignment(parties[0], parties[1])
        elif len(parties) == 1:
            state_alignment[state] = "Split"  # incomplete data

    result = []
    for leg in leg_map.values():
        d = {k: v for k, v in leg.items() if k != "initiative_ids"}
        d["initiative_count"] = len(leg["initiative_ids"])
        if leg["chamber"] == "Senate":
            d["delegation_alignment"] = state_alignment.get(leg["state"], "")
        else:
            d["delegation_alignment"] = ""
        result.append(d)

    return sorted(result, key=lambda x: (x["state"], x["chamber"], x["name"]))


if __name__ == "__main__":
    import config.settings as settings

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    run(settings)
