"""
Step 3: Publish four ArcGIS Online layers using direct REST API calls.

Design principles:
  - No arcgis Python package — pure requests + REST API
  - Idempotent: stores item IDs in config/settings.py; overwrites data in place on subsequent runs
  - Safety check: aborts if new record count < MIN_RECORD_RATIO * existing count
  - ArcGIS Trash: immediately purges after any delete to avoid name-conflict issues

Layers published:
  1. Reparations_Initiatives_Points       — Feature Layer (Point)
  2. Reparations_Legislators_Summary      — Hosted Table (no geometry)
  3. Reparations_Congressional_Districts  — Feature Layer (Polygon)
  4. Reparations_Senate_States            — Feature Layer (Polygon)

Entry point: run(settings)
"""

import json
import logging
import os
import re
import time
from pathlib import Path

import geopandas as gpd
import pandas as pd
import requests

logger = logging.getLogger(__name__)

def _portal_base(settings=None) -> str:
    """Return the sharing/rest base URL for the configured portal."""
    portal = "https://www.arcgis.com"
    if settings is not None:
        portal = getattr(settings, "AGOL_PORTAL_URL", portal).rstrip("/")
    return f"{portal}/sharing/rest"


# ---------------------------------------------------------------------------
# Authentication
# ---------------------------------------------------------------------------

class AGOLSession:
    """Thin ArcGIS Online REST session with automatic token refresh."""

    def __init__(self, username: str, password: str, settings=None):
        self.username = username
        self.password = password
        self._settings = settings
        self._token: str = ""
        self._token_expires: float = 0.0
        self._refresh()

    def _refresh(self) -> None:
        token_endpoint = _portal_base(self._settings) + "/generateToken"
        resp = requests.post(
            token_endpoint,
            data={
                "username": self.username,
                "password": self.password,
                "referer": "https://www.arcgis.com",
                "expiration": 120,
                "f": "json",
            },
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        if "error" in data:
            raise RuntimeError(f"AGOL token error: {data['error']}")
        self._token = data["token"]
        # expires is milliseconds from epoch
        self._token_expires = data.get("expires", 0) / 1000.0 - 60
        logger.info("AGOL token obtained for user '%s'", self.username)

    def token(self) -> str:
        if time.time() > self._token_expires:
            logger.info("AGOL token expiring; refreshing")
            self._refresh()
        return self._token

    def get(self, url: str, **kwargs) -> requests.Response:
        kwargs.setdefault("params", {})
        kwargs["params"]["token"] = self.token()
        kwargs["params"].setdefault("f", "json")
        return requests.get(url, timeout=30, **kwargs)

    def post(self, url: str, **kwargs) -> requests.Response:
        kwargs.setdefault("data", {})
        kwargs["data"]["token"] = self.token()
        kwargs["data"].setdefault("f", "json")
        return requests.post(url, timeout=60, **kwargs)


# ---------------------------------------------------------------------------
# Item management helpers
# ---------------------------------------------------------------------------

def _user_url(session: AGOLSession) -> str:
    return _portal_base(session._settings) + f"/content/users/{session.username}"


def _add_item(session: AGOLSession, title: str, item_type: str,
               tags: str, description: str, file_path: str | None = None,
               text: str | None = None) -> str:
    """Upload an item and return its itemId."""
    url = f"{_user_url(session)}/addItem"
    data = {
        "title": title,
        "type": item_type,
        "tags": tags,
        "description": description,
        "overwrite": "false",
        "f": "json",
        "token": session.token(),
    }
    if text is not None:
        data["text"] = text
        resp = requests.post(url, data=data, timeout=60)
    elif file_path:
        with open(file_path, "rb") as fh:
            resp = requests.post(url, data=data, files={"file": fh}, timeout=120)
    else:
        raise ValueError("Either file_path or text must be provided")

    resp.raise_for_status()
    result = resp.json()
    if not result.get("success"):
        # Check for name conflict — item may be in Trash
        msg = str(result.get("error", result))
        if "already exists" in msg.lower() or "conflict" in msg.lower():
            logger.warning("Name conflict on addItem for '%s'; checking Trash", title)
            _purge_trash_by_name(session, title)
            # Retry once
            if text is not None:
                resp2 = requests.post(url, data=data, timeout=60)
            else:
                with open(file_path, "rb") as fh:
                    resp2 = requests.post(url, data=data, files={"file": fh}, timeout=120)
            resp2.raise_for_status()
            result = resp2.json()
            if not result.get("success"):
                raise RuntimeError(f"addItem failed after purge: {result}")
        else:
            raise RuntimeError(f"addItem failed: {result}")

    return result["id"]


def _delete_item(session: AGOLSession, item_id: str) -> None:
    """Delete an item and immediately purge from Trash."""
    # Delete
    resp = session.post(f"{_user_url(session)}/items/{item_id}/delete")
    data = resp.json()
    if not data.get("success"):
        logger.warning("Delete may have failed for %s: %s", item_id, data)

    # Purge from Trash
    resp2 = session.post(
        f"{_user_url(session)}/deleteItems",
        data={"items": item_id},
    )
    purge = resp2.json()
    logger.debug("Trash purge result for %s: %s", item_id, purge)


def _purge_trash_by_name(session: AGOLSession, title: str) -> None:
    """Find items in Trash matching title and purge them."""
    resp = session.get(
        f"{_portal_base(session._settings)}/search",
        params={"q": f'title:"{title}" owner:{session.username}', "num": 10},
    )
    items = resp.json().get("results", [])
    for item in items:
        if item.get("title") == title:
            logger.info("Purging trashed item %s (%s)", item["id"], title)
            _delete_item(session, item["id"])


def _publish_item(session: AGOLSession, item_id: str, name: str,
                  layer_type: str = "GeoJson") -> str:
    """Publish an uploaded item as a hosted feature layer. Returns the service item ID."""
    url = f"{_user_url(session)}/publish"
    publish_params = json.dumps({
        "name": name,
        "hasStaticData": False,
        "layerInfo": {"capabilities": "Query,Editing"},
    })
    resp = session.post(url, data={
        "itemid": item_id,
        "filetype": layer_type,
        "publishParameters": publish_params,
    })
    resp.raise_for_status()
    data = resp.json()

    services = data.get("services", [])
    if not services:
        raise RuntimeError(f"publish returned no services: {data}")

    service_item_id = services[0].get("serviceItemId", services[0].get("itemId", ""))
    job_id = services[0].get("jobId", "")

    if job_id:
        _poll_job(session, item_id, job_id)

    return service_item_id or item_id


def _poll_job(session: AGOLSession, item_id: str, job_id: str,
              timeout_s: int = 600) -> None:
    """Poll item status until job completes."""
    url = f"{_user_url(session)}/items/{item_id}/status"
    deadline = time.time() + timeout_s
    delay = 2.0
    while time.time() < deadline:
        resp = session.get(url, params={"jobId": job_id})
        status_data = resp.json()
        status = status_data.get("status", "").lower()
        logger.debug("Job %s status: %s", job_id, status)
        if status in ("completed", "complete"):
            return
        if status in ("failed", "error"):
            raise RuntimeError(f"Publish job {job_id} failed: {status_data}")
        time.sleep(delay)
        delay = min(delay * 1.5, 30)
    raise TimeoutError(f"Publish job {job_id} did not complete within {timeout_s}s")


# ---------------------------------------------------------------------------
# Feature layer service URL resolution
# ---------------------------------------------------------------------------

def _get_service_url(session: AGOLSession, item_id: str) -> str:
    """Resolve the FeatureServer URL for an item."""
    resp = session.get(f"{_portal_base(session._settings)}/content/items/{item_id}")
    data = resp.json()
    url = data.get("url", "")
    if not url:
        raise RuntimeError(f"No service URL for item {item_id}: {data}")
    return url.rstrip("/")


def _get_org_id(session: AGOLSession) -> str:
    resp = session.get(f"{_portal_base(session._settings)}/portals/self")
    return resp.json().get("id", "")


# ---------------------------------------------------------------------------
# Feature count
# ---------------------------------------------------------------------------

def _get_feature_count(service_url: str, session: AGOLSession, layer: int = 0) -> int:
    resp = session.get(
        f"{service_url}/FeatureServer/{layer}/query",
        params={"where": "1=1", "returnCountOnly": "true"},
    )
    return resp.json().get("count", 0)


# ---------------------------------------------------------------------------
# Truncate + append
# ---------------------------------------------------------------------------

def _truncate_layer(service_url: str, session: AGOLSession, layer: int = 0) -> None:
    resp = session.post(
        f"{service_url}/FeatureServer/{layer}/deleteFeatures",
        data={"where": "1=1"},
    )
    data = resp.json()
    logger.debug("Truncate result: %s", data)


def _append_features(service_url: str, session: AGOLSession,
                     features: list[dict], layer: int = 0,
                     batch_size: int = 2000) -> None:
    """Append features to a feature layer in batches."""
    total = len(features)
    for i in range(0, total, batch_size):
        batch = features[i: i + batch_size]
        resp = session.post(
            f"{service_url}/FeatureServer/{layer}/addFeatures",
            data={"features": json.dumps(batch)},
        )
        result = resp.json()
        added = len(result.get("addResults", []))
        logger.info("  Added batch %d-%d (%d features)", i, i + len(batch), added)


# ---------------------------------------------------------------------------
# Safety check
# ---------------------------------------------------------------------------

def _safety_check(service_url: str, session: AGOLSession,
                  new_count: int, min_ratio: float, layer: int = 0) -> None:
    existing = _get_feature_count(service_url, session, layer)
    if existing == 0:
        return  # first data load; nothing to compare
    if new_count < existing * min_ratio:
        raise RuntimeError(
            f"Safety check failed: new record count ({new_count}) is less than "
            f"{min_ratio:.0%} of existing count ({existing}). "
            "Aborting publish to protect existing data."
        )


# ---------------------------------------------------------------------------
# Settings writeback
# ---------------------------------------------------------------------------

def _write_item_id_to_settings(field: str, item_id: str) -> None:
    """Patch the AGOL_*_ITEM_ID constant in config/settings.py."""
    # Find the settings file relative to this module
    settings_path = Path(__file__).parent.parent / "config" / "settings.py"
    if not settings_path.exists():
        logger.warning("settings.py not found at %s; skipping writeback", settings_path)
        return

    text = settings_path.read_text(encoding="utf-8")
    pattern = rf'({re.escape(field)}\s*=\s*")[^"]*(")'
    replacement = rf'\g<1>{item_id}\g<2>'
    new_text = re.sub(pattern, replacement, text)
    if new_text == text:
        logger.warning("Could not patch %s in settings.py (pattern not found)", field)
        return
    settings_path.write_text(new_text, encoding="utf-8")
    logger.info("Wrote %s = '%s' to settings.py", field, item_id)


# ---------------------------------------------------------------------------
# GeoJSON / JSON → AGOL feature format
# ---------------------------------------------------------------------------

def _geojson_to_agol_features(geojson_path: str) -> list[dict]:
    """Convert GeoJSON FeatureCollection to AGOL REST feature format."""
    with open(geojson_path, encoding="utf-8") as fh:
        fc = json.load(fh)

    features = []
    for feat in fc.get("features", []):
        geom = feat.get("geometry", {})
        props = feat.get("properties", {})
        agol_feat: dict = {"attributes": props}
        if geom and geom.get("type") == "Point":
            coords = geom["coordinates"]
            agol_feat["geometry"] = {"x": coords[0], "y": coords[1], "spatialReference": {"wkid": 4326}}
        elif geom and geom.get("type") == "Polygon":
            agol_feat["geometry"] = {
                "rings": geom["coordinates"],
                "spatialReference": {"wkid": 4326},
            }
        elif geom and geom.get("type") == "MultiPolygon":
            rings = []
            for poly in geom["coordinates"]:
                rings.extend(poly)
            agol_feat["geometry"] = {"rings": rings, "spatialReference": {"wkid": 4326}}
        features.append(agol_feat)
    return features


def _json_to_agol_features(json_path: str) -> list[dict]:
    """Convert a JSON array (no geometry) to AGOL table feature format."""
    with open(json_path, encoding="utf-8") as fh:
        records = json.load(fh)
    return [{"attributes": r} for r in records]


# ---------------------------------------------------------------------------
# Layer 3: build enriched district GeoJSON
# ---------------------------------------------------------------------------

def _build_districts_geojson(settings, enriched_geojson: str, legislators_json: str) -> str:
    """
    Aggregate initiative data per congressional district and join rep attributes.
    Returns path to a temp GeoJSON file.
    """
    import tempfile  # noqa: PLC0415

    # Load enriched points
    with open(enriched_geojson, encoding="utf-8") as fh:
        fc = json.load(fh)

    # Build district stats: distinct initiative_id per rep_bioguide_id
    dist_map: dict[str, dict] = {}  # rep_bioguide_id -> {init_ids, rep attrs}
    for feat in fc.get("features", []):
        p = feat["properties"]
        bid = p.get("rep_bioguide_id", "")
        if not bid:
            continue
        if bid not in dist_map:
            dist_map[bid] = {
                "rep_bioguide_id": bid,
                "rep_name": p.get("rep_name", ""),
                "rep_party": p.get("rep_party", ""),
                "rep_district": p.get("rep_district", ""),
                "state": p.get("state", ""),
                "hr40_rep_position": p.get("hr40_rep_position", ""),
                "rep_phone": p.get("rep_phone", ""),
                "rep_url": p.get("rep_url", ""),
                "initiative_ids": set(),
            }
        dist_map[bid]["initiative_ids"].add(p.get("initiative_id", ""))

    # Load district polygons from TIGER cache
    import glob as _glob  # noqa: PLC0415
    cd_dir = settings.TIGER_CD_CACHE_DIR
    cd_zips = sorted(_glob.glob(f"{cd_dir}tl_2024_*_cd119.zip"))
    if not cd_zips:
        raise RuntimeError(f"No CD119 zip files found in {cd_dir} — run Step 2 first")
    districts_gdf = pd.concat(
        [gpd.read_file(f"zip://{z}") for z in cd_zips], ignore_index=True
    ).to_crs(epsg=4326)

    features = []
    for _, row in districts_gdf.iterrows():
        statefp = row.get("STATEFP", "")
        # Inline FIPS→state lookup (avoids cross-module import)
        _FIPS_TO_STATE = {v: k for k, v in {
            "AL": "01", "AK": "02", "AZ": "04", "AR": "05", "CA": "06",
            "CO": "08", "CT": "09", "DE": "10", "DC": "11", "FL": "12",
            "GA": "13", "HI": "15", "ID": "16", "IL": "17", "IN": "18",
            "IA": "19", "KS": "20", "KY": "21", "LA": "22", "ME": "23",
            "MD": "24", "MA": "25", "MI": "26", "MN": "27", "MS": "28",
            "MO": "29", "MT": "30", "NE": "31", "NV": "32", "NH": "33",
            "NJ": "34", "NM": "35", "NY": "36", "NC": "37", "ND": "38",
            "OH": "39", "OK": "40", "OR": "41", "PA": "42", "RI": "44",
            "SC": "45", "SD": "46", "TN": "47", "TX": "48", "UT": "49",
            "VT": "50", "VA": "51", "WA": "53", "WV": "54", "WI": "55",
            "WY": "56",
        }.items()}

        state_abbr = _FIPS_TO_STATE.get(statefp, "")
        cd119fp = row.get("CD119FP", row.get("CD119", ""))
        try:
            district_num = int(cd119fp.lstrip("0") or "0")
        except (ValueError, AttributeError):
            district_num = 0

        # Find matching rep by state + district
        matched_bid = ""
        for bid, info in dist_map.items():
            if info["state"] == state_abbr and str(info["rep_district"]) == str(district_num):
                matched_bid = bid
                break

        info = dist_map.get(matched_bid, {})
        initiative_count = len(info.get("initiative_ids", set()))

        geom = row.geometry
        if geom is None:
            continue

        import json as _json  # noqa: PLC0415
        from shapely.geometry import mapping  # noqa: PLC0415
        geom_dict = _json.loads(gpd.GeoSeries([geom]).to_json())["features"][0]["geometry"]

        feat = {
            "type": "Feature",
            "geometry": geom_dict,
            "properties": {
                "STATEFP": statefp,
                "state": state_abbr,
                "CD119FP": cd119fp,
                "district_num": district_num,
                "rep_bioguide_id": info.get("rep_bioguide_id", ""),
                "rep_name": info.get("rep_name", ""),
                "rep_party": info.get("rep_party", ""),
                "rep_district": district_num,
                "hr40_rep_position": info.get("hr40_rep_position", ""),
                "rep_phone": info.get("rep_phone", ""),
                "rep_url": info.get("rep_url", ""),
                "initiative_count": initiative_count,
            },
        }
        features.append(feat)

    out_fc = {"type": "FeatureCollection", "features": features}
    tmp = tempfile.NamedTemporaryFile(
        suffix="_districts.geojson", delete=False, mode="w", encoding="utf-8"
    )
    json.dump(out_fc, tmp, ensure_ascii=False)
    tmp.close()
    return tmp.name


# ---------------------------------------------------------------------------
# Layer 4: build enriched state GeoJSON
# ---------------------------------------------------------------------------

def _build_states_geojson(settings, enriched_geojson: str, legislators_json: str) -> str:
    """
    Aggregate initiative data per state and join senator attributes.
    """
    import tempfile  # noqa: PLC0415

    with open(enriched_geojson, encoding="utf-8") as fh:
        fc = json.load(fh)
    with open(legislators_json, encoding="utf-8") as fh:
        legislators = json.load(fh)

    # State → senator info map
    state_senators: dict[str, list[dict]] = {}
    for leg in legislators:
        if leg.get("chamber") == "Senate":
            state = leg.get("state", "")
            state_senators.setdefault(state, []).append(leg)

    # State initiative count (distinct initiative_ids)
    state_init_ids: dict[str, set] = {}
    for feat in fc.get("features", []):
        p = feat["properties"]
        state = p.get("state", "")
        state_init_ids.setdefault(state, set()).add(p.get("initiative_id", ""))

    # Download state boundaries if not already cached
    state_path = settings.TIGER_STATE_CACHE_PATH
    if not os.path.exists(state_path):
        os.makedirs(os.path.dirname(state_path), exist_ok=True)
        logger.info("Downloading TIGER state boundaries...")
        r = requests.get(settings.TIGER_STATE_URL, timeout=120, stream=True)
        r.raise_for_status()
        with open(state_path, "wb") as fh:
            for chunk in r.iter_content(65536):
                fh.write(chunk)
        logger.info("Downloaded state boundaries to %s", state_path)
    states_gdf = gpd.read_file(f"zip://{state_path}").to_crs(epsg=4326)

    _FIPS_TO_STATE = {v: k for k, v in {
        "AL": "01", "AK": "02", "AZ": "04", "AR": "05", "CA": "06",
        "CO": "08", "CT": "09", "DE": "10", "DC": "11", "FL": "12",
        "GA": "13", "HI": "15", "ID": "16", "IL": "17", "IN": "18",
        "IA": "19", "KS": "20", "KY": "21", "LA": "22", "ME": "23",
        "MD": "24", "MA": "25", "MI": "26", "MN": "27", "MS": "28",
        "MO": "29", "MT": "30", "NE": "31", "NV": "32", "NH": "33",
        "NJ": "34", "NM": "35", "NY": "36", "NC": "37", "ND": "38",
        "OH": "39", "OK": "40", "OR": "41", "PA": "42", "RI": "44",
        "SC": "45", "SD": "46", "TN": "47", "TX": "48", "UT": "49",
        "VT": "50", "VA": "51", "WA": "53", "WV": "54", "WI": "55",
        "WY": "56",
    }.items()}

    _DEM = {"Democrat", "Democratic"}
    _REP = {"Republican"}

    def _alignment(p1: str, p2: str) -> str:
        if p1 in _DEM and p2 in _DEM:
            return "Both Democratic"
        if p1 in _REP and p2 in _REP:
            return "Both Republican"
        if p1 not in _DEM and p1 not in _REP and p2 not in _DEM and p2 not in _REP:
            return "Both Independent"
        return "Split"

    features = []
    for _, row in states_gdf.iterrows():
        statefp = row.get("STATEFP", "")
        state_abbr = _FIPS_TO_STATE.get(statefp, "")
        state_name = row.get("NAME", state_abbr)

        if not state_abbr:
            continue

        senators = state_senators.get(state_abbr, [])
        sen1 = senators[0] if len(senators) > 0 else {}
        sen2 = senators[1] if len(senators) > 1 else {}

        alignment = ""
        if sen1 and sen2:
            alignment = _alignment(sen1.get("party", ""), sen2.get("party", ""))
        elif sen1:
            alignment = "Split"

        init_count = len(state_init_ids.get(state_abbr, set()))

        geom = row.geometry
        if geom is None:
            continue

        geom_dict = json.loads(gpd.GeoSeries([geom]).to_json())["features"][0]["geometry"]

        feat = {
            "type": "Feature",
            "geometry": geom_dict,
            "properties": {
                "STATEFP": statefp,
                "state": state_abbr,
                "state_name": state_name,
                "initiative_count": init_count,
                "sen1_bioguide_id": sen1.get("bioguide_id", ""),
                "sen1_name": sen1.get("name", ""),
                "sen1_party": sen1.get("party", ""),
                "hr40_sen1_position": sen1.get("hr40_position", ""),
                "sen2_bioguide_id": sen2.get("bioguide_id", ""),
                "sen2_name": sen2.get("name", ""),
                "sen2_party": sen2.get("party", ""),
                "hr40_sen2_position": sen2.get("hr40_position", ""),
                "delegation_alignment": alignment,
            },
        }
        features.append(feat)

    out_fc = {"type": "FeatureCollection", "features": features}
    tmp = tempfile.NamedTemporaryFile(
        suffix="_states.geojson", delete=False, mode="w", encoding="utf-8"
    )
    json.dump(out_fc, tmp, ensure_ascii=False)
    tmp.close()
    return tmp.name


# ---------------------------------------------------------------------------
# Relates configuration
# ---------------------------------------------------------------------------

def _configure_relates(session: AGOLSession, points_item_id: str,
                        legislators_item_id: str) -> None:
    """
    Add three ArcGIS Online Relates from Layer 1 to Layer 2 via Bioguide ID.

    AGOL REST does not have a single-call 'setRelates' endpoint.
    We update the item's layerDefinitions via PATCH on the feature service.
    """
    # Get the service URL for Layer 1
    resp = session.get(f"{_portal_base(session._settings)}/content/items/{points_item_id}")
    service_url = resp.json().get("url", "").rstrip("/")
    if not service_url:
        logger.warning("Could not get service URL for points item; skipping relates config")
        return

    # Get Layer 2 service URL
    resp2 = session.get(f"{_portal_base(session._settings)}/content/items/{legislators_item_id}")
    leg_url = resp2.json().get("url", "").rstrip("/")
    if not leg_url:
        logger.warning("Could not get service URL for legislators item; skipping relates config")
        return

    relates = [
        {
            "id": 0,
            "name": "House Representative",
            "relatedTableId": 0,
            "primaryKey": "rep_bioguide_id",
            "foreignKey": "bioguide_id",
            "relatedLayerUrl": f"{leg_url}/FeatureServer/0",
            "cardinality": "esriRelCardinalityOneToMany",
            "composite": False,
        },
        {
            "id": 1,
            "name": "Senior Senator",
            "relatedTableId": 0,
            "primaryKey": "sen1_bioguide_id",
            "foreignKey": "bioguide_id",
            "relatedLayerUrl": f"{leg_url}/FeatureServer/0",
            "cardinality": "esriRelCardinalityOneToMany",
            "composite": False,
        },
        {
            "id": 2,
            "name": "Junior Senator",
            "relatedTableId": 0,
            "primaryKey": "sen2_bioguide_id",
            "foreignKey": "bioguide_id",
            "relatedLayerUrl": f"{leg_url}/FeatureServer/0",
            "cardinality": "esriRelCardinalityOneToMany",
            "composite": False,
        },
    ]

    admin_url = service_url.replace("/rest/services/", "/rest/admin/services/")
    update_url = f"{admin_url}/FeatureServer/updateDefinition"

    resp3 = session.post(
        update_url,
        data={"updateDefinition": json.dumps({"relationships": relates})},
    )
    result = resp3.json()
    logger.info("Relates configuration result: %s", result)


# ---------------------------------------------------------------------------
# Publish or overwrite a single layer
# ---------------------------------------------------------------------------

def _publish_or_overwrite(
    session: AGOLSession,
    title: str,
    item_id_field: str,
    current_item_id: str,
    source_path: str,
    item_type: str,
    file_type: str,
    new_feature_count: int,
    min_ratio: float,
    is_table: bool = False,
) -> str:
    """
    Create (first run) or overwrite (subsequent runs) an AGOL layer.
    Returns the item ID.
    """
    tags = "reparations, initiatives, congress, equity"
    description = f"Reparations Tracker — {title}"

    if not current_item_id:
        # First run: create item and publish
        logger.info("First run: creating '%s'", title)
        upload_id = _add_item(
            session, title, item_type, tags, description, file_path=source_path
        )
        logger.info("Uploaded item %s for '%s'", upload_id, title)

        service_item_id = _publish_item(session, upload_id, title, file_type)
        logger.info("Published '%s' as service item %s", title, service_item_id)
        return service_item_id

    # Subsequent runs: overwrite data in place
    logger.info("Overwriting existing layer '%s' (item %s)", title, current_item_id)
    service_url = _get_service_url(session, current_item_id)

    _safety_check(service_url, session, new_feature_count, min_ratio)

    _truncate_layer(service_url, session)
    logger.info("Truncated existing features for '%s'", title)

    if is_table:
        features = _json_to_agol_features(source_path)
    else:
        features = _geojson_to_agol_features(source_path)

    _append_features(service_url, session, features)
    logger.info("Appended %d features to '%s'", len(features), title)

    return current_item_id


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def run(settings) -> None:
    """
    Publish all four ArcGIS Online layers.
    Reads credentials from environment variables (set by notebook Cell 3).
    Writes item IDs back to config/settings.py after first-run creation.
    """
    username = os.environ.get("AGOL_USERNAME", "")
    password = os.environ.get("AGOL_PASSWORD", "")
    if not username or not password:
        raise ValueError(
            "AGOL_USERNAME and AGOL_PASSWORD environment variables must be set.\n"
            "In Colab: add them as secrets and run Cell 3 before this cell."
        )

    session = AGOLSession(username, password, settings)

    # --- Count new records for safety checks ---
    with open(settings.ENRICHED_DATA_PATH, encoding="utf-8") as fh:
        fc = json.load(fh)
    points_count = len(fc.get("features", []))

    with open(settings.LEGISLATORS_PATH, encoding="utf-8") as fh:
        legislators = json.load(fh)
    legislators_count = len(legislators)

    # Build Layer 3 and 4 GeoJSON in memory
    logger.info("Building congressional district layer...")
    districts_path = _build_districts_geojson(
        settings, settings.ENRICHED_DATA_PATH, settings.LEGISLATORS_PATH
    )
    with open(districts_path, encoding="utf-8") as fh:
        districts_count = len(json.load(fh).get("features", []))

    logger.info("Building state senate layer...")
    states_path = _build_states_geojson(
        settings, settings.ENRICHED_DATA_PATH, settings.LEGISLATORS_PATH
    )
    with open(states_path, encoding="utf-8") as fh:
        states_count = len(json.load(fh).get("features", []))

    min_ratio = settings.MIN_RECORD_RATIO

    # --- Layer 1: Initiatives Points ---
    points_id = _publish_or_overwrite(
        session=session,
        title="Reparations_Initiatives_Points",
        item_id_field="AGOL_POINTS_ITEM_ID",
        current_item_id=settings.AGOL_POINTS_ITEM_ID,
        source_path=settings.ENRICHED_DATA_PATH,
        item_type="GeoJson",
        file_type="GeoJson",
        new_feature_count=points_count,
        min_ratio=min_ratio,
    )
    if not settings.AGOL_POINTS_ITEM_ID:
        _write_item_id_to_settings("AGOL_POINTS_ITEM_ID", points_id)

    # --- Layer 2: Legislators Summary (table, no geometry) ---
    leg_id = _publish_or_overwrite(
        session=session,
        title="Reparations_Legislators_Summary",
        item_id_field="AGOL_LEGISLATORS_ITEM_ID",
        current_item_id=settings.AGOL_LEGISLATORS_ITEM_ID,
        source_path=settings.LEGISLATORS_PATH,
        item_type="CSV",
        file_type="CSV",
        new_feature_count=legislators_count,
        min_ratio=min_ratio,
        is_table=True,
    )
    if not settings.AGOL_LEGISLATORS_ITEM_ID:
        _write_item_id_to_settings("AGOL_LEGISLATORS_ITEM_ID", leg_id)

    # --- Configure Relates (Layer 1 → Layer 2) ---
    if points_id and leg_id:
        _configure_relates(session, points_id, leg_id)

    # --- Layer 3: Congressional Districts ---
    districts_id = _publish_or_overwrite(
        session=session,
        title="Reparations_Congressional_Districts",
        item_id_field="AGOL_DISTRICTS_ITEM_ID",
        current_item_id=settings.AGOL_DISTRICTS_ITEM_ID,
        source_path=districts_path,
        item_type="GeoJson",
        file_type="GeoJson",
        new_feature_count=districts_count,
        min_ratio=min_ratio,
    )
    if not settings.AGOL_DISTRICTS_ITEM_ID:
        _write_item_id_to_settings("AGOL_DISTRICTS_ITEM_ID", districts_id)

    # --- Layer 4: State Senate Layer ---
    states_id = _publish_or_overwrite(
        session=session,
        title="Reparations_Senate_States",
        item_id_field="AGOL_STATES_ITEM_ID",
        current_item_id=settings.AGOL_STATES_ITEM_ID,
        source_path=states_path,
        item_type="GeoJson",
        file_type="GeoJson",
        new_feature_count=states_count,
        min_ratio=min_ratio,
    )
    if not settings.AGOL_STATES_ITEM_ID:
        _write_item_id_to_settings("AGOL_STATES_ITEM_ID", states_id)

    # Clean up temp files
    import os as _os  # noqa: PLC0415
    for p in [districts_path, states_path]:
        try:
            _os.unlink(p)
        except Exception:  # noqa: BLE001
            pass

    logger.info("Publish complete.")
    logger.info("  Layer 1 (Points):       %s", points_id)
    logger.info("  Layer 2 (Legislators):  %s", leg_id)
    logger.info("  Layer 3 (Districts):    %s", districts_id)
    logger.info("  Layer 4 (States):       %s", states_id)


if __name__ == "__main__":
    import config.settings as settings

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    run(settings)
