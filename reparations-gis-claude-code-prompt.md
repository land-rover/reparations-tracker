# Claude Code Prompt: Reparations Initiatives — ArcGIS Online Pipeline

## Project Overview

Build a repeatable, end-to-end data pipeline that:
1. Ingests the reparations initiatives table from **https://www.reparationsresources.com/table**
2. Geocodes each initiative and resolves the U.S. House Representative and both U.S. Senators for its location, keyed by **Bioguide ID**
3. Publishes four ArcGIS Online layers that enable presentation and analysis of the legislators who represent these communities

The pipeline runs as a Google Colab notebook (`notebook/reparations_pipeline.ipynb`). Each functional module — data ingestion, enrichment, and ArcGIS Online publishing — is implemented as a set of clearly delimited notebook cells that can be run in sequence or individually. Persistent data (TIGER cache, geocode cache, and intermediate files) is stored on Google Drive, mounted at the start of each session. Secrets are managed via Colab Secrets (`google.colab.userdata`). The pipeline is re-run on demand by opening the notebook and executing all cells.

---

## Layer Architecture

| # | Name | Type | Geometry | One record per |
|---|------|------|----------|----------------|
| 1 | `Reparations_Initiatives_Points` | Feature Layer | Point | Initiative × Congressional district intersection |
| 2 | `Reparations_Legislators_Summary` | Table (no geometry) | None | Legislator (both chambers) |
| 3 | `Reparations_Congressional_Districts` | Feature Layer | Polygon | House district |
| 4 | `Reparations_Senate_States` | Feature Layer | Polygon | State |

**Data model note:** Because cities frequently span multiple Congressional districts, Layer 1 uses a **full intersection approach**. Each initiative is split into one point feature per overlapping Congressional district, with each point placed at the centroid of the intersection area between the city boundary and the district polygon. All initiative attributes repeat across the split features; an `initiative_id` field groups them back into a logical initiative. This means a city with 8 overlapping districts produces 8 point features, each cleanly inside one district with one unambiguous `rep_bioguide_id`.

**Join key:** Bioguide ID links Layer 1 to Layer 2 via ArcGIS Online Relates:
- Layer 1 carries `initiative_id`, `rep_bioguide_id`, `sen1_bioguide_id`, `sen2_bioguide_id`
- Layer 2 has `bioguide_id` as its primary key
- Each point feature relates to three Layer 2 rows (one rep + two senators)
- `initiative_count` on Layer 2 for House members counts distinct `initiative_id` values — not raw feature count — to avoid double-counting a city that spans district boundaries

---

## Repository Structure

```
/
├── notebook/
│   └── reparations_pipeline.ipynb   # Main Colab notebook — all pipeline logic
├── ingest/
│   └── fetch_table.py               # Step 1: fetch/scrape the source table
├── enrich/
│   └── enrich_initiatives.py        # Step 2: geocode + congressional lookup
├── publish/
│   └── publish_to_agol.py           # Step 3: publish/update ArcGIS Online layers
├── config/
│   └── settings.py                  # Centralised config (URLs, field mappings, item IDs)
├── requirements.txt
└── README.md
```

**Note on module structure:** Each of `fetch_table.py`, `enrich_initiatives.py`, and `publish_to_agol.py` is written as an importable module with a clearly defined entry-point function (e.g., `run(config)`) so it can be called from a notebook cell with a single function call. No script should require CLI invocation (`if __name__ == "__main__"`) as its only execution path — that block may be retained for local development convenience but must not be the primary interface.

**Data directory:** All `data/` paths in `config/settings.py` resolve to a subdirectory of the mounted Google Drive (e.g., `"/content/drive/MyDrive/reparations_gis/data/"`). The notebook mounts Drive and sets this base path in its first cell. No data files are written to the Colab ephemeral filesystem or committed to the GitHub repo.

---

## Notebook Structure (`notebook/reparations_pipeline.ipynb`)

The notebook is organized into the following cells, in order:

**Cell 1 — Install dependencies.** Install all required packages into the Colab runtime:
```python
!pip install -q geopandas shapely playwright pandas requests beautifulsoup4
!playwright install chromium
```

**Cell 2 — Mount Google Drive and set data base path.** Mount Drive and set `REPARATIONS_DATA_BASE` so all modules resolve paths correctly:
```python
from google.colab import drive
drive.mount("/content/drive")
import os
os.environ["REPARATIONS_DATA_BASE"] = "/content/drive/MyDrive/reparations_gis/data"
os.makedirs(os.environ["REPARATIONS_DATA_BASE"], exist_ok=True)
```

**Cell 3 — Load secrets.** Retrieve all credentials from Colab Secrets:
```python
from google.colab import userdata
os.environ["CONGRESS_API_KEY"]      = userdata.get("CONGRESS_API_KEY")
os.environ["AGOL_USERNAME"]         = userdata.get("AGOL_USERNAME")
os.environ["AGOL_PASSWORD"]         = userdata.get("AGOL_PASSWORD")
os.environ["GOOGLE_CIVIC_API_KEY"]  = userdata.get("GOOGLE_CIVIC_API_KEY")
```
All four secrets are required; raise a clear `ValueError` with instructions if any are missing.

**Cell 4 — Clone repo and import modules.**
```python
!git clone https://github.com/{your-org}/reparations-gis /content/reparations-gis
import sys
sys.path.insert(0, "/content/reparations-gis")
from ingest.fetch_table import run as fetch
from enrich.enrich_initiatives import run as enrich
from publish.publish_to_agol import run as publish
import config.settings as settings
```

**Cell 5 — Step 1: Fetch table.**
```python
fetch(settings)
```

**Cell 6 — Step 2: Enrich initiatives.**
```python
enrich(settings)
```

**Cell 7 — Step 3: Publish to ArcGIS Online.**
```python
publish(settings)
```

Each cell is self-contained and can be re-run independently without re-running prior cells (assuming Drive is mounted and secrets are loaded). Add a markdown cell before each step cell describing what it does, its inputs, its outputs, and any warnings the user should expect (e.g., "This step downloads ~600 MB of TIGER/Line files on first run — subsequent runs load from Drive cache and are fast").

---

## Step 1 — Data Ingestion (`ingest/fetch_table.py`)

### Goal
Extract the full initiatives table from https://www.reparationsresources.com/table and save it as `{REPARATIONS_DATA_BASE}/initiatives_raw.json`.

### Approach (try in order; implement whichever works first)

1. **Inspect for a direct API/XHR endpoint.** Many JavaScript-rendered tables (ArcGIS Experience Builder, React, etc.) load data from a JSON REST endpoint. Use `requests` to probe likely ArcGIS REST API patterns (e.g., `/arcgis/rest/services/.../FeatureServer/0/query?where=1=1&outFields=*&f=json`). Check the page source and common ArcGIS URL patterns. If found, call it directly — this is the most robust and maintainable approach.

2. **Playwright browser automation.** If no direct endpoint is found, use `playwright` (headless Chromium) to fully render the page, wait for the table to populate, then extract data from the DOM. Use `page.wait_for_selector()` to ensure the table has loaded before extraction. Install with `playwright install chromium`.

3. **requests + BeautifulSoup.** Only if the page serves meaningful data in plain HTML.

Document the chosen approach in a comment at the top of `fetch_table.py`.

### Output schema (per initiative record)

Extract at minimum:
- `initiative_name` — name of the initiative or organization
- `city` — city where the initiative is based
- `state` — U.S. state (full name or abbreviation; normalise to 2-letter FIPS abbreviation)
- `initiative_type` — e.g., municipal, state, university, faith, private
- `status` — e.g., active, proposed, passed, complete
- `year` — year initiated or passed
- `description` — any narrative description present in the table
- `source_url` — URL to the initiative's own page if present
- Any other fields present in the source table

Write records to `{REPARATIONS_DATA_BASE}/initiatives_raw.json` as a JSON array.

---

## Step 2 — Enrichment (`enrich/enrich_initiatives.py`)

### Goal
For each initiative, add geocoordinates, House member attributes, and Senate attributes — all keyed by Bioguide ID. Then build a separate per-legislator summary.

### 2a — City Boundary Acquisition & District Intersection

**Do not geocode to a single centroid.** Instead, use city boundary polygons to capture all overlapping Congressional districts.

**Step 1 — Load city boundaries.** Use Census Bureau TIGER/Line Place shapefiles (incorporated city boundaries). Files are cached locally in `{REPARATIONS_DATA_BASE}/tiger_place/` as the original state `.zip` downloads on Google Drive, so downloads only occur once.

On each run, check whether `{REPARATIONS_DATA_BASE}/tiger_place/.complete` exists. If it does, load directly from the cached `.zip` files — do not re-download. If it does not exist, download all state Place files from `https://www2.census.gov/geo/tiger/TIGER2024/PLACE/`, save each as `tiger_place/tl_2024_{fips}_place.zip`, then write `tiger_place/.complete` as a sentinel. In both cases, read each `.zip` into GeoPandas using `geopandas.read_file()` with a `zip://` path — do not unzip to disk. Concatenate all state GeoDataFrames into a single GeoDataFrame before matching.

Match each initiative's `city` + `state` to the Place shapefile by `NAME` and state FIPS code (`STATEFP`).

If a city cannot be matched in the Place shapefile (e.g., unincorporated areas, name mismatches), fall back to geocoding the city to a centroid point using the **Census Bureau Geocoding API** (free, no key required):
- Endpoint: `https://geocoding.geo.census.gov/geocoder/locations/onelineaddress`
- Input: `"{city}, {state}"`
- Parse coordinates from `result.addressMatches[0].coordinates`
- If Census geocoder also fails, fall back to **Nominatim (OpenStreetMap)** (`https://nominatim.openstreetmap.org/search`) with a 1-second delay between requests
- For centroid fallbacks, treat the point as a degenerate polygon (the city spans exactly one district)

Record `geometry_source` as `"place_boundary"`, `"census_centroid"`, or `"nominatim_centroid"` on each feature.

**Step 2 — Intersect city boundaries with Congressional districts.** Load the 119th Congress district boundaries:

On each run, check whether `{REPARATIONS_DATA_BASE}/tiger_cd/tl_2024_us_cd119.zip` exists. If it does, load directly from the cached file. If not, download from `https://www2.census.gov/geo/tiger/TIGER2024/CD/tl_2024_us_cd119.zip`, save to `tiger_cd/`, then load. Read directly from the `.zip` using `geopandas.read_file()` — do not unzip to disk.

Use `geopandas.overlay(city_gdf, districts_gdf, how='intersection')` to compute the intersection of each city polygon with all overlapping district polygons. This produces one row per city×district pair.

**Step 3 — Place points at intersection centroids.** For each city×district intersection polygon, compute its centroid (`intersection_polygon.centroid`). This becomes the point geometry for that Layer 1 feature — it sits visually inside its district and is geographically meaningful.

**Step 4 — Assign `initiative_id`.** Generate a stable `initiative_id` for each source initiative (e.g., a slug: `"evanston-il-reparations-program"` derived from city + state + initiative name). All split features from the same initiative share the same `initiative_id`.

Cache city boundary lookups and intersection results in `{REPARATIONS_DATA_BASE}/geocode_cache.json` keyed by `"{city}, {state}"` to avoid redundant computation on subsequent runs. Load the existing cache at startup and write it back at the end of the run — even if no new entries were added.

### 2b — Congressional Lookup

Resolve the U.S. House Representative and both U.S. Senators for each initiative using **Congress.gov** as the primary source, with the Census Bureau shapefiles already loaded in Step 2a.

**House member lookup:**
Use the district polygons from `tiger_cd/tl_2024_us_cd119.zip` to determine which Congressional district each initiative's city boundary (or centroid fallback) falls in. For cities that span multiple districts, each intersection polygon already carries an unambiguous district assignment by construction. Read the `STATEFP` + `CD119FP` fields from the district polygon to construct the district identifier, then query Congress.gov:
- Endpoint: `https://api.congress.gov/v3/member/{stateCode}/{district}`
- Extract the current member: `bioguide_id`, `name`, `party`, `url`, `phone`

**Senate lookup:**
Query Congress.gov by state for current Senators:
- Endpoint: `https://api.congress.gov/v3/member/{stateCode}?chamber=senate&currentMember=true`
- Returns both Senators for the state; assign them as `sen1` and `sen2` in the order returned
- Extract for each: `bioguide_id`, `name`, `party`, `url`, `phone`

**Fallback — Google Civic Information API:**
If a Congress.gov call fails or returns no current member (e.g., a vacancy), fall back to the Google Civic Information API (key: env var `GOOGLE_CIVIC_API_KEY`):
- Endpoint: `https://www.googleapis.com/civicinfo/v2/representatives`
- Query by `address="{city}, {state}"`
- Extract officials where office contains `"U.S. Representative"` or `"U.S. Senator"`
- For each official capture: `name`, `party`, `phones[0]`, `urls[0]`, `photoUrl`
- Match to a Bioguide ID via Congress.gov member search by name and state

Log a WARNING whenever the Google Civic fallback is invoked, noting the initiative and the reason the primary path failed.

**Caching:** Cache all Congress.gov member lookups keyed by `"{stateCode}-{district}"` for House members and `"{stateCode}-senate"` for Senate pairs. This avoids redundant API calls when multiple initiatives share a state or district.

### 2c — H.R. 40 Co-sponsorship Status

For each legislator, retrieve their co-sponsorship status on **H.R. 40** (the Commission to Study and Develop Reparation Proposals for African Americans Act) from the Congress.gov API:
- Endpoint: `https://api.congress.gov/v3/bill/119/hr/40/cosponsors` (and equivalent endpoint for prior Congresses if the member was not in the 119th)
- If the legislator's `bioguide_id` appears in the cosponsor list, set `hr40_position` to `"Co-sponsor"`
- Otherwise set `hr40_position` to `"No co-sponsorship recorded"`

Store `hr40_position` on both the enriched point features (as `hr40_rep_position`, `hr40_sen1_position`, `hr40_sen2_position`) and on the legislators summary record.

Cache cosponsor list results to avoid redundant API calls across legislators.

### 2d — Output: Enriched Points (`data/initiatives_enriched.geojson`)

Write a GeoJSON FeatureCollection. Each Feature represents one initiative×district intersection:
- `geometry`: `{"type": "Point", "coordinates": [longitude, latitude]}` — centroid of the intersection polygon
- `properties`: all Step 1 fields plus:
  - `initiative_id` — stable slug grouping all split features from the same source initiative
  - `rep_bioguide_id`, `rep_name`, `rep_party`, `rep_district`, `rep_phone`, `rep_url`
  - `sen1_bioguide_id`, `sen1_name`, `sen1_party`, `sen1_phone`, `sen1_url`
  - `sen2_bioguide_id`, `sen2_name`, `sen2_party`, `sen2_phone`, `sen2_url`
  - `hr40_rep_position`, `hr40_sen1_position`, `hr40_sen2_position`
  - `geometry_source` — `"place_boundary"`, `"census_centroid"`, or `"nominatim_centroid"`
  - `last_updated` (ISO 8601)

### 2e — Output: Legislators Summary (`data/legislators_summary.json`)

Build a deduplicated list of all legislators represented across all initiatives. One record per legislator:

```json
{
  "bioguide_id": "S000033",
  "name": "Bernard Sanders",
  "chamber": "Senate",
  "party": "Independent",
  "state": "VT",
  "district": null,
  "initiative_count": 3,
  "hr40_position": "Co-sponsor",
  "phone": "202-224-5141",
  "url": "https://www.sanders.senate.gov",
  "photo_url": "https://..."
}
```

Notes:
- `chamber`: `"House"` or `"Senate"`
- `district`: House district number (integer) for House members; `null` for Senators
- `initiative_count`: for House members, count of **distinct `initiative_id` values** in Layer 1 where `rep_bioguide_id` matches — not raw feature count, to avoid inflating counts for cities split across multiple districts; for Senators, count of distinct `initiative_id` values where `sen1_bioguide_id` or `sen2_bioguide_id` matches

### 2f — State Senate Delegation Alignment

For each state that has at least one initiative, derive a `delegation_alignment` field:
- `"Both Democratic"` — both senators are Democrats
- `"Both Republican"` — both senators are Republicans
- `"Split"` — senators are from different parties (including Independent caucusing with either)
- `"Both Independent"` — edge case; handle explicitly

This field drives the choropleth color scheme on Layer 4.

---

## Step 3 — ArcGIS Online Publishing (`publish/publish_to_agol.py`)

### Authentication

Authenticate against ArcGIS Online using direct REST API calls via `requests`. Obtain a token using the generate token endpoint:
- Endpoint: `https://www.arcgis.com/sharing/rest/generateToken`
- POST parameters: `username`, `password`, `referer`, `f=json`
- Store the returned token in memory for the duration of the run; refresh if it expires mid-run (default lifetime is 120 minutes)
- Never use the `arcgis` Python package unless a specific publishing operation cannot be accomplished via the REST API after a good-faith attempt

All subsequent ArcGIS Online operations use this token appended as `?token={token}` (or in the POST body) on standard ArcGIS REST endpoints.

### ArcGIS Online Trash Behavior

When an ArcGIS Online item is deleted, it is moved to the user's Trash and is not permanently removed until explicitly purged. A new item with the same name **cannot** be created until the previous item has been permanently deleted from Trash. This affects any code path that deletes and recreates items.

To guard against name conflicts from previously trashed items, always purge Trash as part of any delete operation:
1. Delete the item:
   - Endpoint: `https://www.arcgis.com/sharing/rest/content/users/{username}/items/{itemId}/delete`
   - POST with `f=json`
2. Immediately purge the user's Trash:
   - Endpoint: `https://www.arcgis.com/sharing/rest/content/users/{username}/deleteItems`
   - POST with `items={itemId}` (the same item just deleted), `f=json`

Never assume a deleted item is gone until the purge call returns successfully. If a first-run item creation fails with a name conflict error, check Trash for a matching item name, purge it, and retry creation before raising an error.

### Publishing Logic

**Idempotency rule:** Store each published layer's ArcGIS item ID in `config/settings.py`. On first run (item ID is blank), create the item and hosted layer via the REST API and write the returned item ID back to `settings.py`. On subsequent runs, overwrite the existing layer's data in place — do not delete and recreate items, as this breaks any maps or dashboards referencing them by item ID.

**First run — item creation:**
1. Upload the source file (GeoJSON for Layers 1, 3, 4; JSON for Layer 2) as a new item:
   - Endpoint: `https://www.arcgis.com/sharing/rest/content/users/{username}/addItem`
   - POST with `file` (multipart), `type=GeoJson` (or `type=Feature Service` as appropriate), `title`, `tags`, `f=json`
2. Publish the uploaded item as a hosted feature layer:
   - Endpoint: `https://www.arcgis.com/sharing/rest/content/users/{username}/publish`
   - POST with `itemid`, `publishParameters` (JSON with `name`, `layerInfo`), `f=json`
3. Poll the publishing job status until complete:
   - Endpoint: `https://www.arcgis.com/sharing/rest/content/users/{username}/items/{itemId}/status`
   - Retry with exponential backoff until `status == "completed"` or timeout after 10 minutes
4. Write the returned item ID to `config/settings.py`

**Subsequent runs — data overwrite:**
1. Truncate the existing feature layer:
   - Endpoint: `https://services.arcgis.com/{orgId}/arcgis/rest/services/{serviceName}/FeatureServer/0/deleteFeatures`
   - POST with `where=1=1`, `f=json`
2. Append new features in batches of 2,000:
   - Endpoint: `.../FeatureServer/0/addFeatures`
   - POST with `features` (JSON array), `f=json`

**Layer 2 (table, no geometry):** Follow the same create/overwrite pattern but use `type=CSV` on first publish and the equivalent table FeatureServer endpoint for subsequent overwrites.

**Safety check:** Before truncating any existing layer, query its current feature count:
- Endpoint: `.../FeatureServer/0/query?where=1=1&returnCountOnly=true&f=json`
- If the new dataset has fewer than `MIN_RECORD_RATIO` (80%) of the existing count, abort, log an error, and raise an exception so the notebook cell fails visibly. Do not truncate.

---

### Layer 1 — Initiatives Point Layer

- **Name**: `Reparations_Initiatives_Points`
- **Source**: `{REPARATIONS_DATA_BASE}/initiatives_enriched.geojson`
- **Fields**: all enriched properties from Step 2d
- **Relates**: configure three Relates to Layer 2 (`Reparations_Legislators_Summary`):
  - `rep_bioguide_id` → `bioguide_id` (label: "House Representative")
  - `sen1_bioguide_id` → `bioguide_id` (label: "Senior Senator")
  - `sen2_bioguide_id` → `bioguide_id` (label: "Junior Senator")
- **Popup template**: initiative name, type, status, year, description, rep name/party, both senator names/parties, H.R. 40 co-sponsorship positions

---

### Layer 2 — Legislators Summary Table

- **Name**: `Reparations_Legislators_Summary`
- **Type**: Hosted table (no geometry)
- **Source**: `{REPARATIONS_DATA_BASE}/legislators_summary.json`
- **Primary key**: `bioguide_id`
- **Fields**: all fields from Step 2e schema above
- **Purpose**: non-spatial analytical layer; joined to Layers 1, 3, and 4 via Bioguide ID for dashboards and ranked lists

---

### Layer 3 — Congressional District Polygons

- **Name**: `Reparations_Congressional_Districts`
- **Source geometry**: Census Bureau TIGER/Line 119th Congress district boundaries — loaded from `{REPARATIONS_DATA_BASE}/tiger_cd/tl_2024_us_cd119.zip`. This file was already downloaded and cached in Step 2a; do not re-download.
- **Enrichment**: aggregate Layer 1 features per district. Because points in `initiatives_enriched.geojson` already sit inside exactly one district (by construction), a simple group-by on `rep_bioguide_id` / `rep_district` is sufficient — no additional spatial join needed:
  - `initiative_count` — count of **distinct `initiative_id` values** per district (not raw feature count)
  - Join House member attributes from `legislators_summary.json` by `rep_bioguide_id`:
    - `rep_bioguide_id`, `rep_name`, `rep_party`, `rep_district`, `hr40_rep_position`, `rep_phone`, `rep_url`
- **Choropleth fields**:
  - Symbology field 1: `initiative_count` (graduated color — count of initiatives)
  - Symbology field 2: `rep_party` (unique value — Blue=Democrat, Red=Republican, Grey=Independent)
- **Popup**: district number, rep name, party, initiative count, H.R. 40 co-sponsorship position, phone, URL
- **Join to Layer 2**: `rep_bioguide_id` → `bioguide_id`

---

### Layer 4 — State Senate Layer

- **Name**: `Reparations_Senate_States`
- **Source geometry**: Census Bureau TIGER/Line state boundaries. On each run, check whether `{REPARATIONS_DATA_BASE}/tiger_state/tl_2024_us_state.zip` exists. If it does, load directly from the cached file. If not, download from `https://www2.census.gov/geo/tiger/TIGER2024/STATE/tl_2024_us_state.zip`, save to `tiger_state/`, then load. Read directly from the `.zip` using `geopandas.read_file()` — do not unzip to disk.
- **Enrichment**: aggregate initiative data per state from `initiatives_enriched.geojson`:
  - `initiative_count` — count of initiatives in the state
  - Join both senators' attributes from `legislators_summary.json` by state:
    - `sen1_bioguide_id`, `sen1_name`, `sen1_party`, `hr40_sen1_position`
    - `sen2_bioguide_id`, `sen2_name`, `sen2_party`, `hr40_sen2_position`
  - `delegation_alignment` — derived field from Step 2f
- **Choropleth fields**:
  - Symbology field 1: `initiative_count` (graduated color — count of initiatives)
  - Symbology field 2: `delegation_alignment` (unique value, 3-category color scheme):
    - Blue = Both Democratic
    - Red = Both Republican
    - Purple = Split
    - Grey = Both Independent (edge case)
- **Popup**: state name, initiative count, both senator names/parties, both H.R. 40 co-sponsorship positions
- **Joins to Layer 2**: `sen1_bioguide_id` → `bioguide_id` and `sen2_bioguide_id` → `bioguide_id`

---

## `requirements.txt`

```
requests
beautifulsoup4
playwright
geopandas
shapely
pandas
```

When running in Google Colab, dependencies are installed directly in Cell 1 of the notebook. `requirements.txt` is provided for local development reference only and is not used by the notebook.

---

## `config/settings.py`

```python
import os

# Base data directory — resolves to Google Drive when running in Colab
DATA_BASE = os.environ.get("REPARATIONS_DATA_BASE", "data")

# Data paths
GEOCODE_CACHE_PATH     = f"{DATA_BASE}/geocode_cache.json"
RAW_DATA_PATH          = f"{DATA_BASE}/initiatives_raw.json"
ENRICHED_DATA_PATH     = f"{DATA_BASE}/initiatives_enriched.geojson"
LEGISLATORS_PATH       = f"{DATA_BASE}/legislators_summary.json"

# TIGER/Line cache paths
TIGER_PLACE_CACHE_DIR  = f"{DATA_BASE}/tiger_place/"
TIGER_CD_CACHE_PATH    = f"{DATA_BASE}/tiger_cd/tl_2024_us_cd119.zip"
TIGER_STATE_CACHE_PATH = f"{DATA_BASE}/tiger_state/tl_2024_us_state.zip"

# TIGER/Line source URLs
TIGER_PLACE_URL  = "https://www2.census.gov/geo/tiger/TIGER2024/PLACE/"
TIGER_CD_URL     = "https://www2.census.gov/geo/tiger/TIGER2024/CD/tl_2024_us_cd119.zip"
TIGER_STATE_URL  = "https://www2.census.gov/geo/tiger/TIGER2024/STATE/tl_2024_us_state.zip"

SOURCE_URL = "https://www.reparationsresources.com/table"

# ArcGIS Online item IDs — populated after first publish run; do not edit manually
AGOL_POINTS_ITEM_ID      = ""   # Layer 1: Reparations_Initiatives_Points
AGOL_LEGISLATORS_ITEM_ID = ""   # Layer 2: Reparations_Legislators_Summary
AGOL_DISTRICTS_ITEM_ID   = ""   # Layer 3: Reparations_Congressional_Districts
AGOL_STATES_ITEM_ID      = ""   # Layer 4: Reparations_Senate_States

# Safety threshold: abort publish if new record count < this fraction of previous count
MIN_RECORD_RATIO = 0.80
```

---

## Implementation Notes & Guardrails

- **Idempotency**: Every module must be safe to re-run without duplicating data or ArcGIS content. Use item IDs in `config/settings.py` to update rather than recreate. After first publish, write the returned item IDs back to `settings.py`.
- **Error handling**: Wrap each initiative's geocode and civic lookup in `try/except`. Log failures to `{REPARATIONS_DATA_BASE}/errors.log` and continue. Never let a single failed record abort a full run.
- **Rate limiting**: 1-second delay between Nominatim calls; exponential backoff for Civic and Congress.gov API calls.
- **Secrets**: Never hardcode credentials. All API keys and ArcGIS credentials are read from `os.environ` only. Modules must not import from `google.colab` directly — secrets are injected as environment variables by the notebook so that modules remain usable outside Colab if needed.
- **Safety check**: Before truncating any ArcGIS layer, compare new record count to previous. If below `MIN_RECORD_RATIO`, abort publish and raise an exception so the notebook cell fails visibly.
- **Logging**: Use Python's `logging` module throughout. INFO for normal progress, WARNING for fallbacks (e.g., Nominatim used instead of Census, Google Civic fallback invoked), ERROR for failures.
- **Party color consistency**: Use consistent party color values across Layers 3 and 4 — Blue for Democrat, Red for Republican, Grey for Independent — so maps read consistently side by side. Layer 4 uses the 3-category `delegation_alignment` scheme (Blue / Red / Purple=Split, Grey=Both Independent) which is visually compatible.
- **ArcGIS REST API over SDK**: All ArcGIS Online operations use direct REST calls via `requests`. The `arcgis` Python package is not used. If a future requirement cannot be met via the REST API, prefer `requests` with raw JSON payloads before reaching for the SDK. Document any exception in a comment in `publish_to_agol.py`.
- **ArcGIS Trash**: Deleted ArcGIS Online content is not permanently removed until explicitly purged from Trash. Any delete operation must be immediately followed by a Trash purge call on the same item ID. Name conflicts on item creation are most likely caused by an item lingering in Trash — handle this case explicitly with a purge-and-retry rather than raising a hard error.
- **Colab filesystem vs. Drive**: Never write persistent data to the Colab ephemeral filesystem (`/content/` outside of `/content/drive/`). All `data/` paths must resolve through `REPARATIONS_DATA_BASE` to a Google Drive location. Temporary files needed only within a single cell (e.g., a downloaded zip before reading into GeoPandas) may use `/tmp/` but must not be assumed to persist across cells or sessions.
- **TIGER/Line cache**: `tiger_place/`, `tiger_cd/`, and `tiger_state/` are stored on Google Drive under `REPARATIONS_DATA_BASE` and persist across Colab sessions. All files are read directly from `.zip` without unzipping to disk. The Place cache uses a `.complete` sentinel (only written after all 50 state files download successfully) to guard against partial downloads; the single-file CD and State downloads are considered complete if the `.zip` is present. To force a full re-download of any cache (e.g., after a Census vintage year update), delete the relevant directory from Drive and re-run the enrichment cell.
- **Geocode cache persistence**: `geocode_cache.json` is stored on Google Drive and persists across Colab sessions. Load the existing cache at startup and write it back at the end of each enrichment run — even if no new entries were added. New initiatives are geocoded on first encounter; existing entries are never re-fetched unless manually deleted from the cache.
- **README**: Document all required Colab Secrets, how to open and run the notebook, how to run each module locally, a description of each ArcGIS layer and its fields, the join/relate structure between layers, the intersection approach, and the Google Drive directory structure.

---

## Deliverables Checklist

When complete, the following must exist and work end-to-end:

- [ ] `notebook/reparations_pipeline.ipynb` runs all seven cells in sequence in Google Colab without errors, producing all four ArcGIS Online layers
- [ ] `ingest/fetch_table.py` exposes a `run(settings)` entry point; extracts the table and writes `initiatives_raw.json` to Google Drive
- [ ] `enrich/enrich_initiatives.py` exposes a `run(settings)` entry point; produces valid GeoJSON where each feature is an initiative×district intersection point, carrying `initiative_id`, `rep_bioguide_id`, and senator Bioguide IDs; plus `legislators_summary.json` with one record per legislator and correct `initiative_count` (distinct `initiative_id` values) and `delegation_alignment` values
- [ ] Cities that span multiple Congressional districts produce multiple point features, each inside one district, with the same `initiative_id`
- [ ] Cities not found in TIGER Place shapefiles fall back gracefully to centroid geocoding with `geometry_source` recorded
- [ ] TIGER/Line Place cache is populated on first run, persisted to Google Drive, and reused on all subsequent runs without re-downloading; a `.complete` sentinel is present when the cache is valid
- [ ] Congressional district and state boundary `.zip` files are cached to Google Drive and reused on subsequent runs
- [ ] `geocode_cache.json` is loaded at startup, updated during the run, and written back to Google Drive at completion
- [ ] `publish/publish_to_agol.py` exposes a `run(settings)` entry point; creates or overwrites all four ArcGIS Online layers using direct REST API calls; item IDs are written back to `config/settings.py`
- [ ] Layer 1 has three configured Relates to Layer 2 via Bioguide ID
- [ ] Layer 3 `initiative_count` counts distinct `initiative_id` values, not raw features
- [ ] Layer 3 supports a choropleth by `initiative_count` and by `rep_party`
- [ ] Layer 4 supports a choropleth by `initiative_count` and by `delegation_alignment` (Both Democratic / Both Republican / Split)
- [ ] Safety check prevents a bad scrape from overwriting good ArcGIS data
- [ ] ArcGIS Trash is purged after any delete operation; name-conflict errors on first-run item creation trigger a purge-and-retry
- [ ] `README.md` documents Colab Secrets setup, the Google Drive directory structure, how to open and run the notebook, layer architecture, the intersection approach, and the join/relate structure between layers
