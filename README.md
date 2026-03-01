# Reparations Tracker — GIS Pipeline

An end-to-end data pipeline that ingests the reparations initiatives table from
[reparationsresources.com/table](https://www.reparationsresources.com/table),
enriches each initiative with geocoordinates and congressional representative data,
and publishes four ArcGIS Online layers for presentation and analysis.

---

## Table of Contents

1. [Quick Start](#quick-start)
2. [Colab Secrets Setup](#colab-secrets-setup)
3. [Google Drive Directory Structure](#google-drive-directory-structure)
4. [Running the Notebook](#running-the-notebook)
5. [Running Modules Locally](#running-modules-locally)
6. [ArcGIS Layer Architecture](#arcgis-layer-architecture)
7. [Intersection Approach](#intersection-approach)
8. [Join / Relate Structure](#join--relate-structure)
9. [Pipeline Re-run Behavior](#pipeline-re-run-behavior)
10. [Repository Structure](#repository-structure)

---

## Quick Start

1. Open [notebook/reparations_pipeline.ipynb](notebook/reparations_pipeline.ipynb) in Google Colab
2. Configure the four required Colab Secrets (see [Colab Secrets Setup](#colab-secrets-setup))
3. Edit `REPO_URL` in Cell 4 to point to your fork
4. Run all cells top-to-bottom (Runtime → Run all)

---

## Colab Secrets Setup

The notebook reads all credentials from Colab Secrets (the key icon in the left sidebar).
No credentials are ever hardcoded in code or committed to the repository.

| Secret Name | Description | Where to get it |
|-------------|-------------|-----------------|
| `CONGRESS_API_KEY` | Congress.gov REST API key | [api.congress.gov](https://api.congress.gov/sign-up/) — free |
| `AGOL_USERNAME` | ArcGIS Online username | Your ArcGIS Online account |
| `AGOL_PASSWORD` | ArcGIS Online password | Your ArcGIS Online account |
| `GOOGLE_CIVIC_API_KEY` | Google Civic Information API key | [console.cloud.google.com](https://console.cloud.google.com) — free tier |

**To add a secret in Colab:**
1. Click the key icon in the left sidebar
2. Click **Add new secret**
3. Enter the secret name (exactly as shown above) and its value
4. Toggle **Notebook access** to enabled
5. Re-run Cell 3

`GOOGLE_CIVIC_API_KEY` is only invoked as a fallback when Congress.gov lookups fail
(e.g., during a vacancy). The pipeline logs a `WARNING` whenever the Civic API is used.

---

## Google Drive Directory Structure

All persistent data is written to your Google Drive under:

```
My Drive/
└── reparations_gis/
    └── data/
        ├── initiatives_raw.json          # Step 1 output: scraped table
        ├── initiatives_enriched.geojson  # Step 2 output: enriched points
        ├── legislators_summary.json      # Step 2 output: legislator records
        ├── geocode_cache.json            # City boundary cache (persists across sessions)
        ├── errors.log                    # Per-initiative enrichment errors
        ├── tiger_place/                  # TIGER/Line Place shapefiles (state zips)
        │   ├── tl_2024_01_place.zip      # Alabama
        │   ├── tl_2024_02_place.zip      # Alaska
        │   ├── ...                       # (one zip per state)
        │   └── .complete                 # Sentinel — all 50 states downloaded
        ├── tiger_cd/
        │   └── tl_2024_us_cd119.zip      # 119th Congress district boundaries
        └── tiger_state/
            └── tl_2024_us_state.zip      # US state boundaries
```

**To force a full TIGER/Line re-download** (e.g., after a Census vintage year update):
delete the relevant directory from Drive and re-run the enrichment cell.

**To force re-geocoding** of a specific city: remove its entry from `geocode_cache.json`
and re-run the enrichment cell.

---

## Running the Notebook

Open [notebook/reparations_pipeline.ipynb](notebook/reparations_pipeline.ipynb) in
Google Colab. Execute cells in order:

| Cell | Purpose | Expected Duration |
|------|---------|-------------------|
| 1 | Install dependencies | 1–3 min (first run) |
| 2 | Mount Google Drive | < 1 min |
| 3 | Load Colab Secrets | < 1 min |
| 4 | Clone repo + import modules | < 1 min |
| **Step 1** | Fetch initiatives table | 10–60 sec |
| **Step 2** | Enrich with geocoding + congressional data | 15–30 min (first run); 3–10 min (subsequent) |
| **Step 3** | Publish to ArcGIS Online | 5–15 min |

Cells 1–4 are setup cells. Step cells (1, 2, 3) can be re-run independently
without repeating the setup cells, as long as Drive remains mounted and
secrets are still in the environment.

---

## Running Modules Locally

Install dependencies:
```bash
pip install -r requirements.txt
playwright install chromium
```

Set environment variables:
```bash
export REPARATIONS_DATA_BASE="./data"
export CONGRESS_API_KEY="your_key"
export AGOL_USERNAME="your_username"
export AGOL_PASSWORD="your_password"
export GOOGLE_CIVIC_API_KEY="your_key"
```

Run each step:
```bash
python -m ingest.fetch_table
python -m enrich.enrich_initiatives
python -m publish.publish_to_agol
```

Or import and call the `run(settings)` entry point from Python:
```python
import config.settings as settings
from ingest.fetch_table import run as fetch
from enrich.enrich_initiatives import run as enrich
from publish.publish_to_agol import run as publish

fetch(settings)
enrich(settings)
publish(settings)
```

---

## ArcGIS Layer Architecture

### Layer 1 — `Reparations_Initiatives_Points`
**Type:** Feature Layer (Point geometry)

One point feature per **initiative × congressional district intersection**.
Cities that span multiple House districts produce multiple points (one per district),
each placed at the centroid of the intersection area between the city boundary and
the district polygon. All points from the same source initiative share the same
`initiative_id`.

**Key fields:**

| Field | Description |
|-------|-------------|
| `initiative_id` | Stable slug grouping split features from the same initiative |
| `initiative_name` | Name of the reparations initiative |
| `city` | City where the initiative is based |
| `state` | 2-letter state abbreviation |
| `initiative_type` | Type (municipal, state, university, faith, private, etc.) |
| `status` | Status (active, proposed, passed, complete, etc.) |
| `year` | Year initiated or passed |
| `description` | Narrative description |
| `rep_bioguide_id` | Bioguide ID of the House Representative |
| `rep_name` | Full name of the House Representative |
| `rep_party` | Party of the House Representative |
| `rep_district` | House district number |
| `rep_phone` | Office phone |
| `rep_url` | Official website |
| `sen1_bioguide_id` | Bioguide ID of senior Senator |
| `sen1_name` | Full name of senior Senator |
| `sen1_party` | Party of senior Senator |
| `sen2_bioguide_id` | Bioguide ID of junior Senator |
| `sen2_name` | Full name of junior Senator |
| `sen2_party` | Party of junior Senator |
| `hr40_rep_position` | H.R. 40 co-sponsorship: "Co-sponsor" or "No co-sponsorship recorded" |
| `hr40_sen1_position` | H.R. 40 co-sponsorship for senior Senator |
| `hr40_sen2_position` | H.R. 40 co-sponsorship for junior Senator |
| `geometry_source` | `place_boundary`, `census_centroid`, or `nominatim_centroid` |
| `last_updated` | ISO 8601 timestamp of last pipeline run |

---

### Layer 2 — `Reparations_Legislators_Summary`
**Type:** Hosted Table (no geometry)

One record per legislator (both chambers) represented across all initiatives.
Primary key: `bioguide_id`.

| Field | Description |
|-------|-------------|
| `bioguide_id` | Unique Congress.gov identifier |
| `name` | Full name |
| `chamber` | `"House"` or `"Senate"` |
| `party` | Party affiliation |
| `state` | 2-letter state abbreviation |
| `district` | House district number (integer); `null` for Senators |
| `initiative_count` | Count of **distinct** initiatives represented (not raw feature count) |
| `hr40_position` | H.R. 40 co-sponsorship status |
| `phone` | Office phone |
| `url` | Official website |
| `photo_url` | Headshot URL (when available) |
| `delegation_alignment` | Senate only: "Both Democratic", "Both Republican", "Split", "Both Independent" |

---

### Layer 3 — `Reparations_Congressional_Districts`
**Type:** Feature Layer (Polygon geometry)

One polygon per 119th Congress House district, enriched with:
- `initiative_count` — count of **distinct** initiatives in that district
- House member attributes joined from Layer 2
- Supports choropleth by `initiative_count` and by `rep_party`

---

### Layer 4 — `Reparations_Senate_States`
**Type:** Feature Layer (Polygon geometry)

One polygon per U.S. state, enriched with:
- `initiative_count` — count of initiatives in the state
- Both senators' attributes joined from Layer 2
- `delegation_alignment` — derived from senator party affiliations
- Supports choropleth by `initiative_count` and by `delegation_alignment`

**`delegation_alignment` values:**
- `"Both Democratic"` — Blue
- `"Both Republican"` — Red
- `"Split"` — Purple
- `"Both Independent"` — Grey (edge case)

---

## Intersection Approach

The pipeline does **not** geocode each initiative to a single centroid point.
Instead it uses the full city boundary polygon and intersects it with all overlapping
119th Congress district polygons:

1. **City boundary lookup:** Each initiative's city is matched to a TIGER/Line
   Place shapefile polygon (incorporated city boundary). This captures the true
   geographic extent of the city.

2. **Boundary × district intersection:** `geopandas.overlay(city, districts, how='intersection')`
   produces one row per city-polygon / district-polygon overlap area.

3. **Intersection centroid as point:** Each overlap area's centroid becomes the
   point feature for that initiative × district combination. The point sits visually
   inside its district and is geographically meaningful.

4. **Result:** A city spanning 8 Congressional districts produces 8 point features,
   each cleanly inside one district, each with one unambiguous `rep_bioguide_id`.
   All 8 features share the same `initiative_id`.

**Fallback chain when city boundary is not found in TIGER Place:**
1. Census Bureau Geocoding API → `geometry_source: "census_centroid"`
2. Nominatim (OpenStreetMap) with 1-second delay → `geometry_source: "nominatim_centroid"`

Centroid fallbacks are treated as degenerate polygons (the city spans exactly one district).

---

## Join / Relate Structure

```
Layer 1 (Points)          Layer 2 (Legislators Table)
─────────────────         ────────────────────────────
rep_bioguide_id  ───────► bioguide_id  (Relate: "House Representative")
sen1_bioguide_id ───────► bioguide_id  (Relate: "Senior Senator")
sen2_bioguide_id ───────► bioguide_id  (Relate: "Junior Senator")

Layer 3 (Districts)
────────────────────
rep_bioguide_id  ───────► bioguide_id  (aggregated join)

Layer 4 (States)
────────────────
sen1_bioguide_id ───────► bioguide_id  (aggregated join)
sen2_bioguide_id ───────► bioguide_id  (aggregated join)
```

**Why Bioguide ID?** It is the stable, canonical identifier for U.S. legislators
across all Congress.gov data products. It never changes even if a member changes
parties or chamber.

**`initiative_count` accuracy:** Layer 2 counts **distinct `initiative_id` values**,
not raw Layer 1 features. This prevents cities that span multiple Congressional
districts from being counted multiple times (once per district intersection).

---

## Pipeline Re-run Behavior

| Resource | First run | Subsequent runs |
|----------|-----------|-----------------|
| TIGER/Line Place files | Downloaded (~600 MB) | Loaded from Drive cache |
| TIGER/Line CD + State files | Downloaded (~30 MB each) | Loaded from Drive cache |
| City geocode results | Computed and cached | Loaded from `geocode_cache.json` |
| Congress.gov API calls | Made for all districts/states | Cached in memory during run |
| ArcGIS Online items | Created (IDs written to `settings.py`) | Overwritten in place |

**Safety check:** Before truncating any ArcGIS layer, the pipeline compares the new
record count to the existing count. If the new count is below 80% of the existing
count, the publish is aborted with a visible error to protect against bad scrapes.

**ArcGIS item IDs** are stored in `config/settings.py` after first publish and
are used on subsequent runs to overwrite data in place. This preserves references
from any maps or dashboards that use these layers.

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
├── requirements.txt                 # Local development reference
└── README.md
```
