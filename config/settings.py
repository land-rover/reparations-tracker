import os

# Base data directory — resolves to Google Drive when running in Colab
DATA_BASE = os.environ.get("REPARATIONS_DATA_BASE", "data")

# Data paths
GEOCODE_CACHE_PATH     = f"{DATA_BASE}/geocode_cache.json"
RAW_DATA_PATH          = f"{DATA_BASE}/initiatives_raw.json"
ENRICHED_DATA_PATH     = f"{DATA_BASE}/initiatives_enriched.geojson"
LEGISLATORS_PATH       = f"{DATA_BASE}/legislators_summary.json"
ERRORS_LOG_PATH        = f"{DATA_BASE}/errors.log"

# TIGER/Line cache paths
TIGER_PLACE_CACHE_DIR  = f"{DATA_BASE}/tiger_place/"
TIGER_PLACE_COMPLETE   = f"{DATA_BASE}/tiger_place/.complete"
TIGER_CD_CACHE_DIR     = f"{DATA_BASE}/tiger_cd/"
TIGER_CD_COMPLETE      = f"{DATA_BASE}/tiger_cd/.complete"
TIGER_STATE_CACHE_PATH = f"{DATA_BASE}/tiger_state/tl_2024_us_state.zip"

# TIGER/Line source URLs
# CD119: per-state files (tl_2024_{fips}_cd119.zip) — no single national file exists
TIGER_PLACE_URL  = "https://www2.census.gov/geo/tiger/TIGER2024/PLACE/"
TIGER_CD_URL     = "https://www2.census.gov/geo/tiger/TIGER2024/CD/"
TIGER_STATE_URL  = "https://www2.census.gov/geo/tiger/TIGER2024/STATE/tl_2024_us_state.zip"

SOURCE_URL = "https://www.reparationsresources.com/table"

# ArcGIS Online item IDs — populated after first publish run; do not edit manually
AGOL_POINTS_ITEM_ID      = ""   # Layer 1: Reparations_Initiatives_Points
AGOL_LEGISLATORS_ITEM_ID = ""   # Layer 2: Reparations_Legislators_Summary
AGOL_DISTRICTS_ITEM_ID   = ""   # Layer 3: Reparations_Congressional_Districts
AGOL_STATES_ITEM_ID      = ""   # Layer 4: Reparations_Senate_States

# Safety threshold: abort publish if new record count < this fraction of previous count
MIN_RECORD_RATIO = 0.80
