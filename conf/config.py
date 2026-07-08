# ============================================================
# Catalog & schemas
# ============================================================
CATALOG = "playback_lakehouse"

SCHEMA_LANDING = "landing"
SCHEMA_BRONZE = "bronze"
SCHEMA_SILVER = "silver"
SCHEMA_GOLD = "gold"

# ============================================================
# Table names (fully qualified: catalog.schema.table)
# ============================================================
# Bronze
BRONZE_HEARTBEATS = f"{CATALOG}.{SCHEMA_BRONZE}.playback_heartbeats"
BRONZE_DIM_TITLE  = f"{CATALOG}.{SCHEMA_BRONZE}.dim_title"

# Silver
SILVER_EVENTS = f"{CATALOG}.{SCHEMA_SILVER}.playback_events"
SILVER_SESSIONS = f"{CATALOG}.{SCHEMA_SILVER}.viewing_sessions"
SILVER_DIM_TITLE = f"{CATALOG}.{SCHEMA_SILVER}.dim_title"

# Gold — dimensions
GOLD_DIM_TITLE = f"{CATALOG}.{SCHEMA_GOLD}.dim_title"

# Gold — facts
GOLD_FCT_SESSIONS = f"{CATALOG}.{SCHEMA_GOLD}.fct_viewing_sessions"
GOLD_FCT_EVENTS = f"{CATALOG}.{SCHEMA_GOLD}.fct_playback_events"

# Gold — marts
GOLD_MART_ENGAGEMENT = f"{CATALOG}.{SCHEMA_GOLD}.mart_engagement"
GOLD_MART_QOS  = f"{CATALOG}.{SCHEMA_GOLD}.mart_qos"

# ============================================================
# Landing-zone paths (Unity Catalog volumes)
#   raw/   -> data only (Auto Loader scans this)
#   _ops/  -> Auto Loader schemas & checkpoints (NEVER inside the scanned path)
# ============================================================
_VOL_ROOT = f"/Volumes/{CATALOG}/{SCHEMA_LANDING}"

RAW_BASE = f"{_VOL_ROOT}/raw"
OPS_BASE = f"{_VOL_ROOT}/_ops"

# Raw data folders per source
RAW_HEARTBEATS = f"{RAW_BASE}/playback_heartbeats"
RAW_TITLE  = f"{RAW_BASE}/title" # date subfolders: {RAW_TITLE}/{SNAP_V1}, ...

# Auto Loader bookkeeping (schemas + checkpoints), grouped by source
SCHEMA_PATH_HEARTBEATS = f"{OPS_BASE}/schemas/playback_heartbeats"
CKPT_PATH_HEARTBEATS = f"{OPS_BASE}/checkpoints/playback_heartbeats"
SCHEMA_PATH_TITLE = f"{OPS_BASE}/schemas/title"
CKPT_PATH_TITLE = f"{OPS_BASE}/checkpoints/title"

# ============================================================
# Snapshot dates & SCD2 conventions
# ============================================================
SNAP_V1 = "2026-01-01"        # January catalog snapshot
SNAP_V2 = "2026-06-15"        # June catalog snapshot (SCD2 changes take effect)

HIGH_DATE = "9999-12-31"      # open-ended valid_to for current SCD2 versions

# ============================================================
# Data-generation parameters (dev defaults; scale up for optimization phase)
# ============================================================
NUM_ROWS = 5_000_000 # playback heartbeats (bump to 100M+ on cloud)
NUM_TITLES = 120_000 # catalog size (~constant regardless of event volume)
NUM_USERS = 1_000_000 # active user base for the dev slice
NUM_DAYS = 30  # events span SNAP-era start .. +NUM_DAYS
GEN_PARTITIONS = 8 # generation parallelism (raise for big runs)
SEED = 42 # reproducibility
EVENTS_START_DATE = "2026-01-01"

# Genre pool for synthetic catalog attributes
GENRE_POOL = [
    "Sci-Fi", "Drama", "Horror", "Comedy", "Action",
    "Documentary", "Thriller", "Romance", "Fantasy", "Crime",
]

# ============================================================
# Business-logic constants
# ============================================================
GAP_SECONDS = 30 * 60   # session boundary: gap > 30 min starts a new session
HEARTBEAT_INTERVAL_SEC = 10        # generator emits a heartbeat ~every 10s (watch-time basis)