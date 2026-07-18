# Databricks notebook source
# MAGIC %md
# MAGIC # 31. Gold Layer — Enriched Session Fact
# MAGIC **Layer:** Medallion Architecture -> **Gold Layer** (`playback_lakehouse.gold.fct_viewing_sessions`)
# MAGIC **Pattern:** SCD2 Date-Range Lookup and Window-Mirrored Incremental Rebuild
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Purpose
# MAGIC Builds the session-grain fact by enriching silver viewing sessions with the
# MAGIC `dim_title` version that was active at session start. The fact carries both the
# MAGIC natural key (`title_id`) and the version surrogate key (`title_sk`), enabling
# MAGIC historically correct attribute lookups downstream.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Key Transformations
# MAGIC 1. **SCD2 Date-Range Lookup:** LEFT JOIN on `title_id` with a half-open interval
# MAGIC    (`session_date >= valid_from AND session_date < valid_to`). The range predicate
# MAGIC    lives in the ON clause — keeping it in WHERE would silently turn LEFT into INNER.
# MAGIC 2. **Version Key Propagation:** `title_sk` pins each session to the exact dimension
# MAGIC    version active at the time; sessions with no matching version keep NULL.
# MAGIC 3. **Window-Mirrored replaceWhere:** The fact is rebuilt for the same date window
# MAGIC    as the upstream `silver.viewing_sessions` rebuild, keeping one incremental
# MAGIC    primitive across the session chain (see ADR: incremental strategy per table).

# COMMAND ----------

dbutils.widgets.text("processing_date", "")
processing_date = dbutils.widgets.get("processing_date")

# COMMAND ----------

from conf.config import (
    SILVER_SESSIONS, GOLD_DIM_TITLE, GOLD_FCT_SESSIONS, MAX_SESSION_HOURS,
)

# COMMAND ----------

# Databricks notebook source
# ============================================================
# 31_gold_fct_sessions
# Enrich silver sessions with the SCD2 dim_title version active
# at session start (date-range LEFT JOIN, half-open interval).
# Incremental strategy: replaceWhere by date window — MIRRORS the
# upstream silver.viewing_sessions rebuild window (see ADR).
# ============================================================
from pyspark.sql import functions as F
from datetime import date, timedelta

# ---- Processing window: mirror the upstream rebuild window.
# For now derived from the source table; becomes a job parameter later.

if not processing_date:
    # Interactive / full run: window = the whole events table
    w = spark.table(SILVER_SESSIONS).select(
        F.min("session_date").cast("string").alias("min_d"),
        F.max("session_date").cast("string").alias("max_d"),
    ).collect()[0]
    win_min, win_max = w["min_d"], w["max_d"]
else:
    # Job run: window derived FROM THE PARAMETER, not from any column
    d = date.fromisoformat(processing_date)
    win_min = str(d - timedelta(days=1))   #  1d lateness horizon
    win_max = str(d)

spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW tmp_fct_sessions AS
    SELECT
        vs.session_key,
        vs.user_id,
        vs.title_id,
        dt.title_sk,
        vs.session_start,
        vs.session_end,
        vs.session_date,
        vs.heartbeats,
        vs.max_position_sec,
        vs.rebuffer_events,
        vs.avg_bitrate,
        vs.country_code,
        vs.device_type
    FROM {SILVER_SESSIONS} AS vs
    LEFT JOIN {GOLD_DIM_TITLE} AS dt
      ON  vs.title_id = dt.title_id
      AND vs.session_date >= dt.valid_from    -- SCD2 lookup: version active
      AND vs.session_date <  dt.valid_to      -- at session start (half-open)
    WHERE vs.session_date BETWEEN '{win_min}' AND '{win_max}'
""")

(spark.table("tmp_fct_sessions").write
    .format("delta")
    .mode("overwrite")
    .option("replaceWhere",
            f"session_date >= '{win_min}' AND session_date <= '{win_max}'")
    .saveAsTable(GOLD_FCT_SESSIONS))

print(f"fct_viewing_sessions: window {win_min}..{win_max} replaced.")