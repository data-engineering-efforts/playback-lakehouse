# Databricks notebook source
# MAGIC %md
# MAGIC # 21. Silver Layer — Sessionization & Late-Arriving Data Handling
# MAGIC **Layer:** Medallion Architecture -> **Silver Layer** (`playback_lakehouse.silver.viewing_sessions`)  
# MAGIC **Pattern:** Gap-Based Sessionization & Partition Replace-by-Scope (`replaceWhere`)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Purpose
# MAGIC This notebook aggregates atomic streaming heartbeats into logical viewing sessions based on a temporal inactivity gap (>30 minutes). It is specifically engineered to handle complex state mutations caused by late-arriving data.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Key Transformations
# MAGIC 1. **Gap-Based Demarcation:** Analyzes chronological event distances via `LAG()` window functions over Unix epochs to detect state transition boundaries (`is_new`).
# MAGIC 2. **Session Collapse:** Condenses billions of independent signals into a single high-level matrix per user session containing duration, total watch time, average bitrates, and QoS metrics (rebuffer events).
# MAGIC 3. **Idempotent Overwrite (Replace-by-Scope):** Utilizes Delta's `.option("replaceWhere", ...)` to atomically drop and re-compute only the affected date partitions. This design choice completely neutralizes "ghost sessions" created by out-of-order data, avoiding heavy row-by-row `MERGE` operations.

# COMMAND ----------

dbutils.widgets.text("processing_date", "")
processing_date = dbutils.widgets.get("processing_date")

# COMMAND ----------

from conf.config import (
    SILVER_SESSIONS, SILVER_EVENTS, MAX_SESSION_HOURS, GAP_SECONDS, HEARTBEAT_INTERVAL_SEC
)

# COMMAND ----------

from pyspark.sql import functions as F, Window
from datetime import date, timedelta

if not processing_date:
    # Interactive / full run: window = the whole events table
    w = spark.table(SILVER_EVENTS).select(
        F.min("event_date").alias("min_d"),
        F.max("event_date").alias("max_d")).collect()[0]
    win_min, win_max = str(w["min_d"]), str(w["max_d"])
else:
    # Job run: window derived FROM THE PARAMETER, not from any column
    d = date.fromisoformat(processing_date)
    win_min = str(d - timedelta(days=2))   # 1d MAX_SESSION_HOURS + 1d lateness horizon
    win_max = str(d)

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW tmp_recalculated_sessions AS
WITH session_begining AS (
  SELECT 
    *, 
      CASE 
        WHEN (CAST(event_ts AS LONG) - CAST(LAG(event_ts) OVER (PARTITION BY user_id, title_id ORDER BY event_ts) AS LONG)) > {GAP_SECONDS}
          THEN 1 
        ELSE 0 
      END AS is_new
  FROM playback_lakehouse.silver.playback_events 
  WHERE event_date BETWEEN '{win_min}' AND '{win_max}'
),
sessionization AS (
  SELECT 
    *,
    SUM(is_new) OVER (PARTITION BY user_id, title_id ORDER BY event_ts) AS session_num 
  FROM session_begining
)
SELECT 
  CONCAT_WS('_', user_id, title_id, session_num) AS session_key,
  user_id,
  title_id,
  MIN(event_ts) AS session_start,
  MAX(event_ts) AS session_end,
  CAST(MIN(event_ts) AS DATE) AS session_date,
  COUNT(*) AS heartbeats,
  MAX(playback_position_sec) AS max_position_sec,
  SUM(CAST(is_rebuffering AS INT)) AS rebuffer_events,
  ROUND(AVG(bitrate_kbps), 0) AS avg_bitrate,
  FIRST(country_code) AS country_code,
  FIRST(device_type) AS device_type,
  COUNT(*) * {HEARTBEAT_INTERVAL_SEC} AS watch_seconds 
FROM sessionization 
GROUP BY user_id, title_id, session_num
""")

recalculated_df = spark.table("tmp_recalculated_sessions")

(recalculated_df.write
   .format("delta")
   .mode("overwrite")
   .option("replaceWhere", f"session_date >= '{win_min}' AND session_date <= '{win_max}'")
   .saveAsTable(SILVER_SESSIONS))

print(f"Scope {win_min} to {win_max} successfully overwritten!")