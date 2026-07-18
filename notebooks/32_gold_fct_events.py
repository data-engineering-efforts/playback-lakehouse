# Databricks notebook source
# MAGIC %md
# MAGIC # 32. Gold Layer — QoS Event Fact
# MAGIC **Layer:** Medallion Architecture -> **Gold Layer** (`playback_lakehouse.gold.fct_playback_events`)
# MAGIC **Pattern:** Event-Grain Projection with Partition-Window replaceWhere
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Purpose
# MAGIC Materializes the event-grain QoS fact directly from silver playback events.
# MAGIC **No dimension join by design:** rebuffering and bitrate degradation are
# MAGIC network/device properties, not content properties. `title_id` is retained as a
# MAGIC natural key for the optional heavy skewed join in the optimization phase.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Key Transformations
# MAGIC 1. **QoS Projection:** Selects only quality-relevant attributes (CDN pop, device,
# MAGIC    bitrate, rebuffering) at the finest grain available.
# MAGIC 2. **Partitioned by event_date:** Enables partition pruning for the QoS mart and
# MAGIC    idempotent incremental rebuilds.
# MAGIC 3. **replaceWhere by Date Window:** Mirrors the upstream event ingestion window;
# MAGIC    reruns overwrite the same partitions with identical results (idempotent).

# COMMAND ----------

dbutils.widgets.text("processing_date", "")
processing_date = dbutils.widgets.get("processing_date")

# COMMAND ----------

from conf.config import SILVER_EVENTS, GOLD_FCT_EVENTS
from pyspark.sql import functions as F

# COMMAND ----------

# Databricks notebook source
# ============================================================
# 32_gold_fct_events
# QoS fact at event grain. No dimension join by design:
# rebuffering is a network/device property. title_id kept for the
# optional heavy skewed join in the optimization phase.
# Incremental strategy: replaceWhere by event_date window.
# ============================================================
from datetime import date, timedelta

if not processing_date:
    # Interactive / full run: window = the whole events table
    w = spark.table(SILVER_EVENTS).select(
        F.min("event_date").cast("string").alias("min_d"),
        F.max("event_date").cast("string").alias("max_d"),
    ).collect()[0]
    win_min, win_max = w["min_d"], w["max_d"]
else:
    # Job run: window derived FROM THE PARAMETER, not from any column
    d = date.fromisoformat(processing_date)
    win_min = str(d - timedelta(days=1))   # 1d lateness horizon
    win_max = str(d)


fct = (spark.table(SILVER_EVENTS)
    .filter(F.col("event_date").between(win_min, win_max))
    .select(
        "event_id", "event_ts", "event_date",
        "user_id", "title_id",
        "country_code", "cdn_pop",
        "device_type", "device_os",
        "bitrate_kbps", "is_rebuffering",
        "playback_position_sec",
    ))

(fct.write
    .format("delta")
    .mode("overwrite")
    .option("replaceWhere",
            f"event_date >= '{win_min}' AND event_date <= '{win_max}'")
    .saveAsTable(GOLD_FCT_EVENTS))

print(f"fct_playback_events: window {win_min}..{win_max} replaced.")