# Databricks notebook source
# MAGIC %md
# MAGIC # 20. Silver Layer — Playback Events Deduplication
# MAGIC **Layer:** Medallion Architecture -> **Silver Layer** (`playback_lakehouse.silver.playback_events`)  
# MAGIC **Pattern:** Incremental Ingestion & Cross-Batch Deduplication via Delta `MERGE`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Purpose
# MAGIC This notebook processes raw playback heartbeats from the Bronze layer, applies data cleansing, flattens structures, and merges them into the cleaned Silver facts table. It ensures that the Silver layer contains uniquely identified, non-duplicated operational facts.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ##  Key Transformations
# MAGIC 1. **Within-Batch Deduplication:** Uses window functions (`row_number()`) to pick the latest record if a telemetry event is duplicated within the same ingestion slice.
# MAGIC 2. **Data Cleansing & Flattening:** Filters out corrupted files via `_rescued_data` validation, drops technical metadata columns, and flattens nested device components into dedicated columns (`device_type`, `device_os`).
# MAGIC 3. **Idempotent Delta MERGE:** Uses an atomic `whenNotMatchedInsertAll` condition on driving operational keys (`event_id`, `event_date`) to prevent cross-batch duplication during pipeline reruns.

# COMMAND ----------

dbutils.widgets.text("processing_date", "")
processing_date = dbutils.widgets.get("processing_date")

# COMMAND ----------

from conf.config import (
    BRONZE_HEARTBEATS, SILVER_EVENTS
)

# COMMAND ----------

from pyspark.sql import functions as F, Window
from delta.tables import DeltaTable
from datetime import date, timedelta

d = date.fromisoformat(processing_date)

if not processing_date: # interactive run -> process everything
    bronze = spark.table(BRONZE_HEARTBEATS)
else:
    bronze = spark.table(BRONZE_HEARTBEATS).filter(
        (F.col("_ingest_ts") >= F.lit(str(d))) &
        (F.col("_ingest_ts") <  F.lit(str(d + timedelta(days=1))))
    )

# ---- Incremental slice: only rows ingested since the last Silver load ----
# For the job this becomes a widget param; for now process everything not yet in Silver.
# Simple robust approach: stage ALL bronze, dedup, then MERGE (idempotent either way).
w_dedup = Window.partitionBy("event_id").orderBy(F.col("_ingest_ts").desc())

staged = (
    bronze
        .withColumn("_rn", F.row_number().over(w_dedup))
        .filter(F.col("_rn") == 1).drop("_rn")
        .filter(F.col("_rescued_data").isNull())
        .withColumn("device_type", F.col("device.device_type"))
        .withColumn("device_os",   F.col("device.device_os"))
        .withColumn("event_date", F.to_date("event_ts"))
        .drop("device", "_rescued_data", "_ingest_ts", "_source_file", "session_id")
        .filter(F.col("event_ts").isNotNull() & F.col("title_id").isNotNull())
)

#  MERGE (cross-batch dedup by event_id) ----
tgt = DeltaTable.forName(spark, SILVER_EVENTS)
(tgt.alias("t").merge(staged.alias("s"), "t.event_id = s.event_id AND t.event_date = s.event_date")
    .whenNotMatchedInsertAll() # new -> insert; existing event_id -> skip (dedup)
    .execute())
print("Silver events after merge:", spark.table(SILVER_EVENTS).count())