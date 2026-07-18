# Databricks notebook source
# MAGIC %md
# MAGIC # 10. Bronze Ingestion — Playback Heartbeats Stream
# MAGIC **Layer:** Medallion Architecture -> **Bronze Layer** (`playback_lakehouse.bronze.playback_heartbeats`)  
# MAGIC **Pattern:** Incremental Streaming Ingestion via Databricks Auto Loader (`cloudFiles`)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Purpose
# MAGIC The primary goal of this notebook is to ingest raw, high-volume JSON playback telemetry (heartbeats) from the Landing Zone managed volume into an append-only Delta Lake table. 
# MAGIC
# MAGIC ## Key Architectural Decisions & Patterns
# MAGIC
# MAGIC ### 1. Databricks Auto Loader (`format("cloudFiles")`)
# MAGIC We utilize Auto Loader to scale efficiently. 
# MAGIC * It uses optimal file discovery (bookkeeping metadata) to process only newly arrived files, eliminating the overhead of full bucket scans.
# MAGIC * It operates via `.trigger(availableNow=True)` providing a cost-effective "streaming pipeline that shuts down automatically" once all currently available files are processed.
# MAGIC
# MAGIC ### 2. Defensive Ingestion & Schema Resilience
# MAGIC * **Schema Evolution:** Using `.option("cloudFiles.schemaLocation", ...)` combined with `.option("mergeSchema", "true")` ensures that any new fields added downstream changes are automatically captured and integrated without manual DDL alterations or downtime.
# MAGIC * **Schema Hints:** We explicitly enforce types on critical driving keys (`event_ts`, `event_id`, `user_id`, `title_id`) using `schemaHints` to guarantee downstream components don't receive invalid data types.
# MAGIC * **Rescued Data:** Corrupted, malformed, or structural type-mismatches are caught in the hidden `_rescued_data` column, keeping the ingestion pipeline fully operational.
# MAGIC
# MAGIC ### 3. Lineage Enrichment
# MAGIC Every row is explicitly injected with metadata attributes:
# MAGIC * `_ingest_ts`: Wall-clock system timestamp of when Spark processed the batch.
# MAGIC * `_source_file`: The exact file path in the cloud object storage.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Target Schema Summary
# MAGIC * **Input Path:** `/Volumes/playback_lakehouse/landing/raw/playback_heartbeats`
# MAGIC * **Output Table:** `playback_lakehouse.bronze.playback_heartbeats`
# MAGIC * **Format:** Delta Lake (Append-Only)

# COMMAND ----------

from conf.config import (
    SCHEMA_PATH_HEARTBEATS, RAW_HEARTBEATS, CKPT_PATH_HEARTBEATS, BRONZE_HEARTBEATS
)

# COMMAND ----------

from pyspark.sql import functions as F

bronze_stream = (
    spark.readStream
        .format("cloudFiles")  # Auto Loader
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", SCHEMA_PATH_HEARTBEATS)  # persisted inferred schema
        .option("cloudFiles.inferColumnTypes", "true") # infer int/long/bool, not all-strings
        .option("cloudFiles.schemaHints", # force the tricky ones
                "event_ts timestamp, event_id long, user_id long, title_id int")
        .load(RAW_HEARTBEATS)
)

# Bronze principle: keep raw as-is, but add ingestion lineage metadata
bronze_enriched = (
    bronze_stream
        .withColumn("_ingest_ts",   F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
)

query = (
    bronze_enriched.writeStream
        .format("delta")
        .option("checkpointLocation", CKPT_PATH_HEARTBEATS)
        .option("mergeSchema", "true")
        .trigger(availableNow=True) # process all available files, then STOP
        .toTable(BRONZE_HEARTBEATS)
)
query.awaitTermination()
print("Bronze ingestion complete.")

# COMMAND ----------

