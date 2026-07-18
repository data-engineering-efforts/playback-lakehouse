# Databricks notebook source
# MAGIC %md
# MAGIC # 11. Bronze Ingestion — Content Catalog Snapshots
# MAGIC **Layer:** Medallion Architecture -> **Bronze Layer** (`playback_lakehouse.bronze.dim_title`)  
# MAGIC **Pattern:** Snapshot Ingestion with Path-Based Date Extraction via Auto Loader
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Purpose
# MAGIC This notebook ingests full or delta snapshots of the media content catalog into the Bronze layer. The catalog arrives as periodic snapshots stored in date-partitioned directory structures within the Landing Zone.
# MAGIC
# MAGIC The main objective is to preserve every historical state of the catalog as-is, while capturing the snapshot effective date directly from the file system layout. This enables downstream SCD2 processing in the Gold layer.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Key Architectural Decisions & Patterns
# MAGIC
# MAGIC ### 1. Dynamic Partition Metadata Extraction
# MAGIC Since the source files themselves do not contain a dedicated column indicating *when* the snapshot was captured, we leverage Spark's hidden metadata directory context:
# MAGIC * `F.col("_metadata.file_path")` is used to audit the physical source of each row.
# MAGIC * A RegEx pattern (`F.regexp_extract`) scans the directory path structure (e.g., `/title/YYYY-MM-DD/`) to dynamically build the `_snapshot_date` column on the fly. This prevents temporal mixing and anchors the data point to a specific snapshot window.
# MAGIC
# MAGIC ### 2. Auto Loader for File Tracking
# MAGIC * Using `cloudFiles` allows  tracking of newly uploaded folders without reloading historical snapshots that have already been converted into Delta format.
# MAGIC * Structured via `.trigger(availableNow=True)` to process all newly added snapshot folders in a single execution block and immediately spin down compute.
# MAGIC
# MAGIC ### 3. Schema Enforcement
# MAGIC * Explicit data types are asserted for key business metrics (`title_id`, `release_year`, `runtime_min`) using `schemaHints` to avoid type coercion bugs (like treating numerical keys as strings) before saving the layout into Delta.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Target Schema Summary
# MAGIC * **Input Path Pattern:** `/Volumes/playback_lakehouse/landing/raw/title/{YYYY-MM-DD}/`
# MAGIC * **Output Table:** `playback_lakehouse.bronze.dim_title`
# MAGIC * **Format:** Delta Lake (Append-Only with Historical Snapshots)

# COMMAND ----------

from conf.config import (
    RAW_TITLE, SCHEMA_PATH_TITLE, CKPT_PATH_TITLE, BRONZE_DIM_TITLE
)

# COMMAND ----------

from pyspark.sql import functions as F

stream = (spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", SCHEMA_PATH_TITLE)
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaHints", "title_id long, release_year int, runtime_min int")
    .load(RAW_TITLE)) 

bronze = (stream
    .withColumn("_source_file",   F.col("_metadata.file_path"))
    # Recover the snapshot date from the path segment /title/YYYY-MM-DD/
    .withColumn("_snapshot_date",
        F.regexp_extract(F.col("_metadata.file_path"), r"/title/(\d{4}-\d{2}-\d{2})/", 1))
    .withColumn("_ingest_ts",     F.current_timestamp()))

query = (bronze.writeStream.format("delta")
    .option("checkpointLocation", CKPT_PATH_TITLE)
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .toTable(BRONZE_DIM_TITLE))

query.awaitTermination()
print("Bronze dim_title ingestion complete.")