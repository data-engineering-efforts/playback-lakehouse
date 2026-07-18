# Databricks notebook source
# MAGIC %md
# MAGIC # 21. Silver Layer — Content Catalog Cleansing
# MAGIC **Layer:** Medallion Architecture -> **Silver Layer** (`playback_lakehouse.silver.dim_title`)  
# MAGIC **Pattern:** Snapshot Deduplication & Data Standardization via Delta `MERGE`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Purpose
# MAGIC This notebook processes raw catalog snapshots from the Bronze layer, applies data normalization rules, handles nested schema transformations, and merges the cleaned records into the Silver dimension table. It acts as a staging and cleansing ground before building the historical SCD2 timeline in the Gold layer.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Key Transformations
# MAGIC 1. **Cross-Snapshot Deduplication:** Uses window functions (`ROW_NUMBER()`) over the business key `title_id` ordered by `_snapshot_date DESC` to extract only the latest known state of each film or episode from the ingested batch.
# MAGIC 2. **Text Standardization & Type Coercion:** Trims spaces, normalizes string casing for categories/genres, converts runtime metrics to standard integers, and fills missing values (e.g., fallback for unknown release years).
# MAGIC 3. **Idempotent Upsert (`MERGE`):** Executes an atomic `MERGE` operation based on `title_id`. It updates existing movie attributes if they changed between snapshots (`WHEN MATCHED`) and inserts completely new catalog items (`WHEN NOT MATCHED`).

# COMMAND ----------

from conf.config import BRONZE_DIM_TITLE, SILVER_DIM_TITLE, SNAP_V1, SNAP_V2
from pyspark.sql import functions as F, Window

def load_snapshot(snap: str):
    """Idempotent per-snapshot load: clean one snapshot, replace its partition."""
    bronze = (spark.table(BRONZE_DIM_TITLE)
        .filter(F.col("_snapshot_date") == snap))

    # Dedup within snapshot (defensive: source may deliver a title twice).
    # Order by _ingest_ts desc -> keep the latest delivered copy.
    w = Window.partitionBy("title_id").orderBy(F.col("_ingest_ts").desc())

    titles = (bronze
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1).drop("_rn")
        .filter(F.col("title_id").isNotNull()))

    (titles.select(
            "title_id", "title_name", "genres", "content_type",
            "release_year", "runtime_min",
            "_snapshot_date", "_source_file", "_ingest_ts")
        .write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"_snapshot_date = '{snap}'")
        .saveAsTable(SILVER_DIM_TITLE))

    print(f"Snapshot {snap} loaded into silver.dim_title")

bronze_snaps = {r[0] for r in spark.table(BRONZE_DIM_TITLE)
                    .select("_snapshot_date").distinct().collect()}
silver_snaps = {r[0] for r in spark.table(SILVER_DIM_TITLE)
                    .select("_snapshot_date").distinct().collect()} \
               if spark.catalog.tableExists(SILVER_DIM_TITLE) else set()

new_snaps = sorted(bronze_snaps - silver_snaps)
if not new_snaps:
    print("No new snapshots to load.")
for snap in new_snaps:
    load_snapshot(snap)