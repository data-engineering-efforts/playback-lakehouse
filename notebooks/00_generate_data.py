# Databricks notebook source
# MAGIC %md
# MAGIC # 00. Data Simulation & Landing Ingestion
# MAGIC **Domain:** Media Analytics  
# MAGIC **System Layer:** Data Source Simulator -> Unity Catalog Landing Zone (`landing.raw`)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Overview
# MAGIC This notebook acts as the core **Data Generator and Simulator** for the entire portfolio project.
# MAGIC
# MAGIC The primary goal of this simulation is to establish a high-volume, realistic dataset specifically engineered to test and practice **Advanced Apache Spark Optimizations** (handling data skew, shuffle tuning, and storage layouts) in later stages of the Medallion architecture.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Tech Stack & Tools
# MAGIC * **Engine:** Apache Spark (PySpark DataFrame API)
# MAGIC * **Library:** `dbldatagen` (Databricks Data Generator) for distributed, scalable synthesis.
# MAGIC * **Target Storage:** Unity Catalog Managed Volume (`/Volumes/playback_lakehouse/landing/raw/`)
# MAGIC * **Format:** Raw unstructured JSON (Streaming Heartbeats)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Simulated Data & Built-in Anomalies
# MAGIC We intentionally inject specific data distributions to simulate a "dirty" production environment:
# MAGIC 1.  **Playback Heartbeats (Fact):** Generated using a **Gamma Distribution** ($shape=0.4$) to mimic **Extreme Data Skew** (Zipf-like behavior where top-10 mega-hits capture ~30% of total traffic). This will intentionally break naive Spark joins later.
# MAGIC 2.  **Temporal Peak (Prime-Time):** Event timestamps are heavily weighted towards evening hours (19:00 - 22:00) to create uneven partitions and file size distributions.
# MAGIC 3.  **Geographic & Network Skew:** Traffic density is concentrated in specific regions (US, IN, BR) and routes to global CDN POPs asynchronously, allowing for cross-region routing anomaly detection.
# MAGIC 4.  **Complex Data Types:** Device telemetry is packed into a nested Spark `STRUCT` to enforce structural flattening patterns on the Silver layer.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Scale Control
# MAGIC * **Scale Mode (Cloud Tuning):** Scales up to **Billions of rows** on a multi-node AWS cluster using Jobs Compute and Spot Instances to perform hard-core Spark UI and memory optimization.

# COMMAND ----------

# %pip install dbldatagen

# # Restart Python so the freshly installed library is importable
# dbutils.library.restartPython()

# COMMAND ----------

from conf.config import (
    NUM_ROWS,  GEN_PARTITIONS, EVENTS_START_DATE,
    NUM_USERS, NUM_TITLES, NUM_DAYS, SEED, RAW_HEARTBEATS,
    SNAP_V1, SNAP_V2, HIGH_DATE, GENRE_POOL, RAW_TITLE

)

# COMMAND ----------

import dbldatagen as dg
import dbldatagen.distributions as dist
from pyspark.sql import functions as F, types as T

spec = (
    dg.DataGenerator(spark, name="playback_heartbeats",
                     rows=NUM_ROWS, partitions=GEN_PARTITIONS, randomSeed=SEED)

    # Unique event id
    .withColumn("event_id", T.LongType(), uniqueValues=NUM_ROWS)

    # user_id: Gamma(shape=0.5) => power users watch far more than average
    .withColumn("user_id", T.LongType(), minValue=1, maxValue=NUM_USERS,
                distribution=dist.Gamma(0.5, 1.0), random=True)

    # title_id: Gamma(shape=0.4) => mega-hits dominate (our MAIN skew / hot key)
    .withColumn("title_id", T.IntegerType(), minValue=1, maxValue=NUM_TITLES,
                distribution=dist.Gamma(0.4, 1.0), random=True)

    # Time components; event_hour is weighted toward prime time (19:00-22:00)
    .withColumn("day_offset", T.IntegerType(), minValue=0, maxValue=NUM_DAYS - 1, random=True)
    .withColumn("event_hour", T.IntegerType(),
                values=list(range(24)),
                weights=[1,1,1,1,1,1,2,2,3,3,3,3,4,4,4,5,6,7,9,10,10,9,6,3], random=True)
    .withColumn("event_minute", T.IntegerType(), minValue=0, maxValue=59, random=True)
    .withColumn("event_second", T.IntegerType(), minValue=0, maxValue=59, random=True)

    # Playback + quality fields
    .withColumn("playback_position_sec", T.IntegerType(), minValue=0, maxValue=7200, random=True)
    .withColumn("bitrate_kbps", T.IntegerType(),
                values=[480, 720, 1080, 2160, 4000], weights=[10, 20, 40, 20, 10], random=True)
    .withColumn("is_rebuffering", T.BooleanType(), expr="rand() < 0.04")

    # Geo skew: US / IN / BR dominate
    .withColumn("country_code", T.StringType(),
                values=["US","IN","BR","GB","DE","JP","MX","CA","FR","KR"],
                weights=[30,22,15,6,5,5,4,3,3,2], random=True)

    # CDN edge POP
    .withColumn("cdn_pop", T.StringType(),
                values=["iad","bom","gru","lhr","fra","nrt","icn","lax","dfw","sin"],
                weights=[20,18,14,8,7,7,6,7,7,6], random=True)

    # Device fields (assembled into a STRUCT after build)
    .withColumn("device_type", T.StringType(),
                values=["smart_tv","mobile","web","tablet","console"],
                weights=[40,30,15,10,5], random=True)
    .withColumn("device_os", T.StringType(),
                values=["tizen","android","ios","webos","roku","windows"], random=True)
)

raw = spec.build()

# ---- Post-build shaping: real timestamp, session id, nested device struct ----
df = (
    raw
    # Build a real event_ts from the time components
    .withColumn("event_ts", F.expr(
        f"timestamp('{EVENTS_START_DATE}') "
        "+ make_dt_interval(day_offset, event_hour, event_minute, event_second)"))
    # Sessionize: heartbeats from same user in same ~2h window share a session
    .withColumn("session_id", F.concat_ws("_",
        F.col("user_id"),
        F.date_format("event_ts", "yyyyMMdd"),
        F.floor(F.col("event_hour") / F.lit(2))))
    # Nest device fields into a STRUCT (complex type -> lands as nested JSON)
    .withColumn("device", F.struct("device_type", "device_os"))
    .drop("day_offset", "event_hour", "event_minute", "event_second",
          "device_type", "device_os")
)

# COMMAND ----------

(df.write
   .mode("overwrite") 
   .json(RAW_HEARTBEATS))

print("Wrote raw JSON to:", RAW_HEARTBEATS)

# COMMAND ----------

# MAGIC %md
# MAGIC # Fill raw title layer 

# COMMAND ----------


from pyspark.sql import functions as F

def add_attrs(df):
    """Attach descriptive attributes deterministically from title_id."""
    picks = F.array(*[F.lit(g) for g in GENRE_POOL])
    return (df
        .withColumn("title_name",  F.concat(F.lit("Title "), F.col("title_id")))
        .withColumn("genres", F.array_distinct(F.array(
            F.element_at(picks, (F.col("title_id") % 10 + 1).cast("int")),
            F.element_at(picks, ((F.col("title_id") * 7) % 10 + 1).cast("int")))))
        .withColumn("content_type",
            F.when(F.col("title_id") % 3 == 0, F.lit("movie")).otherwise(F.lit("series")))
        .withColumn("release_year", (F.lit(2000) + (F.col("title_id") % 26)).cast("int"))
        .withColumn("runtime_min",  (F.lit(20)   + (F.col("title_id") % 120)).cast("int")))

# ---- v1: January snapshot ----
v1 = add_attrs(spark.range(1, NUM_TITLES + 1).withColumnRenamed("id", "title_id"))

# ---- v2: June snapshot = v1 with controlled drift + new titles ----
v2_existing = add_attrs(spark.range(1, NUM_TITLES + 1).withColumnRenamed("id", "title_id"))
v2_existing = (v2_existing
    # ~10% of titles gain a genre (attribute change)
    .withColumn("genres", F.when(F.col("title_id") % 10 == 0,
            F.array_distinct(F.array_union("genres", F.array(F.lit("Drama")))))
        .otherwise(F.col("genres")))
    # a smaller subset changes content_type
    .withColumn("content_type", F.when(F.col("title_id") % 50 == 0,
            F.lit("special")).otherwise(F.col("content_type"))))

v2_new = add_attrs(spark.range(NUM_TITLES + 1, NUM_TITLES + 501)
                        .withColumnRenamed("id", "title_id"))
v2 = v2_existing.unionByName(v2_new)

v1.write.mode("overwrite").json(f"{RAW_TITLE}/{SNAP_V1}")
v2.write.mode("overwrite").json(f"{RAW_TITLE}/{SNAP_V2}")
print("Raw snapshots written to landing.")