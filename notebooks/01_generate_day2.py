# Databricks notebook source
# ============================================================
# 01_generate_day2
# Simulates a realistic "next day" arrival into the SAME landing folder:
#   - NEW events: fresh dbldatagen batch with a DISJOINT event_id range
#   - DUPLICATES: a slice of already-loaded events, replayed with the SAME
#     event_id (true duplicates -> exercise MERGE dedup downstream)
# Auto Loader will pick up only the new files on its next incremental run.
# ============================================================
%pip install dbldatagen
dbutils.library.restartPython()

# COMMAND ----------

from conf.config import (
    NUM_ROWS, NUM_USERS, NUM_TITLES, NUM_DAYS, GEN_PARTITIONS, SEED,
    EVENTS_START_DATE, RAW_HEARTBEATS, BRONZE_HEARTBEATS, DAY2_NEW_ROWS,
    DAY2_DUPE_FRAC, DAY2_OFFSET, EVENT_ID_OFFSET
)
import dbldatagen as dg
import dbldatagen.distributions as dist
from pyspark.sql import functions as F, types as T


# COMMAND ----------

# ---- 1) NEW events: fresh dbldatagen batch, disjoint event_id range ----
spec = (
    dg.DataGenerator(spark, name="day2_new",
                     rows=DAY2_NEW_ROWS, partitions=GEN_PARTITIONS, randomSeed=SEED + 1)
    .withColumn("event_id", T.LongType(),
                minValue=EVENT_ID_OFFSET, maxValue=EVENT_ID_OFFSET + DAY2_NEW_ROWS - 1,
                uniqueValues=DAY2_NEW_ROWS)                       # disjoint from day-1
    .withColumn("user_id", T.LongType(), minValue=1, maxValue=NUM_USERS,
                distribution=dist.Gamma(0.5, 1.0), random=True)
    .withColumn("title_id", T.IntegerType(), minValue=1, maxValue=NUM_TITLES,
                distribution=dist.Gamma(0.4, 1.0), random=True)
    .withColumn("day_offset", T.IntegerType(), minValue=DAY2_OFFSET, maxValue=DAY2_OFFSET, random=True)
    .withColumn("event_hour", T.IntegerType(), values=list(range(24)),
                weights=[1,1,1,1,1,1,2,2,3,3,3,3,4,4,4,5,6,7,9,10,10,9,6,3], random=True)
    .withColumn("event_minute", T.IntegerType(), minValue=0, maxValue=59, random=True)
    .withColumn("event_second", T.IntegerType(), minValue=0, maxValue=59, random=True)
    .withColumn("playback_position_sec", T.IntegerType(), minValue=0, maxValue=7200, random=True)
    .withColumn("bitrate_kbps", T.IntegerType(),
                values=[480,720,1080,2160,4000], weights=[10,20,40,20,10], random=True)
    .withColumn("is_rebuffering", T.BooleanType(), expr="rand() < 0.04")
    .withColumn("country_code", T.StringType(),
                values=["US","IN","BR","GB","DE","JP","MX","CA","FR","KR"],
                weights=[30,22,15,6,5,5,4,3,3,2], random=True)
    .withColumn("cdn_pop", T.StringType(),
                values=["iad","bom","gru","lhr","fra","nrt","icn","lax","dfw","sin"],
                weights=[20,18,14,8,7,7,6,7,7,6], random=True)
    .withColumn("device_type", T.StringType(),
                values=["smart_tv","mobile","web","tablet","console"],
                weights=[40,30,15,10,5], random=True)
    .withColumn("device_os", T.StringType(),
                values=["tizen","android","ios","webos","roku","windows"], random=True)
)

day2_new = (spec.build()
    .withColumn("event_ts", F.expr(
        f"timestamp('{EVENTS_START_DATE}') "
        "+ make_dt_interval(day_offset, event_hour, event_minute, event_second)"))
    .withColumn("session_id", F.concat_ws("_", F.col("user_id"),
        F.date_format("event_ts", "yyyyMMdd"), F.floor(F.col("event_hour") / F.lit(2))))
    .withColumn("device", F.struct("device_type", "device_os"))
    .drop("day_offset","event_hour","event_minute","event_second","device_type","device_os"))

# COMMAND ----------

# ---- 2) DUPLICATES: replay a slice of already-loaded events, SAME event_id ----
# Read existing bronze, strip bronze-only columns to match the landing shape.
existing = (spark.table(BRONZE_HEARTBEATS)
    .drop("_ingest_ts", "_source_file", "_rescued_data"))

day2_dupe = existing.sample(withReplacement=False, fraction=DAY2_DUPE_FRAC, seed=7)

# Align columns and union new + duplicates into one landing batch
cols = ["event_id","user_id","title_id","playback_position_sec","bitrate_kbps",
        "is_rebuffering","country_code","cdn_pop","event_ts","session_id","device"]
day2_batch = day2_new.select(*cols).unionByName(day2_dupe.select(*cols))

# ---- 3) Write as NEW JSON files into the SAME landing folder (append) ----
(day2_batch.write.mode("append").json(RAW_HEARTBEATS))
print("Day-2 batch written:", day2_batch.count(),
      f"({DAY2_NEW_ROWS} new + ~{DAY2_DUPE_FRAC:.0%} duplicates)")