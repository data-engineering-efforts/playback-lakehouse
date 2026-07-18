# Databricks notebook source
# MAGIC %md
# MAGIC # Exercise 5 — Memory & Spill (Window Sessionization, 200M events)
# MAGIC
# MAGIC **Workload:** `silver.viewing_sessions` rebuild. The gap-based sessionization sorts
# MAGIC each `(user_id, title_id)` partition for the LAG window, so the *entire* partition
# MAGIC must fit in a task's execution memory — a natural spill generator.
# MAGIC
# MAGIC ## Baseline (AQE default)
# MAGIC The window sort stage read 1911 MiB of shuffle across **34 partitions** (~56 MiB
# MAGIC compressed per task). Our low cardinality synthetic data decompresses ~13×, so each
# MAGIC task's working set was ~768 MiB — far above per-task execution memory. Result:
# MAGIC
# MAGIC - **Spill (memory): 22.4 GiB**, Spill (disk): 657 MiB
# MAGIC - GC Time median ~1 s (constant disk round-trips)
# MAGIC - Stage wall-clock: 1.5 min
# MAGIC
# MAGIC **Root cause:** AQE's `advisoryPartitionSizeInBytes` targets *compressed* shuffle
# MAGIC bytes (default 64 MB). With a 13× decompression ratio, the in-memory sort working
# MAGIC set (~768 MiB) blew past execution memory. The knob is compressed, the spill is
# MAGIC driven by the uncompressed size.
# MAGIC
# MAGIC ## Fix — shrink the advisory partition size
# MAGIC ```python
# MAGIC spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "16MB")
# MAGIC ```
# MAGIC Target ~16 MB compressed → ~130 MiB uncompressed per partition, which fits in memory.
# MAGIC
# MAGIC - Partitions: 34 → **184**
# MAGIC - **Spill: eliminated (0 / 0)**
# MAGIC - GC Time median: 1 s → **44 ms** (~20× less)
# MAGIC - Stage wall-clock: 1.5 min → **1.8 min** (slightly slower)
# MAGIC
# MAGIC ## Results
# MAGIC
# MAGIC | Variant | Partitions | Spill (mem/disk) | GC median | Dur median/max | Wall-clock |
# MAGIC |---|---|---|---|---|---|
# MAGIC | Baseline (AQE 64MB) | 34 | 22.4 GiB / 657 MiB | ~1 s | 27s / 39s | 1.5 min |
# MAGIC | advisory 16MB | 184 | 0 / 0 | 44 ms | 4s / 17s | 1.8 min |
# MAGIC
# MAGIC ## Takeaways
# MAGIC 1. **AQE's advisory size is in compressed bytes; spill is driven by the uncompressed
# MAGIC    working set.** With high compression ratios, lower the target aggressively.
# MAGIC 2. **Eliminating spill did not speed up the job.** Spill went to local NVMe (m5d),
# MAGIC    which is cheap; the stage was not spill-bound. More partitions added scheduling
# MAGIC    overhead (184 tasks over 8 cores ≈ 23 waves) and exposed key skew (max/median
# MAGIC    duration 4.3× vs 1.4× before), offsetting the win.
# MAGIC 3. **Lesson:** treat spill as a symptom, not an automatic problem. Fix it when it
# MAGIC    dominates stage time or threatens OOM; on fast local disk a moderate spill can be
# MAGIC    cheaper than the overhead of over-partitioning. Alternative memory levers (fewer
# MAGIC    concurrent tasks per executor, larger-memory instances) trade cost for headroom
# MAGIC    and were not exercised.

# COMMAND ----------

from conf.config import (
    NUM_TITLES, NUM_USERS, NUM_DAYS, EVENTS_START_DATE,
    SILVER_EVENTS, GAP_SECONDS, HEARTBEAT_INTERVAL_SEC,
)
NUM_ROWS = 200_000_000
GEN_PARTITIONS = 128  
SEED = 42

# COMMAND ----------

import dbldatagen as dg
import dbldatagen.distributions as dist
from pyspark.sql import functions as F, types as T

spec = (
    dg.DataGenerator(spark, name="events", rows=NUM_ROWS,
                     partitions=GEN_PARTITIONS, randomSeed=SEED)
    .withColumn("event_id", T.LongType(), uniqueValues=NUM_ROWS)
    .withColumn("user_id", T.LongType(), minValue=1, maxValue=NUM_USERS,
                distribution=dist.Gamma(0.5, 1.0), random=True)
    .withColumn("title_id", T.IntegerType(), minValue=1, maxValue=NUM_TITLES,
                distribution=dist.Gamma(0.4, 1.0), random=True)          # THE skew
    .withColumn("day_offset", T.IntegerType(), minValue=0, maxValue=NUM_DAYS - 1, random=True)
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

events = (spec.build()
    .withColumn("event_ts", F.expr(
        f"timestamp('{EVENTS_START_DATE}') "
        "+ make_dt_interval(day_offset, event_hour, event_minute, event_second)"))
    .select(
        "event_id", "user_id", "title_id", "playback_position_sec", "bitrate_kbps",
        "is_rebuffering", "country_code", "cdn_pop", "event_ts",
        "device_type", "device_os"))
    # NOTE: event_date is a generated column in the DDL — do NOT add it here

(events.write.format("delta").mode("overwrite")
    .saveAsTable(SILVER_EVENTS))

print("silver.playback_events rows:", spark.table(SILVER_EVENTS).count())

# COMMAND ----------

from conf.config import SILVER_SESSIONS
from pyspark.sql import functions as F, Window

spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "16MB")

GAP = GAP_SECONDS
w = Window.partitionBy("user_id", "title_id").orderBy("event_ts")

sess = (spark.table(SILVER_EVENTS)
    .withColumn("prev_ts", F.lag("event_ts").over(w))
    .withColumn("is_new",
        ((F.col("prev_ts").isNull()) |
         (F.col("event_ts").cast("long") - F.col("prev_ts").cast("long") > GAP)).cast("int"))
    .withColumn("session_num", F.sum("is_new").over(w.rowsBetween(Window.unboundedPreceding, 0)))
    .withColumn("session_key", F.concat_ws("_", "user_id", "title_id", "session_num")))

sessions = (sess.groupBy("session_key", "user_id", "title_id")
    .agg(
        F.min("event_ts").alias("session_start"),
        F.max("event_ts").alias("session_end"),
        F.count("*").alias("heartbeats"),
        F.max("playback_position_sec").alias("max_position_sec"),
        F.sum(F.col("is_rebuffering").cast("int")).alias("rebuffer_events"),
        F.round(F.avg("bitrate_kbps"), 0).alias("avg_bitrate"),
        F.first("country_code").alias("country_code"),
        F.first("device_type").alias("device_type"))
    .withColumn("watch_seconds", F.col("heartbeats") * F.lit(HEARTBEAT_INTERVAL_SEC)))

(sessions.write.format("delta").mode("overwrite").saveAsTable(SILVER_SESSIONS))
print("silver.viewing_sessions rows:", spark.table(SILVER_SESSIONS).count())