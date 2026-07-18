# Databricks notebook source
# MAGIC %md
# MAGIC # Exercise 1 — Skewed Join: events × dim_title (200M rows)
# MAGIC
# MAGIC **Workload:** join `silver.playback_events` (200M, Zipf-skewed on `title_id`) to
# MAGIC `gold.dim_title` (~15 MiB). Top keys: title_id=1 → 2.88M rows, 2 → 1.48M, 3 → 1.44M
# MAGIC (mean ≈ 1667/title). Equi-join on `is_current=true` preserves the cardinality of the
# MAGIC production SCD2 date-range join while isolating the skew. All runs write to `noop`.
# MAGIC
# MAGIC ## Baseline — naive sort-merge join (broadcast + AQE OFF)
# MAGIC Both sides shuffle by `title_id`; hot key → one partition → one straggler.
# MAGIC - Join stage 51s, 200 tasks, Shuffle Read 3.1 GiB
# MAGIC - Straggler: Duration median 2s / max 10s (5×); hot task 62.2 MiB / 3.72M rec
# MAGIC - Peak Execution Memory (measured): 704 MiB hot vs 288 median; no spill
# MAGIC
# MAGIC ## Fix A — Broadcast join (manual `F.broadcast(dim)`)
# MAGIC Small dim copied to every executor; 200M side never shuffled.
# MAGIC - 11 tasks, no shuffle; Peak Exec Mem 28.4 MiB uniform on ALL tasks (vs 704 hot)
# MAGIC - Skew eliminated structurally. Stage 58s. Wall-clock ~unchanged: the 200M scan
# MAGIC   dominated, not skew/shuffle.
# MAGIC
# MAGIC ## Fix B — Manual selective salting (broadcast OFF; pretend dim is huge)
# MAGIC Salt only dynamically-detected hot keys; explode dim only for those.
# MAGIC - Threshold 1.5M caught only [1]; keys 2 & 3 collided into a NEW 2.8M straggler
# MAGIC - Threshold 900k → [1,2,3,5]; straggler reduced 3.72M → 2.12M but not eliminated
# MAGIC   (salts re-collide when re-hashed into 200 partitions)
# MAGIC - Stage 1.0 min (slower than baseline) + a detection pass. **Fiddly and brittle.**
# MAGIC
# MAGIC ## Fix C — AQE (just enable it)
# MAGIC ```python
# MAGIC spark.conf.set("spark.sql.adaptive.enabled", "true")
# MAGIC spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
# MAGIC spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)  # static broadcast off
# MAGIC events.join(dim, "title_id")   # plain join, no hints
# MAGIC ```
# MAGIC **AQE ignored the static `-1` and chose BroadcastHashJoin anyway** — it has its own
# MAGIC adaptive broadcast path (`spark.sql.adaptive.autoBroadcastJoinThreshold`) and saw the
# MAGIC *actual* post-shuffle dim size (~13 MiB) was tiny. It also coalesced 200 → 55
# MAGIC partitions, removing small-task overhead.
# MAGIC - Plan node: **BroadcastHashJoin**; AQEShuffleRead coalesced to 55 partitions
# MAGIC - Stage **34s** — the fastest of all; Duration median 5s / max 6s (1.2×, even)
# MAGIC - Zero tuning beyond enabling AQE
# MAGIC
# MAGIC ## Results
# MAGIC
# MAGIC | Variant | Join stage | Strategy | Dur max/med | Peak exec mem | Tuning |
# MAGIC |---|---|---|---|---|---|
# MAGIC | Naive SMJ | 51s | SMJ (forced) | 10s / 2s (5×) | 704 MiB uneven | — |
# MAGIC | Broadcast | 58s | BroadcastHashJoin | even | 28 MiB uniform | `F.broadcast` |
# MAGIC | Salting | 1.0 min | SMJ + manual salt | 8s / 2s (~4×) | ~640 MiB | threshold + N, 2 rounds |
# MAGIC | **AQE** | **34s** | **auto-broadcast + coalesce** | **6s / 5s (1.2×)** | uniform | **enable AQE only** |
# MAGIC
# MAGIC ## Takeaways
# MAGIC 1. **Broadcast removes the conditions for skew** (no shuffle of the big side), turning
# MAGIC    a 704 MiB uneven hot partition into a uniform 28 MiB. Right choice whenever the
# MAGIC    small side fits — insures against scaling skew and OOM.
# MAGIC 2. **Manual salting is brittle:** the detection threshold must catch the whole hot
# MAGIC    *tail* (missing keys 2 & 3 created a new straggler); N must match the skew factor;
# MAGIC    salts re-collide on re-hash. Two tuning rounds and still not fully cleared.
# MAGIC 3. **AQE won on every axis** — fastest (34s), skew gone, zero tuning. It chose the
# MAGIC    optimal join *strategy* from actual runtime sizes (auto-broadcast) AND coalesced
# MAGIC    partitions. Static `autoBroadcastJoinThreshold=-1` does NOT stop AQE's adaptive
# MAGIC    broadcast; disable `spark.sql.adaptive.autoBroadcastJoinThreshold` too to force a
# MAGIC    pure skew-join test.
# MAGIC 4. **Why learn manual salting if AQE wins?** To understand the mechanics AQE hides —
# MAGIC    and for cases AQE can't help: non-join skew (aggregations, windows), older engines,
# MAGIC    or when adaptive broadcast is undesirable. On modern Spark, AQE is the default; the
# MAGIC    manual techniques are the fallback.

# COMMAND ----------

from conf.config import SILVER_EVENTS, GOLD_DIM_TITLE

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
spark.conf.set("spark.sql.adaptive.enabled", "false")

events = spark.table(SILVER_EVENTS)                      # 200M, skew intact
dim = spark.table(GOLD_DIM_TITLE).filter("is_current = true")

joined = events.join(dim, "title_id")
joined.write.format("noop").mode("overwrite").save()

# COMMAND ----------

from conf.config import SILVER_EVENTS, GOLD_DIM_TITLE
from pyspark.sql import functions as F

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 10 * 1024 * 1024)  # back to default 10MB
spark.conf.set("spark.sql.adaptive.enabled", "false")                     # keep AQE off for fair compare

events = spark.table(SILVER_EVENTS)
dim = spark.table(GOLD_DIM_TITLE).filter("is_current = true")

joined = events.join(F.broadcast(dim), "title_id")   # explicit broadcast hint
joined.write.format("noop").mode("overwrite").save()

# COMMAND ----------

from conf.config import SILVER_EVENTS, GOLD_DIM_TITLE
from pyspark.sql import functions as F

# ---------------------------------------------------------
# Environment Setup
# ---------------------------------------------------------
# Force Sort-Merge Join to demonstrate manual skew handling (disable Broadcast & AQE)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
spark.conf.set("spark.sql.adaptive.enabled", "false")

# ---------------------------------------------------------
# Configuration & Thresholds
# ---------------------------------------------------------
HOT_THRESHOLD =  900_000  # Any key with a frequency above this is considered "hot"
N = 8                      # Number of salt buckets to distribute the hot keys

# Load source tables
events = spark.table(SILVER_EVENTS)
dim = spark.table(GOLD_DIM_TITLE).filter("is_current = true")

# ---------------------------------------------------------
# Step 1: Dynamically detect hot keys
# ---------------------------------------------------------
# NOTE: This triggers a separate job (first pass over the facts table).
# In a production environment with Petabytes of data, consider using 
# events.sample(fraction=0.1) here to speed up the hot key detection.
hot_rows = (events.groupBy("title_id").count()
            .filter(F.col("count") > HOT_THRESHOLD)
            .select("title_id")
            .collect())

# Extract just the IDs into a standard Python list
hot_ids = [row.title_id for row in hot_rows]
print(f"Detected hot keys (> {HOT_THRESHOLD:,} rows): {hot_ids}")

# Create a boolean column condition for checking if a key is hot.
# Spark automatically optimizes and inlines small Python lists used in .isin()
is_hot = F.col("title_id").isin(hot_ids)

# ---------------------------------------------------------
# Step 2: Targeted salting on the fact table (events)
# ---------------------------------------------------------
# Hot keys get a random salt between 0 and N-1. Cold keys strictly get 0.
events_salted = events.withColumn(
    "salt",
    F.when(is_hot, (F.rand() * N).cast("int")).otherwise(F.lit(0))
)

# ---------------------------------------------------------
# Step 3: Targeted explosion on the dimension table (dim)
# ---------------------------------------------------------
# Hot keys are replicated N times (salt [0..N-1]). Cold keys remain 1:1 (salt [0]).
salt_range_hot  = F.array([F.lit(i) for i in range(N)])
salt_range_cold = F.array(F.lit(0))

dim_exploded = dim.withColumn(
    "salt",
    F.explode(F.when(is_hot, salt_range_hot).otherwise(salt_range_cold))
)

# ---------------------------------------------------------
# Step 4: Perform the Join and Cleanup
# ---------------------------------------------------------
# Join on the composite key (original key + salt)
joined = events_salted.join(dim_exploded, ["title_id", "salt"])

# Drop the technical 'salt' column to keep the schema clean
final_df = joined.drop("salt")

# Trigger execution (noop write = compute everything, write nothing)
final_df.write.format("noop").mode("overwrite").save()

# COMMAND ----------

from conf.config import SILVER_EVENTS, GOLD_DIM_TITLE

# Enable AQE + skew join; keep broadcast OFF so we test AQE's skew handling on a real SMJ
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

events = spark.table(SILVER_EVENTS)
dim = spark.table(GOLD_DIM_TITLE).filter("is_current = true")

joined = events.join(dim, "title_id")   # plain join — no salt, no broadcast, no hints
joined.write.format("noop").mode("overwrite").save()