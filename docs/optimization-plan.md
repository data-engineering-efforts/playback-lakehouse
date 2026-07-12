# Phase: Spark Optimization Program (Cloud)

Goal: hands-on mastery of Spark performance engineering on realistic skewed data.
Every exercise follows the same loop: **expose → observe in Spark UI → fix → measure → document.**

Environment: AWS Databricks trial (classic workspace), Jobs/all-purpose cluster with
Spark UI access. Data scaled to ~200M heartbeat events via `dbldatagen`
(`NUM_ROWS = 200_000_000`, raise `GEN_PARTITIONS` to ~64, generate once on spot, keep as Delta).

Budget guards: cluster auto-terminate 15 min; spot workers; AWS Budget alerts at $50/$100;
generate large data ONCE and reuse.

---

## Exercise 0 — Spark UI literacy (half a day)

Run a healthy medium query (e.g. QoS mart aggregation) and tour the UI:

- **Jobs tab:** job → stages breakdown, where time goes.
- **Stages tab:** per-task duration distribution (min / median / max — healthy = max ≈ median),
  Shuffle Read/Write sizes, Spill columns (should be 0 here).
- **SQL/DataFrame tab:** physical plan graph; find the Exchange (=shuffle) nodes;
  actual rows/bytes per operator.
- **Executors tab:** cores, memory, task distribution across workers.

Artifact: annotated screenshot of a healthy stage — the "before" reference for everything else.

Key numbers to always record per experiment:
| metric | where |
|---|---|
| wall-clock time | job duration |
| max task time vs median | stage detail |
| shuffle read/write bytes | stage summary |
| spill (memory/disk) | stage summary |
| plan (join strategy used) | SQL tab |

---

## Exercise 1 — The skewed join, exposed (the centerpiece)

Join: `silver.viewing_sessions` (large, skewed by title) × `gold.dim_title` (small).

Expose the problem — force the bad plan:
```python
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)  # forbid broadcast
spark.conf.set("spark.sql.adaptive.enabled", "false")        # disable AQE
```
Run the enrichment join, open the join stage:
- one task dramatically longer than median (the `title_id=1` straggler);
- task-level shuffle read: the straggler read far more data.

Artifact: screenshot of the task timeline with the straggler; record max/median ratio.

### Fix 1a — Broadcast join (the right tool here)
Re-enable broadcast (or hint `F.broadcast(dim)`). Observe: Exchange on the big side
disappears from the plan; no straggler; total time drops. Record speedup.
Discussion point: why broadcast works — the big table never shuffles, so skew never
materializes into one partition.

### Fix 1b — Manual salting (pretend dim is too big to broadcast)
Keep broadcast disabled. Technique:
- add `salt = rand() % N` to the fact side;
- explode the dim side into N copies (one per salt value);
- join on `(title_id, salt)` → the hot key spreads over N tasks.
Measure vs the exposed baseline. Discussion: salting trades an N× blowup of the
small side for even distribution; choosing N = observing skew factor.

### Fix 1c — AQE skew join
Re-enable AQE (`spark.sql.adaptive.skewJoin.enabled=true`), keep broadcast off.
AQE splits oversized shuffle partitions automatically. Compare with 1b:
similar effect, zero code. Discussion: when manual salting still matters
(aggregations, non-join skew, older engines).

Final artifact: table `baseline SMJ | broadcast | salting | AQE` with times and
max/median ratios. This table goes into the README.

---

## Exercise 2 — Big × big join: events × dim_title at event grain

`silver.playback_events` (200M) × `dim_title` with broadcast disabled is a true
big-shuffle SMJ. Study:
- cost of shuffling 200M rows (shuffle write GB, stage time);
- `spark.sql.shuffle.partitions`: run at 200 (default) vs 800 vs auto (AQE coalesce);
  observe task size vs overhead trade-off;
- note: Hive-style bucketing is NOT supported for Unity Catalog managed Delta tables —
  the modern layout answer is liquid clustering (Exercise 6).

Artifact: shuffle-partitions sweep table (200 / 800 / AQE) with times.

---

## Exercise 3 — Wide aggregations (the engagement mart at scale)

Rebuild `mart_engagement` on 200M-derived sessions. Study three cost drivers:
1. `explode(genres)` row blowup (input vs output rows of the Generate node in SQL tab);
2. `COUNT(DISTINCT user_id)` — the most expensive aggregate (full shuffle of values);
   compare with `approx_count_distinct` (HLL) — measure time AND error;
3. group-by skew (US/IN/BR dominate) — check per-task distribution on the agg stage.

Artifact: three-row comparison (exact distinct / approx / no-distinct) + note on
when approx is acceptable.

---

## Exercise 4 — Small files problem

Create the pathology deliberately: write events as thousands of tiny files
(`.repartition(2000)` before write, or many tiny Auto Loader batches).
- measure a full scan before vs after `OPTIMIZE` (file count via `DESCRIBE DETAIL`);
- discuss auto compaction / optimized writes and target file sizes;
- connect to streaming reality: micro-batches naturally produce small files.

Artifact: scan-time and file-count before/after OPTIMIZE.

---

## Exercise 5 — Memory & spill

Force a spill: window function or join with oversized partitions
(set `spark.sql.shuffle.partitions` LOW, e.g. 16, on the 200M join).
- find Spill (Memory/Disk) in the stage summary;
- fix in two independent ways and compare: (a) raise partition count so each task
  fits in memory; (b) bigger executors (fewer, larger). Discussion: partitions are
  the cheap knob; memory is the expensive one; executor memory on Databricks is
  fixed by node type, so instance choice IS the memory knob.

Artifact: spill metrics before/after, with the chosen fix rationale.

---

## Exercise 6 — Storage layout: pruning, clustering, skipping

1. Verify partition pruning works: filtered query on `event_date`, check
   "files read / files pruned" in the scan node of the SQL tab.
2. Liquid clustering: `CLUSTER BY (title_id)` on a copy of the fact,
   `OPTIMIZE` it, compare a selective `title_id` query vs the date-partitioned table
   (bytes read from the scan node).
3. Discussion for the ADR: dates → partitions (low cardinality, aligns with
   replaceWhere); high-cardinality selective keys → liquid clustering.

Artifact: bytes-read comparison table.

---

## Exercise 7 — Photon benchmark

Run the same two workloads (mart rebuild; the big SMJ join) on a Photon cluster
vs non-Photon, same size. Record wall-clock and DBU cost (Photon ≈ 2× DBU rate).
Compute actual $/query. Discussion: Photon shines on wide scans/aggregations,
less on shuffle-bound or Python-UDF-bound work.

Artifact: cost-performance table — time, DBUs, effective $.

---

## Deliverables checklist (goes to README / docs)

- [ ] Annotated Spark UI screenshots: healthy stage, straggler, spill.
- [ ] Skew-fix comparison table (SMJ / broadcast / salting / AQE).
- [ ] Shuffle-partitions sweep results.
- [ ] Distinct vs approx_count_distinct trade-off note.
- [ ] Small-files before/after OPTIMIZE.
- [ ] Partitioning vs liquid clustering bytes-read table.
- [ ] Photon cost-performance table.
- [ ] ADR updates: layout strategy, AQE vs manual salting, approx aggregates policy.

## Exit checklist (end of trial)

- [ ] Terminate/delete all clusters.
- [ ] Export any workspace-only artifacts to the repo.
- [ ] Delete classic-workspace AWS resources (VPC, NAT gateway, S3 buckets if unneeded).
- [ ] Remove payment method / cancel subscription per Databricks docs.