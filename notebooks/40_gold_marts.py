# Databricks notebook source
# MAGIC %md
# MAGIC # 40. Gold Layer — Business Marts
# MAGIC **Layer:** Medallion Architecture -> **Gold Layer** (`mart_engagement`, `mart_qos`)
# MAGIC **Pattern:** Full Rebuild of Small Aggregates (CREATE OR REPLACE TABLE AS)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Purpose
# MAGIC Builds the two consumer-facing marts:
# MAGIC * **mart_engagement** — watch hours, sessions, and completion rate by genre /
# MAGIC   country / day. Joins facts to `dim_title` on `title_sk`, so attributes are
# MAGIC   version-correct as of each session. Genres ARRAY is exploded via LATERAL VIEW.
# MAGIC * **mart_qos** — rebuffer ratio and average bitrate by CDN pop / device / day.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Design Notes
# MAGIC 1. **Full rebuild is intentional:** marts are small aggregates (thousands of rows);
# MAGIC    CREATE OR REPLACE is atomic, always consistent, and avoids partial-update logic.
# MAGIC    The full-scan cost is bounded by the fact tables and acceptable at this scale;
# MAGIC    see the incremental-marts note in the ADR for the production-scale alternative.
# MAGIC 2. **Watch time derives from heartbeats** (`COUNT * interval`), not wall-clock —
# MAGIC    pauses do not inflate engagement.
# MAGIC 3. **Completion is capped at 1.0** (`LEAST`) to guard against positions exceeding
# MAGIC    runtime.

# COMMAND ----------

from conf.config import (
    GOLD_FCT_SESSIONS, GOLD_FCT_EVENTS, GOLD_DIM_TITLE,
    GOLD_MART_ENGAGEMENT, GOLD_MART_QOS, HEARTBEAT_INTERVAL_SEC,
)

# COMMAND ----------

# Databricks notebook source
# ============================================================
# 40_gold_marts
# Business marts on top of the gold facts. Small aggregates ->
# full rebuild via CREATE OR REPLACE TABLE AS is the right
# strategy (cheap, always consistent, no partitioning needed).
# ============================================================
from conf.config import (
    GOLD_FCT_SESSIONS, GOLD_FCT_EVENTS, GOLD_DIM_TITLE,
    GOLD_MART_ENGAGEMENT, GOLD_MART_QOS, HEARTBEAT_INTERVAL_SEC,
)

# ---- Engagement mart: watch hours & completion by genre/country/day ----
spark.sql(f"""
    CREATE OR REPLACE TABLE {GOLD_MART_ENGAGEMENT} AS
    SELECT
        f.session_date,
        g.genre,
        f.country_code,
        COUNT(*)                                  AS total_sessions,
        COUNT(DISTINCT f.user_id)                 AS distinct_users,
        ROUND(SUM(f.heartbeats * {HEARTBEAT_INTERVAL_SEC}) / 3600.0, 2)
                                                  AS watch_hours,
        ROUND(AVG(LEAST(f.max_position_sec / (d.runtime_min * 60.0), 1.0)), 3)
                                                  AS avg_completion
    FROM {GOLD_FCT_SESSIONS} f
    JOIN {GOLD_DIM_TITLE} d
      ON f.title_sk = d.title_sk               -- version-correct attributes
    LATERAL VIEW explode(d.genres) g AS genre
    GROUP BY f.session_date, g.genre, f.country_code
""")

# ---- QoS mart: rebuffer ratio by CDN pop / device / day ----
spark.sql(f"""
    CREATE OR REPLACE TABLE {GOLD_MART_QOS} AS
    SELECT
        event_date,
        cdn_pop,
        device_type,
        COUNT(*)                                              AS total_events,
        SUM(CAST(is_rebuffering AS INT))                      AS rebuffer_events,
        ROUND(SUM(CAST(is_rebuffering AS INT)) / COUNT(*), 4) AS rebuffer_ratio,
        ROUND(AVG(bitrate_kbps), 0)                           AS avg_bitrate
    FROM {GOLD_FCT_EVENTS}
    GROUP BY event_date, cdn_pop, device_type
""")

print("Marts rebuilt: mart_engagement, mart_qos")