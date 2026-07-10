-- ============================================================
-- 01_create_tables.sql
-- playback-lakehouse — explicit DDL for Silver & Gold tables.
-- Run ONCE after 00_setup_catalog.sql (deploy-time step).
-- Idempotent: IF NOT EXISTS everywhere, safe to re-run.
--
-- NOT included here:
--   - bronze tables  -> created by Auto Loader (schema inference is the point)
--   - gold marts     -> rebuilt via CREATE OR REPLACE TABLE AS in 40_gold_marts
-- ============================================================

USE CATALOG playback_lakehouse;

-- ============================================================
-- SILVER
-- ============================================================

-- Clean, deduplicated playback events (grain: one heartbeat event)
CREATE TABLE IF NOT EXISTS silver.playback_events (
    event_id BIGINT COMMENT 'Unique event id (dedup / merge key)',
    user_id BIGINT,
    title_id INT,
    playback_position_sec INT,
    bitrate_kbps INT,
    is_rebuffering BOOLEAN,
    country_code STRING,
    cdn_pop STRING,
    event_ts TIMESTAMP COMMENT 'Event time (source of session logic)',
    event_date DATE GENERATED ALWAYS AS (CAST(event_ts AS DATE)), 
    device_type STRING COMMENT 'Flattened from device STRUCT',
    device_os STRING COMMENT 'Flattened from device STRUCT'
)
PARTITIONED BY (event_date)
COMMENT 'Cleaned heartbeats: deduped by event_id, malformed rows dropped, STRUCT flattened';

-- Viewing sessions (grain: one session; derived from playback_events via gap-based sessionization)
CREATE TABLE IF NOT EXISTS silver.viewing_sessions (
  session_key STRING COMMENT 'user_id _ title_id _ session_num (merge key)',
  user_id BIGINT,
  title_id INT,
  session_start TIMESTAMP,
  session_end TIMESTAMP,
  session_date DATE GENERATED ALWAYS AS (CAST(session_start AS DATE)), 
  heartbeats BIGINT COMMENT 'COUNT(*) of events in session',
  max_position_sec INT,
  rebuffer_events BIGINT,
  avg_bitrate DOUBLE,
  country_code STRING,
  device_type STRING,
  watch_seconds BIGINT
)
PARTITIONED BY (session_date)
COMMENT 'Gap-based sessions (gap > 30 min starts a new session)';

-- Clean catalog snapshots (grain: one title per snapshot)
CREATE TABLE IF NOT EXISTS silver.dim_title (
    title_id BIGINT,
    title_name STRING,
    genres ARRAY<STRING>,
    content_type STRING,
    release_year INT,
    runtime_min INT,
    _snapshot_date STRING COMMENT 'Recovered from landing path /title/YYYY-MM-DD/',
    _source_file STRING,
    _ingest_ts TIMESTAMP
)
PARTITIONED BY (_snapshot_date)
COMMENT 'Conformed catalog snapshots; loaded idempotently via replaceWhere per snapshot';

-- ============================================================
-- GOLD
-- ============================================================

-- SCD2 dimension (grain: one VERSION of a title)
CREATE TABLE IF NOT EXISTS gold.dim_title (
    title_sk BIGINT COMMENT 'Surrogate key of a title VERSION (xxhash64)',
    attr_hash STRING COMMENT 'sha2 over business attributes (change detection)',
    title_id BIGINT COMMENT 'Natural key',
    title_name STRING,
    genres  ARRAY<STRING>,
    content_type STRING,
    release_year INT,
    runtime_min INT,
    valid_from DATE,
    valid_to  DATE COMMENT 'Half-open interval: >= valid_from AND < valid_to',
    is_current BOOLEAN
)
COMMENT 'SCD Type 2 via single atomic union-trick MERGE';

-- Enriched session fact (grain: one session)
CREATE TABLE IF NOT EXISTS gold.fct_viewing_sessions (
    session_key STRING,
    user_id BIGINT,
    title_id INT  COMMENT 'Natural key',
    title_sk  BIGINT COMMENT 'Version key of dim_title active at session start (NULL if none)',
    session_start TIMESTAMP,
    session_end TIMESTAMP,
    session_date DATE COMMENT 'Canonical event date: partitioning, aggregation, SCD2 lookup',
    heartbeats  BIGINT,
    max_position_sec INT,
    rebuffer_events  BIGINT,
    avg_bitrate  DOUBLE,
    country_code STRING,
    device_type STRING
)
PARTITIONED BY (session_date)
COMMENT 'Sessions enriched with SCD2 date-range lookup (LEFT JOIN, half-open interval)';

-- QoS fact (grain: one event; no dimension join by design)
CREATE TABLE IF NOT EXISTS gold.fct_playback_events (
    event_id  BIGINT,
    event_ts  TIMESTAMP,
    event_date DATE,
    user_id  BIGINT,
    title_id  INT  COMMENT 'Kept for the optional heavy skewed join (optimization phase)',
    country_code STRING,
    cdn_pop STRING,
    device_type STRING,
    device_os  STRING,
    bitrate_kbps INT,
    is_rebuffering  BOOLEAN,
    playback_position_sec INT
)
PARTITIONED BY (event_date)
COMMENT 'Event-grain QoS fact; rebuffering is a network/device property';