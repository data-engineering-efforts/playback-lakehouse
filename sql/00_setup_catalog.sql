-- ============================================================
-- 00_setup_catalog.sql
-- playback-lakehouse — Unity Catalog structure
-- Media streaming analytics, medallion architecture
-- ============================================================

-- 1. Project catalog (project-level data isolation)
CREATE CATALOG IF NOT EXISTS playback_lakehouse
  COMMENT 'Media streaming analytics lakehouse - medallion (bronze/silver/gold)';

USE CATALOG playback_lakehouse;

-- 2. Schemas per medallion layer and landing zone for raw files
CREATE SCHEMA IF NOT EXISTS landing
  COMMENT 'Raw JSON from generators; source for Auto Loader';

CREATE SCHEMA IF NOT EXISTS bronze
  COMMENT 'Raw ingestion, append-only, schema as-is';

CREATE SCHEMA IF NOT EXISTS silver
  COMMENT 'Cleaned, deduplicated, typed, sessionized, SCD2';

CREATE SCHEMA IF NOT EXISTS gold
  COMMENT 'Business marts: star schema + OBT';

-- 3. Managed volume - physical landing zone for raw files.
--    dbldatagen writes JSON here, Auto Loader picks it up.
CREATE VOLUME IF NOT EXISTS landing.raw
  COMMENT 'Landing zone for raw files before Bronze';

-- 4. Ops volume - Auto Loader schemas & checkpoints (kept outside raw data)
CREATE VOLUME IF NOT EXISTS landing._ops
  COMMENT 'Auto Loader schemas and checkpoints';