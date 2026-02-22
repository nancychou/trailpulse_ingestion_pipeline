-- Migration: Update trails table to match pipeline output after code refactoring
-- Run this in Supabase SQL Editor

BEGIN;

-- No column renames — frontend uses trailhead_lat/trailhead_lng
-- Pipeline renames latitude/longitude → trailhead_lat/trailhead_lng before upsert

-- 2. Add new columns from OSM enrichment
ALTER TABLE trails ADD COLUMN IF NOT EXISTS osm_type          text;
ALTER TABLE trails ADD COLUMN IF NOT EXISTS osm_distance_mi   float8;
ALTER TABLE trails ADD COLUMN IF NOT EXISTS derived_gain_ft   float8;
ALTER TABLE trails ADD COLUMN IF NOT EXISTS geometry_polyline  text;

-- 3. Add TrailRecord columns not yet in the table
ALTER TABLE trails ADD COLUMN IF NOT EXISTS description       text;
ALTER TABLE trails ADD COLUMN IF NOT EXISTS raw_features      text;

-- 4. Add scoring intermediate/output columns
ALTER TABLE trails ADD COLUMN IF NOT EXISTS gain_ft            float8;
ALTER TABLE trails ADD COLUMN IF NOT EXISTS gain_per_mile_ft   float8;
ALTER TABLE trails ADD COLUMN IF NOT EXISTS trail_runnable_pct float8;

-- 5. Add pipeline metadata
ALTER TABLE trails ADD COLUMN IF NOT EXISTS dataset_version    text;

-- 6. Drop columns no longer produced by the pipeline
ALTER TABLE trails DROP COLUMN IF EXISTS grade_p95;
ALTER TABLE trails DROP COLUMN IF EXISTS log_distance;

COMMIT;
