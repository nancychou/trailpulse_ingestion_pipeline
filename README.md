# TrailPulse WTA Weekly Ingestion (GitHub Actions)

This project runs a weekly ingestion pipeline:

1) Discover WTA hike URLs from sitemap (incremental)
2) Crawl a bounded number of changed/new hike pages (robots.txt crawl-delay)
3) Enrich with OSM + optional elevation-derived features
4) Compute Difficulty (0–10) + Runnable %
5) Upsert into Supabase (Postgres)
6) Record `dataset_version` + run metadata

## Operational workflows
There are two GitHub Actions workflows:

1. `Bootstrap WTA Ingestion`
- Manual trigger only (`workflow_dispatch`)
- Intended for initial backfill / catch-up runs
- Default: `max_pages=300`, fixed `crawl_delay=60` (WTA requirement)

2. `Weekly WTA Ingestion`
- Scheduled weekly and manually triggerable
- Default: `max_pages=40`, fixed `crawl_delay=60`
- Uses incremental crawl strategy and only upserts changed rows

Both workflows share the same `concurrency` group, so only one ingestion run can write at a time.

## Supabase setup (one-time)
Run the SQL in `supabase/sql/001_pipeline_tables.sql` in Supabase SQL editor.

## Local run
```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

export SUPABASE_URL="https://<project>.supabase.co"
export SUPABASE_SERVICE_ROLE_KEY="<service_role_key>"
export DATABASE_URL="postgresql://..."   # optional (not required by this pipeline)
export FRONTEND_URL="http://localhost:3000"  # optional

python pipeline/run_weekly.py --max-pages 50 --crawl-delay 60 --save-artifacts
```

## GitHub Actions
Add repository secrets:
- `SUPABASE_URL`
- `SUPABASE_SERVICE_ROLE_KEY`

`Weekly WTA Ingestion` runs on schedule and can be triggered manually from the Actions tab.

## Recommended rollout
1. Run `Bootstrap WTA Ingestion` once (or a few times) to accelerate initial coverage.
2. Verify health checks (queries below).
3. Keep `Weekly WTA Ingestion` as the steady-state maintenance job.

## SQL health checks
Run these in Supabase SQL editor after each run:

```sql
-- 1) Any rows missing dataset_version?
select count(*) as missing_dataset_version
from trails
where dataset_version is null;
```

```sql
-- 2) Crawl state coverage
select count(*) as crawl_state_rows
from wta_crawl_state;
```

```sql
-- 3) Current dataset distribution
select dataset_version, count(*) as trails
from trails
group by 1
order by 1 desc;
```

```sql
-- 4) Recent pipeline runs
select run_id, status, started_at, finished_at, row_count, notes
from pipeline_runs
order by started_at desc
limit 20;
```

## Failure runbook (quick)
1. Open the failed run in GitHub Actions and check the failing step logs.
2. Download artifacts (if present) to inspect intermediate parquet output.
3. Check `pipeline_runs` and `wta_crawl_state` in Supabase for error patterns.
4. Fix root cause, then re-run manually:
   - Backfill scope: `Bootstrap WTA Ingestion`
   - Routine retry: `Weekly WTA Ingestion`
5. Re-run SQL health checks and confirm:
   - no unexpected spike in `failed_urls`
   - `missing_dataset_version = 0`
