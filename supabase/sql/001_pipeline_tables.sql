-- 1) Track dataset snapshots
create table if not exists dataset_versions (
  dataset_version text primary key,
  created_at timestamptz not null default now(),
  notes text
);

-- 2) Track crawl state per WTA URL (for incremental runs)
create table if not exists wta_crawl_state (
  url text primary key,
  last_hash text,
  last_crawled_at timestamptz,
  last_status int,
  last_error text
);

-- 3) Track pipeline runs
create table if not exists pipeline_runs (
  run_id text primary key,
  dataset_version text not null references dataset_versions(dataset_version),
  schema_version text not null,
  status text not null,
  started_at timestamptz not null,
  finished_at timestamptz not null,
  row_count int not null default 0,
  stats jsonb not null default '{}'::jsonb,
  params jsonb not null default '{}'::jsonb,
  notes text
);

-- 4) Add dataset_version to your existing trails table (safe if already exists)
do $$
begin
  if exists (select 1 from information_schema.tables where table_name='trails') then
    if not exists (
      select 1 from information_schema.columns
      where table_name='trails' and column_name='dataset_version'
    ) then
      alter table trails add column dataset_version text;
    end if;

    -- Helpful index for filtering by latest snapshot
    create index if not exists idx_trails_dataset_version on trails(dataset_version);
  end if;
end$$;
