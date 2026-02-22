#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple

import pandas as pd
import requests

from pipeline.utils import fetch_sitemap_urls, filter_hike_urls, dataset_version_now, sha256_text, utc_now_iso
from pipeline.supabase_client import get_supabase
from pipeline.db import (
    insert_dataset_version,
    get_crawl_state,
    upsert_crawl_state,
    upsert_df,
    insert_pipeline_run,
    mark_all_trails_dataset_version,
)
from pipeline.stage_wta import crawl_changed_wta_pages
from pipeline.stage_osm import enrich_df_with_osm
from pipeline.score_difficulty import add_difficulty_score
from pipeline.score_runnable import add_runnable_pct

def new_run_id() -> str:
    return f"run_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}_{uuid.uuid4().hex[:8]}"

def _crawl_state_sort_key(url: str, state: Dict[str, Dict[str, Any]]) -> Tuple[int, str]:
    row = state.get(url, {})
    last = row.get("last_crawled_at")
    if not last:
        return (0, "")
    return (1, str(last))

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--schema-version", default="v1.0")
    ap.add_argument("--max-pages", type=int, default=int(os.getenv("MAX_PAGES_PER_RUN", "50")))
    ap.add_argument("--crawl-delay", type=int, default=int(os.getenv("WTA_CRAWL_DELAY_S", "60")))
    ap.add_argument("--save-artifacts", action="store_true")
    args = ap.parse_args()

    run_id = new_run_id()
    started_at = utc_now_iso()
    dataset_version = dataset_version_now()

    print("=" * 60)
    print("🚀 TrailPulse Weekly Pipeline")
    print(f"   Run ID:  {run_id}")
    print(f"   Version: {dataset_version}")
    print(f"   Config:  max_pages={args.max_pages}, crawl_delay={args.crawl_delay}s")
    print("=" * 60)

    print("\n📡 Connecting to Supabase...")
    sb = get_supabase()

    # Ensure dataset_version row exists
    insert_dataset_version(sb, dataset_version, notes="weekly_ingest")
    print("   ✅ Connected & dataset version registered")

    stats: Dict[str, Any] = {"run_id": run_id, "dataset_version": dataset_version}

    try:
        print("\n🗺️  Fetching WTA sitemap...")
        session = requests.Session()
        urls = filter_hike_urls(fetch_sitemap_urls(session))
        stats["sitemap_hike_urls"] = len(urls)
        print(f"   Found {len(urls)} hike URLs in sitemap")

        print("\n🔍 Checking crawl state...")
        # Pull existing crawl state to decide what to crawl.
        state = get_crawl_state(sb, urls)
        stats["known_urls"] = len(state)

        # Simple incremental strategy:
        # - prioritize URLs never crawled
        # - then re-crawl a small tail of previously crawled URLs (to detect changes)
        never = [u for u in urls if u not in state]
        seen = [u for u in urls if u in state]

        # Re-crawl least-recently crawled seen URLs first (fairness over time)
        seen_sorted = sorted(seen, key=lambda u: _crawl_state_sort_key(u, state))
        sample_seen = seen_sorted[: max(0, args.max_pages - len(never))]

        to_crawl = (never + sample_seen)[: args.max_pages]
        stats["to_crawl"] = len(to_crawl)

        print(f"   Already crawled: {len(seen)} | Never crawled: {len(never)}")
        print(f"   Will crawl: {len(to_crawl)} URLs this run")

        print(f"\n🕷️  Crawling {len(to_crawl)} trail pages (delay={args.crawl_delay}s between pages)...")
        df_wta, failed_urls = crawl_changed_wta_pages(
            to_crawl,
            max_pages=args.max_pages,
            crawl_delay_s=args.crawl_delay,
        )
        stats["crawled_rows"] = int(len(df_wta))
        stats["failed_urls"] = int(len(failed_urls))
        print(f"   ✅ Crawled {len(df_wta)} trails")
        if failed_urls:
            print(f"   ⚠️  Failed to parse: {len(failed_urls)} URLs")

        if df_wta.empty and not failed_urls:
            raise RuntimeError("No WTA trails crawled (empty dataframe).")

        # Hash and write crawl state (best-effort). Mark failed URLs too.
        print("\n💾 Saving crawl state...")
        state_rows = []
        changed_ids: List[str] = []
        for _, r in df_wta.iterrows():
            url = r.get("source_url") or r.get("url")
            h = sha256_text(json.dumps(r.to_dict(), sort_keys=True, default=str))
            prev_hash = (state.get(url) or {}).get("last_hash")
            state_rows.append({
                "url": url,
                "last_hash": h,
                "last_crawled_at": started_at,
                "last_status": 200,
                "last_error": None,
            })
            if h != prev_hash:
                rid = r.get("id")
                if rid:
                    changed_ids.append(rid)
        for u in failed_urls:
            state_rows.append({
                "url": u,
                "last_hash": (state.get(u) or {}).get("last_hash"),
                "last_crawled_at": started_at,
                "last_status": 0,
                "last_error": "parse_failed_or_unreachable",
            })
        upsert_crawl_state(sb, state_rows)
        print(f"   ✅ Saved state for {len(state_rows)} URLs")
        stats["changed_rows"] = len(changed_ids)

        if changed_ids:
            df_wta = df_wta[df_wta["id"].isin(changed_ids)].copy()
        else:
            df_wta = df_wta.iloc[0:0].copy()

        print(f"   🔁 Changed/new trails this run: {len(df_wta)}")
        if df_wta.empty:
            marked = mark_all_trails_dataset_version(sb, dataset_version)
            stats["dataset_version_marked_rows"] = int(marked)
            finished_at = utc_now_iso()
            insert_pipeline_run(sb, {
                "run_id": run_id,
                "dataset_version": dataset_version,
                "schema_version": args.schema_version,
                "status": "success",
                "started_at": started_at,
                "finished_at": finished_at,
                "row_count": 0,
                "stats": stats,
                "params": {
                    "max_pages": args.max_pages,
                    "crawl_delay": args.crawl_delay,
                },
                "notes": "no_changed_rows",
            })
            print("\nNo changed rows detected after hashing; skipped enrichment and upsert.")
            print(f"Marked {marked} existing trails with dataset_version={dataset_version}.")
            print(json.dumps({"ok": True, **stats}, indent=2))
            return

        # Enrich
        print(f"\n🌍 Enriching {len(df_wta)} trails with OSM data...")
        df_enriched = enrich_df_with_osm(df_wta, store_polyline=True)
        stats["enriched_rows"] = int(len(df_enriched))
        osm_matched = df_enriched["match_confidence"].notna().sum() if "match_confidence" in df_enriched.columns else 0
        print(f"   ✅ Enriched {len(df_enriched)} trails ({osm_matched} matched in OSM)")

        # Scores
        print("\n📊 Computing scores...")
        df_scored = add_difficulty_score(df_enriched)
        df_scored = add_runnable_pct(df_scored)
        print(f"   ✅ Difficulty scores: {df_scored['difficulty_score_0_10'].notna().sum()}/{len(df_scored)}")
        print(f"   ✅ Runnable scores:   {df_scored['trail_runnable_pct'].notna().sum()}/{len(df_scored)}")

        # Attach dataset_version
        df_scored["dataset_version"] = dataset_version

        # Optional: save artifacts for debugging
        if args.save_artifacts:
            os.makedirs("artifacts", exist_ok=True)
            df_wta.to_parquet(f"artifacts/{run_id}_01_wta.parquet", index=False)
            df_enriched.to_parquet(f"artifacts/{run_id}_02_enriched.parquet", index=False)
            df_scored.to_parquet(f"artifacts/{run_id}_03_scored.parquet", index=False)
            print(f"\n📁 Artifacts saved to artifacts/{run_id}_*.parquet")

        # Upsert into Supabase trails table
        print(f"\n⬆️  Upserting {len(df_scored)} trails to Supabase...")
        # Drop columns that are only used internally for deriving other fields
        # (these are intermediate scoring columns with no Supabase column)
        cols_to_drop = [c for c in [
            "parking",          # raw parking text, replaced by parking_tags/parking_status
            "distance_mi",      # intermediate: duplicate of distance
            "surface_penalty",  # intermediate: scoring input
            "wta_diff_level",   # intermediate: scoring input
        ] if c in df_scored.columns]
        df_upload = df_scored.drop(columns=cols_to_drop)

        # Rename to match existing Supabase column names (frontend uses trailhead_lat/lng)
        df_upload = df_upload.rename(columns={
            "latitude": "trailhead_lat",
            "longitude": "trailhead_lng",
        })

        upserted = upsert_df(sb, "trails", df_upload, conflict="id")
        stats["upserted"] = int(upserted)
        print(f"   ✅ Upserted {upserted} rows")

        # Keep latest dataset_version queryable as a full-table snapshot marker.
        marked = mark_all_trails_dataset_version(sb, dataset_version)
        stats["dataset_version_marked_rows"] = int(marked)
        print(f"   ✅ Marked {marked} existing trails with dataset_version={dataset_version}")

        finished_at = utc_now_iso()
        insert_pipeline_run(sb, {
            "run_id": run_id,
            "dataset_version": dataset_version,
            "schema_version": args.schema_version,
            "status": "success",
            "started_at": started_at,
            "finished_at": finished_at,
            "row_count": int(len(df_scored)),
            "stats": stats,
            "params": {
                "max_pages": args.max_pages,
                "crawl_delay": args.crawl_delay,
            },
            "notes": "",
        })

        print("\n" + "=" * 60)
        print("✅ Pipeline completed successfully!")
        print(f"   Trails crawled:  {stats.get('crawled_rows', 0)}")
        print(f"   Trails upserted: {stats.get('upserted', 0)}")
        print(f"   Duration: {started_at} → {finished_at}")
        print("=" * 60)
        print(json.dumps({"ok": True, **stats}, indent=2))

    except Exception as e:
        finished_at = utc_now_iso()
        stats["error"] = repr(e)
        try:
            insert_pipeline_run(sb, {
                "run_id": run_id,
                "dataset_version": dataset_version,
                "schema_version": args.schema_version,
                "status": "failed",
                "started_at": started_at,
                "finished_at": finished_at,
                "row_count": 0,
                "stats": stats,
                "params": {
                    "max_pages": args.max_pages,
                    "crawl_delay": args.crawl_delay,
                },
                "notes": "exception",
            })
        except Exception:
            pass
        raise

if __name__ == "__main__":
    main()
