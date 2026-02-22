from __future__ import annotations
import random
import time
from typing import Any, Callable, Dict, List, Optional, Tuple, TypeVar
import numpy as np
import pandas as pd
from supabase import Client

T = TypeVar("T")

def _retry(fn: Callable[[], T], *, retries: int = 4, base_s: float = 2.0, max_s: float = 30.0) -> T:
    """Execute fn with exponential backoff on failure."""
    for attempt in range(1, retries + 1):
        try:
            return fn()
        except Exception:
            if attempt == retries:
                raise
            wait = min(max_s, base_s * (2 ** (attempt - 1))) + random.uniform(0, 1)
            print(f"  ⚠️  Supabase retry {attempt}/{retries} in {wait:.1f}s")
            time.sleep(wait)
    raise RuntimeError("unreachable")

def _sanitize_value(v):
    """Convert non-JSON-serializable values to JSON-safe Python types."""
    if isinstance(v, np.ndarray):
        return v.tolist()
    if isinstance(v, (np.integer,)):
        return int(v)
    if isinstance(v, (np.floating,)):
        if np.isnan(v) or np.isinf(v):
            return None
        return float(v)
    if isinstance(v, np.bool_):
        return bool(v)
    if isinstance(v, float) and (np.isnan(v) or np.isinf(v)):
        return None
    return v

# Columns that are integer in the Supabase schema.
# Pandas converts these to float64 when any row has NaN, so we must cast back.
_INT_COLUMNS = {
    "rank", "num_votes", "elevation", "highest_point", "crowd_level",
    "derived_gain_ft", "osm_id",
}

def _coerce_int_columns(record: Dict[str, Any]) -> Dict[str, Any]:
    """Cast known-integer columns from float back to int (or None)."""
    for col in _INT_COLUMNS:
        v = record.get(col)
        if v is None:
            continue
        if isinstance(v, float):
            record[col] = int(v) if v == v else None  # NaN != NaN
    return record

def upsert_df(sb: Client, table: str, df: pd.DataFrame, *, conflict: Optional[str] = None, chunk_size: int = 500) -> int:
    if df.empty:
        return 0
    raw = df.to_dict(orient="records")
    records = [_coerce_int_columns({k: _sanitize_value(v) for k, v in row.items()}) for row in raw]
    total = 0
    for i in range(0, len(records), chunk_size):
        chunk = records[i:i+chunk_size]
        def _upsert(c=chunk):
            q = sb.table(table).upsert(c, on_conflict=conflict) if conflict else sb.table(table).upsert(c)
            q.execute()
        _retry(_upsert)
        total += len(chunk)
    return total

def insert_dataset_version(sb: Client, dataset_version: str, notes: str = "") -> None:
    _retry(lambda: sb.table("dataset_versions").upsert(
        {"dataset_version": dataset_version, "notes": notes}, on_conflict="dataset_version"
    ).execute())

def get_crawl_state(sb: Client, urls: List[str]) -> Dict[str, Dict[str, Any]]:
    # Supabase IN filter has size limits; chunk it
    state: Dict[str, Dict[str, Any]] = {}
    chunk = 200
    for i in range(0, len(urls), chunk):
        batch = urls[i:i+chunk]
        resp = _retry(lambda b=batch: sb.table("wta_crawl_state").select("*").in_("url", b).execute())
        for row in (resp.data or []):
            state[row["url"]] = row
    return state

def upsert_crawl_state(sb: Client, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        return
    chunk = 500
    for i in range(0, len(rows), chunk):
        batch = rows[i:i+chunk]
        _retry(lambda b=batch: sb.table("wta_crawl_state").upsert(b, on_conflict="url").execute())

def insert_pipeline_run(sb: Client, payload: Dict[str, Any]) -> None:
    _retry(lambda: sb.table("pipeline_runs").upsert(payload, on_conflict="run_id").execute())

def mark_all_trails_dataset_version(sb: Client, dataset_version: str) -> int:
    """
    Stamp all rows in trails with the latest dataset_version so consumers that
    filter by latest version still see a complete table, not just changed rows.
    """
    total = 0
    # PostgREST update requires a filter. Run two passes:
    # 1) rows with NULL dataset_version
    # 2) rows with dataset_version != current
    resp_null = _retry(lambda: sb.table("trails").update(
        {"dataset_version": dataset_version}
    ).is_("dataset_version", "null").execute())
    total += len(resp_null.data or [])

    resp_neq = _retry(lambda: sb.table("trails").update(
        {"dataset_version": dataset_version}
    ).neq("dataset_version", dataset_version).execute())
    total += len(resp_neq.data or [])
    return total
