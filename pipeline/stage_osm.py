from __future__ import annotations
from dataclasses import asdict
from typing import Any, Dict, List, Optional

import pandas as pd

from enrich_wta_with_osm import enrich_row

# Columns that are genuinely new from OSM enrichment (not already in TrailRecord)
OSM_NEW_COLS = [
    "osm_id",
    "osm_name",
    "osm_type",
    "match_confidence",
    "osm_distance_mi",
    "max_grade_p95",
    "derived_gain_ft",
    "surface_primary",
    "surface_breakdown",
    "geometry_polyline",
]

def enrich_df_with_osm(df: pd.DataFrame, *, store_polyline: bool = True) -> pd.DataFrame:
    if df.empty:
        return df
    records = df.where(pd.notnull(df), None).to_dict(orient="records")
    enriched = [asdict(enrich_row(r, store_polyline=store_polyline)) for r in records]
    out = pd.DataFrame(enriched)

    # Only pick the new OSM columns (plus id for joining)
    keep = ["id"] + [c for c in OSM_NEW_COLS if c in out.columns]
    osm_subset = out[keep]

    merged = df.merge(osm_subset, on="id", how="left")
    return merged

