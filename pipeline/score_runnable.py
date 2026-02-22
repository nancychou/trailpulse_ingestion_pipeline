from __future__ import annotations
import numpy as np
import pandas as pd

# Simple runnable proxy:
# - penalize steepness (p95 grade)
# - penalize technical surface
# - lightly penalize climb density
#
# If you already compute DEM-based runnable%, replace this with your DEM pipeline.

def surface_penalty(val) -> float:
    mapping = {"Paved": 0.0, "Gravel": 0.2, "Dirt/Soil": 0.4}
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return 0.6
    return mapping.get(str(val).strip(), 0.6)

def add_runnable_pct(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    # distance: prefer WTA-crawled 'distance', then OSM-derived
    out["distance_mi"] = pd.to_numeric(out.get("distance"), errors="coerce")
    if "osm_distance_mi" in out.columns:
        out["distance_mi"] = out["distance_mi"].fillna(pd.to_numeric(out["osm_distance_mi"], errors="coerce"))

    # gain: prefer WTA-crawled 'elevation', then OSM-derived
    out["gain_ft"] = pd.to_numeric(out.get("elevation"), errors="coerce")
    if "derived_gain_ft" in out.columns:
        out["gain_ft"] = out["gain_ft"].fillna(pd.to_numeric(out["derived_gain_ft"], errors="coerce"))
    out.loc[out["distance_mi"] <= 0, "distance_mi"] = np.nan
    out["gain_per_mile_ft"] = out["gain_ft"] / out["distance_mi"]

    out["max_grade_p95"] = pd.to_numeric(out.get("max_grade_p95"), errors="coerce")

    surf = out.get("surface_primary")
    if surf is None and "surface" in out.columns:
        surf = out["surface"]
    out["surface_penalty"] = (surf if surf is not None else pd.Series([None]*len(out))).apply(surface_penalty)

    # base runnable score in [0,1]
    steep = (out["max_grade_p95"].fillna(0) / 25.0).clip(0, 2)
    climb = (out["gain_per_mile_ft"].fillna(0) / 1500.0).clip(0, 2)
    tech = out["surface_penalty"].fillna(0.6).clip(0, 1)

    # Higher steep/climb/tech => less runnable
    runnable = 1.0 - (0.55*steep + 0.25*climb + 0.20*tech) / 2.0
    runnable_pct = np.clip(runnable * 100.0, 0.0, 100.0)

    out["trail_runnable_pct"] = np.round(runnable_pct.astype(float), 1)
    return out
