from __future__ import annotations
import numpy as np
import pandas as pd

from sklearn.impute import SimpleImputer
from sklearn.linear_model import Ridge
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

def surface_penalty(val) -> float:
    mapping = {"Paved": 0.0, "Gravel": 0.2, "Dirt/Soil": 0.4}
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return 0.5
    return mapping.get(str(val).strip(), 0.5)

def map_wta_label(label):
    if label is None or (isinstance(label, float) and np.isnan(label)):
        return np.nan
    label = str(label).strip()
    return {
        "Easy": 0.0,
        "Easy/Moderate": 0.5,
        "Moderate": 1.0,
        "Moderate/Hard": 1.5,
        "Hard": 2.0,
    }.get(label, np.nan)

def add_difficulty_score(df: pd.DataFrame) -> pd.DataFrame:
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

    out["wta_diff_level"] = out.get("calculated_difficulty")
    if out["wta_diff_level"] is not None:
        out["wta_diff_level"] = out["wta_diff_level"].apply(map_wta_label)
    labeled = out["wta_diff_level"].notna() if "wta_diff_level" in out.columns else pd.Series([False]*len(out))

    features = ["gain_per_mile_ft", "max_grade_p95", "surface_penalty", "distance_mi"]
    X = out[features]

    # Train only if we have labels; else use weighted heuristic.
    if labeled.any():
        model = Pipeline([
            ("imputer", SimpleImputer(strategy="median")),
            ("scaler", StandardScaler()),
            ("ridge", Ridge(alpha=1.0)),
        ])
        model.fit(X.loc[labeled], out.loc[labeled, "wta_diff_level"])
        pred = model.predict(X)
        # map 0..2 -> 0..10 (with clamping)
        score_0_10 = np.clip((pred / 2.0) * 10.0, 0.0, 10.0)
    else:
        # fallback heuristic
        z = (
            0.45 * (out["gain_per_mile_ft"] / 1000.0).fillna(0) +
            0.35 * (out["max_grade_p95"] / 20.0).fillna(0) +
            0.10 * out["surface_penalty"].fillna(0.5) +
            0.10 * (out["distance_mi"] / 15.0).fillna(0)
        )
        score_0_10 = np.clip(z * 10.0, 0.0, 10.0)

    out["difficulty_score_0_10"] = np.round(score_0_10.astype(float), 1)
    return out
