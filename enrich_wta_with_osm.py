#!/usr/bin/env python3
"""
TrailPulse Enrichment Pass (WTA -> OSM + Open-Elevation)

Reads a WTA-exported CSV (from your WTA crawler) that includes at least:
- id
- name
- trailhead_lat
- trailhead_lng
(Optional but useful)
- distance (miles)
- elevation (ft gain)

Enriches each trail with:
- osm_type / osm_id / match_confidence
- osm_distance_mi (derived from geometry)
- max_grade_p95 (p95 absolute grade %, smoothed by resampling)
- surface_primary + surface_breakdown (length-weighted, from OSM tags)
- geometry_polyline (encoded polyline; optional)

Outputs:
- <out>.csv
- <out>.json

Usage:
  python enrich_wta_with_osm.py --in wta_trails.csv --out trails_enriched --limit 100

Notes:
- This script is best-effort. OSM coverage varies by trail.
- It uses public Overpass + Open-Elevation endpoints; be kind with rate limits.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import random
import re
import time
from dataclasses import dataclass, asdict
from difflib import SequenceMatcher
from typing import Any, Dict, List, Optional, Tuple

import requests


# ------------------------------ Utils ------------------------------

def haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Distance in meters."""
    R = 6371000.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dl/2)**2
    return 2 * R * math.asin(math.sqrt(a))


def polyline_encode(coords: List[Tuple[float, float]]) -> str:
    """Google Encoded Polyline Algorithm Format."""
    def encode_value(v: int) -> str:
        v = ~(v << 1) if v < 0 else (v << 1)
        out = []
        while v >= 0x20:
            out.append(chr((0x20 | (v & 0x1f)) + 63))
            v >>= 5
        out.append(chr(v + 63))
        return "".join(out)

    last_lat = 0
    last_lng = 0
    res = []
    for lat, lng in coords:
        ilat = int(round(lat * 1e5))
        ilng = int(round(lng * 1e5))
        dlat = ilat - last_lat
        dlng = ilng - last_lng
        res.append(encode_value(dlat))
        res.append(encode_value(dlng))
        last_lat, last_lng = ilat, ilng
    return "".join(res)


def seq_ratio(a: str, b: str) -> float:
    return SequenceMatcher(None, a.lower(), b.lower()).ratio()


def normalize_name(s: str) -> str:
    s = s.strip().lower()
    s = re.sub(r"\btrail\b", "", s)
    s = re.sub(r"[^a-z0-9\s\-]", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def p95(values: List[float]) -> Optional[float]:
    if not values:
        return None
    values = sorted(values)
    idx = int(math.floor(0.95 * (len(values) - 1)))
    return values[idx]


# ------------------------------ Networking ------------------------------

OVERPASS_ENDPOINTS = [
    "https://overpass-api.de/api/interpreter",
    "https://overpass.kumi.systems/api/interpreter",
    "https://overpass.nchc.org.tw/api/interpreter",
]

OPEN_ELEVATION_ENDPOINTS = [
    "https://api.open-elevation.com/api/v1/lookup",
    "https://open-elevation.com/api/v1/lookup",
]

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/121.0.0.0 Safari/537.36"
    )
}

session = requests.Session()
session.headers.update(DEFAULT_HEADERS)


def post_with_retry(urls: List[str], data: Any, timeout_s: int = 45, retries: int = 3) -> Optional[requests.Response]:
    last_err = None
    for attempt in range(1, retries + 1):
        url = urls[(attempt - 1) % len(urls)]
        try:
            resp = session.post(url, data=data, timeout=timeout_s)
            if resp.status_code == 200:
                return resp
            if resp.status_code in (429, 500, 502, 503, 504):
                wait = min(30, (2 ** attempt) + random.uniform(0, 2))
                print(f"  HTTP {resp.status_code} from {url}; retry {attempt}/{retries} in {int(wait)}s")
                time.sleep(wait)
                continue
            print(f"  HTTP {resp.status_code} from {url}; giving up")
            last_err = Exception(f"HTTP {resp.status_code}")
        except requests.RequestException as e:
            last_err = e
            wait = min(30, (2 ** attempt) + random.uniform(0, 2))
            print(f"  Request error {e}; retry {attempt}/{retries} in {int(wait)}s")
            time.sleep(wait)
    if last_err:
        print(f"  Failed after retries: {last_err}")
    return None


# ------------------------------ OSM Extraction ------------------------------

SURFACE_MAP = {
    "asphalt": "Pavement",
    "paved": "Pavement",
    "concrete": "Pavement",
    "concrete:plates": "Pavement",
    "gravel": "Gravel",
    "fine_gravel": "Gravel",
    "compacted": "Gravel",
    "dirt": "Dirt/Soil",
    "earth": "Dirt/Soil",
    "ground": "Dirt/Soil",
    "mud": "Mud",
    "sand": "Sand",
    "rock": "Rocky/Technical",
    "stone": "Rocky/Technical",
    "pebblestone": "Rocky/Technical",
    "grass": "Dirt/Soil",
    "wood": "Boardwalk/Wood",
    "boardwalk": "Boardwalk/Wood",
}


def overpass_candidates(lat: float, lng: float, radius_m: int = 1200) -> Dict[str, Any]:
    query = f"""
    [out:json][timeout:60];
    (
      way(around:{radius_m},{lat},{lng})["highway"~"path|footway|track"]["access"!="private"];
      way(around:{radius_m},{lat},{lng})["highway"="bridleway"]["access"!="private"];
      relation(around:{radius_m},{lat},{lng})["route"="hiking"];
    );
    out body geom;
    """
    resp = post_with_retry(OVERPASS_ENDPOINTS, data=query)
    if not resp:
        return {"elements": []}
    try:
        return resp.json()
    except Exception:
        return {"elements": []}


def geometry_length_m(coords: List[Tuple[float, float]]) -> float:
    if len(coords) < 2:
        return 0.0
    dist = 0.0
    for (a, b) in zip(coords, coords[1:]):
        dist += haversine_m(a[0], a[1], b[0], b[1])
    return dist


def resample_polyline(coords: List[Tuple[float, float]], step_m: float = 40.0, max_points: int = 220) -> List[Tuple[float, float]]:
    if len(coords) <= 2:
        return coords
    out = [coords[0]]
    acc = 0.0
    last = coords[0]
    for pt in coords[1:]:
        d = haversine_m(last[0], last[1], pt[0], pt[1])
        acc += d
        if acc >= step_m:
            out.append(pt)
            acc = 0.0
            last = pt
        else:
            last = pt
        if len(out) >= max_points:
            break
    if out[-1] != coords[-1] and len(out) < max_points:
        out.append(coords[-1])
    return out


def _get_element_coords(el: Dict[str, Any]) -> List[Tuple[float, float]]:
    """Extract coords from either a way (geometry) or relation (members)."""
    if el.get("type") == "relation":
        return extract_relation_coords(el)
    return extract_way_coords(el)


def score_candidate(wta_name: str, wta_dist_mi: Optional[float], lat: float, lng: float, el: Dict[str, Any]) -> float:
    tags = el.get("tags", {}) or {}
    osm_name = tags.get("name") or tags.get("official_name") or tags.get("alt_name") or ""
    name_score = seq_ratio(normalize_name(wta_name), normalize_name(osm_name)) if osm_name else 0.1

    coords = _get_element_coords(el)

    prox_score = 0.0
    if coords:
        mind = min(haversine_m(lat, lng, a, b) for a, b in coords)
        prox_score = max(0.0, 1.0 - min(1.0, mind / 800.0))

    dist_score = 0.0
    if coords and wta_dist_mi:
        osm_mi = geometry_length_m(coords) / 1609.344
        ratio = min(osm_mi, wta_dist_mi) / max(osm_mi, wta_dist_mi) if osm_mi and wta_dist_mi else 0.0
        dist_score = ratio

    # Prefer relations (complete trails) over individual ways (fragments)
    type_bonus = 0.20 if el.get("type") == "relation" else 0.05
    hw = tags.get("highway", "")
    trail_bonus = 0.10 if hw in ("path", "track", "footway", "bridleway") else 0.0
    sac_bonus = 0.05 if tags.get("sac_scale") else 0.0

    return 0.50 * name_score + 0.20 * prox_score + 0.15 * dist_score + type_bonus + trail_bonus + sac_bonus


def pick_best_osm_feature(wta_name: str, wta_dist_mi: Optional[float], lat: float, lng: float) -> Tuple[Optional[Dict[str, Any]], float]:
    best_el = None
    best_score = -1.0
    for radius in (800, 1200, 1800, 2600):
        data = overpass_candidates(lat, lng, radius_m=radius)
        for el in data.get("elements", []) or []:
            if el.get("type") == "way" and not el.get("geometry"):
                continue
            s = score_candidate(wta_name, wta_dist_mi, lat, lng, el)
            if s > best_score:
                best_score, best_el = s, el
        if best_score >= 0.72:
            break
        time.sleep(random.uniform(0.5, 1.2))
    return best_el, max(0.0, min(1.0, best_score))


def extract_way_coords(el: Dict[str, Any]) -> List[Tuple[float, float]]:
    geom = el.get("geometry") or []
    coords: List[Tuple[float, float]] = []
    for p in geom:
        if isinstance(p, dict) and "lat" in p and "lon" in p:
            coords.append((float(p["lat"]), float(p["lon"])))
    return coords


def extract_relation_coords(el: Dict[str, Any]) -> List[Tuple[float, float]]:
    """
    Extract ordered coordinates from a hiking relation by stitching member ways.
    Handles direction: reverses ways if endpoints don't connect.
    """
    members = el.get("members") or []
    way_segments: List[List[Tuple[float, float]]] = []
    for m in members:
        if m.get("type") != "way":
            continue
        geom = m.get("geometry") or []
        seg = []
        for p in geom:
            if isinstance(p, dict) and "lat" in p and "lon" in p:
                seg.append((float(p["lat"]), float(p["lon"])))
        if seg:
            way_segments.append(seg)

    if not way_segments:
        return []

    # Stitch segments in order, reversing if needed to connect endpoints
    result = list(way_segments[0])
    for seg in way_segments[1:]:
        if not result or not seg:
            result.extend(seg)
            continue
        tail = result[-1]
        # Check which end of the new segment connects to the tail
        d_fwd = haversine_m(tail[0], tail[1], seg[0][0], seg[0][1])
        d_rev = haversine_m(tail[0], tail[1], seg[-1][0], seg[-1][1])
        if d_rev < d_fwd:
            seg = list(reversed(seg))
        # Skip the first point if it's essentially the same as the tail (avoid dups)
        if haversine_m(tail[0], tail[1], seg[0][0], seg[0][1]) < 5.0:
            seg = seg[1:]
        result.extend(seg)

    return result


def stitch_ways_from_trailhead(
    lat: float,
    lng: float,
    wta_dist_mi: Optional[float],
    candidates: List[Dict[str, Any]],
) -> Tuple[List[Tuple[float, float]], List[Dict[str, Any]]]:
    """
    Stitch connected ways outward from the trailhead until accumulated
    distance ≈ WTA's reported distance. Returns (coords, used_elements).
    """
    # Filter to only ways with geometry
    ways = []
    for el in candidates:
        if el.get("type") != "way":
            continue
        coords = extract_way_coords(el)
        if len(coords) >= 2:
            ways.append((el, coords))

    if not ways:
        return [], []

    target_m = (wta_dist_mi or 5.0) * 1609.344  # default ~5 mi if unknown

    # Find the way whose start or end is closest to trailhead
    def endpoint_dist(coords: List[Tuple[float, float]]) -> float:
        return min(
            haversine_m(lat, lng, coords[0][0], coords[0][1]),
            haversine_m(lat, lng, coords[-1][0], coords[-1][1]),
        )

    ways.sort(key=lambda w: endpoint_dist(w[1]))
    seed_el, seed_coords = ways[0]

    # Orient seed so the end closest to trailhead is at index 0
    if haversine_m(lat, lng, seed_coords[-1][0], seed_coords[-1][1]) < \
       haversine_m(lat, lng, seed_coords[0][0], seed_coords[0][1]):
        seed_coords = list(reversed(seed_coords))

    result = list(seed_coords)
    used = {id(seed_el)}
    used_elements = [seed_el]
    accumulated_m = geometry_length_m(result)

    # Greedily stitch connected ways
    max_iters = 50
    for _ in range(max_iters):
        if accumulated_m >= target_m * 0.9:
            break
        tail = result[-1]
        best_way = None
        best_coords = None
        best_dist = 80.0  # max gap in meters to consider "connected"
        for el, coords in ways:
            if id(el) in used:
                continue
            d_start = haversine_m(tail[0], tail[1], coords[0][0], coords[0][1])
            d_end = haversine_m(tail[0], tail[1], coords[-1][0], coords[-1][1])
            min_d = min(d_start, d_end)
            if min_d < best_dist:
                best_dist = min_d
                best_way = el
                if d_end < d_start:
                    best_coords = list(reversed(coords))
                else:
                    best_coords = list(coords)
        if best_way is None or best_coords is None:
            break
        used.add(id(best_way))
        used_elements.append(best_way)
        # Skip duplicate start point
        if haversine_m(tail[0], tail[1], best_coords[0][0], best_coords[0][1]) < 5.0:
            best_coords = best_coords[1:]
        result.extend(best_coords)
        accumulated_m = geometry_length_m(result)

    return result, used_elements


def surface_from_tags(tags: Dict[str, Any]) -> Optional[str]:
    s = tags.get("surface")
    if not s:
        return None
    s = str(s).strip().lower()
    return SURFACE_MAP.get(s)


def compute_surface_breakdown(
    elements: List[Dict[str, Any]],
) -> Tuple[Optional[str], Optional[str]]:
    """
    Compute length-weighted surface breakdown from multiple way elements.
    Returns (surface_primary, surface_breakdown_json).
    """
    surface_lengths: Dict[str, float] = {}
    total_length = 0.0
    for el in elements:
        if el.get("type") != "way":
            continue
        tags = el.get("tags", {}) or {}
        surf = surface_from_tags(tags)
        if not surf:
            continue
        coords = extract_way_coords(el)
        seg_len = geometry_length_m(coords) if len(coords) >= 2 else 0.0
        if seg_len <= 0:
            continue
        surface_lengths[surf] = surface_lengths.get(surf, 0.0) + seg_len
        total_length += seg_len

    if not surface_lengths or total_length <= 0:
        return None, None

    breakdown = {k: round(v / total_length, 2) for k, v in sorted(
        surface_lengths.items(), key=lambda x: -x[1]
    )}
    primary = max(surface_lengths, key=lambda k: surface_lengths[k])
    return primary, json.dumps(breakdown)


# ------------------------------ Elevation + Grade ------------------------------

def open_elevation_lookup(coords: List[Tuple[float, float]], batch_size: int = 90) -> Optional[List[float]]:
    if not coords:
        return None
    elevations: List[float] = []
    for i in range(0, len(coords), batch_size):
        chunk = coords[i:i+batch_size]
        payload = {"locations": [{"latitude": lat, "longitude": lng} for lat, lng in chunk]}
        payload_json = json.dumps(payload)

        resp = None
        for attempt in range(1, 4):
            url = OPEN_ELEVATION_ENDPOINTS[(attempt - 1) % len(OPEN_ELEVATION_ENDPOINTS)]
            try:
                r = session.post(url, data=payload_json, headers={"Content-Type": "application/json"}, timeout=45)
                if r.status_code == 200:
                    resp = r
                    break
                if r.status_code in (429, 500, 502, 503, 504):
                    wait = min(30, (2 ** attempt) + random.uniform(0, 2))
                    time.sleep(wait)
                    continue
            except requests.RequestException:
                wait = min(30, (2 ** attempt) + random.uniform(0, 2))
                time.sleep(wait)
        if not resp:
            return None
        try:
            data = resp.json()
            results = data.get("results", [])
            elevations.extend([float(x["elevation"]) for x in results])
        except Exception:
            return None
        time.sleep(random.uniform(0.4, 0.9))
    return elevations


def compute_max_grade_p95(coords: List[Tuple[float, float]]) -> Tuple[Optional[float], Optional[int]]:
    if len(coords) < 3:
        return None, None
    coords_rs = resample_polyline(coords, step_m=45.0, max_points=220)
    elev_m = open_elevation_lookup(coords_rs)
    if not elev_m or len(elev_m) != len(coords_rs):
        return None, None

    grades: List[float] = []
    gain_m = 0.0
    for (a, b), (ea, eb) in zip(zip(coords_rs, coords_rs[1:]), zip(elev_m, elev_m[1:])):
        dist = haversine_m(a[0], a[1], b[0], b[1])
        if dist <= 1.0:
            continue
        de = eb - ea
        if de > 0:
            gain_m += de
        g = abs(de) / dist * 100.0
        if g <= 60.0:
            grades.append(g)

    mg = p95(grades)
    gain_ft = int(round(gain_m * 3.28084)) if gain_m else None
    return (round(mg, 1) if mg is not None else None), gain_ft


# ------------------------------ Records ------------------------------

@dataclass
class EnrichedTrail:
    id: str
    name: str
    source_url: str = ""
    trailhead_lat: Optional[float] = None
    trailhead_lng: Optional[float] = None
    location: str = ""

    wta_distance_mi: Optional[float] = None
    wta_gain_ft: Optional[int] = None

    osm_type: Optional[str] = None
    osm_id: Optional[int] = None
    osm_name: Optional[str] = None
    match_confidence: Optional[float] = None

    osm_distance_mi: Optional[float] = None
    max_grade_p95: Optional[float] = None
    derived_gain_ft: Optional[int] = None

    surface_primary: Optional[str] = None
    surface_breakdown: Optional[str] = None

    geometry_polyline: Optional[str] = None


def parse_float(x: Any) -> Optional[float]:
    try:
        if x is None or x == "":
            return None
        return float(x)
    except Exception:
        return None


def parse_int(x: Any) -> Optional[int]:
    try:
        if x is None or x == "":
            return None
        return int(float(x))
    except Exception:
        return None


def read_wta_csv(path: str) -> List[Dict[str, Any]]:
    with open(path, "r", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def enrich_row(row: Dict[str, Any], *, store_polyline: bool = True) -> EnrichedTrail:
    tid = row.get("id") or ""
    name = row.get("name") or ""
    url = row.get("source_url") or row.get("url") or ""
    location = row.get("location") or ""

    lat = parse_float(row.get("trailhead_lat") or row.get("lat") or row.get("latitude"))
    lng = parse_float(row.get("trailhead_lng") or row.get("lng") or row.get("longitude"))

    wta_dist = parse_float(row.get("distance")) or parse_float(row.get("wta_distance_mi"))
    wta_gain = parse_int(row.get("elevation")) or parse_int(row.get("wta_gain_ft"))

    out = EnrichedTrail(
        id=tid,
        name=name,
        source_url=url,
        location=location,
        trailhead_lat=lat,
        trailhead_lng=lng,
        wta_distance_mi=wta_dist,
        wta_gain_ft=wta_gain,
    )

    if lat is None or lng is None or not name:
        return out

    best, conf = pick_best_osm_feature(name, wta_dist, lat, lng)
    if not best:
        out.match_confidence = 0.0
        return out

    tags = best.get("tags", {}) or {}
    out.osm_type = best.get("type")
    out.osm_id = best.get("id")
    out.osm_name = tags.get("name")
    out.match_confidence = round(conf, 3)

    coords: List[Tuple[float, float]] = []
    surface_elements: List[Dict[str, Any]] = []

    if best.get("type") == "relation":
        # Full trail geometry from hiking relation
        coords = extract_relation_coords(best)
        # Collect member way elements for surface breakdown
        for m in (best.get("members") or []):
            if m.get("type") == "way" and m.get("tags"):
                surface_elements.append(m)
        if not surface_elements:
            # Relation-level tags as fallback
            surface_elements = [best]
    elif best.get("type") == "way":
        coords = extract_way_coords(best)
        surface_elements = [best]

    # If we got a single way that's much shorter than the WTA distance,
    # try stitching connected ways from the trailhead
    if coords and wta_dist:
        osm_mi = geometry_length_m(coords) / 1609.344
        if osm_mi < wta_dist * 0.4 and best.get("type") == "way":
            # Re-query all candidates and stitch
            all_data = overpass_candidates(lat, lng, radius_m=2600)
            all_elements = all_data.get("elements") or []
            stitched, used_els = stitch_ways_from_trailhead(
                lat, lng, wta_dist, all_elements
            )
            if stitched and geometry_length_m(stitched) > geometry_length_m(coords):
                coords = stitched
                surface_elements = used_els
                out.osm_type = "stitched"

    if coords:
        out.osm_distance_mi = round(geometry_length_m(coords) / 1609.344, 2)

        # Length-weighted surface breakdown
        if surface_elements:
            primary, breakdown = compute_surface_breakdown(surface_elements)
            if primary:
                out.surface_primary = primary
                out.surface_breakdown = breakdown
        else:
            surf = surface_from_tags(tags)
            if surf:
                out.surface_primary = surf
                out.surface_breakdown = json.dumps({surf: 1.0})

        mg, gain_ft = compute_max_grade_p95(coords)
        out.max_grade_p95 = mg
        out.derived_gain_ft = gain_ft

        if store_polyline:
            try:
                coords_small = resample_polyline(coords, step_m=60.0, max_points=160)
                out.geometry_polyline = polyline_encode(coords_small)
            except Exception:
                out.geometry_polyline = None

    return out


def write_csv(path: str, items: List[EnrichedTrail]) -> None:
    if not items:
        return
    fieldnames = list(asdict(items[0]).keys())
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for it in items:
            w.writerow(asdict(it))


def write_json(path: str, items: List[EnrichedTrail]) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump([asdict(it) for it in items], f, indent=2, ensure_ascii=False)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", required=True, help="Input WTA CSV file path")
    ap.add_argument("--out", dest="out", default="trails_enriched", help="Output basename (no extension)")
    ap.add_argument("--limit", type=int, default=100, help="Max trails to enrich")
    ap.add_argument("--sleep", type=float, default=1.2, help="Sleep seconds between trails")
    ap.add_argument("--no-polyline", action="store_true", help="Do not store encoded polyline")
    args = ap.parse_args()

    rows = read_wta_csv(args.inp)
    if not rows:
        print("No rows found in input.")
        return

    enriched: List[EnrichedTrail] = []
    total = min(args.limit, len(rows))

    for idx, row in enumerate(rows[:total], start=1):
        print(f"[{idx}/{total}] Enriching: {row.get('name','')}")
        t0 = time.time()
        try:
            rec = enrich_row(row, store_polyline=not args.no_polyline)
            enriched.append(rec)
            print(f"  ✓ conf={rec.match_confidence} osm={rec.osm_type}:{rec.osm_id} surf={rec.surface_primary} max_grade_p95={rec.max_grade_p95}")
        except Exception as e:
            print(f"  ✗ failed: {e}")
            enriched.append(EnrichedTrail(id=row.get("id", ""), name=row.get("name", ""), source_url=row.get("source_url", "")))
        elapsed = time.time() - t0
        wait = max(0.0, args.sleep - elapsed) + random.uniform(0.1, 0.4)
        time.sleep(wait)

    out_csv = f"{args.out}.csv"
    out_json = f"{args.out}.json"
    write_csv(out_csv, enriched)
    write_json(out_json, enriched)

    print("\nDone")
    print(f"Rows: {len(enriched)}")
    print(f"CSV:  {out_csv}")
    print(f"JSON: {out_json}")


if __name__ == "__main__":
    main()
