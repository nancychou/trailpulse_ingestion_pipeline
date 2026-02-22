#!/usr/bin/env python3
"""TrailPulse: WTA Trail Crawler (robots.txt-friendly)

Enhancements vs earlier version
- Adds a clean, 2-stage pipeline:
  (A) Raw extract from WTA hike HTML (name/location/length/gain/image/parking/description)
  (B) Normalize + enrich (distance/elevation/route_type/surface/water/parking_status/tags/cell/crowd)
- Adds stronger parsing for "feet" as well as "ft"
- Adds description extraction + cleanup (removes "Print Email" noise)
- Adds de-dupe by stable slug id
- Keeps crawl politeness (default ~62s between detail pages)

IMPORTANT
- You MUST read and comply with WTA's robots.txt.
- Keep the crawl delay conservative; do not hammer their site.

Usage
  python crawl_wta_trails_enhanced.py --limit 100 --out trails

"""

from __future__ import annotations

import argparse
import csv
import json
import random
import re
import time
from dataclasses import asdict, dataclass, field
from typing import Dict, List, Optional, Set, Tuple

import requests
from bs4 import BeautifulSoup


# ----------------------------- Data model -----------------------------

@dataclass
class TrailRecord:
    id: str
    name: str

    # ranking/difficulty (you may fill later)
    rank: Optional[int] = None
    difficulty: Optional[float] = None
    calculated_difficulty: Optional[str] = None  # WTA's text difficulty: "Easy", "Easy/Moderate", "Hard", etc.
    rating: Optional[float] = None  # Average star rating (0-5)
    num_votes: Optional[int] = None  # Number of ratings

    # core stats
    distance: Optional[float] = None  # miles
    elevation: Optional[int] = None   # feet gain
    highest_point: Optional[int] = None  # feet elevation at highest point

    # GPS coordinates
    latitude: Optional[float] = None
    longitude: Optional[float] = None

    # categorizations
    surface: Optional[str] = None
    route_type: Optional[str] = None
    water_source: Optional[str] = None

    # assets
    image_url: Optional[str] = None

    # logistics
    parking: Optional[str] = None
    parking_tags: List[str] = field(default_factory=list)

    parking_status: Optional[str] = None
    restrooms: Optional[str] = None  # "Yes", "Vault Toilet", "Pit Toilet", "None", etc.

    # community fields (heuristics)
    cell_coverage: Optional[str] = None
    crowd_level: Optional[int] = None

    # extracted body text for heuristics
    description: Optional[str] = None

    # debug/source
    source_url: str = ""
    raw_features: Optional[str] = None  # WTA features section (lakes, rivers, etc.)


# ----------------------------- Normalize helpers -----------------------------

def _normalize_ws(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())

def _lower(s: str) -> str:
    return (s or "").lower()

def parse_distance_miles(raw_length: str) -> Optional[float]:
    t = _lower(raw_length)
    if not t:
        return None
    m = re.search(r"(\d+(?:\.\d+)?)\s*mile", t)
    return float(m.group(1)) if m else None

def parse_elevation_gain_ft(raw_gain: str) -> Optional[int]:
    t = _lower(raw_gain)
    if not t:
        return None
    m = re.search(r"(\d[\d,]*)\s*(?:feet|ft)\b", t)
    return int(m.group(1).replace(",", "")) if m else None

def parse_highest_point_ft(raw_highpoint: str) -> Optional[int]:
    """Extract highest point elevation in feet from raw text like '4,500 feet' or '3200 ft'"""
    t = _lower(raw_highpoint)
    if not t:
        return None
    m = re.search(r"(\d[\d,]*)\s*(?:feet|ft)\b", t)
    return int(m.group(1).replace(",", "")) if m else None

def detect_restrooms(parking_text: str, description: str = "") -> Optional[str]:
    """Detect restroom facilities from parking/description text"""
    combined = _lower(parking_text + " " + description)
    if not combined:
        return None
    
    # Specific types first
    if "vault toilet" in combined:
        return "Vault Toilet"
    if "pit toilet" in combined:
        return "Pit Toilet"
    if "flush toilet" in combined or "restroom" in combined:
        return "Restrooms"
    if "privy" in combined or "outhouse" in combined:
        return "Outhouse"
    
    # Generic detection
    if "toilet" in combined or "bathroom" in combined:
        return "Yes"
    
    # Explicit no
    if "no toilet" in combined or "no restroom" in combined or "no bathroom" in combined:
        return "None"
    
    return None

ROUTE_TYPE_MAP = [
    ("out-and-back", "Out-and-back"),
    ("out and back", "Out-and-back"),
    ("loop", "Loop"),
    ("point-to-point", "Point-to-point"),
    ("point to point", "Point-to-point"),
    ("one way", "Point-to-point"),
    ("one-way", "Point-to-point"),
]

def normalize_route_type(text: str) -> Optional[str]:
    """Extract route type from text like '5.4 miles, roundtrip' or '3.2 miles one-way'"""
    t = _lower(text)
    if not t:
        return None
    
    # Common WTA patterns in Length field: "X miles, roundtrip" or "X miles one-way"
    for k, v in ROUTE_TYPE_MAP:
        if k in t:
            return v
    return None

def extract_route_type_from_length(raw_length: str) -> Optional[str]:
    """
    WTA typically formats Length as: '4.3 miles, roundtrip' or '3.2 miles one-way'
    Extract the EXACT text after the comma, preserving original case and format.
    Examples:
    - "4.3 miles, roundtrip" → "roundtrip"
    - "5.4 miles, loop" → "loop"  
    - "3.2 miles one-way" → "one-way"
    """
    if not raw_length:
        return None
    
    # Try comma-separated first: "4.3 miles, roundtrip"
    if "," in raw_length:
        parts = raw_length.split(",", 1)
        if len(parts) > 1:
            route_part = parts[1].strip()
            return route_part  # Return exact text from WTA
    
    # Fallback: try space-separated patterns after the number
    # "5.4 miles roundtrip" → extract "roundtrip"
    t = raw_length.strip()
    m = re.search(r"^\d+\.?\d*\s*(?:mile|mi)s?\s+(.+)$", t, re.IGNORECASE)
    if m:
        return m.group(1).strip()
    
    return None

SURFACE_RULES = [
    ("boardwalk", "Boardwalk / Wood"),
    ("wooden bridge", "Boardwalk / Wood"),
    ("paved", "Pavement"),
    ("asphalt", "Pavement"),
    ("concrete", "Pavement"),
    ("scree", "Scree / Talus"),
    ("talus", "Scree / Talus"),
    ("gravel", "Gravel"),
    ("rocky", "Rocky / Technical"),
    ("boulder", "Rocky / Technical"),
    ("boulders", "Rocky / Technical"),
    ("scramble", "Rocky / Technical"),
    ("technical", "Rocky / Technical"),
    ("root", "Rooty"),
    ("rooty", "Rooty"),
    ("sand", "Sand"),
    ("sandy", "Sand"),
    ("mud", "Mud"),
    ("muddy", "Mud"),
    ("snow", "Snow / Ice"),
    ("ice", "Snow / Ice"),
    ("glacier", "Snow / Ice"),
    ("well-maintained", "Maintained Trail"),
    ("groomed", "Maintained Trail"),
]

def detect_surface(text: str) -> Optional[str]:
    """
    Detect trail surface type from description or features.
    WTA descriptions often mention surface conditions.
    """
    t = _lower(text)
    if not t:
        return None
    for key, label in SURFACE_RULES:
        if key in t:
            return label
    
    # Default to dirt/soil if we have text but no specific match
    if len(t) > 50:  # Only default if we have substantial text
        return "Dirt/Soil"
    return None

def detect_water_source(description: str = "", features: str = "") -> Optional[str]:
    """
    Detect water sources from description or WTA features section.
    WTA often has features like 'Lakes', 'Rivers', 'Waterfalls' before parking section.
    """
    combined = _lower(description + " " + features)
    if not combined:
        return None
    
    # Check for explicit no water
    if re.search(r"\bno water\b|\bwithout water\b|\bno (?:re)?fill\b|\bbring (?:all )?water\b", combined):
        return "None"
    
    # Check WTA feature tags first (more reliable)
    feature_lower = _lower(features)
    if "lake" in feature_lower or "alpine lake" in feature_lower:
        return "Natural Lake"
    if "river" in feature_lower or "creek" in feature_lower or "stream" in feature_lower:
        return "Creek/River"
    if "waterfall" in feature_lower:
        return "Creek/River"  # Waterfalls indicate running water
    
    # Check description text
    desc_lower = _lower(description)
    if "campground" in desc_lower and ("water" in desc_lower or "spigot" in desc_lower or "tap" in desc_lower):
        return "Campground"
    
    if re.search(r"\blake\b", desc_lower):
        return "Natural Lake"
    if re.search(r"\briver\b|\bcreek\b|\bstream\b", desc_lower):
        return "Creek/River"
    if "water available" in desc_lower or "refill" in desc_lower or "spigot" in desc_lower:
        return "Any water source"
    
    return None

PARKING_TAG_RULES = [
    ("northwest forest pass", "NW Forest Pass"),
    ("discover pass", "Discover Pass"),
    ("sno-park", "Sno-Park"),
    ("forest pass", "Forest Pass"),
    ("vault toilet", "Restrooms"),
    ("restroom", "Restrooms"),
    ("toilet", "Restrooms"),
    ("trailhead parking", "Trailhead Lot"),
    ("parking lot", "Trailhead Lot"),
    ("street parking", "Street Parking"),
    ("permit", "Permit"),
    ("pass required", "Permit"),
    ("fee", "Paid"),
    ("paid", "Paid"),
]

def extract_parking_tags(parking_text: str) -> List[str]:
    t = _lower(parking_text)
    if not t:
        return []
    tags: List[str] = []
    for key, tag in PARKING_TAG_RULES:
        if key in t and tag not in tags:
            tags.append(tag)
    return tags

def detect_parking_status(parking_text: str) -> Optional[str]:
    t = _lower(parking_text)
    if not t:
        return None
    if "discover pass" in t or "forest pass" in t or "permit" in t or "sno-park" in t or "pass required" in t:
        return "Permit"
    if "fee" in t or "paid" in t or "$" in t:
        return "Paid"
    return "Free"

def detect_cell_coverage(text: str) -> Optional[str]:
    t = _lower(text)
    if not t:
        return None
    if "no cell" in t or "no service" in t:
        return "None"
    if "spotty" in t or "intermittent" in t or "limited cell" in t:
        return "Partial"
    if "good cell" in t or "reliable cell" in t:
        return "Good"
    return None

def detect_crowd_level(text: str) -> Optional[int]:
    """
    1=Quiet, 2=Moderate, 3=Busy
    """
    t = _lower(text)
    if not t:
        return None
    if "very popular" in t or "crowded" in t or "busy" in t:
        return 3
    if "popular" in t or "moderate traffic" in t:
        return 2
    if "quiet" in t or "solitude" in t:
        return 1
    return None


def compute_difficulty_placeholder(distance_mi: Optional[float], gain_ft: Optional[int], surface: Optional[str]) -> Optional[float]:
    if distance_mi is None and gain_ft is None:
        return None
    base = (distance_mi or 0) * 0.6 + (gain_ft or 0) / 1000.0 * 1.2
    if surface in ("Rocky / Technical", "Scree / Talus", "Snow / Ice"):
        base *= 1.15
    return round(base, 2)

def enrich_record(rec: TrailRecord) -> TrailRecord:
    """
    Enrich record with derived fields from description and features.
    Note: distance, elevation, highest_point, and route_type are now set during parsing.
    """
    # Heuristics from description and features
    desc = rec.description or ""
    features = rec.raw_features or ""
    
    # Only set if not already populated
    if not rec.surface:
        rec.surface = detect_surface(desc + " " + features)
    
    if not rec.water_source:
        rec.water_source = detect_water_source(desc, features)
    
    if not rec.cell_coverage:
        rec.cell_coverage = detect_cell_coverage(desc)
    
    if not rec.crowd_level:
        rec.crowd_level = detect_crowd_level(desc)
    

    # Parking enrich
    parking_text = rec.parking or ""
    if not rec.parking_tags:
        rec.parking_tags = extract_parking_tags(parking_text)
    
    if not rec.parking_status:
        rec.parking_status = detect_parking_status(parking_text)
    
    if not rec.restrooms:
        rec.restrooms = detect_restrooms(parking_text, desc)

    # Difficulty placeholder (computed score) - only if not set
    if rec.difficulty is None:
        rec.difficulty = compute_difficulty_placeholder(rec.distance, rec.elevation, rec.surface)
    
    return rec


# ----------------------------- Crawler -----------------------------

class WTATrailCrawler:
    def __init__(
        self,
        *,
        crawl_delay_s: float = 62.0,
        max_list_pages: int = 12,
        start_page: int = 1,
        page_size: int = 30,
        timeout_s: int = 25,
        retries: int = 5,
        debug: bool = False,
    ) -> None:
        # Updated: WTA changed from /go-hiking/hikes to /go-outside/hikes
        self.base_list_url = "https://www.wta.org/go-outside/hikes"
        self.debug = debug

        self.headers = {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/121.0.0.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        }

        self.session = requests.Session()
        self.session.headers.update(self.headers)

        self.crawl_delay_s = float(crawl_delay_s)
        self.max_list_pages = int(max_list_pages)
        self.start_page = max(1, int(start_page))  # Ensure minimum page 1
        self.page_size = int(page_size)
        self.timeout_s = int(timeout_s)
        self.retries = int(retries)

    # ----------------------------- HTTP -----------------------------

    def _get_html(self, url: str) -> Optional[str]:
        for attempt in range(1, self.retries + 1):
            try:
                resp = self.session.get(url, timeout=self.timeout_s)
                if resp.status_code == 200:
                    return resp.text
                if resp.status_code in (429, 500, 502, 503, 504):
                    wait = min(120, (2 ** attempt) + random.uniform(0, 2))
                    print(f"HTTP {resp.status_code} for {url}; retry {attempt}/{self.retries} in {int(wait)}s")
                    time.sleep(wait)
                    continue
                print(f"HTTP {resp.status_code} for {url}; skipping")
                return None
            except requests.RequestException as e:
                wait = min(120, (2 ** attempt) + random.uniform(0, 2))
                print(f"Request error for {url}: {e}; retry {attempt}/{self.retries} in {int(wait)}s")
                time.sleep(wait)
        return None

    # ----------------------------- URL discovery -----------------------------

    def collect_hike_urls(self) -> List[str]:
        """Collect hike detail URLs from list pages (no filtering yet)."""
        urls: List[str] = []
        seen: Set[str] = set()

        # Calculate actual page range
        # start_page is 1-indexed (user-friendly)
        # Convert to 0-indexed for range
        start_idx = self.start_page - 1
        end_idx = start_idx + self.max_list_pages
        
        print(f"\n📄 Crawling pages {self.start_page} to {self.start_page + self.max_list_pages - 1} ({self.max_list_pages} pages total)")
        print(f"   Each page has ~{self.page_size} trails\n")

        for page_idx in range(start_idx, end_idx):
            page_num = page_idx + 1  # Human-readable page number
            b_start = page_idx * self.page_size
            list_url = f"{self.base_list_url}?b_start:int={b_start}"
            print(f"List page {page_num} (offset {b_start}): {list_url}")

            html = self._get_html(list_url)
            if not html:
                print(f"  ⚠️  Failed to fetch page HTML")
                continue
            
            # Debug: save HTML to inspect
            if self.debug:
                debug_file = f"debug_page_{page_num}.html"
                with open(debug_file, 'w', encoding='utf-8') as f:
                    f.write(html)
                print(f"  🐛 Debug: Saved HTML to {debug_file}")

            soup = BeautifulSoup(html, "lxml")
            
            # Try multiple selectors and combine all results
            links = []
            
            # Method 1: List item titles
            links.extend(soup.select("a.list-item-title"))
            
            # Method 2: Search result items
            links.extend(soup.select(".search-result-item h3 a"))
            links.extend(soup.select(".search-result-item a[href*='/go-hiking/hikes/']"))
            
            # Method 3: Result list items
            links.extend(soup.select(".result-list a[href*='/go-hiking/hikes/']"))
            
            # Method 4: Any link containing /go-hiking/hikes/ (the actual trail URLs)
            links.extend(soup.select('a[href*="/go-hiking/hikes/"]'))
            
            # Method 5: Try data attributes if WTA uses them
            links.extend(soup.select('a[data-hike-url]'))
            
            # Debug: show what we found
            if not links:
                print(f"  ⚠️  No links found with any selector")
                # Try to see what's on the page
                all_links = soup.find_all('a', href=True)
                hike_links = [a for a in all_links if '/go-hiking/hikes/' in a.get('href', '')]
                print(f"  📊 Total links on page: {len(all_links)}, hike links: {len(hike_links)}")
                if hike_links:
                    print(f"  🔍 Sample hike link found: {hike_links[0].get('href')[:100]}")
                    links = hike_links  # Use these if found

            added = 0
            for a in links:
                href = (a.get("href") or "").strip()
                if not href or "/go-hiking/hikes/" not in href:
                    continue
                
                # Skip non-trail pages (like the index itself)
                if href.endswith("/go-hiking/hikes") or href.endswith("/go-hiking/hikes/"):
                    continue

                full = href if href.startswith("http") else "https://www.wta.org" + href
                full = full.split("#", 1)[0].split("?")[0].rstrip("/")  # Remove query params and fragments

                if full in seen:
                    continue
                seen.add(full)
                urls.append(full)
                added += 1

            print(f"  Added {added}, total {len(urls)}")
            
            # If we got 0 new links and this isn't the first page of our range, something is wrong
            if added == 0 and page_idx > start_idx:
                print(f"  ⚠️  WARNING: Page {page_num} added 0 links - WTA pagination may have changed")
            
            time.sleep(random.uniform(2.0, 4.0))

        print(f"\n✅ Collected {len(urls)} unique trail URLs from pages {self.start_page}-{self.start_page + self.max_list_pages - 1}\n")
        return urls

    # ----------------------------- Parsing -----------------------------

    @staticmethod
    def _safe_text(soup: BeautifulSoup, selector: str) -> str:
        el = soup.select_one(selector)
        return el.get_text(strip=True) if el else ""

    @staticmethod
    def _extract_stat(soup: BeautifulSoup, keyword: str) -> str:
        """Extract stat near a label like 'Length' or 'Gain'."""
        for dt in soup.select("dt"):
            if keyword.lower() in dt.get_text(" ", strip=True).lower():
                dd = dt.find_next_sibling("dd")
                if dd:
                    return dd.get_text(" ", strip=True)

        blocks = soup.select(".hike-stats li") or soup.select(".hike-stat")
        for b in blocks:
            txt = b.get_text(" ", strip=True)
            if keyword.lower() in txt.lower():
                return re.sub(rf"^{re.escape(keyword)}\s*", "", txt, flags=re.I).strip()

        page_text = soup.get_text("\n", strip=True)
        m = re.search(rf"{re.escape(keyword)}\s*\n\s*([^\n]+)", page_text, flags=re.I)
        return m.group(1).strip() if m else ""

    @staticmethod
    def _extract_image_url(soup: BeautifulSoup) -> Optional[str]:
        og = soup.find("meta", property="og:image")
        if og and og.get("content"):
            return og.get("content")
        img = soup.select_one("img")
        if img and img.get("src"):
            return img.get("src")
        return None

    @staticmethod
    def _clean_wta_noise(text: str) -> str:
        # Common footer/action words that leak into extracted sections
        text = re.sub(r"\bPrint\b|\bEmail\b", "", text, flags=re.I)
        return _normalize_ws(text)

    @staticmethod
    def _extract_description(soup: BeautifulSoup) -> Optional[str]:
        """
        Try multiple containers; WTA HTML changes over time.
        We keep it conservative and return a cleaned block of text.
        """
        candidates = []
        # Common content containers
        selectors = [
            "#content-core",
            ".documentDescription",
            ".documentFirstHeading",
            ".hike-full-description",
            ".hike-body",
            "article",
            ".documentByLine",
        ]
        for sel in selectors:
            el = soup.select_one(sel)
            if el:
                candidates.append(el.get_text(" ", strip=True))

        # Also try "Trip Reports" summary area if present, but this can be noisy
        # We'll keep the best (longest) candidate after cleaning.
        cleaned = [WTATrailCrawler._clean_wta_noise(t) for t in candidates if t]
        cleaned = [t for t in cleaned if len(t) >= 80]  # avoid tiny strings
        if not cleaned:
            # fallback: meta description
            meta = soup.find("meta", attrs={"name": "description"})
            if meta and meta.get("content"):
                return WTATrailCrawler._clean_wta_noise(meta.get("content"))
            return None
        cleaned.sort(key=len, reverse=True)
        return cleaned[0]

    @staticmethod
    def _extract_features(soup: BeautifulSoup) -> Optional[str]:
        """
        Extract WTA features section (Lakes, Rivers, Waterfalls, etc.)
        These appear as green icon badges below the main image.
        
        Common patterns:
        - Green icon badges with text like "Dogs allowed on leash", "Lakes", "Mountain views"
        - <div class="hike-features"> or similar
        - Icon tags before the parking/pass section
        """
        features = []
        
        # Look for green badge icons (most reliable for current WTA design)
        # These are typically in divs with specific classes
        badge_selectors = [
            "span.feature-tag",
            ".hike-features span",
            ".feature-list li",
            "div.feature-badge",
            "div.hike-feature",
        ]
        
        for selector in badge_selectors:
            badges = soup.select(selector)
            for badge in badges:
                text = badge.get_text(strip=True)
                if text and len(text) > 2:  # Skip empty or very short
                    features.append(text)
        
        # If badges not found, try dt/dd structure
        if not features:
            for dt in soup.select("dt"):
                if "feature" in dt.get_text(" ", strip=True).lower():
                    dd = dt.find_next_sibling("dd")
                    if dd:
                        return dd.get_text(" ", strip=True)
        
        # Try dedicated features div/section
        if not features:
            features_el = soup.select_one(".hike-features") or soup.select_one("#hike-features")
            if features_el:
                # Get all text, split by newlines/commas
                text = features_el.get_text("\n", strip=True)
                features = [f.strip() for f in text.split("\n") if f.strip()]
        
        # Try finding any elements with common feature keywords
        if not features:
            # Search for common WTA feature terms in the HTML
            for el in soup.find_all(text=True):
                text = el.strip()
                if text and any(keyword in text.lower() for keyword in 
                               ["lake", "river", "waterfall", "old growth", "wildflower", 
                                "mountain view", "dog", "summit", "wildlife"]):
                    # Make sure it's not in a script or style tag
                    parent = el.parent
                    if parent and parent.name not in ["script", "style"]:
                        features.append(text)
        
        if features:
            # Deduplicate and return
            unique_features = []
            seen = set()
            for f in features:
                f_lower = f.lower()
                if f_lower not in seen and len(f) < 100:  # Skip very long text
                    unique_features.append(f)
                    seen.add(f_lower)
            return ", ".join(unique_features[:10])  # Limit to first 10 features
        
        return None

    @staticmethod
    def _extract_calculated_difficulty(soup: BeautifulSoup) -> Optional[str]:
        """
        Extract WTA's calculated difficulty label like "Easy", "Moderate", "Hard", "Easy/Moderate"
        This appears in the stats section alongside Length, Elevation Gain, Highpoint
        """
        # Method 1: Look for stat using same extraction as other stats
        # WTA uses dt/dd pairs or stat blocks
        for dt in soup.select("dt"):
            dt_text = dt.get_text(" ", strip=True).lower()
            if "difficulty" in dt_text and "calculated" in dt_text:
                dd = dt.find_next_sibling("dd")
                if dd:
                    diff_text = dd.get_text(strip=True)
                    # Filter out URLs and clean
                    if diff_text and not diff_text.startswith("http") and len(diff_text) < 30:
                        # Remove any extra whitespace
                        diff_text = re.sub(r'\s+', ' ', diff_text).strip()
                        return diff_text
        
        # Method 2: Look in hike-stats blocks
        blocks = soup.select(".hike-stats li") or soup.select(".hike-stat") or soup.select(".stat")
        for block in blocks:
            txt = block.get_text(" ", strip=True)
            if "difficulty" in txt.lower() and "calculated" in txt.lower():
                # Extract the value part (after label)
                cleaned = re.sub(r"calculated\s+difficulty\s*:?\s*", "", txt, flags=re.I).strip()
                if cleaned and not cleaned.startswith("http") and len(cleaned) < 30:
                    return cleaned
        
        # Method 3: Look for specific difficulty button/badge
        # WTA might have a badge element
        diff_badge = soup.select_one("button.difficulty") or soup.select_one("span.difficulty-badge")
        if diff_badge:
            text = diff_badge.get_text(strip=True)
            if text and len(text) < 30 and not text.startswith("http"):
                return text
        
        # Method 4: Look for aria-label or title attributes
        for el in soup.find_all(["span", "div", "button"], attrs={"aria-label": True}):
            label = el.get("aria-label", "").lower()
            if "difficulty" in label:
                # Extract the value
                text = el.get_text(strip=True)
                if text and len(text) < 30 and not text.startswith("http"):
                    return text
        
        # Method 5: Scan for patterns like "Difficulty: Easy/Moderate"
        page_text = soup.get_text("\n", strip=True)
        lines = page_text.split("\n")
        for i, line in enumerate(lines):
            if "calculated difficulty" in line.lower():
                # Get next line or current line after the label
                cleaned = re.sub(r".*calculated\s+difficulty\s*:?\s*", "", line, flags=re.I).strip()
                if cleaned and not cleaned.startswith("http") and len(cleaned) < 30 and cleaned.lower() not in ['?', '']:
                    return cleaned
                # Try next line
                if i + 1 < len(lines):
                    next_line = lines[i + 1].strip()
                    if next_line and not next_line.startswith("http") and len(next_line) < 30:
                        return next_line
        
        return None

    @staticmethod  
    def _extract_rating(soup: BeautifulSoup) -> Tuple[Optional[float], Optional[int]]:
        """
        Extract star rating (1-5) and number of votes.
        WTA shows ratings as filled stars, often with aria-label or CSS classes.
        Returns: (average_rating, num_votes)
        Examples: (4.0, 23) means 4 stars with 23 votes
        """
        rating = None
        num_votes = None
        
        # Method 1: JSON-LD structured data (most reliable)
        for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
            try:
                data = json.loads(script.string or "")
                if isinstance(data, dict):
                    rating_obj = data.get("aggregateRating")
                    if isinstance(rating_obj, dict):
                        rating_value = rating_obj.get("ratingValue")
                        rating_count = rating_obj.get("ratingCount") or rating_obj.get("reviewCount")
                        if rating_value is not None:
                            try:
                                rating = float(rating_value)
                                num_votes = int(rating_count) if rating_count else 0
                                return rating, num_votes
                            except (ValueError, TypeError):
                                pass
            except (json.JSONDecodeError, ValueError, TypeError):
                pass
        
        # Method 2: Look for aria-label with star rating
        # WTA often uses aria-label="4 out of 5 stars" or similar
        for el in soup.find_all(attrs={"aria-label": True}):
            label = el.get("aria-label", "").lower()
            if "star" in label:
                # Try patterns like "4 out of 5 stars" or "4.5 stars"
                m = re.search(r"(\d+\.?\d*)\s*(?:out of|\/|stars?)", label)
                if m:
                    try:
                        rating = float(m.group(1))
                    except ValueError:
                        pass
        
        # Method 3: Look for data attributes on star elements
        # data-rating="4" or similar
        star_el = soup.select_one("[data-rating]")
        if star_el and rating is None:
            try:
                rating = float(star_el.get("data-rating"))
            except (ValueError, TypeError):
                pass
        
        # Method 4: Count filled stars
        # Look for elements with class like "star filled" or "star-filled"
        if rating is None:
            filled_stars = soup.select(".star.filled, .star-filled, .icon-star-full")
            if filled_stars:
                rating = float(len(filled_stars))
        
        # Method 5: Look for rating display text
        # Patterns like "Rating: 4.0" or "4 stars"
        rating_el = soup.select_one(".rating-value") or soup.select_one(".star-rating") or soup.select_one(".average-rating")
        if rating_el and rating is None:
            rating_text = rating_el.get_text(strip=True)
            m = re.search(r"(\d+\.?\d*)", rating_text)
            if m:
                try:
                    val = float(m.group(1))
                    # Sanity check: rating should be 0-5
                    if 0 <= val <= 5:
                        rating = val
                except ValueError:
                    pass
        
        # Extract vote count
        # Look for patterns like "(23 votes)" or "23 ratings"
        votes_el = soup.select_one(".vote-count") or soup.select_one(".rating-count") or soup.select_one(".num-votes")
        if votes_el:
            votes_text = votes_el.get_text(strip=True)
            m = re.search(r"(\d+)", votes_text)
            if m:
                try:
                    num_votes = int(m.group(1))
                except ValueError:
                    pass
        
        # Fallback: scan page text for vote count
        if num_votes is None:
            page_text = soup.get_text(" ", strip=True)
            # Pattern: "(123 votes)" or "123 ratings"
            votes_match = re.search(r"\((\d+)\s+(?:vote|rating)s?\)", page_text)
            if votes_match:
                try:
                    num_votes = int(votes_match.group(1))
                except ValueError:
                    pass
        
        # Method 6: Look for text patterns near "Rating"
        if rating is None:
            page_text = soup.get_text("\n", strip=True)
            lines = page_text.split("\n")
            for i, line in enumerate(lines):
                if "rating" in line.lower():
                    # Look for number in this line or next
                    m = re.search(r"(\d+\.?\d*)\s*(?:star|out of)", line, re.I)
                    if m:
                        try:
                            val = float(m.group(1))
                            if 0 <= val <= 5:
                                rating = val
                                break
                        except ValueError:
                            pass
        
        # Default num_votes to 0 if we found a rating but no votes
        if rating is not None and num_votes is None:
            num_votes = 0
        
        return rating, num_votes
    @staticmethod
    def _extract_trailhead_coords(soup: BeautifulSoup) -> Tuple[Optional[float], Optional[float]]:
        """
        Robustly extract trailhead coordinates from a WTA hike page.

        Tries, in order:
        1) Meta tags: place:location:latitude/longitude, geo.position, ICBM
        2) data-lat/data-lng attributes
        3) JSON-LD geo.latitude/geo.longitude
        4) Inline scripts containing lat/lng
        5) Visible text fallback: "lat, lng"
        """
        def meta_content(prop: str = None, name: str = None) -> Optional[str]:
            if prop:
                m = soup.find("meta", attrs={"property": prop})
                if m and m.get("content"):
                    return m.get("content")
            if name:
                m = soup.find("meta", attrs={"name": name})
                if m and m.get("content"):
                    return m.get("content")
            return None

        lat_s = meta_content(prop="place:location:latitude") or meta_content(name="place:location:latitude")
        lng_s = meta_content(prop="place:location:longitude") or meta_content(name="place:location:longitude")
        if lat_s and lng_s:
            try:
                return float(lat_s), float(lng_s)
            except ValueError:
                pass

        geo_pos = meta_content(name="geo.position") or meta_content(prop="geo.position")
        if geo_pos:
            m = re.search(r"(-?\d{1,2}\.\d+)\s*[;,]\s*(-?\d{1,3}\.\d+)", geo_pos)
            if m:
                try:
                    return float(m.group(1)), float(m.group(2))
                except ValueError:
                    pass

        icbm = meta_content(name="ICBM")
        if icbm:
            m = re.search(r"(-?\d{1,2}\.\d+)\s*,\s*(-?\d{1,3}\.\d+)", icbm)
            if m:
                try:
                    return float(m.group(1)), float(m.group(2))
                except ValueError:
                    pass

        el = soup.select_one("[data-lat][data-lng]") or soup.select_one("[data-latitude][data-longitude]")
        if el:
            lat = el.get("data-lat") or el.get("data-latitude")
            lng = el.get("data-lng") or el.get("data-longitude")
            try:
                return float(lat), float(lng)
            except (TypeError, ValueError):
                pass

        for s in soup.find_all("script", attrs={"type": "application/ld+json"}):
            txt = s.string or ""
            if not txt:
                continue
            try:
                data = json.loads(txt)
            except Exception:
                continue

            candidates = data if isinstance(data, list) else [data]
            for obj in candidates:
                if not isinstance(obj, dict):
                    continue
                geo = obj.get("geo")
                if isinstance(geo, dict):
                    lat = geo.get("latitude")
                    lng = geo.get("longitude")
                    if lat is not None and lng is not None:
                        try:
                            return float(lat), float(lng)
                        except (TypeError, ValueError):
                            pass

        script_texts = []
        for s in soup.find_all("script"):
            txt = s.string or ""
            if txt:
                script_texts.append(txt)
        big_js = "\n".join(script_texts)

        m = re.search(r'(?:"lat"|\blat(?:itude)?\b)\s*[:=]\s*(-?\d{1,2}\.\d+)', big_js)
        n = re.search(r'(?:"lng"|\blng\b|\blon(?:gitude)?\b)\s*[:=]\s*(-?\d{1,3}\.\d+)', big_js)
        if m and n:
            try:
                return float(m.group(1)), float(n.group(1))
            except ValueError:
                pass

        page_text = soup.get_text(" ", strip=True)
        m = re.search(r"(-?\d{1,2}\.\d{3,})\s*,\s*(-?\d{1,3}\.\d{3,})", page_text)
        if m:
            try:
                return float(m.group(1)), float(m.group(2))
            except ValueError:
                pass

        return None, None

    @staticmethod
    def _extract_parking(soup: BeautifulSoup) -> Tuple[Optional[str], List[str]]:
        tags: List[str] = []

        section = soup.select_one("#hike-getting-there")
        if not section:
            h = soup.find(lambda tag: tag.name in {"h2", "h3"} and "getting there" in tag.get_text(" ", strip=True).lower())
            if h:
                section = h.find_parent() or h

        if not section:
            return None, tags

        text = WTATrailCrawler._clean_wta_noise(section.get_text(" ", strip=True))
        if not text:
            return None, tags

        # Lightweight tags (more tags come from enrichment too)
        low = text.lower()
        if any(k in low for k in ["discover pass", "northwest forest pass", "permit", "pass required", "sno-park"]):
            tags.append("Permit")
        if any(k in low for k in ["fee", "paid", "$", "payment"]):
            tags.append("Paid")
        if "parking" in low and any(k in low for k in ["limited", "small", "few"]):
            tags.append("Limited")
        if "parking" in low and any(k in low for k in ["large", "plenty", "ample"]):
            tags.append("Easy")

        return text, sorted(set(tags))

    def parse_hike_page(self, url: str) -> Optional[TrailRecord]:
        html = self._get_html(url)
        if not html:
            return None

        soup = BeautifulSoup(html, "lxml")

        name = self._safe_text(soup, "h1.documentFirstHeading")
        if not name:
            return None

        image_url = self._extract_image_url(soup)

        # Extract raw stats (used for parsing but not stored)
        raw_length = self._extract_stat(soup, "Length")
        raw_gain = self._extract_stat(soup, "Gain")
        raw_highpoint = self._extract_stat(soup, "Highpoint") or self._extract_stat(soup, "High Point") or self._extract_stat(soup, "Highest Point")

        # Parse the values immediately
        distance = parse_distance_miles(raw_length or "")
        elevation = parse_elevation_gain_ft(raw_gain or "")
        highest_point = parse_highest_point_ft(raw_highpoint or "")
        route_type = extract_route_type_from_length(raw_length or "")

        # Extract GPS coordinates
        latitude, longitude = self._extract_trailhead_coords(soup)

        # Extract WTA's calculated difficulty and rating
        calculated_difficulty = self._extract_calculated_difficulty(soup)
        rating, num_votes = self._extract_rating(soup)

        parking, parking_tags = self._extract_parking(soup)
        description = self._extract_description(soup)
        features = self._extract_features(soup)  # Lakes, rivers, waterfalls, etc.

        # Stable id from slug
        slug = url.rstrip("/").split("/")[-1]
        trail_id = f"wta_{slug}"

        rec = TrailRecord(
            id=trail_id,
            name=name,
            distance=distance,
            elevation=elevation,
            highest_point=highest_point,
            route_type=route_type,
            latitude=latitude,
            longitude=longitude,
            calculated_difficulty=calculated_difficulty,
            rating=rating,
            num_votes=num_votes,
            image_url=image_url,
            parking=parking,
            parking_tags=parking_tags,
            description=description,
            source_url=url,
            raw_features=features or None,
        )
        return enrich_record(rec)



    # ----------------------------- Run -----------------------------

    def crawl(self, limit: int = 100) -> List[TrailRecord]:
        seed_urls = self.collect_hike_urls()
        print(f"\nSeed URLs: {len(seed_urls)}. Crawling detail pages and filtering...\n")

        out: List[TrailRecord] = []
        seen_ids: Set[str] = set()

        for idx, url in enumerate(seed_urls, start=1):
            t0 = time.time()
            print(f"[{idx}/{len(seed_urls)}] {url}")

            rec = self.parse_hike_page(url)
            if rec:
                if rec.id not in seen_ids:
                    out.append(rec)
                    seen_ids.add(rec.id)
                    
                    # Format output fields
                    coords = f"GPS({rec.latitude:.4f},{rec.longitude:.4f})" if rec.latitude and rec.longitude else "NoGPS"
                    calc_diff = rec.calculated_difficulty or "NoDiff"
                    
                    # Rating: show stars (1-5) and vote count
                    if rec.rating is not None:
                        rate = f"{rec.rating:.1f}★({rec.num_votes or 0}v)"
                    else:
                        rate = f"0★({rec.num_votes or 0}v)" if rec.num_votes else "NoRating"
                    
                    # Features preview
                    features = f"[{rec.raw_features[:40]}...]" if rec.raw_features and len(rec.raw_features) > 40 else f"[{rec.raw_features}]" if rec.raw_features else "NoFeatures"
                    
                    print(
                        f"  ✓ ({len(out)}/{limit}): {rec.name}"
                    )
                    print(
                        f"     {rec.distance}mi/{rec.elevation}ft/{rec.highest_point}ft | "
                        f"Route:{rec.route_type} | Diff:{calc_diff} | {rate}"
                    )
                    print(
                        f"     Water:{rec.water_source or '?'} | Features:{features}"
                    )
                else:
                    print(f"  ~ Duplicate id skipped: {rec.id}")

            if len(out) >= limit:
                break

            elapsed = time.time() - t0
            wait = max(1.0, self.crawl_delay_s - elapsed) + random.uniform(0.5, 3.0)
            print(f"  Sleeping {int(wait)}s (crawl-delay)\n")
            time.sleep(wait)

        return out


# ----------------------------- IO helpers -----------------------------

def write_csv(path: str, records: List[TrailRecord]) -> None:
    if not records:
        return
    fieldnames = list(asdict(records[0]).keys())
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in records:
            d = asdict(r)
            d["parking_tags"] = ",".join(r.parking_tags)
            w.writerow(d)

def write_json(path: str, records: List[TrailRecord]) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump([asdict(r) for r in records], f, indent=2, ensure_ascii=False)


# ----------------------------- Main -----------------------------

def main() -> None:
    ap = argparse.ArgumentParser(
        description="WTA Trail Crawler - Scrape trail data from Washington Trails Association",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Crawl first 100 trails from pages 1-12
  python %(prog)s --limit 100 --list-pages 12
  
  # Crawl pages 50-60 (trails 1500-1800)
  python %(prog)s --start-page 50 --list-pages 10 --out trails_batch2
  
  # Resume from page 100 for 20 pages
  python %(prog)s --start-page 100 --list-pages 20 --out trails_batch4
  
  # Debug mode to inspect HTML
  python %(prog)s --debug --start-page 1 --list-pages 2 --limit 10
        """
    )
    ap.add_argument("--limit", type=int, default=500, help="Number of trails to collect")
    ap.add_argument("--out", type=str, default="wta_trails", help="Output basename (no extension)")
    ap.add_argument("--crawl-delay", type=float, default=62.0, help="Seconds between detail page requests")
    ap.add_argument("--start-page", type=int, default=1, help="Starting page number (1-indexed, default: 1)")
    ap.add_argument("--list-pages", type=int, default=12, help="How many pages to crawl from start-page")

    ap.add_argument("--debug", action="store_true", help="Save HTML pages for debugging")
    args = ap.parse_args()

    print("=" * 70)
    print("WTA Trail Crawler")
    print("=" * 70)
    print(f"Configuration:")
    print(f"  Pages: {args.start_page} to {args.start_page + args.list_pages - 1} ({args.list_pages} pages)")
    print(f"  Expected trails: ~{args.list_pages * 30} (30 per page)")
    print(f"  Limit: {args.limit} trails")
    print(f"  Crawl delay: {args.crawl_delay}s between detail pages")

    print(f"  Debug mode: {'Enabled' if args.debug else 'Disabled'}")
    print("=" * 70 + "\n")

    crawler = WTATrailCrawler(
        crawl_delay_s=args.crawl_delay,
        max_list_pages=args.list_pages,
        start_page=args.start_page,
        debug=args.debug,
    )

    records = crawler.crawl(limit=args.limit)

    csv_path = f"{args.out}.csv"
    json_path = f"{args.out}.json"

    write_csv(csv_path, records)
    write_json(json_path, records)

    print("\nDone")
    print(f"Collected: {len(records)}")
    print(f"CSV:  {csv_path}")
    print(f"JSON: {json_path}")


if __name__ == "__main__":
    main()