from __future__ import annotations
import gzip
import hashlib
import io
import re
from datetime import datetime, timezone
from typing import Iterable, List, Optional, Tuple

import requests

WTA_SITEMAP_GZ = "https://www.wta.org/sitemap.xml.gz"

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def dataset_version_now() -> str:
    # snapshot id
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

def sha256_text(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

def fetch_sitemap_urls(session: requests.Session, sitemap_url: str = WTA_SITEMAP_GZ) -> List[str]:
    r = session.get(sitemap_url, timeout=60)
    r.raise_for_status()
    raw = r.content
    if sitemap_url.endswith(".gz"):
        raw = gzip.decompress(raw)
    text = raw.decode("utf-8", errors="ignore")
    urls = re.findall(r"<loc>(.*?)</loc>", text)
    return urls

def filter_hike_urls(urls: Iterable[str]) -> List[str]:
    # Most WTA hikes live under /go-hiking/hikes/
    out = []
    for u in urls:
        if "/go-hiking/hikes/" in u:
            # avoid disallowed '*view$' endpoints (very conservative)
            if u.rstrip("/").endswith("view"):
                continue
            out.append(u)
    return out
