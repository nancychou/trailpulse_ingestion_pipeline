from __future__ import annotations
import random
import time
from dataclasses import asdict
from datetime import datetime, timezone
from typing import List, Optional, Tuple

import pandas as pd

# Import your existing crawler implementation (copied into project at runtime or vendored)
# We keep the class name stable.
from crawl_wta_trails import WTATrailCrawler

def crawl_changed_wta_pages(
    urls: List[str],
    *,
    max_pages: int,
    crawl_delay_s: int = 60,
    deadline: Optional[datetime] = None,
    user_agent: str = "TrailPulseCrawler/1.0 (contact: you@example.com)",
) -> Tuple[pd.DataFrame, List[str]]:
    crawler = WTATrailCrawler(
        crawl_delay_s=crawl_delay_s,
    )
    # We won't use crawler.collect_hike_urls(); we pass urls explicitly.
    out = []
    failed_urls: List[str] = []
    total = min(max_pages, len(urls))
    for idx, url in enumerate(urls[:max_pages], start=1):
        # Check deadline before each page (need at least 5 min for enrichment + upsert)
        if deadline:
            remaining = (deadline - datetime.now(timezone.utc)).total_seconds()
            if remaining < 300:
                print(f"   ⏰ Deadline approaching ({remaining:.0f}s left), stopping crawl after {idx - 1}/{total} pages")
                break
        trail_name = url.rstrip("/").split("/")[-1].replace("-", " ").title()
        print(f"   [{idx}/{total}] Crawling: {trail_name}...")
        t0 = time.time()
        rec = crawler.parse_hike_page(url)
        elapsed = time.time() - t0
        if not rec:
            print(f"   [{idx}/{total}] ❌ Failed to parse ({elapsed:.1f}s)")
            failed_urls.append(url)
            continue
        out.append(asdict(rec))
        print(f"   [{idx}/{total}] ✅ {rec.name} ({elapsed:.1f}s)")
        # politeness
        wait = max(1.0, crawl_delay_s - elapsed) + random.uniform(0.5, 1.5)
        if idx < total:
            print(f"   ⏳ Waiting {wait:.0f}s before next page...")
        time.sleep(wait)
    return pd.DataFrame(out), failed_urls
