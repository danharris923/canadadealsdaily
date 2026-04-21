#!/usr/bin/env python3
"""Scrape canadadealsdaily.ca, rewrite Amazon affiliate tags, write deals.json."""
import json
import os
import re
import sys
import time
from pathlib import Path
from xml.etree import ElementTree as ET

import httpx

BASE = "https://www.canadadealsdaily.ca"
SITEMAP = f"{BASE}/api/sitemap.xml"
TAG = os.environ["AMAZON_TAG"]
# e.g. https://<user>.github.io/canadadealsdaily — used to emit absolute URLs
# downstream sites can consume without knowing where we publish.
PUBLIC_BASE = os.environ.get("PUBLIC_BASE", "").rstrip("/")
OUTPUT = Path("deals.json")
IMAGES = Path("images")
STATE = Path(".state/seen.json")
UA = "Mozilla/5.0 (compatible; deal-aggregator/1.0)"
DELAY = float(os.environ.get("SCRAPE_DELAY", "1.0"))
MAX_DEALS = int(os.environ["MAX_DEALS"]) if os.environ.get("MAX_DEALS") else None
IMAGE_MAX_BYTES = 3 * 1024 * 1024  # hard cap per file
IMAGE_EXTS = {"jpg", "jpeg", "png", "webp", "avif", "gif"}

JSONLD_RE = re.compile(
    r'<script type="application/ld\+json"[^>]*>(.+?)</script>',
    re.DOTALL,
)
AMZN_SHORT_RE = re.compile(r"amzn\.to/[A-Za-z0-9]+")
ASIN_RE = re.compile(r"/(?:dp|gp/product)/([A-Z0-9]{10})")
SM_NS = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}


def parse_sitemap(client):
    r = client.get(SITEMAP, timeout=30)
    r.raise_for_status()
    root = ET.fromstring(r.text)
    out = []
    for node in root.findall("sm:url", SM_NS):
        loc = node.findtext("sm:loc", namespaces=SM_NS) or ""
        lastmod = node.findtext("sm:lastmod", namespaces=SM_NS) or ""
        if "/deals/" not in loc:
            continue
        # sitemap advertises .com but only .ca serves — normalize
        loc = loc.replace("canadadealsdaily.com", "www.canadadealsdaily.ca")
        out.append((loc, lastmod))
    return out


def extract_offer(html):
    """Pull the Offer or Product JSON-LD block from a deal page."""
    for match in JSONLD_RE.finditer(html):
        try:
            data = json.loads(match.group(1).strip())
        except json.JSONDecodeError:
            continue
        items = data if isinstance(data, list) else [data]
        for item in items:
            if item.get("@type") in ("Offer", "Product"):
                return item
    return None


def _image_ext(url, content_type=""):
    tail = url.split("?", 1)[0].rsplit(".", 1)[-1].lower()
    if tail in IMAGE_EXTS:
        return "jpg" if tail == "jpeg" else tail
    if "webp" in content_type:
        return "webp"
    if "png" in content_type:
        return "png"
    if "avif" in content_type:
        return "avif"
    return "jpg"


def mirror_image(client, url, deal_id):
    """Download image, write to images/<deal_id>.<ext>, return that path.

    Skips download when a file for this deal already exists — deals are
    re-scraped only on lastmod change, so an existing mirror is current.
    """
    existing = next(IMAGES.glob(f"{deal_id}.*"), None)
    if existing:
        return existing.as_posix()
    try:
        with client.stream("GET", url, timeout=20, follow_redirects=True) as r:
            r.raise_for_status()
            ext = _image_ext(url, r.headers.get("content-type", ""))
            IMAGES.mkdir(exist_ok=True)
            out = IMAGES / f"{deal_id}.{ext}"
            total = 0
            with out.open("wb") as fh:
                for chunk in r.iter_bytes():
                    total += len(chunk)
                    if total > IMAGE_MAX_BYTES:
                        fh.close()
                        out.unlink(missing_ok=True)
                        return None
                    fh.write(chunk)
            return out.as_posix()
    except httpx.HTTPError as e:
        print(f"[img-fail] {deal_id}: {e}", file=sys.stderr)
        return None


def resolve_to_asin(client, short):
    try:
        r = client.get(
            f"https://{short}",
            follow_redirects=True,
            timeout=15,
        )
    except httpx.HTTPError:
        return None
    m = ASIN_RE.search(str(r.url))
    if not m:
        return None
    return f"https://www.amazon.ca/dp/{m.group(1)}?tag={TAG}"


def load_state():
    if STATE.exists():
        return json.loads(STATE.read_text())
    return {}


def save_state(state):
    STATE.parent.mkdir(parents=True, exist_ok=True)
    STATE.write_text(json.dumps(state, indent=2, sort_keys=True))


def main():
    state = load_state()
    deals = {}
    if OUTPUT.exists():
        try:
            deals = {d["id"]: d for d in json.loads(OUTPUT.read_text())}
        except (json.JSONDecodeError, KeyError, TypeError):
            deals = {}

    with httpx.Client(headers={"User-Agent": UA}) as client:
        urls = parse_sitemap(client)
        if MAX_DEALS:
            urls = urls[:MAX_DEALS]
        print(f"[sitemap] {len(urls)} deal URLs", file=sys.stderr)

        new_count = 0
        for loc, lastmod in urls:
            deal_id = loc.rstrip("/").rsplit("/", 1)[-1]
            if state.get(deal_id) == lastmod and deal_id in deals:
                continue

            try:
                r = client.get(loc, timeout=20)
                r.raise_for_status()
            except httpx.HTTPError as e:
                print(f"[skip-fetch] {deal_id}: {e}", file=sys.stderr)
                continue

            offer = extract_offer(r.text)
            if not offer:
                state[deal_id] = lastmod
                continue

            shorts = list(dict.fromkeys(AMZN_SHORT_RE.findall(r.text)))
            rewritten = []
            for s in shorts:
                url = resolve_to_asin(client, s)
                if url:
                    rewritten.append(url)
                time.sleep(DELAY)

            image_src = offer.get("image")
            image_local = mirror_image(client, image_src, deal_id) if image_src else None
            image_public = (
                f"{PUBLIC_BASE}/{image_local}" if image_local and PUBLIC_BASE else image_local
            )

            deals[deal_id] = {
                "id": deal_id,
                "source": loc,
                "title": offer.get("name"),
                "description": offer.get("description"),
                "price": offer.get("price"),
                "original_price": offer.get("originalPrice"),
                "currency": offer.get("priceCurrency"),
                "valid_until": offer.get("priceValidUntil"),
                "image": image_public,
                "image_source": image_src,
                "category": offer.get("category"),
                "seller": (offer.get("seller") or {}).get("name"),
                "main_affiliate_url": rewritten[0] if rewritten else None,
                "related_affiliate_urls": rewritten[1:],
                "lastmod": lastmod,
            }
            state[deal_id] = lastmod
            new_count += 1
            # Flush after each deal so partial runs are still useful.
            OUTPUT.write_text(json.dumps(list(deals.values()), indent=2))
            save_state(state)
            time.sleep(DELAY)

    OUTPUT.write_text(json.dumps(list(deals.values()), indent=2))
    save_state(state)
    print(f"[done] {new_count} new/updated, {len(deals)} total", file=sys.stderr)


if __name__ == "__main__":
    main()
