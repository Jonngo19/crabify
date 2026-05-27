#!/usr/bin/env python3
"""
Crabify Backend — unified scraper for property, cars and jobs.
Hosted on Railway. Byparr sidecar handles Cloudflare bypass.

Sources:
  Property: Rightmove, OnTheMarket, Zoopla, Gumtree, SpareRoom
  Cars:     AutoTrader (via Byparr), Exchange&Mart, Gumtree
  Jobs:     Reed.co.uk
"""
import json
import os
import re
import threading
import urllib.request
import urllib.parse
import urllib.error
from http.server import HTTPServer, BaseHTTPRequestHandler
from curl_cffi import requests as cffi_req

# Playwright optional — kept for local dev only, not needed on Railway
try:
    from playwright.sync_api import sync_playwright
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────

CONFIG_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.json")

def _load_config() -> dict:
    try:
        with open(CONFIG_FILE) as f:
            return json.load(f)
    except Exception:
        return {}

def _save_config(cfg: dict):
    try:
        with open(CONFIG_FILE, "w") as f:
            json.dump(cfg, f, indent=2)
    except Exception as e:
        print(f"[Config] Could not save: {e}")

def get_scrapfly_key() -> str:
    env_key = os.environ.get("SCRAPFLY_API_KEY", "")
    if env_key:
        return env_key
    return _load_config().get("scrapfly_api_key", "")

# Byparr / FlareSolverr URLs — set as Railway environment variables
BYPARR_URL        = os.environ.get("BYPARR_URL", "")
FLARESOLVERR_URL  = os.environ.get("FLARESOLVERR_URL", "")

# ─────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────

BROWSER_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-GB,en;q=0.9",
    "Accept-Encoding": "identity",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Cache-Control": "max-age=0",
}

PROPERTY_TYPE_MAP_RM = {
    "detached": "DETACHED", "semi": "SEMI_DETACHED",
    "terraced": "TERRACED", "flat": "FLAT", "bungalow": "BUNGALOW",
}

PROPERTY_TYPE_MAP_OTM = {
    "detached": "detached-houses", "semi": "semi-detached-houses",
    "terraced": "terraced-houses", "flat": "flats", "bungalow": "bungalows",
}

ZOOPLA_PROPERTY_TYPE_MAP = {
    "detached": "detached-houses", "semi": "semi-detached-houses",
    "terraced": "terraced-houses", "flat": "flats", "bungalow": "bungalows",
}

# ═══════════════════════════════════════════════════════════════
# BYPARR / FLARESOLVERR BYPASS HELPER
# ═══════════════════════════════════════════════════════════════

def bypass_fetch(url: str, timeout: int = 45) -> str:
    """
    Fetch a Cloudflare-protected URL through cascade:
      1. Byparr (Camoufox — strongest)
      2. FlareSolverr (fallback)
      3. curl_cffi (TLS fingerprint spoof — lightest)
    Raises Exception if all methods fail.
    """
    # ── Byparr ──────────────────────────────────────────────────
    if BYPARR_URL:
        try:
            payload = json.dumps({"cmd": "request.get", "url": url, "maxTimeout": timeout * 1000}).encode()
            req = urllib.request.Request(f"{BYPARR_URL}/v1", data=payload, headers={"Content-Type": "application/json"})
            with urllib.request.urlopen(req, timeout=timeout + 10) as r:
                data = json.loads(r.read())
            if data.get("status") == "ok":
                html = data["solution"]["response"]
                if len(html) > 1000:
                    print(f"  [Bypass] ✓ Byparr ({len(html)} chars)")
                    return html
        except Exception as e:
            print(f"  [Bypass] Byparr failed: {e}")

    # ── FlareSolverr ─────────────────────────────────────────────
    if FLARESOLVERR_URL:
        try:
            payload = json.dumps({"cmd": "request.get", "url": url, "maxTimeout": timeout * 1000}).encode()
            req = urllib.request.Request(f"{FLARESOLVERR_URL}/v1", data=payload, headers={"Content-Type": "application/json"})
            with urllib.request.urlopen(req, timeout=timeout + 10) as r:
                data = json.loads(r.read())
            if data.get("status") == "ok":
                html = data["solution"]["response"]
                if len(html) > 1000:
                    print(f"  [Bypass] ✓ FlareSolverr ({len(html)} chars)")
                    return html
        except Exception as e:
            print(f"  [Bypass] FlareSolverr failed: {e}")

    # ── curl_cffi ────────────────────────────────────────────────
    try:
        r = cffi_req.get(url, headers=BROWSER_HEADERS, impersonate="chrome124", timeout=timeout)
        if r.status_code == 200 and len(r.text) > 1000:
            print(f"  [Bypass] ✓ curl_cffi ({len(r.text)} chars)")
            return r.text
    except Exception as e:
        print(f"  [Bypass] curl_cffi failed: {e}")

    raise Exception(f"All bypass methods failed for {url}")

# ═══════════════════════════════════════════════════════════════
# RIGHTMOVE
# ═══════════════════════════════════════════════════════════════

def rm_typeahead(query: str) -> tuple:
    slug = urllib.parse.quote(query.strip())
    url = f"https://www.rightmove.co.uk/property-for-sale/{slug}.html"
    req = urllib.request.Request(url, headers=BROWSER_HEADERS)
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            html = resp.read().decode("utf-8", errors="replace")
    except Exception as e:
        print(f"  [RM] Location resolve error for '{query}': {e}")
        return "", query

    m = re.search(r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>', html, re.DOTALL)
    if not m:
        prefixes = ['Greater ', 'City of ', 'Royal Borough of ', 'London Borough of ', 'Borough of ']
        simplified = query
        for pfx in prefixes:
            if simplified.startswith(pfx):
                simplified = simplified[len(pfx):]
                break
        if simplified != query:
            return rm_typeahead(simplified)
        return "", query

    try:
        data = json.loads(m.group(1))
    except Exception:
        return "", query

    data_str = json.dumps(data)
    loc_match = re.search(r'"locationIdentifier"\s*:\s*"([A-Z]+\^[0-9]+)"', data_str)
    if loc_match:
        loc_id = loc_match.group(1)
        print(f"  [RM] Resolved '{query}' → {loc_id}")
        return loc_id, query

    props = data.get("props", {}).get("pageProps", {}).get("searchResults", {}).get("properties", [])
    if props:
        return "DIRECT:" + slug, query

    prefixes = ['Greater ', 'City of ', 'Royal Borough of ', 'London Borough of ', 'Borough of ']
    simplified = query
    for pfx in prefixes:
        if simplified.startswith(pfx):
            simplified = simplified[len(pfx):]
            break
    if simplified != query:
        return rm_typeahead(simplified)
    return "", query


def rm_search_html(location_id: str, params: dict) -> tuple:
    channel = "BUY" if params.get("transaction_type", "buy") == "buy" else "RENT"
    endpoint = "property-for-sale" if channel == "BUY" else "property-to-rent"
    sort_map = {"price_asc": "6", "price_desc": "10", "newest": "2", "beds_desc": "2"}
    rm_sort = sort_map.get(params.get("sort", "newest"), "2")

    if location_id.startswith("DIRECT:"):
        slug = location_id[len("DIRECT:"):]
        parts = []
        if params.get("min_beds"): parts.append(f"minBedrooms={params['min_beds']}")
        if params.get("max_beds"): parts.append(f"maxBedrooms={params['max_beds']}")
        if params.get("min_price"): parts.append(f"minPrice={params['min_price']}")
        if params.get("max_price"): parts.append(f"maxPrice={params['max_price']}")
        if params.get("index"): parts.append(f"index={params['index']}")
        parts.append(f"sortType={rm_sort}")
        url = f"https://www.rightmove.co.uk/{endpoint}/{slug}.html" + ("?" + "&".join(parts) if parts else "")
    else:
        qp = {
            "locationIdentifier": location_id,
            "radius": str(params.get("radius", "1.0")),
            "sortType": rm_sort,
            "index": str(params.get("index", 0)),
        }
        if params.get("min_beds"): qp["minBedrooms"] = str(params["min_beds"])
        if params.get("max_beds"): qp["maxBedrooms"] = str(params["max_beds"])
        if params.get("min_price"): qp["minPrice"] = str(params["min_price"])
        if params.get("max_price"): qp["maxPrice"] = str(params["max_price"])
        if params.get("property_type") and params["property_type"] != "any":
            pt = PROPERTY_TYPE_MAP_RM.get(params["property_type"], "")
            if pt: qp["propertyTypes"] = pt
        if params.get("must_parking"): qp["mustHaveParking"] = "true"
        if params.get("must_garden"): qp["mustHaveGarden"] = "true"
        url = f"https://www.rightmove.co.uk/{endpoint}/find.html?{urllib.parse.urlencode(qp)}"

    print(f"  [RM] Fetching: {url}")
    try:
        req = urllib.request.Request(url, headers=BROWSER_HEADERS)
        with urllib.request.urlopen(req, timeout=20) as resp:
            html = resp.read().decode("utf-8", errors="replace")
    except Exception as e:
        print(f"  [RM] HTTP error: {e}")
        return [], 0

    match = re.search(r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>', html, re.DOTALL)
    if not match:
        return [], 0
    try:
        nd = json.loads(match.group(1))
        sr = nd["props"]["pageProps"].get("searchResults", {})
        props_raw = sr.get("properties", [])
        total = int(str(sr.get("resultCount", 0)).replace(",", ""))
        properties = [parse_rm_property(p, channel) for p in props_raw]
        print(f"  [RM] Got {len(properties)} (total: {total})")
        return properties, total
    except Exception as e:
        print(f"  [RM] Parse error: {e}")
        return [], 0


def parse_rm_property(p: dict, channel: str = "BUY") -> dict:
    price_obj = p.get("price", {})
    display_prices = price_obj.get("displayPrices", [{}])
    price = display_prices[0].get("displayPrice", "") if display_prices else ""
    price_qual = display_prices[0].get("displayPriceQualifier", "") if display_prices else ""
    if not price:
        amt = price_obj.get("amount", 0)
        freq = price_obj.get("frequency", "")
        if amt:
            price = f"£{int(amt):,}"
            if freq and freq != "not specified":
                price += f" {freq}"
    customer = p.get("customer", {})
    agent_name = customer.get("branchDisplayName", "") or customer.get("brandTradingName", "")
    agent_phone = customer.get("contactTelephone", "") or customer.get("telephone", "")
    branch_url = customer.get("branchLandingPageUrl", "")
    if branch_url and not branch_url.startswith("http"):
        branch_url = "https://www.rightmove.co.uk" + branch_url
    images = p.get("propertyImages", {}) or {}
    img_list = images.get("images", []) or []
    main_img = ""
    for img in img_list[:3]:
        src = img.get("srcUrl", "") or img.get("url", "")
        if src and src.startswith("http"):
            main_img = src
            break
    prop_id = p.get("id", "")
    prop_url = p.get("propertyUrl", "")
    if prop_url and not prop_url.startswith("http"):
        prop_url = "https://www.rightmove.co.uk" + prop_url
    elif prop_id and not prop_url:
        prop_url = f"https://www.rightmove.co.uk/properties/{prop_id}#/"
    listing_update = p.get("listingUpdate", {}) or {}
    added_reduced = p.get("addedOrReduced", "") or listing_update.get("listingUpdateReason", "")
    return {
        "id": f"rm_{prop_id}", "address": p.get("displayAddress", "").strip(),
        "price": price, "price_qualifier": price_qual,
        "bedrooms": p.get("bedrooms", 0), "bathrooms": p.get("bathrooms", 0),
        "property_type": p.get("propertySubType", "") or p.get("propertyTypeFullDescription", ""),
        "description": p.get("summary", ""), "key_features": p.get("keyFeatures", []),
        "agent": agent_name, "agent_phone": agent_phone, "agent_contact_url": branch_url,
        "source": "Rightmove", "source_url": prop_url, "image": main_img,
        "added_or_reduced": added_reduced, "transaction_type": channel,
        "is_featured": p.get("featuredProperty", False),
        "latitude": p.get("location", {}).get("latitude", 0),
        "longitude": p.get("location", {}).get("longitude", 0),
    }

# ═══════════════════════════════════════════════════════════════
# ONTHEMARKET
# ═══════════════════════════════════════════════════════════════

def _otm_location_slug(location: str) -> str:
    slug = location.strip().lower()
    slug = re.sub(r'[^a-z0-9]+', '-', slug)
    return slug.strip('-')


def otm_search(location: str, params: dict) -> tuple:
    channel = params.get("transaction_type", "buy")
    endpoint = "for-sale" if channel == "buy" else "to-rent"
    prop_type = params.get("property_type", "any")
    type_slug = PROPERTY_TYPE_MAP_OTM.get(prop_type, "property") if prop_type and prop_type != "any" else "property"
    loc_slug = _otm_location_slug(location)
    sort_map = {"price_asc": "price-asc", "price_desc": "price-desc", "newest": "most-recent", "beds_desc": "most-recent"}
    otm_sort = sort_map.get(params.get("sort", "newest"), "most-recent")
    qp = {"sort": otm_sort}
    if params.get("min_beds"):
        try: qp["min-bedrooms"] = str(int(params["min_beds"]))
        except: pass
    if params.get("max_beds"):
        try: qp["max-bedrooms"] = str(int(params["max_beds"]))
        except: pass
    if params.get("min_price"):
        try: qp["min-price"] = str(int(params["min_price"]))
        except: pass
    if params.get("max_price"):
        try: qp["max-price"] = str(int(params["max_price"]))
        except: pass
    index = int(params.get("index", 0))
    if index > 0:
        qp["page"] = str((index // 25) + 1)
    qs = urllib.parse.urlencode(qp)
    url = f"https://www.onthemarket.com/{endpoint}/{type_slug}/{loc_slug}/"
    if qs: url += f"?{qs}"
    print(f"  [OTM] Fetching: {url}")
    try:
        req = urllib.request.Request(url, headers=BROWSER_HEADERS)
        with urllib.request.urlopen(req, timeout=20) as resp:
            html = resp.read().decode("utf-8", errors="replace")
    except Exception as e:
        print(f"  [OTM] HTTP error: {e}")
        return [], 0
    match = re.search(r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>', html, re.DOTALL)
    if not match:
        return [], 0
    try:
        nd = json.loads(match.group(1))
        redux = nd["props"]["initialReduxState"]
        results_state = redux.get("results", {})
        listings_raw = results_state.get("list", [])
        total = results_state.get("totalResults", 0)
        if isinstance(total, str): total = int(total.replace(",", ""))
        pagination = results_state.get("paginationControls", {})
        if pagination.get("total"):
            try: total = int(pagination["total"])
            except: pass
        channel_upper = "BUY" if channel == "buy" else "RENT"
        properties = [parse_otm_property(p, channel_upper) for p in listings_raw]
        print(f"  [OTM] Got {len(properties)} (total: {total})")
        return properties, total
    except Exception as e:
        print(f"  [OTM] Parse error: {e}")
        return [], 0


def parse_otm_property(p: dict, channel: str = "BUY") -> dict:
    prop_id = str(p.get("id", ""))
    price = p.get("price", "") or p.get("short-price", "")
    agent = p.get("agent", {}) or {}
    agent_name = agent.get("name", "")
    agent_phone = agent.get("telephone", "")
    agent_contact_url = agent.get("contact-url", "")
    if agent_contact_url and not agent_contact_url.startswith("http"):
        agent_contact_url = "https://www.onthemarket.com" + agent_contact_url
    cover = p.get("cover-image", {}) or {}
    main_img = cover.get("default", "") or cover.get("webp", "")
    if not main_img:
        images = p.get("images", []) or []
        if images:
            first_img = images[0] or {}
            main_img = first_img.get("default", "") or first_img.get("webp", "")
    details_url = p.get("details-url", "")
    if details_url and not details_url.startswith("http"):
        prop_url = "https://www.onthemarket.com" + details_url
    else:
        prop_url = details_url or f"https://www.onthemarket.com/details/{prop_id}/"
    loc = p.get("location", {}) or {}
    return {
        "id": f"otm_{prop_id}", "address": p.get("address", "").strip(),
        "price": price, "price_qualifier": p.get("price-qualifier", "") or "",
        "bedrooms": p.get("bedrooms", 0) or 0, "bathrooms": p.get("bathrooms", 0) or 0,
        "property_type": p.get("humanised-property-type", ""),
        "description": p.get("property-title", ""),
        "key_features": p.get("features", []) or [],
        "agent": agent_name, "agent_phone": agent_phone, "agent_contact_url": agent_contact_url,
        "source": "OnTheMarket", "source_url": prop_url, "image": main_img,
        "added_or_reduced": p.get("days-since-added-reduced", ""),
        "transaction_type": channel, "is_featured": p.get("spotlight?", False),
        "latitude": loc.get("lat", 0), "longitude": loc.get("lon", 0),
    }

# ═══════════════════════════════════════════════════════════════
# ZOOPLA
# ═══════════════════════════════════════════════════════════════

def _zoopla_location_slug(location: str) -> str:
    loc = location.strip().lower()
    postcode_re = re.compile(r'^[a-z]{1,2}\d[a-z\d]?(\s*\d[a-z]{2})?$')
    if postcode_re.match(loc.replace(' ', '')):
        return loc.split()[0]
    slug = re.sub(r'[^a-z0-9\s\-]', '', loc)
    return re.sub(r'\s+', '-', slug.strip())


def _parse_zoopla_rsc(content: str) -> list:
    chunks = re.findall(r'self\.__next_f\.push\(\[1,(.*?)\]\s*\)', content, re.DOTALL)
    all_text = ''
    for chunk in chunks:
        try: all_text += json.loads(chunk)
        except: all_text += chunk
    idx = all_text.find('regularListingsFormatted')
    if idx < 0: return []
    arr_start = all_text.find('[', idx)
    if arr_start < 0: return []
    depth = 0
    arr_end = arr_start
    for i, ch in enumerate(all_text[arr_start:arr_start + 400000]):
        if ch in '[{': depth += 1
        elif ch in ']}':
            depth -= 1
            if depth == 0:
                arr_end = arr_start + i + 1
                break
    try: return json.loads(all_text[arr_start:arr_end])
    except: return []


def _map_zoopla_listing(raw: dict, transaction_type: str) -> dict:
    listing_id = raw.get("listingId", "")
    address = raw.get("address", "")
    price_raw = raw.get("price", "")
    price = price_raw.get("value", "") or price_raw.get("displayPrice", "") if isinstance(price_raw, dict) else str(price_raw)
    bedrooms = 0
    title = raw.get("title", "")
    bed_match = re.search(r'(\d+)\s*bed', title, re.IGNORECASE)
    if bed_match: bedrooms = int(bed_match.group(1))
    if not bedrooms: bedrooms = int(raw.get("numBedrooms", 0) or 0)
    prop_type = "property"
    for pt in ["flat", "detached", "semi-detached", "terraced", "bungalow", "house", "studio"]:
        if pt in title.lower(): prop_type = pt; break
    branch = raw.get("branch", {}) or {}
    listing_uris = raw.get("listingUris", {}) or {}
    detail_path = listing_uris.get("detail", "")
    source_url = f"https://www.zoopla.co.uk{detail_path}" if detail_path else "https://www.zoopla.co.uk"
    image_obj = raw.get("image", {}) or {}
    main_img = image_obj.get("src", "") or image_obj.get("url", "")
    features = raw.get("features", []) or []
    if isinstance(features, list):
        features = [f.get("content", "") if isinstance(f, dict) else str(f) for f in features]
    return {
        "id": f"zoopla_{listing_id}", "address": address, "price": price,
        "price_qualifier": "", "bedrooms": bedrooms, "bathrooms": 0,
        "property_type": prop_type, "description": raw.get("summaryDescription", "") or "",
        "key_features": features, "agent": branch.get("name", ""),
        "agent_phone": branch.get("phone", ""), "agent_contact_url": source_url,
        "source": "Zoopla", "source_url": source_url, "image": main_img,
        "added_or_reduced": raw.get("publishedOnLabel", ""),
        "transaction_type": transaction_type, "is_featured": raw.get("isPremium", False),
        "latitude": 0, "longitude": 0,
    }


def _zoopla_parse_html(content: str, transaction_type: str) -> tuple:
    if not content or len(content) < 50000: return [], 0, 0
    raw_listings = _parse_zoopla_rsc(content)
    listings = [_map_zoopla_listing(r, transaction_type) for r in raw_listings] if raw_listings else []
    total = len(listings)
    page_max = 1
    chunks = re.findall(r'self\.__next_f\.push\(\[1,(.*?)\]\s*\)', content, re.DOTALL)
    all_text = ''
    for chunk in chunks:
        try: all_text += json.loads(chunk)
        except: all_text += chunk
    tr_match = re.search(r'"totalResults"\s*:\s*(\d+)', all_text)
    if tr_match: total = int(tr_match.group(1))
    pm_match = re.search(r'"pageNumberMax"\s*:\s*(\d+)', all_text)
    if pm_match: page_max = int(pm_match.group(1))
    return listings, total, page_max


def zoopla_search(location: str, params: dict) -> tuple:
    transaction_type = params.get("transaction_type", "buy")
    channel = "for-sale" if transaction_type == "buy" else "to-rent"
    slug = _zoopla_location_slug(location)
    qp = {}
    if params.get("min_beds"): qp["beds_min"] = params["min_beds"]
    if params.get("max_beds"): qp["beds_max"] = params["max_beds"]
    if params.get("min_price"): qp["price_min"] = params["min_price"]
    if params.get("max_price"): qp["price_max"] = params["max_price"]
    prop_type = params.get("property_type", "any")
    prop_subpath = ZOOPLA_PROPERTY_TYPE_MAP.get(prop_type, "property") if prop_type and prop_type != "any" else "property"
    index = int(params.get("index", 0))
    page_num = (index // 25) + 1
    if page_num > 1: qp["pn"] = page_num
    qs_suffix = ("?" + urllib.parse.urlencode(qp)) if qp else ""
    url = f"https://www.zoopla.co.uk/{channel}/{prop_subpath}/{slug}/{qs_suffix}"
    print(f"  [Zoopla] Fetching: {url}")

    # Try curl_cffi with Chrome impersonation first (fast, no browser needed)
    try:
        r = cffi_req.get(url, headers=BROWSER_HEADERS, impersonate="chrome124", timeout=20)
        html = r.text
        if "just a moment" not in html.lower() or len(html) >= 50000:
            listings, total, _ = _zoopla_parse_html(html, transaction_type)
            if listings:
                must_parking = params.get("must_parking", False)
                must_garden = params.get("must_garden", False)
                if must_parking:
                    listings = [p for p in listings if any("park" in str(f).lower() for f in (p.get("key_features") or []) + [p.get("description", "")])]
                if must_garden:
                    listings = [p for p in listings if any("garden" in str(f).lower() for f in (p.get("key_features") or []) + [p.get("description", "")])]
                print(f"  [Zoopla] ✓ {len(listings)} via curl_cffi (total: {total:,})")
                return listings, total, None
    except Exception as e:
        print(f"  [Zoopla] curl_cffi failed: {e}")

    # Scrapfly fallback
    api_key = get_scrapfly_key()
    if api_key:
        try:
            acct_req = urllib.request.Request(f"https://api.scrapfly.io/account?key={api_key}", headers={"Accept": "application/json"})
            with urllib.request.urlopen(acct_req, timeout=8) as r_acct:
                acct = json.loads(r_acct.read())
            remaining = acct.get("subscription", {}).get("usage", {}).get("scrape", {}).get("remaining", 0)
            quota_reached = acct.get("project", {}).get("quota_reached", False)
            if quota_reached or remaining < 20:
                return [], 0, "relay_mode"
        except: pass
        scrapfly_params = urllib.parse.urlencode({"key": api_key, "url": url, "asp": "true", "render_js": "false", "country": "gb", "proxy_pool": "public_residential_pool"})
        try:
            req = urllib.request.Request(f"https://api.scrapfly.io/scrape?{scrapfly_params}")
            req.add_header("Accept", "application/json")
            with urllib.request.urlopen(req, timeout=30) as r:
                resp_data = json.loads(r.read())
            result = resp_data.get("result", {})
            if result.get("status_code") == 200:
                fb_listings, fb_total, _ = _zoopla_parse_html(result.get("content", ""), transaction_type)
                if fb_listings:
                    return fb_listings, fb_total, None
        except urllib.error.HTTPError as e:
            if e.code == 429: return [], 0, "quota_exceeded"
        except Exception as e:
            print(f"  [Zoopla] Scrapfly error: {e}")

    return [], 0, "relay_mode"

# ═══════════════════════════════════════════════════════════════
# GUMTREE PROPERTY
# ═══════════════════════════════════════════════════════════════

GUMTREE_SORT_MAP = {"price_asc": "price_asc", "price_desc": "price_desc", "newest": "date", "beds_desc": "date"}


def gumtree_search(location: str, params: dict) -> tuple:
    channel = params.get("transaction_type", "buy")
    slug = re.sub(r"[^a-z0-9]+", "-", location.strip().lower()).strip("-") or "london"
    category = "property-for-sale" if channel == "buy" else "property-to-rent"
    base_url = f"https://www.gumtree.com/flats-houses/{category}/uk/{slug}"
    qp = {}
    if params.get("min_beds"):
        try: qp["min_bedrooms"] = str(int(params["min_beds"]))
        except: pass
    if params.get("max_beds"):
        try: qp["max_bedrooms"] = str(int(params["max_beds"]))
        except: pass
    if params.get("min_price"):
        try: qp["min_price"] = str(int(params["min_price"]))
        except: pass
    if params.get("max_price"):
        try: qp["max_price"] = str(int(params["max_price"]))
        except: pass
    qp["sort"] = GUMTREE_SORT_MAP.get(params.get("sort", "newest"), "date")
    index = int(params.get("index", 0))
    if index > 0: qp["page"] = str((index // 25) + 1)
    qs = urllib.parse.urlencode(qp)
    url = f"{base_url}?{qs}" if qs else base_url
    print(f"  [Gumtree] Fetching: {url}")
    try:
        req = urllib.request.Request(url, headers=BROWSER_HEADERS)
        with urllib.request.urlopen(req, timeout=15) as r:
            html = r.read().decode("utf-8", errors="replace")
    except Exception as e:
        print(f"  [Gumtree] HTTP error: {e}")
        return [], 0
    listing_blocks = re.findall(r'(data-q="search-result-anchor"\s+href="(/p/[^"]+)".*?)(?=data-q="search-result-anchor"|</main>|<footer)', html, re.DOTALL)
    total_m = re.search(r'([\d,]+)\s+(?:ads?|result|listing)', html, re.I)
    total = 0
    if total_m:
        try: total = int(total_m.group(1).replace(",", ""))
        except: pass
    properties = []
    for block_text, href in listing_blocks:
        prop = _parse_gumtree_listing(block_text, href, channel)
        if prop: properties.append(prop)
    if not total: total = len(properties)
    print(f"  [Gumtree] Got {len(properties)} (total: {total})")
    return properties, total


def _parse_gumtree_listing(block: str, href: str, channel: str) -> dict:
    clean_block = re.sub(r'<style[^>]*>.*?</style>', '', block, flags=re.DOTALL)
    price_m = re.search(r'£([\d,]+)\s*(pcm|pw|per\s+month|per\s+week)?', clean_block, re.I)
    price = ""
    if price_m:
        freq = price_m.group(2) or ""
        price = f"£{price_m.group(1)}" + (f" {freq.lower()}" if freq else "")
    parts = href.rstrip("/").split("/")
    listing_id = parts[-1] if parts[-1].isdigit() else re.sub(r"[^0-9]", "", parts[-1]) or parts[-1]
    title_m = re.search(r'data-q="tile-title"[^>]*>(.*?)</div>', clean_block, re.DOTALL)
    if title_m:
        address = re.sub(r'<[^>]+>', '', title_m.group(1)).strip()
        address = re.sub(r'&[a-z]+;', ' ', address).strip()[:120]
    else:
        slug_part = parts[-2] if len(parts) > 1 else ""
        address = slug_part.replace("-", " ").title()[:80]
    beds_m = re.search(r'(\d+)\s*(?:bed(?:room)?s?)', clean_block, re.I)
    beds = int(beds_m.group(1)) if beds_m else 0
    type_m = re.search(r'\b(flat|house|apartment|studio|bungalow|maisonette|terraced|detached|semi-detached)\b', clean_block, re.I)
    prop_type = type_m.group(1).title() if type_m else ""
    img_m = re.search(r'src="(https://img\.gumtree\.com[^"]*)"', clean_block, re.I)
    image = img_m.group(1) if img_m else ""
    prop_url = f"https://www.gumtree.com{href}"
    if not price and not address: return None
    return {
        "id": f"gumtree_{listing_id}", "address": address, "price": price,
        "price_qualifier": "", "bedrooms": beds, "bathrooms": 0,
        "property_type": prop_type, "description": "",
        "key_features": [], "agent": "Gumtree / Private Seller",
        "agent_phone": "", "agent_contact_url": prop_url,
        "source": "Gumtree", "source_url": prop_url, "image": image,
        "added_or_reduced": "", "transaction_type": channel, "is_featured": False,
        "latitude": 0, "longitude": 0,
    }

# ═══════════════════════════════════════════════════════════════
# SPAREROOM
# ═══════════════════════════════════════════════════════════════

def spareroom_search(location: str, params: dict) -> tuple:
    if params.get("transaction_type", "buy") != "rent": return [], 0
    slug = re.sub(r"[^a-z0-9]+", "-", location.strip().lower()).strip("-") or "london"
    qp = {"per": "pcm"}
    if params.get("min_beds"):
        try: qp["min_rooms"] = str(int(params["min_beds"]))
        except: pass
    if params.get("max_price"):
        try: qp["max_monthly_price"] = str(int(params["max_price"]))
        except: pass
    if params.get("min_price"):
        try: qp["min_monthly_price"] = str(int(params["min_price"]))
        except: pass
    index = int(params.get("index", 0))
    if index > 0: qp["offset"] = str(index)
    url = f"https://www.spareroom.co.uk/flatshare/{slug}?{urllib.parse.urlencode(qp)}"
    print(f"  [SpareRoom] Fetching: {url}")
    try:
        req = urllib.request.Request(url, headers=BROWSER_HEADERS)
        with urllib.request.urlopen(req, timeout=15) as r:
            html = r.read().decode("utf-8", errors="replace")
    except Exception as e:
        print(f"  [SpareRoom] HTTP error: {e}")
        return [], 0
    total_m = re.search(r'([\d,]+)\s+(?:rooms?|results?|ads?)\s+found', html, re.I)
    total = 0
    if total_m:
        try: total = int(total_m.group(1).replace(",", ""))
        except: pass
    articles = re.findall(r'<article[^>]*class="[^"]*listing[^"]*"[^>]*>(.*?)</article>', html, re.DOTALL)
    properties = [p for art in articles for p in [_parse_spareroom_listing(art)] if p]
    if not total: total = len(properties)
    print(f"  [SpareRoom] Got {len(properties)} (total: {total})")
    return properties, total


def _parse_spareroom_listing(article: str) -> dict:
    h2_m = re.search(r'<h2[^>]*>(.*?)</h2>', article, re.DOTALL)
    title = re.sub(r'<[^>]+>', '', h2_m.group(1)).strip() if h2_m else ""
    price_m = re.search(r'(?:£|&pound;)([\d,]+)\s*(pcm|pw|per\s*month|per\s*week)?', article, re.I)
    price = ""
    if price_m:
        freq = (price_m.group(2) or "pcm").lower().strip()
        price = f"£{price_m.group(1)} {freq}"
    rooms_m = re.search(r'(\d+)\s*(?:room|bedroom|bed)', article, re.I)
    beds = int(rooms_m.group(1)) if rooms_m else 1
    loc_m = re.search(r'<strong[^>]*>([^<]{3,50})</strong>', article)
    address = re.sub(r'<[^>]+>', '', loc_m.group(1)).strip() if loc_m else title
    link_m = re.search(r'href="(/flatshare/\d+[^"]*)"', article)
    prop_url = f"https://www.spareroom.co.uk{link_m.group(1)}" if link_m else "https://www.spareroom.co.uk"
    id_m = re.search(r'/flatshare/(\d+)', prop_url)
    listing_id = id_m.group(1) if id_m else prop_url.split("/")[-1]
    img_m = re.search(r'<img[^>]+src="(https://[^"]+(?:jpg|jpeg|png|webp)[^"]*)"', article, re.I)
    image = img_m.group(1) if img_m else ""
    if not title and not price: return None
    return {
        "id": f"spareroom_{listing_id}", "address": address or title, "price": price,
        "price_qualifier": "pcm", "bedrooms": beds, "bathrooms": 0,
        "property_type": "Flat / Room", "description": "",
        "key_features": [], "agent": "SpareRoom", "agent_phone": "",
        "agent_contact_url": prop_url, "source": "SpareRoom", "source_url": prop_url,
        "image": image, "added_or_reduced": "", "transaction_type": "rent",
        "is_featured": False, "latitude": 0, "longitude": 0,
    }

# ═══════════════════════════════════════════════════════════════
# COMBINED PROPERTY SEARCH
# ═══════════════════════════════════════════════════════════════

def _normalise_location(location: str) -> str:
    loc = location.strip()
    LOCATION_MAP = {
        "greater london": "London", "greater manchester": "Manchester",
        "greater birmingham": "Birmingham", "city of westminster": "Westminster",
        "royal borough of kensington and chelsea": "Kensington",
    }
    mapped = LOCATION_MAP.get(loc.lower())
    if mapped:
        print(f"  [Location] Normalised '{loc}' → '{mapped}'")
        return mapped
    for pfx in ['Greater ', 'City of ', 'Royal Borough of ', 'London Borough of ', 'Borough of ']:
        if loc.startswith(pfx):
            stripped = loc[len(pfx):]
            print(f"  [Location] Stripped prefix → '{stripped}'")
            return stripped
    return loc


def combined_search(location: str, params: dict) -> dict:
    location = _normalise_location(location)
    rm_results, rm_total, otm_results, otm_total = [], 0, [], 0
    zp_results, zp_total, gt_results, gt_total = [], 0, [], 0
    sr_results, sr_total = [], 0
    rm_error = otm_error = zp_error = gt_error = sr_error = None

    def fetch_rm():
        nonlocal rm_results, rm_total, rm_error
        try:
            loc_id, _ = rm_typeahead(location)
            if loc_id: rm_results, rm_total = rm_search_html(loc_id, params)
            else: rm_error = f"Could not resolve '{location}'"
        except Exception as e: rm_error = str(e)

    def fetch_otm():
        nonlocal otm_results, otm_total, otm_error
        try: otm_results, otm_total = otm_search(location, params)
        except Exception as e: otm_error = str(e)

    def fetch_zoopla():
        nonlocal zp_results, zp_total, zp_error
        try: zp_results, zp_total, zp_error = zoopla_search(location, params)
        except Exception as e: zp_error = str(e)

    def fetch_gumtree():
        nonlocal gt_results, gt_total, gt_error
        try: gt_results, gt_total = gumtree_search(location, params)
        except Exception as e: gt_error = str(e)

    def fetch_spareroom():
        nonlocal sr_results, sr_total, sr_error
        try: sr_results, sr_total = spareroom_search(location, params)
        except Exception as e: sr_error = str(e)

    threads = [
        threading.Thread(target=fetch_rm, daemon=True),
        threading.Thread(target=fetch_otm, daemon=True),
        threading.Thread(target=fetch_zoopla, daemon=True),
        threading.Thread(target=fetch_gumtree, daemon=True),
        threading.Thread(target=fetch_spareroom, daemon=True),
    ]
    for t in threads: t.start()
    for t, timeout in zip(threads, [25, 25, 25, 20, 20]): t.join(timeout=timeout)

    all_results = rm_results + otm_results + zp_results + gt_results + sr_results
    sort_val = params.get("sort", "newest")

    def _price_num(p):
        digits = re.sub(r"[^0-9]", "", p.get("price", "") or "")
        return int(digits) if digits else 0

    def _is_rental(p):
        s = (p.get("price", "") or "").lower()
        return "pcm" in s or " pw" in s or "per week" in s or "per month" in s

    if sort_val == "price_asc":
        all_results.sort(key=lambda p: (_price_num(p) or 999_999_999) if not _is_rental(p) else 900_000_000 + (_price_num(p) or 0))
    elif sort_val == "price_desc":
        all_results.sort(key=lambda p: _price_num(p), reverse=True)
    elif sort_val == "beds_desc":
        all_results.sort(key=lambda p: -(p.get("bedrooms") or 0))
    else:
        sources_list = [s for s in [rm_results, otm_results, zp_results, gt_results, sr_results] if s]
        interleaved = []
        max_len = max((len(s) for s in sources_list), default=0)
        for i in range(max_len):
            for s in sources_list:
                if i < len(s): interleaved.append(s[i])
        all_results = interleaved

    sources = []
    source_totals = {}
    for name, results, total, error in [
        ("Rightmove", rm_results, rm_total, rm_error),
        ("OnTheMarket", otm_results, otm_total, otm_error),
        ("Gumtree", gt_results, gt_total, gt_error),
        ("SpareRoom", sr_results, sr_total, sr_error),
    ]:
        if results or total: sources.append(name); source_totals[name] = total
        elif error: sources.append(f"{name} (error)"); source_totals[name] = 0
        else: source_totals[name] = 0

    if zp_results or zp_total:
        sources.append("Zoopla"); source_totals["Zoopla"] = zp_total
    else:
        source_totals["Zoopla"] = 0

    return {
        "results": all_results,
        "total": rm_total + otm_total + zp_total + gt_total + sr_total,
        "shown": len(all_results), "location": location,
        "sources": sources, "source_totals": source_totals,
        "zoopla_status": zp_error or ("ok" if (zp_results or zp_total) else "no_results"),
        "errors": {k: v for k, v in {"Rightmove": rm_error, "OnTheMarket": otm_error, "Zoopla": zp_error, "Gumtree": gt_error, "SpareRoom": sr_error}.items() if v},
    }

# ═══════════════════════════════════════════════════════════════
# CARS — AUTOTRADER
# ═══════════════════════════════════════════════════════════════

def autotrader_search(location: str, params: dict) -> tuple:
    """
    Scrape AutoTrader via Byparr/FlareSolverr bypass.
    Returns (list_of_cars, total_count).
    """
    qp = {"postcode": location, "sort": "relevance", "page": str(params.get("page", 1))}
    if params.get("car_make") and params["car_make"].lower() not in ("", "any"): qp["make"] = params["car_make"]
    if params.get("car_model") and params["car_model"].lower() not in ("", "any"): qp["model"] = params["car_model"]
    if params.get("car_min_price"): qp["price-from"] = params["car_min_price"]
    if params.get("car_max_price"): qp["price-to"] = params["car_max_price"]
    if params.get("car_year_from"): qp["year-from"] = params["car_year_from"]
    if params.get("car_year_to"): qp["year-to"] = params["car_year_to"]
    if params.get("car_fuel") and params["car_fuel"].lower() not in ("", "any"): qp["fuel-type"] = params["car_fuel"]
    if params.get("car_transmission") and params["car_transmission"].lower() not in ("", "any"): qp["transmission"] = params["car_transmission"]

    url = f"https://www.autotrader.co.uk/cars?{urllib.parse.urlencode(qp)}"
    print(f"  [AutoTrader] Fetching: {url}")

    try:
        html = bypass_fetch(url, timeout=45)
    except Exception as e:
        print(f"  [AutoTrader] All bypass methods failed: {e}")
        return [], 0

    cars = []

    # Try __NEXT_DATA__ first
    nd_m = re.search(r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>', html, re.DOTALL)
    if nd_m:
        try:
            nd = json.loads(nd_m.group(1))
            pp = nd.get("props", {}).get("pageProps", {})
            results_raw = (pp.get("initialData", {}).get("results", {}).get("advertSummary", [])
                           or pp.get("search", {}).get("results", {}).get("advertSummary", []))
            total_raw = pp.get("initialData", {}).get("results", {}).get("totalResults", 0) or 0
            for r in results_raw:
                try:
                    ad_id = str(r.get("id", ""))
                    title = r.get("title", "") or f"{r.get('make','')} {r.get('model','')}".strip()
                    price_obj = r.get("price", {})
                    if isinstance(price_obj, dict):
                        price = price_obj.get("displayPrice", "") or f"£{price_obj.get('advertisedPrice',0):,}"
                        price_num = int(price_obj.get("advertisedPrice", 0) or 0)
                    else:
                        price = f"£{price_obj:,}" if price_obj else ""
                        price_num = int(price_obj) if price_obj else 0
                    spec = r.get("spec", {}) or {}
                    cars.append({
                        "id": f"at_{ad_id}", "title": title,
                        "make": r.get("make", ""), "model": r.get("model", ""),
                        "year": str(spec.get("year", r.get("year", ""))),
                        "price": price, "price_num": price_num,
                        "mileage": f"{spec.get('mileage','')} miles" if spec.get("mileage") else "",
                        "fuel": spec.get("fuelType", ""), "transmission": spec.get("transmission", ""),
                        "image": (r.get("images", [{}]) or [{}])[0].get("url", ""),
                        "description": r.get("description", "")[:200],
                        "url": f"https://www.autotrader.co.uk/car-details/{ad_id}",
                        "source": "AutoTrader",
                        "source_url": f"https://www.autotrader.co.uk/car-details/{ad_id}",
                        "location": location,
                    })
                except Exception: pass
            if cars:
                print(f"  [AutoTrader] ✓ {len(cars)} from __NEXT_DATA__ (total: {total_raw})")
                return cars, total_raw or len(cars)
        except Exception as e:
            print(f"  [AutoTrader] __NEXT_DATA__ parse failed: {e}")

    # HTML regex fallback
    total_m = re.search(r'"totalResults"\s*:\s*(\d+)', html)
    total = int(total_m.group(1)) if total_m else 0
    for chunk in re.findall(r'<article[^>]*>(.*?)</article>', html, re.DOTALL)[:20]:
        try:
            title_m = re.search(r'<h2[^>]*>([^<]+)</h2>', chunk)
            title = title_m.group(1).strip() if title_m else ""
            price_m = re.search(r'£[\d,]+', chunk)
            price = price_m.group(0) if price_m else ""
            price_num = int(re.sub(r'[^\d]', '', price)) if price else 0
            href_m = re.search(r'href="(/car-details/[^"]+)"', chunk)
            listing_url = f"https://www.autotrader.co.uk{href_m.group(1)}" if href_m else ""
            ad_id = href_m.group(1).split("/")[-1] if href_m else ""
            img_m = re.search(r'<img[^>]+src="(https://[^"]+)"', chunk)
            image = img_m.group(1) if img_m else ""
            year_m = re.search(r'\b(19|20)\d{2}\b', chunk)
            year = year_m.group(0) if year_m else ""
            mileage_m = re.search(r'([\d,]+)\s*miles', chunk, re.I)
            mileage = f"{mileage_m.group(1)} miles" if mileage_m else ""
            if not title or not listing_url: continue
            cars.append({
                "id": f"at_{ad_id}", "title": title,
                "make": title.split()[0] if title else "",
                "model": " ".join(title.split()[1:3]) if len(title.split()) > 1 else "",
                "year": year, "price": price, "price_num": price_num,
                "mileage": mileage, "fuel": "", "transmission": "",
                "image": image, "description": "",
                "url": listing_url, "source": "AutoTrader",
                "source_url": listing_url, "location": location,
            })
        except Exception: pass

    print(f"  [AutoTrader] ✓ {len(cars)} via HTML regex (total: {total})")
    return cars, total or len(cars)

# ═══════════════════════════════════════════════════════════════
# CARS — EXCHANGE & MART
# ═══════════════════════════════════════════════════════════════

def _parse_price_int(price_str: str) -> int:
    try: return int(re.sub(r'[^\d]', '', price_str))
    except: return 0


def exchangeandmart_search(location: str, params: dict) -> tuple:
    qp = {"location": location}
    if params.get("car_min_price"): qp["price-from"] = params["car_min_price"]
    if params.get("car_max_price"): qp["price-to"] = params["car_max_price"]
    if params.get("car_year_from"): qp["year-from"] = params["car_year_from"]
    if params.get("car_make") and params["car_make"].lower() not in ("", "any"): qp["make"] = params["car_make"]
    if params.get("car_model") and params["car_model"].lower() not in ("", "any"): qp["model"] = params["car_model"]
    if params.get("car_fuel") and params["car_fuel"].lower() not in ("", "any"): qp["fuel-type"] = params["car_fuel"]
    if params.get("car_transmission") and params["car_transmission"].lower() not in ("", "any"): qp["transmission"] = params["car_transmission"]
    index = int(params.get("index", 0))
    page = index // 15 + 1
    if page > 1: qp["page"] = str(page)
    url = "https://www.exchangeandmart.co.uk/used-cars-for-sale?" + urllib.parse.urlencode(qp)
    print(f"  [E&M] Fetching: {url}")
    try:
        req = urllib.request.Request(url, headers=BROWSER_HEADERS)
        with urllib.request.urlopen(req, timeout=20) as resp:
            html = resp.read().decode("utf-8", errors="replace")
    except Exception as e:
        print(f"  [E&M] Error: {e}")
        return [], 0
    total_match = re.search(r'(\d[\d,]*)\s+(?:used\s+)?(?:cars?|vehicles?|results?)\s+found', html, re.IGNORECASE)
    if total_match: total = int(re.sub(r'[^\d]', '', total_match.group(1)))
    else: total = len(re.findall(r'adid="\d+"', html)) * 20
    parts = re.split(r'(?=<div class="result-item")', html)
    cars = []
    for item in parts[1:16]:
        try:
            make = re.search(r'\bmake="([^"]+)"', item)
            model = re.search(r'\bmodel="([^"]+)"', item)
            make = make.group(1).strip() if make else ""
            model = model.group(1).strip() if model else ""
            variant_m = re.search(r'class="result-item__variant"[^>]*>([^<]+)<', item)
            variant = variant_m.group(1).strip() if variant_m else ""
            title = f"{make} {model} {variant}".strip()
            price_m = re.search(r'class="price price--primary"[^>]*>(£[\d,]+)', item)
            if not price_m: price_m = re.search(r'class="price[^"]*"[^>]*>(£[\d,]+)', item)
            price = price_m.group(1).strip() if price_m else ""
            href_m = re.search(r'href="(/ad/\d+)"', item)
            url_suffix = href_m.group(1) if href_m else ""
            listing_url = f"https://www.exchangeandmart.co.uk{url_suffix}" if url_suffix else ""
            kd = re.findall(r'class="key-details__item">([^<]+)<', item)
            year = next((k.strip() for k in kd if re.match(r'^\s*\d{4}\s*$', k)), "")
            mileage_raw = next((k for k in kd if "mile" in k.lower()), "")
            mileage = re.sub(r'(?i)mileage:\s*', '', mileage_raw).strip()
            fuel = next((k.strip() for k in kd if k.strip().lower() in ("petrol","diesel","electric","hybrid","plug-in hybrid")), "")
            transmission = next((k.strip() for k in kd if k.strip().lower() in ("manual","automatic","semi-auto","semi-automatic")), "")
            img_m = re.search(r'data-mainimage="(https?://[^"]+)"', item)
            if not img_m: img_m = re.search(r'<img[^>]+src="(https?://[^"]+\.(?:jpg|jpeg|png|webp)[^"]*)"', item, re.IGNORECASE)
            image = img_m.group(1) if img_m else ""
            if not title.strip() or not price: continue
            cars.append({
                "id": f"em_{url_suffix.replace('/ad/','')}",
                "title": title, "make": make, "model": model, "year": year,
                "price": price, "price_num": _parse_price_int(price),
                "mileage": mileage, "fuel": fuel, "transmission": transmission,
                "image": image, "description": "",
                "url": listing_url, "source": "Exchange&Mart",
                "source_url": listing_url, "agent": "", "agent_phone": "",
            })
        except Exception: pass
    print(f"  [E&M] Found {len(cars)} (total: {total})")
    return cars, total

# ═══════════════════════════════════════════════════════════════
# CARS — GUMTREE
# ═══════════════════════════════════════════════════════════════

def gumtree_car_search(location: str, params: dict) -> tuple:
    loc_slug = re.sub(r'[^a-z0-9]+', '-', location.strip().lower()).strip('-')
    qp = {}
    if params.get("car_min_price"): qp["min_price"] = params["car_min_price"]
    if params.get("car_max_price"): qp["max_price"] = params["car_max_price"]
    if params.get("car_year_from"): qp["min_year"] = params["car_year_from"]
    index = int(params.get("index", 0))
    page = index // 20 + 1
    if page > 1: qp["page"] = str(page)
    sort_val = params.get("sort", "newest")
    if sort_val == "price_asc": qp["sort"] = "price_asc"
    elif sort_val == "price_desc": qp["sort"] = "price_desc"
    qs = urllib.parse.urlencode(qp)
    url = f"https://www.gumtree.com/cars/uk/{loc_slug}{'?' + qs if qs else ''}"
    print(f"  [GT Cars] Fetching: {url}")
    try:
        req = urllib.request.Request(url, headers=BROWSER_HEADERS)
        with urllib.request.urlopen(req, timeout=20) as resp:
            if resp.status not in (200, 201): return [], 0
            html = resp.read().decode("utf-8", errors="replace")
    except Exception as e:
        print(f"  [GT Cars] Error: {e}")
        return [], 0
    if len(html) < 5000: return [], 0
    article_chunks = re.findall(r'<article[^>]+data-q="[^"]*listing[^"]*"[^>]*>(.*?)(?=<article|</section)', html, re.DOTALL)
    cars = []
    for chunk in article_chunks[:20]:
        try:
            title_m = re.search(r'data-q="listing-title"[^>]*>([^<]+)<', chunk)
            if not title_m: title_m = re.search(r'<h2[^>]*>.*?<a[^>]*>([^<]+)</a>', chunk, re.DOTALL)
            title = title_m.group(1).strip() if title_m else ""
            price_m = re.search(r'data-q="listing-price"[^>]*>(£[\d,]+)', chunk)
            if not price_m: price_m = re.search(r'>(£[\d,]+)<', chunk)
            price = price_m.group(1).strip() if price_m else ""
            href_m = re.search(r'href="(/p/[^"]+)"', chunk)
            listing_url = f"https://www.gumtree.com{href_m.group(1)}" if href_m else ""
            img_m = re.search(r'src="(https://img\.gumtree\.com[^"]*)"', chunk, re.IGNORECASE)
            image = img_m.group(1) if img_m else ""
            year_m = re.search(r'\b(19[89]\d|20[012]\d)\b', title)
            year = year_m.group(1) if year_m else ""
            if not title or not price: continue
            cars.append({
                "id": f"gt_car_{re.sub(r'[^a-z0-9]','_',title.lower())[:30]}",
                "title": title,
                "make": title.split()[0] if title else "",
                "model": title.split()[1] if len(title.split()) > 1 else "",
                "year": year, "price": price, "price_num": _parse_price_int(price),
                "mileage": "", "fuel": "", "transmission": "",
                "image": image, "description": "",
                "url": listing_url, "source": "Gumtree",
                "source_url": listing_url, "agent": "Private Seller", "agent_phone": "",
            })
        except Exception: pass
    total_m = re.search(r'([\d,]+)\s+(?:cars?|vehicles?|ads?)\s+(?:found|available)', html, re.IGNORECASE)
    total = int(re.sub(r'[^\d]', '', total_m.group(1))) if total_m else len(cars)
    print(f"  [GT Cars] Found {len(cars)} (total: {total})")
    return cars, total

# ═══════════════════════════════════════════════════════════════
# COMBINED CAR SEARCH
# ═══════════════════════════════════════════════════════════════


# ═══════════════════════════════════════════════════════════════
# ═══════════════════════════════════════════════════════════════
# CARS — CARGURUS
# CarGurus returns JSON directly — no browser needed
# ═══════════════════════════════════════════════════════════════

CG_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Accept-Language": "en-GB,en;q=0.9",
    "Referer": "https://www.cargurus.co.uk/",
    "X-Requested-With": "XMLHttpRequest",
}

def cargurus_car_search(location: str, params: dict) -> tuple:
    """
    Scrape CarGurus UK listings via their JSON search API.
    Returns (list_of_cars, total_count).
    """
    qp = {
        "zip": location,
        "distance": "100",
        "listingTypes": "USED,CERTIFIED,NEW",
        "offset": str(int(params.get("index", 0))),
    }
    if params.get("car_min_price"): qp["minPrice"] = params["car_min_price"]
    if params.get("car_max_price"): qp["maxPrice"] = params["car_max_price"]
    if params.get("car_year_from"): qp["startYear"] = params["car_year_from"]
    if params.get("car_year_to"): qp["endYear"] = params["car_year_to"]
    if params.get("car_make") and params["car_make"].lower() not in ("", "any"):
        qp["selectedEntity"] = params["car_make"]
    if params.get("car_max_mileage"): qp["maxMileage"] = params["car_max_mileage"]

    url = f"https://www.cargurus.co.uk/Cars/searchResults.action?{urllib.parse.urlencode(qp)}"
    print(f"  [CarGurus] Fetching: {url}")

    try:
        req = urllib.request.Request(url, headers=CG_HEADERS)
        with urllib.request.urlopen(req, timeout=15) as resp:
            text = resp.read().decode("utf-8", errors="replace")
    except Exception as e:
        print(f"  [CarGurus] Error: {e}")
        return [], 0

    # CarGurus returns a page with embedded JSON — extract it
    listings = []
    total = 0

    # Try __NEXT_DATA__ first
    nd_m = re.search(r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>', text, re.DOTALL)
    if nd_m:
        try:
            nd = json.loads(nd_m.group(1))
            # Navigate to listings
            results_raw = (
                nd.get("props", {}).get("pageProps", {}).get("initialData", {}).get("listings", [])
                or nd.get("props", {}).get("pageProps", {}).get("listings", [])
            )
            total = nd.get("props", {}).get("pageProps", {}).get("totalListings", len(results_raw))
            for r in results_raw:
                try:
                    listing_id = str(r.get("id", ""))
                    price = r.get("priceString", "") or (f"£{r.get('price', 0):,}" if r.get("price") else "")
                    price_num = int(r.get("price", 0) or 0)
                    listings.append({
                        "id": f"cg_{listing_id}",
                        "title": r.get("heading", "") or f"{r.get('modelYear','')} {r.get('make','')} {r.get('model','')}".strip(),
                        "make": r.get("make", ""),
                        "model": r.get("model", ""),
                        "year": str(r.get("modelYear", "")),
                        "price": price,
                        "price_num": price_num,
                        "mileage": f"{r.get('mileage', 0):,} miles" if r.get("mileage") else "",
                        "fuel": r.get("fuelType", ""),
                        "transmission": r.get("transmission", ""),
                        "image": r.get("mainPictureUrl", "") or r.get("pictureUrl", ""),
                        "description": r.get("trim", ""),
                        "url": f"https://www.cargurus.co.uk/Cars/listing/l{listing_id}",
                        "source": "CarGurus",
                        "source_url": f"https://www.cargurus.co.uk/Cars/listing/l{listing_id}",
                        "location": r.get("dealerCity", location),
                    })
                except Exception:
                    pass
            if listings:
                print(f"  [CarGurus] ✓ {len(listings)} from __NEXT_DATA__ (total: {total})")
                return listings, total
        except Exception as e:
            print(f"  [CarGurus] __NEXT_DATA__ parse failed: {e}")

    # Fallback: look for inline JSON
    json_m = re.search(r'window\.CG_DATA\s*=\s*({.*?});\s*</script>', text, re.DOTALL)
    if json_m:
        try:
            data = json.loads(json_m.group(1))
            for r in data.get("listings", [])[:20]:
                try:
                    listing_id = str(r.get("id", ""))
                    price_num = int(r.get("price", 0) or 0)
                    listings.append({
                        "id": f"cg_{listing_id}",
                        "title": f"{r.get('modelYear','')} {r.get('make','')} {r.get('model','')}".strip(),
                        "make": r.get("make", ""),
                        "model": r.get("model", ""),
                        "year": str(r.get("modelYear", "")),
                        "price": f"£{price_num:,}" if price_num else "",
                        "price_num": price_num,
                        "mileage": f"{r.get('mileage', 0):,} miles" if r.get("mileage") else "",
                        "fuel": r.get("fuelType", ""),
                        "transmission": r.get("transmission", ""),
                        "image": r.get("mainPictureUrl", ""),
                        "description": "",
                        "url": f"https://www.cargurus.co.uk/Cars/listing/l{listing_id}",
                        "source": "CarGurus",
                        "source_url": f"https://www.cargurus.co.uk/Cars/listing/l{listing_id}",
                        "location": location,
                    })
                except Exception:
                    pass
            total = data.get("totalListings", len(listings))
        except Exception as e:
            print(f"  [CarGurus] JSON fallback failed: {e}")

    print(f"  [CarGurus] ✓ {len(listings)} listings (total: {total})")
    return listings, total


# ═══════════════════════════════════════════════════════════════
# CARS — CINCH
# Cinch embeds all data in __NEXT_DATA__ — no bypass needed
# ═══════════════════════════════════════════════════════════════

def cinch_car_search(location: str, params: dict) -> tuple:
    """
    Scrape Cinch UK listings via __NEXT_DATA__ JSON.
    Returns (list_of_cars, total_count).
    """
    qp = {
        "location": location,
        "pageSize": "32",
        "pageNumber": str(int(params.get("index", 0)) // 32 + 1),
    }
    if params.get("car_make") and params["car_make"].lower() not in ("", "any"):
        qp["make"] = params["car_make"]
    if params.get("car_model") and params["car_model"].lower() not in ("", "any"):
        qp["model"] = params["car_model"]
    if params.get("car_min_price"): qp["priceFrom"] = params["car_min_price"]
    if params.get("car_max_price"): qp["priceTo"] = params["car_max_price"]
    if params.get("car_year_from"): qp["yearFrom"] = params["car_year_from"]
    if params.get("car_year_to"): qp["yearTo"] = params["car_year_to"]
    if params.get("car_fuel") and params["car_fuel"].lower() not in ("", "any"):
        qp["fuelType"] = params["car_fuel"]
    if params.get("car_transmission") and params["car_transmission"].lower() not in ("", "any"):
        qp["transmission"] = params["car_transmission"]
    if params.get("car_max_mileage"): qp["mileageTo"] = params["car_max_mileage"]

    url = f"https://www.cinch.co.uk/used-cars?{urllib.parse.urlencode(qp)}"
    print(f"  [Cinch] Fetching: {url}")

    try:
        req = urllib.request.Request(url, headers=BROWSER_HEADERS)
        with urllib.request.urlopen(req, timeout=20) as resp:
            text = resp.read().decode("utf-8", errors="replace")
    except Exception as e:
        print(f"  [Cinch] Error: {e}")
        return [], 0

    # Cinch embeds everything in __NEXT_DATA__
    match = re.search(r'<script id="__NEXT_DATA__" type="application/json">([\s\S]*?)</script>', text)
    if not match:
        print("  [Cinch] No __NEXT_DATA__ found")
        return [], 0

    try:
        nd = json.loads(match.group(1))
        # Navigate Cinch's data structure
        pp = nd.get("props", {}).get("pageProps", {})
        vehicles = (
            pp.get("initialData", {}).get("vehicles", [])
            or pp.get("vehicles", [])
            or pp.get("results", {}).get("vehicles", [])
            or []
        )
        total = (
            pp.get("initialData", {}).get("totalCount", 0)
            or pp.get("totalCount", 0)
            or len(vehicles)
        )

        listings = []
        for v in vehicles:
            try:
                vid = str(v.get("id", "") or v.get("vehicleId", ""))
                make = v.get("make", "") or v.get("manufacturer", "")
                model = v.get("model", "")
                year = str(v.get("year", "") or v.get("registrationYear", ""))
                price_num = int(v.get("price", 0) or v.get("retailPrice", 0) or 0)
                price = f"£{price_num:,}" if price_num else ""
                mileage = v.get("mileage", 0) or 0
                slug = v.get("slug", "") or v.get("urlSlug", "")
                listing_url = f"https://www.cinch.co.uk/used-cars/{slug}" if slug else f"https://www.cinch.co.uk/used-cars/{vid}"
                images = v.get("images", []) or v.get("photos", []) or []
                image = images[0].get("url", "") if images and isinstance(images[0], dict) else (images[0] if images else "")

                listings.append({
                    "id": f"cinch_{vid}",
                    "title": f"{year} {make} {model}".strip(),
                    "make": make,
                    "model": model,
                    "year": year,
                    "price": price,
                    "price_num": price_num,
                    "mileage": f"{mileage:,} miles" if mileage else "",
                    "fuel": v.get("fuelType", "") or v.get("fuel", ""),
                    "transmission": v.get("transmission", "") or v.get("gearbox", ""),
                    "image": image,
                    "description": v.get("description", "") or v.get("shortDescription", ""),
                    "url": listing_url,
                    "source": "Cinch",
                    "source_url": listing_url,
                    "location": "Nationwide (Cinch delivers)",
                })
            except Exception:
                pass

        print(f"  [Cinch] ✓ {len(listings)} listings (total: {total})")
        return listings, total

    except Exception as e:
        print(f"  [Cinch] Parse error: {e}")
        return [], 0


# ═══════════════════════════════════════════════════════════════
# CARS — MOTORS.CO.UK
# Protected by Cloudflare — uses bypass_fetch
# ═══════════════════════════════════════════════════════════════

def motors_car_search(location: str, params: dict) -> tuple:
    """
    Scrape Motors.co.uk listings via Byparr/FlareSolverr bypass.
    Returns (list_of_cars, total_count).
    """
    qp = {
        "action": "search",
        "searchtype": "used-cars",
        "location": location,
        "radius": "50",
    }
    if params.get("car_make") and params["car_make"].lower() not in ("", "any"):
        qp["make"] = params["car_make"]
    if params.get("car_model") and params["car_model"].lower() not in ("", "any"):
        qp["model"] = params["car_model"]
    if params.get("car_min_price"): qp["price_from"] = params["car_min_price"]
    if params.get("car_max_price"): qp["price_to"] = params["car_max_price"]
    if params.get("car_year_from"): qp["year_from"] = params["car_year_from"]
    if params.get("car_fuel") and params["car_fuel"].lower() not in ("", "any"):
        qp["fuel_type"] = params["car_fuel"]
    index = int(params.get("index", 0))
    if index > 0: qp["page"] = str(index // 20 + 1)

    url = f"https://www.motors.co.uk/search/?{urllib.parse.urlencode(qp)}"
    print(f"  [Motors] Fetching: {url}")

    try:
        html = bypass_fetch(url, timeout=45)
    except Exception as e:
        print(f"  [Motors] Bypass failed: {e}")
        return [], 0

    listings = []
    total = 0

    # Try __NEXT_DATA__ first
    nd_m = re.search(r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>', html, re.DOTALL)
    if nd_m:
        try:
            nd = json.loads(nd_m.group(1))
            pp = nd.get("props", {}).get("pageProps", {})
            results_raw = (
                pp.get("results", {}).get("vehicles", [])
                or pp.get("vehicles", [])
                or pp.get("listings", [])
            )
            total = pp.get("results", {}).get("totalCount", 0) or pp.get("totalCount", len(results_raw))
            for r in results_raw:
                try:
                    vid = str(r.get("id", "") or r.get("vehicleId", ""))
                    make = r.get("make", "") or r.get("manufacturer", "")
                    model = r.get("model", "")
                    year = str(r.get("year", "") or r.get("registrationYear", ""))
                    price_num = int(r.get("price", 0) or r.get("advertisedPrice", 0) or 0)
                    price = f"£{price_num:,}" if price_num else ""
                    mileage = r.get("mileage", 0) or 0
                    slug = r.get("slug", "") or r.get("url", "")
                    listing_url = (f"https://www.motors.co.uk{slug}" if slug.startswith("/") else slug) or f"https://www.motors.co.uk/used-cars/{vid}"
                    images = r.get("images", []) or r.get("photos", []) or []
                    image = images[0].get("url", "") if images and isinstance(images[0], dict) else (images[0] if images else "")
                    listings.append({
                        "id": f"motors_{vid}",
                        "title": f"{year} {make} {model}".strip(),
                        "make": make, "model": model, "year": year,
                        "price": price, "price_num": price_num,
                        "mileage": f"{mileage:,} miles" if mileage else "",
                        "fuel": r.get("fuelType", "") or r.get("fuel", ""),
                        "transmission": r.get("transmission", "") or r.get("gearbox", ""),
                        "image": image,
                        "description": r.get("description", "")[:200],
                        "url": listing_url,
                        "source": "Motors",
                        "source_url": listing_url,
                        "location": r.get("location", location),
                    })
                except Exception:
                    pass
            if listings:
                print(f"  [Motors] ✓ {len(listings)} from __NEXT_DATA__ (total: {total})")
                return listings, total
        except Exception as e:
            print(f"  [Motors] __NEXT_DATA__ parse failed: {e}")

    # HTML regex fallback
    total_m = re.search(r'"totalCount"\s*:\s*(\d+)', html)
    if total_m: total = int(total_m.group(1))

    chunks = re.findall(r'class="[^"]*listing-card[^"]*"[^>]*>(.*?)(?=class="[^"]*listing-card|</section)', html, re.DOTALL)
    for chunk in chunks[:20]:
        try:
            title_m = re.search(r'<h2[^>]*>([^<]+)</h2>', chunk)
            title = title_m.group(1).strip() if title_m else ""
            price_m = re.search(r'£[\d,]+', chunk)
            price = price_m.group(0) if price_m else ""
            price_num = int(re.sub(r'[^\d]', '', price)) if price else 0
            href_m = re.search(r'href="(/used-cars/[^"]+)"', chunk)
            listing_url = f"https://www.motors.co.uk{href_m.group(1)}" if href_m else ""
            vid = href_m.group(1).split("/")[-1] if href_m else ""
            img_m = re.search(r'src="(https://[^"]+)"', chunk)
            image = img_m.group(1) if img_m else ""
            year_m = re.search(r'\b(19|20)\d{2}\b', chunk)
            year = year_m.group(0) if year_m else ""
            mileage_m = re.search(r'([\d,]+)\s*miles', chunk, re.I)
            mileage = f"{mileage_m.group(1)} miles" if mileage_m else ""
            if not title or not listing_url: continue
            listings.append({
                "id": f"motors_{vid}",
                "title": title,
                "make": title.split()[0] if title else "",
                "model": " ".join(title.split()[1:3]) if len(title.split()) > 1 else "",
                "year": year, "price": price, "price_num": price_num,
                "mileage": mileage, "fuel": "", "transmission": "",
                "image": image, "description": "",
                "url": listing_url, "source": "Motors",
                "source_url": listing_url, "location": location,
            })
        except Exception:
            pass

    print(f"  [Motors] ✓ {len(listings)} via HTML regex (total: {total})")
    return listings, total


# ═══════════════════════════════════════════════════════════════
# COMBINED CAR SEARCH — ALL SOURCES
# Replace existing combined_car_search with this
# ═══════════════════════════════════════════════════════════════

def combined_car_search(location: str, params: dict) -> dict:
    """
    Run all car scrapers in parallel.
    Sources: AutoTrader, CarGurus, Cinch, Motors, Exchange&Mart, Gumtree
    """
    at_results, at_total     = [], 0
    cg_results, cg_total     = [], 0
    cn_results, cn_total     = [], 0
    mo_results, mo_total     = [], 0
    em_results, em_total     = [], 0
    gt_results, gt_total     = [], 0
    at_error = cg_error = cn_error = mo_error = em_error = gt_error = None

    def fetch_at():
        nonlocal at_results, at_total, at_error
        try: at_results, at_total = autotrader_search(location, params)
        except Exception as e: at_error = str(e); print(f"  [AT] Error: {e}")

    def fetch_cg():
        nonlocal cg_results, cg_total, cg_error
        try: cg_results, cg_total = cargurus_car_search(location, params)
        except Exception as e: cg_error = str(e); print(f"  [CG] Error: {e}")

    def fetch_cn():
        nonlocal cn_results, cn_total, cn_error
        try: cn_results, cn_total = cinch_car_search(location, params)
        except Exception as e: cn_error = str(e); print(f"  [Cinch] Error: {e}")

    def fetch_mo():
        nonlocal mo_results, mo_total, mo_error
        try: mo_results, mo_total = motors_car_search(location, params)
        except Exception as e: mo_error = str(e); print(f"  [Motors] Error: {e}")

    def fetch_em():
        nonlocal em_results, em_total, em_error
        try: em_results, em_total = exchangeandmart_search(location, params)
        except Exception as e: em_error = str(e); print(f"  [E&M] Error: {e}")

    def fetch_gt():
        nonlocal gt_results, gt_total, gt_error
        try: gt_results, gt_total = gumtree_car_search(location, params)
        except Exception as e: gt_error = str(e); print(f"  [GT] Error: {e}")

    threads = [
        threading.Thread(target=fetch_at, daemon=True),
        threading.Thread(target=fetch_cg, daemon=True),
        threading.Thread(target=fetch_cn, daemon=True),
        threading.Thread(target=fetch_mo, daemon=True),
        threading.Thread(target=fetch_em, daemon=True),
        threading.Thread(target=fetch_gt, daemon=True),
    ]
    for t in threads: t.start()
    # AutoTrader and Motors need longer timeout (bypass stack)
    timeouts = [50, 20, 20, 50, 25, 25]
    for t, timeout in zip(threads, timeouts): t.join(timeout=timeout)

    # Interleave results — one from each source in rotation
    all_sources = [at_results, cg_results, cn_results, mo_results, em_results, gt_results]
    all_results = []
    max_len = max((len(s) for s in all_sources), default=0)
    for i in range(max_len):
        for source in all_sources:
            if i < len(source): all_results.append(source[i])

    # Sort if requested
    sort_val = params.get("sort", "newest")
    if sort_val == "price_asc":
        all_results.sort(key=lambda x: x.get("price_num") or 999_999_999)
    elif sort_val == "price_desc":
        all_results.sort(key=lambda x: x.get("price_num") or 0, reverse=True)
    elif sort_val == "year_desc":
        all_results.sort(key=lambda x: int(x.get("year", 0) or 0), reverse=True)

    # Build source totals
    source_totals = {}
    sources = []
    for name, results, total in [
        ("AutoTrader", at_results, at_total),
        ("CarGurus", cg_results, cg_total),
        ("Cinch", cn_results, cn_total),
        ("Motors", mo_results, mo_total),
        ("Exchange&Mart", em_results, em_total),
        ("Gumtree", gt_results, gt_total),
    ]:
        source_totals[name] = total
        if results: sources.append(name)

    print(f"\n🚗 Cars total: AT={at_total} CG={cg_total} Cinch={cn_total} Motors={mo_total} E&M={em_total} GT={gt_total}")

    return {
        "results": all_results,
        "total": at_total + cg_total + cn_total + mo_total + em_total + gt_total,
        "shown": len(all_results),
        "location": location,
        "sources": sources,
        "source_totals": source_totals,
        "errors": {k: v for k, v in {
            "AutoTrader": at_error, "CarGurus": cg_error, "Cinch": cn_error,
            "Motors": mo_error, "Exchange&Mart": em_error, "Gumtree": gt_error,
        }.items() if v},
        "category": "cars",
    }

def _reed_slug(query: str, location: str) -> str:
    def slugify(s): return re.sub(r'[^a-z0-9]+', '-', s.strip().lower()).strip('-')
    q = slugify(query) if query else "jobs"
    loc_clean = location.strip().lower() if location else ""
    loc = "anywhere" if not loc_clean or loc_clean in ("uk", "united kingdom", "nationwide", "anywhere") else slugify(location)
    return f"{q}-jobs-in-{loc}"


def reed_search(query: str, location: str, params: dict) -> tuple:
    slug = _reed_slug(query, location)
    qp = {}
    if params.get("job_min_salary"): qp["salaryFrom"] = params["job_min_salary"]
    if params.get("job_max_salary"): qp["salaryTo"] = params["job_max_salary"]
    if params.get("job_radius"): qp["proximity"] = params["job_radius"]
    if params.get("job_type") and params["job_type"] != "any":
        t = {"permanent": "permanent", "contract": "contract", "temp": "temp", "part_time": "part-time", "full_time": "full-time"}.get(params["job_type"], "")
        if t: qp["contract"] = t
    index = int(params.get("index", 0))
    if index > 0: qp["pageno"] = str(index // 25 + 1)
    qs = urllib.parse.urlencode(qp)
    url = f"https://www.reed.co.uk/jobs/{slug}{'?' + qs if qs else ''}"
    print(f"  [Reed] Fetching: {url}")
    try:
        req = urllib.request.Request(url, headers=BROWSER_HEADERS)
        with urllib.request.urlopen(req, timeout=20) as resp:
            html = resp.read().decode("utf-8", errors="replace")
    except Exception as e:
        print(f"  [Reed] Error: {e}")
        return [], 0
    next_data = re.findall(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', html, re.DOTALL)
    if not next_data: return [], 0
    try:
        d = json.loads(next_data[0])
        sr = d.get("props", {}).get("pageProps", {}).get("searchResults", {})
        raw_jobs = sr.get("jobs", []) + sr.get("promotedJobs", [])[:2]
        total = sr.get("count", 0)
        jobs = []
        for rj in raw_jobs:
            try:
                jd = rj.get("jobDetail", {})
                job_id = jd.get("jobId", "")
                salary_from = jd.get("salaryFrom", 0)
                salary_to = jd.get("salaryTo", 0)
                salary_desc = jd.get("salaryDescription", "")
                salary = salary_desc or (f"£{salary_from:,} – £{salary_to:,}" if salary_from and salary_to else f"£{salary_from:,}+" if salary_from else "Salary not specified")
                job_url = f"https://www.reed.co.uk{rj.get('url', '')}" if rj.get("url", "").startswith("/") else rj.get("url", f"https://www.reed.co.uk/jobs/{job_id}")
                remote_labels = {0: "", 1: "Remote", 2: "Hybrid", 3: "Office"}
                jobs.append({
                    "id": f"reed_{job_id}", "title": jd.get("jobTitle", ""),
                    "company": jd.get("ouName", rj.get("profileName", "")),
                    "location": jd.get("displayLocationName", location),
                    "salary": salary, "salary_num": salary_from or 0,
                    "job_type": {1: "Permanent", 2: "Contract", 3: "Temp", 4: "Part-time"}.get(jd.get("jobType", 1), "Permanent"),
                    "is_full_time": jd.get("isFullTime", True),
                    "remote": remote_labels.get(jd.get("remoteWorkingOption", 0), ""),
                    "date_posted": jd.get("displayDate", ""),
                    "description": re.sub(r'<[^>]+>', '', jd.get("jobDescription", ""))[:300].strip(),
                    "url": job_url, "source": "Reed", "apply_url": job_url,
                    "logo": (rj.get("logoImage", {}) or {}).get("url", ""),
                    "taxonomy": jd.get("taxonomyLevel1", ""), "easy_apply": jd.get("isEasyApply", False),
                })
            except Exception: pass
        print(f"  [Reed] Found {len(jobs)} (total: {total})")
        return jobs, total
    except Exception as e:
        print(f"  [Reed] Parse error: {e}")
        return [], 0


def combined_job_search(query: str, location: str, params: dict) -> dict:
    reed_results, reed_total, reed_error = [], 0, None
    def fetch_reed():
        nonlocal reed_results, reed_total, reed_error
        try: reed_results, reed_total = reed_search(query, location, params)
        except Exception as e: reed_error = str(e)
    t = threading.Thread(target=fetch_reed, daemon=True)
    t.start(); t.join(timeout=25)
    all_results = list(reed_results)
    sort_val = params.get("sort", "newest")
    if sort_val == "salary_asc": all_results.sort(key=lambda x: x.get("salary_num", 0) or 0)
    elif sort_val == "salary_desc": all_results.sort(key=lambda x: x.get("salary_num", 0), reverse=True)
    return {
        "results": all_results, "total": reed_total, "shown": len(all_results),
        "query": query, "location": location,
        "sources": ["Reed"] if reed_results else [],
        "source_totals": {"Reed": reed_total},
        "errors": {"reed": reed_error} if reed_error else {},
        "category": "jobs",
    }

# ═══════════════════════════════════════════════════════════════
# EMAIL HELPER
# ═══════════════════════════════════════════════════════════════

def build_viewing_emails(user_info: dict, properties: list) -> dict:
    name = user_info.get("name", "")
    email = user_info.get("email", "")
    phone = user_info.get("phone", "")
    message = user_info.get("message", "")
    results = {}
    for prop in properties:
        prop_id = prop.get("id", "")
        agent = prop.get("agent", "The Agent")
        prop_url = prop.get("source_url", "")
        address = prop.get("address", "")
        price = prop.get("price", "")
        source = prop.get("source", "")
        body = (f"Dear {agent} Team,\n\nI am writing to request a viewing for:\n\n"
                f"Address: {address}\nAsking Price: {price}\nListed on: {source}\nListing URL: {prop_url}\n\n"
                f"My details:\nName: {name}\nEmail: {email}\nPhone: {phone}\n")
        if message: body += f"\nAdditional notes:\n{message}\n"
        body += f"\nKind regards,\n{name}"
        subject = f"Viewing Request — {address}"
        results[prop_id] = {
            "agent": agent, "agent_phone": prop.get("agent_phone", ""),
            "address": address, "price": price, "source": source,
            "subject": subject, "email_body": body,
            "mailto": f"mailto:?subject={urllib.parse.quote(subject)}&body={urllib.parse.quote(body)}",
            "status": "ready",
        }
    return results

# ═══════════════════════════════════════════════════════════════
# HTTP HANDLER
# ═══════════════════════════════════════════════════════════════

class CrabifyHandler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        print(f"[{self.client_address[0]}] {fmt % args}")

    def cors_headers(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")

    def send_json(self, data, status=200):
        body = json.dumps(data, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.cors_headers()
        self.end_headers()
        self.wfile.write(body)

    def do_OPTIONS(self):
        self.send_response(200)
        self.cors_headers()
        self.end_headers()

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path
        qs = urllib.parse.parse_qs(parsed.query)

        if path == "/health":
            self.send_json({"status": "ok", "sources": {
                "property": ["Rightmove", "OnTheMarket", "Zoopla", "Gumtree", "SpareRoom"],
                "cars": ["AutoTrader", "Exchange&Mart", "Gumtree"],
                "jobs": ["Reed"],
            }})

        elif path == "/api/search":
            location = qs.get("location", [""])[0].strip()
            if not location:
                self.send_json({"error": "Missing location"}, 400); return
            params = {
                "transaction_type": qs.get("type", ["buy"])[0],
                "radius": qs.get("radius", ["1.0"])[0],
                "min_beds": qs.get("min_beds", [""])[0],
                "max_beds": qs.get("max_beds", [""])[0],
                "min_price": qs.get("min_price", [""])[0],
                "max_price": qs.get("max_price", [""])[0],
                "property_type": qs.get("property_type", ["any"])[0],
                "index": int(qs.get("index", ["0"])[0]),
                "must_parking": qs.get("must_parking", ["false"])[0].lower() == "true",
                "must_garden": qs.get("must_garden", ["false"])[0].lower() == "true",
                "sort": qs.get("sort", ["newest"])[0],
            }
            print(f"\n🔍 Search: '{location}' | {params}")
            self.send_json(combined_search(location, params))

        elif path == "/api/cars":
            location = qs.get("location", [""])[0].strip()
            if not location:
                self.send_json({"error": "Missing location"}, 400); return
            car_params = {
                "car_make": qs.get("make", [""])[0],
                "car_model": qs.get("model", [""])[0],
                "car_min_price": qs.get("min_price", [""])[0],
                "car_max_price": qs.get("max_price", [""])[0],
                "car_year_from": qs.get("min_year", [""])[0] or qs.get("year_from", [""])[0],
                "car_year_to": qs.get("max_year", [""])[0] or qs.get("year_to", [""])[0],
                "car_fuel": qs.get("fuel", [""])[0],
                "car_transmission": qs.get("transmission", [""])[0],
                "car_max_mileage": qs.get("max_mileage", [""])[0],
                "sort": qs.get("sort", ["newest"])[0],
                "index": int(qs.get("index", ["0"])[0]),
            }
            print(f"\n🚗 Car Search: '{location}'")
            self.send_json(combined_car_search(location, car_params))

        elif path == "/api/autotrader":
            # ← NEW: dedicated AutoTrader endpoint (called by Base44)
            location = qs.get("location", [""])[0].strip()
            if not location:
                self.send_json({"error": "Missing location"}, 400); return
            car_params = {
                "car_make": qs.get("make", [""])[0],
                "car_model": qs.get("model", [""])[0],
                "car_min_price": qs.get("min_price", [""])[0],
                "car_max_price": qs.get("max_price", [""])[0],
                "car_year_from": qs.get("min_year", [""])[0],
                "car_year_to": qs.get("max_year", [""])[0],
                "page": int(qs.get("page", ["1"])[0]),
            }
            print(f"\n🚗 AutoTrader: '{location}'")
            results, total = autotrader_search(location, car_params)
            self.send_json({"results": results, "total": total, "source": "AutoTrader"})

        elif path == "/api/jobs":
            query = qs.get("query", [""])[0].strip()
            location = qs.get("location", ["uk"])[0].strip() or "uk"
            if not query:
                self.send_json({"error": "Missing query"}, 400); return
            job_params = {
                "job_min_salary": qs.get("min_salary", [""])[0],
                "job_max_salary": qs.get("max_salary", [""])[0],
                "job_type": qs.get("job_type", ["any"])[0],
                "job_radius": qs.get("radius", ["20"])[0],
                "sort": qs.get("sort", ["newest"])[0],
                "index": int(qs.get("index", ["0"])[0]),
            }
            print(f"\n💼 Jobs: '{query}' in '{location}'")
            self.send_json(combined_job_search(query, location, job_params))

        elif path == "/api/config":
            has_key = bool(get_scrapfly_key())
            self.send_json({"has_scrapfly_key": has_key, "zoopla_enabled": has_key})

        elif path == "/api/zoopla-status":
            self.send_json({"enabled": True, "relay_only": True, "browser_mode": False, "scrapfly_fallback": bool(get_scrapfly_key())})

        elif path == "/api/ip-info":
            try:
                client_ip = self.headers.get("X-Forwarded-For", "").split(",")[0].strip() or self.client_address[0]
                if client_ip.startswith("::ffff:"): client_ip = client_ip[7:]
                geo = {}
                if client_ip and client_ip not in ("127.0.0.1", "::1"):
                    try:
                        geo_req = urllib.request.Request(f"http://ip-api.com/json/{client_ip}?fields=status,country,countryCode,regionName,city,zip,lat,lon", headers={"User-Agent": "Crabify/1.0"})
                        with urllib.request.urlopen(geo_req, timeout=6) as r: geo = json.loads(r.read())
                    except: pass
                self.send_json({"ip": client_ip, "city": geo.get("city",""), "region": geo.get("regionName",""), "country": geo.get("country",""), "countryCode": geo.get("countryCode",""), "zip": geo.get("zip",""), "lat": geo.get("lat",0), "lon": geo.get("lon",0), "status": "ok"})
            except Exception as e:
                self.send_json({"error": str(e)}, 500)

        else:
            self.send_response(404); self.end_headers()

    def do_POST(self):
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path
        length = int(self.headers.get("Content-Length", 0))
        body_raw = self.rfile.read(length) if length else b""
        try: body = json.loads(body_raw) if body_raw else {}
        except: body = {}

        if path == "/api/zoopla-relay":
            html = body.get("html", "")
            transaction_type = body.get("transaction_type", "buy")
            if not html or len(html) < 10000:
                self.send_json({"error": "html_too_short", "listings": [], "total": 0}, 400); return
            if "just a moment" in html.lower() and len(html) < 50000:
                self.send_json({"error": "cf_blocked", "listings": [], "total": 0}, 403); return
            raw_listings = _parse_zoopla_rsc(html)
            if not raw_listings:
                self.send_json({"listings": [], "total": 0, "error": None}); return
            listings = [_map_zoopla_listing(r, transaction_type) for r in raw_listings]
            total = len(listings)
            chunks = re.findall(r'self\.__next_f\.push\(\[1,(.*?)\]\s*\)', html, re.DOTALL)
            all_text = ''
            for chunk in chunks:
                try: all_text += json.loads(chunk)
                except: all_text += chunk
            ni_match = re.search(r'"(?:numberOfItems|totalResults|total)"\s*:\s*(\d+)', all_text)
            if ni_match: total = int(ni_match.group(1))
            self.send_json({"listings": listings, "total": total, "error": None})

        elif path == "/api/config":
            new_key = body.get("scrapfly_api_key", "").strip()
            if not new_key:
                self.send_json({"error": "scrapfly_api_key is required"}, 400); return
            try:
                req_v = urllib.request.Request(f"https://api.scrapfly.io/account?key={new_key}")
                with urllib.request.urlopen(req_v, timeout=10) as r_v:
                    acct = json.loads(r_v.read())
                usage = acct.get("subscription", {}).get("usage", {}).get("scrape", {})
                plan = acct.get("subscription", {}).get("plan_name", "")
                remaining = usage.get("remaining", 0)
                cfg = _load_config(); cfg["scrapfly_api_key"] = new_key; _save_config(cfg)
                self.send_json({"success": True, "plan": plan, "remaining": remaining})
            except urllib.error.HTTPError as e:
                if e.code == 401: self.send_json({"error": "Invalid key"}, 400); return
                self.send_json({"error": f"HTTP {e.code}"}, 400)
            except Exception as e:
                self.send_json({"error": str(e)[:80]}, 400)

        elif path == "/api/email":
            user_info = body.get("user", {})
            properties = body.get("properties", [])
            if not user_info.get("name") or not user_info.get("email"):
                self.send_json({"error": "Name and email required"}, 400); return
            if not properties:
                self.send_json({"error": "No properties selected"}, 400); return
            emails = build_viewing_emails(user_info, properties)
            self.send_json({"success": True, "count": len(emails), "emails": emails})

        elif path == "/api/search":
            location = body.get("location", "").strip()
            if not location:
                self.send_json({"error": "Missing location"}, 400); return
            params = {
                "transaction_type": body.get("type", "buy"),
                "radius": str(body.get("radius", "1.0")),
                "min_beds": str(body.get("min_beds", "")),
                "max_beds": str(body.get("max_beds", "")),
                "min_price": str(body.get("min_price", "")),
                "max_price": str(body.get("max_price", "")),
                "property_type": body.get("property_type", "any"),
                "index": 0,
                "must_parking": bool(body.get("must_parking", False)),
                "must_garden": bool(body.get("must_garden", False)),
                "sort": body.get("sort", "newest"),
            }
            self.send_json(combined_search(location, params))

        else:
            self.send_response(404); self.end_headers()

# ═══════════════════════════════════════════════════════════════
# ENTRY POINT
# ═══════════════════════════════════════════════════════════════

def run(port=3000):
    server = HTTPServer(("0.0.0.0", port), CrabifyHandler)
    byparr_status = f"✅ {BYPARR_URL}" if BYPARR_URL else "⚠️  not set (add BYPARR_URL env var)"
    flare_status = f"✅ {FLARESOLVERR_URL}" if FLARESOLVERR_URL else "⚠️  not set"
    print(f"🦀 Crabify server on http://0.0.0.0:{port}")
    print(f"   Byparr:       {byparr_status}")
    print(f"   FlareSolverr: {flare_status}")
    print(f"   Property:     Rightmove ✅ OnTheMarket ✅ Zoopla ✅ Gumtree ✅ SpareRoom ✅")
    print(f"   Cars:         AutoTrader ✅ Exchange&Mart ✅ Gumtree ✅")
    print(f"   Jobs:         Reed ✅")
    server.serve_forever()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 3000))
    run(port)

