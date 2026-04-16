#!/usr/bin/env python3
"""
Crabify Property Search Backend
Scrapes real UK property listings from Rightmove, OnTheMarket and Zoopla
via HTML/Next.js data extraction. Runs all sources in parallel.

Zoopla requires Scrapfly (https://scrapfly.io) to bypass Cloudflare.
Set SCRAPFLY_API_KEY in the config file or via the app settings.
"""
import json
import os
import re
import threading
import urllib.request
import urllib.parse
import urllib.error
from http.server import HTTPServer, BaseHTTPRequestHandler

# ─────────────────────────────────────────────
# CONFIG (persistent key store)
# ─────────────────────────────────────────────

CONFIG_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.json")

def _load_config() -> dict:
    """Load config from JSON file."""
    try:
        with open(CONFIG_FILE, "r") as f:
            return json.load(f)
    except Exception:
        return {}

def _save_config(cfg: dict):
    """Save config to JSON file."""
    try:
        with open(CONFIG_FILE, "w") as f:
            json.dump(cfg, f, indent=2)
    except Exception as e:
        print(f"[Config] Could not save: {e}")

def get_scrapfly_key() -> str:
    """Return Scrapfly API key from config file or environment."""
    # Environment variable takes priority
    env_key = os.environ.get("SCRAPFLY_API_KEY", "")
    if env_key:
        return env_key
    cfg = _load_config()
    return cfg.get("scrapfly_api_key", "")

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

TYPEAHEAD_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Accept-Language": "en-GB,en;q=0.9",
    "Accept-Encoding": "identity",
    "Referer": "https://www.rightmove.co.uk/",
    "X-Requested-With": "XMLHttpRequest",
}

PROPERTY_TYPE_MAP_RM = {
    "detached": "DETACHED",
    "semi": "SEMI_DETACHED",
    "terraced": "TERRACED",
    "flat": "FLAT",
    "bungalow": "BUNGALOW",
}

# OnTheMarket property type slugs
PROPERTY_TYPE_MAP_OTM = {
    "detached": "detached-houses",
    "semi": "semi-detached-houses",
    "terraced": "terraced-houses",
    "flat": "flats",
    "bungalow": "bungalows",
}


# ─────────────────────────────────────────────
# RIGHTMOVE TYPEAHEAD
# ─────────────────────────────────────────────

def _make_typeahead_path(query: str) -> str:
    """
    Rightmove typeahead URL format: 2-character chunks separated by '/'.
    e.g. "CLAPHAM" → "CL/AP/HA/M"
    """
    q = query.strip().upper().replace(" ", "")
    chunks = [q[i:i+2] for i in range(0, len(q), 2)]
    return "/".join(chunks)


def rm_typeahead(query: str) -> tuple:
    """
    Convert a place name / postcode to Rightmove locationIdentifier.
    Returns (identifier, display_name).
    """
    def try_typeahead(path):
        url = f"https://www.rightmove.co.uk/typeAhead/uknostreet/{path}"
        try:
            req = urllib.request.Request(url, headers=TYPEAHEAD_HEADERS)
            with urllib.request.urlopen(req, timeout=8) as resp:
                data = json.loads(resp.read().decode("utf-8", errors="replace"))
                locations = data.get("typeAheadLocations", [])
                if locations:
                    loc = locations[0]
                    return loc.get("locationIdentifier", ""), loc.get("displayName", query)
        except Exception as e:
            print(f"  RM typeahead failed ({url}): {e}")
        return None, None

    q_clean = query.strip().upper()

    # Primary: full query as 2-char pairs
    path1 = _make_typeahead_path(q_clean)
    loc_id, display = try_typeahead(path1)
    if loc_id:
        return loc_id, display

    # Fallback 1: first 8 chars
    if len(q_clean) > 8:
        path2 = _make_typeahead_path(q_clean[:8])
        loc_id, display = try_typeahead(path2)
        if loc_id:
            return loc_id, display

    # Fallback 2: first 4 chars
    if len(q_clean) >= 4:
        path3 = _make_typeahead_path(q_clean[:4])
        loc_id, display = try_typeahead(path3)
        if loc_id:
            return loc_id, display

    print(f"  All RM typeahead attempts failed for: '{query}'")
    return "", query


# ─────────────────────────────────────────────
# RIGHTMOVE HTML SEARCH SCRAPER
# ─────────────────────────────────────────────

def rm_search_html(location_id: str, params: dict) -> tuple:
    """
    Fetch Rightmove search results page (HTML) and extract __NEXT_DATA__ JSON.
    Returns (list_of_properties, total_count).
    """
    channel = "BUY" if params.get("transaction_type", "buy") == "buy" else "RENT"
    endpoint = "property-for-sale" if channel == "BUY" else "property-to-rent"

    # Map frontend sort value to Rightmove sortType codes:
    # 2 = most recent, 6 = price asc, 10 = price desc, 1 = most relevant
    sort_map_rm = {
        "price_asc":  "6",
        "price_desc": "10",
        "newest":     "2",
        "beds_desc":  "2",   # RM has no beds sort, fall back to newest
    }
    sort_val = params.get("sort", "newest")
    rm_sort = sort_map_rm.get(sort_val, "2")

    query_params = {
        "locationIdentifier": location_id,
        "radius": str(params.get("radius", "1.0")),
        "sortType": rm_sort,
        "index": str(params.get("index", 0)),
    }

    if params.get("min_beds"):
        query_params["minBedrooms"] = str(params["min_beds"])
    if params.get("max_beds"):
        query_params["maxBedrooms"] = str(params["max_beds"])
    if params.get("min_price"):
        query_params["minPrice"] = str(params["min_price"])
    if params.get("max_price"):
        query_params["maxPrice"] = str(params["max_price"])
    if params.get("property_type") and params["property_type"] != "any":
        pt = PROPERTY_TYPE_MAP_RM.get(params["property_type"], "")
        if pt:
            query_params["propertyTypes"] = pt
    # Parking filter (RM supports mustHaveParking)
    if params.get("must_parking"):
        query_params["mustHaveParking"] = "true"
    # Garden filter (RM supports mustHaveGarden)
    if params.get("must_garden"):
        query_params["mustHaveGarden"] = "true"

    qs = urllib.parse.urlencode(query_params)
    url = f"https://www.rightmove.co.uk/{endpoint}/find.html?{qs}"

    print(f"  [RM] Fetching: {url}")

    try:
        req = urllib.request.Request(url, headers=BROWSER_HEADERS)
        with urllib.request.urlopen(req, timeout=20) as resp:
            html = resp.read().decode("utf-8", errors="replace")
    except Exception as e:
        print(f"  [RM] HTTP error: {e}")
        return [], 0

    match = re.search(
        r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>',
        html,
        re.DOTALL
    )
    if not match:
        print("  [RM] No __NEXT_DATA__ found")
        return [], 0

    try:
        next_data = json.loads(match.group(1))
        page_props = next_data["props"]["pageProps"]
        search_results = page_props.get("searchResults", {})
        props_raw = search_results.get("properties", [])
        result_count_str = search_results.get("resultCount", "0")
        try:
            total = int(str(result_count_str).replace(",", ""))
        except Exception:
            total = len(props_raw)
        properties = [parse_rm_property(p, channel) for p in props_raw]
        print(f"  [RM] Got {len(properties)} properties (total: {total})")
        return properties, total
    except Exception as e:
        print(f"  [RM] Parse error: {e}")
        import traceback
        traceback.print_exc()
        return [], 0


def parse_rm_property(p: dict, channel: str = "BUY") -> dict:
    """Parse a single Rightmove property dict into clean format."""
    # Price
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

    # Agent / branch
    customer = p.get("customer", {})
    agent_name = customer.get("branchDisplayName", "") or customer.get("brandTradingName", "")
    agent_phone = customer.get("contactTelephone", "") or customer.get("telephone", "")
    branch_url = customer.get("branchLandingPageUrl", "")
    if branch_url and not branch_url.startswith("http"):
        branch_url = "https://www.rightmove.co.uk" + branch_url

    # Images
    images = p.get("propertyImages", {}) or {}
    img_list = images.get("images", []) or []
    main_img = ""
    for img in img_list[:3]:
        src = img.get("srcUrl", "") or img.get("url", "")
        if src and src.startswith("http"):
            main_img = src
            break

    # Listing URL
    prop_id = p.get("id", "")
    prop_url = p.get("propertyUrl", "")
    if prop_url and not prop_url.startswith("http"):
        prop_url = "https://www.rightmove.co.uk" + prop_url
    elif prop_id and not prop_url:
        prop_url = f"https://www.rightmove.co.uk/properties/{prop_id}#/"

    # Added/reduced
    listing_update = p.get("listingUpdate", {}) or {}
    added_reduced = p.get("addedOrReduced", "") or listing_update.get("listingUpdateReason", "")

    return {
        "id": f"rm_{prop_id}",
        "address": p.get("displayAddress", "").strip(),
        "price": price,
        "price_qualifier": price_qual,
        "bedrooms": p.get("bedrooms", 0),
        "bathrooms": p.get("bathrooms", 0),
        "property_type": p.get("propertySubType", "") or p.get("propertyTypeFullDescription", ""),
        "description": p.get("summary", ""),
        "key_features": p.get("keyFeatures", []),
        "agent": agent_name,
        "agent_phone": agent_phone,
        "agent_contact_url": branch_url,
        "source": "Rightmove",
        "source_url": prop_url,
        "image": main_img,
        "added_or_reduced": added_reduced,
        "transaction_type": channel,
        "is_featured": p.get("featuredProperty", False),
        "latitude": p.get("location", {}).get("latitude", 0),
        "longitude": p.get("location", {}).get("longitude", 0),
    }


# ─────────────────────────────────────────────
# ONTHEMARKET SCRAPER
# ─────────────────────────────────────────────

def _otm_location_slug(location: str) -> str:
    """
    Convert a location string to an OTM URL slug.
    e.g. "Clapham" → "clapham"
         "SW4 9AA" → "sw4-9aa"
         "Manchester City Centre" → "manchester-city-centre"
    """
    slug = location.strip().lower()
    # Replace spaces and special chars with hyphens
    slug = re.sub(r'[^a-z0-9]+', '-', slug)
    slug = slug.strip('-')
    return slug


def otm_search(location: str, params: dict) -> tuple:
    """
    Scrape OnTheMarket search results via HTML + Redux __NEXT_DATA__.
    Returns (list_of_properties, total_count).
    """
    channel = params.get("transaction_type", "buy")
    endpoint = "for-sale" if channel == "buy" else "to-rent"

    # Build property type path segment
    prop_type = params.get("property_type", "any")
    if prop_type and prop_type != "any":
        type_slug = PROPERTY_TYPE_MAP_OTM.get(prop_type, "property")
    else:
        type_slug = "property"

    # Build location slug
    loc_slug = _otm_location_slug(location)

    # Build query params
    # Map frontend sort to OTM sort param
    sort_map_otm = {
        "price_asc":  "price-asc",
        "price_desc": "price-desc",
        "newest":     "most-recent",
        "beds_desc":  "most-recent",  # OTM has no beds sort
    }
    sort_val = params.get("sort", "newest")
    otm_sort = sort_map_otm.get(sort_val, "most-recent")
    qp = {"sort": otm_sort}
    if params.get("min_beds"):
        try:
            qp["min-bedrooms"] = str(int(params["min_beds"]))
        except Exception:
            pass
    if params.get("max_beds"):
        try:
            qp["max-bedrooms"] = str(int(params["max_beds"]))
        except Exception:
            pass
    if params.get("min_price"):
        try:
            qp["min-price"] = str(int(params["min_price"]))
        except Exception:
            pass
    if params.get("max_price"):
        try:
            qp["max-price"] = str(int(params["max_price"]))
        except Exception:
            pass
    # Pagination (OTM uses 'page' param, 1-indexed; we use 25-item strides)
    index = int(params.get("index", 0))
    if index > 0:
        page_num = (index // 25) + 1
        qp["page"] = str(page_num)

    # OTM doesn't have a parking/garden URL filter but we note it
    qs = urllib.parse.urlencode(qp)
    url = f"https://www.onthemarket.com/{endpoint}/{type_slug}/{loc_slug}/"
    if qs:
        url += f"?{qs}"

    print(f"  [OTM] Fetching: {url}")

    try:
        req = urllib.request.Request(url, headers=BROWSER_HEADERS)
        with urllib.request.urlopen(req, timeout=20) as resp:
            html = resp.read().decode("utf-8", errors="replace")
    except Exception as e:
        print(f"  [OTM] HTTP error: {e}")
        return [], 0

    match = re.search(
        r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>',
        html,
        re.DOTALL
    )
    if not match:
        print("  [OTM] No __NEXT_DATA__ found in response")
        return [], 0

    try:
        next_data = json.loads(match.group(1))
        redux = next_data["props"]["initialReduxState"]
        results_state = redux.get("results", {})

        listings_raw = results_state.get("list", [])
        total = results_state.get("totalResults", 0)
        if isinstance(total, str):
            total = int(total.replace(",", ""))

        # OTM returns 1000 as cap when there are more; use paginationControls for real total
        pagination = results_state.get("paginationControls", {})
        if pagination.get("total"):
            try:
                total = int(pagination["total"])
            except Exception:
                pass

        channel_upper = "BUY" if channel == "buy" else "RENT"
        properties = [parse_otm_property(p, channel_upper) for p in listings_raw]
        print(f"  [OTM] Got {len(properties)} properties (total: {total})")
        return properties, total

    except Exception as e:
        print(f"  [OTM] Parse error: {e}")
        import traceback
        traceback.print_exc()
        return [], 0


def parse_otm_property(p: dict, channel: str = "BUY") -> dict:
    """Parse a single OnTheMarket property dict into clean format."""
    prop_id = str(p.get("id", ""))

    # Price (OTM gives formatted price like "£339,950")
    price = p.get("price", "")
    if not price:
        price = p.get("short-price", "")

    # Agent info
    agent = p.get("agent", {}) or {}
    agent_name = agent.get("name", "")
    agent_phone = agent.get("telephone", "")
    agent_contact_url = agent.get("contact-url", "")
    if agent_contact_url and not agent_contact_url.startswith("http"):
        agent_contact_url = "https://www.onthemarket.com" + agent_contact_url

    # Images
    cover = p.get("cover-image", {}) or {}
    main_img = cover.get("default", "") or cover.get("webp", "")
    if not main_img:
        images = p.get("images", []) or []
        if images:
            first_img = images[0] or {}
            main_img = first_img.get("default", "") or first_img.get("webp", "")

    # Source URL
    details_url = p.get("details-url", "")
    if details_url and not details_url.startswith("http"):
        prop_url = "https://www.onthemarket.com" + details_url
    else:
        prop_url = details_url or f"https://www.onthemarket.com/details/{prop_id}/"

    # Property type
    prop_type = p.get("humanised-property-type", "")

    # Features (key features like "Parking", "Garden" etc.)
    features = p.get("features", []) or []

    # Description from title
    description = p.get("property-title", "")

    # Added/reduced
    added_reduced = p.get("days-since-added-reduced", "")

    # Location coords
    loc = p.get("location", {}) or {}

    return {
        "id": f"otm_{prop_id}",
        "address": p.get("address", "").strip(),
        "price": price,
        "price_qualifier": p.get("price-qualifier", "") or "",
        "bedrooms": p.get("bedrooms", 0) or 0,
        "bathrooms": p.get("bathrooms", 0) or 0,
        "property_type": prop_type,
        "description": description,
        "key_features": features,
        "agent": agent_name,
        "agent_phone": agent_phone,
        "agent_contact_url": agent_contact_url,
        "source": "OnTheMarket",
        "source_url": prop_url,
        "image": main_img,
        "added_or_reduced": added_reduced,
        "transaction_type": channel,
        "is_featured": p.get("spotlight?", False),
        "latitude": loc.get("lat", 0),
        "longitude": loc.get("lon", 0),
    }


# ─────────────────────────────────────────────
# ZOOPLA SCRAPER (via Scrapfly)
# ─────────────────────────────────────────────

ZOOPLA_PROPERTY_TYPE_MAP = {
    "detached":  "detached-houses",
    "semi":      "semi-detached-houses",
    "terraced":  "terraced-houses",
    "flat":      "flats",
    "bungalow":  "bungalows",
}


def _zoopla_location_slug(location: str) -> str:
    """
    Convert a location name or postcode to a Zoopla URL slug.
    e.g. 'Clapham' → 'clapham', 'SW4 9AA' → 'sw4', 'Greater London' → 'greater-london'
    """
    loc = location.strip().lower()
    # If it looks like a postcode (e.g. SW4, SW11, SW4 9AA, EC1A 1BB)
    postcode_re = re.compile(r'^[a-z]{1,2}\d[a-z\d]?(\s*\d[a-z]{2})?$')
    if postcode_re.match(loc.replace(' ', '')):
        district = loc.split()[0]
        return district
    # General area name
    slug = re.sub(r'[^a-z0-9\s\-]', '', loc)
    slug = re.sub(r'\s+', '-', slug.strip())
    return slug


def _parse_zoopla_rsc(content: str) -> list:
    """
    Parse Zoopla's Next.js RSC payload to extract regularListingsFormatted.
    Returns a list of raw listing dicts.
    """
    # Collect type-1 RSC chunks (JSON-encoded strings)
    chunks = re.findall(r'self\.__next_f\.push\(\[1,(.*?)\]\s*\)', content, re.DOTALL)
    all_text = ''
    for chunk in chunks:
        try:
            all_text += json.loads(chunk)  # decode JSON string
        except Exception:
            all_text += chunk

    idx = all_text.find('regularListingsFormatted')
    if idx < 0:
        return []

    arr_start = all_text.find('[', idx)
    if arr_start < 0:
        return []

    # Walk brackets to find array end
    depth = 0
    arr_end = arr_start
    for i, ch in enumerate(all_text[arr_start:arr_start + 400000]):
        if ch in '[{':
            depth += 1
        elif ch in ']}':
            depth -= 1
            if depth == 0:
                arr_end = arr_start + i + 1
                break

    try:
        return json.loads(all_text[arr_start:arr_end])
    except Exception:
        return []


def _map_zoopla_listing(raw: dict, transaction_type: str) -> dict:
    """Map a Zoopla raw listing dict to Crabify's standard property format."""
    listing_id = raw.get("listingId", "")
    address = raw.get("address", "")
    price_raw = raw.get("price", "")
    # price may be a string like "£450,000" or a dict
    if isinstance(price_raw, dict):
        price = price_raw.get("value", "") or price_raw.get("displayPrice", "")
    else:
        price = str(price_raw)

    # Bedrooms: in 'title' like "3 bed flat" or 'features'
    bedrooms = 0
    title = raw.get("title", "")
    bed_match = re.search(r'(\d+)\s*bed', title, re.IGNORECASE)
    if bed_match:
        bedrooms = int(bed_match.group(1))
    if not bedrooms:
        bedrooms = int(raw.get("numBedrooms", 0) or 0)

    # Property type from title
    prop_type = "property"
    for pt in ["flat", "detached", "semi-detached", "terraced", "bungalow", "house", "studio"]:
        if pt in title.lower():
            prop_type = pt
            break

    branch = raw.get("branch", {}) or {}
    agent_name = branch.get("name", "")
    agent_phone = branch.get("phone", "")

    listing_uris = raw.get("listingUris", {}) or {}
    detail_path = listing_uris.get("detail", "")
    source_url = f"https://www.zoopla.co.uk{detail_path}" if detail_path else "https://www.zoopla.co.uk"

    image_obj = raw.get("image", {}) or {}
    main_img = image_obj.get("src", "") or image_obj.get("url", "")

    description = raw.get("summaryDescription", "") or ""
    features = raw.get("features", []) or []
    if isinstance(features, list):
        features = [f.get("content", "") if isinstance(f, dict) else str(f) for f in features]

    return {
        "id": f"zoopla_{listing_id}",
        "address": address,
        "price": price,
        "price_qualifier": "",
        "bedrooms": bedrooms,
        "bathrooms": 0,
        "property_type": prop_type,
        "description": description,
        "key_features": features,
        "agent": agent_name,
        "agent_phone": agent_phone,
        "agent_contact_url": source_url,
        "source": "Zoopla",
        "source_url": source_url,
        "image": main_img,
        "added_or_reduced": raw.get("publishedOnLabel", ""),
        "transaction_type": transaction_type,
        "is_featured": raw.get("isPremium", False),
        "latitude": 0,
        "longitude": 0,
    }


def zoopla_search(location: str, params: dict) -> tuple:
    """
    Search Zoopla via Scrapfly.
    Returns (results_list, total_count, error_string_or_None).
    """
    api_key = get_scrapfly_key()
    if not api_key:
        return [], 0, "no_key"

    transaction_type = params.get("transaction_type", "buy")
    channel = "for-sale" if transaction_type == "buy" else "to-rent"
    slug = _zoopla_location_slug(location)

    # Build Zoopla search URL
    zoopla_params = {}
    min_beds = params.get("min_beds", "")
    max_beds = params.get("max_beds", "")
    min_price = params.get("min_price", "")
    max_price = params.get("max_price", "")
    prop_type = params.get("property_type", "any")

    if min_beds:
        zoopla_params["beds_min"] = min_beds
    if max_beds:
        zoopla_params["beds_max"] = max_beds
    if min_price:
        zoopla_params["price_min"] = min_price
    if max_price:
        zoopla_params["price_max"] = max_price

    # Property type sub-path
    prop_subpath = "property"
    if prop_type and prop_type != "any":
        prop_subpath = ZOOPLA_PROPERTY_TYPE_MAP.get(prop_type, "property")

    page = (params.get("index", 0) // 25) + 1
    if page > 1:
        zoopla_params["pn"] = page

    zoopla_url = f"https://www.zoopla.co.uk/{channel}/{prop_subpath}/{slug}/"
    if zoopla_params:
        zoopla_url += "?" + urllib.parse.urlencode(zoopla_params)

    print(f"  [Zoopla] Fetching: {zoopla_url}")

    # Call Scrapfly
    scrapfly_params = urllib.parse.urlencode({
        "key": api_key,
        "url": zoopla_url,
        "asp": "true",
        "render_js": "false",
        "country": "gb",
        "proxy_pool": "public_residential_pool",
    })
    scrapfly_url = f"https://api.scrapfly.io/scrape?{scrapfly_params}"

    try:
        req = urllib.request.Request(scrapfly_url)
        req.add_header("Accept", "application/json")
        with urllib.request.urlopen(req, timeout=30) as r:
            resp = json.loads(r.read())
    except urllib.error.HTTPError as e:
        if e.code == 401:
            return [], 0, "invalid_key"
        if e.code == 429:
            return [], 0, "quota_exceeded"
        return [], 0, f"scrapfly_http_{e.code}"
    except Exception as e:
        return [], 0, f"scrapfly_error: {str(e)[:80]}"

    result = resp.get("result", {})
    status_code = result.get("status_code", 0)
    content = result.get("content", "")

    if status_code != 200:
        return [], 0, f"zoopla_http_{status_code}"

    raw_listings = _parse_zoopla_rsc(content)
    if not raw_listings:
        # Check if quota reached in response body
        if "quota" in content.lower() or "upgrade" in content.lower():
            return [], 0, "quota_exceeded"
        return [], 0, None  # No results for this area (not an error)

    listings = [_map_zoopla_listing(r, transaction_type) for r in raw_listings]

    # Apply parking / garden filters
    must_parking = params.get("must_parking", False)
    must_garden = params.get("must_garden", False)
    if must_parking:
        listings = [p for p in listings if any(
            'park' in str(f).lower() for f in (p.get("key_features") or []) + [p.get("description", "")]
        )]
    if must_garden:
        listings = [p for p in listings if any(
            'garden' in str(f).lower() for f in (p.get("key_features") or []) + [p.get("description", "")]
        )]

    # Total count from RSC meta
    total = len(listings)
    chunks = re.findall(r'self\.__next_f\.push\(\[1,(.*?)\]\s*\)', content, re.DOTALL)
    all_text = ''
    for chunk in chunks:
        try:
            all_text += json.loads(chunk)
        except Exception:
            all_text += chunk
    ni_match = re.search(r'"numberOfItems":(\d+)', all_text)
    if ni_match:
        total = int(ni_match.group(1))

    print(f"  [Zoopla] Found {len(listings)} listings (total: {total})")
    return listings, total, None


# ─────────────────────────────────────────────
# GUMTREE SCRAPER  (buy + rent)
# ─────────────────────────────────────────────

GUMTREE_SORT_MAP = {
    "price_asc":  "price_asc",
    "price_desc": "price_desc",
    "newest":     "date",
    "beds_desc":  "date",
}

def gumtree_search(location: str, params: dict) -> tuple:
    """
    Scrape Gumtree property listings via HTML.
    Returns (list_of_properties, total_count).
    """
    channel = params.get("transaction_type", "buy")
    slug = location.strip().lower()
    slug = re.sub(r"[^a-z0-9]+", "-", slug).strip("-") or "london"

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

    sort_val = params.get("sort", "newest")
    qp["sort"] = GUMTREE_SORT_MAP.get(sort_val, "date")

    # Pagination: Gumtree uses page param
    index = int(params.get("index", 0))
    if index > 0:
        qp["page"] = str((index // 25) + 1)

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

    # Extract listing anchors — each is: data-q="search-result-anchor" href="/p/..."
    listing_blocks = re.findall(
        r'(data-q="search-result-anchor"\s+href="(/p/[^"]+)".*?)(?=data-q="search-result-anchor"|</main>|<footer)',
        html, re.DOTALL
    )

    # Total count from heading e.g. "1,234 ads"
    total_m = re.search(r'([\d,]+)\s+(?:ads?|result|listing)', html, re.I)
    total = 0
    if total_m:
        try: total = int(total_m.group(1).replace(",", ""))
        except: pass

    properties = []
    for block_text, href in listing_blocks:
        prop = _parse_gumtree_listing(block_text, href, channel)
        if prop:
            properties.append(prop)

    # Fallback total
    if not total:
        total = len(properties)

    print(f"  [Gumtree] Got {len(properties)} listings (total: {total})")
    return properties, total


def _parse_gumtree_listing(block: str, href: str, channel: str) -> dict:
    """Parse a single Gumtree listing block into clean format."""

    # Strip inline <style> blocks first to clean up parsing
    clean_block = re.sub(r'<style[^>]*>.*?</style>', '', block, flags=re.DOTALL)

    # Price — handles £525,000 or £1,200 pcm
    price_m = re.search(r'£([\d,]+)\s*(pcm|pw|per\s+month|per\s+week)?', clean_block, re.I)
    price = ""
    if price_m:
        amount = price_m.group(1)
        freq = price_m.group(2) or ""
        price = f"£{amount}"
        if freq:
            price += f" {freq.lower()}"

    # Listing ID from URL — last numeric segment
    parts = href.rstrip("/").split("/")
    listing_id = parts[-1] if parts[-1].isdigit() else re.sub(r"[^0-9]", "", parts[-1]) or parts[-1]

    # Title — from data-q="tile-title" div
    title_m = re.search(r'data-q="tile-title"[^>]*>(.*?)</div>', clean_block, re.DOTALL)
    if title_m:
        address = re.sub(r'<[^>]+>', '', title_m.group(1)).strip()
        address = re.sub(r'&amp;', '&', address)
        address = re.sub(r'&[a-z]+;', ' ', address).strip()[:120]
    else:
        # Fallback: derive from URL slug
        slug_idx = -2 if (parts and parts[-1].isdigit()) else -1
        slug_part = parts[slug_idx] if len(parts) > abs(slug_idx) else ""
        address = slug_part.replace("-", " ").title()[:80]

    # Bedrooms
    beds_m = re.search(r'(\d+)\s*(?:bed(?:room)?s?)', clean_block, re.I)
    beds = int(beds_m.group(1)) if beds_m else 0

    # Property type
    type_m = re.search(r'\b(flat|house|apartment|studio|bungalow|maisonette|terraced|detached|semi-detached)\b', clean_block, re.I)
    prop_type = type_m.group(1).title() if type_m else ""

    # Image — Gumtree CDN URLs don't have file extensions, match img.gumtree.com
    img_m = re.search(r'src="(https://img\.gumtree\.com[^"]*)"', clean_block, re.I)
    image = img_m.group(1) if img_m else ""

    prop_url = f"https://www.gumtree.com{href}"

    # Description — extract meaningful text from tile-description, fallback to all text
    desc_m = re.search(r'data-q="tile-description"[^>]*>(.*?)(?=data-q="|</div></div>)', clean_block, re.DOTALL)
    if desc_m:
        desc_text = re.sub(r'<[^>]+>', ' ', desc_m.group(1))
    else:
        desc_text = re.sub(r'<[^>]+>', ' ', clean_block)
    desc_text = re.sub(r'&amp;', '&', desc_text)
    desc_text = re.sub(r'&[a-z]+;', ' ', desc_text)
    desc_text = re.sub(r'\s+', ' ', desc_text).strip()
    # Build a clean description: type + beds + price info
    parts_desc = []
    if prop_type: parts_desc.append(prop_type)
    if beds: parts_desc.append(f"{beds} bed{'s' if beds > 1 else ''}")
    if price: parts_desc.append(price)
    # Also grab any sentence-like content
    sentences = re.findall(r'[A-Z][^.!?]{20,120}[.!?]', desc_text)
    if sentences:
        parts_desc.append(sentences[0])
    description = " · ".join(parts_desc) if parts_desc else desc_text[:200]

    if not price and not address:
        return None

    return {
        "id": f"gumtree_{listing_id}",
        "address": address,
        "price": price,
        "price_qualifier": "",
        "bedrooms": beds,
        "bathrooms": 0,
        "property_type": prop_type,
        "description": description,
        "key_features": [],
        "agent": "Gumtree / Private Seller",
        "agent_phone": "",
        "agent_contact_url": prop_url,
        "source": "Gumtree",
        "source_url": prop_url,
        "image": image,
        "added_or_reduced": "",
        "transaction_type": channel,
        "is_featured": False,
        "latitude": 0,
        "longitude": 0,
    }


# ─────────────────────────────────────────────
# SPAREROOM SCRAPER  (rent only — rooms & flats)
# ─────────────────────────────────────────────

def spareroom_search(location: str, params: dict) -> tuple:
    """
    Scrape SpareRoom for room / flat rentals.
    Only relevant when transaction_type == 'rent'.
    Returns (list_of_properties, total_count).
    """
    channel = params.get("transaction_type", "buy")
    if channel != "rent":
        return [], 0   # SpareRoom is rentals only

    slug = location.strip().lower()
    slug = re.sub(r"[^a-z0-9]+", "-", slug).strip("-") or "london"

    qp = {}
    if params.get("min_beds"):
        try: qp["min_rooms"] = str(int(params["min_beds"]))
        except: pass
    if params.get("max_price"):
        try: qp["max_monthly_price"] = str(int(params["max_price"]))
        except: pass
    if params.get("min_price"):
        try: qp["min_monthly_price"] = str(int(params["min_price"]))
        except: pass
    qp["per"] = "pcm"

    # Pagination
    index = int(params.get("index", 0))
    if index > 0:
        qp["offset"] = str(index)

    qs = urllib.parse.urlencode(qp)
    url = f"https://www.spareroom.co.uk/flatshare/{slug}?{qs}"
    print(f"  [SpareRoom] Fetching: {url}")

    try:
        req = urllib.request.Request(url, headers=BROWSER_HEADERS)
        with urllib.request.urlopen(req, timeout=15) as r:
            html = r.read().decode("utf-8", errors="replace")
    except Exception as e:
        print(f"  [SpareRoom] HTTP error: {e}")
        return [], 0

    # Total results
    total_m = re.search(r'([\d,]+)\s+(?:rooms?|results?|ads?)\s+found', html, re.I)
    total = 0
    if total_m:
        try: total = int(total_m.group(1).replace(",", ""))
        except: pass

    # Extract article listings
    articles = re.findall(
        r'<article[^>]*class="[^"]*listing[^"]*"[^>]*>(.*?)</article>',
        html, re.DOTALL
    )

    properties = []
    for art in articles:
        prop = _parse_spareroom_listing(art)
        if prop:
            properties.append(prop)

    if not total:
        total = len(properties)

    print(f"  [SpareRoom] Got {len(properties)} listings (total: {total})")
    return properties, total


def _parse_spareroom_listing(article: str) -> dict:
    """Parse a single SpareRoom article block."""
    # Title from h2
    h2_m = re.search(r'<h2[^>]*>(.*?)</h2>', article, re.DOTALL)
    title = re.sub(r'<[^>]+>', '', h2_m.group(1)).strip() if h2_m else ""

    # Price — SpareRoom stores as &pound;1,200 pcm or £1,200 pcm
    price_m = re.search(r'(?:£|&pound;)([\d,]+)\s*(pcm|pw|per\s*month|per\s*week)?', article, re.I)
    price = ""
    if price_m:
        amount = price_m.group(1)
        freq = (price_m.group(2) or "pcm").lower().strip()
        price = f"£{amount} {freq}"

    # Beds/rooms
    rooms_m = re.search(r'(\d+)\s*(?:room|bedroom|bed)', article, re.I)
    beds = int(rooms_m.group(1)) if rooms_m else 1

    # Location
    loc_m = re.search(r'<strong[^>]*>([^<]{3,50})</strong>', article)
    address = re.sub(r'<[^>]+>', '', loc_m.group(1)).strip() if loc_m else title

    # Link
    link_m = re.search(r'href="(/flatshare/\d+[^"]*)"', article)
    prop_url = f"https://www.spareroom.co.uk{link_m.group(1)}" if link_m else "https://www.spareroom.co.uk"

    # ID
    id_m = re.search(r'/flatshare/(\d+)', prop_url)
    listing_id = id_m.group(1) if id_m else prop_url.split("/")[-1]

    # Image
    img_m = re.search(r'<img[^>]+src="(https://[^"]+(?:jpg|jpeg|png|webp)[^"]*)"', article, re.I)
    image = img_m.group(1) if img_m else ""

    # Description
    clean = re.sub(r'<[^>]+>', ' ', article)
    clean = re.sub(r'&[a-z]+;', ' ', clean)
    clean = re.sub(r'\s+', ' ', clean).strip()
    description = clean[:200]

    if not title and not price:
        return None

    return {
        "id": f"spareroom_{listing_id}",
        "address": address or title,
        "price": price,
        "price_qualifier": "pcm",
        "bedrooms": beds,
        "bathrooms": 0,
        "property_type": "Flat / Room",
        "description": description,
        "key_features": [],
        "agent": "SpareRoom",
        "agent_phone": "",
        "agent_contact_url": prop_url,
        "source": "SpareRoom",
        "source_url": prop_url,
        "image": image,
        "added_or_reduced": "",
        "transaction_type": "rent",
        "is_featured": False,
        "latitude": 0,
        "longitude": 0,
    }


# ─────────────────────────────────────────────
# COMBINED SEARCH (runs RM + OTM in parallel)
# ─────────────────────────────────────────────

def combined_search(location: str, params: dict) -> dict:
    """
    Run Rightmove, OnTheMarket, Zoopla, Gumtree and SpareRoom searches in parallel threads.
    Returns merged, deduplicated results.
    """
    rm_results = []
    rm_total = 0
    otm_results = []
    otm_total = 0
    zp_results = []
    zp_total = 0
    gt_results = []
    gt_total = 0
    sr_results = []
    sr_total = 0
    rm_error = None
    otm_error = None
    zp_error = None
    gt_error = None
    sr_error = None

    # ── Rightmove thread ──────────────────────
    def fetch_rm():
        nonlocal rm_results, rm_total, rm_error
        try:
            loc_id, display_name = rm_typeahead(location)
            if loc_id:
                rm_results, rm_total = rm_search_html(loc_id, params)
            else:
                rm_error = f"Could not resolve '{location}' on Rightmove"
        except Exception as e:
            rm_error = str(e)
            print(f"  [RM] Thread error: {e}")

    # ── OnTheMarket thread ────────────────────
    def fetch_otm():
        nonlocal otm_results, otm_total, otm_error
        try:
            otm_results, otm_total = otm_search(location, params)
        except Exception as e:
            otm_error = str(e)
            print(f"  [OTM] Thread error: {e}")

    # ── Zoopla thread ─────────────────────────
    def fetch_zoopla():
        nonlocal zp_results, zp_total, zp_error
        try:
            zp_results, zp_total, zp_error = zoopla_search(location, params)
        except Exception as e:
            zp_error = str(e)
            print(f"  [Zoopla] Thread error: {e}")

    # ── Gumtree thread ────────────────────────
    def fetch_gumtree():
        nonlocal gt_results, gt_total, gt_error
        try:
            gt_results, gt_total = gumtree_search(location, params)
        except Exception as e:
            gt_error = str(e)
            print(f"  [Gumtree] Thread error: {e}")

    # ── SpareRoom thread ──────────────────────
    def fetch_spareroom():
        nonlocal sr_results, sr_total, sr_error
        try:
            sr_results, sr_total = spareroom_search(location, params)
        except Exception as e:
            sr_error = str(e)
            print(f"  [SpareRoom] Thread error: {e}")

    t_rm  = threading.Thread(target=fetch_rm,        daemon=True)
    t_otm = threading.Thread(target=fetch_otm,       daemon=True)
    t_zp  = threading.Thread(target=fetch_zoopla,    daemon=True)
    t_gt  = threading.Thread(target=fetch_gumtree,   daemon=True)
    t_sr  = threading.Thread(target=fetch_spareroom, daemon=True)

    t_rm.start(); t_otm.start(); t_zp.start(); t_gt.start(); t_sr.start()
    t_rm.join(timeout=25)
    t_otm.join(timeout=25)
    t_zp.join(timeout=35)
    t_gt.join(timeout=20)
    t_sr.join(timeout=20)

    # Merge: combine all results then sort by user preference
    all_results = rm_results + otm_results + zp_results + gt_results + sr_results

    sort_val = params.get("sort", "newest")

    def _price_num(p):
        """Extract numeric price from a property dict. Returns 0 if unparseable."""
        price_str = p.get("price", "") or ""
        digits = re.sub(r"[^0-9]", "", price_str)
        return int(digits) if digits else 0

    def _is_rental_price(p):
        """True if this listing has a pcm/pw rental price (different scale to purchase prices)."""
        s = (p.get("price", "") or "").lower()
        return "pcm" in s or " pw" in s or "per week" in s or "per month" in s

    if sort_val == "price_asc":
        # Rental prices (pcm) use a different scale — sort them after purchase prices
        # Items with no price sink to the bottom
        def _asc_key(p):
            n = _price_num(p)
            if n == 0:
                return 999_999_999  # no price → bottom
            if _is_rental_price(p):
                return 900_000_000 + n  # rentals after all purchase prices
            return n
        all_results.sort(key=_asc_key)
    elif sort_val == "price_desc":
        # Items with no price sink to the bottom (0 treated as lowest)
        # Rental prices after purchase prices in descending too (they're incomparable)
        all_results.sort(key=lambda p: _price_num(p), reverse=True)
    elif sort_val == "beds_desc":
        all_results.sort(key=lambda p: -(p.get("bedrooms") or 0))
    else:
        # "newest" – interleave all sources to show variety
        sources_list = [s for s in [rm_results, otm_results, zp_results, gt_results, sr_results] if s]
        interleaved = []
        max_len = max((len(s) for s in sources_list), default=0)
        for i in range(max_len):
            for s in sources_list:
                if i < len(s):
                    interleaved.append(s[i])
        all_results = interleaved

    # Build sources list
    sources = []
    source_totals = {}

    if rm_results or rm_total:
        sources.append("Rightmove")
        source_totals["Rightmove"] = rm_total
    elif rm_error:
        sources.append("Rightmove (error)")
        source_totals["Rightmove"] = 0

    if otm_results or otm_total:
        sources.append("OnTheMarket")
        source_totals["OnTheMarket"] = otm_total
    elif otm_error:
        sources.append("OnTheMarket (error)")
        source_totals["OnTheMarket"] = 0

    if gt_results or gt_total:
        sources.append("Gumtree")
        source_totals["Gumtree"] = gt_total
    elif gt_error:
        sources.append("Gumtree (error)")
        source_totals["Gumtree"] = 0

    if sr_results or sr_total:
        sources.append("SpareRoom")
        source_totals["SpareRoom"] = sr_total
    elif sr_error:
        sources.append("SpareRoom (error)")
        source_totals["SpareRoom"] = 0

    # Zoopla status
    if zp_error == "no_key":
        sources.append("Zoopla (no API key)")
        source_totals["Zoopla"] = 0
    elif zp_error == "invalid_key":
        sources.append("Zoopla (invalid key)")
        source_totals["Zoopla"] = 0
    elif zp_error == "quota_exceeded":
        sources.append("Zoopla (quota exceeded)")
        source_totals["Zoopla"] = 0
    elif zp_error:
        sources.append("Zoopla (error)")
        source_totals["Zoopla"] = 0
    elif zp_results or zp_total:
        sources.append("Zoopla")
        source_totals["Zoopla"] = zp_total
    else:
        source_totals["Zoopla"] = 0

    return {
        "results": all_results,
        "total": rm_total + otm_total + zp_total + gt_total + sr_total,
        "shown": len(all_results),
        "location": location,
        "sources": sources,
        "source_totals": source_totals,
        "zoopla_status": zp_error or ("ok" if (zp_results or zp_total) else "no_results"),
        "errors": {
            k: v for k, v in {
                "Rightmove": rm_error,
                "OnTheMarket": otm_error,
                "Zoopla": zp_error,
                "Gumtree": gt_error,
                "SpareRoom": sr_error,
            }.items() if v
        }
    }


# ─────────────────────────────────────────────
# EMAIL HELPER
# ─────────────────────────────────────────────

def build_viewing_emails(user_info: dict, properties: list) -> dict:
    """Build viewing request email data for each selected property."""
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

        body = (
            f"Dear {agent} Team,\n\n"
            f"I am writing to request a viewing for the following property:\n\n"
            f"Address: {address}\n"
            f"Asking Price: {price}\n"
            f"Listed on: {source}\n"
            f"Listing URL: {prop_url}\n\n"
            f"My details:\n"
            f"Name: {name}\n"
            f"Email: {email}\n"
            f"Phone: {phone}\n"
        )
        if message:
            body += f"\nAdditional notes:\n{message}\n"
        body += f"\nI look forward to hearing from you.\n\nKind regards,\n{name}"

        subject = f"Viewing Request — {address}"
        mailto = (
            f"mailto:?subject={urllib.parse.quote(subject)}"
            f"&body={urllib.parse.quote(body)}"
        )

        results[prop_id] = {
            "agent": agent,
            "agent_phone": prop.get("agent_phone", ""),
            "address": address,
            "price": price,
            "source": source,
            "subject": subject,
            "email_body": body,
            "mailto": mailto,
            "status": "ready",
        }
    return results


# ═══════════════════════════════════════════════════════════════
# CARS SCRAPERS
# ═══════════════════════════════════════════════════════════════

def _parse_price_int(price_str: str) -> int:
    """Convert '£15,998' to 15998; returns 0 on failure."""
    try:
        return int(re.sub(r'[^\d]', '', price_str))
    except Exception:
        return 0


def exchangeandmart_search(location: str, params: dict) -> tuple:
    """
    Scrape Exchange & Mart used-car listings.
    Returns (list_of_cars, total_count).
    URL pattern: /used-cars-for-sale?location={loc}&price-to={max}&price-from={min}&year-from={yr}&make={make}&model={model}&page={p}
    """
    qp = {"location": location}
    if params.get("car_min_price"):
        qp["price-from"] = params["car_min_price"]
    if params.get("car_max_price"):
        qp["price-to"] = params["car_max_price"]
    if params.get("car_year_from"):
        qp["year-from"] = params["car_year_from"]
    if params.get("car_make") and params["car_make"].lower() not in ("", "any"):
        qp["make"] = params["car_make"]
    if params.get("car_model") and params["car_model"].lower() not in ("", "any"):
        qp["model"] = params["car_model"]
    if params.get("car_fuel") and params["car_fuel"].lower() not in ("", "any"):
        qp["fuel-type"] = params["car_fuel"]
    if params.get("car_transmission") and params["car_transmission"].lower() not in ("", "any"):
        qp["transmission"] = params["car_transmission"]

    index = int(params.get("index", 0))
    page = index // 15 + 1
    if page > 1:
        qp["page"] = str(page)

    url = "https://www.exchangeandmart.co.uk/used-cars-for-sale?" + urllib.parse.urlencode(qp)
    print(f"  [E&M] Fetching: {url}")

    try:
        req = urllib.request.Request(url, headers=BROWSER_HEADERS)
        with urllib.request.urlopen(req, timeout=20) as resp:
            if resp.status not in (200, 301, 302):
                raise Exception(f"HTTP {resp.status}")
            html = resp.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as e:
        print(f"  [E&M] HTTP error: {e.code}")
        return [], 0
    except Exception as e:
        print(f"  [E&M] Error: {e}")
        return [], 0

    # Parse total count — E&M doesn't show explicit total so count ads on page * estimate
    total_match = re.search(r'(\d[\d,]*)\s+(?:used\s+)?(?:cars?|vehicles?|results?)\s+found', html, re.IGNORECASE)
    if not total_match:
        total_match = re.search(r'<span[^>]*class="[^"]*count[^"]*"[^>]*>([\d,]+)', html, re.IGNORECASE)
    if total_match:
        total = int(re.sub(r'[^\d]', '', total_match.group(1)))
    else:
        # Count ad IDs as proxy for page size; assume many more pages available
        ad_count = len(re.findall(r'adid="\d+"', html))
        total = ad_count * 20  # rough estimate

    # Parse listings — extract each <div class="result-item"...> chunk
    # Split HTML on result-item divs
    parts = re.split(r'(?=<div class="result-item")', html)
    cars = []
    for item in parts[1:16]:  # Skip preamble, limit to 15
        try:
            # Make/model from div attributes
            make = re.search(r'\bmake="([^"]+)"', item)
            model = re.search(r'\bmodel="([^"]+)"', item)
            make = make.group(1).strip() if make else ""
            model = model.group(1).strip() if model else ""

            # Variant
            variant_m = re.search(r'class="result-item__variant"[^>]*>([^<]+)<', item)
            variant = variant_m.group(1).strip() if variant_m else ""
            title = f"{make} {model} {variant}".strip()

            # Price
            price_m = re.search(r'class="price price--primary"[^>]*>(£[\d,]+)', item)
            if not price_m:
                price_m = re.search(r'class="price[^"]*"[^>]*>(£[\d,]+)', item)
            price = price_m.group(1).strip() if price_m else ""

            # URL
            href_m = re.search(r'href="(/ad/\d+)"', item)
            url_suffix = href_m.group(1) if href_m else ""
            listing_url = f"https://www.exchangeandmart.co.uk{url_suffix}" if url_suffix else ""

            # Key details (year, transmission, fuel, mileage)
            kd = re.findall(r'class="key-details__item">([^<]+)<', item)
            year = next((k.strip() for k in kd if re.match(r'^\s*\d{4}\s*$', k)), "")
            mileage_raw = next((k for k in kd if "mileage" in k.lower() or "mile" in k.lower()), "")
            mileage = re.sub(r'(?i)mileage:\s*', '', mileage_raw).strip()
            fuel = next((k.strip() for k in kd if k.strip().lower() in ("petrol","diesel","electric","hybrid","plug-in hybrid")), "")
            transmission = next((k.strip() for k in kd if k.strip().lower() in ("manual","automatic","semi-auto","semi-automatic")), "")

            # Image
            img_m = re.search(r'data-mainimage="(https?://[^"]+)"', item)
            if not img_m:
                img_m = re.search(r'<img[^>]+src="(https?://[^"]+\.(?:jpg|jpeg|png|webp)[^"]*)"', item, re.IGNORECASE)
            image = img_m.group(1) if img_m else ""

            # Description
            desc_m = re.search(r'class="result-item__description[^"]*">([^<]{10,})<', item)
            desc = desc_m.group(1).strip() if desc_m else ""

            if not title.strip() or not price:
                continue

            cars.append({
                "id": f"em_{url_suffix.replace('/ad/','')}",
                "title": title,
                "make": make,
                "model": model,
                "year": year,
                "price": price,
                "price_num": _parse_price_int(price),
                "mileage": mileage,
                "fuel": fuel,
                "transmission": transmission,
                "image": image,
                "description": desc,
                "url": listing_url,
                "source": "Exchange&Mart",
                "agent": "",
                "agent_phone": "",
            })
        except Exception as e:
            print(f"  [E&M] Parse error: {e}")

    print(f"  [E&M] Found {len(cars)} cars (total: {total})")
    return cars, total


def gumtree_car_search(location: str, params: dict) -> tuple:
    """
    Scrape Gumtree car listings.
    Returns (list_of_cars, total_count).
    """
    loc_slug = re.sub(r'[^a-z0-9]+', '-', location.strip().lower()).strip('-')
    qp = {}
    if params.get("car_min_price"):
        qp["min_price"] = params["car_min_price"]
    if params.get("car_max_price"):
        qp["max_price"] = params["car_max_price"]
    if params.get("car_year_from"):
        qp["min_year"] = params["car_year_from"]

    index = int(params.get("index", 0))
    page = index // 20 + 1
    if page > 1:
        qp["page"] = str(page)

    sort_val = params.get("sort", "newest")
    if sort_val == "price_asc":
        qp["sort"] = "price_asc"
    elif sort_val == "price_desc":
        qp["sort"] = "price_desc"

    qs = urllib.parse.urlencode(qp)
    url = f"https://www.gumtree.com/cars/uk/{loc_slug}{'?' + qs if qs else ''}"
    print(f"  [GT Cars] Fetching: {url}")

    try:
        req = urllib.request.Request(url, headers=BROWSER_HEADERS)
        with urllib.request.urlopen(req, timeout=20) as resp:
            if resp.status not in (200, 201):
                print(f"  [GT Cars] HTTP {resp.status} — skipping")
                return [], 0
            html = resp.read().decode("utf-8", errors="replace")
    except Exception as e:
        print(f"  [GT Cars] Error: {e}")
        return [], 0

    if "kramericaindustries" in html or len(html) < 5000:
        print("  [GT Cars] Bot challenge detected")
        return [], 0

    # Parse using article pattern (same as property gumtree scraper)
    article_chunks = re.findall(
        r'<article[^>]+data-q="[^"]*listing[^"]*"[^>]*>(.*?)(?=<article|</section)',
        html, re.DOTALL
    )
    if not article_chunks:
        article_chunks = re.findall(r'<li[^>]+class="[^"]*listing-maxi[^"]*"[^>]*>(.*?)(?=<li|$)', html, re.DOTALL)

    cars = []
    for chunk in article_chunks[:20]:
        try:
            title_m = re.search(r'data-q="listing-title"[^>]*>([^<]+)<', chunk)
            if not title_m:
                title_m = re.search(r'<h2[^>]*>.*?<a[^>]*>([^<]+)</a>', chunk, re.DOTALL)
            title = title_m.group(1).strip() if title_m else ""

            price_m = re.search(r'data-q="listing-price"[^>]*>(£[\d,]+)', chunk)
            if not price_m:
                price_m = re.search(r'>(£[\d,]+)<', chunk)
            price = price_m.group(1).strip() if price_m else ""

            href_m = re.search(r'href="(/p/[^"]+)"', chunk)
            listing_url = f"https://www.gumtree.com{href_m.group(1)}" if href_m else ""

            # Gumtree CDN images don't have file extensions — match img.gumtree.com directly
            img_m = re.search(r'src="(https://img\.gumtree\.com[^"]*)"', chunk, re.IGNORECASE)
            if not img_m:
                img_m = re.search(r'src="(https://[^"]+(?:jpg|jpeg|png|webp)[^"]*)"', chunk, re.IGNORECASE)
            image = img_m.group(1) if img_m else ""

            desc_m = re.search(r'data-q="tile-description"[^>]*>(.*?)(?=data-q="|</div></div>)', chunk, re.DOTALL)
            if desc_m:
                desc = re.sub(r'<[^>]+>', ' ', desc_m.group(1))
                desc = re.sub(r'\s+', ' ', desc).strip()[:200]
            else:
                desc_m2 = re.search(r'data-q="listing-description"[^>]*>([^<]{10,})<', chunk)
                desc = desc_m2.group(1).strip() if desc_m2 else ""

            year_m = re.search(r'\b(19[89]\d|20[012]\d)\b', title + " " + desc)
            year = year_m.group(1) if year_m else ""

            if not title or not price:
                continue

            cars.append({
                "id": f"gt_car_{re.sub(r'[^a-z0-9]','_',title.lower())[:30]}",
                "title": title,
                "make": title.split()[0] if title else "",
                "model": title.split()[1] if len(title.split()) > 1 else "",
                "year": year,
                "price": price,
                "price_num": _parse_price_int(price),
                "mileage": "",
                "fuel": "",
                "transmission": "",
                "image": image,
                "description": desc,
                "url": listing_url,
                "source": "Gumtree",
                "agent": "Private Seller",
                "agent_phone": "",
            })
        except Exception as e:
            print(f"  [GT Cars] Parse error: {e}")

    total_m = re.search(r'([\d,]+)\s+(?:cars?|vehicles?|ads?)\s+(?:found|available)', html, re.IGNORECASE)
    total = int(re.sub(r'[^\d]', '', total_m.group(1))) if total_m else len(cars)
    print(f"  [GT Cars] Found {len(cars)} cars (total: {total})")
    return cars, total


def combined_car_search(location: str, params: dict) -> dict:
    """
    Run Exchange & Mart and Gumtree car searches in parallel.
    Returns merged results dict.
    """
    em_results, em_total = [], 0
    gt_results, gt_total = [], 0
    em_error, gt_error = None, None

    def fetch_em():
        nonlocal em_results, em_total, em_error
        try:
            em_results, em_total = exchangeandmart_search(location, params)
        except Exception as e:
            em_error = str(e)
            print(f"  [E&M] Thread error: {e}")

    def fetch_gt():
        nonlocal gt_results, gt_total, gt_error
        try:
            gt_results, gt_total = gumtree_car_search(location, params)
        except Exception as e:
            gt_error = str(e)
            print(f"  [GT Cars] Thread error: {e}")

    t1 = threading.Thread(target=fetch_em, daemon=True)
    t2 = threading.Thread(target=fetch_gt, daemon=True)
    t1.start(); t2.start()
    t1.join(timeout=25); t2.join(timeout=25)

    # Merge and sort
    all_results = []
    max_len = max(len(em_results), len(gt_results), 1)
    for i in range(max_len):
        if i < len(em_results): all_results.append(em_results[i])
        if i < len(gt_results): all_results.append(gt_results[i])

    sort_val = params.get("sort", "newest")
    if sort_val == "price_asc":
        # Items with no price_num (0) sink to the bottom
        all_results.sort(key=lambda x: x.get("price_num") or 999_999_999)
    elif sort_val == "price_desc":
        # Items with no price_num (0) sink to the bottom
        all_results.sort(key=lambda x: x.get("price_num") or 0, reverse=True)
    elif sort_val == "year_desc":
        all_results.sort(key=lambda x: int(x.get("year", 0) or 0), reverse=True)
    elif sort_val == "mileage_asc":
        def miles_key(x):
            m = re.sub(r'[^\d]', '', x.get("mileage",""))
            return int(m) if m else 999999999
        all_results.sort(key=miles_key)

    sources = []
    source_totals = {}
    if em_results:
        sources.append("Exchange&Mart")
        source_totals["Exchange&Mart"] = em_total
    else:
        source_totals["Exchange&Mart"] = 0
    if gt_results:
        sources.append("Gumtree")
        source_totals["Gumtree"] = gt_total
    else:
        source_totals["Gumtree"] = 0

    return {
        "results": all_results,
        "total": em_total + gt_total,
        "shown": len(all_results),
        "location": location,
        "sources": sources,
        "source_totals": source_totals,
        "errors": {"exchange_mart": em_error, "gumtree": gt_error},
        "category": "cars",
    }


# ═══════════════════════════════════════════════════════════════
# JOBS SCRAPERS
# ═══════════════════════════════════════════════════════════════

def _reed_slug(query: str, location: str) -> str:
    """Build Reed URL path: /jobs/{query-slug}-jobs-in-{location-slug}"""
    def slugify(s):
        return re.sub(r'[^a-z0-9]+', '-', s.strip().lower()).strip('-')
    q = slugify(query) if query else "jobs"
    # "uk", "UK", or empty → use "anywhere" so Reed returns nationwide results
    loc_clean = location.strip().lower() if location else ""
    if not loc_clean or loc_clean in ("uk", "united kingdom", "nationwide", "anywhere"):
        loc = "anywhere"
    else:
        loc = slugify(location)
    return f"{q}-jobs-in-{loc}"


def reed_search(query: str, location: str, params: dict) -> tuple:
    """
    Scrape Reed.co.uk job listings via __NEXT_DATA__.
    Returns (list_of_jobs, total_count).
    """
    slug = _reed_slug(query, location)
    qp = {}
    if params.get("job_min_salary"):
        qp["salaryFrom"] = params["job_min_salary"]
    if params.get("job_max_salary"):
        qp["salaryTo"] = params["job_max_salary"]
    if params.get("job_radius"):
        qp["proximity"] = params["job_radius"]
    if params.get("job_type") and params["job_type"] != "any":
        type_map = {"permanent": "permanent", "contract": "contract", "temp": "temp",
                    "part_time": "part-time", "full_time": "full-time"}
        t = type_map.get(params["job_type"], "")
        if t:
            qp["contract"] = t

    index = int(params.get("index", 0))
    page = index // 25 + 1
    if page > 1:
        qp["pageno"] = str(page)

    sort_val = params.get("sort", "newest")
    if sort_val == "salary_asc":
        qp["sortBy"] = "salary"
    elif sort_val == "salary_desc":
        qp["sortBy"] = "salary"  # Reed doesn't separate asc/desc — sort locally

    qs = urllib.parse.urlencode(qp)
    url = f"https://www.reed.co.uk/jobs/{slug}{'?' + qs if qs else ''}"
    print(f"  [Reed] Fetching: {url}")

    try:
        req = urllib.request.Request(url, headers=BROWSER_HEADERS)
        with urllib.request.urlopen(req, timeout=20) as resp:
            if resp.status not in (200, 301, 302):
                raise Exception(f"HTTP {resp.status}")
            html = resp.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as e:
        print(f"  [Reed] HTTP error: {e.code}")
        return [], 0
    except Exception as e:
        print(f"  [Reed] Error: {e}")
        return [], 0

    next_data = re.findall(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', html, re.DOTALL)
    if not next_data:
        print("  [Reed] No __NEXT_DATA__ found")
        return [], 0

    try:
        d = json.loads(next_data[0])
    except Exception as e:
        print(f"  [Reed] JSON parse error: {e}")
        return [], 0

    props = d.get("props", {}).get("pageProps", {})
    sr = props.get("searchResults", {})
    raw_jobs = sr.get("jobs", []) + sr.get("promotedJobs", [])[:2]
    total = sr.get("count", 0)

    jobs = []
    for rj in raw_jobs:
        try:
            jd = rj.get("jobDetail", {})
            job_id = jd.get("jobId", "")
            title = jd.get("jobTitle", "")
            company = jd.get("ouName", rj.get("profileName", ""))
            location_name = jd.get("displayLocationName", location)
            salary_desc = jd.get("salaryDescription", "")
            salary_from = jd.get("salaryFrom", 0)
            salary_to = jd.get("salaryTo", 0)
            job_type_id = jd.get("jobType", 1)
            job_type_map = {1: "Permanent", 2: "Contract", 3: "Temp", 4: "Part-time"}
            job_type_label = job_type_map.get(job_type_id, "Permanent")
            is_full_time = jd.get("isFullTime", True)
            remote = jd.get("remoteWorkingOption", 0)
            remote_labels = {0: "", 1: "Remote", 2: "Hybrid", 3: "Office"}
            remote_label = remote_labels.get(remote, "")
            date_posted = jd.get("displayDate", jd.get("dateCreated", ""))
            description = re.sub(r'<[^>]+>', '', jd.get("jobDescription", ""))[:300].strip()
            job_url = f"https://www.reed.co.uk{rj.get('url', '')}" if rj.get("url", "").startswith("/") else rj.get("url", "")
            if not job_url and job_id:
                job_url = f"https://www.reed.co.uk/jobs/{job_id}"
            logo = rj.get("logoImage", {})
            logo_url = logo.get("url", "") if isinstance(logo, dict) else ""

            # Format salary
            if salary_desc:
                salary = salary_desc
            elif salary_from and salary_to:
                salary = f"£{salary_from:,} – £{salary_to:,}"
            elif salary_from:
                salary = f"£{salary_from:,}+"
            else:
                salary = "Salary not specified"

            jobs.append({
                "id": f"reed_{job_id}",
                "title": title,
                "company": company,
                "location": location_name,
                "salary": salary,
                "salary_num": salary_from or 0,
                "job_type": job_type_label,
                "is_full_time": is_full_time,
                "remote": remote_label,
                "date_posted": date_posted,
                "description": description,
                "url": job_url,
                "logo": logo_url,
                "source": "Reed",
                "apply_url": job_url,
                "taxonomy": jd.get("taxonomyLevel1", ""),
                "easy_apply": jd.get("isEasyApply", False),
            })
        except Exception as e:
            print(f"  [Reed] Parse error: {e}")

    print(f"  [Reed] Found {len(jobs)} jobs (total: {total})")
    return jobs, total


def combined_job_search(query: str, location: str, params: dict) -> dict:
    """
    Run Reed job search (expandable to more sources).
    Returns merged results dict.
    """
    reed_results, reed_total = [], 0
    reed_error = None

    def fetch_reed():
        nonlocal reed_results, reed_total, reed_error
        try:
            reed_results, reed_total = reed_search(query, location, params)
        except Exception as e:
            reed_error = str(e)
            print(f"  [Reed] Thread error: {e}")

    t1 = threading.Thread(target=fetch_reed, daemon=True)
    t1.start()
    t1.join(timeout=25)

    all_results = list(reed_results)

    sort_val = params.get("sort", "newest")
    if sort_val == "salary_asc":
        all_results.sort(key=lambda x: x.get("salary_num", 0) or 0)
    elif sort_val == "salary_desc":
        all_results.sort(key=lambda x: x.get("salary_num", 0), reverse=True)

    sources = []
    source_totals = {}
    if reed_results:
        sources.append("Reed")
        source_totals["Reed"] = reed_total
    else:
        source_totals["Reed"] = 0

    return {
        "results": all_results,
        "total": reed_total,
        "shown": len(all_results),
        "query": query,
        "location": location,
        "sources": sources,
        "source_totals": source_totals,
        "errors": {"reed": reed_error},
        "category": "jobs",
    }


# ─────────────────────────────────────────────
# HTTP REQUEST HANDLER
# ─────────────────────────────────────────────

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

    def send_file(self, path, content_type):
        try:
            with open(path, "rb") as f:
                content = f.read()
            self.send_response(200)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(content)))
            self.end_headers()
            self.wfile.write(content)
        except FileNotFoundError:
            self.send_response(404)
            self.end_headers()

    def do_OPTIONS(self):
        self.send_response(200)
        self.cors_headers()
        self.end_headers()

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path
        qs = urllib.parse.parse_qs(parsed.query)

        if path in ("/", "/index.html"):
            self.send_file("/home/user/webapp/index.html", "text/html; charset=utf-8")
            return

        elif path.startswith("/public/"):
            # Serve static files from the public directory
            file_path = "/home/user/webapp" + path
            ext = os.path.splitext(file_path)[1].lower()
            mime_types = {
                ".png": "image/png", ".jpg": "image/jpeg", ".jpeg": "image/jpeg",
                ".gif": "image/gif", ".webp": "image/webp", ".svg": "image/svg+xml",
                ".css": "text/css", ".js": "application/javascript",
            }
            mime = mime_types.get(ext, "application/octet-stream")
            self.send_file(file_path, mime)
            return

        elif path == "/api/location":
            query = qs.get("q", [""])[0].strip()
            if not query:
                self.send_json({"error": "Missing q parameter"}, 400)
                return
            loc_id, display = rm_typeahead(query)
            self.send_json({
                "locationIdentifier": loc_id,
                "displayName": display,
                "query": query,
                "found": bool(loc_id)
            })

        elif path == "/api/search":
            location = qs.get("location", [""])[0].strip()
            if not location:
                self.send_json({"error": "Missing location parameter"}, 400)
                return

            params = {
                "transaction_type": qs.get("type", ["buy"])[0],
                "radius":        qs.get("radius", ["1.0"])[0],
                "min_beds":      qs.get("min_beds", [""])[0],
                "max_beds":      qs.get("max_beds", [""])[0],
                "min_price":     qs.get("min_price", [""])[0],
                "max_price":     qs.get("max_price", [""])[0],
                "property_type": qs.get("property_type", ["any"])[0],
                "index":         int(qs.get("index", ["0"])[0]),
                "must_parking":  qs.get("must_parking", ["false"])[0].lower() == "true",
                "must_garden":   qs.get("must_garden", ["false"])[0].lower() == "true",
                "sort":          qs.get("sort", ["newest"])[0],
            }

            print(f"\n🔍 Search: '{location}' | {params}")
            result = combined_search(location, params)
            self.send_json(result)

        elif path == "/api/config":
            cfg = _load_config()
            has_key = bool(get_scrapfly_key())
            # Check quota if key exists
            quota_info = {}
            if has_key:
                try:
                    acct_url = f"https://api.scrapfly.io/account?key={get_scrapfly_key()}"
                    req_acct = urllib.request.Request(acct_url)
                    with urllib.request.urlopen(req_acct, timeout=10) as r_acct:
                        acct_data = json.loads(r_acct.read())
                    usage = acct_data.get("subscription", {}).get("usage", {}).get("scrape", {})
                    quota_info = {
                        "plan": acct_data.get("subscription", {}).get("plan_name", ""),
                        "used": usage.get("current", 0),
                        "limit": usage.get("limit", 0),
                        "remaining": usage.get("remaining", 0),
                        "quota_reached": acct_data.get("project", {}).get("quota_reached", False),
                    }
                except Exception as e:
                    quota_info = {"error": str(e)[:80]}
            self.send_json({
                "has_scrapfly_key": has_key,
                "zoopla_enabled": has_key,
                "quota": quota_info,
            })

        elif path == "/api/zoopla-status":
            has_key = bool(get_scrapfly_key())
            self.send_json({"enabled": has_key, "key_set": has_key})

        elif path == "/api/cars":
            location = qs.get("location", [""])[0].strip()
            if not location:
                self.send_json({"error": "Missing location parameter"}, 400)
                return
            car_params = {
                "car_make":         qs.get("make", [""])[0],
                "car_model":        qs.get("model", [""])[0],
                "car_min_price":    qs.get("min_price", [""])[0],
                "car_max_price":    qs.get("max_price", [""])[0],
                "car_year_from":    qs.get("min_year", [""])[0] or qs.get("year_from", [""])[0],
                "car_year_to":      qs.get("max_year", [""])[0] or qs.get("year_to", [""])[0],
                "car_fuel":         qs.get("fuel", [""])[0],
                "car_transmission": qs.get("transmission", [""])[0],
                "car_max_mileage":  qs.get("max_mileage", [""])[0],
                "sort":             qs.get("sort", ["newest"])[0],
                "index":            int(qs.get("index", ["0"])[0]),
            }
            print(f"\n🚗 Car Search: '{location}' | {car_params}")
            result = combined_car_search(location, car_params)
            self.send_json(result)

        elif path == "/api/jobs":
            query = qs.get("query", [""])[0].strip()
            location = qs.get("location", ["uk"])[0].strip() or "uk"
            job_params = {
                "job_min_salary":   qs.get("min_salary", [""])[0],
                "job_max_salary":   qs.get("max_salary", [""])[0],
                "job_type":         qs.get("job_type", ["any"])[0],
                "job_radius":       qs.get("radius", ["20"])[0],
                "sort":             qs.get("sort", ["newest"])[0],
                "index":            int(qs.get("index", ["0"])[0]),
            }
            if not query:
                self.send_json({"error": "Missing query parameter"}, 400)
                return
            print(f"\n💼 Job Search: '{query}' in '{location}' | {job_params}")
            result = combined_job_search(query, location, job_params)
            self.send_json(result)

        else:
            self.send_response(404)
            self.end_headers()

    def do_POST(self):
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path

        length = int(self.headers.get("Content-Length", 0))
        body_raw = self.rfile.read(length) if length else b""
        try:
            body = json.loads(body_raw) if body_raw else {}
        except Exception:
            body = {}

        if path == "/api/config":
            # Save Scrapfly API key
            new_key = body.get("scrapfly_api_key", "").strip()
            if not new_key:
                self.send_json({"error": "scrapfly_api_key is required"}, 400)
                return
            # Validate key by calling Scrapfly account API
            try:
                acct_url = f"https://api.scrapfly.io/account?key={new_key}"
                req_v = urllib.request.Request(acct_url)
                with urllib.request.urlopen(req_v, timeout=10) as r_v:
                    acct = json.loads(r_v.read())
                usage = acct.get("subscription", {}).get("usage", {}).get("scrape", {})
                plan = acct.get("subscription", {}).get("plan_name", "")
                remaining = usage.get("remaining", 0)
                quota_reached = acct.get("project", {}).get("quota_reached", False)
            except urllib.error.HTTPError as e:
                if e.code == 401:
                    self.send_json({"error": "Invalid Scrapfly API key (401 Unauthorized)"}, 400)
                    return
                self.send_json({"error": f"Could not validate key: HTTP {e.code}"}, 400)
                return
            except Exception as e:
                self.send_json({"error": f"Could not reach Scrapfly: {str(e)[:80]}"}, 400)
                return

            # Save to config
            cfg = _load_config()
            cfg["scrapfly_api_key"] = new_key
            _save_config(cfg)
            print(f"[Config] Scrapfly key saved. Plan: {plan}, remaining: {remaining}")
            self.send_json({
                "success": True,
                "plan": plan,
                "remaining": remaining,
                "quota_reached": quota_reached,
                "message": f"Key saved! Plan: {plan}, {remaining} scrapes remaining.",
            })
            return

        elif path == "/api/email":
            user_info = body.get("user", {})
            properties = body.get("properties", [])

            if not user_info.get("name") or not user_info.get("email"):
                self.send_json({"error": "Name and email are required"}, 400)
                return
            if not properties:
                self.send_json({"error": "No properties selected"}, 400)
                return

            emails = build_viewing_emails(user_info, properties)
            self.send_json({
                "success": True,
                "count": len(emails),
                "emails": emails,
            })

        elif path == "/api/search":
            # POST variant
            location = body.get("location", "").strip()
            if not location:
                self.send_json({"error": "Missing location"}, 400)
                return

            params = {
                "transaction_type": body.get("type", "buy"),
                "radius":        str(body.get("radius", "1.0")),
                "min_beds":      str(body.get("min_beds", "")),
                "max_beds":      str(body.get("max_beds", "")),
                "min_price":     str(body.get("min_price", "")),
                "max_price":     str(body.get("max_price", "")),
                "property_type": body.get("property_type", "any"),
                "index":         0,
                "must_parking":  bool(body.get("must_parking", False)),
                "must_garden":   bool(body.get("must_garden", False)),
                "sort":          body.get("sort", "newest"),
            }

            print(f"\n🔍 POST Search: '{location}' | {params}")
            result = combined_search(location, params)
            self.send_json(result)

        else:
            self.send_response(404)
            self.end_headers()


# ─────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────

def run(port=3000):
    server = HTTPServer(("0.0.0.0", port), CrabifyHandler)
    zp_key = get_scrapfly_key()
    zp_status = "✅ enabled" if zp_key else "⚠️  no key (add at /api/config)"
    print(f"🦀 Crabify server running on http://0.0.0.0:{port}")
    print(f"   Property: Rightmove ✅ | OnTheMarket ✅ | Zoopla {zp_status} | Gumtree ✅ | SpareRoom ✅ (rent)")
    print(f"   Cars:     Exchange&Mart ✅ | Gumtree ✅")
    print(f"   Jobs:     Reed.co.uk ✅")
    server.serve_forever()


if __name__ == "__main__":
    run()
