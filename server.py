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

    query_params = {
        "locationIdentifier": location_id,
        "radius": str(params.get("radius", "1.0")),
        "sortType": "2",
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
    qp = {}
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
    # Pagination (OTM uses 'page' param, 1-indexed, 30 per page)
    index = int(params.get("index", 0))
    if index > 0:
        page_num = (index // 30) + 1
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
# COMBINED SEARCH (runs RM + OTM in parallel)
# ─────────────────────────────────────────────

def combined_search(location: str, params: dict) -> dict:
    """
    Run Rightmove, OnTheMarket, and Zoopla searches in parallel threads.
    Returns merged, deduplicated results.
    """
    rm_results = []
    rm_total = 0
    otm_results = []
    otm_total = 0
    zp_results = []
    zp_total = 0
    rm_error = None
    otm_error = None
    zp_error = None

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

    t_rm = threading.Thread(target=fetch_rm, daemon=True)
    t_otm = threading.Thread(target=fetch_otm, daemon=True)
    t_zp = threading.Thread(target=fetch_zoopla, daemon=True)
    t_rm.start()
    t_otm.start()
    t_zp.start()
    t_rm.join(timeout=25)
    t_otm.join(timeout=25)
    t_zp.join(timeout=35)

    # Merge: interleave RM, OTM, and Zoopla results
    all_results = []
    max_len = max(len(rm_results), len(otm_results), len(zp_results))
    for i in range(max_len):
        if i < len(rm_results):
            all_results.append(rm_results[i])
        if i < len(otm_results):
            all_results.append(otm_results[i])
        if i < len(zp_results):
            all_results.append(zp_results[i])

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
        sources.append(f"Zoopla (error)")
        source_totals["Zoopla"] = 0
    elif zp_results or zp_total:
        sources.append("Zoopla")
        source_totals["Zoopla"] = zp_total
    else:
        source_totals["Zoopla"] = 0

    return {
        "results": all_results,
        "total": rm_total + otm_total + zp_total,
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
    print(f"   Sources: Rightmove ✅ | OnTheMarket ✅ | Zoopla {zp_status}")
    server.serve_forever()


if __name__ == "__main__":
    run()
