import json
import math
import os
import re
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests

# =========================
# CONFIG
# =========================
NPI_API = "https://npiregistry.cms.hhs.gov/api/"
VERSION = "2.1"
LIMIT = 200

HOME_ADDRESS = "1651 Cascade Drive, Greenwood, IN"
HOME_LAT = 39.6130   # fallback
HOME_LON = -86.1067  # fallback

NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"

# IMPORTANT: replace with YOUR real email (Nominatim etiquette)
USER_AGENT = "DiabetesProvidersRelated/1.0 (contact: your-real-email@example.com)"

CACHE_DIR = "cache"
GEOCODE_CACHE_PATH = os.path.join(CACHE_DIR, "geocode_cache.json")

MAX_NEW_GEOCODES_PER_RUN = 120
GEOCODE_SLEEP_SECONDS = 1.0

STATE = "IN"
CITIES = [
    "Indianapolis", "Carmel", "Fishers", "Noblesville", "Westfield", "Zionsville",
    "Greenwood", "Franklin", "Avon", "Plainfield", "Brownsburg", "Danville",
    "Mooresville", "Martinsville", "Shelbyville", "Lebanon", "Lawrence", "Speedway",
    "Beech Grove"
]

# Diabetes-related specialties (taxonomy codes)
TAXONOMY_ALLOWLIST = {
    "207RE0101X",  # Endocrinology, Diabetes & Metabolism
    "2080P0205X",  # Pediatric Endocrinology
    "207Q00000X",  # Family Medicine
    "207R00000X",  # Internal Medicine
    "363L00000X",  # Nurse Practitioner
    "363A00000X",  # Physician Assistant
}

def clean_phone(phone: str) -> str:
    if not phone:
        return ""
    digits = re.sub(r"\D+", "", phone)
    if len(digits) == 10:
        return f"({digits[0:3]}) {digits[3:6]}-{digits[6:10]}"
    return phone.strip()

def years_since(date_str: str) -> float:
    try:
        d = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        now = datetime.now(timezone.utc)
        return round((now - d).days / 365.25, 1)
    except Exception:
        return 0.0

def haversine_miles(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 3958.7613
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c

def load_cache() -> Dict[str, Any]:
    os.makedirs(CACHE_DIR, exist_ok=True)
    if not os.path.exists(GEOCODE_CACHE_PATH):
        return {"_meta": {"created_utc": datetime.now(timezone.utc).isoformat()}, "items": {}}
    try:
        with open(GEOCODE_CACHE_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"_meta": {"created_utc": datetime.now(timezone.utc).isoformat()}, "items": {}}

def save_cache(cache: Dict[str, Any]) -> None:
    cache["_meta"]["updated_utc"] = datetime.now(timezone.utc).isoformat()
    with open(GEOCODE_CACHE_PATH, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)

def normalize_addr_key(addr: str) -> str:
    a = re.sub(r"\s+", " ", (addr or "").strip().lower())
    a = a.replace(".", "").replace(",", "")
    return a

def geocode_address(addr: str, cache: Dict[str, Any], budget: Dict[str, int]) -> Optional[Tuple[float, float]]:
    key = normalize_addr_key(addr)
    if not key:
        return None

    items = cache.get("items", {})
    if key in items and "lat" in items[key] and "lon" in items[key]:
        return float(items[key]["lat"]), float(items[key]["lon"])

    if budget["remaining"] <= 0:
        return None

    params = {"q": addr, "format": "json", "limit": 1, "addressdetails": 0}
    try:
        r = requests.get(
            NOMINATIM_URL,
            params=params,
            headers={"User-Agent": USER_AGENT},
            timeout=30
        )
        r.raise_for_status()
        res = r.json()
        if res and isinstance(res, list):
            lat = float(res[0]["lat"])
            lon = float(res[0]["lon"])
            items[key] = {"lat": lat, "lon": lon, "raw": addr}
            cache["items"] = items
            budget["remaining"] -= 1
            time.sleep(GEOCODE_SLEEP_SECONDS)
            return lat, lon
    except Exception:
        items[key] = {"error": True, "raw": addr}
        cache["items"] = items
        return None

    return None

def fetch_city(city: str) -> List[Dict[str, Any]]:
    params = {"version": VERSION, "state": STATE, "city": city, "limit": LIMIT, "skip": 0}
    r = requests.get(NPI_API, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()

    total = int(data.get("result_count", 0))
    if total == 0:
        return []

    pages = int(math.ceil(total / LIMIT))
    results: List[Dict[str, Any]] = []
    results.extend(data.get("results", []) or [])

    for p in range(1, pages):
        params["skip"] = p * LIMIT
        rp = requests.get(NPI_API, params=params, timeout=30)
        rp.raise_for_status()
        dp = rp.json()
        results.extend(dp.get("results", []) or [])

    return results

def provider_matches_taxonomy(item: Dict[str, Any]) -> bool:
    for t in (item.get("taxonomies") or []):
        code = (t.get("code") or "").strip()
        if code in TAXONOMY_ALLOWLIST:
            return True
    return False

def pick_best_address(item: Dict[str, Any]) -> Dict[str, Any]:
    addrs = item.get("addresses") or []
    for a in addrs:
        if (a.get("address_purpose") or "").lower() == "location":
            return a
    return addrs[0] if addrs else {}

def build_full_address(addr: Dict[str, Any]) -> str:
    parts = []
    for k in ["address_1", "address_2"]:
        v = (addr.get(k) or "").strip()
        if v:
            parts.append(v)
    city = (addr.get("city") or "").strip()
    state = (addr.get("state") or "").strip()
    zipc = (addr.get("postal_code") or "").strip()
    line1 = " ".join(parts).strip()
    line2 = " ".join([x for x in [city + ",", state, zipc] if x]).replace(" ,", ",").strip()
    return ", ".join([x for x in [line1, line2] if x])

def build_provider_record(item: Dict[str, Any], home_lat: float, home_lon: float,
                          cache: Dict[str, Any], budget: Dict[str, int]) -> Dict[str, Any]:
    basic = item.get("basic") or {}
    npi = str(item.get("number") or "")

    addr = pick_best_address(item)
    phone = clean_phone(addr.get("telephone_number") or "")
    enumeration_date = (basic.get("enumeration_date") or "")
    years = years_since(enumeration_date)

    if basic.get("organization_name"):
        display_name = basic.get("organization_name")
        provider_type = "Organization"
    else:
        first = basic.get("first_name") or ""
        last = basic.get("last_name") or ""
        cred = basic.get("credential") or ""
        display_name = " ".join([x for x in [first, last] if x]).strip()
        if cred:
            display_name = f"{display_name}, {cred}".strip()
        provider_type = "Individual"

    tax_labels = []
    for t in (item.get("taxonomies") or []):
        code = (t.get("code") or "").strip()
        desc = (t.get("desc") or "").strip()
        if code in TAXONOMY_ALLOWLIST:
            tax_labels.append(desc or code)
    tax_labels = sorted(set([x for x in tax_labels if x]))

    full_addr = build_full_address(addr)
    latlon = geocode_address(full_addr, cache, budget)

    if latlon:
        plat, plon = latlon
        dist = round(haversine_miles(home_lat, home_lon, plat, plon), 1)
    else:
        dist = None

    return {
        "npi": npi,
        "name": display_name,
        "provider_type": provider_type,
        "taxonomy": ", ".join(tax_labels),
        "phone": phone,
        "address": (addr.get("address_1") or "").strip() + ((" " + (addr.get("address_2") or "").strip()) if (addr.get("address_2") or "").strip() else ""),
        "city": (addr.get("city") or "").strip(),
        "state": (addr.get("state") or "").strip(),
        "zip": (addr.get("postal_code") or "").strip(),
        "enumeration_date": enumeration_date,
        "years_in_practice_proxy": years,
        "distance_miles": dist,
    }

def main() -> None:
    cache = load_cache()
    budget = {"remaining": MAX_NEW_GEOCODES_PER_RUN}

    home_latlon = geocode_address(HOME_ADDRESS, cache, budget)
    if home_latlon:
        home_lat, home_lon = home_latlon
    else:
        home_lat, home_lon = HOME_LAT, HOME_LON

    all_results: List[Dict[str, Any]] = []
    for city in CITIES:
        try:
            all_results.extend(fetch_city(city))
        except Exception as e:
            print(f"[WARN] Failed city={city}: {e}")

    filtered = [x for x in all_results if provider_matches_taxonomy(x)]

    by_npi: Dict[str, Dict[str, Any]] = {}
    for item in filtered:
        npi = str(item.get("number") or "")
        if npi:
            by_npi[npi] = item

    providers = [build_provider_record(v, home_lat, home_lon, cache, budget) for v in by_npi.values()]
    save_cache(cache)

    # Default sort: closest first, then endocrinology, then years, then name
    def sort_key(p: Dict[str, Any]):
        dist = p.get("distance_miles")
        dist_key = dist if isinstance(dist, (int, float)) else 1e9
        endo_boost = 0 if "endocrinology" in (p.get("taxonomy") or "").lower() else 1
        return (dist_key, endo_boost, -float(p.get("years_in_practice_proxy") or 0), p.get("name") or "")

    providers.sort(key=sort_key)

    payload = {
        "generated_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        "count": len(providers),
        "home_address": HOME_ADDRESS,
        "geocode_budget_remaining": budget["remaining"],
        "providers": providers,
    }

    with open("src/template.html", "r", encoding="utf-8") as f:
        tmpl = f.read()

    html = tmpl.replace("/*__EMBEDDED_DATA__*/", "const DIRECTORY_DATA = " + json.dumps(payload) + ";")

    os.makedirs("docs", exist_ok=True)
    with open("docs/index.plain.html", "w", encoding="utf-8") as f:
        f.write(html)

    print(f"Built docs/index.plain.html with {len(providers)} providers.")

if __name__ == "__main__":
    main()
