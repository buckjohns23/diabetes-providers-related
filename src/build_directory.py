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
HOME_LAT = 39.6130
HOME_LON = -86.1067

NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"

# ✅ Updated email
USER_AGENT = "DiabetesProvidersRelated/1.0 (contact: c_m_johnson@yahoo.com)"

CACHE_DIR = "cache"
GEOCODE_CACHE_PATH = os.path.join(CACHE_DIR, "geocode_cache.json")

MAX_NEW_GEOCODES_PER_RUN = 80
GEOCODE_SLEEP_SECONDS = 1.1

STATE = "IN"
CITIES = [
    "Indianapolis", "Carmel", "Fishers", "Noblesville", "Westfield", "Zionsville",
    "Greenwood", "Franklin", "Avon", "Plainfield", "Brownsburg", "Danville",
    "Mooresville", "Martinsville", "Shelbyville", "Lebanon", "Lawrence", "Speedway",
    "Beech Grove"
]

TAXONOMY_ALLOWLIST = {
    "207RE0101X",
    "2080P0205X",
    "207Q00000X",
    "207R00000X",
    "363L00000X",
    "363A00000X",
}


# =========================
# HELPERS
# =========================

def clean_phone(phone: str) -> str:
    if not phone:
        return ""
    digits = re.sub(r"\D+", "", phone)
    if len(digits) == 10:
        return f"({digits[0:3]}) {digits[3:6]}-{digits[6:10]}"
    return phone.strip()


def haversine_miles(lat1, lon1, lat2, lon2):
    R = 3958.7613
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dlambda/2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c


def load_cache():
    os.makedirs(CACHE_DIR, exist_ok=True)
    if not os.path.exists(GEOCODE_CACHE_PATH):
        return {"items": {}}
    try:
        with open(GEOCODE_CACHE_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"items": {}}


def save_cache(cache):
    with open(GEOCODE_CACHE_PATH, "w", encoding="utf-8") as f:
        json.dump(cache, f, indent=2)


# SAFE GEOCODING — WILL NOT CRASH WORKFLOW
def geocode_address(addr, cache, budget):
    key = addr.lower().strip()
    if not key:
        return None

    if key in cache["items"]:
        return cache["items"][key]

    if budget["remaining"] <= 0:
        return None

    params = {
        "q": addr,
        "format": "json",
        "limit": 1
    }

    try:
        r = requests.get(
            NOMINATIM_URL,
            params=params,
            headers={"User-Agent": USER_AGENT},
            timeout=30
        )

        if r.status_code != 200:
            return None

        try:
            results = r.json()
        except Exception:
            return None

        if results:
            lat = float(results[0]["lat"])
            lon = float(results[0]["lon"])
            cache["items"][key] = (lat, lon)
            budget["remaining"] -= 1
            time.sleep(GEOCODE_SLEEP_SECONDS)
            return lat, lon

    except Exception:
        return None

    return None


def fetch_city(city):
    params = {
        "version": VERSION,
        "state": STATE,
        "city": city,
        "limit": LIMIT
    }
    try:
        r = requests.get(NPI_API, params=params, timeout=30)
        return r.json().get("results", [])
    except Exception:
        return []


def provider_matches(item):
    for t in item.get("taxonomies", []):
        if t.get("code") in TAXONOMY_ALLOWLIST:
            return True
    return False


def build_provider(item, home_lat, home_lon, cache, budget):
    basic = item.get("basic", {})
    addr = item.get("addresses", [{}])[0]

    name = basic.get("organization_name") or \
           f"{basic.get('first_name','')} {basic.get('last_name','')}".strip()

    full_address = f"{addr.get('address_1','')}, {addr.get('city','')}, {addr.get('state','')} {addr.get('postal_code','')}"

    latlon = geocode_address(full_address, cache, budget)

    distance = None
    if latlon:
        distance = round(haversine_miles(home_lat, home_lon, latlon[0], latlon[1]), 1)

    return {
        "name": name,
        "phone": clean_phone(addr.get("telephone_number")),
        "address": addr.get("address_1"),
        "city": addr.get("city"),
        "state": addr.get("state"),
        "zip": addr.get("postal_code"),
        "distance_miles": distance
    }


def main():
    cache = load_cache()
    budget = {"remaining": MAX_NEW_GEOCODES_PER_RUN}

    home_latlon = geocode_address(HOME_ADDRESS, cache, budget)
    home_lat, home_lon = home_latlon if home_latlon else (HOME_LAT, HOME_LON)

    results = []
    for city in CITIES:
        for item in fetch_city(city):
            if provider_matches(item):
                results.append(item)

    providers = [
        build_provider(p, home_lat, home_lon, cache, budget)
        for p in results
    ]

    providers.sort(key=lambda x: x["distance_miles"] or 999)

    payload = {
        "generated_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        "home_address": HOME_ADDRESS,
        "providers": providers
    }

    with open("src/template.html", "r", encoding="utf-8") as f:
        template = f.read()

    html = template.replace(
        "/*__EMBEDDED_DATA__*/",
        "const DIRECTORY_DATA = " + json.dumps(payload) + ";"
    )

    os.makedirs("docs", exist_ok=True)
    with open("docs/index.plain.html", "w", encoding="utf-8") as f:
        f.write(html)

    save_cache(cache)


if __name__ == "__main__":
    main()
