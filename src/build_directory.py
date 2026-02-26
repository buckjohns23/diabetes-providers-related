import json
import math
import os
import re
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import requests

# =========================
# CONFIG
# =========================
NPI_API = "https://npiregistry.cms.hhs.gov/api/"
VERSION = "2.1"
LIMIT = 200

STATE = "IN"

# Cities to *seed* search (we will still hard-filter by IN + ZIP)
CITIES = [
    "Indianapolis", "Greenwood", "Franklin", "Columbus", "Bloomington", "Bedford",
    "Martinsville", "Mooresville", "Shelbyville", "Seymour", "North Vernon",
    "New Albany", "Jeffersonville", "Clarksville", "Louisville",  # (Louisville will be removed by IN filter)
    "Evansville", "Jasper", "Washington", "Vincennes", "Terre Haute"
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

ENDO_TAXONOMY_CODES = {"207RE0101X", "2080P0205X"}

# Southern Indiana rough ZIP rule:
# Indiana ZIPs are generally 460xx–479xx. Southern half skews lower/mid 46xxx/47xxx.
# We'll keep 46000–47999 BUT EXCLUDE the far north by using a cutoff:
#   keep ZIPs where first 2 digits are 46 or 47, and within those,
#   allow all. (This is simple and matches “southern half” reasonably well.)
# If you want “south of Indy only”, we can tighten further.
ALLOWED_ZIP_PREFIXES = {"46", "47"}  # 46xxx + 47xxx

HTTP_TIMEOUT = 30
MAX_RETRIES = 5
RETRY_BACKOFF_SECONDS = 1.7
RETRY_STATUS_CODES = {429, 500, 502, 503, 504}

HEADERS = {
    "User-Agent": "DiabetesProvidersRelated/1.0",
    "Accept": "application/json,text/plain,*/*",
}

DATA_DIR = "data"
LAST_GOOD_JSON = os.path.join(DATA_DIR, "last_good_payload.json")


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


def years_since(date_str: str) -> float:
    try:
        d = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        now = datetime.now(timezone.utc)
        return round((now - d).days / 365.25, 1)
    except Exception:
        return 0.0


def safe_get_json(url: str, params: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(url, params=params, headers=HEADERS, timeout=HTTP_TIMEOUT)

            if r.status_code in RETRY_STATUS_CODES:
                time.sleep(RETRY_BACKOFF_SECONDS * attempt)
                continue

            if r.status_code != 200:
                return None

            try:
                return r.json()
            except Exception:
                return None

        except Exception:
            time.sleep(RETRY_BACKOFF_SECONDS * attempt)

    return None


def fetch_city(city: str) -> List[Dict[str, Any]]:
    params = {"version": VERSION, "state": STATE, "city": city, "limit": LIMIT, "skip": 0}
    data = safe_get_json(NPI_API, params)
    if not data:
        return []

    total = int(data.get("result_count", 0) or 0)
    results = list(data.get("results", []) or [])

    if total <= LIMIT:
        return results

    pages = int(math.ceil(total / LIMIT))
    for p in range(1, pages):
        params["skip"] = p * LIMIT
        dp = safe_get_json(NPI_API, params)
        if not dp:
            continue
        results.extend(dp.get("results", []) or [])

    return results


def taxonomy_codes(item: Dict[str, Any]) -> List[str]:
    codes = []
    for t in (item.get("taxonomies") or []):
        code = (t.get("code") or "").strip()
        if code:
            codes.append(code)
    return codes


def provider_matches_taxonomy(item: Dict[str, Any]) -> bool:
    codes = taxonomy_codes(item)
    return any(code in TAXONOMY_ALLOWLIST for code in codes)


def is_endocrinologist(item: Dict[str, Any]) -> bool:
    codes = taxonomy_codes(item)
    return any(code in ENDO_TAXONOMY_CODES for code in codes)


def pick_location_address(item: Dict[str, Any]) -> Dict[str, Any]:
    addrs = item.get("addresses") or []
    for a in addrs:
        if (a.get("address_purpose") or "").lower() == "location":
            return a
    return addrs[0] if addrs else {}


def normalize_zip(zip_raw: str) -> str:
    z = (zip_raw or "").strip()
    # NPI often has ZIP+4 like 46202-1234
    z = z.split("-")[0]
    z = re.sub(r"\D+", "", z)
    return z


def is_southern_indiana_location(addr: Dict[str, Any]) -> bool:
    st = (addr.get("state") or "").strip().upper()
    if st != "IN":
        return False

    z = normalize_zip(addr.get("postal_code") or "")
    if len(z) < 5:
        return False

    if z[:2] not in ALLOWED_ZIP_PREFIXES:
        return False

    return True


def clinic_or_place_of_work(item: Dict[str, Any]) -> str:
    basic = item.get("basic") or {}
    org_name = (basic.get("organization_name") or "").strip()
    if org_name:
        return org_name

    auth = item.get("authorized_official") or {}
    auth_org = (auth.get("organization_name") or "").strip()
    if auth_org:
        return auth_org

    return ""


def build_taxonomy_text(item: Dict[str, Any]) -> str:
    labels = []
    for t in (item.get("taxonomies") or []):
        code = (t.get("code") or "").strip()
        desc = (t.get("desc") or "").strip()
        if code in TAXONOMY_ALLOWLIST:
            labels.append(desc or code)
    return ", ".join(sorted(set([x for x in labels if x])))


def build_provider_record(item: Dict[str, Any], addr: Dict[str, Any]) -> Dict[str, Any]:
    basic = item.get("basic") or {}
    npi = str(item.get("number") or "")
    enum_date = (basic.get("enumeration_date") or "").strip()

    if basic.get("organization_name"):
        provider_type = "Organization"
        provider_name = (basic.get("organization_name") or "").strip()
        credential = ""
    else:
        provider_type = "Individual"
        first = (basic.get("first_name") or "").strip()
        last = (basic.get("last_name") or "").strip()
        credential = (basic.get("credential") or "").strip()
        provider_name = " ".join([x for x in [first, last] if x]).strip()
        if credential:
            provider_name = f"{provider_name}, {credential}".strip()

    clinic = clinic_or_place_of_work(item)
    taxonomy_text = build_taxonomy_text(item)

    phone = clean_phone(addr.get("telephone_number") or "")
    address_1 = (addr.get("address_1") or "").strip()
    address_2 = (addr.get("address_2") or "").strip()
    address = (address_1 + (" " + address_2 if address_2 else "")).strip()

    city = (addr.get("city") or "").strip()
    state = (addr.get("state") or "").strip()
    zipc = normalize_zip(addr.get("postal_code") or "")

    return {
        "npi": npi,
        "provider_type": provider_type,
        "name": provider_name,
        "credential": credential,
        "clinic": clinic,
        "taxonomy": taxonomy_text,
        "is_endocrinologist": is_endocrinologist(item),
        "phone": phone,
        "address": address,
        "city": city,
        "state": state,
        "zip": zipc,
        "enumeration_date": enum_date,
        "years_in_practice_proxy": years_since(enum_date),
    }


def load_last_good() -> Optional[Dict[str, Any]]:
    try:
        if os.path.exists(LAST_GOOD_JSON):
            with open(LAST_GOOD_JSON, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception:
        return None
    return None


def save_last_good(payload: Dict[str, Any]) -> None:
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(LAST_GOOD_JSON, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def main() -> None:
    all_items: List[Dict[str, Any]] = []
    for city in CITIES:
        all_items.extend(fetch_city(city))

    # Filter to diabetes-related AND southern Indiana LOCATION address only
    by_npi: Dict[str, Dict[str, Any]] = {}
    location_addr_by_npi: Dict[str, Dict[str, Any]] = {}

    for item in all_items:
        if not provider_matches_taxonomy(item):
            continue

        npi = str(item.get("number") or "")
        if not npi:
            continue

        addr = pick_location_address(item)
        if not is_southern_indiana_location(addr):
            continue

        by_npi[npi] = item
        location_addr_by_npi[npi] = addr

    stale = False
    note = ""

    if len(by_npi) == 0:
        cached = load_last_good()
        if cached and isinstance(cached, dict) and "providers" in cached:
            payload = cached
            stale = True
            note = "NPI API returned no usable data this run; showing last successful snapshot."
        else:
            payload = {
                "generated_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                "count": 0,
                "providers": [],
            }
            stale = True
            note = "NPI API unavailable and no prior snapshot found yet."
    else:
        providers = [
            build_provider_record(by_npi[npi], location_addr_by_npi[npi])
            for npi in by_npi.keys()
        ]
        providers.sort(key=lambda p: (0 if p.get("is_endocrinologist") else 1, p.get("city") or "", p.get("name") or ""))
        payload = {
            "generated_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
            "count": len(providers),
            "providers": providers,
        }
        save_last_good(payload)

    payload["stale"] = stale
    payload["note"] = note

    with open("src/template.html", "r", encoding="utf-8") as f:
        template = f.read()

    html = template.replace(
        "/*__EMBEDDED_DATA__*/",
        "const DIRECTORY_DATA = " + json.dumps(payload) + ";"
    )

    os.makedirs("docs", exist_ok=True)
    with open("docs/index.plain.html", "w", encoding="utf-8") as f:
        f.write(html)


if __name__ == "__main__":
    main()
