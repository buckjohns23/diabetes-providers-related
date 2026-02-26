import json
import math
import os
import re
from datetime import datetime, timezone
from typing import Any, Dict, List

import requests

# =========================
# CONFIG
# =========================
NPI_API = "https://npiregistry.cms.hhs.gov/api/"
VERSION = "2.1"
LIMIT = 200

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


def fetch_city(city: str) -> List[Dict[str, Any]]:
    params = {"version": VERSION, "state": STATE, "city": city, "limit": LIMIT, "skip": 0}
    try:
        r = requests.get(NPI_API, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
    except Exception:
        return []

    total = int(data.get("result_count", 0))
    results = list(data.get("results", []) or [])
    if total <= LIMIT:
        return results

    pages = int(math.ceil(total / LIMIT))
    for p in range(1, pages):
        params["skip"] = p * LIMIT
        try:
            rp = requests.get(NPI_API, params=params, timeout=30)
            rp.raise_for_status()
            dp = rp.json()
            results.extend(dp.get("results", []) or [])
        except Exception:
            continue

    return results


def provider_matches_taxonomy(item: Dict[str, Any]) -> bool:
    for t in (item.get("taxonomies") or []):
        code = (t.get("code") or "").strip()
        if code in TAXONOMY_ALLOWLIST:
            return True
    return False


def pick_location_address(item: Dict[str, Any]) -> Dict[str, Any]:
    addrs = item.get("addresses") or []
    for a in addrs:
        if (a.get("address_purpose") or "").lower() == "location":
            return a
    return addrs[0] if addrs else {}


def clinic_or_place_of_work(item: Dict[str, Any]) -> str:
    """
    Best-effort: NPI does not reliably include employer/clinic for individuals.
    We try a few common fields and return blank if none exist.
    """
    basic = item.get("basic") or {}

    # If the record itself is an organization provider (NPI-2)
    org_name = (basic.get("organization_name") or "").strip()
    if org_name:
        return org_name

    # Some entries include authorized official org name
    auth = item.get("authorized_official") or {}
    auth_org = (auth.get("organization_name") or "").strip()
    if auth_org:
        return auth_org

    # Some entries have "authorized_official_organization_name" in other formats (rare)
    for k in ["authorized_official_organization_name", "organization_name"]:
        v = (item.get(k) or "").strip() if isinstance(item.get(k), str) else ""
        if v:
            return v

    return ""


def build_provider_record(item: Dict[str, Any]) -> Dict[str, Any]:
    basic = item.get("basic") or {}
    npi = str(item.get("number") or "")
    enum_date = (basic.get("enumeration_date") or "").strip()

    # Provider type and name
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

    # Clinic / place of work (best effort)
    clinic = clinic_or_place_of_work(item)

    # Specialty labels
    labels = []
    for t in (item.get("taxonomies") or []):
        code = (t.get("code") or "").strip()
        desc = (t.get("desc") or "").strip()
        if code in TAXONOMY_ALLOWLIST:
            labels.append(desc or code)
    taxonomy_text = ", ".join(sorted(set([x for x in labels if x])))

    # Address + phone from location
    addr = pick_location_address(item)
    phone = clean_phone(addr.get("telephone_number") or "")
    address_1 = (addr.get("address_1") or "").strip()
    address_2 = (addr.get("address_2") or "").strip()
    address = (address_1 + (" " + address_2 if address_2 else "")).strip()

    city = (addr.get("city") or "").strip()
    state = (addr.get("state") or "").strip()
    zipc = (addr.get("postal_code") or "").strip()

    return {
        "npi": npi,
        "provider_type": provider_type,
        "name": provider_name,
        "credential": credential,
        "clinic": clinic,
        "taxonomy": taxonomy_text,
        "phone": phone,
        "address": address,
        "city": city,
        "state": state,
        "zip": zipc,
        "enumeration_date": enum_date,
        "years_in_practice_proxy": years_since(enum_date),
    }


def main() -> None:
    all_items: List[Dict[str, Any]] = []
    for city in CITIES:
        all_items.extend(fetch_city(city))

    # Filter + dedupe by NPI
    by_npi: Dict[str, Dict[str, Any]] = {}
    for item in all_items:
        if not provider_matches_taxonomy(item):
            continue
        npi = str(item.get("number") or "")
        if npi:
            by_npi[npi] = item

    providers = [build_provider_record(item) for item in by_npi.values()]

    # Default sort: Endocrinology first, then City, then Name
    def sort_key(p: Dict[str, Any]):
        endo_first = 0 if "endocrinology" in (p.get("taxonomy") or "").lower() else 1
        return (endo_first, p.get("city") or "", p.get("name") or "")

    providers.sort(key=sort_key)

    payload = {
        "generated_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        "count": len(providers),
        "providers": providers,
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


if __name__ == "__main__":
    main()
