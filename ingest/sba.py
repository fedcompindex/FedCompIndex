"""
SBA Certifications API ingestion — Nevada
Pulls all certified small businesses registered in NV.

Saves:
  data/raw/sba_nv_raw.json     — raw API responses
  data/nv_entities.json        — normalized, keyed by UEI

Usage:
    python ingest/sba.py --state NV
"""

import json
import time
import argparse
from pathlib import Path

import requests

BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data"
RAW_DIR = DATA_DIR / "raw"

SBA_URL = "https://search.certifications.sba.gov/_api/v2/search"

CERT_FILTERS = [
    {"label": "8(a) Business Development", "value": "1"},
    {"label": "HUBZone", "value": "2"},
    {"label": "WOSB", "value": "5"},
    {"label": "EDWOSB", "value": "6"},
    {"label": "VOSB", "value": "7"},
    {"label": "SDVOSB", "value": "8"},
]

HEADERS = {
    "accept": "*/*",
    "content-type": "application/json",
    "origin": "https://search.certifications.sba.gov",
    "referer": "https://search.certifications.sba.gov/advanced",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
}


def build_payload(state_label, state_value):
    return {
        "searchProfiles": {"searchTerm": ""},
        "location": {
            "states": [{"label": state_label, "value": state_value}],
            "zipCodes": [],
            "counties": [],
            "districts": [],
            "msas": [],
        },
        "sbaCertifications": {
            "activeCerts": [],
            "isPreviousCert": False,
            "operatorType": "Or",
        },
        "naics": {"codes": [], "isPrimary": False, "operatorType": "Or"},
        "selfCertifications": {"certifications": [], "operatorType": "Or"},
        "keywords": {"list": [], "operatorType": "Or"},
        "lastUpdated": {"date": {"label": "Anytime", "value": "anytime"}},
        "samStatus": {"isActiveSAM": True},
        "qualityAssuranceStandards": {"qas": []},
        "bondingLevels": {
            "constructionIndividual": "",
            "constructionAggregate": "",
            "serviceIndividual": "",
            "serviceAggregate": "",
        },
        "businessSize": {"relationOperator": "at-least", "numberOfEmployees": ""},
        "annualRevenue": {"relationOperator": "at-least", "annualGrossRevenue": ""},
        "entityDetailId": "",
    }


def normalize_entity(raw):
    """Extract only the fields we need for scoring."""
    certs = raw.get("certs", [])
    active_certs = [c["name"] for c in certs if c.get("active")]

    # Cert expiry — find earliest expiring active cert
    from datetime import datetime
    earliest_expiry = None
    for c in certs:
        if c.get("active") and c.get("exitDate"):
            try:
                d = datetime.strptime(c["exitDate"], "%Y-%m-%d")
                if earliest_expiry is None or d < earliest_expiry:
                    earliest_expiry = d
            except Exception:
                pass

    # Profile completeness score (0-100)
    completeness = 0
    if raw.get("capabilities_narrative"): completeness += 30
    if raw.get("website"): completeness += 15
    if raw.get("keywords"): completeness += 15
    if raw.get("current_principals"): completeness += 20
    if raw.get("phone"): completeness += 10
    if raw.get("email"): completeness += 10

    return {
        "uei": raw.get("uei", ""),
        "cage": raw.get("cage_code", ""),
        "name": raw.get("legal_business_name", raw.get("dba_name", "")),
        "dba": raw.get("dba_name", ""),
        "state": raw.get("state", ""),
        "city": raw.get("city", ""),
        "county": raw.get("county", ""),
        "zip": raw.get("zipcode", ""),
        "address": raw.get("address_1", ""),
        "phone": raw.get("phone", ""),
        "email": raw.get("email", ""),
        "website": raw.get("website", ""),
        "naics_primary": raw.get("naics_primary", ""),
        "naics_all": raw.get("naics_all_codes", []),
        "naics_small": raw.get("naics_small_codes", []),
        "active_certs": active_certs,
        "cert_count": len(active_certs),
        "self_cert_count": len(raw.get("meili_self_certifications", [])),
        "certs_detail": certs,
        "earliest_cert_expiry": earliest_expiry.isoformat() if earliest_expiry else None,
        "sam_active": raw.get("sam_extract_code") == "A",
        "year_established": raw.get("year_established"),
        "profile_completeness": completeness,
        "capabilities_narrative": raw.get("capabilities_narrative", ""),
        "keywords": raw.get("keywords", []),
        "congressional_district": raw.get("concat_state_congressional_district", ""),
    }


def fetch_state(state_abbr):
    state_map = {
        "NV": ("Nevada (NV)", "NV - Nevada"),
        "AZ": ("Arizona (AZ)", "AZ - Arizona"),
        "TX": ("Texas (TX)", "TX - Texas"),
    }
    state_label, state_value = state_map.get(state_abbr, (state_abbr, state_abbr))

    print(f"Fetching SBA entities for {state_abbr}...")

    payload = build_payload(state_label, state_value)
    url = f"{SBA_URL}?page=0"

    max_retries = 3
    for attempt in range(max_retries):
        try:
            resp = requests.post(url, json=payload, headers=HEADERS, timeout=60)
            resp.raise_for_status()
            data = resp.json()
            results = data.get("results", []) if isinstance(data, dict) else data
            print(f"Fetched {len(results)} entities from SBA.")
            return results
        except Exception as e:
            print(f"  SBA error (attempt {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                time.sleep(5 * (attempt + 1))

    print(f"  SBA failed after {max_retries} attempts.")
    return []


def run(state):
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    raw = fetch_state(state)

    # Save raw
    raw_path = RAW_DIR / f"sba_{state.lower()}_raw.json"
    with open(raw_path, "w") as f:
        json.dump(raw, f, indent=2)
    print(f"Raw saved: {raw_path} ({len(raw)} records)")

    # Normalize — deduplicate by UEI
    seen_ueis = set()
    normalized = []
    for r in raw:
        entity = normalize_entity(r)
        uei = entity["uei"]
        if uei and uei not in seen_ueis:
            seen_ueis.add(uei)
            normalized.append(entity)
        elif not uei:
            # Keep entities without UEI — join on CAGE later
            normalized.append(entity)

    out_path = DATA_DIR / f"{state.lower()}_entities.json"
    with open(out_path, "w") as f:
        json.dump(normalized, f, indent=2)
    print(f"Normalized: {out_path} ({len(normalized)} entities)")

    # Push to backup repo
    push_to_data_repo(state)

    return normalized


def push_to_data_repo(state):
    import subprocess
    data_repo = BASE_DIR / "data"
    try:
        result = subprocess.run(
            ["git", "-C", str(BASE_DIR), "add", "data/"],
            capture_output=True, text=True
        )
        subprocess.run(
            ["git", "-C", str(BASE_DIR), "commit", "-m", f"ingest: sba {state.lower()} entities"],
            capture_output=True, text=True
        )
        print("Data committed locally. Push to FedComp-Data manually or configure remote.")
    except Exception as e:
        print(f"Backup skipped: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--state", default="NV")
    args = parser.parse_args()
    run(args.state)
