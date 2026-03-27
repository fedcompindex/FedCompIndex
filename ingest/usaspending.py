"""
USASpending awards ingestion — Nevada
Two pulls:
  1. Recipient in Nevada  — contractor's own awards (goes on their Dossier)
  2. Performed in Nevada  — work done in NV regardless of contractor origin
                           (shows out-of-state competition, seeds stub pages)

Saves:
  data/raw/usaspending_nv_recipient_raw.json
  data/raw/usaspending_nv_performed_raw.json
  data/nv_awards_recipient.json   — indexed by UEI
  data/nv_awards_performed.json   — indexed by UEI (includes out-of-state)

Usage:
    python ingest/usaspending.py --state NV
"""

import json
import time
import argparse
from datetime import date, datetime, timedelta
from pathlib import Path

import requests

BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data"
RAW_DIR = DATA_DIR / "raw"

USASPENDING_URL = "https://api.usaspending.gov/api/v2/search/spending_by_award/"

HEADERS = {
    "accept": "application/json",
    "content-type": "application/json",
    "origin": "https://www.usaspending.gov",
    "referer": "https://www.usaspending.gov/",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "x-requested-with": "USASpendingFrontend",
}

FIELDS = [
    "Award ID",
    "Recipient Name",
    "Recipient UEI",
    "Award Amount",
    "Total Outlays",
    "Description",
    "Contract Award Type",
    "Awarding Agency",
    "Awarding Sub Agency",
    "Start Date",
    "End Date",
    "NAICS",
    "PSC",
    "Recipient Location",
    "Primary Place of Performance",
    "prime_award_recipient_id",
]

# Default: 5 year range (overridden by --days flag for incremental mode)
DEFAULT_YEARS = 5
START_DATE = (date.today().replace(year=date.today().year - DEFAULT_YEARS)).strftime("%Y-%m-%d")
END_DATE = date.today().strftime("%Y-%m-%d")


def build_payload_recipient(state, page):
    """Awards where the recipient is based in the state."""
    return {
        "filters": {
            "time_period": [{"start_date": START_DATE, "end_date": END_DATE}],
            "award_type_codes": ["A", "B", "C", "D"],
            "recipient_locations": [{"country": "USA", "state": state}],
        },
        "fields": FIELDS,
        "page": page,
        "limit": 100,
        "sort": "Award Amount",
        "order": "desc",
        "spending_level": "awards",
    }


def build_payload_performed(state, page):
    """Awards where the work was performed in the state."""
    return {
        "filters": {
            "time_period": [{"start_date": START_DATE, "end_date": END_DATE}],
            "award_type_codes": ["A", "B", "C", "D"],
            "place_of_performance_locations": [{"country": "USA", "state": state}],
        },
        "fields": FIELDS,
        "page": page,
        "limit": 100,
        "sort": "Award Amount",
        "order": "desc",
        "spending_level": "awards",
    }


def fetch_awards(payload_fn, state, label):
    all_raw = []
    page = 1
    total_seen = 0

    print(f"Fetching USASpending awards ({label}) for {state}...")

    max_retries = 3

    while True:
        payload = payload_fn(state, page)

        data = None
        for attempt in range(max_retries):
            try:
                resp = requests.post(USASPENDING_URL, json=payload, headers=HEADERS, timeout=60)
                resp.raise_for_status()
                data = resp.json()
                break
            except Exception as e:
                print(f"  Error on page {page} (attempt {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(5 * (attempt + 1))

        if data is None:
            print(f"  Failed page {page} after {max_retries} attempts, stopping.")
            break

        results = data.get("results", [])
        if not results:
            print(f"  No results on page {page}, stopping.")
            break

        all_raw.extend(results)
        total_seen += len(results)

        has_next = data.get("page_metadata", {}).get("hasNext", False)
        print(f"  Page {page}: {len(results)} awards (total: {total_seen}, has_next: {has_next})")

        # Checkpoint save
        raw_path = RAW_DIR / f"usaspending_{state.lower()}_{label}_raw.json"
        with open(raw_path, "w") as f:
            json.dump(all_raw, f)

        if not has_next:
            break

        page += 1
        time.sleep(0.3)

    print(f"Fetched {total_seen} total awards ({label}).")
    return all_raw


def _extract_code(field):
    """NAICS and PSC fields may be dicts like {'code': '541512', 'description': '...'}"""
    if not field:
        return ""
    if isinstance(field, dict):
        return str(field.get("code", "") or "")
    return str(field)


def normalize_award(raw):
    """Normalize a single award record."""
    amount = raw.get("Award Amount") or 0
    try:
        amount = float(amount)
    except (TypeError, ValueError):
        amount = 0.0

    start = raw.get("Start Date", "")
    end = raw.get("End Date", "")

    # Recency weight: awards in last 12mo = 3x, 12-24mo = 2x, 24-36mo = 1.5x, older = 1x
    recency_weight = 1.0
    if start:
        try:
            award_date = datetime.strptime(start[:10], "%Y-%m-%d").date()
            days_ago = (date.today() - award_date).days
            if days_ago <= 365:
                recency_weight = 3.0
            elif days_ago <= 730:
                recency_weight = 2.0
            elif days_ago <= 1095:
                recency_weight = 1.5
        except Exception:
            pass

    recipient_loc = raw.get("Recipient Location", {}) or {}
    perf_loc = raw.get("Primary Place of Performance", {}) or {}

    return {
        "award_id": raw.get("Award ID", ""),
        "uei": raw.get("Recipient UEI", ""),
        "recipient_name": raw.get("Recipient Name", ""),
        "amount": amount,
        "description": raw.get("Description", ""),
        "agency": raw.get("Awarding Agency", ""),
        "sub_agency": raw.get("Awarding Sub Agency", ""),
        "naics": _extract_code(raw.get("NAICS")),
        "psc": _extract_code(raw.get("PSC")),
        "start_date": start,
        "end_date": end,
        "recipient_state": recipient_loc.get("state_code", ""),
        "performance_state": perf_loc.get("state_code", ""),
        "recency_weight": recency_weight,
        "weighted_amount": amount * recency_weight,
    }


def index_by_uei(awards):
    """Group normalized awards by UEI."""
    index = {}
    for a in awards:
        uei = a["uei"]
        if not uei:
            continue
        if uei not in index:
            index[uei] = []
        index[uei].append(a)
    return index


def merge_awards(existing_indexed, new_indexed):
    """Merge new awards into existing dataset by UEI. Deduplicates by award_id."""
    merged = dict(existing_indexed)
    for uei, new_awards in new_indexed.items():
        if uei not in merged:
            merged[uei] = new_awards
        else:
            existing_ids = {a.get("award_id") for a in merged[uei]}
            for a in new_awards:
                if a.get("award_id") not in existing_ids:
                    merged[uei].append(a)
    return merged


def run(state, days=None):
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    if days:
        start_date = (date.today() - timedelta(days=days)).strftime("%Y-%m-%d")
        print(f"Incremental mode: pulling last {days} days only")
    else:
        start_date = (date.today().replace(year=date.today().year - DEFAULT_YEARS)).strftime("%Y-%m-%d")

    # Override the start date in payload builders via closure
    orig_recipient = build_payload_recipient
    orig_performed = build_payload_performed

    def patched_recipient(st, pg):
        p = orig_recipient(st, pg)
        p["filters"]["time_period"] = [{"start_date": start_date, "end_date": END_DATE}]
        return p

    def patched_performed(st, pg):
        p = orig_performed(st, pg)
        p["filters"]["time_period"] = [{"start_date": start_date, "end_date": END_DATE}]
        return p

    # Pull 1: recipient in state
    recipient_raw = fetch_awards(patched_recipient, state, "recipient")
    raw_path = RAW_DIR / f"usaspending_{state.lower()}_recipient_raw.json"
    with open(raw_path, "w") as f:
        json.dump(recipient_raw, f, indent=2)

    recipient_normalized = [normalize_award(r) for r in recipient_raw]
    recipient_indexed = index_by_uei(recipient_normalized)

    # If incremental, merge into existing data
    out_path = DATA_DIR / f"{state.lower()}_awards_recipient.json"
    if days and out_path.exists():
        with open(out_path) as f:
            existing = json.load(f)
        recipient_indexed = merge_awards(existing, recipient_indexed)
        print(f"Merged incremental data into existing recipient awards")

    with open(out_path, "w") as f:
        json.dump(recipient_indexed, f, indent=2)
    print(f"Recipient awards: {out_path} ({len(recipient_indexed)} unique contractors)")

    # Pull 2: performed in state
    performed_raw = fetch_awards(patched_performed, state, "performed")
    raw_path = RAW_DIR / f"usaspending_{state.lower()}_performed_raw.json"
    with open(raw_path, "w") as f:
        json.dump(performed_raw, f, indent=2)

    performed_normalized = [normalize_award(r) for r in performed_raw]
    performed_indexed = index_by_uei(performed_normalized)

    out_path = DATA_DIR / f"{state.lower()}_awards_performed.json"
    if days and out_path.exists():
        with open(out_path) as f:
            existing = json.load(f)
        performed_indexed = merge_awards(existing, performed_indexed)
        print(f"Merged incremental data into existing performed awards")

    with open(out_path, "w") as f:
        json.dump(performed_indexed, f, indent=2)
    print(f"Performed awards: {out_path} ({len(performed_indexed)} unique contractors)")

    print(f"\nDone. Run next: python score/fedcomp_index.py --state {state}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--state", default="NV")
    parser.add_argument("--days", type=int, default=None,
                        help="Only pull last N days of awards (incremental mode)")
    args = parser.parse_args()
    run(args.state, days=args.days)
