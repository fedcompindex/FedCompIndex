"""
USASpending awards ingestion -- contractors based in a given state.

Splits into yearly windows (all parallel), then recursively halves any
window that hits the API's 10K result cap. Adapts to any data volume
without code changes. Nevada needs 5 parallel year pulls. California
might need sub-year splits. Handled automatically.

Saves:
  data/raw/usaspending_{state}_recipient_raw.json
  data/{state}_awards_recipient.json   -- indexed by UEI

Usage:
    python ingest/usaspending.py --state NV
"""

import json
import time
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
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

DEFAULT_YEARS = 5
BATCH_WORKERS = 10
API_CAP = 9900


# --- Fetching infrastructure ---------------------------------------------------

def _build_payload(state, page, start_date, end_date):
    return {
        "filters": {
            "time_period": [{"start_date": start_date, "end_date": end_date}],
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


def _fetch_page(state, page, start_date, end_date):
    payload = _build_payload(state, page, start_date, end_date)
    for attempt in range(3):
        try:
            resp = requests.post(USASPENDING_URL, json=payload, headers=HEADERS, timeout=60)
            resp.raise_for_status()
            return (page, resp.json())
        except Exception as e:
            if attempt < 2:
                time.sleep(2 * (attempt + 1))
    return (page, None)


def _midpoint(start_str, end_str):
    s = datetime.strptime(start_str, "%Y-%m-%d").date()
    e = datetime.strptime(end_str, "%Y-%m-%d").date()
    mid = s + (e - s) / 2
    return mid.strftime("%Y-%m-%d")


def _fetch_all_pages(state, start_date, end_date):
    """Paginate through all results in a single window using parallel batches."""
    _, first = _fetch_page(state, 1, start_date, end_date)
    if not first or not first.get("results"):
        return []

    all_raw = list(first["results"])
    if not first.get("page_metadata", {}).get("hasNext", False):
        return all_raw

    page = 2
    with ThreadPoolExecutor(max_workers=BATCH_WORKERS) as pool:
        while True:
            batch_end = page + BATCH_WORKERS
            futures = {
                pool.submit(_fetch_page, state, p, start_date, end_date): p
                for p in range(page, batch_end)
            }

            results = {}
            for f in as_completed(futures):
                p, data = f.result()
                results[p] = data

            stop = False
            for p in range(page, batch_end):
                data = results.get(p)
                if not data:
                    stop = True
                    break
                awards = data.get("results", [])
                if not awards:
                    stop = True
                    break
                all_raw.extend(awards)
                if not data.get("page_metadata", {}).get("hasNext", False):
                    stop = True
                    break

            if stop:
                break
            page = batch_end

    return all_raw


def _fetch_window(state, start_date, end_date):
    """
    Fetch a single time window. If result count hits the API cap,
    split in half and recurse both halves in parallel.
    """
    results = _fetch_all_pages(state, start_date, end_date)

    if len(results) >= API_CAP:
        mid = _midpoint(start_date, end_date)
        if mid == start_date or mid == end_date:
            print(f"  {start_date}: {len(results)} awards (single day, can't split)")
            return results

        print(f"  {start_date} to {end_date}: {len(results)} awards (CAPPED), splitting at {mid}")
        with ThreadPoolExecutor(max_workers=2) as pool:
            f1 = pool.submit(_fetch_window, state, start_date, mid)
            f2 = pool.submit(_fetch_window, state, mid, end_date)
            return f1.result() + f2.result()

    print(f"  {start_date} to {end_date}: {len(results)} awards")
    return results


def fetch_awards(state, start_date, end_date):
    """
    Split into yearly windows, run all in parallel.
    Each window self-splits if it hits the API cap.
    """
    s = datetime.strptime(start_date, "%Y-%m-%d").date()
    e = datetime.strptime(end_date, "%Y-%m-%d").date()

    # Build yearly windows
    windows = []
    cursor = s
    while cursor < e:
        window_end = min(date(cursor.year + 1, cursor.month, cursor.day), e)
        windows.append((cursor.strftime("%Y-%m-%d"), window_end.strftime("%Y-%m-%d")))
        cursor = window_end

    print(f"  {len(windows)} yearly windows, all parallel")

    with ThreadPoolExecutor(max_workers=len(windows)) as pool:
        futures = [
            pool.submit(_fetch_window, state, ws, we)
            for ws, we in windows
        ]
        all_raw = []
        for fut in futures:
            all_raw.extend(fut.result())

    return all_raw


# --- Normalization -------------------------------------------------------------

def _extract_code(field):
    if not field:
        return ""
    if isinstance(field, dict):
        return str(field.get("code", "") or "")
    return str(field)


def normalize_award(raw):
    amount = raw.get("Award Amount") or 0
    try:
        amount = float(amount)
    except (TypeError, ValueError):
        amount = 0.0

    start = raw.get("Start Date", "")
    end = raw.get("End Date", "")

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
        "award_type": raw.get("Contract Award Type", ""),
        "recency_weight": recency_weight,
        "weighted_amount": amount * recency_weight,
    }


def index_by_uei(awards):
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


# --- Entry point ---------------------------------------------------------------

def run(state, days=None):
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    end_date = date.today().strftime("%Y-%m-%d")
    if days:
        start_date = (date.today() - timedelta(days=days)).strftime("%Y-%m-%d")
        print(f"Incremental mode: last {days} days")
    else:
        start_date = date.today().replace(year=date.today().year - DEFAULT_YEARS).strftime("%Y-%m-%d")

    print(f"Fetching USASpending awards for {state} ({start_date} to {end_date})...")
    all_raw = fetch_awards(state, start_date, end_date)

    # Deduplicate by Award ID (boundary dates can appear in both halves of a split)
    seen = set()
    deduped = []
    for r in all_raw:
        aid = r.get("Award ID", "")
        if aid and aid not in seen:
            seen.add(aid)
            deduped.append(r)
        elif not aid:
            deduped.append(r)

    if len(all_raw) != len(deduped):
        print(f"Deduplicated: {len(all_raw)} -> {len(deduped)}")
    all_raw = deduped

    print(f"Total: {len(all_raw)} unique awards")

    # Save raw
    raw_path = RAW_DIR / f"usaspending_{state.lower()}_recipient_raw.json"
    with open(raw_path, "w") as f:
        json.dump(all_raw, f, indent=2)

    # Normalize + index
    normalized = [normalize_award(r) for r in all_raw]
    indexed = index_by_uei(normalized)

    out_path = DATA_DIR / f"{state.lower()}_awards_recipient.json"
    if days and out_path.exists():
        with open(out_path) as f:
            existing = json.load(f)
        indexed = merge_awards(existing, indexed)
        print(f"Merged incremental data into existing awards")

    with open(out_path, "w") as f:
        json.dump(indexed, f, indent=2)
    print(f"Awards: {out_path} ({len(indexed)} unique contractors)")

    print(f"\nDone. Run next: python score/fedcomp_index.py --state {state}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--state", default="NV")
    parser.add_argument("--days", type=int, default=None,
                        help="Only pull last N days (incremental mode)")
    args = parser.parse_args()
    run(args.state, days=args.days)
