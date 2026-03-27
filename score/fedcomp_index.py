"""
FedComp Index scoring algorithm
Joins SBA entities + USASpending awards, computes 4 Index Drivers,
builds Proximity Maps, outputs scored contractor records.

Usage:
    python score/fedcomp_index.py --state NV
"""

import json
import re
import math
import argparse
from datetime import date, datetime
from pathlib import Path
from collections import defaultdict

BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data"

# ─── Weights ──────────────────────────────────────────────────────────────────
W_AWARD_VOLUME   = 0.90  # Log-scaled total $ won - dominant signal
W_AWARD_RECENCY  = 0.10  # Last award recency bucket - still in the game?

TODAY = date.today()


# ─── Utilities ────────────────────────────────────────────────────────────────

def slugify(name):
    s = name.lower()
    s = re.sub(r"[^a-z0-9]+", "-", s)
    return s.strip("-")[:60]


def assign_classes(scored):
    """
    Score-based classes. Fixed thresholds.
    Class 1: 60+ (good), Class 2: 40-59 (warn), Class 3: <40 (bad).
    """
    for r in scored:
        score = r["score"]
        if score >= 60:
            r["posture_class"] = "Class 1"
        elif score >= 40:
            r["posture_class"] = "Class 2"
        else:
            r["posture_class"] = "Class 3"


def score_class_css(score):
    if score >= 60: return "good"
    if score >= 40: return "warn"
    return "bad"


def percentile_normalize(value, all_values, zero_floor=None):
    """
    Normalize a value to 0-100 based on its percentile in the distribution.
    zero_floor: if set, any value <= 0 gets this score instead of percentile rank.
    This prevents zero-award contractors from scoring mid-range just because
    most others also have zero awards.
    """
    if not all_values:
        return 50
    if zero_floor is not None and value <= 0:
        return zero_floor
    # Only normalize against non-zero values when zero_floor is set
    compare_vals = [v for v in all_values if v > 0] if zero_floor is not None else all_values
    if not compare_vals:
        return zero_floor if zero_floor is not None else 0
    sorted_vals = sorted(compare_vals)
    n = len(sorted_vals)
    rank = sum(1 for v in sorted_vals if v <= value)
    # Scale to zero_floor+1 .. 100 range when zero_floor is set
    if zero_floor is not None:
        base = zero_floor + 1
        return round(base + (rank / n) * (100 - base))
    return round((rank / n) * 100)


def days_until(date_str):
    if not date_str:
        return None
    try:
        d = datetime.strptime(date_str[:10], "%Y-%m-%d").date()
        return (d - TODAY).days
    except Exception:
        return None


# ─── Driver functions ─────────────────────────────────────────────────────────

LOG_MIN = 4   # log10($10K)
LOG_MAX = 10  # log10($10B)

def driver_award_volume(awards):
    """
    Log10-scaled total award amount, mapped directly to 0-100.
    $10K -> 0, $1M -> 33, $10M -> 50, $100M -> 67, $1B -> 83, $10B -> 100.
    Not percentile-normalized - a $1B contractor always outscores a $10M one.
    """
    if not awards:
        return 0
    total = sum(a.get("amount", 0) or 0 for a in awards)
    if total <= 0:
        return 0
    log_val = math.log10(total)
    return max(0, min(100, round((log_val - LOG_MIN) / (LOG_MAX - LOG_MIN) * 100)))


def driver_award_recency(awards):
    """
    Bucketed recency of last award. Stable - doesn't decay daily.
    Rewards contractors still actively winning. Doesn't penalize large-contract model.
    Won past 12 months: 100, 1-2yr: 60, 2-3yr: 30, 3-5yr: 10.
    """
    if not awards:
        return 0
    dates = [a.get("start_date", "") for a in awards if a.get("start_date")]
    if not dates:
        return 0
    days_ago = (TODAY - datetime.strptime(max(dates)[:10], "%Y-%m-%d").date()).days
    if days_ago <= 365:
        return 100
    elif days_ago <= 730:
        return 60
    elif days_ago <= 1095:
        return 30
    else:
        return 10


def driver_cert_profile(entity):
    """
    Certification strength: formal SBA certs + registration currency.
    Formal certs: 0-5+ (WOSB, 8(a), HUBZone, VOSB, SDVOSB, etc.)
    Currency: SAM active + no expiring certs
    """
    score = 0
    # Formal certs - each one is a genuine credential
    formal = entity.get("cert_count", 0)
    score += formal * 20  # 20 pts per cert, max 5 = 100

    # SAM active baseline
    if entity.get("sam_active"):
        score += 10

    # Registration health
    expiry = entity.get("earliest_cert_expiry")
    if expiry:
        days = days_until(expiry)
        if days is not None:
            if days > 365:
                score += 10
            elif days > 90:
                score += 5
            elif days < 0:
                score -= 20  # Expired cert is a liability

    return max(0, score)


def driver_posture(entity, awards):
    """
    Profile completeness + cert/award activity signal.
    Internal AFPR boundary score wrapper.
    """
    score = 0

    # Profile completeness (0-100 from sba.py)
    score += entity.get("profile_completeness", 0) * 0.5  # max 50

    # Cert + recent award activity
    has_cert = entity.get("cert_count", 0) > 0 or entity.get("self_cert_count", 0) > 0
    has_recent = any(a.get("recency_weight", 1.0) >= 2.0 for a in awards)

    if has_cert and has_recent:
        score += 50
    elif has_cert:
        score += 25
    elif has_recent:
        score += 20

    return min(100, score)


# ─── Proximity Map ────────────────────────────────────────────────────────────

def build_proximity_indexes(scored):
    """Precompute NAICS->UEIs and PSC->UEIs reverse indexes."""
    naics_to_ueis = defaultdict(set)
    psc_to_ueis = defaultdict(set)
    uei_to_record = {}
    for r in scored:
        uei = r["uei"]
        uei_to_record[uei] = r
        for n in r.get("_validated_naics", []):
            naics_to_ueis[n].add(uei)
        for p in r.get("_validated_pscs", []):
            psc_to_ueis[p].add(uei)
    return naics_to_ueis, psc_to_ueis, uei_to_record


def build_proximity_map(target_uei, target_record, naics_idx, psc_idx, uei_map, top_n=6):
    """Find real competitors: shared codes weighted by rarity AND scale similarity.
    Two contractors sharing a rare PSC at similar dollar volume = genuine competitors.
    Same code but 100x apart in scale = not competing on the same solicitations."""
    target_naics = set(target_record.get("_validated_naics", []))
    target_pscs = set(target_record.get("_validated_pscs", []))

    if not target_naics and not target_pscs:
        return []

    target_vol = target_record.get("awards_5yr_m", 0) * 1e6

    # Inverse frequency squared: rare codes explode in value (convex)
    candidates = defaultdict(float)
    for n in target_naics:
        members = naics_idx.get(n, set())
        weight = 1.0 / (len(members) ** 2) if members else 0
        for uei in members:
            if uei != target_uei:
                candidates[uei] += weight
    for p in target_pscs:
        members = psc_idx.get(p, set())
        weight = 2.0 / (len(members) ** 2) if members else 0
        for uei in members:
            if uei != target_uei:
                candidates[uei] += weight

    # Scale similarity squared: mismatched scale = not competitors (convex)
    matches = []
    for uei, overlap_score in candidates.items():
        if overlap_score <= 0:
            continue
        cand_vol = uei_map.get(uei, {}).get("awards_5yr_m", 0) * 1e6
        if target_vol > 0 and cand_vol > 0:
            ratio = min(target_vol, cand_vol) / max(target_vol, cand_vol)
            score = overlap_score * (ratio ** 2)
        else:
            score = overlap_score * 0.01
        matches.append((score, uei))

    matches.sort(key=lambda x: x[0], reverse=True)
    top = matches[:top_n]

    return [
        {
            "slug": uei_map[uei]["slug"],
            "name": uei_map[uei]["name"],
            "naics": uei_map[uei]["naics_primary"],
            "certifications": uei_map[uei].get("certifications", []),
            "score": uei_map[uei]["score"],
            "posture_class": uei_map[uei]["posture_class"],
            "score_class": score_class_css(uei_map[uei]["score"]),
        }
        for _, uei in top
    ]


# ─── Main scoring ─────────────────────────────────────────────────────────────

def score_all(entities, recipient_index, performed_index, state_code="NV", state_name="Nevada"):
    """
    Score all entities. Two passes:
    Pass 1: compute raw driver values
    Pass 2: normalize all drivers against the full distribution, compute final scores
    """

    print(f"Scoring {len(entities)} entities...")

    # ─── Pass 1: raw values ───────────────────────────────────────────────────
    raw_records = []
    cutoff = date.today().replace(year=date.today().year - 5)

    for entity in entities:
        uei = entity.get("uei", "")
        cage = entity.get("cage", "")

        # Join awards by UEI
        awards = recipient_index.get(uei, [])

        # Only count awards with start_date within the past 5 years for scoring.
        # USASpending filters on action date (modifications) but returns original
        # start_date, so old contracts with recent modifications can slip through.
        def _in_window(a):
            sd = a.get("start_date", "")
            if not sd:
                return False
            try:
                return datetime.strptime(sd[:10], "%Y-%m-%d").date() >= cutoff
            except Exception:
                return False

        scored_awards = [a for a in awards if _in_window(a)]

        # Validated NAICS and PSC from actual scored awards
        validated_naics = list(set(a["naics"] for a in scored_awards if a.get("naics")))
        validated_pscs = list(set(a["psc"] for a in scored_awards if a.get("psc")))

        raw_records.append({
            "entity": entity,
            "awards": awards,           # full list for display
            "scored_awards": scored_awards,  # 5yr window for scoring
            "uei": uei,
            "validated_naics": validated_naics,
            "validated_pscs": validated_pscs,
            "raw_volume":  driver_award_volume(scored_awards),
            "raw_recency": driver_award_recency(scored_awards),
        })

    # ─── Pass 2: normalize and compute final scores ───────────────────────────

    all_volume  = [r["raw_volume"]  for r in raw_records]
    all_recency = [r["raw_recency"] for r in raw_records]

    scored = []

    for r in raw_records:
        entity = r["entity"]
        awards = r["awards"]
        scored_awards = r["scored_awards"]

        d1 = r["raw_volume"]   # already 0-100, no normalization
        d2 = r["raw_recency"]  # already 0-100, no normalization

        raw_score = (
            d1 * W_AWARD_VOLUME +
            d2 * W_AWARD_RECENCY
        )

        final_score = min(100, max(0, round(raw_score)))

        # Format awards for display
        contracts_display = [
            {
                "date": a["start_date"][:7] if a.get("start_date") else "-",
                "agency": a.get("agency", "-"),
                "naics": a.get("naics", "-"),
                "psc": a.get("psc", "-"),
                "value_fmt": f"{a.get('amount', 0):,.0f}",
            }
            for a in sorted(awards, key=lambda x: x.get("start_date", ""), reverse=True)[:20]
        ]

        total_awards_m = round(sum(a.get("amount", 0) for a in scored_awards) / 1_000_000, 1)
        award_count = len(scored_awards)

        active_contracts = sum(
            1 for a in scored_awards
            if a.get("end_date") and days_until(a["end_date"]) and days_until(a["end_date"]) > 0
        )

        last_award_date = ""
        if awards:
            sorted_awards = sorted(awards, key=lambda x: x.get("start_date", ""), reverse=True)
            if sorted_awards[0].get("start_date"):
                last_award_date = sorted_awards[0]["start_date"][:7]

        name = entity.get("name") or entity.get("dba") or "Unknown"

        record = {
            # Identity
            "slug": slugify(name),
            "name": name,
            "uei": r["uei"],
            "cage": entity.get("cage", ""),
            "naics_primary": entity.get("naics_primary", ""),
            "naics_codes": entity.get("naics_all", [])[:6],
            "certifications": entity.get("active_certs", []),
            "state_name": state_name,
            "state_slug": state_code.lower(),

            # Score
            "score": final_score,
            "score_class": score_class_css(final_score),
            "posture_class": "",  # assigned after sort

            # Awards
            "awards_5yr_m": total_awards_m,
            "award_count": award_count,
            "active_contracts": active_contracts,
            "last_award_date": last_award_date,
            "contracts": contracts_display,

            # Index Drivers (for display)
            "index_drivers": [
                {"name": "Award Volume",  "score": d1, "vs_avg": _vs_avg(d1)},
                {"name": "Award Recency", "score": d2, "vs_avg": _vs_avg(d2)},
            ],

            # Proximity (built in second pass)
            "proximity": [],

            # Internal - used for proximity map, stripped before final output
            "_validated_naics": r["validated_naics"],
            "_validated_pscs": r["validated_pscs"],

            # Display
            "rank": 0,
            "total": 0,  # set after filtering
        }

        # Add score_class to each index driver
        for d in record["index_drivers"]:
            d["score_class"] = score_class_css(d["score"])

        # Skip contractors with no awards or negligible volume (<$1K)
        if record["award_count"] == 0 or record["awards_5yr_m"] < 0.001:
            continue

        scored.append(record)

    # Sort by score descending
    scored.sort(key=lambda x: x["score"], reverse=True)

    # Resolve slug collisions - append suffix to duplicates
    slug_counts = {}
    for r in scored:
        s = r["slug"]
        if s in slug_counts:
            slug_counts[s] += 1
            r["slug"] = f"{s}-{slug_counts[s]}"
            print(f"Slug collision: {s} -> {r['slug']} ({r['name']})")
        else:
            slug_counts[s] = 1

    # Assign ranks, totals, and classes
    total = len(scored)
    for i, r in enumerate(scored):
        r["rank"] = i + 1
        r["total"] = total
    assign_classes(scored)

    # Build Proximity Maps
    print("Building Proximity Maps...")
    naics_idx, psc_idx, uei_map = build_proximity_indexes(scored)
    for record in scored:
        record["proximity"] = build_proximity_map(record["uei"], record, naics_idx, psc_idx, uei_map)

    # Strip internal fields
    for r in scored:
        r.pop("_validated_naics", None)
        r.pop("_validated_pscs", None)

    return scored


def _vs_avg(score):
    """Format score relative to 50 (the median)."""
    diff = score - 50
    if diff > 0:
        return f"+{diff}"
    return str(diff)


# ─── Out-of-state stubs ───────────────────────────────────────────────────────

def build_stubs(performed_index, state_ueis, state_code="NV", min_awards=2, min_amount=250_000):
    """
    Generate stub records for out-of-state contractors who won work in state_code.
    Threshold: 2+ awards and $250k+ total in the target state.
    """
    state_code = state_code.upper()
    stubs = []
    for uei, awards in performed_index.items():
        if uei in state_ueis:
            continue  # Already have a full Dossier

        # Filter to awards performed in state_code by non-state contractors
        nv_awards = [
            a for a in awards
            if a.get("performance_state") == state_code and a.get("recipient_state", "") != state_code
        ]

        if len(nv_awards) < min_awards:
            continue

        total = sum(a.get("amount", 0) for a in nv_awards)
        if total < min_amount:
            continue

        name = nv_awards[0].get("recipient_name", "Unknown Contractor")
        pscs = list(set(a.get("psc", "") for a in nv_awards if a.get("psc")))
        recipient_state = nv_awards[0].get("recipient_state", "Unknown")

        stubs.append({
            "slug": "stub-" + slugify(name),
            "name": name,
            "uei": uei,
            "is_stub": True,
            "recipient_state": recipient_state,
            "nv_award_count": len(nv_awards),
            "nv_total_m": round(total / 1_000_000, 1),
            "psc_codes": pscs[:5],
        })

    stubs.sort(key=lambda x: x["nv_total_m"], reverse=True)
    print(f"Generated {len(stubs)} out-of-state stub records.")
    return stubs


# ─── Entry point ─────────────────────────────────────────────────────────────

def run(state):
    STATE_NAMES = {"NV": "Nevada", "TX": "Texas", "CA": "California", "AZ": "Arizona", "FL": "Florida"}
    state_lower = state.lower()
    state_name = STATE_NAMES.get(state.upper(), state.upper())

    entities_path  = DATA_DIR / f"{state_lower}_entities.json"
    recipient_path = DATA_DIR / f"{state_lower}_awards_recipient.json"
    performed_path = DATA_DIR / f"{state_lower}_awards_performed.json"

    with open(entities_path) as f:
        entities = json.load(f)
    with open(recipient_path) as f:
        recipient_index = json.load(f)
    with open(performed_path) as f:
        performed_index = json.load(f)

    scored = score_all(entities, recipient_index, performed_index, state_code=state.upper(), state_name=state_name)

    out_path = DATA_DIR / f"{state_lower}_scored.json"
    with open(out_path, "w") as f:
        json.dump(scored, f, indent=2)
    print(f"\nScored {len(scored)} contractors -> {out_path}")

    # Snapshot + movers
    snapshot_path = DATA_DIR / f"{state_lower}_snapshot.json"
    prev_snapshot = {}
    if snapshot_path.exists():
        with open(snapshot_path) as f:
            prev_snapshot = json.load(f)

    movers_rising = []
    movers_declining = []

    if prev_snapshot:
        for c in scored:
            uei = c.get("uei", "")
            if uei and uei in prev_snapshot:
                prev = prev_snapshot[uei]
                delta = c["score"] - prev["score"]
                if abs(delta) >= 3:
                    entry = {
                        "name": c["name"],
                        "slug": c["slug"],
                        "score": c["score"],
                        "score_class": score_class_css(c["score"]),
                        "prev_score": prev["score"],
                        "delta": delta,
                        "posture_class": c["posture_class"],
                        "prev_class": prev.get("posture_class", ""),
                        "class_changed": c["posture_class"] != prev.get("posture_class", ""),
                    }
                    if delta > 0:
                        movers_rising.append(entry)
                    else:
                        movers_declining.append(entry)
        movers_rising.sort(key=lambda x: x["delta"], reverse=True)
        movers_declining.sort(key=lambda x: x["delta"])
        movers_rising = movers_rising[:8]
        movers_declining = movers_declining[:8]

    movers_data = {"rising": movers_rising, "declining": movers_declining}
    with open(DATA_DIR / f"{state_lower}_movers.json", "w") as f:
        json.dump(movers_data, f, indent=2)

    # Save new snapshot
    new_snapshot = {
        c["uei"]: {"score": c["score"], "posture_class": c["posture_class"]}
        for c in scored if c.get("uei")
    }
    with open(snapshot_path, "w") as f:
        json.dump(new_snapshot, f)

    # Score distribution report
    buckets = {"Class 1": 0, "Class 2": 0, "Class 3": 0}
    for r in scored:
        rc = r["posture_class"]
        buckets[rc] = buckets.get(rc, 0) + 1
    print("\nScore distribution:")
    for cls, count in buckets.items():
        print(f"  {cls}: {count}")

    # Out-of-state stubs
    state_ueis = set(e.get("uei") for e in entities)
    stubs = build_stubs(performed_index, state_ueis, state_code=state)
    stubs_path = DATA_DIR / f"{state_lower}_stubs.json"
    with open(stubs_path, "w") as f:
        json.dump(stubs, f, indent=2)

    print(f"\nRun next: python generate/build.py --state {state}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--state", default="NV")
    args = parser.parse_args()
    run(args.state)
