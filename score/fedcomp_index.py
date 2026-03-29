"""
FedComp Index v1.1 scoring algorithm
Joins SBA entities + USASpending awards, classifies contractors into 4 Posture
Classes using two-axis thresholds (volume x frequency; volume = all award dollars,
frequency = base contracts only),
builds Proximity Maps, outputs classified contractor records.

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

# ─── Classification Thresholds ────────────────────────────────────────────────
# Frequency counts only independent competitive wins (definitive contracts + purchase orders).
# BPA calls and delivery orders count toward volume but not frequency.
BASE_TYPES = {"DEFINITIVE CONTRACT", "PURCHASE ORDER"}
VOL_THRESHOLD  = 5_000_000   # $5M in total award dollars over 5 years
FREQ_THRESHOLD = 3            # 3 distinct base contracts over 5 years

TODAY = date.today()


# ─── Utilities ────────────────────────────────────────────────────────────────

def slugify(name):
    s = name.lower()
    s = re.sub(r"[^a-z0-9]+", "-", s)
    return s.strip("-")[:60]


def assign_classes(scored):
    """
    Two-axis classification: volume x frequency.
    Class 1: high vol + high freq (systematic winners)
    Class 2: high vol + low freq (concentrated risk)
    Class 3: low vol + high freq (growth pipeline)
    Class 4: low vol + low freq (entry level)
    """
    for r in scored:
        bd = r.get("total_dollars_5yr", 0)
        bc = r.get("base_contract_count", 0)
        high_vol = bd >= VOL_THRESHOLD
        high_freq = bc >= FREQ_THRESHOLD
        if high_vol and high_freq:
            r["posture_class"] = "Class 1"
        elif high_vol and not high_freq:
            r["posture_class"] = "Class 2"
        elif not high_vol and high_freq:
            r["posture_class"] = "Class 3"
        else:
            r["posture_class"] = "Class 4"


CLASS_CSS = {
    "Class 1": "class-1",
    "Class 2": "class-2",
    "Class 3": "class-3",
    "Class 4": "class-4",
}

def posture_class_css(posture_class):
    return CLASS_CSS.get(posture_class, "class-4")


def days_until(date_str):
    if not date_str:
        return None
    try:
        d = datetime.strptime(date_str[:10], "%Y-%m-%d").date()
        return (d - TODAY).days
    except Exception:
        return None


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

    target_vol = target_record.get("total_dollars_5yr", 0)

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
        cand_vol = uei_map.get(uei, {}).get("total_dollars_5yr", 0)
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
            "total_dollars_5yr": uei_map[uei]["total_dollars_5yr"],
            "certifications": uei_map[uei].get("certifications", []),
            "posture_class": uei_map[uei]["posture_class"],
            "class_css": posture_class_css(uei_map[uei]["posture_class"]),
        }
        for _, uei in top
    ]


# ─── Main scoring ─────────────────────────────────────────────────────────────

def _fmt_dollars(amount):
    """Format dollar amount for display."""
    if amount >= 1_000_000_000:
        return f"${amount / 1_000_000_000:.1f}B"
    if amount >= 1_000_000:
        return f"${amount / 1_000_000:.1f}M"
    if amount >= 1_000:
        return f"${amount / 1_000:.0f}K"
    return f"${amount:,.0f}"


def score_all(recipient_index, entities=None, state_code="NV", state_name="Nevada"):
    """
    Score all contractors found in awards data. SBA entities enrich, not gate.
    Base contracts only for classification. All awards for NAICS/PSC proximity.
    """

    # Build entity lookup for enrichment (optional)
    entity_by_uei = {}
    if entities:
        for e in entities:
            uei = e.get("uei", "")
            if uei:
                entity_by_uei[uei] = e

    print(f"Scoring {len(recipient_index)} contractors ({len(entity_by_uei)} with SBA enrichment)...")

    scored = []
    cutoff = date.today().replace(year=date.today().year - 5)

    for uei, awards in recipient_index.items():
        if not uei:
            continue

        # SBA enrichment (optional)
        entity = entity_by_uei.get(uei, {})

        # Only count awards with start_date within the past 5 years
        def _in_window(a):
            sd = a.get("start_date", "")
            if not sd:
                return False
            try:
                return datetime.strptime(sd[:10], "%Y-%m-%d").date() >= cutoff
            except Exception:
                return False

        scored_awards = [a for a in awards if _in_window(a)]

        # Base contract filtering (frequency) + total dollars (volume)
        base_awards = [a for a in scored_awards if a.get("award_type", "") in BASE_TYPES]
        base_contract_count = len(base_awards)
        total_dollars = sum(a.get("amount", 0) for a in scored_awards)

        if base_contract_count == 0 or total_dollars <= 1000:
            continue

        # NAICS/PSC from ALL awards (including delivery orders)
        validated_naics = list(set(a["naics"] for a in scored_awards if a.get("naics")))
        validated_pscs = list(set(a["psc"] for a in scored_awards if a.get("psc")))

        # Compute metrics
        obligation_density = total_dollars / base_contract_count
        log_volume = math.log10(total_dollars) if total_dollars > 0 else 0
        log_frequency = math.log10(base_contract_count) if base_contract_count > 0 else 0

        # Format awards for display
        contracts_display = [
            {
                "date": a["start_date"][:7] if a.get("start_date") else "-",
                "agency": a.get("agency", "-"),
                "naics": a.get("naics", "-"),
                "psc": a.get("psc", "-"),
                "value_fmt": f"{a.get('amount', 0):,.0f}",
                "type": a.get("award_type", ""),
                "is_base": a.get("award_type", "") in BASE_TYPES,
            }
            for a in sorted(awards, key=lambda x: x.get("start_date", ""), reverse=True)[:20]
        ]

        active_contracts = sum(
            1 for a in scored_awards
            if a.get("end_date") and days_until(a["end_date"]) and days_until(a["end_date"]) > 0
        )

        last_award_date = ""
        if awards:
            sorted_awards = sorted(awards, key=lambda x: x.get("start_date", ""), reverse=True)
            if sorted_awards[0].get("start_date"):
                last_award_date = sorted_awards[0]["start_date"][:7]

        # Name: SBA entity name > most common award recipient name
        name = entity.get("name") or entity.get("dba") or ""
        if not name:
            names = [a.get("recipient_name", "") for a in awards if a.get("recipient_name")]
            name = max(set(names), key=names.count) if names else "Unknown"

        # NAICS primary: SBA > most common from awards
        naics_primary = entity.get("naics_primary", "")
        if not naics_primary and validated_naics:
            all_naics = [a.get("naics", "") for a in scored_awards if a.get("naics")]
            naics_primary = max(set(all_naics), key=all_naics.count) if all_naics else ""

        record = {
            # Identity
            "slug": slugify(name),
            "name": name,
            "uei": uei,
            "naics_primary": naics_primary,
            "naics_codes": entity.get("naics_all", validated_naics)[:6],
            "certifications": entity.get("active_certs", []),
            "state_name": state_name,
            "state_slug": state_code.lower(),

            # Classification (v1.1)
            "posture_class": "",  # assigned after sort
            "total_dollars_5yr": float(total_dollars),
            "base_contract_count": base_contract_count,
            "obligation_density": float(obligation_density),
            "log_volume": round(log_volume, 3),
            "log_frequency": round(log_frequency, 3),

            # Awards (all, including delivery orders, for reference)
            "award_count": len(scored_awards),
            "active_contracts": active_contracts,
            "last_award_date": last_award_date,
            "contracts": contracts_display,

            # Index Drivers (for display)
            "index_drivers": [
                {"name": "Award Volume", "value": _fmt_dollars(total_dollars), "log": round(log_volume, 2)},
                {"name": "Award Frequency", "value": f"{base_contract_count} base contracts", "log": round(log_frequency, 2)},
                {"name": "Obligation Density", "value": _fmt_dollars(obligation_density)},
            ],

            # Proximity (built in second pass)
            "proximity": [],

            # Internal
            "_validated_naics": validated_naics,
            "_validated_pscs": validated_pscs,

            # Display
            "rank": 0,
            "total": 0,
        }

        scored.append(record)

    # Sort by total_dollars descending
    scored.sort(key=lambda x: x["total_dollars_5yr"], reverse=True)

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

    # ─── Velocity (cadence-based) ───────────────────────────────────
    print("Computing velocity vectors...")
    midpoint = date.today().replace(year=date.today().year - 2)
    _today = date.today()
    _cutoff = _today.replace(year=_today.year - 5)
    for r in scored:
        uei = r["uei"]
        all_awards = recipient_index.get(uei, [])
        base = [a for a in all_awards if a.get("award_type", "") in BASE_TYPES]

        # Collect award dates in window
        # Volume (dv): ALL awards. Frequency (df): base contracts only.
        award_dates = []
        early_vol = 0; late_vol = 0; early_freq = 0; late_freq = 0
        for a in all_awards:
            sd = a.get("start_date", "")
            if not sd:
                continue
            try:
                d = datetime.strptime(sd[:10], "%Y-%m-%d").date()
            except Exception:
                continue
            if d < _cutoff:
                continue
            award_dates.append(d)
            amt = a.get("amount", 0) or 0
            is_base = a.get("award_type", "") in BASE_TYPES
            if d < midpoint:
                early_vol += amt
                if is_base:
                    early_freq += 1
            else:
                late_vol += amt
                if is_base:
                    late_freq += 1

        # Fixed-split velocity (for aggregate stats / charts)
        dv = math.log10(max(late_vol, 1)) - math.log10(max(early_vol, 1))
        df = math.log10(max(late_freq, 1)) - math.log10(max(early_freq, 1))
        mag = math.sqrt(dv ** 2 + df ** 2)

        agg_direction = "stable"
        if mag > 0.3:
            if dv > 0 and df > 0:
                agg_direction = "growing"
            elif dv < 0 and df < 0:
                agg_direction = "declining"
            elif dv > 0:
                agg_direction = "concentrating"
            elif df > 0:
                agg_direction = "diversifying"

        # Single-contract: no meaningful aggregate direction
        if len(award_dates) <= 1:
            agg_direction = None
            mag = 0
            dv = 0
            df = 0

        # Cadence-based velocity (for individual dossier, requires 2+ contracts)
        award_dates.sort()
        if len(award_dates) <= 1:
            cadence_direction = None  # no velocity for single-contract
        else:
            avg_gap = (award_dates[-1] - award_dates[0]).days / (len(award_dates) - 1)
            if avg_gap == 0:
                avg_gap = 1
            days_since = (_today - award_dates[-1]).days
            gap_ratio = days_since / avg_gap

            mid_idx = len(award_dates) // 2
            if mid_idx > 0 and mid_idx < len(award_dates) - 1:
                early_gap = (award_dates[mid_idx] - award_dates[0]).days / mid_idx
                late_gap = (award_dates[-1] - award_dates[mid_idx]).days / (len(award_dates) - mid_idx - 1)
                pace_ratio = late_gap / early_gap if early_gap > 0 else 1.0
            else:
                pace_ratio = 1.0

            if gap_ratio > 5:
                cadence_direction = "inactive"
            elif pace_ratio < 0.7 and gap_ratio < 2:
                cadence_direction = "accelerating"
            elif gap_ratio < 2 and pace_ratio < 1.5:
                cadence_direction = "on pace"
            elif gap_ratio < 4 or pace_ratio < 2.5:
                cadence_direction = "slowing"
            else:
                cadence_direction = "declining"

        r["velocity"] = {
            "dv": round(dv, 3), "df": round(df, 3), "magnitude": round(mag, 3),
            "direction": agg_direction,
            "cadence": cadence_direction,
        }


    # ─── Competitive Density ─────────────────────────────────────────
    print("Computing competitive density...")
    slug_map = {r["slug"]: r for r in scored}
    for r in scored:
        neighbors = r.get("proximity", [])
        if len(neighbors) < 2:
            r["neighborhood_density"] = 0.0
            continue
        neighbor_slugs = set(n["slug"] for n in neighbors)
        cross = 0; possible = 0
        for n in neighbors:
            nr = slug_map.get(n["slug"])
            if not nr:
                continue
            nn_slugs = set(nn["slug"] for nn in nr.get("proximity", []))
            cross += len(neighbor_slugs.intersection(nn_slugs) - {n["slug"]})
            possible += len(neighbor_slugs) - 1
        r["neighborhood_density"] = round(cross / possible, 3) if possible > 0 else 0.0

    # ─── Proximity Pressure (code-specific velocity) ───────────────
    print("Computing proximity pressure...")

    # Precompute per-contractor code-specific velocity from raw awards
    uei_code_vel = {}
    for r in scored:
        uei = r["uei"]
        all_awards = recipient_index.get(uei, [])
        base = [a for a in all_awards if a.get("award_type", "") in BASE_TYPES]
        # Group awards by code, compute velocity per code
        code_early = defaultdict(float)
        code_late = defaultdict(float)
        for a in base:
            sd = a.get("start_date", "")
            if not sd:
                continue
            try:
                d = datetime.strptime(sd[:10], "%Y-%m-%d").date()
            except Exception:
                continue
            amt = a.get("amount", 0) or 0
            codes = set()
            if a.get("naics"):
                codes.add(a["naics"])
            if a.get("psc"):
                codes.add(a["psc"])
            for code in codes:
                if d < midpoint:
                    code_early[code] += amt
                else:
                    code_late[code] += amt
        uei_code_vel[uei] = (code_early, code_late)

    for r in scored:
        neighbors = r.get("proximity", [])
        if not neighbors:
            r["proximity_pressure"] = {"net": 0, "type": "isolated"}
            continue

        # Target's codes
        target_codes = set(r.get("_validated_naics", []) + r.get("_validated_pscs", []))
        if not target_codes:
            target_codes = set()
            for a in recipient_index.get(r["uei"], []):
                if a.get("naics"): target_codes.add(a["naics"])
                if a.get("psc"): target_codes.add(a["psc"])

        incoming = 0; outgoing = 0
        for n in neighbors:
            nr = slug_map.get(n["slug"])
            if not nr:
                continue
            n_early, n_late = uei_code_vel.get(nr["uei"], ({}, {}))
            # Only count velocity in SHARED codes
            shared = target_codes.intersection(set(n_early.keys()) | set(n_late.keys()))
            if not shared:
                continue
            shared_early = sum(n_early.get(c, 0) for c in shared)
            shared_late = sum(n_late.get(c, 0) for c in shared)
            dv = math.log10(max(shared_late, 1)) - math.log10(max(shared_early, 1))
            if dv > 0.3:
                incoming += dv
            elif dv < -0.3:
                outgoing += abs(dv)
        net = round(incoming - outgoing, 2)
        if net > 1:
            ptype = "compression"
        elif net < -1:
            ptype = "expansion"
        else:
            ptype = "neutral"
        r["proximity_pressure"] = {"net": net, "type": ptype}

    # Strip internal fields
    for r in scored:
        r.pop("_validated_naics", None)
        r.pop("_validated_pscs", None)

    return scored


# ─── Entry point ─────────────────────────────────────────────────────────────

def run(state):
    _STATES = json.loads((Path(__file__).parent.parent / "states.json").read_text())
    STATE_NAMES = {k: v["name"] for k, v in _STATES.items()}
    state_lower = state.lower()
    state_name = STATE_NAMES.get(state.upper(), state.upper())

    recipient_path = DATA_DIR / f"{state_lower}_awards_recipient.json"
    entities_path  = DATA_DIR / f"{state_lower}_entities.json"

    with open(recipient_path) as f:
        recipient_index = json.load(f)

    # SBA entities enrich but don't gate
    entities = None
    if entities_path.exists():
        with open(entities_path) as f:
            entities = json.load(f)

    scored = score_all(recipient_index, entities=entities, state_code=state.upper(), state_name=state_name)

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
                entry = {
                    "name": c["name"],
                    "slug": c["slug"],
                    "posture_class": c["posture_class"],
                    "prev_class": prev.get("posture_class", ""),
                    "class_changed": c["posture_class"] != prev.get("posture_class", ""),
                    "total_dollars_5yr": c["total_dollars_5yr"],
                    "prev_total_dollars": prev.get("total_dollars_5yr", 0),
                }
                if entry["class_changed"]:
                    # Class number decreased = rising (Class 4 -> Class 1 is improvement)
                    curr_num = int(c["posture_class"].split()[-1])
                    prev_num = int(prev.get("posture_class", "Class 4").split()[-1])
                    if curr_num < prev_num:
                        movers_rising.append(entry)
                    else:
                        movers_declining.append(entry)
        movers_rising = movers_rising[:8]
        movers_declining = movers_declining[:8]

    movers_data = {"rising": movers_rising, "declining": movers_declining}
    with open(DATA_DIR / f"{state_lower}_movers.json", "w") as f:
        json.dump(movers_data, f, indent=2)

    # Save new snapshot
    new_snapshot = {
        c["uei"]: {
            "posture_class": c["posture_class"],
            "total_dollars_5yr": c["total_dollars_5yr"],
        }
        for c in scored if c.get("uei")
    }
    with open(snapshot_path, "w") as f:
        json.dump(new_snapshot, f)

    # Score distribution report
    buckets = {"Class 1": 0, "Class 2": 0, "Class 3": 0, "Class 4": 0}
    for r in scored:
        rc = r["posture_class"]
        buckets[rc] = buckets.get(rc, 0) + 1
    print("\nPosture Class distribution:")
    for cls, count in buckets.items():
        print(f"  {cls}: {count}")

    print(f"\nRun next: python generate/build.py --state {state}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--state", default="NV")
    args = parser.parse_args()
    run(args.state)
