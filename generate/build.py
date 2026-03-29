"""
FedComp Index - Static Site Generator
Reads scored contractor data, renders Jinja2 templates, writes site/dist/

Usage:
    python generate/build.py --state NV
    python generate/build.py --state NV --data data/nv_scored.json
"""

import os
import json
import hashlib
import shutil
import argparse
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path
from jinja2 import Environment, FileSystemLoader

BASE_DIR = Path(__file__).parent.parent
TEMPLATES_DIR = BASE_DIR / "site" / "templates"
STATIC_DIR = BASE_DIR / "site" / "static"
DIST_DIR = BASE_DIR / "site" / "dist"

SITE_URL = os.getenv("SITE_URL", "https://fedcompindex.org")
API_URL = os.getenv("API_URL", "https://fedcomp-api.n-petrova.workers.dev")
DATA_DATE = os.getenv("DATA_DATE", str(date.today()))
NODES_FILE = BASE_DIR / "nodes.json"


def load_nodes():
    if NODES_FILE.exists():
        with open(NODES_FILE) as f:
            return json.load(f)
    return {}


# Contextual link distribution - each dossier gets one link to one signal node
def get_dossier_links(state_name):
    if not state_name:
        return []
    return [
        ("huggingface_contractors", f"View this contractor in the full {state_name} dataset on HuggingFace"),
        ("kaggle_contractors", f"Analyze {state_name} contractor posture data on Kaggle"),
        ("pypi", "Score contractors programmatically with fedcomp-index"),
        ("pypi_data", "Access pre-classified contractor data via Python"),
        ("pypi_scoring", "Classify contractors programmatically with fedcomp-index"),
        ("huggingface_awards", f"Browse {state_name} federal contract awards on HuggingFace"),
        ("kaggle_awards", f"Explore {state_name} contract award data on Kaggle"),
        ("faq", "Frequently asked questions about the FedComp Index"),
        ("tabularium", "Data, tools, and resources in the FedComp Tabularium"),
    ]


def get_dossier_link(slug, nodes, state_name):
    """Deterministic link assignment based on contractor slug hash."""
    dossier_links = get_dossier_links(state_name)
    if not nodes or not dossier_links:
        return None
    idx = hash(slug) % len(dossier_links)
    key, text = dossier_links[idx]
    url = nodes.get(key)
    if not url:
        return None
    return {"url": url, "text": text}

_STATES = json.loads((BASE_DIR / "states.json").read_text())
STATE_NAMES = {k: v["name"] for k, v in _STATES.items()}
DEPLOYED_STATES = [
    {"code": k.lower(), "name": v["name"], "rankings": True}
    for k, v in _STATES.items() if v.get("deployed")
]
API_LIVE = os.getenv("API_LIVE", "1") == "1"


def fmt_dollars(amount):
    """Smart dollar formatting. $1.2B, $45.3M, $892K, $1,786."""
    if amount is None:
        return "$0"
    amount = float(amount)
    if amount >= 1_000_000_000:
        return f"${amount / 1_000_000_000:.1f}B"
    if amount >= 1_000_000:
        return f"${amount / 1_000_000:.1f}M"
    if amount >= 10_000:
        return f"${amount / 1_000:.0f}K"
    if amount >= 1_000:
        return f"${amount / 1_000:.1f}K"
    return f"${amount:,.0f}"


def build_contractor_summary(c, state_name, total):
    """Build a unique prose paragraph from contractor data.
    Each dimension branches on the data. More dimensions = multiplicative growth
    in the structural variant space. Target: combinatorial space >> contractor count.

    Dimensions (variants):
      Opening (5) x Value (6) x Agency (5) x Certs (5) x Active (4) x
      Recency (4) x Proximity (3) x Rank position (3) = 108,000 structural variants
    """
    parts = []
    name = c["name"]
    rank = c["rank"]
    base_dollars = c.get("base_dollars_5yr", 0)
    base_vol_fmt = fmt_dollars(base_dollars)
    base_count = c.get("base_contract_count", 0)
    cls = c["posture_class"]
    certs = c.get("certifications", [])
    contracts = c.get("contracts", [])
    proximity = c.get("proximity", [])
    active = c.get("active_contracts", 0)
    last_award = c.get("last_award_date", "")

    # ── Opening (5 variants: class x rank position) ──────────────────
    top_10 = rank <= 10
    if cls == "Class 1" and top_10:
        parts.append(f"{name} is a Class 1 {state_name} federal contractor, ranking #{rank} of {total} classified firms.")
    elif cls == "Class 1":
        parts.append(f"{name} is a Class 1 {state_name} federal contractor, ranked #{rank} of {total}. Systematic winner with high volume and high frequency.")
    elif cls == "Class 2" and rank <= total // 4:
        parts.append(f"{name} is a Class 2 {state_name} federal contractor ranked #{rank} of {total}. High volume from concentrated contract vehicles.")
    elif cls == "Class 3":
        parts.append(f"{name} is a Class 3 {state_name} federal contractor ranked #{rank} of {total}. Growth pipeline with repeated wins at lower dollar values.")
    else:
        parts.append(f"{name} holds position #{rank} among {total} {state_name} federal contractors on the FedComp Index as {cls}.")

    # ── Value (6 variants) ───────────────────────────────────────────
    if base_dollars >= 500_000_000:
        parts.append(f"Base contract volume totals {base_vol_fmt} over the past five years across {base_count} base contracts.")
    elif base_dollars >= 100_000_000:
        parts.append(f"Five-year base contract volume totals {base_vol_fmt} from {base_count} contracts.")
    elif base_dollars >= 50_000_000:
        parts.append(f"Over five years, {name} has secured {base_vol_fmt} in base contract value from {base_count} contracts.")
    elif base_dollars >= 10_000_000:
        parts.append(f"Base contract history shows {base_vol_fmt} across {base_count} contracts in the past five years.")
    elif base_dollars >= 1_000_000:
        parts.append(f"Base contract records total {base_vol_fmt} from {base_count} federal contracts over five years.")
    else:
        parts.append(f"Base contract records show {base_vol_fmt} from {base_count} contracts over the five-year window.")

    # ── Agency (5 variants) ──────────────────────────────────────────
    if contracts:
        agencies = set(a.get("agency", "") for a in contracts if a.get("agency") and a["agency"] != "-")
        agency_list = sorted(agencies)
        if len(agencies) == 1:
            parts.append(f"All recorded awards come from {agency_list[0]}.")
        elif len(agencies) == 2:
            parts.append(f"Awards span {agency_list[0]} and {agency_list[1]}.")
        elif len(agencies) == 3:
            parts.append(f"Contracts come from {agency_list[0]}, {agency_list[1]}, and {agency_list[2]}.")
        elif len(agencies) <= 5:
            parts.append(f"Contract history spans {len(agencies)} federal agencies including {agency_list[0]}.")
        elif len(agencies) > 5:
            parts.append(f"Awards spread across {len(agencies)} federal agencies.")

    # ── Certifications (5 variants) ──────────────────────────────────
    if len(certs) >= 4:
        parts.append(f"Holds {len(certs)} active SBA certifications: {', '.join(certs)}.")
    elif len(certs) == 3:
        parts.append(f"SBA-certified as {certs[0]}, {certs[1]}, and {certs[2]}.")
    elif len(certs) == 2:
        parts.append(f"Certified as {certs[0]} and {certs[1]} through the SBA.")
    elif len(certs) == 1:
        parts.append(f"Carries an active {certs[0]} certification.")

    # ── Active contracts (4 variants) ────────────────────────────────
    if active >= 10:
        parts.append(f"Currently managing {active} active contracts.")
    elif active >= 5:
        parts.append(f"Has {active} active contracts on record.")
    elif active > 1:
        parts.append(f"{active} contracts currently active.")
    elif active == 1:
        parts.append(f"One contract currently active.")

    # ── Recency (4 variants) ─────────────────────────────────────────
    if last_award:
        year = last_award[:4]
        month = last_award[5:7] if len(last_award) >= 7 else ""
        month_names = {"01":"January","02":"February","03":"March","04":"April","05":"May","06":"June","07":"July","08":"August","09":"September","10":"October","11":"November","12":"December"}
        month_name = month_names.get(month, "")
        if year == "2026":
            parts.append(f"Most recent award recorded in {month_name} {year}.")
        elif year == "2025" and month and int(month) >= 7:
            parts.append(f"Last award dated {month_name} {year}.")
        elif year == "2025":
            parts.append(f"Most recent contract activity from early {year}.")
        else:
            parts.append(f"Last recorded award dates to {year}.")

    # ── Proximity (3 variants) ───────────────────────────────────────
    if len(proximity) >= 3:
        parts.append(f"Top competitors by award overlap include {proximity[0]['name']} and {proximity[1]['name']}.")
    elif len(proximity) >= 1:
        parts.append(f"Closest competitor by award overlap is {proximity[0]['name']}.")

    return " ".join(parts)


CLASS_CSS = {"Class 1": "class-1", "Class 2": "class-2", "Class 3": "class-3", "Class 4": "class-4"}


def class_css(posture_class):
    return CLASS_CSS.get(posture_class, "")


def load_data(path):
    with open(path) as f:
        return json.load(f)


def compute_top_performers(contractors, recipient_index, days, n=5):
    cutoff = str(date.today() - timedelta(days=days))
    slug_map = {c["uei"]: c for c in contractors if c.get("uei")}
    results = []
    for uei, awards in recipient_index.items():
        if uei not in slug_map:
            continue
        recent = [a for a in awards if (a.get("start_date") or "") >= cutoff]
        if not recent:
            continue
        total = sum(a.get("amount", 0) for a in recent)
        c = slug_map[uei]
        results.append({
            "name": c["name"],
            "slug": c["slug"],
            "posture_class": c.get("posture_class", ""),
            "class_css": class_css(c.get("posture_class", "")),
            "total_m": round(total / 1_000_000, 1),
            "count": len(recent),
        })
    results.sort(key=lambda x: x["total_m"], reverse=True)
    return results[:n]


def build(state, data_path):
    env = Environment(loader=FileSystemLoader(str(TEMPLATES_DIR)), autoescape=True)
    env.filters['class_css'] = class_css
    env.filters['fmt_dollars'] = fmt_dollars

    state_lower = state.lower()
    state_name = STATE_NAMES.get(state.upper(), state.upper())

    # Cache-busting hash from file contents
    try:
        css_hash = hashlib.md5((STATIC_DIR / "global.css").read_bytes()).hexdigest()[:8]
    except FileNotFoundError:
        print("WARNING: global.css not found, using no-cache fallback")
        css_hash = "nocache"
    try:
        js_hash = hashlib.md5((STATIC_DIR / "main.js").read_bytes()).hexdigest()[:8]
    except FileNotFoundError:
        print("WARNING: main.js not found, using no-cache fallback")
        js_hash = "nocache"

    ctx_base = {
        "site_url": SITE_URL,
        "api_url": API_URL,
        "data_date": DATA_DATE,
        "state_name": state_name,
        "state_code": state_lower,
        "api_live": API_LIVE,
        "nav_states": DEPLOYED_STATES,
        "css_hash": css_hash,
        "js_hash": js_hash,
    }

    contractors = load_data(data_path)

    # Pre-compute display fields for all contractors
    for c in contractors:
        base_dollars = c.get("base_dollars_5yr", 0)
        base_count = c.get("base_contract_count", 0)
        c["class_css"] = class_css(c.get("posture_class", ""))

    state_lower = state.lower()
    DIST_DIR.mkdir(parents=True, exist_ok=True)
    dossier_root = DIST_DIR / "dossier"
    if dossier_root.exists():
        shutil.rmtree(dossier_root)
    dossier_root.mkdir(parents=True, exist_ok=True)

    # Copy static files into dist (cards/ lives in static/ as source of truth)
    dist_static = DIST_DIR / "static"
    if dist_static.exists():
        shutil.rmtree(dist_static)
    shutil.copytree(STATIC_DIR, dist_static)

    # State stats
    entities_path = BASE_DIR / "data" / f"{state_lower}_entities.json"
    total_registered = 0
    if entities_path.exists():
        total_registered = len(load_data(entities_path))

    total_volume = sum(c.get("base_dollars_5yr", 0) for c in contractors)
    total_volume_fmt = fmt_dollars(total_volume)

    stats = {
        "total": len(contractors),
        "total_registered": total_registered,
        "class_1_count": sum(1 for c in contractors if c.get("posture_class") == "Class 1"),
        "class_2_count": sum(1 for c in contractors if c.get("posture_class") == "Class 2"),
        "class_3_count": sum(1 for c in contractors if c.get("posture_class") == "Class 3"),
        "class_4_count": sum(1 for c in contractors if c.get("posture_class") == "Class 4"),
        "win_pct": round(len(contractors) / total_registered * 100, 1) if total_registered else 0,
        "total_volume": total_volume_fmt,
    }

    # ─── Methodology stats (computed early, used by landing + methodology pages) ──
    meth = {}
    if not contractors:
        # Empty data guard: set all meth keys to safe defaults so templates render
        meth["total"] = 0
        meth["total_registered"] = total_registered
        meth["rog_pct"] = 0
        meth["od_min"] = "$0"
        meth["od_max"] = "$0"
        meth["od_range"] = "0x"
        meth["gini"] = 0
        for pct_label in ["top_1", "top_5", "top_10", "top_20"]:
            meth[f"{pct_label}_pct"] = 0
            meth[f"{pct_label}_n"] = 0
        for cls_num in [1, 2, 3, 4]:
            meth[f"c{cls_num}_pct"] = 0
            meth[f"c{cls_num}_vol_pct"] = 0
            meth[f"c{cls_num}_exp_pct"] = 0
            meth[f"c{cls_num}_declining_pct"] = 0
            meth[f"c{cls_num}_growing_pct"] = 0
            meth[f"c{cls_num}_density"] = 0
            meth[f"c{cls_num}_compression_pct"] = 0
            meth[f"c{cls_num}_expansion_pct"] = 0
    else:
        meth["total"] = len(contractors)
        meth["total_registered"] = total_registered
        meth["rog_pct"] = round(100 - (len(contractors) / total_registered * 100), 1) if total_registered else 0
        meth["od_min"] = fmt_dollars(min(c["obligation_density"] for c in contractors))
        meth["od_max"] = fmt_dollars(max(c["obligation_density"] for c in contractors))
        od_sorted = sorted(c["obligation_density"] for c in contractors)
        meth["od_range"] = f"{int(od_sorted[-1] / max(od_sorted[0], 1)):,}x"
        vols_desc = sorted((c["base_dollars_5yr"] for c in contractors), reverse=True)
        vol_total = sum(vols_desc)
        n_m = len(vols_desc)
        for pct_label, pct in [("top_1", 1), ("top_5", 5), ("top_10", 10), ("top_20", 20)]:
            top_n = max(1, int(n_m * pct / 100))
            meth[f"{pct_label}_pct"] = round(sum(vols_desc[:top_n]) / vol_total * 100, 1)
            meth[f"{pct_label}_n"] = top_n
        sv = sorted(c["base_dollars_5yr"] for c in contractors)
        cum = 0; gini_sum = 0
        for i, v in enumerate(sv):
            cum += v; gini_sum += cum
        meth["gini"] = round(1 - 2 * (gini_sum / (n_m * vol_total)) + 1/n_m, 3)
        for cls_num in [1, 2, 3, 4]:
            cls_name = f"Class {cls_num}"
            members = [c for c in contractors if c["posture_class"] == cls_name]
            meth[f"c{cls_num}_pct"] = round(len(members) / n_m * 100, 1)
            cls_vol = sum(c["base_dollars_5yr"] for c in members)
            meth[f"c{cls_num}_vol_pct"] = round(cls_vol / vol_total * 100, 1)
        from datetime import datetime as _dt
        _today = date.today()
        awards_path = BASE_DIR / "data" / f"{state_lower}_awards_recipient.json"
        _BASE_TYPES = {"DEFINITIVE CONTRACT", "PURCHASE ORDER", "BPA CALL"}
        if awards_path.exists():
            import json as _json
            with open(awards_path) as _f:
                _awards = _json.load(_f)
            for cls_num in [1, 2, 3, 4]:
                cls_name = f"Class {cls_num}"
                members = [c for c in contractors if c["posture_class"] == cls_name]
                total_base = 0; expiring = 0
                for c in members:
                    base = [a for a in _awards.get(c["uei"], []) if a.get("award_type", "") in _BASE_TYPES]
                    total_base += len(base)
                    for a in base:
                        end = a.get("end_date", "")
                        if end:
                            try:
                                end_d = _dt.strptime(end[:10], "%Y-%m-%d").date()
                                if 0 < (end_d - _today).days <= 365: expiring += 1
                            except Exception: pass
                meth[f"c{cls_num}_exp_pct"] = round(expiring / total_base * 100, 1) if total_base else 0
        for cls_num in [1, 2, 3, 4]:
            cls_name = f"Class {cls_num}"
            members = [c for c in contractors if c["posture_class"] == cls_name]
            declining = sum(1 for c in members if c.get("velocity", {}).get("direction") == "declining")
            growing = sum(1 for c in members if c.get("velocity", {}).get("direction") == "growing")
            meth[f"c{cls_num}_declining_pct"] = round(declining / len(members) * 100) if members else 0
            meth[f"c{cls_num}_growing_pct"] = round(growing / len(members) * 100) if members else 0
        for cls_num in [1, 2, 3, 4]:
            cls_name = f"Class {cls_num}"
            members = [c for c in contractors if c["posture_class"] == cls_name and c.get("neighborhood_density") is not None]
            with_n = [c for c in members if len(c.get("proximity", [])) >= 2]
            meth[f"c{cls_num}_density"] = round(sum(c["neighborhood_density"] for c in with_n) / len(with_n), 3) if with_n else 0
        for cls_num in [1, 2, 3, 4]:
            cls_name = f"Class {cls_num}"
            members = [c for c in contractors if c["posture_class"] == cls_name]
            compression = sum(1 for c in members if c.get("proximity_pressure", {}).get("type") == "compression")
            expansion = sum(1 for c in members if c.get("proximity_pressure", {}).get("type") == "expansion")
            meth[f"c{cls_num}_compression_pct"] = round(compression / len(members) * 100) if members else 0
            meth[f"c{cls_num}_expansion_pct"] = round(expansion / len(members) * 100) if members else 0

    # Top 5 by certification type
    CERT_COLS = ["8(a)", "HUBZone", "WOSB", "EDWOSB", "VOSB", "SDVOSB"]
    cert_groups = defaultdict(list)
    seen_per_cert = defaultdict(set)
    for c in contractors:
        for cert_str in c.get("certifications", []):
            cu = cert_str.upper()
            if "SDVOSB" in cu and c["uei"] not in seen_per_cert["SDVOSB"]:
                cert_groups["SDVOSB"].append(c)
                seen_per_cert["SDVOSB"].add(c["uei"])
            if "VOSB" in cu and c["uei"] not in seen_per_cert["VOSB"]:
                cert_groups["VOSB"].append(c)
                seen_per_cert["VOSB"].add(c["uei"])
            if "WOSB" in cu and c["uei"] not in seen_per_cert["WOSB"]:
                cert_groups["WOSB"].append(c)
                seen_per_cert["WOSB"].add(c["uei"])
            if ("8(A)" in cu or "8A" in cu) and c["uei"] not in seen_per_cert["8(a)"]:
                cert_groups["8(a)"].append(c)
                seen_per_cert["8(a)"].add(c["uei"])
            if "HUBZONE" in cu and c["uei"] not in seen_per_cert["HUBZone"]:
                cert_groups["HUBZone"].append(c)
                seen_per_cert["HUBZone"].add(c["uei"])
            if "EDWOSB" in cu and c["uei"] not in seen_per_cert["EDWOSB"]:
                cert_groups["EDWOSB"].append(c)
                seen_per_cert["EDWOSB"].add(c["uei"])

    top_by_cert = {}
    for cert in CERT_COLS:
        top_by_cert[cert] = [
            {
                "name": c["name"],
                "slug": c["slug"],
                "posture_class": c["posture_class"],
                "class_css": class_css(c["posture_class"]),
                "base_dollars_5yr": c.get("base_dollars_5yr", 0),
            }
            for c in cert_groups[cert][:5]
        ]

    # Top performers by time window
    recipient_index_path = BASE_DIR / "data" / f"{state_lower}_awards_recipient.json"
    recipient_index = {}
    if recipient_index_path.exists():
        with open(recipient_index_path) as f:
            recipient_index = json.load(f)

    top_performers = {
        "30d": compute_top_performers(contractors, recipient_index, 30),
        "6m": compute_top_performers(contractors, recipient_index, 180),
        "1yr": compute_top_performers(contractors, recipient_index, 365),
        "5yr": compute_top_performers(contractors, recipient_index, 1825),
    }

    # Carousel chart data
    class_counts = {"Class 1": 0, "Class 2": 0, "Class 3": 0, "Class 4": 0}
    for c in contractors:
        cls = c.get("posture_class", "")
        if cls in class_counts:
            class_counts[cls] += 1

    naics_totals = defaultdict(float)
    agency_totals = defaultdict(float)
    for c in contractors:
        for contract in c.get("contracts", []):
            naics = contract.get("naics", "")
            agency = contract.get("agency", "")
            try:
                val = float(contract.get("value_fmt", "0").replace(",", ""))
            except ValueError:
                val = 0
            if naics and naics != "-":
                naics_totals[naics] += val
            if agency and agency != "-":
                agency_totals[agency] += val

    top_naics = sorted(naics_totals.items(), key=lambda x: x[1], reverse=True)[:8]
    top_agencies = sorted(agency_totals.items(), key=lambda x: x[1], reverse=True)[:6]

    class_dist = {
        "Class 1": sum(1 for c in contractors if c.get("posture_class") == "Class 1"),
        "Class 2": sum(1 for c in contractors if c.get("posture_class") == "Class 2"),
        "Class 3": sum(1 for c in contractors if c.get("posture_class") == "Class 3"),
        "Class 4": sum(1 for c in contractors if c.get("posture_class") == "Class 4"),
    }

    # Top 10 contractors by value
    top_contractors = [
        {"name": c["name"][:28], "value_fmt": fmt_dollars(c.get("base_dollars_5yr", 0))}
        for c in contractors[:10]
    ]

    # Cert coverage - how many contractors hold each cert
    cert_coverage = {}
    for cert in CERT_COLS:
        cert_coverage[cert] = len(cert_groups[cert])

    # Velocity distribution for chart (cadence-based)
    vel_data = {}
    for cls_num in [1, 2, 3, 4]:
        cls_name = f"Class {cls_num}"
        members = [c for c in contractors if c["posture_class"] == cls_name]
        vel_data[cls_name] = {
            "accelerating": sum(1 for c in members if c.get("velocity", {}).get("cadence") == "accelerating"),
            "on_pace": sum(1 for c in members if c.get("velocity", {}).get("cadence") == "on pace"),
            "slowing": sum(1 for c in members if c.get("velocity", {}).get("cadence") == "slowing"),
            "inactive": sum(1 for c in members if c.get("velocity", {}).get("cadence") == "inactive"),
        }

    charts_data = {
        "class_counts": class_counts,
        "top_naics": [{"code": k, "value_m": round(v / 1e6, 1)} for k, v in top_naics],
        "top_agencies": [{"name": k.replace("Department of the ", "").replace("Department of ", ""), "value_m": round(v / 1e6, 1)} for k, v in top_agencies],
        "class_dist": class_dist,
        "top_contractors": top_contractors,
        "cert_coverage": cert_coverage,
        "velocity": vel_data,
        "fragility": {
            "Class 1": meth.get("c1_exp_pct", 0),
            "Class 2": meth.get("c2_exp_pct", 0),
            "Class 3": meth.get("c3_exp_pct", 0),
            "Class 4": meth.get("c4_exp_pct", 0),
        },
    }

    # ─── Landing page ─────────────────────────────────────────────────
    tmpl = env.get_template("state_landing.html")
    out = tmpl.render(**ctx_base, stats=stats, m=meth, top_by_cert=top_by_cert, top_performers=top_performers, charts_data=charts_data)
    state_dir = DIST_DIR / state_lower
    state_dir.mkdir(parents=True, exist_ok=True)
    (state_dir / "index.html").write_text(out, encoding="utf-8")
    print(f"Built: /{state_lower}/ (landing)")

    # ─── Rankings page ────────────────────────────────────────────────
    tmpl = env.get_template("state_index.html")
    out = tmpl.render(**ctx_base, contractors=contractors, stats=stats)
    rankings_dir = DIST_DIR / state_lower / "rankings"
    rankings_dir.mkdir(parents=True, exist_ok=True)
    (rankings_dir / "index.html").write_text(out, encoding="utf-8")
    print(f"Built: /{state_lower}/rankings/")

    # ─── Contractor dossier pages ──────────────────────────────────────
    nodes = load_nodes()
    tmpl = env.get_template("dossier.html")
    seen_slugs = set()
    for c in contractors:
        if not c.get("name") or not c.get("uei"):
            print(f"WARNING: skipping contractor missing name/uei: {c}")
            continue
        if not c.get("posture_class"):
            print(f"WARNING: skipping contractor missing posture_class: {c.get('name')} ({c.get('uei')})")
            continue
        # Add computed display fields
        c["class_css"] = class_css(c.get("posture_class", ""))
        base_dollars = c.get("base_dollars_5yr", 0)
        base_count = c.get("base_contract_count", 0)
        # Propagate class_css to proximity entries
        for p in c.get("proximity", []):
            p["class_css"] = class_css(p.get("posture_class", ""))
        c["summary"] = build_contractor_summary(c, state_name, len(contractors))
        base_slug = c["slug"]
        slug = base_slug
        counter = 1
        while slug in seen_slugs:
            slug = f"{base_slug}-{counter}"
            counter += 1
        seen_slugs.add(slug)
        c["slug"] = slug
        ext_link = get_dossier_link(slug, nodes, state_name)
        out = tmpl.render(**ctx_base, c=c, ext_link=ext_link)
        dossier_dir = DIST_DIR / "dossier" / slug
        dossier_dir.mkdir(parents=True, exist_ok=True)
        (dossier_dir / "index.html").write_text(out, encoding="utf-8")

    print(f"Built: {len(contractors)} dossier pages")

    # ─── Home page ────────────────────────────────────────────────────
    tmpl = env.get_template("home.html")
    out = tmpl.render(**ctx_base, stats=stats)
    (DIST_DIR / "index.html").write_text(out, encoding="utf-8")
    print(f"Built: / (home)")

    # ─── Methodology page (charts + render) ────────────────────────
    # Generate charts
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import math

        chart_dir = DIST_DIR / "static" / "img"
        chart_dir.mkdir(parents=True, exist_ok=True)

        colors_map = {"Class 1": "#0D9488", "Class 2": "#D97706", "Class 3": "#2563EB", "Class 4": "#475569"}

        # Scatter plot
        fig, ax = plt.subplots(figsize=(7, 5))
        fig.patch.set_facecolor("#0f1724"); ax.set_facecolor("#0f1724")
        for c in contractors:
            ax.scatter(c["log_frequency"], c["log_volume"], c=colors_map[c["posture_class"]], s=14, alpha=0.5, edgecolors="none", zorder=2)
        vol_t = math.log10(5_000_000); freq_t = math.log10(3)
        ax.axhline(y=vol_t, color="#ffffff", linewidth=0.8, alpha=0.3, linestyle="--")
        ax.axvline(x=freq_t, color="#ffffff", linewidth=0.8, alpha=0.3, linestyle="--")
        for label, x, y, color in [("Class 1",2.2,8.3,"#0D9488"),("Class 2",0.05,8.3,"#D97706"),("Class 3",2.2,4.3,"#2563EB"),("Class 4",0.05,4.3,"#475569")]:
            ax.text(x, y, label, color=color, fontsize=10, fontweight="bold", alpha=0.5)
        ax.set_xlabel("log\u2081\u2080(base contract count)", color="#9ca3af", fontsize=10)
        ax.set_ylabel("log\u2081\u2080(base contract dollars)", color="#9ca3af", fontsize=10)
        ax.tick_params(colors="#6b7280", labelsize=9)
        for s in ["bottom","left"]: ax.spines[s].set_color("#1e293b")
        for s in ["top","right"]: ax.spines[s].set_visible(False)
        ax.grid(True, alpha=0.08, color="#ffffff")
        plt.tight_layout()
        plt.savefig(str(chart_dir / "chart_scatter.png"), dpi=200, facecolor="#0f1724")
        plt.close()

        # Velocity field
        fig, ax = plt.subplots(figsize=(8, 6))
        fig.patch.set_facecolor("#0f1724"); ax.set_facecolor("#0f1724")
        for c in contractors:
            ax.scatter(c["log_frequency"], c["log_volume"], c=colors_map[c["posture_class"]], s=12, alpha=0.4, edgecolors="none", zorder=2)
        for c in contractors:
            v = c.get("velocity", {})
            if v.get("magnitude", 0) < 1.0: continue
            dx = v.get("df", 0) * 0.12; dy = v.get("dv", 0) * 0.12
            ax.annotate("", xy=(c["log_frequency"]+dx, c["log_volume"]+dy), xytext=(c["log_frequency"], c["log_volume"]),
                        arrowprops=dict(arrowstyle="->", color=colors_map[c["posture_class"]], lw=0.9, alpha=0.75), zorder=3)
        ax.axhline(y=vol_t, color="#ffffff", linewidth=0.8, alpha=0.3, linestyle="--")
        ax.axvline(x=freq_t, color="#ffffff", linewidth=0.8, alpha=0.3, linestyle="--")
        for label, x, y, color in [("Class 1",2.2,8.3,"#0D9488"),("Class 2",0.05,8.3,"#D97706"),("Class 3",2.2,4.3,"#2563EB"),("Class 4",0.05,4.3,"#475569")]:
            ax.text(x, y, label, color=color, fontsize=10, fontweight="bold", alpha=0.5)
        ax.set_xlabel("log10(base contract count)", color="#9ca3af", fontsize=10)
        ax.set_ylabel("log10(base contract dollars)", color="#9ca3af", fontsize=10)
        ax.tick_params(colors="#6b7280", labelsize=9)
        for s in ["bottom","left"]: ax.spines[s].set_color("#1e293b")
        for s in ["top","right"]: ax.spines[s].set_visible(False)
        ax.grid(True, alpha=0.08, color="#ffffff")
        plt.tight_layout()
        plt.savefig(str(chart_dir / "chart_velocity.png"), dpi=200, facecolor="#0f1724")
        plt.close()

        # Lorenz curve
        fig, ax = plt.subplots(figsize=(5.5, 4.5))
        fig.patch.set_facecolor("#0f1724"); ax.set_facecolor("#0f1724")
        vols = sorted(c["base_dollars_5yr"] for c in contractors)
        total_l = sum(vols); n_l = len(vols)
        cum_pop = [i/n_l for i in range(n_l+1)]
        cum_vol = [0]; running = 0
        for v in vols:
            running += v
            cum_vol.append(running / total_l)
        ax.fill_between(cum_pop, cum_pop, cum_vol, alpha=0.15, color="#D97706")
        ax.plot(cum_pop, cum_vol, color="#D97706", linewidth=2, label=f"Gini = {meth['gini']}")
        ax.plot([0,1], [0,1], color="#ffffff", linewidth=0.8, alpha=0.3, linestyle="--", label="Perfect equality")
        ax.set_xlabel("Cumulative share of contractors", color="#9ca3af", fontsize=10)
        ax.set_ylabel("Cumulative share of dollars", color="#9ca3af", fontsize=10)
        ax.tick_params(colors="#6b7280", labelsize=9)
        for s in ["bottom","left"]: ax.spines[s].set_color("#1e293b")
        for s in ["top","right"]: ax.spines[s].set_visible(False)
        ax.set_xlim(0,1); ax.set_ylim(0,1)
        legend = ax.legend(loc="upper left", fontsize=8, framealpha=0.3, edgecolor="#1e293b")
        for text in legend.get_texts(): text.set_color("#9ca3af")
        legend.get_frame().set_facecolor("#0f1724")
        plt.tight_layout()
        plt.savefig(str(chart_dir / "chart_lorenz.png"), dpi=200, facecolor="#0f1724")
        plt.close()

        # Compress to JPEG
        try:
            from PIL import Image as PILImage
            for name in ["chart_scatter", "chart_velocity", "chart_lorenz"]:
                png_path = chart_dir / f"{name}.png"
                jpg_path = chart_dir / f"{name}.jpg"
                if png_path.exists():
                    PILImage.open(str(png_path)).convert("RGB").save(str(jpg_path), "JPEG", quality=85, optimize=True)
        except ImportError:
            pass  # Pillow not available, keep PNGs

        print("Built: methodology charts (scatter, velocity, lorenz)")
    except ImportError:
        print("WARNING: matplotlib not available, skipping methodology charts")

    tmpl = env.get_template("methodology.html")
    out = tmpl.render(**ctx_base, m=meth, stats=stats)
    meth_dir = DIST_DIR / "methodology"
    meth_dir.mkdir(parents=True, exist_ok=True)
    (meth_dir / "index.html").write_text(out, encoding="utf-8")
    print(f"Built: /methodology/")

    # Redirect old /wiki/methodology/ to /methodology/
    old_meth = DIST_DIR / "wiki" / "methodology"
    old_meth.mkdir(parents=True, exist_ok=True)
    (old_meth / "index.html").write_text(
        '<!DOCTYPE html><html><head><meta http-equiv="refresh" content="0;url=/methodology/"><link rel="canonical" href="/methodology/"></head><body></body></html>',
        encoding="utf-8",
    )

    # ─── Contact page ────────────────────────────────────────────────
    tmpl = env.get_template("contact.html")
    out = tmpl.render(**ctx_base)
    contact_dir = DIST_DIR / "contact"
    contact_dir.mkdir(parents=True, exist_ok=True)
    (contact_dir / "index.html").write_text(out, encoding="utf-8")
    print(f"Built: /contact/")

    # ─── FAQ page ──────────────────────────────────────────────────────
    tmpl = env.get_template("faq.html")
    out = tmpl.render(**ctx_base, stats=stats)
    faq_dir = DIST_DIR / "faq"
    faq_dir.mkdir(parents=True, exist_ok=True)
    (faq_dir / "index.html").write_text(out, encoding="utf-8")
    print(f"Built: /faq/")

    # ─── Tabularium page ─────────────────────────────────────────────
    tmpl = env.get_template("tabularium.html")
    tab_nodes = load_nodes()
    tab_datasets = [
        {"url": "https://huggingface.co/npetro6", "label": "HuggingFace Datasets", "platform": "HuggingFace"},
        {"url": "https://www.kaggle.com/npetro6/datasets", "label": "Kaggle Datasets", "platform": "Kaggle"},
    ]
    tab_packages = [
        {"url": tab_nodes.get("pypi", ""), "label": "fedcomp-index", "desc": "Scoring engine + bundled data"},
        {"url": tab_nodes.get("npm", ""), "label": "fedcomp-index", "desc": "Node.js package"},
    ]
    tab_sources = [
        {"url": tab_nodes.get("github", ""), "label": "GitHub", "desc": "Source code, wiki, releases"},
        {"url": tab_nodes.get("docker", ""), "label": "Docker Hub", "desc": "Containerized classification engine"},
        {"url": tab_nodes.get("methodology", ""), "label": "Methodology", "desc": "Scoring methodology"},
        {"url": SITE_URL + "/faq/", "label": "FAQ", "desc": "Definitions and common questions"},
    ]
    # Filter out empty URLs
    tab_datasets = [d for d in tab_datasets if d["url"]]
    tab_packages = [p for p in tab_packages if p["url"]]
    tab_sources = [s for s in tab_sources if s["url"]]
    out = tmpl.render(**ctx_base, datasets=tab_datasets, packages=tab_packages, sources=tab_sources)
    tab_dir = DIST_DIR / "tabularium"
    tab_dir.mkdir(parents=True, exist_ok=True)
    (tab_dir / "index.html").write_text(out, encoding="utf-8")
    print(f"Built: /tabularium/")

    # ─── Wiki articles ────────────────────────────────────────────────
    wiki_content_dir = BASE_DIR / "content" / "wiki"
    wiki_articles = []
    if wiki_content_dir.exists():
        for md_file in sorted(wiki_content_dir.glob("*.md")):
            raw = md_file.read_text(encoding="utf-8")
            # Parse frontmatter
            if raw.startswith("---"):
                parts = raw.split("---", 2)
                if len(parts) >= 3:
                    fm_block = parts[1].strip()
                    body_raw = parts[2].strip()
                else:
                    continue
            else:
                continue
            fm = {}
            for line in fm_block.splitlines():
                if ":" in line:
                    key, val = line.split(":", 1)
                    fm[key.strip()] = val.strip()
            if not fm.get("title") or not fm.get("slug"):
                continue
            # Convert paragraphs to HTML
            paragraphs = [p.strip() for p in body_raw.split("\n\n") if p.strip()]
            body_html = "\n".join(f"<p>{p}</p>" for p in paragraphs)
            wiki_articles.append({
                "title": fm["title"],
                "slug": fm["slug"],
                "term": fm.get("term", ""),
                "body": body_html,
            })

    # Render each wiki article
    if wiki_articles:
        wiki_tmpl = env.get_template("wiki_article.html")
        for article in wiki_articles:
            # Related = other articles with same term, excluding self
            related = [
                {"title": a["title"], "slug": a["slug"]}
                for a in wiki_articles
                if a["term"] == article["term"] and a["slug"] != article["slug"]
            ]
            out = wiki_tmpl.render(
                **ctx_base,
                title=article["title"],
                body=article["body"],
                slug=article["slug"],
                related_articles=related,
            )
            article_dir = DIST_DIR / "wiki" / article["slug"]
            article_dir.mkdir(parents=True, exist_ok=True)
            (article_dir / "index.html").write_text(out, encoding="utf-8")
        print(f"Built: {len(wiki_articles)} wiki articles")

        # Wiki index page
        wiki_idx_tmpl = env.get_template("wiki_index.html")
        out = wiki_idx_tmpl.render(
            **ctx_base,
            articles=[{"title": a["title"], "slug": a["slug"], "term": a["term"]} for a in wiki_articles],
        )
        wiki_index_dir = DIST_DIR / "wiki"
        wiki_index_dir.mkdir(parents=True, exist_ok=True)
        (wiki_index_dir / "index.html").write_text(out, encoding="utf-8")
        print(f"Built: /wiki/ (index)")

    # ─── robots.txt ───────────────────────────────────────────────────
    robots = f"User-agent: *\nAllow: /\nDisallow: /spectators/\nSitemap: {SITE_URL}/sitemap.xml\n"
    (DIST_DIR / "robots.txt").write_text(robots, encoding="utf-8")

    # ─── sitemap.xml ──────────────────────────────────────────────────
    urls = [SITE_URL + "/"]
    urls.append(SITE_URL + "/" + state_lower + "/")
    urls.append(SITE_URL + "/" + state_lower + "/rankings/")
    urls.append(SITE_URL + "/methodology/")
    urls.append(SITE_URL + "/contact/")
    urls.append(SITE_URL + "/tabularium/")
    urls.append(SITE_URL + "/faq/")
    if wiki_articles:
        urls.append(SITE_URL + "/wiki/")
        for article in wiki_articles:
            urls.append(SITE_URL + "/wiki/" + article["slug"] + "/")
    for c in contractors:
        urls.append(SITE_URL + "/dossier/" + c["slug"] + "/")


    lastmod = DATA_DATE
    # Build sitemap with image extensions for dossier pages
    sitemap_lines = []
    dossier_slugs = {c["slug"] for c in contractors}
    for u in urls:
        # Check if this URL is a dossier page
        slug_match = None
        for s in dossier_slugs:
            if f"/dossier/{s}/" in u:
                slug_match = s
                break
        if slug_match:
            img_url = f"{SITE_URL}/static/cards/{slug_match}.jpg"
            c_match = next((c for c in contractors if c["slug"] == slug_match), None)
            caption = ""
            if c_match:
                from xml.sax.saxutils import escape as xml_escape
                caption = xml_escape(f"{c_match['name']} FedComp Index Posture Card - {c_match.get('posture_class', '')}, Rank #{c_match.get('rank', '')}")
            sitemap_lines.append(
                f'  <url><loc>{u}</loc><lastmod>{lastmod}</lastmod><changefreq>weekly</changefreq>'
                f'<image:image><image:loc>{img_url}</image:loc>'
                f'<image:caption>{caption}</image:caption></image:image></url>'
            )
        else:
            sitemap_lines.append(
                f"  <url><loc>{u}</loc><lastmod>{lastmod}</lastmod><changefreq>weekly</changefreq></url>"
            )
    sitemap_entries = "\n".join(sitemap_lines)
    sitemap = f"""<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9"
        xmlns:image="http://www.google.com/schemas/sitemap-image/1.1">
{sitemap_entries}
</urlset>"""
    (DIST_DIR / "sitemap.xml").write_text(sitemap, encoding="utf-8")

    print(f"Built: sitemap.xml ({len(urls)} URLs)")
    print(f"Built: robots.txt")

    # ─── search index ────────────────────────────────────────────────
    search_data = [
        {"n": c["name"], "s": c["slug"], "cl": c["posture_class"], "u": c.get("uei", "")}
        for c in contractors
    ]
    (DIST_DIR / "static" / "search.json").write_text(json.dumps(search_data), encoding="utf-8")
    print(f"Built: search.json ({len(search_data)} entries)")

    uei_index = {
        c["uei"]: {"posture_class": c["posture_class"], "slug": c["slug"]}
        for c in contractors if c.get("uei")
    }
    (DIST_DIR / "static" / "uei_index.json").write_text(json.dumps(uei_index), encoding="utf-8")
    print(f"Built: uei_index.json ({len(uei_index)} entries)")


    # ─── Spectators page ────────────────────────────────────────────
    tmpl = env.get_template("spectators.html")
    out = tmpl.render(**ctx_base)
    spec_dir = DIST_DIR / "spectators"
    spec_dir.mkdir(parents=True, exist_ok=True)
    (spec_dir / "index.html").write_text(out, encoding="utf-8")
    print(f"Built: /spectators/")

    print(f"\nOutput: {DIST_DIR}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--state", default="NV")
    parser.add_argument("--data", default=None)
    args = parser.parse_args()

    data_path = args.data or BASE_DIR / "data" / f"{args.state.lower()}_scored.json"
    build(args.state, data_path)
