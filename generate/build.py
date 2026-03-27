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
        ("pypi_data", "Access pre-scored contractor data via Python"),
        ("pypi_scoring", "Compute FedComp Index scores with the scoring engine"),
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

STATE_NAMES = {
    "NV": "Nevada", "TX": "Texas", "CA": "California",
    "AZ": "Arizona", "FL": "Florida",
}
DEPLOYED_STATES = [
    {"code": "nv", "name": "Nevada", "rankings": True},
]
API_LIVE = os.getenv("API_LIVE", "1") == "1"


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
    score = c["score"]
    rank = c["rank"]
    awards = c["award_count"]
    value = c["awards_5yr_m"]
    cls = c["posture_class"]
    certs = c.get("certifications", [])
    contracts = c.get("contracts", [])
    proximity = c.get("proximity", [])
    active = c.get("active_contracts", 0)
    last_award = c.get("last_award_date", "")

    # ── Opening (5 variants: class x rank position) ──────────────────
    top_10 = rank <= 10
    if cls == "Class 1" and top_10:
        parts.append(f"{name} scores {score} out of 100 on the FedComp Index, ranking #{rank} of {total} scored {state_name} contractors.")
    elif cls == "Class 1":
        parts.append(f"{name} is a Class 1 {state_name} federal contractor with a FedComp Index score of {score}, ranked #{rank} of {total}.")
    elif cls == "Class 2" and rank <= total // 4:
        parts.append(f"{name} is a {state_name}-based federal contractor ranked #{rank} of {total} with a FedComp Index score of {score}.")
    elif cls == "Class 2":
        parts.append(f"Ranked #{rank} among {total} {state_name} federal contractors, {name} carries a FedComp Index score of {score}.")
    else:
        parts.append(f"{name} holds position #{rank} among {total} {state_name} federal contractors scored by the FedComp Index at {score}.")

    # ── Value (6 variants) ───────────────────────────────────────────
    if value >= 500:
        parts.append(f"The company has won ${value}M in federal contracts over the past five years across {awards} awards.")
    elif value >= 100:
        parts.append(f"Five-year federal contract volume totals ${value}M from {awards} awards.")
    elif value >= 50:
        parts.append(f"Over five years, {name} has secured ${value}M in federal contract value from {awards} awards.")
    elif value >= 10:
        parts.append(f"Federal contract history shows ${value}M across {awards} awards in the past five years.")
    elif value >= 1:
        parts.append(f"Award records total ${value}M from {awards} federal contracts over five years.")
    else:
        parts.append(f"Contract records show ${value}M in federal awards over the five-year scoring window.")

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


def score_class(score):
    if score >= 60:
        return "good"
    elif score >= 40:
        return "warn"
    else:
        return "bad"


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
            "score": c["score"],
            "score_class": c["score_class"],
            "total_m": round(total / 1_000_000, 1),
            "count": len(recent),
        })
    results.sort(key=lambda x: x["total_m"], reverse=True)
    return results[:n]


def build(state, data_path):
    env = Environment(loader=FileSystemLoader(str(TEMPLATES_DIR)), autoescape=True)
    env.filters['score_class'] = score_class

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

    scores = [c["score"] for c in contractors if c.get("score") is not None]
    total_volume_m = sum(c.get("awards_5yr_m", 0) for c in contractors)
    if total_volume_m >= 1000:
        total_volume_fmt = f"${round(total_volume_m / 1000, 1)}B"
    else:
        total_volume_fmt = f"${round(total_volume_m)}M"

    stats = {
        "total": len(contractors),
        "total_registered": total_registered,
        "avg_score": round(sum(scores) / len(scores), 1) if scores else 0,
        "class_1_count": sum(1 for c in contractors if c.get("posture_class") == "Class 1"),
        "win_pct": round(len(contractors) / total_registered * 100, 1) if total_registered else 0,
        "total_volume": total_volume_fmt,
    }

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
                "score": c["score"],
                "score_class": score_class(c["score"]),
                "posture_class": c["posture_class"],
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
    score_dist = [0] * 10
    for c in contractors:
        if c.get("score") is None:
            continue
        score_dist[min(9, c["score"] // 10)] += 1

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
    }

    # Top 10 contractors by value
    top_contractors = [
        {"name": c["name"][:28], "value_m": c["awards_5yr_m"]}
        for c in contractors[:10]
    ]

    # Cert coverage - how many contractors hold each cert
    cert_coverage = {}
    for cert in CERT_COLS:
        cert_coverage[cert] = len(cert_groups[cert])

    charts_data = {
        "score_dist": score_dist,
        "top_naics": [{"code": k, "value_m": round(v / 1e6, 1)} for k, v in top_naics],
        "top_agencies": [{"name": k.replace("Department of the ", "").replace("Department of ", ""), "value_m": round(v / 1e6, 1)} for k, v in top_agencies],
        "class_dist": class_dist,
        "top_contractors": top_contractors,
        "cert_coverage": cert_coverage,
    }

    # ─── Landing page ─────────────────────────────────────────────────
    tmpl = env.get_template("state_landing.html")
    out = tmpl.render(**ctx_base, stats=stats, top_by_cert=top_by_cert, top_performers=top_performers, charts_data=charts_data)
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
        if c.get("score") is None:
            print(f"WARNING: skipping contractor missing score: {c.get('name')} ({c.get('uei')})")
            continue
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

    # ─── Methodology page ─────────────────────────────────────────────
    tmpl = env.get_template("methodology.html")
    out = tmpl.render(**ctx_base)
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
        {"url": tab_nodes.get("huggingface_contractors", ""), "label": f"{state_name} Federal Contractors", "platform": "HuggingFace"},
        {"url": tab_nodes.get("huggingface_awards", ""), "label": f"{state_name} Federal Awards (Monthly)", "platform": "HuggingFace"},
        {"url": tab_nodes.get("kaggle_contractors", ""), "label": f"{state_name} Federal Contractors", "platform": "Kaggle"},
        {"url": tab_nodes.get("kaggle_awards", ""), "label": f"{state_name} Federal Awards (Monthly)", "platform": "Kaggle"},
    ]
    tab_packages = [
        {"url": tab_nodes.get("pypi", ""), "label": "fedcomp-index", "desc": "Meta-package"},
        {"url": tab_nodes.get("pypi_scoring", ""), "label": "fedcomp-index-scoring", "desc": "Scoring engine"},
        {"url": tab_nodes.get("pypi_data", ""), "label": "fedcomp-index-data", "desc": "Pre-scored datasets"},
    ]
    tab_sources = [
        {"url": tab_nodes.get("github", ""), "label": "GitHub", "desc": "Source code, wiki, releases"},
        {"url": tab_nodes.get("docker", ""), "label": "Docker Hub", "desc": "Containerized scoring engine"},
        {"url": tab_nodes.get("methodology", ""), "label": "Methodology", "desc": "Scoring methodology"},
        {"url": SITE_URL + "/faq/", "label": "FAQ", "desc": "Definitions and common questions"},
        {"url": tab_nodes.get("glossary", ""), "label": "Glossary", "desc": "Terminology and definitions"},
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
                caption = xml_escape(f"{c_match['name']} FedComp Index Posture Card - Score {c_match['score']}/100, {c_match.get('posture_class', '')}")
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
        {"n": c["name"], "s": c["slug"], "sc": c["score"], "cl": c["posture_class"], "ca": c.get("cage", "")}
        for c in contractors
    ]
    (DIST_DIR / "static" / "search.json").write_text(json.dumps(search_data), encoding="utf-8")
    print(f"Built: search.json ({len(search_data)} entries)")

    uei_index = {
        c["uei"]: {"score": c["score"], "posture_class": c["posture_class"], "slug": c["slug"]}
        for c in contractors if c.get("uei")
    }
    (DIST_DIR / "static" / "uei_index.json").write_text(json.dumps(uei_index), encoding="utf-8")
    print(f"Built: uei_index.json ({len(uei_index)} entries)")

    cage_index = {
        c["cage"]: {"name": c["name"], "slug": c["slug"]}
        for c in contractors if c.get("cage")
    }
    (DIST_DIR / "static" / "cage_index.json").write_text(json.dumps(cage_index), encoding="utf-8")
    print(f"Built: cage_index.json ({len(cage_index)} entries)")

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
