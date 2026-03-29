"""
FedComp Index - Posture Card Image Generator

Renders one 1200x630 PNG per contractor using Playwright.
Tabularium aesthetic with continuous variation per the Continuity axiom.

Usage:
    python generate/cards.py --data data/nv_scored.json
    python generate/cards.py --data data/nv_scored.json --slug fleet-vehicle-source-inc
    python generate/cards.py --data data/nv_scored.json --output site/dist/static/cards/
"""

import argparse
import base64
import hashlib
import html as html_mod
import json
import random
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
TEMPLATE_PATH = Path(__file__).resolve().parent / "card_template.html"
BG_DIR = BASE_DIR / "site" / "static" / "cards" / "bg"
DEFAULT_OUTPUT = BASE_DIR / "site" / "static" / "cards"


CLASS_HUES = {"Class 1": 170, "Class 2": 35, "Class 3": 220, "Class 4": 215}

def class_to_hue(posture_class):
    """Class-based color. Class 1=teal, Class 2=amber, Class 3=blue, Class 4=slate."""
    return CLASS_HUES.get(posture_class, 215)


def fmt_dollars(amount):
    if amount >= 1_000_000_000:
        return f"${amount / 1_000_000_000:.1f}B"
    if amount >= 1_000_000:
        return f"${amount / 1_000_000:.1f}M"
    if amount >= 10_000:
        return f"${amount / 1_000:.0f}K"
    if amount >= 1_000:
        return f"${amount / 1_000:.1f}K"
    return f"${amount:,.0f}"


def build_card_html(contractor, total, template, bg_data_uris):
    """Generate HTML string for one contractor with seeded continuous variation."""
    slug = contractor["slug"]
    seed = int(hashlib.md5(slug.encode()).hexdigest()[:8], 16)
    rng = random.Random(seed)

    posture_class = contractor.get("posture_class", "Class 4")
    name = contractor["name"]
    certs = contractor.get("certifications", [])

    # Pick background texture deterministically
    bg_path = bg_data_uris[seed % len(bg_data_uris)]

    # --- Continuous parameters ---
    bg_brightness = rng.uniform(0.6, 0.9)
    bg_saturate = rng.uniform(0.7, 1.3)

    vignette_x = rng.uniform(35, 65)
    vignette_y = rng.uniform(30, 50)
    vignette_opacity = rng.uniform(0.5, 0.75)

    noise_opacity = rng.uniform(0.03, 0.08)
    grain_freq = rng.uniform(0.5, 0.8)
    grain_seed = rng.randint(1, 9999)

    pad_top = rng.randint(40, 52)
    pad_right = rng.randint(44, 60)
    pad_bottom = rng.randint(36, 48)
    pad_left = rng.randint(48, 64)

    label_opacity = rng.uniform(0.4, 0.6)
    badge_border_opacity = rng.uniform(0.12, 0.25)
    tag_border_opacity = rng.uniform(0.15, 0.3)

    # Name sizing - auto-scale for long names
    name_len = len(name)
    if name_len <= 20:
        name_size = rng.uniform(44, 52)
    elif name_len <= 30:
        name_size = rng.uniform(38, 46)
    elif name_len <= 45:
        name_size = rng.uniform(32, 40)
    else:
        name_size = rng.uniform(26, 34)

    name_weight = rng.choice([600, 700, 700, 800])
    name_spacing = rng.uniform(-0.02, 0.02)
    name_opacity = rng.uniform(0.88, 0.96)
    name_glow = rng.uniform(0.04, 0.12)

    # Rule
    rule_width = rng.uniform(82, 96)
    rule_height = rng.uniform(1.0, 2.5)
    rule_opacity = rng.uniform(0.4, 0.7)
    rule_margin_top = rng.randint(6, 14)
    rule_margin_bottom = rng.randint(8, 16)

    # Class color
    hue = class_to_hue(posture_class)
    score_sat = rng.uniform(55, 80)
    score_light = rng.uniform(50, 65)
    score_color = f"hsl({hue:.0f}, {score_sat:.0f}%, {score_light:.0f}%)"

    score_size = rng.uniform(58, 72)
    score_glow = rng.uniform(15, 35)

    # Accent color - teal family
    accent_hue = rng.uniform(180, 210)
    accent_color = f"hsl({accent_hue:.0f}, 50%, 45%)"

    data_gap = rng.randint(20, 36)

    # Certs HTML
    if certs:
        cert_tags = "".join(f'<span class="cert-tag">{html_mod.escape(c)}</span>' for c in certs)
        certs_html = f'<div class="certs">{cert_tags}</div>'
    else:
        certs_html = ""

    # Format values
    volume_fmt = fmt_dollars(contractor.get("base_dollars_5yr", 0))
    contracts = contractor.get("base_contract_count", 0)
    velocity_dir = contractor.get("velocity", {}).get("direction", "stable")

    # HTML-escape contractor name and certs to prevent breakage
    name_safe = html_mod.escape(name)

    # Template substitution
    html = template
    replacements = {
        "{{BG_PATH}}": bg_path,
        "{{BG_BRIGHTNESS}}": f"{bg_brightness:.2f}",
        "{{BG_SATURATE}}": f"{bg_saturate:.2f}",
        "{{VIGNETTE_X}}": f"{vignette_x:.0f}",
        "{{VIGNETTE_Y}}": f"{vignette_y:.0f}",
        "{{VIGNETTE_OPACITY}}": f"{vignette_opacity:.2f}",
        "{{NOISE_OPACITY}}": f"{noise_opacity:.2f}",
        "{{GRAIN_FREQ}}": f"{grain_freq:.2f}",
        "{{GRAIN_SEED}}": str(grain_seed),
        "{{PAD_TOP}}": str(pad_top),
        "{{PAD_RIGHT}}": str(pad_right),
        "{{PAD_BOTTOM}}": str(pad_bottom),
        "{{PAD_LEFT}}": str(pad_left),
        "{{LABEL_OPACITY}}": f"{label_opacity:.2f}",
        "{{BADGE_BORDER_OPACITY}}": f"{badge_border_opacity:.2f}",
        "{{TAG_BORDER_OPACITY}}": f"{tag_border_opacity:.2f}",
        "{{NAME_SIZE}}": f"{name_size:.0f}",
        "{{NAME_WEIGHT}}": str(name_weight),
        "{{NAME_SPACING}}": f"{name_spacing:.3f}",
        "{{NAME_OPACITY}}": f"{name_opacity:.2f}",
        "{{NAME_GLOW}}": f"{name_glow:.2f}",
        "{{RULE_WIDTH}}": f"{rule_width:.0f}",
        "{{RULE_HEIGHT}}": f"{rule_height:.1f}",
        "{{RULE_OPACITY}}": f"{rule_opacity:.2f}",
        "{{RULE_MARGIN_TOP}}": str(rule_margin_top),
        "{{RULE_MARGIN_BOTTOM}}": str(rule_margin_bottom),
        "{{SCORE_COLOR}}": score_color,
        "{{SCORE_SIZE}}": f"{score_size:.0f}",
        "{{SCORE_GLOW}}": f"{score_glow:.0f}",
        "{{ACCENT_COLOR}}": accent_color,
        "{{DATA_GAP}}": str(data_gap),
        "{{CONTRACTOR_NAME}}": name_safe,
        "{{CERTS_HTML}}": certs_html,
        "{{POSTURE_CLASS}}": posture_class,
        "{{RANK}}": str(contractor.get("rank", "")),
        "{{TOTAL}}": str(total),
        "{{VOLUME}}": volume_fmt,
        "{{CONTRACTS}}": str(contracts),
        "{{VELOCITY}}": velocity_dir,
        "{{STATE_NAME}}": contractor.get("state_name", "Nevada").upper(),
    }

    for key, val in replacements.items():
        html = html.replace(key, val)

    return html


def _compress_to_jpeg(png_path, jpg_path, contractor, total):
    """Convert PNG screenshot to optimized JPEG with EXIF metadata."""
    try:
        from PIL import Image

        img = Image.open(png_path).convert("RGB")
        # Embed IPTC-style info via JPEG comment isn't standard,
        # but we can use PIL's info dict for EXIF-like data
        img.save(str(jpg_path), "JPEG", quality=CARD_QUALITY, optimize=True)
        png_path.unlink()  # delete temp PNG

        # Write XMP sidecar-style metadata into JPEG via piexif if available
        _embed_jpeg_metadata(jpg_path, contractor, total)
    except ImportError:
        # No PIL - just rename the PNG as fallback
        import shutil
        shutil.move(str(png_path), str(jpg_path))
        print("WARNING: Pillow not installed, cards saved as uncompressed PNG")


def _embed_jpeg_metadata(path, contractor, total):
    """Embed metadata into JPEG via Pillow EXIF."""
    try:
        import piexif
        name = contractor["name"]
        rank = contractor.get("rank", 0)
        cls = contractor.get("posture_class", "")
        slug = contractor["slug"]

        desc = f"{name}. FedComp Index {cls}. Rank #{rank} of {total} Nevada federal contractors."

        exif_dict = {"0th": {
            piexif.ImageIFD.ImageDescription: desc.encode(),
            piexif.ImageIFD.Artist: b"FedComp Index",
            piexif.ImageIFD.Copyright: b"FedComp Index",
        }}
        exif_bytes = piexif.dump(exif_dict)
        piexif.insert(exif_bytes, str(path))
    except ImportError:
        pass  # piexif not available, metadata skipped silently
    except Exception:
        pass  # non-critical


CARD_QUALITY = 85  # JPEG quality (85 = good balance of size/quality)
HASH_FILE_DIR = BASE_DIR / "generate"  # survives dist/ wipes
HASH_FILE = ".card_hashes.json"

NUM_WORKERS = 6


def _contractor_hash(c):
    """Hash the data fields that affect card rendering."""
    key = json.dumps({
        "slug": c["slug"], "name": c["name"],
        "posture_class": c.get("posture_class", ""),
        "rank": c.get("rank", ""),
        "base_dollars_5yr": c.get("base_dollars_5yr", 0),
        "base_contract_count": c.get("base_contract_count", 0),
        "certifications": c.get("certifications", []),
    }, sort_keys=True)
    return hashlib.md5(key.encode()).hexdigest()[:12]


def _load_hashes():
    p = HASH_FILE_DIR / HASH_FILE
    if p.exists():
        return json.loads(p.read_text())
    return {}


def _save_hashes(hashes):
    p = HASH_FILE_DIR / HASH_FILE
    p.write_text(json.dumps(hashes))


def _render_batch(batch, total, template, bg_data_uris, output_dir):
    """Render a batch of contractors in one browser page."""
    from playwright.sync_api import sync_playwright

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page(viewport={"width": 1200, "height": 630})

        for c in batch:
            html = build_card_html(c, total, template, bg_data_uris)
            page.set_content(html, wait_until="networkidle")
            out_path = Path(output_dir) / f"{c['slug']}.png"
            # Render as PNG then convert to optimized JPEG
            jpg_path = Path(output_dir) / f"{c['slug']}.jpg"
            png_tmp = Path(output_dir) / f"{c['slug']}.tmp.png"
            page.screenshot(path=str(png_tmp), type="png")
            _compress_to_jpeg(png_tmp, jpg_path, c, total)

        browser.close()

    return len(batch)


def generate_cards(contractors, output_dir, slug_filter=None, force=False):
    """Render posture card PNGs via Playwright. Parallel + skip-unchanged."""
    import concurrent.futures

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    template = TEMPLATE_PATH.read_text(encoding="utf-8")
    total = len(contractors)

    # Collect background textures as base64 data URIs
    bg_files = sorted(BG_DIR.glob("*.png"))
    if not bg_files:
        print("ERROR: No background textures found in", BG_DIR)
        sys.exit(1)
    bg_data_uris = []
    for bf in bg_files:
        b64 = base64.b64encode(bf.read_bytes()).decode()
        bg_data_uris.append(f"data:image/png;base64,{b64}")
    print(f"Loaded {len(bg_data_uris)} background textures")

    # Filter if single slug requested
    if slug_filter:
        contractors = [c for c in contractors if c["slug"] == slug_filter]
        if not contractors:
            print(f"ERROR: No contractor found with slug '{slug_filter}'")
            sys.exit(1)

    # Add rank if not present
    for i, c in enumerate(contractors):
        if "rank" not in c:
            c["rank"] = i + 1

    # Skip unchanged cards
    old_hashes = _load_hashes() if not force else {}
    new_hashes = {}
    to_render = []
    for c in contractors:
        h = _contractor_hash(c)
        new_hashes[c["slug"]] = h
        out_path = output_dir / f"{c['slug']}.jpg"
        if old_hashes.get(c["slug"]) == h and out_path.exists():
            continue  # unchanged, skip
        to_render.append(c)

    skipped = len(contractors) - len(to_render)
    if skipped:
        print(f"Skipping {skipped} unchanged cards")

    if not to_render:
        print("All cards up to date")
        _save_hashes(new_hashes)
        return

    print(f"Generating {len(to_render)} posture cards ({NUM_WORKERS} workers)...")

    # Split into batches for parallel rendering
    batches = [[] for _ in range(NUM_WORKERS)]
    for i, c in enumerate(to_render):
        batches[i % NUM_WORKERS].append(c)
    batches = [b for b in batches if b]  # remove empty

    with concurrent.futures.ThreadPoolExecutor(max_workers=len(batches)) as pool:
        futures = [
            pool.submit(_render_batch, batch, total, template, bg_data_uris, output_dir)
            for batch in batches
        ]
        done = 0
        for f in concurrent.futures.as_completed(futures):
            done += f.result()
            print(f"  {done}/{len(to_render)}")

    _save_hashes(new_hashes)
    print(f"Done: {len(to_render)} cards rendered, {skipped} skipped")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate FedComp Index posture card images")
    parser.add_argument("--data", default=str(BASE_DIR / "data" / "nv_scored.json"))
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT))
    parser.add_argument("--slug", default=None, help="Generate card for a single contractor")
    parser.add_argument("--force", action="store_true", help="Regenerate all cards ignoring cache")
    args = parser.parse_args()

    with open(args.data) as f:
        data = json.load(f)

    generate_cards(data, args.output, slug_filter=args.slug, force=args.force)
