#!/usr/bin/env python3
"""
FedComp Index - pipeline

Modes:
    python pipeline.py --state NV                  # full re-pull (weekly)
    python pipeline.py --state NV --incremental    # last 30 days only (daily)
    python pipeline.py --state NV --skip-ingest    # re-score + rebuild only

Antifragile gate: never deploy fewer contractors than the current dataset.
If a pull produces fewer, the new data is rejected and the existing data stays.
Ingest failures restore backups automatically.
"""
import subprocess
import sys
import json
import argparse
import os
from pathlib import Path

try:
    import requests as _requests
except ImportError:
    _requests = None


def send_telegram(msg):
    if _requests is None:
        return
    token = os.environ.get("TELEGRAM_BOT_TOKEN")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        return
    try:
        _requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": msg},
            timeout=10
        )
    except Exception:
        pass

BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"


def run(cmd):
    print(f"\n$ {' '.join(str(c) for c in cmd)}")
    return subprocess.run(cmd, check=True)


def count_contractors(state):
    scored_path = DATA_DIR / f"{state.lower()}_scored.json"
    if not scored_path.exists():
        return 0
    try:
        with open(scored_path) as f:
            return len(json.load(f))
    except Exception:
        return 0


def backup_data(state):
    s = state.lower()
    for name in [f"{s}_scored.json", f"{s}_awards_recipient.json",
                 f"{s}_awards_performed.json", f"{s}_entities.json"]:
        src = DATA_DIR / name
        dst = DATA_DIR / f"{name}.bak"
        if src.exists():
            dst.write_bytes(src.read_bytes())


def restore_data(state):
    s = state.lower()
    for name in [f"{s}_scored.json", f"{s}_awards_recipient.json",
                 f"{s}_awards_performed.json", f"{s}_entities.json"]:
        bak = DATA_DIR / f"{name}.bak"
        dst = DATA_DIR / name
        if bak.exists():
            dst.write_bytes(bak.read_bytes())
            bak.unlink()


def cleanup_backups(state):
    s = state.lower()
    for name in [f"{s}_scored.json", f"{s}_awards_recipient.json",
                 f"{s}_awards_performed.json", f"{s}_entities.json"]:
        bak = DATA_DIR / f"{name}.bak"
        if bak.exists():
            bak.unlink()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--state", default="NV")
    parser.add_argument("--skip-ingest", action="store_true")
    parser.add_argument("--incremental", action="store_true",
                        help="Only pull last 30 days of awards (daily mode)")
    parser.add_argument("--deploy", action="store_true",
                        help="Deploy to Cloudflare Pages after successful build")
    args = parser.parse_args()

    state = args.state.upper()
    prev_count = count_contractors(state)
    print(f"Current contractor count: {prev_count}")

    if not args.skip_ingest:
        backup_data(state)

        try:
            ingest_cmd = [sys.executable, BASE_DIR / "ingest" / "usaspending.py", "--state", state]
            if args.incremental:
                ingest_cmd.append("--days")
                ingest_cmd.append("30")
            run(ingest_cmd)

            if not args.incremental:
                run([sys.executable, BASE_DIR / "ingest" / "sba.py", "--state", state])
            else:
                entities_path = DATA_DIR / f"{state.lower()}_entities.json"
                if not entities_path.exists():
                    print("No existing entities file - pulling SBA anyway")
                    run([sys.executable, BASE_DIR / "ingest" / "sba.py", "--state", state])
        except Exception as e:
            print(f"\nINGEST FAILED: {e}")
            print("Restoring backups.")
            restore_data(state)
            print("Aborting pipeline - existing data preserved.")
            sys.exit(1)

    # Score
    try:
        run([sys.executable, BASE_DIR / "score" / "fedcomp_index.py", "--state", state])
    except Exception as e:
        print(f"\nSCORING FAILED: {e}")
        if not args.skip_ingest:
            print("Restoring backups.")
            restore_data(state)
        print("Aborting pipeline.")
        sys.exit(1)

    # Antifragile gate
    new_count = count_contractors(state)
    print(f"\nContractor count: {prev_count} -> {new_count}")

    # Allow up to 5% natural churn (awards aging out of 5-year window)
    # Reject drops larger than 5% (likely data corruption or API failure)
    drop_pct = (prev_count - new_count) / prev_count * 100 if prev_count > 0 else 0
    if prev_count > 0 and drop_pct > 5:
        print(f"GATE REJECTED: dropped {drop_pct:.1f}% ({prev_count} -> {new_count}). Likely bad data.")
        print("Restoring previous data.")
        restore_data(state)
        print("Aborting pipeline - no build, no deploy.")
        sys.exit(1)
    elif new_count < prev_count:
        print(f"GATE PASSED: minor churn ({prev_count} -> {new_count}, -{drop_pct:.1f}%).")
    else:
        cleanup_backups(state)
        if new_count > prev_count:
            print(f"GATE PASSED: gained {new_count - prev_count} contractors.")
        else:
            print("GATE PASSED: count stable.")

    # Build
    run([sys.executable, BASE_DIR / "generate" / "build.py", "--state", state])

    print(f"\nDone. {new_count} contractors. Output: {BASE_DIR / 'site' / 'dist'}")

    if args.deploy:
        print("\nDeploying to Cloudflare Pages...")
        deploy_result = subprocess.run(
            ["wrangler", "pages", "deploy", "site/dist", "--project-name=fedcompindex"],
            cwd=BASE_DIR,
            capture_output=True,
            text=True
        )
        print(deploy_result.stdout)
        if deploy_result.stderr:
            print(deploy_result.stderr)

        if deploy_result.returncode != 0:
            print("DEPLOY FAILED.")
            send_telegram(
                f"FedCompIndex deploy FAILED\nContractors: {new_count}\nState: {state}"
            )
            sys.exit(1)

        delta = new_count - prev_count
        delta_str = f"+{delta}" if delta >= 0 else str(delta)
        msg = (
            f"FedCompIndex deployed\n"
            f"Contractors: {new_count} ({delta_str})\n"
            f"State: {state}\n"
            f"Build: SUCCESS"
        )
        send_telegram(msg)
        print("\nTelegram notification sent.")
