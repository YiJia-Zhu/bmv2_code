#!/usr/bin/env python3
"""Read the collector's atomic progress snapshot without opening its database."""

import argparse
import json
import time
from pathlib import Path

from swh_history_common import duration


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path("data/raw/repositories/swh_full_history"))
    parser.add_argument("--watch", type=float, default=0, help="refresh interval in seconds; zero prints once")
    parser.add_argument("--active", action="store_true")
    args = parser.parse_args()
    while True:
        try:
            record = json.loads((args.root / "status.json").read_text())
            print("\n{}  elapsed={}  Git ETA~{}".format(
                record["updated_at"], duration(record["elapsed_s"]), duration(record["git_eta_s"])))
            print("Git processed={}/{} ({:.2f}%) verified={} status={}".format(
                record["git_terminal"], record["git_repositories"], record["git_progress_percent"],
                record["mirror_complete"], record["git_status"]))
            if "github_total" in record:
                print("GitHub processed={}/{} verified={} observed={:.2f} repos/min".format(
                    record["github_terminal"], record["github_total"], record["github_verified"],
                    record["github_repos_per_min"]))
            print("Mirrors={:.3f} GiB; commits={:,}; local payload growth={} MiB/s; free={} GiB".format(
                record["mirror_bytes"] / 1024 ** 3, record["reachable_commits"],
                record["local_payload_growth_mib_s"], record["free_gib"]))
            print("API: {}; active={}; pages/min={}; current queue ETA~{} "
                  "(newly discovered pages can extend ETA)".format(
                      record["api_status"], record.get("api_active", "unknown"),
                      record.get("api_pages_per_min", "unknown"), duration(record.get("api_eta_s"))))
            print("Non-Git: {}; archives: {}".format(record["non_git_status"], record["asset_status"]))
            print("Per-host Git concurrency: {}".format(record["host_limits"]))
            print("Stopped/final: {}; reason: {}".format(record["final"], record["pause_reason"]))
            if args.active:
                for task in record["active_repositories"].values():
                    print("  {} [{}] {}".format(task["url"], task["phase"], task["last_progress"]))
        except (OSError, ValueError) as exc:
            print("Progress snapshot not yet available: {}".format(exc))
        if args.watch <= 0:
            break
        time.sleep(args.watch)


if __name__ == "__main__":
    main()
