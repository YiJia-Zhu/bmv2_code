#!/usr/bin/env python3
"""Wait for hash shards, screen all contents, then export every source hit to S3."""

import collections
import datetime as dt
import fcntl
import json
import os
import subprocess
import sys
import time
from pathlib import Path

from run_swh_content_hash_map_full import MERGED_URI, aws, save_state


ROOT = Path("data/raw/swh_p4/content_screen/full-128-20261020")
PREFIX = "s3://swh2p4/file-screen/swh-p4-20260604/full-128-20261020"
EXPORT_JOB = "swh-p4-screen-export-full"
PYTHON = sys.executable


def now():
    return dt.datetime.now(dt.timezone.utc).isoformat()


def publish(state):
    state["updated_at_utc"] = now()
    save_state(ROOT / "pipeline.json", state)
    aws("s3", "cp", str(ROOT / "pipeline.json"), PREFIX + "/pipeline.json", "--only-show-errors")
    if (ROOT / "map-state.json").exists():
        aws("s3", "cp", str(ROOT / "map-state.json"), PREFIX + "/map-state.json", "--only-show-errors")


def launch_screen():
    command = [PYTHON, "-u", "data/tools/swh_p4_content_screen.py", "--source", "s3",
               "--workers", "64", "--batch-size", "1000", "--max-bytes", str(10 * 1024 * 1024),
               "--retry-errors", "--hash-map-uri", MERGED_URI + "/matches",
               "--output-dir", str(ROOT / "screen"), "--s3-output-uri", PREFIX + "/content_annotations"]
    with (ROOT / "screen.log").open("a", encoding="utf-8") as handle:
        subprocess.run(command, stdout=handle, stderr=subprocess.STDOUT, check=True)


def launch_api_recovery():
    command = [PYTHON, "-u", "data/tools/swh_p4_recover_failed_content.py",
               "--output-dir", str(ROOT / "screen"),
               "--s3-output-uri", PREFIX + "/content_annotations",
               "--max-bytes", str(10 * 1024 * 1024), "--delay", "0.5"]
    with (ROOT / "api-recovery.log").open("a", encoding="utf-8") as handle:
        subprocess.run(command, stdout=handle, stderr=subprocess.STDOUT, check=True)


def screen_summary():
    from swh_p4_content_screen import load_content_ids_cached, s3_client

    expected = set(load_content_ids_cached(s3_client("us-east-1"),
        "s3://swh2p4/preprocess/swh-p4-20260604/dedup-v1/runs/full-128-20261019-v1/unique_content_ids",
        Path("data/raw/swh_p4/content_screen/index_cache")))
    seen = set()
    counts = collections.Counter()
    versions = collections.Counter()
    quality = collections.Counter()
    for path in sorted((ROOT / "screen").glob("batch-*.jsonl")):
        for line in path.read_text(encoding="utf-8").splitlines():
            row = json.loads(line)
            content_id = row["content_id"]
            if content_id in seen:
                raise RuntimeError("duplicate annotation: " + content_id)
            if row.get("fetch_status") in ("fetched", "cached") and not row.get("sha1_git_verified"):
                raise RuntimeError("unverified successful row: " + content_id)
            seen.add(content_id)
            counts[row["fetch_status"]] += 1
            versions[row["version_status"]] += 1
            quality[row["quality_status"]] += 1
    if seen != expected:
        raise RuntimeError("screen ID coverage mismatch: {} of {}".format(len(seen), len(expected)))
    return {"content_ids": len(seen), "fetch_status": dict(counts),
            "version_status": dict(versions), "quality_status": dict(quality)}


def run():
    ROOT.mkdir(parents=True, exist_ok=True)
    with (ROOT / "pipeline.lock").open("w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        state_path = ROOT / "pipeline.json"
        state = json.loads(state_path.read_text()) if state_path.exists() else {
            "schema_version": 1, "created_at_utc": now(), "phase": "hash_mapping",
            "output_prefix": PREFIX, "original_csv_count": 128,
            "expected_content_ids": 143211, "expected_source_rows": 14060834,
        }
        if state.get("phase") == "complete":
            return
        while state["phase"] == "hash_mapping":
            map_path = ROOT / "map-state.json"
            map_state = json.loads(map_path.read_text()) if map_path.exists() else None
            shards = map_state["shards"] if map_state else []
            complete = sum(shard["status"] == "completed" for shard in shards)
            failed = sum(shard["status"] == "failed" for shard in shards)
            state["mapping_completed_shards"] = complete
            state["mapping_running_shards"] = sum(shard["status"] == "running" for shard in shards)
            publish(state)
            print(json.dumps({"event": "waiting_for_mapping", "completed": complete,
                              "total": 128, "at_utc": now()}), flush=True)
            if complete == 128:
                state["phase"] = "content_screening"
                publish(state)
                break
            if failed:
                raise RuntimeError("hash shards exhausted retries; see map-state.json")
            alive = subprocess.run(["tmux", "has-session", "-t", "swh_p4_map_full"],
                                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode == 0
            if not alive:
                command = (PYTHON + " -u data/tools/run_swh_content_hash_map_full.py "
                           "--state " + str(map_path) + " --max-parallel 32 >> " +
                           str(ROOT / "map.log") + " 2>&1")
                subprocess.run(["tmux", "new-session", "-d", "-s", "swh_p4_map_full",
                                command], check=True)
                print(json.dumps({"event": "mapping_supervisor_restarted",
                                  "at_utc": now()}), flush=True)
            time.sleep(30)
        if state["phase"] == "content_screening":
            for attempt in range(1, 4):
                state["screen_attempt"] = attempt
                publish(state)
                print(json.dumps({"event": "screen_started", "attempt": attempt, "at_utc": now()}), flush=True)
                try:
                    launch_screen()
                except subprocess.CalledProcessError:
                    if attempt == 3:
                        raise
                    print(json.dumps({"event": "screen_process_retry", "attempt": attempt,
                                      "at_utc": now()}), flush=True)
                    time.sleep(30)
                    continue
                summary = screen_summary()
                state["screen_summary"] = summary
                publish(state)
                transient = sum(summary["fetch_status"].get(key, 0) for key in (
                    "network_error", "s3_error", "decompression_error", "hash_mismatch"))
                if not transient:
                    break
                time.sleep(30)
            state["phase"] = "api_recovery"
            publish(state)
        if state["phase"] == "api_recovery":
            launch_api_recovery()
            state["screen_summary"] = screen_summary()
            state["phase"] = "csv_export"
            publish(state)
        if state["phase"] == "csv_export":
            arguments = {"--SOURCE_URI": "s3://swh2p4/preprocess/swh-p4-20260604/dedup-v1/runs/full-128-20261019-v1/source_rows",
                         "--ANNOTATION_URI": PREFIX + "/content_annotations/batch-*.jsonl",
                         "--OUTPUT_URI": PREFIX + "/filtered"}
            if state.get("export_job_status") in ("FAILED", "STOPPED", "TIMEOUT", "ERROR", "EXPIRED"):
                if state.get("export_attempt", 1) >= 3:
                    raise RuntimeError("CSV export exhausted three attempts")
                state.pop("export_job_run_id", None)
                state.pop("export_job_status", None)
            if not state.get("export_job_run_id"):
                result = json.loads(aws("glue", "start-job-run", "--job-name", EXPORT_JOB,
                                        "--arguments", json.dumps(arguments), "--output", "json"))
                state["export_job_run_id"] = result["JobRunId"]
                state["export_attempt"] = state.get("export_attempt", 0) + 1
                publish(state)
            while True:
                job = json.loads(aws("glue", "get-job-run", "--job-name", EXPORT_JOB,
                                     "--run-id", state["export_job_run_id"], "--output", "json"))["JobRun"]
                status = job["JobRunState"]
                state["export_job_status"] = status
                publish(state)
                if status == "SUCCEEDED":
                    state["phase"] = "complete"
                    state["completed_at_utc"] = now()
                    state["selected_csv_uri"] = PREFIX + "/filtered/selected_csv/"
                    state["review_csv_uri"] = PREFIX + "/filtered/review_csv/"
                    state["excluded_csv_uri"] = PREFIX + "/filtered/excluded_csv/"
                    publish(state)
                    print(json.dumps({"event": "pipeline_completed", "at_utc": now(),
                                      "selected_csv_uri": state["selected_csv_uri"]}), flush=True)
                    break
                if status in ("FAILED", "STOPPED", "TIMEOUT", "ERROR", "EXPIRED"):
                    raise RuntimeError("CSV export {}: {}".format(status, job.get("ErrorMessage", "")))
                time.sleep(30)


def main():
    for attempt in range(1, 4):
        try:
            run()
            return
        except Exception as exc:
            print(json.dumps({"event": "pipeline_failed", "error": str(exc)[:1000],
                              "supervisor_attempt": attempt, "at_utc": now()}), flush=True)
            if attempt == 3:
                raise
            time.sleep(30)


if __name__ == "__main__":
    main()
