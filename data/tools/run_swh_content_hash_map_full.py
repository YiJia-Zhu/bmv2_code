#!/usr/bin/env python3
"""Resume the bounded SWH content hash join across all 128 ORC objects."""

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import datetime as dt
import json
import os
import subprocess
import sys
import time
from pathlib import Path


SOURCE_PREFIX = "graph/2026-06-04/orc/content/"
SOURCE_BUCKET = "softwareheritage"
JOB_NAME = "swh-p4-content-hash-map-full"
INPUT_URI = ("s3://swh2p4/preprocess/swh-p4-20260604/dedup-v1/runs/"
             "full-128-20261019-v1/unique_content_ids")
RUNS_URI = "s3://swh2p4/preprocess/swh-p4-20260604/content-hash-map-v1/runs"
MERGED_URI = "s3://swh2p4/preprocess/swh-p4-20260604/content-hash-map-v1/full-128-20261020"
PILOT_KEY = SOURCE_PREFIX + "content-00c775c4-5fd7-4ce8-bce8-c29c9082ea4e.orc"
PILOT_OUTPUT = RUNS_URI + "/pilot-one-orc-20261020"


def now():
    return dt.datetime.now(dt.timezone.utc).isoformat()


def aws(*args):
    attempts = 1 if args[:2] == ("glue", "start-job-run") else 3
    for attempt in range(attempts):
        try:
            completed = subprocess.run(["aws"] + list(args), capture_output=True, text=True,
                                       timeout=180, env=dict(os.environ, AWS_PAGER=""))
            if completed.returncode == 0:
                return completed.stdout
            message = completed.stderr.strip()[:500]
        except subprocess.TimeoutExpired:
            message = "request timed out"
        if attempt + 1 < attempts:
            time.sleep(2 ** attempt)
    raise RuntimeError("aws {} failed: {}".format(" ".join(args[:2]), message))


def save_state(path, state):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(state, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(str(temporary), str(path))


def discover():
    result = json.loads(aws("s3api", "list-objects-v2", "--bucket", SOURCE_BUCKET,
                           "--prefix", SOURCE_PREFIX, "--output", "json"))
    if result.get("IsTruncated"):
        raise RuntimeError("content ORC list truncated")
    items = sorted((item["Key"], item["Size"]) for item in result.get("Contents", [])
                   if item["Key"].endswith(".orc"))
    if len(items) != 128 or PILOT_KEY not in dict(items):
        raise RuntimeError("expected 128 content ORC objects including pilot, found {}".format(len(items)))
    return items


def initialize(state_path):
    items = discover()
    if state_path.exists():
        state = json.loads(state_path.read_text(encoding="utf-8"))
        if [(entry["key"], entry["size"]) for entry in state["shards"]] != items:
            raise RuntimeError("content ORC listing changed since the run started")
        return state
    shards = []
    for index, (key, size) in enumerate(items):
        shards.append({"index": index, "key": key, "size": size, "status": "pending",
                       "attempt": 0})
    state = {"schema_version": 1, "created_at_utc": now(), "job_name": JOB_NAME,
             "merged_uri": MERGED_URI, "shards": shards}
    save_state(state_path, state)
    return state


def start(shard):
    attempt = shard["attempt"] + 1
    run_id = "full-128-20261020-{:03d}-a{}".format(shard["index"], attempt)
    output = RUNS_URI + "/" + run_id
    arguments = {
        "--INPUT_URI": INPUT_URI,
        "--CONTENT_URI": "s3://{}/{}".format(SOURCE_BUCKET, shard["key"]),
        "--OUTPUT_URI": output,
        "--RUN_ID": run_id,
        "--CONFIRM_SCAN": "true",
        "--EXPECTED_COUNT": "143211",
    }
    # Recover a submit that succeeded remotely but lost its response locally.
    history = json.loads(aws("glue", "get-job-runs", "--job-name", JOB_NAME,
                             "--max-results", "200", "--output", "json"))
    existing = next((job for job in history.get("JobRuns", [])
                     if job.get("Arguments", {}).get("--RUN_ID") == run_id), None)
    response = ({"JobRunId": existing["Id"]} if existing else json.loads(aws(
        "glue", "start-job-run", "--job-name", JOB_NAME,
        "--arguments", json.dumps(arguments), "--output", "json")))
    shard.update(status="running", attempt=attempt, run_id=run_id, output_uri=output,
                 job_run_id=response["JobRunId"], started_at_utc=now())
    print(json.dumps({"event": "started", "index": shard["index"],
                      "job_run_id": shard["job_run_id"], "at_utc": now()}), flush=True)


def finish(shard):
    run = json.loads(aws("glue", "get-job-run", "--job-name", JOB_NAME,
                        "--run-id", shard["job_run_id"], "--output", "json"))["JobRun"]
    status = run["JobRunState"]
    if status in ("SUCCEEDED", "FAILED", "STOPPED", "TIMEOUT", "ERROR", "EXPIRED"):
        shard["execution_seconds"] = run.get("ExecutionTime")
        shard["dpu_seconds"] = run.get("DPUSeconds")
        shard["ended_at_utc"] = now()
        if status == "SUCCEEDED":
            shard["status"] = "copy_pending"
        else:
            shard["status"] = "retry_pending" if shard["attempt"] < 3 else "failed"
            shard["error"] = str(run.get("ErrorMessage", ""))[:500]
        print(json.dumps({"event": "job_finished", "index": shard["index"],
                          "state": status, "attempt": shard["attempt"],
                          "dpu_seconds": shard["dpu_seconds"], "at_utc": now()}), flush=True)


def copy_matches(shard):
    source = shard["output_uri"] + "/matches/"
    target = MERGED_URI + "/matches/part-{:03d}/".format(shard["index"])
    aws("s3", "cp", source, target, "--recursive", "--only-show-errors")
    bucket, prefix = "swh2p4", target[len("s3://swh2p4/"):]
    copied = json.loads(aws("s3api", "list-objects-v2", "--bucket", bucket,
                            "--prefix", prefix, "--max-keys", "10", "--output", "json"))
    if not any(item["Key"].endswith(".parquet") for item in copied.get("Contents", [])):
        raise RuntimeError("missing merged Parquet shard after copy: " + target)
    shard["status"] = "completed"
    shard["merged_matches_uri"] = target
    print(json.dumps({"event": "shard_completed", "index": shard["index"],
                      "at_utc": now()}), flush=True)


def run(args):
    state = initialize(args.state)
    pilot = next(shard for shard in state["shards"] if shard["key"] == PILOT_KEY)
    if pilot["status"] == "pending":
        pilot.update(status="copy_pending", output_uri=PILOT_OUTPUT,
                     job_run_id="jr_9123ac44462338965d78840c32921abab745c333e805d53e47664d4b6cc4f771",
                     attempt=1, execution_seconds=430, dpu_seconds=860)
        save_state(args.state, state)
    started = time.monotonic()
    baseline_complete = sum(shard["status"] == "completed" for shard in state["shards"])
    while True:
        pending_copies = [shard for shard in state["shards"] if shard["status"] == "copy_pending"]
        if pending_copies:
            with ThreadPoolExecutor(max_workers=min(4, len(pending_copies))) as executor:
                futures = {executor.submit(copy_matches, shard): shard for shard in pending_copies}
                for future in as_completed(futures):
                    future.result()
                    save_state(args.state, state)
        for shard in state["shards"]:
            if shard["status"] == "running":
                finish(shard)
                save_state(args.state, state)
        completed = sum(shard["status"] == "completed" for shard in state["shards"])
        running = sum(shard["status"] == "running" for shard in state["shards"])
        failed = sum(shard["status"] == "failed" for shard in state["shards"])
        if completed + failed == 128:
            break
        parallel = args.initial_parallel if completed < 5 else args.max_parallel
        for shard in state["shards"]:
            if running >= parallel:
                break
            if shard["status"] in ("pending", "retry_pending"):
                try:
                    start(shard)
                except RuntimeError as exc:
                    print(json.dumps({"event": "start_deferred", "index": shard["index"],
                                      "error": str(exc), "at_utc": now()}), flush=True)
                    break
                save_state(args.state, state)
                running += 1
        elapsed = time.monotonic() - started
        dpu_seconds = sum(shard.get("dpu_seconds") or 0 for shard in state["shards"])
        completed_since_start = max(0, completed - baseline_complete)
        eta = (128 - completed) * elapsed / completed_since_start if completed_since_start else None
        print(json.dumps({"event": "progress", "completed": completed, "running": running,
                          "failed": failed, "total": 128, "elapsed_seconds": round(elapsed),
                          "dpu_seconds": dpu_seconds, "eta_seconds": round(eta) if eta else None,
                          "at_utc": now()}), flush=True)
        time.sleep(args.poll_seconds)
    state["finished_at_utc"] = now()
    save_state(args.state, state)
    if failed:
        raise RuntimeError("{} content ORC shards failed after retries".format(failed))
    print(json.dumps({"event": "all_hashes_mapped", "at_utc": now(),
                      "matches_uri": MERGED_URI + "/matches"}), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state", type=Path,
                        default=Path("data/raw/swh_p4/content_screen/full-128-20261020/map-state.json"))
    parser.add_argument("--initial-parallel", type=int, default=4)
    parser.add_argument("--max-parallel", type=int, default=32)
    parser.add_argument("--poll-seconds", type=int, default=30)
    args = parser.parse_args()
    if not 1 <= args.initial_parallel <= args.max_parallel <= 32 or args.poll_seconds < 10:
        parser.error("parallelism must be 1..32 and poll interval at least 10 seconds")
    for attempt in range(1, 4):
        try:
            run(args)
            return
        except Exception as exc:
            print(json.dumps({"event": "supervisor_error", "attempt": attempt,
                              "error": str(exc)[:1000], "at_utc": now()}), flush=True)
            if attempt == 3:
                raise
            time.sleep(30)


if __name__ == "__main__":
    sys.exit(main())
