#!/usr/bin/env python3
"""Prepare and run a bounded SWH Graph API probe from the full parent index."""

import argparse
import csv
import datetime as dt
import hashlib
import json
import os
import re
import socket
import time
import urllib.error
import urllib.parse
import urllib.request


SOURCE_URI = (
    "s3://swh2p4/file-screen/swh-p4-20260604/full-128-20261020/"
    "candidate-parent-index-v1/parent_directory_csv/"
)
GRAPH_API = "https://archive.softwareheritage.org/api/1/graph"
EXPECTED_DIRECTORIES = 65321
EXPECTED_HITS = 13465757
HEX40 = re.compile(r"^[0-9a-f]{40}$")
GROUPS = (
    ("one", 1, 1),
    ("few", 2, 9),
    ("tens", 10, 49),
    ("hundreds", 100, 199),
    ("many", 500, 999),
    ("thousands", 1000, 9999),
    ("extreme", 10000, None),
)


def utc_now():
    return dt.datetime.now(dt.timezone.utc).isoformat()


def prepare(index_csv, work_dir):
    os.makedirs(work_dir, exist_ok=True)
    digest = hashlib.sha256()
    with open(index_csv, "rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    selected = {}
    all_ids = set()
    total_hits = 0
    with open(index_csv, newline="", encoding="utf-8-sig") as source:
        reader = csv.DictReader(source)
        if reader.fieldnames != ["directory_id", "candidate_hit_count"]:
            raise ValueError("unexpected parent index columns")
        for row in reader:
            directory_id = row["directory_id"]
            hits = int(row["candidate_hit_count"])
            if not HEX40.fullmatch(directory_id) or hits < 1 or directory_id in all_ids:
                raise ValueError("invalid or duplicate direct parent ID")
            all_ids.add(directory_id)
            total_hits += hits
            for group, lower, upper in GROUPS:
                if hits >= lower and (upper is None or hits <= upper):
                    current = selected.get(group)
                    if current is None or directory_id < current["directory_id"]:
                        selected[group] = {"group": group, "directory_id": directory_id,
                                           "candidate_hit_count": hits}
                    break
    if len(all_ids) != EXPECTED_DIRECTORIES or total_hits != EXPECTED_HITS:
        raise ValueError("full parent index totals do not match validated Glue output")
    if set(selected) != {group for group, _, _ in GROUPS}:
        raise ValueError("a probe size group has no representative")

    manifest = {
        "prepared_at_utc": utc_now(),
        "source_uri": SOURCE_URI,
        "source_csv_sha256": digest.hexdigest(),
        "direct_parent_count": len(all_ids),
        "candidate_hit_count": total_hits,
        "graph_release_of_source": "2026-06-04",
        "graph_api_release_verified": False,
        "sample_selection": "lexicographically smallest directory ID in each size group",
        "samples": [selected[group] for group, _, _ in GROUPS],
    }
    with open(os.path.join(work_dir, "manifest.json"), "w", encoding="utf-8") as output:
        json.dump(manifest, output, ensure_ascii=False, indent=2, sort_keys=True)
        output.write("\n")
    with open(os.path.join(work_dir, "directory_ids.txt"), "w", encoding="ascii") as output:
        for directory_id in sorted(all_ids):
            output.write(directory_id + "\n")
    with open(os.path.join(work_dir, "sample.csv"), "w", newline="", encoding="utf-8") as output:
        writer = csv.DictWriter(output, fieldnames=("group", "directory_id",
                                                     "candidate_hit_count"))
        writer.writeheader()
        writer.writerows(manifest["samples"])
    return manifest


def read_token(env_file):
    token = os.environ.get("SWH_API_TOKEN", "").strip()
    if token:
        return token
    if not env_file or not os.path.isfile(env_file):
        return ""
    with open(env_file, encoding="utf-8") as source:
        for line in source:
            key, sep, value = line.strip().partition("=")
            if sep and key.strip() == "SWH_API_TOKEN":
                return value.strip().strip("\"'")
    return ""


def request_origins(directory_id, token, timeout, max_response_bytes):
    query = urllib.parse.urlencode({
        "direction": "backward", "edges": "*", "return_types": "ori",
        "max_edges": "0", "max_matching_nodes": "0",
    })
    url = "{}/leaves/swh:1:dir:{}/?{}".format(GRAPH_API, directory_id, query)
    request = urllib.request.Request(url, headers={
        "Accept": "text/plain",
        "User-Agent": "swh-p4-directory-probe/1.0",
        "Authorization": "Bearer " + token,
    })
    started = time.monotonic()
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            body = response.read(max_response_bytes + 1)
            headers = response.headers
            code = response.status
    except urllib.error.HTTPError as error:
        return {"status": "http_error", "http_status": error.code,
                "elapsed_seconds": round(time.monotonic() - started, 3),
                "retry_after": error.headers.get("Retry-After")}
    except (urllib.error.URLError, socket.timeout, TimeoutError) as error:
        return {"status": "network_error", "error_type": type(error).__name__,
                "elapsed_seconds": round(time.monotonic() - started, 3)}
    result = {
        "http_status": code,
        "elapsed_seconds": round(time.monotonic() - started, 3),
        "response_bytes_read": len(body),
        "rate_limit": headers.get("X-Ratelimit-Limit"),
        "rate_remaining": headers.get("X-Ratelimit-Remaining"),
        "rate_reset": headers.get("X-Ratelimit-Reset"),
    }
    if len(body) > max_response_bytes:
        result["status"] = "response_too_large"
        return result
    lines = [line.strip().lower() for line in body.decode("utf-8", "replace").splitlines()
             if line.strip()]
    parsed = [line.split(":")[-1] for line in lines
              if line.startswith("swh:1:ori:") and
              HEX40.fullmatch(line.split(":")[-1])]
    origins = sorted(set(parsed))
    result["origin_ids"] = origins
    result["origin_count"] = len(origins)
    result["unexpected_line_count"] = len(lines) - len(parsed)
    if result["unexpected_line_count"]:
        result["status"] = "unexpected_response"
    elif origins:
        result["status"] = "response_received"
    else:
        result["status"] = "empty_response"
    return result


def probe(work_dir, env_file, max_requests, timeout, max_response_bytes, include_extreme,
          delay_seconds):
    token = read_token(env_file)
    if not token:
        raise RuntimeError("SWH_API_TOKEN is missing from environment and .env")
    with open(os.path.join(work_dir, "manifest.json"), encoding="utf-8") as source:
        manifest = json.load(source)
    checkpoint_path = os.path.join(work_dir, "probe_results.jsonl")
    completed = set()
    if os.path.isfile(checkpoint_path):
        with open(checkpoint_path, encoding="utf-8") as source:
            for line in source:
                record = json.loads(line)
                if record.get("status") == "response_received":
                    completed.add(record["directory_id"])
    samples = [item for item in manifest["samples"]
               if (include_extreme or item["group"] != "extreme") and
               item["directory_id"] not in completed]
    requests_made = 0
    with open(checkpoint_path, "a", encoding="utf-8") as output:
        for item in samples:
            if requests_made >= max_requests:
                break
            result = request_origins(item["directory_id"], token, timeout, max_response_bytes)
            result.update(item)
            result["completed_at_utc"] = utc_now()
            output.write(json.dumps(result, ensure_ascii=False, sort_keys=True) + "\n")
            output.flush()
            os.fsync(output.fileno())
            requests_made += 1
            print(json.dumps({"event": "graph_probe_result", "directory_id": item["directory_id"],
                              "group": item["group"], "status": result["status"],
                              "http_status": result.get("http_status"),
                              "origin_count": result.get("origin_count"),
                              "elapsed_seconds": result["elapsed_seconds"]}, sort_keys=True),
                  flush=True)
            if result["status"] != "response_received":
                break
            if delay_seconds:
                time.sleep(delay_seconds)
    return requests_made


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("prepare", "probe"))
    parser.add_argument("--index-csv")
    parser.add_argument("--work-dir", required=True)
    parser.add_argument("--env-file", default=".env")
    parser.add_argument("--max-requests", type=int, default=4)
    parser.add_argument("--timeout", type=int, default=30)
    parser.add_argument("--max-response-bytes", type=int, default=2000000)
    parser.add_argument("--include-extreme", action="store_true")
    parser.add_argument("--delay-seconds", type=float, default=1.0)
    args = parser.parse_args()
    if args.action == "prepare":
        if not args.index_csv:
            parser.error("prepare requires --index-csv")
        manifest = prepare(args.index_csv, args.work_dir)
        print(json.dumps({"event": "probe_input_prepared", "manifest": manifest},
                         sort_keys=True), flush=True)
    else:
        if args.max_requests < 1 or args.max_requests > 10:
            parser.error("--max-requests must be between 1 and 10")
        if args.timeout < 1 or args.timeout > 180:
            parser.error("--timeout must be between 1 and 180 seconds")
        if args.max_response_bytes < 1024 or args.max_response_bytes > 10000000:
            parser.error("--max-response-bytes must be 1024..10000000")
        count = probe(args.work_dir, args.env_file, args.max_requests, args.timeout,
                      args.max_response_bytes, args.include_extreme, args.delay_seconds)
        print(json.dumps({"event": "graph_probe_stopped", "requests_made": count}), flush=True)


if __name__ == "__main__":
    main()
