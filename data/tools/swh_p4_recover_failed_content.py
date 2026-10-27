#!/usr/bin/env python3
"""Recover only failed S3 P4 blobs through the rate-limited SWH API."""

import argparse
import datetime as dt
import hashlib
import json
import logging
import os
import time
from pathlib import Path

from swh_p4_content_screen import (RULE_VERSION, cache_content, fetch_content,
                                   inspect_content, s3_client, upload_raw_batch,
                                   write_batch)


RETRY_STATUSES = {"s3_error", "network_error", "decompression_error", "hash_mismatch"}
LOG = logging.getLogger("swh_p4_recovery")


def recover_row(row, cache_dir, api_root, token, max_bytes):
    if row.get("fetch_status") not in RETRY_STATUSES:
        return row
    content_id = row["content_id"]
    data = cache_content(cache_dir, content_id, None)
    if data is None:
        data, info = fetch_content(content_id, api_root, 20.0, max_bytes, 2, token)
    else:
        info = {"fetch_status": "cached", "bytes_read": len(data)}
    if data is None:
        result = dict(row)
        result["api_fallback_status"] = info["fetch_status"]
        result["api_fallback_http_status"] = info.get("http_status")
        result["api_fallback_at_utc"] = dt.datetime.now(dt.timezone.utc).isoformat()
        return result
    if "sha1" in row and hashlib.sha1(data).hexdigest() != row["sha1"]:
        result = dict(row)
        result["api_fallback_status"] = "sha1_mismatch"
        result["api_fallback_at_utc"] = dt.datetime.now(dt.timezone.utc).isoformat()
        return result
    result = inspect_content(content_id, data)
    if not result["sha1_git_verified"]:
        raise RuntimeError("SWH API content did not match Git blob ID " + content_id)
    cache_content(cache_dir, content_id, data)
    result.update(info)
    result["sha1"] = row.get("sha1")
    result["fetched_at_utc"] = row.get("fetched_at_utc")
    result["api_fallback_status"] = "recovered"
    result["api_fallback_at_utc"] = dt.datetime.now(dt.timezone.utc).isoformat()
    return result


def recover(output_dir, s3_output_uri, api_root, token, max_bytes, delay, s3=None):
    s3 = s3 or s3_client("us-east-1")
    cache_dir = output_dir / "raw_cache"
    counts = {"attempted": 0, "recovered": 0, "still_uninspected": 0, "batches_updated": 0}
    for path in sorted(output_dir.glob("batch-*.jsonl")):
        number = int(path.stem.split("-")[1])
        original = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
        if any(row.get("rule_version") != RULE_VERSION for row in original):
            raise RuntimeError("unknown annotation rule version in " + str(path))
        rows = []
        modified = any(row.get("api_fallback_status") == "recovered" for row in original)
        for row in original:
            if row.get("fetch_status") not in RETRY_STATUSES:
                rows.append(row)
                continue
            counts["attempted"] += 1
            updated = recover_row(row, cache_dir, api_root, token, max_bytes)
            rows.append(updated)
            modified = True
            if updated.get("api_fallback_status") == "recovered":
                counts["recovered"] += 1
            else:
                counts["still_uninspected"] += 1
            if delay:
                time.sleep(delay)
        if modified:
            if [row["content_id"] for row in rows] != [row["content_id"] for row in original]:
                raise RuntimeError("recovery changed batch identity")
            write_batch(s3, rows, output_dir, s3_output_uri, number)
            upload_raw_batch(s3, rows, output_dir, cache_dir, s3_output_uri, number,
                             rebuild=True)
            counts["batches_updated"] += 1
            LOG.info("batch=%d attempted=%d recovered=%d still_uninspected=%d",
                     number, counts["attempted"], counts["recovered"],
                     counts["still_uninspected"])
    return counts


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--s3-output-uri", required=True)
    parser.add_argument("--api-root", default="https://archive.softwareheritage.org/api/1")
    parser.add_argument("--token", default=os.environ.get("SWH_API_TOKEN"))
    parser.add_argument("--max-bytes", type=int, default=10 * 1024 * 1024)
    parser.add_argument("--delay", type=float, default=0.5)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    counts = recover(args.output_dir, args.s3_output_uri, args.api_root, args.token,
                     args.max_bytes, args.delay)
    print(json.dumps(counts, sort_keys=True), flush=True)


if __name__ == "__main__":
    main()
