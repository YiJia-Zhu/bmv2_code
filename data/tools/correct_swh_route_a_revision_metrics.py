#!/usr/bin/env python3
"""Correct cumulative Range GET metrics after a resumed revision mapping run."""

import datetime as dt
import hashlib
import json
import os
from pathlib import Path

from botocore.config import Config

from run_swh_route_a_revision_resolve import (
    BUCKET,
    BUCKET_OWNER,
    S3_PREFIX,
    historical_range_counters,
    private_session,
)


ROOT = Path("/mnt/8T/xgr/swh-graph-poc-20260604/route-a-revision-origin-v1/revision-resolve-v1")


def sha256(data):
    return hashlib.sha256(data).hexdigest()


def write_json(path, value):
    temporary = path.with_suffix(path.suffix + ".partial")
    temporary.write_text(
        json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.replace(str(temporary), str(path))


def main():
    requests, byte_count = historical_range_counters(ROOT / "events.jsonl")
    if (requests, byte_count) != (5378, 34422278):
        raise RuntimeError("event log counters differ from the reviewed run")

    complete_path = ROOT / "complete.json"
    verification_path = ROOT / "final-verification.json"
    run_complete_path = ROOT / "run_complete.json"
    archive_path = ROOT / "archive.json"
    postrun_manifest_path = ROOT / "postrun-manifest.json"
    originals = {
        "complete-original.json": complete_path.read_bytes(),
        "final-verification-original.json": verification_path.read_bytes(),
        "run_complete-original.json": run_complete_path.read_bytes(),
    }
    for name, payload in originals.items():
        original_path = ROOT / "postrun" / name
        original_path.parent.mkdir(parents=True, exist_ok=True)
        if original_path.exists() and original_path.read_bytes() != payload:
            raise RuntimeError("existing preserved original differs: " + name)
        original_path.write_bytes(payload)

    corrected_at = dt.datetime.now(dt.timezone.utc).isoformat()
    complete = json.loads(complete_path.read_text(encoding="utf-8"))
    if complete.get("status") != "passed":
        raise RuntimeError("core completion marker is not passed")
    complete.update(
        range_requests=requests,
        range_bytes=byte_count,
        range_requests_this_process=0,
        range_bytes_this_process=0,
        range_metrics_scope="cumulative across resumed mapping processes",
        metrics_corrected_at_utc=corrected_at,
    )
    write_json(complete_path, complete)

    verification = json.loads(verification_path.read_text(encoding="utf-8"))
    if (verification.get("range_requests"), verification.get("range_bytes")) != (0, 0):
        raise RuntimeError("verification marker no longer has the known resume-only counters")
    verification.update(
        range_requests=requests,
        range_bytes=byte_count,
        range_requests_this_process=0,
        range_bytes_this_process=0,
        verified_at_utc=corrected_at,
        correction_note="Cumulative counters from the append-only events log; revision mappings were not rerun.",
        preserved_original_sha256=sha256(originals["final-verification-original.json"]),
        source_events_sha256=sha256((ROOT / "events.jsonl").read_bytes()),
    )
    write_json(verification_path, verification)

    run_complete = json.loads(run_complete_path.read_text(encoding="utf-8"))
    run_complete["summary"] = complete
    run_complete["metrics_corrected_at_utc"] = corrected_at
    write_json(run_complete_path, run_complete)

    correction = {
        "status": "passed",
        "corrected_at_utc": corrected_at,
        "range_requests_cumulative": requests,
        "range_bytes_cumulative": byte_count,
        "resume_process_requests": 0,
        "resume_process_bytes": 0,
        "source_events_sha256": sha256((ROOT / "events.jsonl").read_bytes()),
        "preserved_originals": {
            name: sha256(payload) for name, payload in originals.items()
        },
        "scope": "Accounting correction only; Parquet outputs and revision mapping are unchanged.",
    }
    correction_path = ROOT / "postrun" / "metrics-correction.json"
    write_json(correction_path, correction)

    archive = json.loads(archive_path.read_text(encoding="utf-8"))
    complete_key = S3_PREFIX + "complete.json"
    complete_entry = next(item for item in archive["objects"] if item["key"] == complete_key)
    complete_payload = complete_path.read_bytes()
    complete_entry.update(bytes=len(complete_payload), sha256=sha256(complete_payload))
    archive["summary"] = complete
    archive["completed_at_utc"] = corrected_at
    write_json(archive_path, archive)

    s3 = private_session().client(
        "s3", config=Config(retries={"total_max_attempts": 3, "mode": "standard"})
    )
    uploads = {
        "complete.json": complete_payload,
        "archive.json": archive_path.read_bytes(),
        "postrun/final-verification.json": verification_path.read_bytes(),
        "postrun/run_complete.json": run_complete_path.read_bytes(),
        "postrun/metrics-correction.json": correction_path.read_bytes(),
    }
    uploads.update({"postrun/" + name: payload for name, payload in originals.items()})
    entries = []
    for relative, payload in uploads.items():
        key = S3_PREFIX + relative
        s3.put_object(
            Bucket=BUCKET,
            Key=key,
            Body=payload,
            ExpectedBucketOwner=BUCKET_OWNER,
            ServerSideEncryption="AES256",
            ChecksumAlgorithm="SHA256",
            Metadata={"sha256": sha256(payload)},
        )
        response = s3.get_object(
            Bucket=BUCKET, Key=key, ExpectedBucketOwner=BUCKET_OWNER
        )
        with response["Body"] as body:
            if body.read() != payload:
                raise RuntimeError("S3 readback mismatch: " + relative)
        entries.append(
            {"key": key, "bytes": len(payload), "sha256": sha256(payload),
             "remote_checksum_verified": True}
        )

    manifest = {
        "status": "passed",
        "uploaded_at_utc": corrected_at,
        "objects": entries,
    }
    write_json(postrun_manifest_path, manifest)
    manifest_payload = postrun_manifest_path.read_bytes()
    key = S3_PREFIX + "postrun-manifest.json"
    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=manifest_payload,
        ExpectedBucketOwner=BUCKET_OWNER,
        ServerSideEncryption="AES256",
        ChecksumAlgorithm="SHA256",
        Metadata={"sha256": sha256(manifest_payload)},
    )
    response = s3.get_object(Bucket=BUCKET, Key=key, ExpectedBucketOwner=BUCKET_OWNER)
    with response["Body"] as body:
        if body.read() != manifest_payload:
            raise RuntimeError("S3 postrun manifest readback mismatch")

    print(json.dumps({
        "status": "passed",
        "range_requests": requests,
        "range_bytes": byte_count,
        "verified_objects": len(entries) + 1,
        "s3_prefix": "s3://" + BUCKET + "/" + S3_PREFIX,
    }, sort_keys=True))


if __name__ == "__main__":
    main()
