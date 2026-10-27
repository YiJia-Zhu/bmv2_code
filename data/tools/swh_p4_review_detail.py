#!/usr/bin/env python3
"""Publish detailed reasons for every remaining SWH P4 review hit to S3."""

import argparse
import collections
from concurrent.futures import ThreadPoolExecutor, as_completed
import csv
import datetime as dt
import gzip
import hashlib
import json
import logging
import os
from pathlib import Path
import re
import shutil
import time

from swh_p4_content_screen import (code_without_comments_or_strings, git_blob_sha1,
                                   parse_s3_uri, s3_client)


RULE_VERSION = "p4-review-detail-v3"
DEFAULT_ROOT = Path("data/raw/swh_p4/content_screen/full-128-20261020")
BASE_URI = "s3://swh2p4/file-screen/swh-p4-20260604/full-128-20261020"
SOURCE_URI = BASE_URI + "/filtered_v2/review_csv"
OUTPUT_URI = BASE_URI + "/review_detail_v3"
EXPECTED_IDS = 5445
EXPECTED_ROWS = 202389
EXPECTED_SHARDS = 128
NO_EVIDENCE = {"whitespace_only", "comments_only", "recognized_non_source"}
DETAIL_FIELDS = [
    "review_detail_rule_version", "review_detail_category", "review_detail_reason",
    "review_detail_action", "review_detail_format", "review_detail_sha1_git_verified",
    "review_detail_raw_bytes", "review_detail_has_no_language_evidence",
]
LOG = logging.getLogger("swh_p4_review_detail")


def now():
    return dt.datetime.now(dt.timezone.utc).isoformat()


def sha256_file(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def save_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True)
                         + "\n", encoding="utf-8")
    os.replace(str(temporary), str(path))


def recognized_format(data):
    signatures = (
        (b"SQLite format 3\x00", "sqlite"),
        (b"\x89PNG\r\n\x1a\n", "png"),
        (b"\x7fELF", "elf"),
        (b"\xff\xd8\xff", "jpeg"),
        (b"\x00\x05\x16\x07\x00\x02\x00\x00", "appledouble"),
    )
    for signature, name in signatures:
        if data.startswith(signature):
            return name
    return None


def classify_detail(row, data):
    """Refine reasons without promoting unknown files to confirmed P4_16."""
    if row.get("review_disposition") != "unresolved":
        raise ValueError("detail input must be an unresolved second-pass annotation")
    result = {"content_id": row["content_id"],
              "review_detail_rule_version": RULE_VERSION,
              "previous_review_reasons": row["review_reasons"],
              "review_detail_format": "unknown",
              "review_detail_sha1_git_verified": False,
              "review_detail_raw_bytes": ""}
    reason = row["review_reasons"]
    if data is None:
        if row.get("fetch_status") != "too_large":
            raise RuntimeError("expected cached bytes are missing: " + row["content_id"])
        category, explanation, action = (
            "over_read_limit", "raw_content_exceeded_10_mib_read_limit",
            "retry_complete_fetch_with_larger_budget")
    else:
        if git_blob_sha1(data) != row["content_id"]:
            raise RuntimeError("raw cache hash mismatch: " + row["content_id"])
        result.update(review_detail_sha1_git_verified=True,
                      review_detail_raw_bytes=len(data))
        if reason == ["unsupported_encoding_with_nul"]:
            binary_format = recognized_format(data)
            if binary_format:
                category, explanation, action = (
                    "recognized_non_source", "recognized_file_signature:" + binary_format,
                    "exclude_as_language_evidence_keep_provenance")
                result["review_detail_format"] = binary_format
            else:
                category, explanation, action = (
                    "unrecognized_encoding_or_binary", "unrecognized_format_with_nul",
                    "inspect_format_or_decode_reliably")
        elif reason == ["conflicting_syntax_evidence"]:
            category, explanation, action = (
                "conflicting_syntax", "p4_14_and_p4_16_rule_evidence_coexist",
                "review_preprocessor_context_and_syntax")
            result["review_detail_format"] = row.get("review_encoding", "text")
        elif reason == ["insufficient_version_evidence"]:
            encoding = row["review_encoding"]
            source = data.decode("latin-1" if encoding == "latin-1-fallback" else
                                 "utf-16" if encoding == "utf-16" else "utf-8-sig")
            result["review_detail_format"] = encoding
            code = code_without_comments_or_strings(source, keep_strings=True)
            lines = [line.strip() for line in code.splitlines() if line.strip()]
            if not source.strip():
                category, explanation, action = (
                    "whitespace_only", "empty_after_whitespace_strip",
                    "exclude_as_language_evidence_keep_provenance")
            elif not code.strip():
                category, explanation, action = (
                    "comments_only", "no_code_after_comment_removal",
                    "exclude_as_language_evidence_keep_provenance")
            elif lines and all(line.startswith("#") for line in lines):
                category, explanation, action = (
                    "preprocessor_only", "all_nonempty_code_lines_are_preprocessor_directives",
                    "retain_for_repository_context")
            else:
                category, explanation, action = (
                    "other_readable_text", "readable_text_without_matching_version_rules",
                    "identify_language_and_extend_syntax_review")
        else:
            raise RuntimeError("unexpected unresolved reason: " + repr(reason))
    result.update(review_detail_category=category, review_detail_reason=explanation,
                  review_detail_action=action,
                  review_detail_has_no_language_evidence=category in NO_EVIDENCE)
    return result


def build_annotations(input_path, cache_dir, expected_ids):
    annotations = {}
    with input_path.open(encoding="utf-8") as stream:
        for line in stream:
            row = json.loads(line)
            if row.get("review_disposition") != "unresolved":
                continue
            content_id = row["content_id"]
            if content_id in annotations:
                raise RuntimeError("duplicate unresolved ID: " + content_id)
            path = cache_dir / content_id[:2] / content_id
            data = path.read_bytes() if path.exists() else None
            annotations[content_id] = classify_detail(row, data)
            if len(annotations) % 1000 == 0:
                LOG.info("annotation_progress completed=%d expected=%d", len(annotations), expected_ids)
    if len(annotations) != expected_ids:
        raise RuntimeError("expected {} review IDs, got {}".format(expected_ids, len(annotations)))
    return annotations


def transform_csv(source_path, relative_key, output_root, annotations, shard):
    """Preserve every original column and every source row in the detailed CSV."""
    paths = {}
    handles = []
    counts = collections.Counter()
    content_ids = set()
    source_keys = set()
    try:
        with source_path.open(encoding="utf-8-sig", newline="") as stream:
            reader = csv.DictReader(stream)
            fields = reader.fieldnames
            required = {"content_id", "directory_id", "name", "source_csv", "source_row"}
            if not fields or not required.issubset(fields) or len(fields) != len(set(fields)):
                raise RuntimeError("missing or duplicated source columns")
            if set(fields) & set(DETAIL_FIELDS):
                raise RuntimeError("source already has detail fields")
            output_fields = fields + ([] if "source_shard" in fields else ["source_shard"]) + DETAIL_FIELDS
            writers = {}
            for name in ("classified_review_csv", "no_code_or_non_source_csv", "remaining_review_csv"):
                path = output_root / name / (relative_key + ".gz")
                path.parent.mkdir(parents=True, exist_ok=True)
                paths[name] = path
                handle = gzip.open(str(path), "wt", encoding="utf-8", newline="")
                handles.append(handle)
                writers[name] = csv.DictWriter(handle, fieldnames=output_fields)
                writers[name].writeheader()
            for row in reader:
                if None in row or any(row.get(column) in (None, "") for column in required):
                    raise RuntimeError("invalid source CSV record")
                if row.get("discovery_disposition") != "needs_review":
                    raise RuntimeError("source includes a non-review row")
                if str(row.get("source_csv_index", shard)) != str(shard):
                    raise RuntimeError("source CSV index does not match partition")
                if "source_shard" in row and row["source_shard"] != str(shard):
                    raise RuntimeError("source shard column does not match partition")
                source_key = (row["source_csv"], row["source_row"])
                if source_key in source_keys:
                    raise RuntimeError("duplicate CSV source row")
                source_keys.add(source_key)
                content_id = row["content_id"]
                annotation = annotations[content_id]
                row["source_shard"] = str(shard)
                row.update({field: annotation[field] for field in DETAIL_FIELDS})
                writers["classified_review_csv"].writerow(row)
                group = ("no_code_or_non_source_csv" if annotation[
                    "review_detail_has_no_language_evidence"] else "remaining_review_csv")
                writers[group].writerow(row)
                counts[annotation["review_detail_category"]] += 1
                content_ids.add(content_id)
    finally:
        for handle in handles:
            handle.close()
    # Verify the exported records, including original column values, before publishing.
    with source_path.open(encoding="utf-8-sig", newline="") as source, gzip.open(
            str(paths["classified_review_csv"]), "rt", encoding="utf-8", newline="") as target:
        left, right = csv.DictReader(source), csv.DictReader(target)
        for original in left:
            saved = next(right, None)
            if saved is None or any(saved[key] != value for key, value in original.items()):
                raise RuntimeError("export changed original source values")
        if next(right, None) is not None:
            raise RuntimeError("export added unexpected source rows")
    no_evidence = sum(counts[name] for name in NO_EVIDENCE)
    expected = {"classified_review_csv": sum(counts.values()),
                "no_code_or_non_source_csv": no_evidence,
                "remaining_review_csv": sum(counts.values()) - no_evidence}
    for name, path in paths.items():
        with gzip.open(str(path), "rt", encoding="utf-8", newline="") as stream:
            if sum(1 for row in csv.DictReader(stream)) != expected[name]:
                raise RuntimeError("export row count mismatch: " + name)
    return {"row_counts": dict(counts), "content_ids": sorted(content_ids),
            "rows": sum(counts.values()), "no_language_evidence_rows": no_evidence,
            "files": [{"path": str(path), "relative_key": str(path.relative_to(output_root)),
                       "sha256": sha256_file(path), "bytes": path.stat().st_size,
                       "rows": expected[name]} for name, path in paths.items()]}


def publish_file(s3, path, bucket, key, content_type, transfer_config):
    digest = sha256_file(path)
    s3.upload_file(str(path), bucket, key, ExtraArgs={
        "ContentType": content_type, "Metadata": {"sha256": digest}}, Config=transfer_config)
    remote = s3.head_object(Bucket=bucket, Key=key)
    if remote["ContentLength"] != path.stat().st_size or remote["Metadata"].get("sha256") != digest:
        raise RuntimeError("uploaded object failed verification: " + key)
    return {"key": key, "bytes": remote["ContentLength"], "sha256": digest}


def run(args):
    from boto3.s3.transfer import TransferConfig

    started = time.monotonic()
    created_at = now()
    root = args.run_root
    output = root / "output"
    output.mkdir(parents=True, exist_ok=True)
    s3 = s3_client("us-east-1")
    transfer_config = TransferConfig(max_concurrency=1, use_threads=False)
    source_bucket, source_prefix = parse_s3_uri(args.source_uri)
    target_bucket, target_prefix = parse_s3_uri(args.output_uri)
    if target_bucket != "swh2p4" or target_prefix != parse_s3_uri(OUTPUT_URI)[1]:
        raise ValueError("output must use the dedicated review_detail_v3 prefix")
    items = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=source_bucket, Prefix=source_prefix + "/"):
        for item in page.get("Contents", []):
            if item["Key"].endswith(".csv"):
                match = re.search(r"/source_shard=(\d+)/", item["Key"])
                if not match:
                    raise RuntimeError("source object has no shard partition")
                items.append({"key": item["Key"], "bytes": item["Size"],
                              "etag": item["ETag"], "shard": int(match.group(1))})
    items.sort(key=lambda item: item["key"])
    if len(items) != EXPECTED_SHARDS or {item["shard"] for item in items} != set(range(EXPECTED_SHARDS)):
        raise RuntimeError("expected one source CSV for every shard 0..127")
    fingerprint = hashlib.sha256(json.dumps({
        "rule_version": RULE_VERSION, "code_sha256": sha256_file(Path(__file__)),
        "annotations_sha256": sha256_file(args.annotations), "source_uri": args.source_uri,
        "objects": items, "output_uri": args.output_uri}, sort_keys=True).encode()).hexdigest()
    state_path = root / "pipeline.json"
    if state_path.exists() and json.loads(state_path.read_text())["fingerprint"] != fingerprint:
        raise RuntimeError("existing run inputs or rules changed; use a new run root and output version")
    state = {"rule_version": RULE_VERSION, "fingerprint": fingerprint, "phase": "classifying",
             "started_at_utc": created_at, "source_uri": args.source_uri, "output_uri": args.output_uri}
    save_json(state_path, state)
    LOG.info("started_at_utc=%s source_csvs=%d source_bytes=%d", created_at, len(items), sum(i["bytes"] for i in items))
    annotations = build_annotations(args.annotations, args.cache_dir, EXPECTED_IDS)
    annotation_path = output / "content_annotations.jsonl"
    with annotation_path.open("w", encoding="utf-8") as stream:
        for content_id in sorted(annotations):
            stream.write(json.dumps(annotations[content_id], sort_keys=True, ensure_ascii=False) + "\n")
    save_json(output / "source_inventory.json", {"source_uri": args.source_uri, "objects": items})

    def process(item):
        relative = item["key"][len(source_prefix) + 1:]
        source_path = root / "source_cache" / relative
        receipt_path = source_path.with_suffix(".receipt.json")
        source_path.parent.mkdir(parents=True, exist_ok=True)
        receipt = json.loads(receipt_path.read_text()) if receipt_path.exists() else None
        if not (receipt and receipt["etag"] == item["etag"] and source_path.exists()
                and sha256_file(source_path) == receipt["sha256"]):
            temporary = source_path.with_suffix(".download")
            # get_object supports IfMatch, unlike managed download_file ExtraArgs.
            with s3.get_object(Bucket=source_bucket, Key=item["key"],
                               IfMatch=item["etag"])["Body"] as body:
                with temporary.open("wb") as stream:
                    shutil.copyfileobj(body, stream)
            if temporary.stat().st_size != item["bytes"]:
                raise RuntimeError("download size mismatch: " + item["key"])
            os.replace(str(temporary), str(source_path))
            save_json(receipt_path, {"etag": item["etag"], "sha256": sha256_file(source_path)})
        result = transform_csv(source_path, relative, output, annotations, item["shard"])
        for entry in result["files"]:
            publish_file(s3, Path(entry["path"]), target_bucket,
                         target_prefix + "/" + entry["relative_key"], "text/csv", transfer_config)
        return result

    counts = collections.Counter()
    seen_ids = set()
    files = []
    state["phase"] = "exporting_review_csv"
    save_json(state_path, state)
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = [executor.submit(process, item) for item in items]
        for completed, future in enumerate(as_completed(futures), 1):
            result = future.result()
            counts.update(result["row_counts"])
            seen_ids.update(result["content_ids"])
            files.extend(result["files"])
            elapsed = time.monotonic() - started
            state.update(completed_source_csvs=completed, processed_review_rows=sum(counts.values()),
                         elapsed_seconds=round(elapsed, 2), updated_at_utc=now())
            save_json(state_path, state)
            LOG.info("export_progress csv=%d/%d rows=%d elapsed_s=%.1f eta_s=%.1f",
                     completed, len(items), sum(counts.values()), elapsed,
                     elapsed / completed * (len(items) - completed))
    if sum(counts.values()) != EXPECTED_ROWS or seen_ids != set(annotations):
        raise RuntimeError("review row count or content ID set changed during export")
    content_counts = collections.Counter(row["review_detail_category"] for row in annotations.values())
    no_evidence = sum(counts[name] for name in NO_EVIDENCE)
    metrics = {
        "rule_version": RULE_VERSION, "fingerprint": fingerprint,
        "started_at_utc": created_at, "completed_at_utc": now(),
        "source_review_rows": EXPECTED_ROWS, "unique_content_ids": len(annotations),
        "source_csv_count": len(items), "category_content_counts": dict(content_counts),
        "category_hit_row_counts": dict(counts), "no_language_evidence_hit_rows": no_evidence,
        "remaining_review_hit_rows": EXPECTED_ROWS - no_evidence,
        "remaining_review_content_ids": sum(value for name, value in content_counts.items() if name not in NO_EVIDENCE),
        "binary_format_content_counts": dict(collections.Counter(
            row["review_detail_format"] for row in annotations.values()
            if row["review_detail_category"] == "recognized_non_source")),
        "source_uri": args.source_uri, "output_uri": args.output_uri,
        "source_columns_preserved_and_read_back": True,
        "p4_16_repository_confirmed": False,
        "default_graph_input": "s3://swh2p4/preprocess/swh-p4-20260604/dedup-v1/runs/full-128-20261019-v1/unique_hits/",
    }
    save_json(output / "metrics.json", metrics)
    summary_path = output / "category_summary.csv"
    with summary_path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=["category", "unique_content_ids", "hit_rows"])
        writer.writeheader()
        for category in sorted(counts):
            writer.writerow({"category": category, "unique_content_ids": content_counts[category],
                             "hit_rows": counts[category]})
    manifest = {
        "rule_version": RULE_VERSION, "fingerprint": fingerprint, "phase": "complete",
        "source_uri": args.source_uri, "output_uri": args.output_uri,
        "unchanged_candidate_csv_uri": BASE_URI + "/filtered_v2/candidate_csv/",
        "unchanged_previous_excluded_csv_uri": BASE_URI + "/filtered_v2/excluded_csv/",
        "all_original_hits_uri": BASE_URI + "/filtered_v2/all_annotated_hits/",
        "classification_scope": "all 202389 remaining review hit rows; all 5445 content IDs",
        "meaning": "detailed review reasons; no new confirmed P4_16 labels or complete quality gate",
        "graph_mapping_policy": "all original triples remain the default graph input; these are evidence annotations",
        "metrics": metrics,
        "csv_objects": [{key: value for key, value in entry.items() if key != "path"}
                        for entry in sorted(files, key=lambda entry: entry["relative_key"])],
    }
    save_json(output / "result_manifest.json", manifest)
    for name, content_type in (("content_annotations.jsonl", "application/x-ndjson"),
                               ("source_inventory.json", "application/json"),
                               ("category_summary.csv", "text/csv"),
                               ("metrics.json", "application/json"),
                               ("result_manifest.json", "application/json")):
        publish_file(s3, output / name, target_bucket, target_prefix + "/" + name,
                     content_type, transfer_config)
    with s3.get_object(Bucket=target_bucket, Key=target_prefix + "/metrics.json")["Body"] as body:
        if json.loads(body.read()) != metrics:
            raise RuntimeError("S3 metrics read-back differs from local result")
    # Publish the completion marker only after all data and metadata passed verification.
    marker = output / "_COMPLETE.json"
    save_json(marker, {"fingerprint": fingerprint, "completed_at_utc": now(),
                       "manifest_sha256": sha256_file(output / "result_manifest.json"),
                       "source_review_rows": EXPECTED_ROWS, "unique_content_ids": len(annotations)})
    publish_file(s3, marker, target_bucket, target_prefix + "/_COMPLETE.json", "application/json", transfer_config)
    state.update(phase="complete", completed_at_utc=now(), metrics=metrics)
    save_json(state_path, state)
    LOG.info("completed %s", json.dumps(metrics, sort_keys=True))
    return metrics


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--annotations", type=Path, default=DEFAULT_ROOT / "review-v2/content_annotations.jsonl")
    parser.add_argument("--cache-dir", type=Path, default=DEFAULT_ROOT / "screen/raw_cache")
    parser.add_argument("--run-root", type=Path, default=DEFAULT_ROOT / "review-detail-v3")
    parser.add_argument("--source-uri", default=SOURCE_URI)
    parser.add_argument("--output-uri", default=OUTPUT_URI)
    parser.add_argument("--workers", type=int, default=16)
    args = parser.parse_args()
    if not 1 <= args.workers <= 32:
        parser.error("workers must be in 1..32")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    csv.field_size_limit(16 * 1024 * 1024)
    run(args)


if __name__ == "__main__":
    main()
