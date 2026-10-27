#!/usr/bin/env python3
"""Recompute candidate file quality and export provenance-preserving S3 views."""

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
import time

from swh_p4_content_screen import GENERATED, git_blob_sha1, parse_s3_uri, s3_client


RULE_VERSION = "p4-candidate-quality-v1"
BASE = "s3://swh2p4/file-screen/swh-p4-20260604/full-128-20261020"
SOURCE_URI = BASE + "/filtered_v2/candidate_csv"
OUTPUT_URI = BASE + "/candidate_quality_v1"
ROOT = Path("data/raw/swh_p4/content_screen/full-128-20261020")
EXPECTED_ROWS = 13465757
EXPECTED_SHARDS = 128
EXPECTED_IDS = 122392
GROUPS = ("quality_pass_csv", "quality_excluded_csv", "quality_unknown_csv")
FIELDS = ("quality_v1_status", "quality_v1_flags", "quality_v1_training_eligible",
          "quality_v1_rule_version")
ENCODED_RUN = re.compile(r"[A-Za-z0-9+/]{64,}={0,2}|(?:\\x[0-9a-fA-F]{2}){8,}|(?:\\u[0-9a-fA-F]{4}){8,}")
LOG = logging.getLogger("swh_p4_candidate_quality")


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
    temporary.write_text(json.dumps(value, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    os.replace(str(temporary), str(path))


def load_candidates(first_pass_dir, review_path):
    rows = {}
    for path in sorted(first_pass_dir.glob("batch-*.jsonl")):
        for line in path.open(encoding="utf-8"):
            row = json.loads(line)
            content_id = row["content_id"]
            if content_id in rows:
                raise RuntimeError("duplicate first-pass content ID: " + content_id)
            if row.get("version_status") == "p4_16_likely" and row.get("sha1_git_verified"):
                rows[content_id] = row
    for line in review_path.open(encoding="utf-8"):
        row = json.loads(line)
        if row.get("review_disposition") != "p4_16_candidate":
            continue
        content_id = row["content_id"]
        if content_id in rows:
            raise RuntimeError("candidate repeated between screens: " + content_id)
        rows[content_id] = row
    if len(rows) != EXPECTED_IDS:
        raise RuntimeError("expected {} candidate content IDs, got {}".format(EXPECTED_IDS, len(rows)))
    return rows


def encoded_metrics(source):
    spans = []
    for match in ENCODED_RUN.finditer(source):
        start, end = match.span()
        if spans and start <= spans[-1][1]:
            spans[-1] = (spans[-1][0], max(end, spans[-1][1]))
        else:
            spans.append((start, end))
    total = sum(end - start for start, end in spans)
    longest = max((end - start for start, end in spans), default=0)
    return longest, total / len(source) if source else 0.0


def inspect_quality(content_id, data, encoding=None):
    result = {"content_id": content_id, "quality_rule_version": RULE_VERSION,
              "quality_status": "unknown", "quality_flags": [],
              "raw_bytes": len(data) if data is not None else None}
    if data is None:
        result["quality_reason"] = "raw_content_missing"
        return result
    if git_blob_sha1(data) != content_id:
        result["quality_reason"] = "git_blob_hash_mismatch"
        return result
    result["sha1_git_verified"] = True
    try:
        if encoding:
            selected = encoding
        elif data.startswith((b"\xff\xfe", b"\xfe\xff")):
            selected = "utf-16"
        elif data.startswith(b"\xef\xbb\xbf"):
            selected = "utf-8-sig"
        else:
            selected = "utf-8"
        source = data.decode(selected)
        if selected == "utf-16" and data.startswith(b"\xfe\xff"):
            roundtrip = b"\xfe\xff" + source.encode("utf-16-be")
        else:
            roundtrip = source.encode(selected)
        if roundtrip != data:
            raise UnicodeError("decoded content does not roundtrip")
    except (UnicodeError, LookupError):
        result["quality_reason"] = "unsupported_encoding"
        return result
    result["quality_encoding"] = selected
    lines = source.splitlines()
    if not lines and source:
        lines = [source]
    lengths = [len(line) for line in lines]
    characters = sum(lengths)
    counts = collections.Counter(line.strip() for line in lines if line.strip())
    repeated = sum(count - 1 for count in counts.values())
    nonempty = sum(counts.values())
    longest_encoded, encoded_ratio = encoded_metrics(source)
    metrics = {
        "line_count": len(lines),
        "average_line_length": characters / len(lines) if lines else 0.0,
        "longest_line_length": max(lengths, default=0),
        "letter_ratio": sum(char.isalpha() for line in lines for char in line) / characters if characters else 0.0,
        "longest_encoded_segment": longest_encoded,
        "encoded_data_ratio": encoded_ratio,
        "duplicate_nonempty_line_ratio": repeated / nonempty if nonempty else 0.0,
        "over_500_kb": len(data) > 500000,
    }
    result.update(metrics)
    flags = []
    if not source or not source.strip():
        flags.append("empty_or_whitespace")
    if "\x00" in source:
        flags.append("nul_character")
    if metrics["line_count"] > 100000:
        flags.append("too_many_lines")
    if metrics["average_line_length"] > 100:
        flags.append("long_average_line")
    if metrics["longest_line_length"] > 1000:
        flags.append("very_long_line")
    if metrics["letter_ratio"] < 0.25:
        flags.append("low_letter_ratio")
    if GENERATED.search("\n".join(lines[:5])):
        flags.append("generated_header")
    if longest_encoded > 1024 or encoded_ratio > 0.5:
        flags.append("encoded_data")
    result.update(quality_status="excluded" if flags else "pass", quality_flags=flags,
                  training_file_eligible=False if flags else True)
    return result


def build_annotations(args, output):
    candidates = load_candidates(args.screen_dir, args.review_annotations)
    overrides = {}
    for line in args.special5.open(encoding="utf-8"):
        row = json.loads(line)
        overrides[row["content_id"]] = row
    if len(overrides) != 5 or not set(overrides).issubset(candidates):
        raise RuntimeError("five reviewed encoding overrides do not match candidates")
    path = output / "content_annotations.jsonl"
    temporary = path.with_suffix(".tmp")
    counts = collections.Counter()
    with temporary.open("w", encoding="utf-8") as stream:
        for number, content_id in enumerate(sorted(candidates), 1):
            raw_path = args.raw_cache / content_id[:2] / content_id
            data = raw_path.read_bytes() if raw_path.exists() else None
            override = overrides.get(content_id)
            if override and (data is None or hashlib.sha256(data).hexdigest() != override["raw_sha256"]):
                raise RuntimeError("reviewed encoding source changed: " + content_id)
            result = inspect_quality(content_id, data, override["quality_encoding"] if override else None)
            if override and result["quality_status"] != "pass":
                raise RuntimeError("five-file review does not match new rules: " + content_id)
            if override:
                result["encoding_review"] = "candidate_quality_special5_v1"
            counts[result["quality_status"]] += 1
            stream.write(json.dumps(result, sort_keys=True) + "\n")
            if number % 10000 == 0:
                LOG.info("quality_progress contents=%d/%d statuses=%s", number, len(candidates), dict(counts))
    os.replace(str(temporary), str(path))
    LOG.info("quality_annotations_complete contents=%d statuses=%s", len(candidates), dict(counts))
    return path, counts


def load_annotations(path):
    annotations = {}
    for line in path.open(encoding="utf-8"):
        row = json.loads(line)
        content_id = row["content_id"]
        if content_id in annotations:
            raise RuntimeError("duplicate quality annotation: " + content_id)
        annotations[content_id] = row
    if len(annotations) != EXPECTED_IDS:
        raise RuntimeError("quality annotation count changed")
    return annotations


def list_source_objects(s3, bucket, prefix):
    items = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix + "/"):
        for item in page.get("Contents", []):
            if not item["Key"].endswith(".csv"):
                continue
            match = re.search(r"/source_shard=(\d+)/", item["Key"])
            if not match:
                raise RuntimeError("candidate CSV has no shard index")
            items.append({"key": item["Key"], "etag": item["ETag"],
                          "bytes": item["Size"], "shard": int(match.group(1))})
    items.sort(key=lambda item: item["shard"])
    if len(items) != EXPECTED_SHARDS or {item["shard"] for item in items} != set(range(EXPECTED_SHARDS)):
        raise RuntimeError("candidate export does not have exactly 128 shards")
    return items


def publish_file(s3, path, bucket, key, content_type):
    from boto3.s3.transfer import TransferConfig

    digest = sha256_file(path)
    s3.upload_file(str(path), bucket, key, ExtraArgs={
        "ContentType": content_type, "Metadata": {"sha256": digest}},
        Config=TransferConfig(use_threads=False))
    remote = s3.head_object(Bucket=bucket, Key=key)
    if remote["ContentLength"] != path.stat().st_size or remote["Metadata"].get("sha256") != digest:
        raise RuntimeError("S3 output verification failed: " + key)
    return {"key": key, "bytes": path.stat().st_size, "sha256": digest}


def transform_shard(source_path, output, shard, annotations):
    paths = {group: output / group / "source_shard={}".format(shard) / "part.csv.gz" for group in GROUPS}
    writers = {}
    handles = []
    counts = collections.Counter()
    directories = {group: set() for group in GROUPS}
    ids = set()
    try:
        with source_path.open(encoding="utf-8-sig", newline="") as source:
            reader = csv.DictReader(source)
            fields = reader.fieldnames
            required = {"content_id", "directory_id", "name", "source_csv", "source_row"}
            if not fields or not required.issubset(fields) or set(fields) & set(FIELDS):
                raise RuntimeError("unexpected candidate CSV schema")
            output_fields = fields + ([] if "source_shard" in fields else ["source_shard"]) + list(FIELDS)
            for group, path in paths.items():
                path.parent.mkdir(parents=True, exist_ok=True)
                handle = gzip.open(str(path), "wt", encoding="utf-8", newline="", compresslevel=5)
                handles.append(handle)
                writer = csv.DictWriter(handle, fieldnames=output_fields)
                writer.writeheader()
                writers[group] = writer
            for row in reader:
                if None in row or any(not row.get(field) for field in required):
                    raise RuntimeError("malformed candidate CSV row")
                if row.get("discovery_disposition") != "p4_16_candidate":
                    raise RuntimeError("non-candidate row in candidate CSV")
                if str(row.get("source_csv_index", shard)) != str(shard):
                    raise RuntimeError("candidate source index does not match shard")
                content_id = row["content_id"]
                annotation = annotations.get(content_id)
                if annotation is None:
                    raise RuntimeError("candidate content lacks quality annotation: " + content_id)
                status = annotation["quality_status"]
                group = "quality_{}_csv".format(status)
                row["source_shard"] = str(shard)
                row.update({FIELDS[0]: status, FIELDS[1]: "|".join(annotation["quality_flags"]),
                            FIELDS[2]: "unknown" if status == "unknown" else str(status == "pass").lower(),
                            FIELDS[3]: RULE_VERSION})
                writers[group].writerow(row)
                counts[status] += 1
                directories[group].add(row["directory_id"])
                ids.add(content_id)
    finally:
        for handle in handles:
            handle.close()
    files = []
    for group, path in paths.items():
        expected = counts[group[len("quality_"):-len("_csv")]]
        with gzip.open(str(path), "rt", encoding="utf-8", newline="") as stream:
            if sum(1 for _ in csv.DictReader(stream)) != expected:
                raise RuntimeError("written CSV row count differs: " + group)
        files.append({"group": group, "path": str(path), "rows": expected,
                      "sha256": sha256_file(path), "bytes": path.stat().st_size})
        directory_path = output / "checkpoints" / "dirs-{}-{}.txt.gz".format(shard, group)
        directory_path.parent.mkdir(parents=True, exist_ok=True)
        with gzip.open(str(directory_path), "wt", encoding="ascii") as stream:
            for directory_id in sorted(directories[group]):
                stream.write(directory_id + "\n")
    return {"shard": shard, "rows": sum(counts.values()), "counts": dict(counts),
            "content_ids": sorted(ids), "files": files}


def run(args):
    from boto3.s3.transfer import TransferConfig

    started = time.monotonic()
    started_at = now()
    output = args.run_root / "output"
    output.mkdir(parents=True, exist_ok=True)
    s3 = s3_client("us-east-1")
    source_bucket, source_prefix = parse_s3_uri(args.source_uri)
    target_bucket, target_prefix = parse_s3_uri(args.output_uri)
    if target_bucket != "swh2p4" or target_prefix != parse_s3_uri(OUTPUT_URI)[1]:
        raise ValueError("output must use the dedicated candidate_quality_v1 prefix")
    items = list_source_objects(s3, source_bucket, source_prefix)
    annotation_existing = output / "content_annotations.jsonl"
    prior_annotation_sha = sha256_file(annotation_existing) if annotation_existing.exists() else None
    annotation_path, content_counts = build_annotations(args, output)
    annotations = load_annotations(annotation_path)
    fingerprint = hashlib.sha256(json.dumps({
        "rule_version": RULE_VERSION, "code_sha256": sha256_file(Path(__file__)),
        "annotations_sha256": sha256_file(annotation_path), "source_objects": items,
        "output_uri": args.output_uri}, sort_keys=True).encode()).hexdigest()
    state_path = args.run_root / "pipeline.json"
    if state_path.exists():
        previous = json.loads(state_path.read_text())
        if previous["fingerprint"] != fingerprint:
            previous_inventory_path = output / "source_inventory.json"
            same_inventory = (previous_inventory_path.exists() and
                              json.loads(previous_inventory_path.read_text()).get("objects") == items)
            if not (args.resume_transport_fix and previous.get("rule_version") == RULE_VERSION
                    and previous.get("phase") == "exporting" and same_inventory
                    and prior_annotation_sha == sha256_file(annotation_path)):
                raise RuntimeError("run inputs or quality rules changed; use a new run root")
            fingerprint = previous["fingerprint"]
            LOG.warning("resuming prior checkpoints after transfer-only code change")
    state = {"rule_version": RULE_VERSION, "fingerprint": fingerprint, "phase": "exporting",
             "started_at_utc": started_at, "source_uri": args.source_uri, "output_uri": args.output_uri}
    save_json(state_path, state)
    save_json(output / "source_inventory.json", {"source_uri": args.source_uri, "objects": items})
    LOG.info("started_at_utc=%s source_csvs=%d source_bytes=%d candidate_contents=%d",
             started_at, len(items), sum(item["bytes"] for item in items), len(annotations))

    def process(item):
        shard = item["shard"]
        checkpoint_path = output / "checkpoints" / "shard-{}.json".format(shard)
        if checkpoint_path.exists():
            saved = json.loads(checkpoint_path.read_text())
            if saved["fingerprint"] == fingerprint and saved["source_etag"] == item["etag"]:
                for entry in saved["result"]["files"]:
                    path = Path(entry["path"])
                    remote = s3.head_object(Bucket=target_bucket, Key=entry["key"])
                    if (not path.exists() or sha256_file(path) != entry["sha256"]
                            or remote["Metadata"].get("sha256") != entry["sha256"]):
                        break
                else:
                    return saved["result"]
        relative = item["key"][len(source_prefix) + 1:]
        source_path = args.run_root / "source_cache" / relative
        receipt_path = source_path.with_suffix(".receipt.json")
        source_path.parent.mkdir(parents=True, exist_ok=True)
        receipt = json.loads(receipt_path.read_text()) if receipt_path.exists() else None
        if not (receipt and receipt["etag"] == item["etag"] and source_path.exists()
                and sha256_file(source_path) == receipt["sha256"]):
            temporary = source_path.with_suffix(".download")
            if s3.head_object(Bucket=source_bucket, Key=item["key"])["ETag"] != item["etag"]:
                raise RuntimeError("source ETag changed before download: " + item["key"])
            s3.download_file(source_bucket, item["key"], str(temporary),
                             Config=TransferConfig(max_concurrency=2,
                                                   multipart_chunksize=16 * 1024 * 1024))
            if temporary.stat().st_size != item["bytes"]:
                raise RuntimeError("source size changed: " + item["key"])
            if s3.head_object(Bucket=source_bucket, Key=item["key"])["ETag"] != item["etag"]:
                raise RuntimeError("source ETag changed after download: " + item["key"])
            os.replace(str(temporary), str(source_path))
            save_json(receipt_path, {"etag": item["etag"], "sha256": sha256_file(source_path)})
        result = transform_shard(source_path, output, shard, annotations)
        for entry in result["files"]:
            path = Path(entry["path"])
            key = target_prefix + "/" + entry["group"] + "/source_shard={}/part.csv.gz".format(shard)
            receipt = publish_file(s3, path, target_bucket, key, "text/csv")
            entry["key"] = receipt["key"]
        save_json(checkpoint_path, {"fingerprint": fingerprint, "source_etag": item["etag"],
                                    "result": result})
        return result

    results = []
    failures = []
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {executor.submit(process, item): item["shard"] for item in items}
        for done, future in enumerate(as_completed(futures), 1):
            try:
                result = future.result()
            except Exception as exc:
                failures.append((futures[future], repr(exc)))
                LOG.exception("export_shard_failed shard=%d completion=%d/%d",
                              futures[future], done, len(items))
                continue
            results.append(result)
            elapsed = time.monotonic() - started
            processed = sum(row["rows"] for row in results)
            state.update(completed_shards=done, processed_rows=processed, elapsed_seconds=round(elapsed, 2),
                         updated_at_utc=now())
            save_json(state_path, state)
            LOG.info("export_progress csv=%d/%d rows=%d/%d elapsed_s=%.1f eta_s=%.1f",
                     done, len(items), processed, EXPECTED_ROWS, elapsed, elapsed / done * (len(items) - done))
    if failures:
        raise RuntimeError("{} shards failed; completed checkpoints are resumable".format(len(failures)))
    rows = collections.Counter()
    seen_ids = set()
    directories = {group: set() for group in GROUPS}
    for result in results:
        rows.update(result["counts"])
        seen_ids.update(result["content_ids"])
        for group in GROUPS:
            path = output / "checkpoints" / "dirs-{}-{}.txt.gz".format(result["shard"], group)
            with gzip.open(str(path), "rt", encoding="ascii") as stream:
                directories[group].update(line.strip() for line in stream)
    if sum(rows.values()) != EXPECTED_ROWS or seen_ids != set(annotations):
        raise RuntimeError("candidate rows or content ID set changed during quality export")
    metrics = {"rule_version": RULE_VERSION, "fingerprint": fingerprint,
               "executed_script_sha256": sha256_file(Path(__file__)),
               "started_at_utc": started_at, "completed_at_utc": now(),
               "source_candidate_rows": EXPECTED_ROWS, "source_shards": len(items),
               "unique_content_ids": len(annotations), "content_status_counts": dict(content_counts),
               "hit_status_counts": dict(rows), "unique_parent_directories": {
                   group: len(values) for group, values in directories.items()},
               "all_candidate_parent_directories": len(set().union(*directories.values())),
               "source_uri": args.source_uri, "output_uri": args.output_uri,
               "p4_16_compiler_confirmed": False, "repository_mapping_completed": False,
               "size_500kb_is_diagnostic": True, "duplicate_line_ratio_is_diagnostic": True}
    save_json(output / "metrics.json", metrics)
    manifest = {"rule_version": RULE_VERSION, "fingerprint": fingerprint,
                "source_uri": args.source_uri, "output_uri": args.output_uri,
                "all_hits_uri": BASE + "/filtered_v2/all_annotated_hits/",
                "original_candidate_csv_unchanged": True, "metrics": metrics,
                "csv_objects": [entry for result in sorted(results, key=lambda row: row["shard"])
                                for entry in result["files"]]}
    save_json(output / "manifest.json", manifest)
    for name in ("content_annotations.jsonl", "source_inventory.json", "metrics.json", "manifest.json"):
        publish_file(s3, output / name, target_bucket, target_prefix + "/" + name,
                     "application/x-ndjson" if name.endswith(".jsonl") else "application/json")
    with s3.get_object(Bucket=target_bucket, Key=target_prefix + "/metrics.json")["Body"] as body:
        if json.loads(body.read()) != metrics:
            raise RuntimeError("remote metrics differ from local metrics")
    save_json(output / "_COMPLETE.json", {"fingerprint": fingerprint,
              "manifest_sha256": sha256_file(output / "manifest.json"),
              "completed_at_utc": now(), "source_candidate_rows": EXPECTED_ROWS})
    publish_file(s3, output / "_COMPLETE.json", target_bucket,
                 target_prefix + "/_COMPLETE.json", "application/json")
    state.update(phase="complete", metrics=metrics, completed_at_utc=now())
    save_json(state_path, state)
    LOG.info("completed metrics=%s", json.dumps(metrics, sort_keys=True))
    return metrics


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--screen-dir", type=Path, default=ROOT / "screen")
    parser.add_argument("--review-annotations", type=Path,
                        default=ROOT / "review-v2/content_annotations.jsonl")
    parser.add_argument("--special5", type=Path,
                        default=ROOT / "candidate-quality-special5-v1/content_annotations.jsonl")
    parser.add_argument("--raw-cache", type=Path, default=ROOT / "screen/raw_cache")
    parser.add_argument("--run-root", type=Path, default=ROOT / "candidate-quality-v1")
    parser.add_argument("--source-uri", default=SOURCE_URI)
    parser.add_argument("--output-uri", default=OUTPUT_URI)
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--resume-transport-fix", action="store_true",
                        help="reuse prior checkpoints only when annotations and source inventory match")
    args = parser.parse_args()
    if not 1 <= args.workers <= 16:
        parser.error("workers must be in 1..16")
    csv.field_size_limit(16 * 1024 * 1024)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    run(args)


if __name__ == "__main__":
    main()
