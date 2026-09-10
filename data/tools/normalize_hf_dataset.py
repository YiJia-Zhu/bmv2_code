#!/usr/bin/env python3
"""Normalize the downloaded SFU P4 Hugging Face dataset.

The Hugging Face snapshot is intentionally kept separate from the GitHub
normalization output.  ``raw_p4`` is retained for provenance while the
cleaned source distributed in ``individual_files`` is exposed as the
compile-ready ``content`` field.  No files under the raw snapshot are
modified.
"""

import argparse
import datetime as dt
import hashlib
import json
import re
from collections import Counter
from pathlib import Path


DATA_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_RAW_ROOT = DATA_ROOT / "raw" / "hf" / "SFU_p4_dataset"
DEFAULT_OUTPUT_ROOT = DATA_ROOT / "normalized" / "hf"


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--raw-root", type=Path, default=DEFAULT_RAW_ROOT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    return parser.parse_args()


def sha256_text(value):
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def safe_repo_id(repo_path):
    value = str(repo_path).replace("raw-repositories/", "", 1)
    value = value.replace("/", "__")
    return "hf_" + re.sub(r"[^A-Za-z0-9_.-]+", "_", value).strip("._")


def bool_value(value):
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def file_for_row(raw_root, index):
    prefix = "{:06d}__".format(index)
    matches = sorted((raw_root / "individual_files").glob(prefix + "*.p4"))
    return matches[0] if matches else None


def write_jsonl(path, records):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record, ensure_ascii=False) + "\n")


def main():
    args = parse_args()
    raw_root = args.raw_root.resolve()
    output_root = args.output_root.resolve()
    dataset_path = raw_root / "p4_dataset.jsonl"
    if not dataset_path.is_file():
        raise SystemExit("Hugging Face dataset not found: {}".format(dataset_path))
    output_root.mkdir(parents=True, exist_ok=True)

    rows = [json.loads(line) for line in dataset_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    all_records = []
    accepted_records = []
    filtered_records = []
    seen_hashes = {}
    stats = {
        "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "source_dataset": "SFU_p4_dataset",
        "raw_root": str(raw_root),
        "dataset_jsonl": str(dataset_path),
        "row_count": len(rows),
        "missing_individual_file": 0,
        "content_mismatch": 0,
        "duplicate_content": 0,
        "hf_claimed_compiles_true": 0,
        "by_language": Counter(),
        "by_architecture": Counter(),
        "by_repo": Counter(),
    }

    for index, row in enumerate(rows):
        source_file = file_for_row(raw_root, index)
        source_file_rel = str(source_file.relative_to(raw_root)) if source_file else None
        repo_path = row.get("repo_path", "")
        original_path = row.get("p4_file_path", "")
        cleaned = row.get("cleaned_p4") or row.get("raw_p4") or ""
        raw = row.get("raw_p4") or ""
        actual = source_file.read_text(encoding="utf-8", errors="replace") if source_file else None
        language = row.get("p4_version") or "unknown"
        value = (original_path + "\n" + cleaned).lower()
        if "v1model" in value or "v1switch" in value:
            architecture = "v1model"
        elif "psa.p4" in value or "package psa" in value:
            architecture = "psa"
        elif "pna.p4" in value or "package pna" in value:
            architecture = "pna"
        elif "tna.p4" in value or "package tna" in value:
            architecture = "tna/tofino"
        elif "ebpf" in value:
            architecture = "ebpf"
        else:
            architecture = "unknown"

        record_id = "hf:{:06d}:{}".format(index, original_path)
        source_repo_id = safe_repo_id(repo_path)
        content_hash = sha256_text(cleaned)
        duplicate_of = seen_hashes.get(content_hash)
        if duplicate_of is None:
            seen_hashes[content_hash] = record_id
        else:
            stats["duplicate_content"] += 1
        if source_file is None:
            stats["missing_individual_file"] += 1
        elif actual != cleaned:
            stats["content_mismatch"] += 1

        record = {
            "record_id": record_id,
            "source_repo_id": source_repo_id,
            "source_repo": row.get("repo_name"),
            "source_url": "https://huggingface.co/datasets/SFU-CONNECT/p4_dataset",
            "source_commit": None,
            "source_branch": None,
            "source_path": original_path,
            "source_file": source_file_rel,
            "source_dataset": "SFU_p4_dataset",
            "record_kind": "p4_source",
            "source_role": "p4_source",
            "license_hint": row.get("license") or "unknown",
            "size_bytes": len(cleaned.encode("utf-8")),
            "raw_size_bytes": len(raw.encode("utf-8")),
            "line_count": len(cleaned.splitlines()),
            "sha256": sha256_text(raw),
            "normalized_sha256": content_hash,
            "duplicate_of": duplicate_of,
            "status": "accepted" if source_file is not None and cleaned.strip() else "filtered",
            "filter_reason": None if source_file is not None and cleaned.strip() else (
                "missing_individual_file" if source_file is None else "empty_cleaned_p4"
            ),
            "language": language,
            "architecture": architecture,
            "is_probably_p4": bool(re.search(r"\b(header|parser|control|action|table|package|bit\s*<)", cleaned)),
            "content": cleaned,
            "cleaned_content": cleaned,
            "raw_content": raw,
            "hf_compiles": bool_value(row.get("compiles")),
            "annotation": row.get("annotation"),
            "detailed_annotation": row.get("detailed_annotation"),
            "readme_path": row.get("readme_path"),
            "inferred_from": row.get("inferred_from"),
            "desc": row.get("desc"),
        }
        if record["hf_compiles"]:
            stats["hf_claimed_compiles_true"] += 1
        all_records.append(record)
        stats["by_language"][language] += 1
        stats["by_architecture"][architecture] += 1
        stats["by_repo"][row.get("repo_name") or "unknown"] += 1
        if record["status"] == "accepted":
            accepted_records.append(record)
        else:
            filtered_records.append(record)

    # Every generated artifact is prefixed to make accidental mixing with the
    # GitHub dataset impossible, even when both directories are copied together.
    write_jsonl(output_root / "hf_all_records.jsonl", all_records)
    write_jsonl(output_root / "hf_accepted_records.jsonl", accepted_records)
    write_jsonl(output_root / "hf_p4_code.jsonl", [r for r in accepted_records if r["record_kind"] == "p4_source"])
    write_jsonl(output_root / "hf_filtered_records.jsonl", filtered_records)
    stats["accepted_count"] = len(accepted_records)
    stats["filtered_count"] = len(filtered_records)
    stats["by_language"] = dict(stats["by_language"])
    stats["by_architecture"] = dict(stats["by_architecture"])
    stats["by_repo"] = dict(stats["by_repo"])
    (output_root / "hf_stats.json").write_text(
        json.dumps(stats, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    print(json.dumps(stats, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
