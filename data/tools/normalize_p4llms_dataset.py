#!/usr/bin/env python3
"""Normalize the native ``p4llms/p4dataset`` Hugging Face export.

The native repository contains ``code.jsonl`` and the augmented
``code+text.jsonl``.  The latter is used as the canonical union because it
contains the code records plus the generated P4 knowledge/context records.
Both variants are preserved in the normalized output with an explicit
``source_variant`` field.  The native HF files are never modified.
"""

import argparse
import datetime as dt
import hashlib
import json
import re
from collections import Counter
from pathlib import Path


DATA_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_RAW_ROOT = DATA_ROOT / "raw" / "hf" / "p4dataset"
DEFAULT_OUTPUT_ROOT = DATA_ROOT / "normalized" / "hf_p4llms"
DATASET_URL = "https://huggingface.co/datasets/p4llms/p4dataset"


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--raw-root", type=Path, default=DEFAULT_RAW_ROOT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--max-bytes", type=int, default=500 * 1024)
    parser.add_argument("--max-lines", type=int, default=100000)
    return parser.parse_args()


def sha256_text(value):
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def safe_name(value):
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", str(value)).strip("._") or "record"


def safe_repo_id(value):
    value = str(value).replace("/", "__")
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", value).strip("._") or "unknown"


def read_jsonl(path):
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def write_jsonl(path, records):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record, ensure_ascii=False) + "\n")


def p4_hints(content, source_path):
    """Use the repository's GitHub-compatible P4 version/target heuristics."""
    # Import lazily so this script can still be copied with the dataset tools.
    from normalize_dataset import architecture_hint, is_probably_p4, p4_version

    path = Path(source_path)
    return (
        p4_version(content, path),
        architecture_hint(content, path),
        is_probably_p4(content),
    )


def quality_reason(content, is_p4, max_bytes, max_lines):
    if not content.strip():
        return "empty"
    if len(content.encode("utf-8")) > max_bytes:
        return "larger_than_max_bytes"
    if len(content.splitlines()) > max_lines:
        return "more_than_max_lines"
    # Context records are deliberately not required to look like P4.  P4
    # headers/fragments are retained as source material even when they do not
    # contain a complete parser/control/package.
    if is_p4 is False:
        return None
    return None


def normalize_rows(rows, variant, max_bytes, max_lines):
    records = []
    accepted = []
    filtered = []
    seen_content = {}
    stats = {
        "variant": variant,
        "rows": len(rows),
        "accepted": 0,
        "filtered": 0,
        "duplicates": 0,
        "by_record_kind": Counter(),
        "by_language": Counter(),
        "by_architecture": Counter(),
    }

    for index, row in enumerate(rows):
        repo_name = row.get("repo_name") or "unknown"
        original_path = row.get("repo_path") or ""
        content = row.get("content") or ""
        is_context = repo_name == "__dummy_name" and original_path == "__dummy_path"
        source_path = original_path.lstrip("/") or "unknown"
        if is_context:
            source_path = "__dummy_path/{:06d}.txt".format(index)
            language, architecture, probably_p4 = "unknown", "unknown", False
            record_kind = "project_context"
            source_role = "documentation_or_project_context"
        else:
            language, architecture, probably_p4 = p4_hints(content, source_path)
            record_kind = "p4_source"
            source_role = "p4_source"

        record_id = "hf_p4llms:{}:{:06d}:{}:{}".format(
            variant, index, safe_name(repo_name), safe_name(source_path)
        )
        content_hash = sha256_text(content)
        duplicate_of = seen_content.get(content_hash)
        if duplicate_of is None:
            seen_content[content_hash] = record_id
        else:
            stats["duplicates"] += 1
        reason = quality_reason(content, probably_p4, max_bytes, max_lines)
        record = {
            "record_id": record_id,
            "source_repo_id": "hf_p4llms_" + safe_repo_id(repo_name),
            "source_repo": None if is_context else repo_name,
            "source_url": DATASET_URL,
            "source_commit": None,
            "source_branch": None,
            "source_path": source_path,
            "original_source_path": original_path,
            "source_file": None,
            "source_dataset": "p4llms/p4dataset",
            "source_variant": variant,
            "native_id": row.get("id"),
            "record_kind": record_kind,
            "source_role": source_role,
            "license_hint": "unknown",
            "size_bytes": len(content.encode("utf-8")),
            "raw_size_bytes": len(content.encode("utf-8")),
            "line_count": len(content.splitlines()),
            "sha256": content_hash,
            "normalized_sha256": content_hash,
            "duplicate_of": duplicate_of,
            "status": "filtered" if reason else ("accepted_duplicate" if duplicate_of else "accepted"),
            "filter_reason": reason,
            "language": language,
            "architecture": architecture,
            "is_probably_p4": probably_p4,
            "content": content,
            "cleaned_content": content,
            "raw_content": content,
            "annotation": None,
            "detailed_annotation": None,
        }
        records.append(record)
        stats["by_record_kind"][record_kind] += 1
        stats["by_language"][language] += 1
        stats["by_architecture"][architecture] += 1
        if reason:
            filtered.append(record)
            stats["filtered"] += 1
        else:
            accepted.append(record)
            stats["accepted"] += 1

    for key in ("by_record_kind", "by_language", "by_architecture"):
        stats[key] = dict(stats[key])
    return records, accepted, filtered, stats


def main():
    args = parse_args()
    raw_root = args.raw_root.resolve()
    output_root = args.output_root.resolve()
    code_path = raw_root / "code.jsonl"
    code_text_path = raw_root / "code+text.jsonl"
    if not code_path.is_file() or not code_text_path.is_file():
        raise SystemExit("expected code.jsonl and code+text.jsonl under {}".format(raw_root))

    code_rows = read_jsonl(code_path)
    code_text_rows = read_jsonl(code_text_path)
    code_records, code_accepted, code_filtered, code_stats = normalize_rows(
        code_rows, "code", args.max_bytes, args.max_lines
    )
    text_records, text_accepted, text_filtered, text_stats = normalize_rows(
        code_text_rows, "code+text", args.max_bytes, args.max_lines
    )

    # ``code+text`` is the canonical final pipeline output.  Its code records
    # and dummy context records are all retained in the common all_records
    # stream; P4-only consumers use hf_p4llms_p4_code.jsonl.
    write_jsonl(output_root / "hf_p4llms_code.jsonl", code_accepted)
    write_jsonl(output_root / "hf_p4llms_code_text.jsonl", text_accepted)
    write_jsonl(output_root / "hf_p4llms_all_records.jsonl", text_records)
    write_jsonl(output_root / "hf_p4llms_accepted_records.jsonl", text_accepted)
    write_jsonl(
        output_root / "hf_p4llms_p4_code.jsonl",
        [record for record in text_accepted if record["record_kind"] == "p4_source"],
    )
    write_jsonl(output_root / "hf_p4llms_filtered_records.jsonl", text_filtered)

    stats = {
        "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "source_dataset": "p4llms/p4dataset",
        "source_url": DATASET_URL,
        "raw_root": str(raw_root),
        "canonical_variant": "code+text",
        "code": code_stats,
        "code_text": text_stats,
        "canonical_all_records": len(text_records),
        "canonical_accepted_records": len(text_accepted),
        "canonical_p4_records": sum(r["record_kind"] == "p4_source" for r in text_accepted),
        "canonical_filtered_records": len(text_filtered),
    }
    (output_root / "hf_p4llms_stats.json").write_text(
        json.dumps(stats, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    print(json.dumps(stats, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
