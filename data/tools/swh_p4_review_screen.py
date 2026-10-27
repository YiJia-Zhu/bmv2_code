#!/usr/bin/env python3
"""Recheck ambiguous SWH P4 contents using cached, hash-verified bytes."""

import argparse
import collections
import datetime as dt
import json
import os
import re
from pathlib import Path

from swh_p4_content_screen import (cache_content, code_without_comments_or_strings,
                                   git_blob_sha1, s3_client)


RULE_VERSION = "p4-review-screen-v2"
REVIEW_VERSIONS = {"unknown", "mixed_or_conflicted", "uninspected"}
P4_16_PATTERNS = {
    "width_type_declaration": re.compile(
        r"\b(?:bit|int|varbit)\s*<\s*[1-9]\d*\s*>\s+[A-Za-z_]\w*"),
    "control_parameters": re.compile(r"\bcontrol\s+[A-Za-z_]\w*\s*\("),
    "parser_parameters": re.compile(r"\bparser\s+[A-Za-z_]\w*\s*\("),
    "package_parameters": re.compile(r"\bpackage\s+[A-Za-z_]\w*\s*\("),
    "extern_definition": re.compile(r"\bextern\s+[A-Za-z_]\w*\s*(?:\(|\{)"),
}
P4_14_PATTERNS = {
    "legacy_field_operation": re.compile(
        r"\b(?:modify_field|add_header|remove_header|copy_header|"
        r"register_read|register_write|add_to_field|subtract_from_field|"
        r"bit_xor|bit_and|bit_or|shift_left|shift_right)\s*\("),
    "legacy_action_parameters": re.compile(
        r"\baction\s+[A-Za-z_]\w*\s*\(\s*[A-Za-z_]\w*\s*(?:,|\))"),
}
DIFF_HEADER = re.compile(r"\A--- [^\n]+\n\+\+\+ [^\n]+\n@@ ")
PATH_ONLY = re.compile(r"\A/?(?:[^\s\n]+/)+[^\s\n]+\.p4\s*\Z", re.I)


def classify_review(row, data):
    result = dict(row)
    result["review_rule_version"] = RULE_VERSION
    result["review_evidence_p4_16"] = []
    result["review_evidence_p4_14"] = []
    result["review_reasons"] = []
    result["review_version_status"] = "uninspected"
    result["review_disposition"] = "unresolved"
    if data is None:
        result["review_reasons"] = ["unavailable_or_over_limit"]
        return result
    if git_blob_sha1(data) != row["content_id"]:
        raise RuntimeError("raw cache hash mismatch: " + row["content_id"])
    result["review_sha1_git_verified"] = True
    result["review_raw_bytes"] = len(data)
    try:
        source = data.decode("utf-16" if data.startswith((b"\xff\xfe", b"\xfe\xff"))
                             else "utf-8-sig")
        result["review_encoding"] = "utf-16" if data.startswith(
            (b"\xff\xfe", b"\xfe\xff")) else "utf-8"
    except UnicodeDecodeError:
        if b"\x00" in data:
            result["review_reasons"] = ["unsupported_encoding_with_nul"]
            return result
        source = data.decode("latin-1")
        result["review_encoding"] = "latin-1-fallback"
    if "\x00" in source:
        result.update(review_version_status="non_source",
                      review_disposition="non_source",
                      review_reasons=["nul_character"])
        return result
    if DIFF_HEADER.match(source) or (len(source) <= 1024 and PATH_ONLY.fullmatch(source)):
        result.update(review_version_status="non_source",
                      review_disposition="non_source",
                      review_reasons=["unified_diff_or_path_pointer"])
        return result
    code = code_without_comments_or_strings(source)
    positive = [name for name, pattern in P4_16_PATTERNS.items() if pattern.search(code)]
    negative = [name for name, pattern in P4_14_PATTERNS.items() if pattern.search(code)]
    if row["version_status"] == "mixed_or_conflicted":
        positive = sorted(set(positive + row.get("positive_evidence", [])))
        negative = sorted(set(negative + row.get("negative_evidence", [])))
    result["review_evidence_p4_16"] = positive
    result["review_evidence_p4_14"] = negative
    if positive and negative:
        result.update(review_version_status="mixed_or_conflicted",
                      review_reasons=["conflicting_syntax_evidence"])
    elif positive:
        result.update(review_version_status="p4_16_likely",
                      review_disposition="p4_16_candidate")
    elif negative:
        result.update(review_version_status="p4_14_likely",
                      review_disposition="p4_14_likely")
    else:
        result.update(review_version_status="unknown",
                      review_reasons=["insufficient_version_evidence"])
    return result


def run(input_dir, output_path, s3_output_uri):
    cache_dir = input_dir / "raw_cache"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = output_path.with_suffix(".tmp")
    counts = collections.Counter()
    seen = set()
    existing_ids = []
    if temporary.exists():
        for line in temporary.open(encoding="utf-8"):
            saved = json.loads(line)
            existing_ids.append(saved["content_id"])
            counts[saved["review_disposition"]] += 1
    with temporary.open("a" if existing_ids else "w", encoding="utf-8") as output:
        for path in sorted(input_dir.glob("batch-*.jsonl")):
            for line in path.open(encoding="utf-8"):
                row = json.loads(line)
                if row["version_status"] not in REVIEW_VERSIONS:
                    continue
                content_id = row["content_id"]
                if content_id in seen:
                    raise RuntimeError("duplicate review ID: " + content_id)
                seen.add(content_id)
                if len(seen) <= len(existing_ids):
                    if content_id != existing_ids[len(seen) - 1]:
                        raise RuntimeError("review checkpoint does not match input order")
                    continue
                data = cache_content(cache_dir, content_id, None)
                if data is None and row["fetch_status"] == "too_large":
                    row["review_fetch_status"] = "over_limit_preserved_for_review"
                result = classify_review(row, data)
                counts[result["review_disposition"]] += 1
                output.write(json.dumps(result, sort_keys=True, ensure_ascii=False) + "\n")
                if len(seen) % 2000 == 0:
                    print(json.dumps({"event": "review_progress", "contents": len(seen),
                                      "counts": dict(counts)}), flush=True)
    if len(seen) != 26826 or len(existing_ids) > len(seen):
        raise RuntimeError("expected 26826 review content IDs, got {}".format(len(seen)))
    os.replace(str(temporary), str(output_path))
    if s3_output_uri:
        from swh_p4_content_screen import parse_s3_uri
        bucket, key = parse_s3_uri(s3_output_uri)
        s3_client("us-east-1").upload_file(str(output_path), bucket, key,
                                             ExtraArgs={"ContentType": "application/x-ndjson"})
    summary = {"completed_at_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
               "review_content_ids": len(seen), "disposition_counts": dict(counts),
               "rule_version": RULE_VERSION, "annotation_uri": s3_output_uri}
    print(json.dumps(summary, sort_keys=True), flush=True)
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-dir", type=Path, required=True)
    parser.add_argument("--output-path", type=Path, required=True)
    parser.add_argument("--s3-output-uri", required=True)
    args = parser.parse_args()
    run(args.input_dir, args.output_path, args.s3_output_uri)


if __name__ == "__main__":
    main()
