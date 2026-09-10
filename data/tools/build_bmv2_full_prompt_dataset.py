#!/usr/bin/env python3
"""Create a full, provenance-linked BMv2 prompt JSONL from compile-clean P4.

This is an automatic reconstruction prompt set, not the reviewed 300-task
benchmark.  It deliberately does not place reference source in the prompt;
only detected P4/v1model feature requirements are exposed.
"""

import argparse
import json
import re
from pathlib import Path


FEATURE_PATTERNS = (
    ("parser", r"\bparser\s+\w+\s*\("),
    ("table", r"\btable\s+\w+\s*\{"),
    ("action", r"\baction\s+\w+\s*\("),
    ("register", r"\bregister\s*<"),
    ("counter", r"\bcounter\s*<"),
    ("meter", r"\bmeter\s*<"),
    ("clone", r"\bclone(?:3|_preserving_field_list)?\s*\("),
    ("recirculate", r"\brecirculate\s*\("),
    ("resubmit", r"\bresubmit\s*\("),
    ("digest", r"\bdigest\s*\("),
    ("hash", r"\bhash\s*\("),
    ("checksum", r"\b(?:verify|update)_checksum\s*\("),
    ("mark_to_drop", r"\bmark_to_drop\s*\("),
    ("lpm", r":\s*lpm\b"),
    ("ternary", r":\s*ternary\b"),
    ("range", r":\s*range\b"),
    ("v1model", r"\bv1model\.p4\b|\bV1Switch\b"),
)


def read_jsonl(path):
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                yield json.loads(line)


def features(content):
    return [name for name, pattern in FEATURE_PATTERNS if re.search(pattern, content, re.I)]


def prompt_for(features_found):
    requirements = ", ".join(features_found) or "parser, controls, and deparser"
    return (
        "You are a P4_16 engineer targeting BMv2 simple_switch with v1model. "
        "Return only one complete main.p4 source file, with no Markdown or explanation. "
        "Implement a self-contained program using these required language/target "
        "features: {}. Include a complete parser, ingress/egress controls, "
        "checksum controls when needed, deparser, and V1Switch main package. "
        "Use only standard P4_16 core/v1model facilities; do not use PSA, TNA, "
        "Tofino, FPGA, or non-v1model externs. The result must compile with "
        "p4c-bm2-ss --p4v 16.".format(requirements)
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with args.output.open("w", encoding="utf-8") as output:
        for row in read_jsonl(args.input):
            content = row.get("content") or row.get("cleaned_content") or ""
            detected = features(content)
            record = {
                "id": "full_{}".format(row["record_id"].replace(":", "__")),
                "prompt": prompt_for(detected),
                "dataset_role": "auto_generated_reconstruction_prompt",
                "prompt_version": 1,
                "source_record_id": row["record_id"],
                "source_repo_id": row.get("source_repo_id"),
                "source_path": row.get("source_path"),
                "source_sha256": row.get("sha256"),
                "architecture": row.get("architecture"),
                "language": row.get("language"),
                "detected_features": detected,
                "human_review": "pending",
            }
            output.write(json.dumps(record, ensure_ascii=False) + "\n")
            count += 1
    print(json.dumps({"records": count, "output": str(args.output)}, ensure_ascii=False))


if __name__ == "__main__":
    main()
