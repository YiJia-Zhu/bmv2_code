#!/usr/bin/env python3
"""Build final BMv2 eligibility, exclusion, and repair-needed JSONL splits."""

import argparse
import datetime as dt
import json
from collections import Counter
from pathlib import Path


UNSUITABLE = {
    "intended_negative",
    "intermediate_artifact",
    "target_specific_or_arch_mismatch",
    "compiler_or_target_limitation",
    "incomplete_project_sample",
}
REPAIR_NEEDED = {"missing_dependency", "source_syntax_or_type_error"}


def read_jsonl(path):
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                yield json.loads(line)


def write_jsonl(path, rows):
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--classified-root", type=Path, required=True)
    parser.add_argument("--prefix", default="hf_p4llms_repaired_")
    parser.add_argument("--output-root", type=Path, required=True)
    args = parser.parse_args()
    source_root = args.classified_root.resolve()
    output_root = args.output_root.resolve()
    input_path = source_root / (args.prefix + "positive_compile_clean.jsonl")
    names = {
        "positive_compile_clean": input_path,
        "intended_negative": source_root / (args.prefix + "negative_compile_examples.jsonl"),
        "intermediate_artifact": source_root / (args.prefix + "intermediate_artifacts.jsonl"),
        "missing_dependency": source_root / (args.prefix + "missing_dependency_samples.jsonl"),
        "target_specific_or_arch_mismatch": source_root / (args.prefix + "unsupported_or_target_specific.jsonl"),
        "compiler_or_target_limitation": source_root / (args.prefix + "compiler_or_target_failures.jsonl"),
        "incomplete_project_sample": source_root / (args.prefix + "incomplete_project_samples.jsonl"),
        "source_syntax_or_type_error": source_root / (args.prefix + "source_compile_failures.jsonl"),
    }
    rows_by_class = {name: list(read_jsonl(path)) for name, path in names.items()}
    all_ids = []
    for rows in rows_by_class.values():
        all_ids.extend(row.get("record_id") for row in rows)
    if len(all_ids) != len(set(all_ids)):
        raise SystemExit("classified splits overlap by record_id")
    output_root.mkdir(parents=True, exist_ok=True)

    eligible = rows_by_class["positive_compile_clean"]
    excluded = []
    repair_needed = []
    for category, rows in rows_by_class.items():
        if category in UNSUITABLE:
            excluded.extend(rows)
        elif category in REPAIR_NEEDED:
            repair_needed.extend(rows)
    write_jsonl(output_root / "hf_p4llms_bmv2_eligible.jsonl", eligible)
    write_jsonl(output_root / "hf_p4llms_bmv2_excluded.jsonl", excluded)
    write_jsonl(output_root / "hf_p4llms_bmv2_repair_needed.jsonl", repair_needed)
    summary = {
        "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "classified_root": str(source_root),
        "prefix": args.prefix,
        "candidate_count": len(all_ids),
        "eligible_count": len(eligible),
        "excluded_count": len(excluded),
        "repair_needed_count": len(repair_needed),
        "excluded_categories": sorted(UNSUITABLE),
        "repair_needed_categories": sorted(REPAIR_NEEDED),
        "by_category": dict(sorted((name, len(rows)) for name, rows in rows_by_class.items())),
        "eligible_definition": "p4c-bm2-ss --p4v 16 returned code 0 after repository-context/include recovery",
        "excluded_definition": "negative tests, intermediate artifacts, non-v1model targets, BMv2 backend limitations, or incomplete fragments",
    }
    (output_root / "hf_p4llms_bmv2_split_summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
