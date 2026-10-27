#!/usr/bin/env python3
"""Measure lexical similarity between P4 continuation output and its GT span.

The continuation benchmark stores one hidden reference target per case.  This
tool compares only that target span with ``model_completion.txt``; comparing
the assembled P4 files would mostly measure the shared prefix and suffix.

The paper's primary metric is compilability and its coherence/functionality
labels are manual.  The metrics here are therefore diagnostics, not a
replacement for the BMv2 compile-through rate:

* raw exact match compares bytes;
* normalized exact match ignores line-ending and trailing-whitespace changes;
* token exact match ignores whitespace and comments while preserving token
  order;
* token edit similarity is one minus normalized token Levenshtein distance.

The script is deliberately standard-library only and can analyze a run while
it is still being produced.  Cases missing one of the two text artifacts are
reported as unscored instead of terminating the analysis.
"""

import argparse
import collections
import hashlib
import json
import re
import statistics
import sys
from pathlib import Path


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("run_dir", type=Path, help="continuation_* benchmark run directory")
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="summary JSON path (default: <run_dir>/similarity_summary.json)",
    )
    parser.add_argument(
        "--jsonl-output",
        type=Path,
        default=None,
        help="per-task JSONL path (default: <run_dir>/similarity.jsonl)",
    )
    return parser.parse_args()


def read_json(path):
    with path.open(encoding="utf-8") as handle:
        return json.load(handle)


def read_jsonl(path):
    if not path.is_file():
        return []
    records = []
    with path.open(encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, 1):
            if not line.strip():
                continue
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError as exc:
                raise ValueError("{}:{} is invalid JSON: {}".format(path, line_number, exc))
    return records


def normalize_text(value):
    """Normalize formatting without changing source tokens or comments."""
    value = value.replace("\r\n", "\n").replace("\r", "\n")
    lines = [line.rstrip() for line in value.split("\n")]
    while lines and not lines[0].strip():
        lines.pop(0)
    while lines and not lines[-1].strip():
        lines.pop()
    return "\n".join(lines)


_MULTI_CHAR_TOKENS = tuple(
    sorted(
        (
            "<<<",
            ">>>",
            "<<=",
            ">>=",
            "==",
            "!=",
            "<=",
            ">=",
            "&&",
            "||",
            "<<",
            ">>",
            "++",
            "--",
            "->",
            "+=",
            "-=",
            "*=",
            "/=",
            "%=",
            "&=",
            "|=",
            "^=",
        ),
        key=len,
        reverse=True,
    )
)


def tokenize_p4(value):
    """Tokenize enough P4 lexical structure for stable code comparison.

    Comments and whitespace are discarded.  Identifiers, numeric literals,
    quoted literals and operators are retained in source order.  This is not
    intended to replace p4c's lexer; it only avoids treating indentation and
    comments as semantic differences in a diagnostic metric.
    """
    tokens = []
    index = 0
    length = len(value)
    while index < length:
        char = value[index]
        next_char = value[index + 1] if index + 1 < length else ""
        if char.isspace():
            index += 1
            continue
        if char == "/" and next_char == "/":
            index += 2
            while index < length and value[index] not in "\r\n":
                index += 1
            continue
        if char == "/" and next_char == "*":
            end = value.find("*/", index + 2)
            index = length if end < 0 else end + 2
            continue
        if char in ("'", '"'):
            quote = char
            start = index
            index += 1
            escaped = False
            while index < length:
                current = value[index]
                index += 1
                if escaped:
                    escaped = False
                elif current == "\\":
                    escaped = True
                elif current == quote:
                    break
            tokens.append(value[start:index])
            continue
        if char.isalpha() or char == "_":
            start = index
            index += 1
            while index < length and (value[index].isalnum() or value[index] == "_"):
                index += 1
            tokens.append(value[start:index])
            continue
        if char.isdigit():
            start = index
            index += 1
            # P4 literals such as 4w4, 32w1, 0x0800 and 8s0 are best kept as
            # one token.  A decimal point is included for real literals.
            while index < length and (
                value[index].isalnum() or value[index] in "_."
            ):
                index += 1
            tokens.append(value[start:index])
            continue
        matched = None
        for candidate in _MULTI_CHAR_TOKENS:
            if value.startswith(candidate, index):
                matched = candidate
                break
        if matched is not None:
            tokens.append(matched)
            index += len(matched)
        else:
            tokens.append(char)
            index += 1
    return tokens


def levenshtein_distance(left, right):
    if len(left) < len(right):
        left, right = right, left
    if not right:
        return len(left)
    previous = list(range(len(right) + 1))
    for left_index, left_item in enumerate(left, 1):
        current = [left_index]
        for right_index, right_item in enumerate(right, 1):
            insert_cost = current[right_index - 1] + 1
            delete_cost = previous[right_index] + 1
            replace_cost = previous[right_index - 1] + (left_item != right_item)
            current.append(min(insert_cost, delete_cost, replace_cost))
        previous = current
    return previous[-1]


def edit_similarity(left, right):
    denominator = max(len(left), len(right))
    if denominator == 0:
        return 1.0
    return 1.0 - levenshtein_distance(left, right) / float(denominator)


def f1_score(left, right):
    """Order-insensitive token multiset F1, retained as a diagnostic."""
    if not left and not right:
        return 1.0
    if not left or not right:
        return 0.0
    left_counts = collections.Counter(left)
    right_counts = collections.Counter(right)
    overlap = sum((left_counts & right_counts).values())
    precision = overlap / float(len(left))
    recall = overlap / float(len(right))
    if precision + recall == 0:
        return 0.0
    return 2.0 * precision * recall / (precision + recall)


def safe_mean(values):
    return statistics.mean(values) if values else None


def safe_median(values):
    return statistics.median(values) if values else None


def rate(values):
    return sum(bool(value) for value in values) / float(len(values)) if values else None


def task_key(record):
    return record.get("task_id"), record.get("attempt", 1)


def case_index(run_dir):
    index = {}
    for path in sorted(run_dir.iterdir()):
        if not path.is_dir():
            continue
        result_path = path / "result.json"
        if not result_path.is_file():
            continue
        try:
            result = read_json(result_path)
        except (OSError, ValueError, json.JSONDecodeError):
            continue
        index[task_key(result)] = path
    return index


def metric_record(result, case_dir):
    pred_path = case_dir / "model_completion.txt"
    target_path = case_dir / "reference_target.p4"
    if not pred_path.is_file() or not target_path.is_file():
        return {
            "task_id": result.get("task_id"),
            "attempt": result.get("attempt", 1),
            "case_dir": str(case_dir),
            "compiled": bool(result.get("compiled")),
            "status": result.get("status"),
            "target_kind": result.get("target_kind"),
            "scored": False,
            "missing_artifacts": [
                name
                for name, path in (
                    ("model_completion.txt", pred_path),
                    ("reference_target.p4", target_path),
                )
                if not path.is_file()
            ],
        }

    prediction = pred_path.read_text(encoding="utf-8")
    reference = target_path.read_text(encoding="utf-8")
    normalized_prediction = normalize_text(prediction)
    normalized_reference = normalize_text(reference)
    prediction_tokens = tokenize_p4(prediction)
    reference_tokens = tokenize_p4(reference)
    return {
        "task_id": result.get("task_id"),
        "attempt": result.get("attempt", 1),
        "case_dir": str(case_dir),
        "record_id": result.get("record_id"),
        "compiled": bool(result.get("compiled")),
        "status": result.get("status"),
        "target_kind": result.get("target_kind"),
        "fallback": bool(result.get("fallback")),
        "scored": True,
        "prediction_chars": len(prediction),
        "reference_chars": len(reference),
        "prediction_sha256": hashlib.sha256(prediction.encode("utf-8")).hexdigest(),
        "reference_target_sha256": hashlib.sha256(reference.encode("utf-8")).hexdigest(),
        "prediction_lines": len(normalized_prediction.splitlines()) if normalized_prediction else 0,
        "reference_lines": len(normalized_reference.splitlines()) if normalized_reference else 0,
        "prediction_tokens": len(prediction_tokens),
        "reference_tokens": len(reference_tokens),
        "length_ratio": (
            len(prediction_tokens) / float(len(reference_tokens))
            if reference_tokens
            else (1.0 if not prediction_tokens else None)
        ),
        "raw_exact_match": prediction == reference,
        "normalized_exact_match": normalized_prediction == normalized_reference,
        "token_exact_match": prediction_tokens == reference_tokens,
        "char_edit_similarity": edit_similarity(normalized_prediction, normalized_reference),
        "token_edit_similarity": edit_similarity(prediction_tokens, reference_tokens),
        "token_f1": f1_score(prediction_tokens, reference_tokens),
    }


def summarize_group(records):
    scored = [record for record in records if record.get("scored")]
    compiled = [record for record in records if record.get("compiled")]
    compiled_scored = [record for record in scored if record.get("compiled")]

    def values(name, source=scored):
        return [record[name] for record in source if record.get(name) is not None]

    return {
        "total": len(records),
        "compiled": len(compiled),
        "compile_rate": rate([record.get("compiled") for record in records]),
        "scored": len(scored),
        "scored_rate": rate([record.get("scored") for record in records]),
        "raw_exact_match_rate": rate([record["raw_exact_match"] for record in scored]),
        "normalized_exact_match_rate": rate(
            [record["normalized_exact_match"] for record in scored]
        ),
        "token_exact_match_rate": rate([record["token_exact_match"] for record in scored]),
        "mean_char_edit_similarity": safe_mean(values("char_edit_similarity")),
        "median_char_edit_similarity": safe_median(values("char_edit_similarity")),
        "mean_token_edit_similarity": safe_mean(values("token_edit_similarity")),
        "median_token_edit_similarity": safe_median(values("token_edit_similarity")),
        "mean_token_f1": safe_mean(values("token_f1")),
        "mean_length_ratio": safe_mean(values("length_ratio")),
        "compiled_scored": len(compiled_scored),
        "compiled_mean_token_edit_similarity": safe_mean(
            values("token_edit_similarity", compiled_scored)
        ),
        "compiled_token_exact_match_rate": rate(
            [record["token_exact_match"] for record in compiled_scored]
        ),
        "request_failures": sum(record.get("status") == "request_failed" for record in records),
        "missing_artifacts": sum(not record.get("scored") for record in records),
    }


def main():
    args = parse_args()
    run_dir = args.run_dir.resolve()
    if not run_dir.is_dir():
        raise SystemExit("run directory does not exist: {}".format(run_dir))
    results_path = run_dir / "results.jsonl"
    results = read_jsonl(results_path)
    if not results:
        raise SystemExit("no results found: {}".format(results_path))
    cases = case_index(run_dir)
    per_task = []
    for result in results:
        case_dir = cases.get(task_key(result))
        if case_dir is None:
            per_task.append(
                {
                    "task_id": result.get("task_id"),
                    "attempt": result.get("attempt", 1),
                    "case_dir": None,
                    "compiled": bool(result.get("compiled")),
                    "status": result.get("status"),
                    "target_kind": result.get("target_kind"),
                    "scored": False,
                    "missing_artifacts": ["case_directory"],
                }
            )
        else:
            per_task.append(metric_record(result, case_dir))

    by_target_kind = {}
    for target_kind in sorted({record.get("target_kind") or "unknown" for record in per_task}):
        group = [
            record
            for record in per_task
            if (record.get("target_kind") or "unknown") == target_kind
        ]
        by_target_kind[target_kind] = summarize_group(group)

    summary = {
        "schema_version": 1,
        "run_dir": str(run_dir),
        "results_file": str(results_path),
        "comparison": "model_completion.txt versus reference_target.p4",
        "tokenization": "P4-like lexer; whitespace and // or /* */ comments ignored; token order retained",
        "metrics_are_diagnostic": True,
        "overall": summarize_group(per_task),
        # The paper contains only local syntactic-element tasks.  The
        # generated dataset has a small EOF fallback category; keep it in the
        # overall denominator for auditability but expose the block-only
        # summary separately for a closer paper-style comparison.
        "block_tasks_only": summarize_group(
            [record for record in per_task if record.get("target_kind") != "fallback_line_suffix"]
        ),
        "by_target_kind": by_target_kind,
    }
    output = (args.output or run_dir / "similarity_summary.json").resolve()
    jsonl_output = (args.jsonl_output or run_dir / "similarity.jsonl").resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    jsonl_output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    with jsonl_output.open("w", encoding="utf-8") as handle:
        for record in per_task:
            handle.write(json.dumps(record, ensure_ascii=False) + "\n")
    print(json.dumps(summary["overall"], ensure_ascii=False, indent=2))
    print("Summary: {}".format(output))
    print("Per-task: {}".format(jsonl_output))
    return 0


if __name__ == "__main__":
    sys.exit(main())
