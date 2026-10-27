#!/usr/bin/env python3
"""Compile and compare completed PINC full-program rollout results.

The PINC rollout directories contain complete generated programs, but some
older runs were created with ``compile_skipped=true``.  This tool performs the
same BMv2 compile check used by ``bmv2_llm_compile_benchmark.py`` and writes
an aligned base/after report.  When both runs are supplied, the after run
defines the case/attempt population used for both models.

The F1 metric is lexical diagnostic only.  It compares the generated
``candidate.p4`` with ``ground_truth.p4`` after a P4-like tokenizer removes
whitespace and comments.  Token overlap is treated as a multiset, so F1 does
not measure semantic or functional correctness.
"""

import argparse
import csv
import datetime as dt
import json
import math
import os
import statistics
import subprocess
import sys
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parents[2]
DEFAULT_COMPILER = "p4c-bm2-ss"
EXACT_FIELDS = (
    "raw_exact_match",
    "normalized_exact_match",
    "token_exact_match",
)
METRIC_FIELDS = (
    "char_edit_similarity",
    "token_edit_similarity",
    "token_f1",
)
MULTI_CHAR_TOKENS = tuple(
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


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-run", type=Path, required=True)
    parser.add_argument("--after-run", type=Path, required=True)
    parser.add_argument("--compiler", default=DEFAULT_COMPILER)
    parser.add_argument(
        "--compile-workers",
        type=int,
        default=max(1, min(os.cpu_count() or 1, 8)),
    )
    parser.add_argument("--compile-timeout", type=float, default=30.0)
    parser.add_argument("--include-path", action="append", default=[])
    parser.add_argument(
        "--aggregate-only",
        action="store_true",
        help="reuse per-attempt compile_evaluation_result.json files without compiling",
    )
    args = parser.parse_args()
    if args.compile_workers < 1:
        parser.error("--compile-workers must be positive")
    if args.compile_timeout <= 0:
        parser.error("--compile-timeout must be positive")
    return args


def read_json(path):
    with path.open(encoding="utf-8") as handle:
        return json.load(handle)


def read_results(run_dir):
    path = run_dir / "results.jsonl"
    if not path.is_file():
        raise ValueError("results file does not exist: {}".format(path))
    results = {}
    with path.open(encoding="utf-8", errors="replace") as handle:
        for line_number, line in enumerate(handle, 1):
            if not line.strip():
                continue
            try:
                result = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError("{}:{} is invalid JSON: {}".format(path, line_number, exc))
            key = result_key(result)
            if key is None:
                raise ValueError("{}:{} has no valid case_id/attempt".format(path, line_number))
            if key in results:
                raise ValueError("{} contains duplicate key {}".format(path, key))
            results[key] = result
    if not results:
        raise ValueError("{} contains no results".format(path))
    return results


def result_key(result):
    case_id = result.get("case_id")
    attempt = result.get("attempt")
    if case_id is None or attempt is None:
        return None
    try:
        return str(case_id), int(attempt)
    except (TypeError, ValueError):
        return None


def index_case_dirs(run_dir):
    index = {}
    for case_dir in sorted(run_dir.iterdir()):
        if not case_dir.is_dir():
            continue
        result_path = case_dir / "result.json"
        if not result_path.is_file():
            continue
        try:
            result = read_json(result_path)
        except (OSError, ValueError, json.JSONDecodeError):
            continue
        key = result_key(result)
        if key is not None:
            index[key] = case_dir
    return index


def normalize_text(value):
    value = value.replace("\r\n", "\n").replace("\r", "\n")
    lines = [line.rstrip() for line in value.split("\n")]
    while lines and not lines[0].strip():
        lines.pop(0)
    while lines and not lines[-1].strip():
        lines.pop()
    return "\n".join(lines)


def tokenize_p4(value):
    """Tokenize enough P4 syntax for a stable lexical comparison."""
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
            while index < length and (
                value[index].isalnum() or value[index] in "_."
            ):
                index += 1
            tokens.append(value[start:index])
            continue
        matched = None
        for candidate in MULTI_CHAR_TOKENS:
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
            current.append(
                min(
                    current[right_index - 1] + 1,
                    previous[right_index] + 1,
                    previous[right_index - 1] + (left_item != right_item),
                )
            )
        previous = current
    return previous[-1]


def edit_similarity(left, right):
    denominator = max(len(left), len(right))
    if denominator == 0:
        return 1.0
    return 1.0 - levenshtein_distance(left, right) / float(denominator)


def token_f1(left, right):
    """Order-insensitive token multiset F1."""
    if not left and not right:
        return 1.0
    if not left or not right:
        return 0.0
    left_counts = defaultdict(int)
    right_counts = defaultdict(int)
    for token in left:
        left_counts[token] += 1
    for token in right:
        right_counts[token] += 1
    overlap = sum(min(left_counts[token], right_counts[token]) for token in left_counts)
    precision = overlap / float(len(left))
    recall = overlap / float(len(right))
    if precision + recall == 0:
        return 0.0
    return 2.0 * precision * recall / (precision + recall)


def score_candidate(case_dir):
    candidate_path = case_dir / "candidate.p4"
    reference_path = case_dir / "ground_truth.p4"
    if not candidate_path.is_file() or not reference_path.is_file():
        return {
            "scored": False,
            "missing_artifacts": [
                name
                for name, path in (
                    ("candidate.p4", candidate_path),
                    ("ground_truth.p4", reference_path),
                )
                if not path.is_file()
            ],
        }
    candidate = candidate_path.read_text(encoding="utf-8", errors="replace")
    reference = reference_path.read_text(encoding="utf-8", errors="replace")
    normalized_candidate = normalize_text(candidate)
    normalized_reference = normalize_text(reference)
    candidate_tokens = tokenize_p4(candidate)
    reference_tokens = tokenize_p4(reference)
    return {
        "scored": True,
        "prediction_chars": len(candidate),
        "reference_chars": len(reference),
        "prediction_tokens": len(candidate_tokens),
        "reference_tokens": len(reference_tokens),
        "raw_exact_match": candidate == reference,
        "normalized_exact_match": normalized_candidate == normalized_reference,
        "token_exact_match": candidate_tokens == reference_tokens,
        "char_edit_similarity": edit_similarity(
            normalized_candidate, normalized_reference
        ),
        "token_edit_similarity": edit_similarity(
            candidate_tokens, reference_tokens
        ),
        "token_f1": token_f1(candidate_tokens, reference_tokens),
    }


def decode_output(value):
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return value or ""


def compile_result(task):
    method = task["method"]
    key = task["key"]
    result = dict(task["result"])
    case_dir = task["case_dir"]
    candidate_path = case_dir / "candidate.p4"
    p4info_path = case_dir / "compile_program.p4info.txt"
    output_json_path = case_dir / "compile_program.json"
    command = [task["compiler"], "--p4v", "16"]
    for include_path in task["include_paths"]:
        command.extend(["-I", str(include_path)])
    command.extend(
        [
            "--p4runtime-files",
            str(p4info_path),
            "-o",
            str(output_json_path),
            str(candidate_path),
        ]
    )

    result["method"] = method
    result["compile_evaluated"] = True
    result["compile_evaluated_at"] = dt.datetime.now(dt.timezone.utc).isoformat()
    result["compile_skipped"] = False
    result["compiler_command"] = command
    result["generation_status"] = task["result"].get("status")

    if not candidate_path.is_file():
        result.update(
            {
                "compiled": False,
                "status": "compile_failed",
                "compiler_returncode": None,
                "compiler_error": "candidate.p4 does not exist",
            }
        )
        return key, result

    try:
        completed = subprocess.run(
            command,
            cwd=str(case_dir),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=task["compile_timeout"],
            check=False,
        )
        stdout = decode_output(completed.stdout)
        stderr = decode_output(completed.stderr)
        (case_dir / "compile.compiler.stdout.txt").write_text(
            stdout, encoding="utf-8"
        )
        (case_dir / "compile.compiler.stderr.txt").write_text(
            stderr, encoding="utf-8"
        )
        result.update(
            {
                "compiled": completed.returncode == 0,
                "status": "compiled" if completed.returncode == 0 else "compile_failed",
                "compiler_returncode": completed.returncode,
            }
        )
    except subprocess.TimeoutExpired as exc:
        stdout = decode_output(exc.stdout)
        stderr = decode_output(exc.stderr)
        (case_dir / "compile.compiler.stdout.txt").write_text(
            stdout, encoding="utf-8"
        )
        (case_dir / "compile.compiler.stderr.txt").write_text(
            stderr, encoding="utf-8"
        )
        result.update(
            {
                "compiled": False,
                "status": "compile_failed",
                "compiler_returncode": None,
                "compiler_error": "compiler timed out after {} seconds".format(
                    task["compile_timeout"]
                ),
            }
        )
    except OSError as exc:
        result.update(
            {
                "compiled": False,
                "status": "compile_failed",
                "compiler_returncode": None,
                "compiler_error": "{}: {}".format(type(exc).__name__, exc),
            }
        )

    result.update(score_candidate(case_dir))
    return key, result


def estimate_pass_at_k(sample_count, success_count, k):
    if k < 1 or sample_count < k:
        return None
    failures = sample_count - success_count
    if failures < k:
        return 1.0
    return 1.0 - math.comb(failures, k) / math.comb(sample_count, k)


def mean(values):
    return statistics.mean(values) if values else None


def median(values):
    return statistics.median(values) if values else None


def values(rows, field):
    return [
        float(row[field])
        for row in rows
        if row.get(field) is not None
    ]


def rate(rows, field):
    return (
        sum(bool(row.get(field)) for row in rows) / float(len(rows))
        if rows
        else None
    )


def per_case_stats(case_id, rows):
    rows = sorted(rows, key=lambda row: int(row.get("attempt", 0)))
    compiled = sum(bool(row.get("compiled")) for row in rows)
    stats = {
        "case_id": case_id,
        "samples": len(rows),
        "compiled": compiled,
        "compile_rate": compiled / float(len(rows)) if rows else None,
        "mean_token_f1": mean(values(rows, "token_f1")),
        "median_token_f1": median(values(rows, "token_f1")),
        "best_token_f1": max(values(rows, "token_f1"), default=None),
        "mean_char_edit_similarity": mean(values(rows, "char_edit_similarity")),
        "mean_token_edit_similarity": mean(values(rows, "token_edit_similarity")),
    }
    for field in EXACT_FIELDS:
        stats[field + "_count"] = sum(bool(row.get(field)) for row in rows)
    for k in range(1, 11):
        first_rows = rows[:k]
        # PINC/P4LLM pass@k uses all n recorded samples for the task and
        # estimates the chance that any k of those n samples succeeds.
        stats["pass@{}".format(k)] = estimate_pass_at_k(
            len(rows),
            compiled,
            k,
        )
        stats["best_of_{}_token_f1".format(k)] = max(
            values(first_rows, "token_f1"),
            default=None,
        )
    return stats


def aggregate_stats(grouped):
    all_rows = [row for rows in grouped.values() for row in rows]
    case_stats = [per_case_stats(case_id, rows) for case_id, rows in grouped.items()]
    result = per_case_stats("__overall__", all_rows)
    result["cases"] = len(grouped)
    for k in range(1, 11):
        pass_values = [
            item["pass@{}".format(k)]
            for item in case_stats
            if item["pass@{}".format(k)] is not None
        ]
        best_values = [
            item["best_of_{}_token_f1".format(k)]
            for item in case_stats
            if item["best_of_{}_token_f1".format(k)] is not None
        ]
        result["pass@{}".format(k)] = mean(pass_values)
        result["best_of_{}_token_f1".format(k)] = mean(best_values)
        result["pass@{}_eligible_cases".format(k)] = len(pass_values)
    compiled_rows = [row for row in all_rows if row.get("compiled")]
    result["compiled_mean_token_f1"] = mean(values(compiled_rows, "token_f1"))
    result["compiled_mean_char_edit_similarity"] = mean(
        values(compiled_rows, "char_edit_similarity")
    )
    result["compile_rate"] = (
        sum(bool(row.get("compiled")) for row in all_rows) / float(len(all_rows))
        if all_rows
        else None
    )
    return result


def write_json(path, value):
    path.write_text(
        json.dumps(value, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


def write_jsonl(path, rows):
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")


def write_model_csv(path, grouped):
    fields = [
        "case_id",
        "samples",
        "compiled",
        "compile_rate",
        "mean_token_f1",
        "median_token_f1",
        "best_token_f1",
        "mean_char_edit_similarity",
        "mean_token_edit_similarity",
        "raw_exact_match_count",
        "normalized_exact_match_count",
        "token_exact_match_count",
    ]
    fields.extend("pass@{}".format(k) for k in range(1, 11))
    fields.extend("best_of_{}_token_f1".format(k) for k in range(1, 11))
    rows = [per_case_stats(case_id, values) for case_id, values in grouped.items()]
    rows.append(aggregate_stats(grouped))
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field) for field in fields})


def write_comparison_csv(path, grouped_by_method):
    base = grouped_by_method["base"]
    after = grouped_by_method["after"]
    fields = [
        "case_id",
        "base_samples",
        "after_samples",
        "base_compiled",
        "after_compiled",
        "base_compile_rate",
        "after_compile_rate",
        "delta_compile_rate",
        "base_pass@1",
        "after_pass@1",
        "delta_pass@1",
        "base_pass@10",
        "after_pass@10",
        "delta_pass@10",
        "base_mean_token_f1",
        "after_mean_token_f1",
        "delta_mean_token_f1",
        "base_best_of_10_token_f1",
        "after_best_of_10_token_f1",
        "delta_best_of_10_token_f1",
        "base_mean_char_edit_similarity",
        "after_mean_char_edit_similarity",
        "delta_mean_char_edit_similarity",
    ]
    rows = []
    for case_id in sorted(after):
        base_stats = per_case_stats(case_id, base[case_id])
        after_stats = per_case_stats(case_id, after[case_id])
        row = {
            "case_id": case_id,
            "base_samples": base_stats["samples"],
            "after_samples": after_stats["samples"],
        }
        for field in (
            "compiled",
            "compile_rate",
            "pass@1",
            "pass@10",
            "mean_token_f1",
            "best_of_10_token_f1",
            "mean_char_edit_similarity",
        ):
            base_value = base_stats[field]
            after_value = after_stats[field]
            row["base_" + field] = base_value
            row["after_" + field] = after_value
            if field != "compiled":
                row["delta_" + field] = (
                    after_value - base_value
                    if base_value is not None and after_value is not None
                    else None
                )
        rows.append(row)

    base_stats = aggregate_stats(base)
    after_stats = aggregate_stats(after)
    overall = {
        "case_id": "__overall__",
        "base_samples": base_stats["samples"],
        "after_samples": after_stats["samples"],
    }
    for field in (
        "compiled",
        "compile_rate",
        "pass@1",
        "pass@10",
        "mean_token_f1",
        "best_of_10_token_f1",
        "mean_char_edit_similarity",
    ):
        base_value = base_stats[field]
        after_value = after_stats[field]
        overall["base_" + field] = base_value
        overall["after_" + field] = after_value
        if field != "compiled":
            overall["delta_" + field] = after_value - base_value
    rows.append(overall)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def write_updated_case_result(case_dir, result):
    write_json(case_dir / "compile_evaluation_result.json", result)


def load_evaluated_case_result(case_dir, key):
    path = case_dir / "compile_evaluation_result.json"
    if not path.is_file():
        raise ValueError("missing evaluated result: {}".format(path))
    result = read_json(path)
    if result_key(result) != key:
        raise ValueError("{} has key {}, expected {}".format(path, result_key(result), key))
    return result


def main():
    args = parse_args()
    base_dir = args.base_run.resolve()
    after_dir = args.after_run.resolve()
    for run_dir in (base_dir, after_dir):
        if not run_dir.is_dir():
            print("Run directory does not exist: {}".format(run_dir), file=sys.stderr)
            return 2
    if shutil_which(args.compiler) is None:
        print("Compiler not found: {}".format(args.compiler), file=sys.stderr)
        return 2

    try:
        base_results = read_results(base_dir)
        after_results = read_results(after_dir)
        base_dirs = index_case_dirs(base_dir)
        after_dirs = index_case_dirs(after_dir)
    except (OSError, ValueError) as exc:
        print("Input error: {}".format(exc), file=sys.stderr)
        return 2

    selected_keys = set(after_results)
    missing_base = sorted(selected_keys - set(base_results))
    if missing_base:
        print(
            "Base run is missing {} after keys, first: {}".format(
                len(missing_base), missing_base[:3]
            ),
            file=sys.stderr,
        )
        return 2
    missing_dirs = [
        (method, key)
        for method, directories in (("base", base_dirs), ("after", after_dirs))
        for key in selected_keys
        if key not in directories
    ]
    if missing_dirs:
        print(
            "Missing case directories, first: {}".format(missing_dirs[:3]),
            file=sys.stderr,
        )
        return 2

    include_paths = [str(Path(value).resolve()) for value in args.include_path]
    selected = {
        "base": {
            key: base_results[key]
            for key in selected_keys
        },
        "after": {
            key: after_results[key]
            for key in selected_keys
        },
    }
    directories = {"base": base_dirs, "after": after_dirs}
    tasks = []
    for method in ("base", "after"):
        for key in sorted(selected[method]):
            tasks.append(
                {
                    "method": method,
                    "key": key,
                    "result": selected[method][key],
                    "case_dir": directories[method][key],
                    "compiler": args.compiler,
                    "include_paths": include_paths,
                    "compile_timeout": args.compile_timeout,
                }
            )

    evaluated = {"base": {}, "after": {}}
    if args.aggregate_only:
        print("Reusing saved compile results for {} aligned attempts...".format(len(tasks)))
        for task in tasks:
            key = task["key"]
            evaluated[task["method"]][key] = load_evaluated_case_result(
                task["case_dir"], key
            )
    else:
        print(
            "Compiling {} aligned attempts ({} base + {} after) with {} workers...".format(
                len(tasks),
                len(selected["base"]),
                len(selected["after"]),
                args.compile_workers,
            ),
            flush=True,
        )
        completed_count = 0
        with ThreadPoolExecutor(max_workers=args.compile_workers) as executor:
            future_to_task = {
                executor.submit(compile_result, task): task for task in tasks
            }
            for future in as_completed(future_to_task):
                task = future_to_task[future]
                key = task["key"]
                try:
                    returned_key, result = future.result()
                except Exception as exc:
                    returned_key = key
                    result = dict(task["result"])
                    result.update(
                        {
                            "method": task["method"],
                            "compiled": False,
                            "status": "compile_failed",
                            "compile_evaluated": True,
                            "compile_skipped": False,
                            "compiler_returncode": None,
                            "compiler_error": "{}: {}".format(type(exc).__name__, exc),
                            "generation_status": task["result"].get("status"),
                        }
                    )
                evaluated[task["method"]][returned_key] = result
                write_updated_case_result(task["case_dir"], result)
                completed_count += 1
                status = "PASS" if result.get("compiled") else "FAIL"
                print(
                    "[{}/{}] {} {} attempt {}: {}".format(
                        completed_count,
                        len(tasks),
                        task["method"],
                        key[0],
                        key[1],
                        status,
                    ),
                    flush=True,
                )

    grouped = {}
    for method in ("base", "after"):
        grouped[method] = defaultdict(list)
        for key, result in sorted(evaluated[method].items()):
            result["case_id"] = key[0]
            result["attempt"] = key[1]
            grouped[method][key[0]].append(result)
        grouped[method] = dict(sorted(grouped[method].items()))

    timestamp = dt.datetime.now(dt.timezone.utc).isoformat()
    summaries = {}
    for method, run_dir in (("base", base_dir), ("after", after_dir)):
        summary = aggregate_stats(grouped[method])
        summary.update(
            {
                "generated_at": timestamp,
                "method": method,
                "run_dir": str(run_dir),
                "aligned_to_run": str(after_dir),
                "compile_definition": "{} --p4v 16 returned code 0".format(
                    args.compiler
                ),
                "f1_definition": (
                    "P4-like token multiset F1 between candidate.p4 and "
                    "ground_truth.p4; whitespace/comments ignored; token order ignored"
                ),
            }
        )
        summaries[method] = summary
        write_json(run_dir / "aligned_compile_summary.json", summary)
        write_model_csv(run_dir / "aligned_compile_stats.csv", grouped[method])
        write_jsonl(
            run_dir / "aligned_compile_results.jsonl",
            [
                result
                for case_results in grouped[method].values()
                for result in sorted(case_results, key=lambda row: int(row["attempt"]))
            ],
        )

    comparison_path = after_dir.parent / "sfu_pinc_base_after_aligned_compile_comparison.csv"
    write_comparison_csv(comparison_path, grouped)
    write_json(
        after_dir.parent / "sfu_pinc_base_after_aligned_compile_summary.json",
        {
            "generated_at": timestamp,
            "base": summaries["base"],
            "after": summaries["after"],
            "alignment": {
                "reference_run": str(after_dir),
                "case_count": len(grouped["after"]),
                "attempt_count": len(evaluated["after"]),
            },
        },
    )

    print("Base compile rate: {:.4f}".format(summaries["base"]["compile_rate"]))
    print("After compile rate: {:.4f}".format(summaries["after"]["compile_rate"]))
    print("Base mean token F1: {:.4f}".format(summaries["base"]["mean_token_f1"]))
    print("After mean token F1: {:.4f}".format(summaries["after"]["mean_token_f1"]))
    print("Comparison CSV: {}".format(comparison_path))
    return 0


def shutil_which(command):
    """Small standard-library equivalent kept local for Python 3.8 clarity."""
    for directory in os.environ.get("PATH", "").split(os.pathsep):
        candidate = Path(directory) / command
        if candidate.is_file() and os.access(str(candidate), os.X_OK):
            return str(candidate)
    return None


if __name__ == "__main__":
    sys.exit(main())
