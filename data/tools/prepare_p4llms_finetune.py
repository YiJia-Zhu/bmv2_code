#!/usr/bin/env python3
"""Prepare leakage-safe p4llms PT and continuation-SFT datasets.

The split is performed on complete source files grouped by repository before
any continuation windows are generated.  This prevents multiple windows from
one source file (or repository) from leaking between train/dev/test.  The
script writes both raw-code records for LlamaFactory continued pretraining and
ShareGPT-style records for continuation SFT.
"""

import argparse
import collections
import datetime as dt
import hashlib
import json
import re
from pathlib import Path


DEFAULT_INPUT = Path("data/normalized/hf_p4llms/hf_p4llms_bmv2_eligible.jsonl")
DEFAULT_OUTPUT = Path("data/finetune")

SYSTEM_PROMPT = (
    "You are a P4_16 code-completion engine targeting BMv2 simple_switch "
    "with v1model. Complete the missing local syntactic element exactly from "
    "the cursor. Return only replacement source text: no Markdown fences, "
    "explanations, analysis, repeated prefix, or outer closing brace."
)

BLOCK_OPEN_RE = re.compile(
    r"^\s*(?:action\b|state\b|apply\b|control\b|parser\b|table\b).*\{"
)


def read_jsonl(path):
    with path.open(encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, 1):
            if not line.strip():
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError("{}:{} is invalid JSON: {}".format(path, line_number, exc))


def write_jsonl(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")


def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def content_digest(content):
    normalized = re.sub(r"\s+", " ", content).strip()
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def clean_records(input_path):
    records = []
    seen = set()
    skipped = collections.Counter()
    for record in read_jsonl(input_path):
        compile_status = record.get("compile_status") or record.get("status")
        if compile_status != "compiled" or record.get("returncode") != 0:
            skipped["not_compiled"] += 1
            continue
        if str(record.get("language", "")).lower() != "p4_16":
            skipped["not_p4_16"] += 1
            continue
        if record.get("architecture") != "v1model":
            skipped["not_v1model"] += 1
            continue
        content = record.get("cleaned_content") or record.get("content") or ""
        if not content.strip():
            skipped["empty"] += 1
            continue
        digest = record.get("normalized_sha256") or record.get("sha256") or content_digest(content)
        if digest in seen:
            skipped["exact_or_normalized_duplicate"] += 1
            continue
        seen.add(digest)
        row = dict(record)
        row["content"] = content
        row["source_sha256"] = digest
        row["family_id"] = record.get("source_repo_id") or record.get("source_repo") or digest
        records.append(row)
    if not records:
        raise ValueError("no clean p4llms P4_16/v1model records found in {}".format(input_path))
    return records, skipped


def group_key(record):
    return str(record.get("source_repo_id") or record.get("source_repo") or record["source_sha256"])


def split_by_repository(records, ratios):
    groups = collections.defaultdict(list)
    for record in records:
        groups[group_key(record)].append(record)

    # Greedy assignment by descending group size keeps the record counts close
    # to the requested ratios while never separating a repository.
    ordered = sorted(groups.items(), key=lambda item: (-len(item[1]), item[0]))
    counts = collections.Counter()
    assignments = {}
    split_names = ("train", "dev", "test")
    for key, group in ordered:
        scores = {
            name: (counts[name] + len(group)) / float(ratios[name])
            for name in split_names
        }
        selected = min(split_names, key=lambda name: (scores[name], name))
        assignments[key] = selected
        counts[selected] += len(group)

    split_rows = {name: [] for name in split_names}
    for record in records:
        split = assignments[group_key(record)]
        row = dict(record)
        row["split"] = split
        split_rows[split].append(row)
    for name in split_names:
        split_rows[name].sort(key=lambda row: row["record_id"])
    return split_rows, assignments


def scan_line(line, in_block_comment):
    """Return brace delta while ignoring comments and quoted strings."""
    index = 0
    delta = 0
    quote = None
    escaped = False
    while index < len(line):
        char = line[index]
        next_char = line[index + 1] if index + 1 < len(line) else ""
        if in_block_comment:
            if char == "*" and next_char == "/":
                in_block_comment = False
                index += 2
            else:
                index += 1
            continue
        if quote is not None:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == quote:
                quote = None
            index += 1
            continue
        if char == "/" and next_char == "/":
            break
        if char == "/" and next_char == "*":
            in_block_comment = True
            index += 2
            continue
        if char in ("'", '"'):
            quote = char
        elif char == "{":
            delta += 1
        elif char == "}":
            delta -= 1
        index += 1
    return delta, in_block_comment


def block_candidates(lines, min_lines, max_lines):
    depths = []
    depth = 0
    in_block_comment = False
    for line in lines:
        before = depth
        delta, in_block_comment = scan_line(line, in_block_comment)
        depth += delta
        depths.append((before, depth))

    candidates = []
    for open_index, line in enumerate(lines):
        before, after = depths[open_index]
        if after <= before or not BLOCK_OPEN_RE.match(line.rstrip("\n")):
            continue
        for close_index in range(open_index + 1, len(lines)):
            if depths[close_index][1] != before:
                continue
            body_count = close_index - open_index - 1
            if min_lines <= body_count <= max_lines:
                kind = line.strip().split(None, 1)[0]
                if kind.startswith("apply"):
                    kind = "apply"
                candidates.append(
                    {
                        "kind": kind,
                        "open_line": open_index,
                        "close_line": close_index,
                        "body_line_count": body_count,
                    }
                )
            break
    return candidates


def build_prompt(prefix, target_kind):
    if target_kind == "fallback_line_suffix":
        return (
            "Continue the following P4_16 source from the cursor. Output only "
            "the remaining source text; do not repeat the prefix or add prose.\n\n"
            "<P4_PREFIX>\n" + prefix + "<P4_CURSOR>\n"
        )
    return (
        "Complete the missing {} body in the following P4_16 source. The opening "
        "brace is already present. Output only the replacement body lines; do not "
        "output the outer closing brace, repeat the prefix, or add prose.\n\n"
        "<P4_PREFIX>\n{}<P4_CURSOR_BODY>\n"
    ).format(target_kind, prefix)


def continuation_rows(record, min_lines, max_lines, max_windows):
    lines = record["content"].splitlines(keepends=True)
    candidates = block_candidates(lines, min_lines, max_lines)
    if not candidates:
        target_lines = min(max_lines, max(min_lines, 2), max(1, len(lines) - 1))
        candidates = [
            {
                "kind": "fallback_line_suffix",
                "open_line": max(0, len(lines) - target_lines - 1),
                "close_line": len(lines),
                "body_line_count": target_lines,
            }
        ]
    # Stable hash ordering gives reproducible caps without favoring the first
    # parser state or action in a repository.
    candidates.sort(
        key=lambda candidate: hashlib.sha256(
            "{}:{}:{}".format(
                record["source_sha256"], candidate["open_line"], candidate["close_line"]
            ).encode("utf-8")
        ).hexdigest()
    )
    candidates = candidates[:max_windows]
    rows = []
    for index, candidate in enumerate(candidates):
        open_line = candidate["open_line"]
        close_line = candidate["close_line"]
        prefix = "".join(lines[: open_line + 1])
        target = "".join(lines[open_line + 1 : close_line])
        if not prefix.endswith("\n"):
            prefix += "\n"
        prompt = build_prompt(prefix, candidate["kind"])
        rows.append(
            {
                "id": "{}_cont_{:03d}".format(record["record_id"], index),
                "messages": [
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": prompt},
                    {"role": "assistant", "content": target},
                ],
                "metadata": {
                    "source_dataset": record.get("source_dataset"),
                    "source_record_id": record["record_id"],
                    "source_repo": record.get("source_repo"),
                    "source_repo_id": record.get("source_repo_id"),
                    "source_path": record.get("source_path"),
                    "source_sha256": record["source_sha256"],
                    "family_id": record["family_id"],
                    "split": record["split"],
                    "target_kind": candidate["kind"],
                    "target_lines": candidate["body_line_count"],
                    "prefix_lines": open_line + 1,
                    "reference_target_sha256": hashlib.sha256(target.encode("utf-8")).hexdigest(),
                },
            }
        )
    return rows


def dataset_info():
    return {
        "p4llms_p4_code_train": {
            "file_name": "p4llms_p4_code_train.jsonl",
            "formatting": "alpaca",
            "columns": {"prompt": "content", "query": None, "response": None}
        },
        "p4llms_p4_code_dev": {
            "file_name": "p4llms_p4_code_dev.jsonl",
            "formatting": "alpaca",
            "columns": {"prompt": "content", "query": None, "response": None}
        },
        "p4llms_continuation_train": {
            "file_name": "p4llms_continuation_train.jsonl",
            "formatting": "sharegpt",
            "columns": {"messages": "messages"},
            "tags": {
                "role_tag": "role",
                "content_tag": "content",
                "user_tag": "user",
                "assistant_tag": "assistant",
                "system_tag": "system"
            }
        },
        "p4llms_continuation_dev": {
            "file_name": "p4llms_continuation_dev.jsonl",
            "formatting": "sharegpt",
            "columns": {"messages": "messages"},
            "tags": {
                "role_tag": "role",
                "content_tag": "content",
                "user_tag": "user",
                "assistant_tag": "assistant",
                "system_tag": "system"
            }
        }
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--min-target-lines", type=int, default=2)
    parser.add_argument("--max-target-lines", type=int, default=10)
    parser.add_argument("--max-windows-per-file", type=int, default=8)
    args = parser.parse_args()
    if args.min_target_lines < 1 or args.max_target_lines < args.min_target_lines:
        parser.error("invalid target line bounds")
    if args.max_windows_per_file < 1:
        parser.error("--max-windows-per-file must be positive")

    input_path = args.input.resolve()
    output_root = args.output_root.resolve()
    records, skipped = clean_records(input_path)
    split_rows, assignments = split_by_repository(
        records, {"train": 0.8, "dev": 0.1, "test": 0.1}
    )

    # Preserve the source fields in these files so the existing compiler
    # benchmark can consume the p4llms test split directly.
    for split, rows in split_rows.items():
        write_jsonl(output_root / "p4llms_p4_code_{}.jsonl".format(split), rows)

    continuation = {"train": [], "dev": [], "test": []}
    kind_counts = {split: collections.Counter() for split in continuation}
    for split in continuation:
        for record in split_rows[split]:
            rows = continuation_rows(
                record,
                args.min_target_lines,
                args.max_target_lines,
                args.max_windows_per_file,
            )
            continuation[split].extend(rows)
            for row in rows:
                kind_counts[split][row["metadata"]["target_kind"]] += 1
        continuation[split].sort(key=lambda row: row["id"])
        # Test prompts are useful for audit, but are deliberately not registered
        # as a LlamaFactory training/eval dataset.
        write_jsonl(
            output_root / "p4llms_continuation_{}.jsonl".format(split), continuation[split]
        )

    write_json(output_root / "dataset_info.json", dataset_info())
    summary = {
        "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "input": str(input_path),
        "output_root": str(output_root),
        "source_record_count_after_dedup": len(records),
        "source_record_counts": {split: len(rows) for split, rows in split_rows.items()},
        "repository_counts": {
            split: sum(1 for value in assignments.values() if value == split)
            for split in ("train", "dev", "test")
        },
        "continuation_counts": {split: len(rows) for split, rows in continuation.items()},
        "target_kind_counts": {
            split: dict(sorted(counter.items())) for split, counter in kind_counts.items()
        },
        "skipped": dict(sorted(skipped.items())),
        "split_policy": "repository-level split before continuation-window generation",
        "positive_gate": "compile_status=compiled, returncode=0, language=P4_16, architecture=v1model",
    }
    write_json(output_root / "p4llms_finetune_manifest.json", summary)
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
