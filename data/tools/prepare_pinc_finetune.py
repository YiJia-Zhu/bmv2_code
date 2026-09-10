#!/usr/bin/env python3
"""Prepare the PINC intent-to-P4 dataset for LlamaFactory SFT.

This pipeline is intentionally separate from the existing p4llms/Generating
pipeline.  PINC examples are formatted as:

    high-level network intent -> one complete P4_16 program

The split is performed by repository before examples are written so that
closely related files from one source repository do not cross train/dev/test.
The raw SFU/PINC JSONL is never modified.
"""

import argparse
import collections
import datetime as dt
import hashlib
import json
import random
from pathlib import Path


DEFAULT_INPUT = Path("data/raw/hf/SFU_p4_dataset/p4_dataset.jsonl")
DEFAULT_OUTPUT = Path("data/finetune/pinc")

PINC_SYSTEM_PROMPT = (
    "You are a P4 code generator for PINC. Given a high-level network intent, "
    "generate exactly one complete, compilable P4_16 program targeting the BMv2 "
    "simple_switch with the v1model architecture. Include the required headers, "
    "parser, checksum controls, ingress and egress controls, deparser, and "
    "V1Switch main block. Return only P4 source code without Markdown fences, "
    "explanations, or analysis."
)


def read_jsonl(path):
    with path.open(encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, 1):
            if not line.strip():
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(
                    "{}:{} is invalid JSON: {}".format(path, line_number, exc)
                )


def write_jsonl(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")


def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(value, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


def normalized_digest(value):
    normalized = " ".join(str(value or "").split())
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def source_code(record):
    return (
        record.get("cleaned_p4")
        or record.get("cleaned_content")
        or record.get("content")
        or ""
    ).strip()


def source_repo(record):
    return (
        record.get("repo_name")
        or record.get("source_repo_id")
        or record.get("source_repo")
        or record.get("repo_path")
        or normalized_digest(source_code(record))
    )


def source_path(record):
    return (
        record.get("p4_file_path")
        or record.get("source_path")
        or record.get("p4_path")
        or ""
    )


def is_usable_record(record):
    code = source_code(record)
    annotation = str(record.get("annotation") or "").strip()
    if not code or not annotation:
        return False, "missing_code_or_annotation"

    if record.get("compiles") is False:
        return False, "dataset_compile_flag_false"
    if record.get("is_compile_clean") is False:
        return False, "local_compile_flag_false"
    if record.get("compile_status") not in (None, "compiled"):
        return False, "local_compile_status_not_compiled"
    if record.get("returncode") not in (None, 0):
        return False, "local_compile_returncode_nonzero"

    version = str(record.get("p4_version") or record.get("language") or "").lower()
    if version and version not in ("p4_16", "p4", "unknown"):
        return False, "not_p4_16"
    return True, None


def build_user_prompt(record, include_detailed_annotation=False):
    annotation = str(record["annotation"]).strip()
    prompt = (
        "Implement the following high-level network intent as one complete "
        "P4_16 program for a BMv2 v1model switch.\n\n"
        "NETWORK INTENT:\n"
        "{}\n\n"
        "OUTPUT REQUIREMENTS:\n"
        "- Return exactly one complete P4_16 source file.\n"
        "- Target BMv2 simple_switch and the v1model architecture.\n"
        "- Include all declarations needed to compile the program.\n"
        "- Do not return prose, Markdown fences, or multiple alternatives."
    ).format(annotation)

    if include_detailed_annotation:
        details = record.get("detailed_annotation") or []
        if isinstance(details, str):
            details = [details]
        details = [str(item).strip() for item in details if str(item).strip()]
        if details:
            prompt += "\n\nFUNCTIONAL DETAILS:\n" + "\n".join(
                "- " + item for item in details
            )
    return prompt


def load_tokenizer(tokenizer_path):
    if tokenizer_path is None:
        return None
    from transformers import AutoTokenizer

    tokenizer = AutoTokenizer.from_pretrained(
        str(tokenizer_path),
        trust_remote_code=True,
        use_fast=True,
    )
    # The audit must be able to measure the outlier program instead of
    # emitting a tokenizer warning before we drop it by --max-seq-len.
    tokenizer.model_max_length = max(
        int(getattr(tokenizer, "model_max_length", 0)), 10**9
    )
    return tokenizer


def chat_length(tokenizer, messages):
    kwargs = {
        "tokenize": True,
        "add_generation_prompt": False,
    }
    try:
        return len(
            tokenizer.apply_chat_template(
                messages,
                enable_thinking=False,
                **kwargs,
            )
        )
    except TypeError:
        return len(tokenizer.apply_chat_template(messages, **kwargs))


def build_example(record, split, include_detailed_annotation=False):
    code = source_code(record)
    messages = [
        {"role": "system", "content": PINC_SYSTEM_PROMPT},
        {
            "role": "user",
            "content": build_user_prompt(
                record,
                include_detailed_annotation=include_detailed_annotation,
            ),
        },
        {"role": "assistant", "content": code + "\n"},
    ]
    return {
        "id": "pinc_{}".format(
            record.get("record_id")
            or record.get("p4_file_path")
            or normalized_digest(code)[:16]
        ),
        "messages": messages,
        "metadata": {
            "source_dataset": "SFU-CONNECT/p4_dataset",
            "source_record_id": record.get("record_id"),
            "repo_name": record.get("repo_name"),
            "repo_path": record.get("repo_path"),
            "source_repo": record.get("source_repo"),
            "p4_file_path": record.get("p4_file_path"),
            "source_path": source_path(record),
            "license": record.get("license") or record.get("license_hint"),
            "p4_version": record.get("p4_version") or record.get("language"),
            "architecture": record.get("architecture") or "v1model",
            "source_code_sha256": normalized_digest(code),
            "split": split,
            "task": "intent_to_complete_p4_program",
        },
    }


def load_records(input_path):
    records = []
    skipped = collections.Counter()
    seen_code = set()

    for record in read_jsonl(input_path):
        usable, reason = is_usable_record(record)
        if not usable:
            skipped[reason] += 1
            continue

        code_digest = normalized_digest(source_code(record))
        if code_digest in seen_code:
            skipped["duplicate_normalized_p4"] += 1
            continue
        seen_code.add(code_digest)
        records.append(record)

    if not records:
        raise ValueError("no usable PINC records found in {}".format(input_path))
    return records, skipped


def split_by_repository(records, seed, ratios):
    grouped = collections.defaultdict(list)
    for record in records:
        grouped[str(source_repo(record))].append(record)

    group_items = list(grouped.items())
    random.Random(seed).shuffle(group_items)
    group_items.sort(key=lambda item: (-len(item[1]), item[0]))

    split_names = ("train", "dev", "test")
    counts = collections.Counter()
    assignments = {}
    for group_name, group in group_items:
        scores = {
            split: (counts[split] + len(group)) / float(ratios[split])
            for split in split_names
        }
        selected = min(split_names, key=lambda split: (scores[split], split))
        assignments[group_name] = selected
        counts[selected] += len(group)

    split_records = {split: [] for split in split_names}
    for record in records:
        split_records[assignments[str(source_repo(record))]].append(record)
    for split in split_names:
        split_records[split].sort(
            key=lambda record: (
                str(source_repo(record)),
                source_path(record),
                str(record.get("record_id") or ""),
            )
        )
    return split_records, assignments


def dataset_info():
    tags = {
        "role_tag": "role",
        "content_tag": "content",
        "user_tag": "user",
        "assistant_tag": "assistant",
        "system_tag": "system",
    }
    return {
        "pinc_sft_train": {
            "file_name": "pinc_sft_train.jsonl",
            "formatting": "sharegpt",
            "columns": {"messages": "messages"},
            "tags": tags,
        },
        "pinc_sft_dev": {
            "file_name": "pinc_sft_dev.jsonl",
            "formatting": "sharegpt",
            "columns": {"messages": "messages"},
            "tags": tags,
        },
        "pinc_sft_test": {
            "file_name": "pinc_sft_test.jsonl",
            "formatting": "sharegpt",
            "columns": {"messages": "messages"},
            "tags": tags,
        },
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--tokenizer", type=Path, default=None)
    parser.add_argument(
        "--max-seq-len",
        type=int,
        default=16384,
        help="Drop examples whose full Qwen chat-formatted length exceeds this.",
    )
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--train-ratio", type=float, default=0.8)
    parser.add_argument("--dev-ratio", type=float, default=0.1)
    parser.add_argument("--test-ratio", type=float, default=0.1)
    parser.add_argument(
        "--include-detailed-annotation",
        action="store_true",
        help="Append PINC detailed_annotation bullets to the user prompt.",
    )
    args = parser.parse_args()

    ratios = {
        "train": args.train_ratio,
        "dev": args.dev_ratio,
        "test": args.test_ratio,
    }
    if any(value <= 0 for value in ratios.values()) or abs(sum(ratios.values()) - 1.0) > 1e-6:
        parser.error("train/dev/test ratios must be positive and sum to 1")
    if args.max_seq_len < 256:
        parser.error("--max-seq-len must be at least 256")

    input_path = args.input.resolve()
    output_root = args.output_root.resolve()
    records, skipped = load_records(input_path)
    split_records, assignments = split_by_repository(records, args.seed, ratios)
    tokenizer = load_tokenizer(args.tokenizer.resolve() if args.tokenizer else None)

    examples = {split: [] for split in ("train", "dev", "test")}
    length_stats = {split: [] for split in examples}
    dropped_overlength = collections.Counter()
    for split, split_rows in split_records.items():
        for record in split_rows:
            example = build_example(
                record,
                split,
                include_detailed_annotation=args.include_detailed_annotation,
            )
            if tokenizer is not None:
                length = chat_length(tokenizer, example["messages"])
                if length > args.max_seq_len:
                    dropped_overlength[split] += 1
                    continue
                length_stats[split].append(length)
                example["metadata"]["chat_template_tokens"] = length
            examples[split].append(example)
        examples[split].sort(key=lambda row: row["id"])

    for split in examples:
        write_jsonl(output_root / "pinc_sft_{}.jsonl".format(split), examples[split])
    write_json(output_root / "dataset_info.json", dataset_info())

    summary = {
        "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "input": str(input_path),
        "output_root": str(output_root),
        "source_dataset": "SFU-CONNECT/p4_dataset",
        "source_records_after_dedup": len(records),
        "written_records": {split: len(rows) for split, rows in examples.items()},
        "repository_counts": {
            split: sum(1 for value in assignments.values() if value == split)
            for split in ("train", "dev", "test")
        },
        "written_repository_counts": {
            split: len(
                {
                    (
                        row["metadata"].get("repo_name")
                        or row["metadata"].get("source_repo")
                        or row["id"]
                    )
                    for row in rows
                }
            )
            for split, rows in examples.items()
        },
        "repository_split": True,
        "split_seed": args.seed,
        "split_ratios": ratios,
        "prompt_fields": ["annotation"],
        "include_detailed_annotation": args.include_detailed_annotation,
        "tokenizer": str(args.tokenizer.resolve()) if args.tokenizer else None,
        "max_seq_len": args.max_seq_len if tokenizer is not None else None,
        "chat_template_token_stats": {
            split: (
                {
                    "min": min(values),
                    "p50": sorted(values)[len(values) // 2],
                    "p90": sorted(values)[int(len(values) * 0.9)],
                    "max": max(values),
                }
                if values
                else {}
            )
            for split, values in length_stats.items()
        },
        "dropped_overlength": dict(sorted(dropped_overlength.items())),
        "skipped": dict(sorted(skipped.items())),
        "format": "ShareGPT messages: PINC intent -> complete P4_16 source",
    }
    write_json(output_root / "pinc_finetune_manifest.json", summary)
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
