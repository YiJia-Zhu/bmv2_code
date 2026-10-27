#!/usr/bin/env python3
"""Measure BMv2 compile-through for P4 continuation completions.

The task follows the auto-completion criterion from ``ref_paper``: show a
model a prefix ending at a missing syntactic element, append its continuation
at that cursor, restore the hidden reference tail, and count the task as
successful when the resulting P4_16/v1model source compiles without errors.
Warnings are retained in the per-task stderr file but do not affect the rate.

The input is dataset-agnostic: SFU, p4llms, or another normalized JSONL can be
used as long as records carry a clean P4_16/v1model compile result.  The hidden
suffix and the original provenance are retained in the run directory for
audit, but are never sent to the model.
"""

import argparse
import datetime as dt
import hashlib
import json
import math
import os
import re
import shutil
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent
SMOKE_DIR = SCRIPT_DIR.parent
REPO_ROOT = SMOKE_DIR.parent
DEFAULT_DATASET = REPO_ROOT / "data" / "normalized" / "hf" / "hf_positive_compile_clean.jsonl"
DEFAULT_BASE_URL = "http://127.0.0.1:18001/v1"
DEFAULT_API_KEY = "local-qwen3-14b"
DEFAULT_MODEL = "qwen3-14b-p4pt-3ep"
DEFAULT_COMPILER = "p4c-bm2-ss"
DEFAULT_INCLUDE_PATHS = (
    REPO_ROOT / "data" / "raw" / "hf" / "SFU_p4_dataset" / "individual_files",
    REPO_ROOT / "data" / "raw" / "hf" / "SFU_p4_dataset",
    REPO_ROOT / "data" / "raw" / "hf" / "p4dataset_supplemental",
    Path("/home/xiegr/p4-install/share/p4c/p4include"),
    Path("/usr/local/share/p4c/p4include"),
    Path("/usr/include"),
)

SYSTEM_PROMPT = (
    "You are a P4_16 code-completion engine targeting BMv2 simple_switch "
    "with v1model. Complete the missing local syntactic element exactly from "
    "the cursor. Return only replacement source text: no Markdown fences, "
    "explanations, analysis, repeated prefix, or outer closing brace."
)


def load_dotenv(path):
    if not path.is_file():
        return
    for original_line in path.read_text(encoding="utf-8").splitlines():
        line = original_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[7:].lstrip()
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", key):
            continue
        if value[:1] in ("'", '"'):
            quote = value[0]
            closing = value.find(quote, 1)
            if closing >= 0:
                value = value[1:closing]
        else:
            value = re.split(r"\s+#", value, maxsplit=1)[0].strip()
        os.environ.setdefault(key, value)


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env-file", type=Path, default=REPO_ROOT / ".env")
    parser.add_argument("--dataset", type=Path, default=DEFAULT_DATASET)
    parser.add_argument("--base-url", default=None)
    parser.add_argument("--api-key", default=None)
    parser.add_argument("--model", default=None)
    parser.add_argument("--compiler", default=DEFAULT_COMPILER)
    parser.add_argument("--include-path", action="append", default=[])
    parser.add_argument("--max-tasks", type=int, default=0, help="0 means all eligible records")
    parser.add_argument("--attempts", type=int, default=1)
    parser.add_argument("--target-min-lines", type=int, default=2)
    parser.add_argument("--target-max-lines", type=int, default=10)
    parser.add_argument(
        "--temperature",
        type=float,
        default=0.8,
        help="sampling temperature (default: %(default)s; use 0 for deterministic output)",
    )
    parser.add_argument("--max-tokens", type=int, default=8192)
    parser.add_argument(
        "--enable-thinking",
        action="store_true",
        help="allow Qwen3-style visible reasoning; disabled by default for code-only completion",
    )
    parser.add_argument("--request-timeout", type=float, default=300.0)
    parser.add_argument("--compile-timeout", type=float, default=30.0)
    parser.add_argument("--output-dir", type=Path, default=SMOKE_DIR / "runs" / "benchmark_runs")
    parser.add_argument("--run-dir", type=Path, help="reuse an existing run directory with --resume")
    parser.add_argument("--resume", action="store_true")
    args = parser.parse_args()
    if args.max_tasks < 0 or args.attempts < 1:
        parser.error("--max-tasks must be >= 0 and --attempts must be >= 1")
    if args.target_min_lines < 1 or args.target_max_lines < args.target_min_lines:
        parser.error("target line bounds are invalid")
    if args.max_tokens < 1 or args.request_timeout <= 0 or args.compile_timeout <= 0:
        parser.error("token and timeout values must be positive")
    load_dotenv(args.env_file.resolve())
    args.base_url = args.base_url or os.environ.get("QWEN3_VLLM_BASE_URL") or DEFAULT_BASE_URL
    args.api_key = (
        args.api_key
        or os.environ.get("QWEN3_VLLM_API_KEY")
        or os.environ.get("VLLM_API_KEY")
        or DEFAULT_API_KEY
    )
    args.model = args.model or os.environ.get("QWEN3_MODEL") or DEFAULT_MODEL
    return args


def safe_name(value):
    cleaned = re.sub(r"[^A-Za-z0-9_.-]+", "_", str(value)).strip("._")
    return (cleaned or "case")[:120]


def record_include_paths(record):
    """Resolve repository-local include roots from the supplemental overlay.

    Normalized p4llms compile records contain temporary include paths from the
    dataset build.  Those paths are not portable, so recover the equivalent
    local repository root when it exists and add the source directory and its
    ancestors.  The source directory is needed for includes such as
    ``#include \"include/headers.p4\"``.
    """
    repo_id = record.get("source_repo_id")
    source_path = record.get("source_path") or record.get("original_source_path")
    if not repo_id or not source_path:
        return []
    repo_root = REPO_ROOT / "data" / "raw" / "hf" / "p4dataset_supplemental" / str(repo_id)
    if not repo_root.is_dir():
        return []
    relative_path = Path(str(source_path).lstrip("/"))
    source_dir = repo_root / relative_path.parent
    paths = [repo_root]
    current = source_dir
    while current != repo_root and repo_root in current.parents:
        paths.append(current)
        current = current.parent
    include_dir = source_dir / "include"
    if include_dir.is_dir():
        paths.append(include_dir)
    return [path.resolve() for path in paths if path.is_dir()]


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def write_results(path, results):
    with path.open("w", encoding="utf-8") as handle:
        for result in results:
            handle.write(json.dumps(result, ensure_ascii=False) + "\n")


def append_jsonl(path, value):
    """Append one complete JSON record and flush it immediately."""
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(value, ensure_ascii=False) + "\n")
        handle.flush()


def estimate_pass_at_k(sample_count, success_count, k):
    """Estimate pass@k from n sampled candidates and c successful candidates."""
    if k < 1 or sample_count < k:
        return None
    failure_count = sample_count - success_count
    if failure_count < k:
        return 1.0
    return 1.0 - (
        math.comb(failure_count, k) / math.comb(sample_count, k)
    )


def summarize_pass_at_k(results, group_field, max_k):
    """Return aggregate and per-task pass@k values for grouped attempts."""
    grouped = {}
    for result in results:
        grouped.setdefault(result.get(group_field), []).append(result)

    aggregate = {}
    eligible_tasks = {}
    per_task = {}
    for group_id, group_results in grouped.items():
        sample_count = len(group_results)
        success_count = sum(1 for result in group_results if result.get("compiled"))
        per_task[str(group_id)] = {
            "samples": sample_count,
            "compiled": success_count,
            "pass_at_k": {},
        }
        for k in range(1, max_k + 1):
            estimate = estimate_pass_at_k(sample_count, success_count, k)
            per_task[str(group_id)]["pass_at_k"][str(k)] = estimate

    for k in range(1, max_k + 1):
        estimates = [
            task["pass_at_k"][str(k)]
            for task in per_task.values()
            if task["pass_at_k"][str(k)] is not None
        ]
        aggregate[str(k)] = sum(estimates) / len(estimates) if estimates else None
        eligible_tasks[str(k)] = len(estimates)

    return aggregate, eligible_tasks, per_task


def load_jsonl_keys(path):
    """Load task/attempt keys from an aggregate JSONL file for resume support."""
    if not path.is_file():
        return set()
    keys = set()
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        value = json.loads(line)
        keys.add((value.get("task_id"), value.get("attempt")))
    return keys


def load_eligible_records(path):
    if not path.is_file():
        raise ValueError("dataset JSONL not found: {}".format(path))
    records = []
    for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        if not line.strip():
            continue
        try:
            record = json.loads(line)
        except json.JSONDecodeError as exc:
            raise ValueError("{}:{} is not valid JSON: {}".format(path, line_number, exc))
        compile_status = record.get("compile_status") or record.get("status")
        if compile_status != "compiled" or record.get("returncode") != 0:
            continue
        if record.get("architecture") != "v1model" or record.get("language") != "p4_16":
            continue
        content = record.get("cleaned_content") or record.get("content") or ""
        if not content.strip():
            continue
        records.append(
            {
                "record_id": str(record.get("record_id", "sfu_{:06d}".format(line_number))),
                "source_dataset": record.get("source_dataset") or "unknown",
                "source_repo": record.get("source_repo"),
                "source_repo_id": record.get("source_repo_id"),
                "source_path": record.get("source_path"),
                "source_file": record.get("source_file"),
                "source_sha256": record.get("normalized_sha256") or record.get("sha256"),
                "content": content,
            }
        )
    if not records:
        raise ValueError("no clean v1model/P4_16 records found in {}".format(path))
    return records


BLOCK_OPEN_RE = re.compile(
    r"^\s*(?:action\b|state\b|apply\b|control\b|parser\b|table\b).*\{\s*(?://.*)?$"
)


def _scan_line(line, in_block_comment):
    """Return brace delta while ignoring comments and quoted strings."""
    i = 0
    delta = 0
    quote = None
    escaped = False
    while i < len(line):
        char = line[i]
        nxt = line[i + 1] if i + 1 < len(line) else ""
        if in_block_comment:
            if char == "*" and nxt == "/":
                in_block_comment = False
                i += 2
            else:
                i += 1
            continue
        if quote is not None:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == quote:
                quote = None
            i += 1
            continue
        if char == "/" and nxt == "/":
            break
        if char == "/" and nxt == "*":
            in_block_comment = True
            i += 2
            continue
        if char in ('"', "'"):
            quote = char
        elif char == "{":
            delta += 1
        elif char == "}":
            delta -= 1
        i += 1
    return delta, in_block_comment


def _block_candidates(lines, min_lines, max_lines):
    depths = []
    depth = 0
    in_block_comment = False
    for line in lines:
        before = depth
        delta, in_block_comment = _scan_line(line, in_block_comment)
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
            body_lines = close_index - open_index - 1
            if min_lines <= body_lines <= max_lines:
                kind = line.strip().split(None, 1)[0]
                if kind.startswith("apply"):
                    kind = "apply"
                candidates.append(
                    {
                        "kind": kind,
                        "open_line": open_index,
                        "close_line": close_index,
                        "body_line_count": body_lines,
                    }
                )
            break
    return candidates


def make_task(record, min_lines, max_lines):
    lines = record["content"].splitlines(keepends=True)
    if len(lines) < 2:
        raise ValueError("source has fewer than two lines: {}".format(record["record_id"]))
    candidates = _block_candidates(lines, min_lines, max_lines)
    fallback = False
    if candidates:
        # Stable selection gives every source one task while avoiding a bias
        # toward only the first parser state in each file.
        digest = hashlib.sha256(record["record_id"].encode("utf-8")).digest()
        selected = candidates[int.from_bytes(digest[:8], "big") % len(candidates)]
        open_line = selected["open_line"]
        close_line = selected["close_line"]
        target_kind = selected["kind"]
    else:
        # Four clean SFU files have no small balanced block. Keep them in the
        # denominator with a clearly marked short end-of-file fallback.
        target_lines = min(max_lines, max(min_lines, 2), len(lines) - 1)
        open_line = len(lines) - target_lines - 1
        close_line = len(lines)
        target_kind = "fallback_line_suffix"
        fallback = True
    prefix = "".join(lines[: open_line + 1])
    target = "".join(lines[open_line + 1 : close_line])
    suffix = "".join(lines[close_line:])
    if not prefix.endswith("\n"):
        prefix += "\n"
    return {
        "record_id": record["record_id"],
        "source_dataset": record["source_dataset"],
        "source_repo": record["source_repo"],
        "source_repo_id": record["source_repo_id"],
        "source_path": record["source_path"],
        "source_file": record["source_file"],
        "source_sha256": record["source_sha256"],
        "source_line_count": len(lines),
        "prefix_line_count": open_line + 1,
        "target_line_count": close_line - open_line - 1,
        "suffix_line_count": len(lines) - close_line,
        "target_kind": target_kind,
        "fallback": fallback,
        "prefix": prefix,
        "reference_target": target,
        "reference_suffix": suffix,
    }


def build_prompt(prefix, target_kind):
    if target_kind == "fallback_line_suffix":
        return (
            "Continue the following P4_16 source from the cursor. Output only "
            "the remaining source text; do not repeat the prefix or add prose.\n\n"
            "<P4_PREFIX>\n"
            + prefix
            + "<P4_CURSOR>\n"
        )
    instruction = (
        "Complete the missing {} body in the following P4_16 source. The opening "
        "brace is already present. Output only the replacement body lines; do not "
        "output the outer closing brace, repeat the prefix, or add prose.\n\n"
    ).format(target_kind)
    return instruction + "<P4_PREFIX>\n" + prefix + "<P4_CURSOR_BODY>\n"


def redacted(text, api_key):
    return text.replace(api_key, "<redacted>") if text and api_key else (text or "")


def build_request_payload(args, prompt):
    payload = {
        "model": args.model,
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": prompt},
        ],
        "temperature": args.temperature,
        "max_tokens": args.max_tokens,
        "stream": False,
    }
    # Qwen3 emits a visible <think> block unless explicitly disabled.  That
    # reasoning text consumes the completion budget and is not P4 source, so
    # code-completion benchmarks disable it by default.  Keep an opt-in flag
    # for experiments that intentionally measure reasoning-enabled output.
    if not args.enable_thinking:
        payload["chat_template_kwargs"] = {"enable_thinking": False}
    return payload


def call_model(args, payload):
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    request = urllib.request.Request(
        args.base_url.rstrip("/") + "/chat/completions",
        data=body,
        headers={
            "Authorization": "Bearer " + args.api_key,
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=args.request_timeout) as response:
            response_body = response.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError("API returned HTTP {}: {}".format(exc.code, redacted(detail, args.api_key)))
    except urllib.error.URLError as exc:
        raise RuntimeError("API request failed: {}".format(exc.reason))
    except TimeoutError:
        raise RuntimeError(
            "API request timed out after {} seconds".format(args.request_timeout)
        )
    try:
        parsed = json.loads(response_body)
        message = parsed["choices"][0]["message"]
        content = message.get("content")
    except (KeyError, IndexError, TypeError, json.JSONDecodeError) as exc:
        raise RuntimeError("API response did not contain choices[0].message.content: {}".format(exc))
    if not isinstance(content, str) or not content.strip():
        raise RuntimeError("API returned an empty message.content")
    return parsed, content


def _brace_delta(text):
    depth = 0
    in_block_comment = False
    for line in text.splitlines(True):
        delta, in_block_comment = _scan_line(line, in_block_comment)
        depth += delta
    return depth


def trim_outer_closing_brace(text):
    """Drop a model-emitted outer `}` and everything after it."""
    lines = text.splitlines(True)
    depth = 0
    in_block_comment = False
    for index, line in enumerate(lines):
        delta, in_block_comment = _scan_line(line, in_block_comment)
        if depth + delta < 0:
            # Keep content before the outer close. If it shares a line with
            # code, preserve the part before the closing brace.
            position = line.find("}")
            return "".join(lines[:index]) + line[:position]
        depth += delta
    return text


def extract_completion(text, prefix):
    # Qwen reasoning-capable servers may include a visible think block when no
    # reasoning parser is configured.  It is not source code and must not be
    # concatenated into the candidate.
    text = re.sub(r"<think>.*?</think>", "", text, flags=re.DOTALL | re.IGNORECASE)
    blocks = re.findall(r"```[^\n]*\n(.*?)```", text, flags=re.DOTALL)
    if blocks:
        text = max(blocks, key=len)
    text = text.replace("<|endoftext|>", "").replace("<|eot_id|>", "")
    # Some chat wrappers return the complete prompt before the continuation.
    if prefix and text.startswith(prefix):
        text = text[len(prefix) :]
    text = trim_outer_closing_brace(text)
    return text.lstrip("\n") + ("\n" if text.strip() else "")


def compile_candidate(args, candidate, case_dir, include_paths):
    output_json = case_dir / "program.json"
    p4info = case_dir / "program.p4info.txt"
    command = [args.compiler, "--p4v", "16"]
    for include_path in include_paths:
        command.extend(["-I", str(include_path)])
    command.extend(["--p4runtime-files", str(p4info), "-o", str(output_json), str(candidate)])
    try:
        completed = subprocess.run(
            command,
            cwd=str(case_dir),
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=args.compile_timeout,
            check=False,
        )
        (case_dir / "compiler.stdout.txt").write_text(completed.stdout, encoding="utf-8")
        (case_dir / "compiler.stderr.txt").write_text(completed.stderr, encoding="utf-8")
        return {
            "compiled": completed.returncode == 0,
            "compiler_returncode": completed.returncode,
            "compiler_command": command,
            "compiler_stdout": completed.stdout,
            "compiler_stderr": completed.stderr,
        }
    except subprocess.TimeoutExpired as exc:
        stdout = exc.stdout or ""
        stderr = exc.stderr or ""
        if isinstance(stdout, bytes):
            stdout = stdout.decode("utf-8", errors="replace")
        if isinstance(stderr, bytes):
            stderr = stderr.decode("utf-8", errors="replace")
        (case_dir / "compiler.stdout.txt").write_text(stdout, encoding="utf-8")
        (case_dir / "compiler.stderr.txt").write_text(stderr, encoding="utf-8")
        return {
            "compiled": False,
            "compiler_returncode": None,
            "compiler_command": command,
            "compiler_error": "compiler timed out after {} seconds".format(args.compile_timeout),
            "compiler_stdout": stdout,
            "compiler_stderr": stderr,
        }
    except FileNotFoundError:
        return {
            "compiled": False,
            "compiler_returncode": None,
            "compiler_command": command,
            "compiler_error": "compiler not found: {}".format(args.compiler),
            "compiler_stdout": "",
            "compiler_stderr": "",
        }


def prepare_run_dir(args):
    if args.run_dir:
        run_dir = args.run_dir.resolve()
        if run_dir.exists() and not args.resume:
            raise ValueError("run directory exists; pass --resume to reuse it: {}".format(run_dir))
        run_dir.mkdir(parents=True, exist_ok=True)
        return run_dir
    stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    run_dir = args.output_dir.resolve() / ("continuation_{}_{}".format(stamp, os.getpid()))
    run_dir.mkdir(parents=True, exist_ok=False)
    return run_dir


def load_previous_results(path):
    if not path.is_file():
        return {}
    results = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.strip():
            result = json.loads(line)
            results[(result.get("task_id"), result.get("attempt"))] = result
    return results


def main():
    args = parse_args()
    try:
        records = load_eligible_records(args.dataset.resolve())
        run_dir = prepare_run_dir(args)
    except (OSError, ValueError) as exc:
        print("Input error: {}".format(exc), file=sys.stderr)
        return 2
    if args.max_tasks:
        records = records[: args.max_tasks]
    if shutil.which(args.compiler) is None:
        print("Compiler not found: {}".format(args.compiler), file=sys.stderr)
        return 2
    include_paths = [Path(value).resolve() for value in args.include_path]
    include_paths.extend(path.resolve() for path in DEFAULT_INCLUDE_PATHS if path.is_dir())
    # Keep order stable while removing duplicates.
    include_paths = list(dict.fromkeys(include_paths))

    source_datasets = sorted(set(record["source_dataset"] for record in records))
    source_label = safe_name("__".join(source_datasets))
    write_json(
        run_dir / "run.json",
        {
            "task_definition": "local body continuation; prefix + model body + hidden reference tail is compiled",
            "paper_criterion": "p4c-bm2-ss --p4v 16 returncode 0; warnings ignored",
            "dataset": str(args.dataset.resolve()),
            "source_dataset": source_datasets,
            "eligible_record_count": len(records),
            "target_min_lines": args.target_min_lines,
            "target_max_lines": args.target_max_lines,
            "provider": "local",
            "base_url": args.base_url,
            "model": args.model,
            "temperature": args.temperature,
            "max_tokens": args.max_tokens,
            "enable_thinking": args.enable_thinking,
            "compiler": args.compiler,
            "include_paths": [str(path) for path in include_paths],
            "attempts_per_task": args.attempts,
            "artifacts": {
                "summary": "summary.json",
                "results": "results.jsonl",
                "full_question_answer": "qa.jsonl",
            },
            "api_key_written": False,
        },
    )
    results_path = run_dir / "results.jsonl"
    qa_path = run_dir / "qa.jsonl"
    previous = load_previous_results(results_path) if args.resume else {}
    previous_qa = load_jsonl_keys(qa_path) if args.resume else set()
    results = list(previous.values())
    completed_keys = set(previous)
    total = len(records) * args.attempts
    print("Provider: local | Model: {} | Endpoint: {}".format(args.model, args.base_url))
    print("Eligible {} continuation tasks: {} | attempts: {}".format(
        ", ".join(source_datasets), len(records), args.attempts
    ))
    for index, record in enumerate(records, 1):
        task = make_task(record, args.target_min_lines, args.target_max_lines)
        task_id = "{}_continuation_{:04d}_{}".format(
            source_label, index, safe_name(record["record_id"])
        )
        task_include_paths = list(include_paths)
        task_include_paths.extend(record_include_paths(record))
        task_include_paths = list(dict.fromkeys(task_include_paths))
        for attempt in range(1, args.attempts + 1):
            key = (task_id, attempt)
            if key in completed_keys:
                continue
            case_dir = run_dir / "{:04d}_{}_attempt_{:03d}".format(index, safe_name(record["record_id"]), attempt)
            case_dir.mkdir(parents=True, exist_ok=True)
            prompt = build_prompt(task["prefix"], task["target_kind"])
            write_json(
                case_dir / "task.json",
                {
                    "task_id": task_id,
                    "attempt": attempt,
                    "record_id": task["record_id"],
                    "source_dataset": task["source_dataset"],
                    "source_repo": task["source_repo"],
                    "source_path": task["source_path"],
                    "source_sha256": task["source_sha256"],
                    "source_line_count": task["source_line_count"],
                    "prefix_line_count": task["prefix_line_count"],
                    "target_line_count": task["target_line_count"],
                    "suffix_line_count": task["suffix_line_count"],
                    "target_kind": task["target_kind"],
                    "fallback": task["fallback"],
                    "prefix_sha256": hashlib.sha256(task["prefix"].encode("utf-8")).hexdigest(),
                    "reference_target_sha256": hashlib.sha256(task["reference_target"].encode("utf-8")).hexdigest(),
                    "reference_suffix_sha256": hashlib.sha256(task["reference_suffix"].encode("utf-8")).hexdigest(),
                    "include_paths": [str(path) for path in task_include_paths],
                },
            )
            (case_dir / "prefix.p4").write_text(task["prefix"], encoding="utf-8")
            (case_dir / "reference_target.p4").write_text(task["reference_target"], encoding="utf-8")
            (case_dir / "reference_suffix.p4").write_text(task["reference_suffix"], encoding="utf-8")
            (case_dir / "prompt.txt").write_text(prompt, encoding="utf-8")
            result = {
                "task_id": task_id,
                "attempt": attempt,
                "record_id": task["record_id"],
                "source_dataset": task["source_dataset"],
                "source_repo": task["source_repo"],
                "source_path": task["source_path"],
                "source_line_count": task["source_line_count"],
                "prefix_line_count": task["prefix_line_count"],
                "target_line_count": task["target_line_count"],
                "suffix_line_count": task["suffix_line_count"],
                "target_kind": task["target_kind"],
                "fallback": task["fallback"],
                "compiled": False,
                "status": "request_failed",
            }
            prompt_messages = [
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": prompt},
            ]
            qa_record = {
                "task_id": task_id,
                "attempt": attempt,
                "record_id": task["record_id"],
                "source_dataset": task["source_dataset"],
                "source_repo": task["source_repo"],
                "source_path": task["source_path"],
                "source_sha256": task["source_sha256"],
                "target_kind": task["target_kind"],
                "question": prompt_messages,
                "request": None,
                "response": None,
                "answer": None,
                "postprocess": None,
                "compile": None,
                "error": None,
            }
            request_started = time.monotonic()
            request_seconds = None
            compile_seconds = 0.0
            try:
                request_payload = build_request_payload(args, prompt)
                request_payload["messages"] = prompt_messages
                qa_record["request"] = request_payload
                write_json(case_dir / "request.json", request_payload)
                response, content = call_model(args, request_payload)
                request_seconds = time.monotonic() - request_started
                write_json(case_dir / "response.json", response)
                (case_dir / "model_output.txt").write_text(content, encoding="utf-8")
                qa_record["response"] = response
                qa_record["answer"] = {
                    "raw_content": content,
                    "content_chars": len(content),
                }
                completion = extract_completion(content, task["prefix"])
                (case_dir / "model_completion.txt").write_text(completion, encoding="utf-8")
                candidate = task["prefix"] + completion + task["reference_suffix"]
                (case_dir / "candidate.p4").write_text(candidate, encoding="utf-8")
                compile_started = time.monotonic()
                compile_info = compile_candidate(
                    args, case_dir / "candidate.p4", case_dir, task_include_paths
                )
                compile_seconds = time.monotonic() - compile_started
                result.update(compile_info)
                result["status"] = "compiled" if result["compiled"] else "compile_failed"
                result["completion_chars"] = len(completion)
                qa_record["answer"]["compile_content"] = completion
                qa_record["postprocess"] = {
                    "compile_content": completion,
                    "assembled_p4": candidate,
                    "assembled_p4_sha256": hashlib.sha256(candidate.encode("utf-8")).hexdigest(),
                }
                qa_record["compile"] = compile_info
            except RuntimeError as exc:
                result["error"] = str(exc)
                qa_record["error"] = str(exc)
            if request_seconds is None:
                request_seconds = time.monotonic() - request_started
            qa_record["timing"] = {
                "request_seconds": request_seconds,
                "compile_seconds": compile_seconds,
                "total_seconds": time.monotonic() - request_started,
            }
            write_json(case_dir / "result.json", result)
            write_json(case_dir / "qa.json", qa_record)
            results.append(result)
            completed_keys.add(key)
            write_results(results_path, results)
            if key not in previous_qa:
                append_jsonl(qa_path, qa_record)
                previous_qa.add(key)
            progress = "PASS" if result["compiled"] else result["status"].upper()
            print("[{}/{}] {} attempt {}: {}".format(len(results), total, task_id, attempt, progress), flush=True)

    compiled = sum(1 for result in results if result.get("compiled"))
    pass_at_k, pass_at_k_eligible_tasks, pass_at_k_by_task = summarize_pass_at_k(
        results, "task_id", args.attempts
    )
    summary = {
        "total": len(results),
        "compiled": compiled,
        "compile_rate": compiled / len(results) if results else 0.0,
        "request_failures": sum(1 for result in results if result.get("status") == "request_failed"),
        "compile_failures": sum(1 for result in results if result.get("status") == "compile_failed"),
        "definition": "compiled iff prefix + model body + hidden reference tail returns p4c-bm2-ss --p4v 16 code 0; warnings ignored",
        "attempts_per_task": args.attempts,
        "pass_at_k_definition": "mean over tasks of 1 - C(n-c, k) / C(n, k), where n is the number of recorded attempts and c is the number compiled",
        "pass_at_k": pass_at_k,
        "pass_at_k_eligible_tasks": pass_at_k_eligible_tasks,
    }
    write_json(run_dir / "summary.json", summary)
    write_json(run_dir / "pass_at_k_by_task.json", pass_at_k_by_task)
    print("Run directory: {}".format(run_dir))
    print("Compile rate: {}/{} ({:.1%})".format(compiled, len(results), summary["compile_rate"]))
    return 0


if __name__ == "__main__":
    sys.exit(main())
