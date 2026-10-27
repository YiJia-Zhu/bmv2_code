#!/usr/bin/env python3
"""Run BMv2 P4 rollouts directly with vLLM, without an HTTP API.

For every input prompt, vLLM generates ``--attempts`` candidates in one
batched call using ``SamplingParams(n=...)``.  Candidate compilation is then
performed concurrently with a thread pool.  Each raw model completion,
vLLM metadata, cleaned candidate, compiler output, and result is retained for
auditing.  The final summary includes the standard pass@1..pass@N estimate.

This script intentionally uses the vLLM Python engine directly.  It does not
connect to an OpenAI-compatible endpoint and does not require an API key.
"""

import argparse
import csv
import datetime as dt
import json
import math
import os
import re
import shutil
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent
SMOKE_DIR = SCRIPT_DIR.parent
REPO_ROOT = SMOKE_DIR.parent
DEFAULT_PROMPTS = REPO_ROOT / "data" / "tasks" / "bmv2_full_auto_prompts.jsonl"
DEFAULT_MODEL_PATH = Path(
    os.environ.get(
        "QWEN3_14B_MODEL_PATH",
        "/mnt/8T/xgr/zhuyijia/huggingface_models/Qwen3-14B",
    )
)
DEFAULT_LORA_PATH = Path(
    os.environ.get(
        "QWEN3_LORA_PATH",
        str(SMOKE_DIR / "runs" / "training_runs" / "qwen3_14b_p4_pt_3ep"),
    )
)
DEFAULT_LORA_NAME = os.environ.get("QWEN3_LORA_NAME", "qwen3-14b-p4pt-3ep")
DEFAULT_COMPILER = "p4c-bm2-ss"
DEFAULT_OUTPUT_DIR = SMOKE_DIR / "runs" / "benchmark_runs"

SYSTEM_PROMPT = (
    "You are a P4_16 engineer targeting BMv2 simple_switch with v1model. "
    "Return only complete, compilable P4_16 source code. Do not use Markdown "
    "fences or explanatory prose."
)


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--prompts", type=Path, default=DEFAULT_PROMPTS)
    parser.add_argument("--model-path", type=Path, default=DEFAULT_MODEL_PATH)
    parser.add_argument(
        "--lora-path",
        type=Path,
        default=DEFAULT_LORA_PATH,
        help="LoRA adapter directory; use --no-lora for the base model",
    )
    parser.add_argument("--lora-name", default=DEFAULT_LORA_NAME)
    parser.add_argument("--no-lora", action="store_true")
    parser.add_argument("--attempts", type=int, default=10)
    parser.add_argument("--max-tasks", type=int, default=0, help="0 means all prompts")
    parser.add_argument(
        "--prompt-batch-size",
        type=int,
        default=16,
        help="number of prompts submitted to vLLM per generation batch",
    )
    parser.add_argument("--temperature", type=float, default=0.8)
    parser.add_argument("--top-p", type=float, default=1.0)
    parser.add_argument("--top-k", type=int, default=-1)
    parser.add_argument("--max-tokens", type=int, default=10240)
    parser.add_argument("--seed", type=int, default=None)
    parser.add_argument(
        "--enable-thinking",
        action="store_true",
        help="keep Qwen3 visible reasoning enabled; disabled by default",
    )
    parser.add_argument("--tensor-parallel-size", type=int, default=1)
    parser.add_argument("--gpu-memory-utilization", type=float, default=0.90)
    parser.add_argument("--max-model-len", type=int, default=32768)
    parser.add_argument("--dtype", default="auto")
    parser.add_argument("--max-lora-rank", type=int, default=8)
    parser.add_argument("--compiler", default=DEFAULT_COMPILER)
    parser.add_argument("--include-path", action="append", default=[])
    parser.add_argument("--compile-workers", type=int, default=max(1, min(os.cpu_count() or 1, 8)))
    parser.add_argument("--compile-timeout", type=float, default=30.0)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument(
        "--run-dir",
        type=Path,
        default=None,
        help="use this exact run directory; required together with --resume",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="resume an existing run directory and skip recorded case attempts",
    )
    args = parser.parse_args()

    if args.attempts < 1:
        parser.error("--attempts must be at least 1")
    if args.max_tasks < 0:
        parser.error("--max-tasks must be >= 0")
    if args.resume and args.run_dir is None:
        parser.error("--resume requires --run-dir")
    if args.max_tokens < 1 or args.max_model_len < 1:
        parser.error("token limits must be positive")
    if args.tensor_parallel_size < 1 or args.compile_workers < 1 or args.prompt_batch_size < 1:
        parser.error("parallelism values must be positive")
    if args.gpu_memory_utilization <= 0 or args.gpu_memory_utilization > 1:
        parser.error("--gpu-memory-utilization must be in (0, 1]")
    if args.compile_timeout <= 0:
        parser.error("--compile-timeout must be positive")
    if not args.model_path.is_dir():
        parser.error("model directory does not exist: {}".format(args.model_path))
    if not args.no_lora and not args.lora_path.is_dir():
        parser.error("LoRA directory does not exist: {}".format(args.lora_path))
    return args


def safe_name(value):
    cleaned = re.sub(r"[^A-Za-z0-9_.-]+", "_", str(value)).strip("._")
    return (cleaned or "case")[:120]


def load_cases(path, max_tasks):
    if not path.is_file():
        raise ValueError("prompt file does not exist: {}".format(path))
    cases = []
    for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        if not line.strip():
            continue
        try:
            record = json.loads(line)
        except json.JSONDecodeError as exc:
            raise ValueError("{}:{} is not valid JSON: {}".format(path, line_number, exc))
        if not isinstance(record, dict) or not isinstance(record.get("prompt"), str):
            raise ValueError("{}:{} needs a string 'prompt' field".format(path, line_number))
        cases.append(
            {
                "id": str(record.get("id", "case_{:03d}".format(line_number))),
                "prompt": record["prompt"],
            }
        )
        if max_tasks and len(cases) >= max_tasks:
            break
    if not cases:
        raise ValueError("{} contains no prompt records".format(path))
    return cases


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def result_key(result):
    """Return the stable identity of one case attempt."""
    case_id = result.get("case_id")
    attempt = result.get("attempt")
    if case_id is None or attempt is None:
        return None
    try:
        attempt = int(attempt)
    except (TypeError, ValueError):
        return None
    return str(case_id), attempt


def write_results(path, results):
    path.parent.mkdir(parents=True, exist_ok=True)
    ordered = sorted(
        results,
        key=lambda result: (
            str(result.get("case_id")),
            int(result.get("attempt", 0)),
        ),
    )
    with path.open("w", encoding="utf-8") as handle:
        for result in ordered:
            handle.write(json.dumps(result, ensure_ascii=False) + "\n")


def append_result(path, result):
    """Persist one completed attempt before moving to the next candidate."""
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(result, ensure_ascii=False) + "\n")
        handle.flush()


def load_jsonl_results(path):
    """Load valid result records, tolerating a truncated final JSONL line."""
    if not path.is_file():
        return []
    records = []
    with path.open(encoding="utf-8", errors="replace") as handle:
        for line_number, line in enumerate(handle, 1):
            if not line.strip():
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError as exc:
                print(
                    "Warning: ignoring invalid result line {}:{}: {}".format(
                        path, line_number, exc
                    ),
                    file=sys.stderr,
                )
                continue
            if isinstance(record, dict):
                records.append(record)
    return records


def load_json_result(path):
    """Load one per-case checkpoint written as a pretty-printed JSON object."""
    if not path.is_file():
        return []
    try:
        record = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return []
    return [record] if isinstance(record, dict) else []


def load_existing_results(run_dir):
    """Recover aggregate and per-case checkpoints from an interrupted run."""
    records_by_key = {}
    for record in load_jsonl_results(run_dir / "results.jsonl"):
        key = result_key(record)
        if key is not None:
            records_by_key[key] = record
    for path in sorted(run_dir.glob("*/result.json")):
        for record in load_json_result(path):
            key = result_key(record)
            if key is not None:
                records_by_key[key] = record
    return records_by_key


def prepare_run_dir(args):
    if args.run_dir is not None:
        run_dir = args.run_dir.resolve()
        if args.resume:
            if not run_dir.is_dir():
                raise ValueError("run directory does not exist: {}".format(run_dir))
        elif run_dir.exists():
            raise ValueError(
                "run directory exists; pass --resume to reuse it: {}".format(run_dir)
            )
        run_dir.mkdir(parents=True, exist_ok=True)
        return run_dir

    if args.resume:
        raise ValueError("--resume requires --run-dir")
    stamp = dt.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    run_dir = args.output_dir.resolve() / ("vllm_rollout_" + stamp + "_" + str(os.getpid()))
    run_dir.mkdir(parents=True, exist_ok=False)
    return run_dir


def validate_resume_config(run_dir, args, cases):
    """Reject accidental resume with a different prompt set or attempt count."""
    run_path = run_dir / "run.json"
    if not run_path.is_file():
        return
    try:
        previous = json.loads(run_path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise ValueError("cannot read {}: {}".format(run_path, exc))

    mismatches = []
    previous_prompts = previous.get("prompts")
    if previous_prompts:
        previous_prompts = str(Path(previous_prompts).resolve())
        current_prompts = str(args.prompts.resolve())
        if previous_prompts != current_prompts:
            mismatches.append("prompts")
    if previous.get("case_count") is not None and previous["case_count"] != len(cases):
        mismatches.append("case_count")
    if (
        previous.get("attempts_per_case") is not None
        and previous["attempts_per_case"] != args.attempts
    ):
        mismatches.append("attempts_per_case")
    if mismatches:
        raise ValueError(
            "resume configuration differs for: {}".format(", ".join(mismatches))
        )


def build_messages(prompt):
    return [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": prompt},
    ]


def format_chat_prompt(tokenizer, messages, enable_thinking):
    kwargs = {
        "tokenize": False,
        "add_generation_prompt": True,
    }
    if not enable_thinking:
        kwargs["enable_thinking"] = False
    try:
        return tokenizer.apply_chat_template(messages, **kwargs)
    except TypeError:
        # Older tokenizer wrappers may not expose enable_thinking as a keyword.
        kwargs.pop("enable_thinking", None)
        return tokenizer.apply_chat_template(messages, **kwargs)


def extract_p4(text):
    """Remove optional Qwen reasoning/fences while retaining raw text separately."""
    cleaned = re.sub(r"<think>.*?</think>", "", text, flags=re.DOTALL | re.IGNORECASE)
    cleaned = cleaned.replace("<|endoftext|>", "").replace("<|eot_id|>", "")
    blocks = re.findall(r"```[^\n]*\n(.*?)```", cleaned, flags=re.DOTALL)
    if blocks:
        return max(blocks, key=len).strip() + "\n", "markdown_fence"
    return cleaned.strip() + "\n", "raw_response"


def serialize_completion(completion):
    return {
        "index": completion.index,
        "text": completion.text,
        "token_ids": list(completion.token_ids),
        "cumulative_logprob": completion.cumulative_logprob,
        "finish_reason": completion.finish_reason,
        "stop_reason": completion.stop_reason,
    }


def compile_candidate(args, candidate_path, case_dir):
    case_dir = Path(case_dir).resolve()
    candidate_path = Path(candidate_path).resolve()
    output_json = case_dir / "program.json"
    p4info = case_dir / "program.p4info.txt"
    command = [
        args.compiler,
        "--p4v",
        "16",
    ]
    for include_path in args.include_path:
        command.extend(["-I", str(Path(include_path).resolve())])
    command.extend(
        [
            "--p4runtime-files",
            str(p4info),
            "-o",
            str(output_json),
            str(candidate_path),
        ]
    )
    try:
        completed = subprocess.run(
            command,
            cwd=str(case_dir),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=args.compile_timeout,
            check=False,
        )
        stdout = (completed.stdout or b"").decode("utf-8", errors="replace")
        stderr = (completed.stderr or b"").decode("utf-8", errors="replace")
        (case_dir / "compiler.stdout.txt").write_text(stdout, encoding="utf-8")
        (case_dir / "compiler.stderr.txt").write_text(stderr, encoding="utf-8")
        return {
            "compiled": completed.returncode == 0,
            "compiler_returncode": completed.returncode,
            "compiler_command": command,
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
        }
    except FileNotFoundError:
        return {
            "compiled": False,
            "compiler_returncode": None,
            "compiler_command": command,
            "compiler_error": "compiler not found: {}".format(args.compiler),
        }


def estimate_pass_at_k(sample_count, success_count, k):
    if k < 1 or sample_count < k:
        return None
    failure_count = sample_count - success_count
    if failure_count < k:
        return 1.0
    return 1.0 - math.comb(failure_count, k) / math.comb(sample_count, k)


def summarize_pass_at_k(results, max_k):
    grouped = {}
    for result in results:
        grouped.setdefault(result["case_id"], []).append(result)
    per_case = {}
    aggregate = {}
    eligible_cases = {}
    for case_id, case_results in grouped.items():
        sample_count = len(case_results)
        success_count = sum(1 for result in case_results if result.get("compiled"))
        values = {}
        for k in range(1, max_k + 1):
            values[str(k)] = estimate_pass_at_k(sample_count, success_count, k)
        per_case[case_id] = {
            "samples": sample_count,
            "compiled": success_count,
            "pass_at_k": values,
        }
    for k in range(1, max_k + 1):
        values = [
            item["pass_at_k"][str(k)]
            for item in per_case.values()
            if item["pass_at_k"][str(k)] is not None
        ]
        aggregate[str(k)] = sum(values) / len(values) if values else None
        eligible_cases[str(k)] = len(values)
    return aggregate, eligible_cases, per_case


def write_pass_at_k_csv(path, aggregate, per_case, max_k):
    """Write one row per case plus an overall mean row for spreadsheet use."""
    fieldnames = ["case_id", "samples", "compiled"] + [
        "pass@{}".format(k) for k in range(1, max_k + 1)
    ]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for case_id in sorted(per_case):
            case = per_case[case_id]
            row = {
                "case_id": case_id,
                "samples": case["samples"],
                "compiled": case["compiled"],
            }
            for k in range(1, max_k + 1):
                value = case["pass_at_k"].get(str(k))
                row["pass@{}".format(k)] = None if value is None else round(value, 10)
            writer.writerow(row)

        total_samples = sum(case["samples"] for case in per_case.values())
        total_compiled = sum(case["compiled"] for case in per_case.values())
        overall = {
            "case_id": "__overall__",
            "samples": total_samples,
            "compiled": total_compiled,
        }
        for k in range(1, max_k + 1):
            value = aggregate.get(str(k))
            overall["pass@{}".format(k)] = None if value is None else round(value, 10)
        writer.writerow(overall)


def make_failure_result(case, attempt, error):
    return {
        "case_id": case["id"],
        "attempt": attempt,
        "compiled": False,
        "status": "request_failed",
        "error": error,
    }


def compile_one(args, item):
    case = item["case"]
    attempt = item["attempt"]
    case_dir = item["case_dir"]
    try:
        case_dir.mkdir(parents=True, exist_ok=True)
        write_json(
            case_dir / "request.json",
            {
                "engine": "vllm.LLM",
                "model_path": str(args.model_path),
                "lora_path": None if args.no_lora else str(args.lora_path),
                "lora_name": None if args.no_lora else args.lora_name,
                "messages": item["messages"],
                "formatted_prompt": item["formatted_prompt"],
                "sampling_params": item["sampling_params"],
                "attempt": attempt,
            },
        )
        completion = item.get("completion")
        if completion is None:
            result = make_failure_result(case, attempt, item["error"])
            write_json(case_dir / "result.json", result)
            return result

        response = {
            "engine": "vllm.LLM",
            "request_id": item["request_id"],
            "case_id": case["id"],
            "attempt": attempt,
            "prompt": item["formatted_prompt"],
            "completion": serialize_completion(completion),
        }
        write_json(case_dir / "response.json", response)
        raw_text = completion.text
        (case_dir / "model_output.txt").write_text(raw_text, encoding="utf-8")
        candidate, extraction_method = extract_p4(raw_text)
        (case_dir / "candidate.p4").write_text(candidate, encoding="utf-8")
        result = {
            "case_id": case["id"],
            "attempt": attempt,
            "compiled": False,
            "status": "compile_failed",
            "extraction_method": extraction_method,
            "completion_chars": len(raw_text),
            "completion_tokens": len(completion.token_ids),
        }
        result.update(compile_candidate(args, case_dir / "candidate.p4", case_dir))
        result["status"] = "compiled" if result["compiled"] else "compile_failed"
        write_json(case_dir / "result.json", result)
        return result
    except Exception as exc:
        result = {
            "case_id": case["id"],
            "attempt": attempt,
            "compiled": False,
            "status": "worker_failed",
            "error": "{}: {}".format(type(exc).__name__, exc),
        }
        try:
            write_json(case_dir / "result.json", result)
        except OSError:
            pass
        return result


def finalize_run(run_dir, results, attempts):
    results = sorted(
        results,
        key=lambda result: (
            str(result.get("case_id")),
            int(result.get("attempt", 0)),
        ),
    )
    compiled = sum(1 for result in results if result.get("compiled"))
    pass_at_k, eligible_cases, pass_at_k_by_case = summarize_pass_at_k(results, attempts)
    summary = {
        "total": len(results),
        "compiled": compiled,
        "compile_rate": compiled / len(results) if results else 0.0,
        "request_failures": sum(
            1 for result in results if result.get("status") == "request_failed"
        ),
        "compile_failures": sum(
            1
            for result in results
            if result.get("status") in {"compile_failed", "worker_failed"}
        ),
        "attempts_per_case": attempts,
        "pass_at_k_definition": "mean over cases of 1 - C(n-c, k) / C(n, k), where n is recorded attempts and c is compiled attempts",
        "pass_at_k": pass_at_k,
        "pass_at_k_eligible_cases": eligible_cases,
    }
    write_json(run_dir / "summary.json", summary)
    write_json(run_dir / "pass_at_k_by_case.json", pass_at_k_by_case)
    write_pass_at_k_csv(run_dir / "pass_at_k.csv", pass_at_k, pass_at_k_by_case, attempts)
    write_results(run_dir / "results.jsonl", results)
    return summary


def main():
    args = parse_args()
    try:
        cases = load_cases(args.prompts, args.max_tasks)
        run_dir = prepare_run_dir(args)
        if args.resume:
            validate_resume_config(run_dir, args, cases)
    except (OSError, ValueError) as exc:
        print("Input error: {}".format(exc), file=sys.stderr)
        return 2
    if shutil.which(args.compiler) is None:
        print("Compiler not found: {}".format(args.compiler), file=sys.stderr)
        return 2

    run_path = run_dir / "run.json"
    if not run_path.is_file():
        write_json(
            run_path,
            {
                "engine": "vllm.LLM",
                "model_path": str(args.model_path),
                "lora_path": None if args.no_lora else str(args.lora_path),
                "lora_name": None if args.no_lora else args.lora_name,
                "prompts": str(args.prompts.resolve()),
                "case_count": len(cases),
                "attempts_per_case": args.attempts,
                "prompt_batch_size": args.prompt_batch_size,
                "temperature": args.temperature,
                "top_p": args.top_p,
                "top_k": args.top_k,
                "max_tokens": args.max_tokens,
                "seed": args.seed,
                "enable_thinking": args.enable_thinking,
                "tensor_parallel_size": args.tensor_parallel_size,
                "gpu_memory_utilization": args.gpu_memory_utilization,
                "max_model_len": args.max_model_len,
                "dtype": args.dtype,
                "compiler": args.compiler,
                "compile_workers": args.compile_workers,
                "checkpointing": "aggregate results are flushed after every completed candidate",
                "artifacts": {
                    "summary": "summary.json",
                    "results": "results.jsonl",
                    "pass_at_k_by_case": "pass_at_k_by_case.json",
                    "pass_at_k_csv": "pass_at_k.csv",
                    "raw_completion": "<case_dir>/model_output.txt",
                    "vllm_metadata": "<case_dir>/response.json",
                },
            },
        )

    results_path = run_dir / "results.jsonl"
    results_by_key = load_existing_results(run_dir)
    write_results(results_path, list(results_by_key.values()))
    completed_count = len(results_by_key)
    total_count = len(cases) * args.attempts
    pending_cases = []
    for case_index, case in enumerate(cases, 1):
        pending_attempts = [
            attempt
            for attempt in range(1, args.attempts + 1)
            if (case["id"], attempt) not in results_by_key
        ]
        if pending_attempts:
            pending_cases.append((case_index, case, pending_attempts))

    if args.resume:
        print(
            "Recovered {} completed attempts from {}".format(
                completed_count, run_dir
            ),
            flush=True,
        )
    if not pending_cases:
        summary = finalize_run(run_dir, list(results_by_key.values()), args.attempts)
        print("Run directory: {}".format(run_dir))
        print(
            "Compile rate: {}/{} ({:.1%})".format(
                summary["compiled"], summary["total"], summary["compile_rate"]
            )
        )
        print(
            "pass@k: {}".format(
                json.dumps(summary["pass_at_k"], ensure_ascii=False, sort_keys=True)
            )
        )
        return 0

    # Import lazily so --help and static syntax checks do not initialize CUDA.
    try:
        from vllm import LLM, SamplingParams
        from vllm.lora.request import LoRARequest
    except ImportError as exc:
        print("vLLM import failed: {}".format(exc), file=sys.stderr)
        return 2

    print("Loading vLLM model: {}".format(args.model_path), flush=True)
    llm_kwargs = {
        "model": str(args.model_path),
        "tensor_parallel_size": args.tensor_parallel_size,
        "gpu_memory_utilization": args.gpu_memory_utilization,
        "max_model_len": args.max_model_len,
        "dtype": args.dtype,
        "trust_remote_code": True,
        "seed": args.seed,
    }
    lora_request = None
    if not args.no_lora:
        llm_kwargs.update({"enable_lora": True, "max_lora_rank": args.max_lora_rank})
        lora_request = LoRARequest(args.lora_name, 1, str(args.lora_path))
    llm = LLM(**llm_kwargs)
    tokenizer = llm.get_tokenizer()

    formatted_prompts = []
    prompt_metadata = []
    for case_index, case, pending_attempts in pending_cases:
        messages = build_messages(case["prompt"])
        formatted = format_chat_prompt(tokenizer, messages, args.enable_thinking)
        formatted_prompts.append(formatted)
        prompt_metadata.append((case_index, case, messages, formatted, pending_attempts))

    sampling_params = SamplingParams(
        n=args.attempts,
        temperature=args.temperature,
        top_p=args.top_p,
        top_k=args.top_k,
        max_tokens=args.max_tokens,
        seed=args.seed,
    )
    sampling_metadata = {
        "n": args.attempts,
        "temperature": args.temperature,
        "top_p": args.top_p,
        "top_k": args.top_k,
        "max_tokens": args.max_tokens,
        "seed": args.seed,
    }
    print(
        "Generating {} pending cases ({} total attempts) with vLLM (batch size {})...".format(
            len(pending_cases), total_count - completed_count, args.prompt_batch_size
        ),
        flush=True,
    )
    for batch_start in range(0, len(prompt_metadata), args.prompt_batch_size):
        batch_metadata = prompt_metadata[batch_start : batch_start + args.prompt_batch_size]
        batch_prompts = formatted_prompts[batch_start : batch_start + args.prompt_batch_size]
        try:
            outputs = llm.generate(
                batch_prompts,
                sampling_params,
                use_tqdm=True,
                lora_request=lora_request,
            )
        except Exception as exc:
            print("vLLM generation failed for batch {}: {}".format(batch_start, exc), file=sys.stderr)
            outputs = [None] * len(batch_metadata)

        compile_items = []
        for local_index, metadata in enumerate(batch_metadata, batch_start + 1):
            case_index, case, messages, formatted, pending_attempts = metadata
            output = outputs[local_index - batch_start - 1] if local_index - batch_start - 1 < len(outputs) else None
            completions = list(output.outputs) if output is not None else []
            by_index = {completion.index: completion for completion in completions}
            for attempt in pending_attempts:
                case_dir = run_dir / "{:03d}_{}_attempt_{:03d}".format(
                    case_index, safe_name(case["id"]), attempt
                )
                compile_items.append(
                    {
                        "case": case,
                        "attempt": attempt,
                        "case_dir": case_dir,
                        "messages": messages,
                        "formatted_prompt": formatted,
                        "sampling_params": sampling_metadata,
                        "request_id": getattr(output, "request_id", None) if output is not None else None,
                        "completion": by_index.get(attempt - 1),
                        "error": "vLLM generation failed or candidate was not returned",
                    }
                )

        print("Compiling batch with {} workers...".format(args.compile_workers), flush=True)
        with ThreadPoolExecutor(max_workers=args.compile_workers) as executor:
            future_to_item = {
                executor.submit(compile_one, args, item): item for item in compile_items
            }
            for future in as_completed(future_to_item):
                item = future_to_item[future]
                try:
                    result = future.result()
                except Exception as exc:
                    result = {
                        "case_id": item["case"]["id"],
                        "attempt": item["attempt"],
                        "compiled": False,
                        "status": "worker_failed",
                        "error": "{}: {}".format(type(exc).__name__, exc),
                    }
                    try:
                        write_json(item["case_dir"] / "result.json", result)
                    except OSError:
                        pass
                key = result_key(result)
                if key is None:
                    print(
                        "Warning: compiler worker returned an invalid result: {}".format(result),
                        file=sys.stderr,
                    )
                    continue
                if key in results_by_key:
                    continue
                results_by_key[key] = result
                append_result(results_path, result)
                completed_count += 1
                progress = "PASS" if result.get("compiled") else result.get("status", "failed").upper()
                print(
                    "[{}/{}] {} attempt {}: {}".format(
                        completed_count, total_count, result["case_id"], result["attempt"], progress
                    ),
                    flush=True,
                )
        write_results(results_path, list(results_by_key.values()))

    summary = finalize_run(run_dir, list(results_by_key.values()), args.attempts)

    print("Run directory: {}".format(run_dir))
    print(
        "Compile rate: {}/{} ({:.1%})".format(
            summary["compiled"], summary["total"], summary["compile_rate"]
        )
    )
    print(
        "pass@k: {}".format(
            json.dumps(summary["pass_at_k"], ensure_ascii=False, sort_keys=True)
        )
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
