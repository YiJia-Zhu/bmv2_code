#!/usr/bin/env python3
"""Generate P4 programs with an OpenAI-compatible LLM and compile them for BMv2.

The local provider connects directly to the model exposed from my server.  The
PCL provider reads PCL_BASE_URL/PCL_API_KEY from .env and uses the project's
SOCKS5 tunnel by default.  The DeepSeek provider reads DEEPSEEK_BASE and
DEEPSEEK_KEY (with *_URL/*_API_KEY aliases) and defaults to deepseek-chat. The
OpenAI-compatible provider reads OPENAI_BASE_URL/OPENAI_API_KEY and defaults to
gpt-5.5.

The script intentionally does not start Mininet or simple_switch.  A successful
``p4c-bm2-ss`` exit code means the generated P4 source compiles for the BMv2
simple_switch target; it does not prove the resulting data plane is correct.
"""

import argparse
import datetime as dt
import json
import math
import os
import re
import shutil
import subprocess
import sys
import urllib.error
import urllib.request
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_ENV_FILE = SCRIPT_DIR / ".env"
LOCAL_DEFAULT_BASE_URL = "http://127.0.0.1:18001/v1"
LOCAL_DEFAULT_API_KEY = "local-qwen3-14b"
LOCAL_DEFAULT_MODEL = "qwen3-14b-p4pt-3ep"
DEEPSEEK_DEFAULT_BASE_URL = "https://api.deepseek.com/v1"
DEEPSEEK_DEFAULT_MODEL = "deepseek-chat"
OPENAI_DEFAULT_BASE_URL = "https://api.vveai.com/v1"
OPENAI_DEFAULT_MODEL = "gpt-5.5"
PCL_DEFAULT_PROXY = "socks5h://127.0.0.1:11451"
PCL_DEFAULT_MODEL = "GLM-5.2"
PCL_CHAT_MODELS = (
    "DeepSeek-V4-Flash-0731",
    "DeepSeek-V4-Pro",
    "GLM-5.2",
    "PCNL本地大模型",
    "Qwen3.6-35B",
)
CURL_HTTP_STATUS_MARKER = b"\n__BMV2_HTTP_STATUS__="
SYSTEM_PROMPT = (
    "You are a P4_16 engineer. Return only complete, compilable P4_16 source "
    "code. Do not use Markdown fences or explanatory prose."
)
DEFAULT_PROMPT = """Write a complete P4_16 program for the BMv2 v1model target.
Implement IPv4 LPM forwarding: parse Ethernet and IPv4, use a table keyed by
IPv4 destination address with lpm matching, set the egress port and next-hop
destination MAC address, decrement IPv4 TTL, recompute the IPv4 checksum, and
emit Ethernet followed by IPv4. It must compile with:
p4c-bm2-ss --p4v 16
"""


def load_dotenv(path):
    if not path.is_file():
        return

    for line_number, original_line in enumerate(
        path.read_text(encoding="utf-8").splitlines(), 1
    ):
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
            closing_quote = value.find(quote, 1)
            if closing_quote == -1:
                raise ValueError(
                    "{}:{} has an unclosed quote".format(path, line_number)
                )
            value = value[1:closing_quote]
        else:
            value = re.split(r"\s+#", value, maxsplit=1)[0].strip()
        os.environ.setdefault(key, value)


def parse_args():
    env_parser = argparse.ArgumentParser(add_help=False)
    env_parser.add_argument("--env-file", type=Path, default=DEFAULT_ENV_FILE)
    env_args, _ = env_parser.parse_known_args()
    try:
        load_dotenv(env_args.env_file.resolve())
    except (OSError, ValueError) as exc:
        env_parser.error(str(exc))

    parser = argparse.ArgumentParser(
        description="Call an OpenAI-compatible LLM and measure BMv2 P4 compile rate.",
        epilog="PCL chat models currently include: {}; DeepSeek defaults to deepseek-chat".format(
            ", ".join(PCL_CHAT_MODELS)
        ),
    )
    parser.add_argument(
        "--env-file",
        type=Path,
        default=env_args.env_file,
        help="dotenv file used for provider configuration (default: %(default)s)",
    )
    parser.add_argument(
        "--provider",
        choices=("local", "pcl", "deepseek", "openai"),
        default=os.environ.get("BMV2_LLM_PROVIDER", "local"),
        help="local: my-server; pcl: PCL; deepseek: api.deepseek.com; openai: OPENAI_BASE_URL (default: %(default)s)",
    )
    parser.add_argument(
        "--base-url",
        help="override the provider's OpenAI-compatible base URL",
    )
    parser.add_argument(
        "--api-key",
        help="override the provider API key; prefer storing it in --env-file",
    )
    parser.add_argument(
        "--model",
        help="model name; defaults to QWEN3_MODEL, PCL_MODEL, deepseek-chat, or gpt-5.5",
    )
    proxy_group = parser.add_mutually_exclusive_group()
    proxy_group.add_argument(
        "--proxy",
        help="HTTP/SOCKS proxy; PCL defaults to PCL_PROXY or {}".format(
            PCL_DEFAULT_PROXY
        ),
    )
    proxy_group.add_argument(
        "--direct",
        action="store_true",
        help="disable the provider's default proxy",
    )
    parser.add_argument(
        "--prompts",
        type=Path,
        help="JSONL file with {'id': ..., 'prompt': ...} records. Defaults to one basic P4 prompt.",
    )
    parser.add_argument(
        "--attempts",
        type=int,
        default=1,
        help="Generations per prompt (default: %(default)s)",
    )
    parser.add_argument(
        "--temperature",
        type=float,
        default=0.8,
        help="sampling temperature (default: %(default)s; use 0 for deterministic output)",
    )
    parser.add_argument("--max-tokens", type=int, default=4096)
    parser.add_argument(
        "--compiler", default="p4c-bm2-ss", help="P4 compiler executable (default: %(default)s)"
    )
    parser.add_argument(
        "--compile-timeout",
        type=float,
        default=30.0,
        help="Per-candidate compiler time limit in seconds (default: %(default)s)",
    )
    parser.add_argument(
        "--request-timeout",
        type=float,
        default=300.0,
        help="Per-request API time limit in seconds (default: %(default)s)",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("benchmark_runs"),
        help="Parent directory for auditable run results (default: %(default)s)",
    )
    args = parser.parse_args()
    if args.attempts < 1:
        parser.error("--attempts must be at least 1")
    if args.max_tokens < 1:
        parser.error("--max-tokens must be at least 1")
    if args.compile_timeout <= 0 or args.request_timeout <= 0:
        parser.error("timeouts must be greater than zero")

    if args.provider == "local":
        args.base_url = (
            args.base_url
            or os.environ.get("QWEN3_VLLM_BASE_URL")
            or LOCAL_DEFAULT_BASE_URL
        )
        args.api_key = (
            args.api_key
            or os.environ.get("QWEN3_VLLM_API_KEY")
            or os.environ.get("VLLM_API_KEY")
            or LOCAL_DEFAULT_API_KEY
        )
        args.model = args.model or os.environ.get("QWEN3_MODEL") or LOCAL_DEFAULT_MODEL
        provider_proxy = None
    elif args.provider == "pcl":
        args.base_url = args.base_url or os.environ.get("PCL_BASE_URL", "")
        args.api_key = args.api_key or os.environ.get("PCL_API_KEY", "")
        args.model = args.model or os.environ.get("PCL_MODEL") or PCL_DEFAULT_MODEL
        if "PCL_PROXY" in os.environ:
            provider_proxy = os.environ["PCL_PROXY"].strip() or None
        else:
            provider_proxy = PCL_DEFAULT_PROXY
        lowered_model = args.model.lower()
        if any(token in lowered_model for token in ("bge-", "rerank", "whisper")):
            parser.error(
                "model {!r} is not a chat model and cannot generate P4 source".format(
                    args.model
                )
            )
    elif args.provider == "deepseek":
        args.base_url = (
            args.base_url
            or os.environ.get("DEEPSEEK_BASE")
            or os.environ.get("DEEPSEEK_BASE_URL")
            or DEEPSEEK_DEFAULT_BASE_URL
        )
        args.api_key = (
            args.api_key
            or os.environ.get("DEEPSEEK_KEY")
            or os.environ.get("DEEPSEEK_API_KEY")
            or ""
        )
        args.model = args.model or os.environ.get("DEEPSEEK_MODEL") or DEEPSEEK_DEFAULT_MODEL
        provider_proxy = os.environ.get("DEEPSEEK_PROXY", "").strip() or None
    else:
        args.base_url = (
            args.base_url
            or os.environ.get("OPENAI_BASE_URL")
            or OPENAI_DEFAULT_BASE_URL
        )
        args.api_key = (
            args.api_key
            or os.environ.get("OPENAI_API_KEY")
            or ""
        )
        args.model = (
            args.model
            or os.environ.get("OPENAI_MODEL")
            or os.environ.get("LLM_MODEL")
            or OPENAI_DEFAULT_MODEL
        )
        provider_proxy = os.environ.get("OPENAI_PROXY", "").strip() or None

    if not args.base_url:
        parser.error("{} provider base URL is missing".format(args.provider))
    if not args.api_key:
        parser.error("{} provider API key is missing".format(args.provider))
    args.base_url = args.base_url.rstrip("/")
    args.proxy = None if args.direct else (args.proxy or provider_proxy)
    args.transport = "curl" if args.proxy or args.provider == "pcl" else "urllib"
    return args


def safe_name(value):
    value = re.sub(r"[^A-Za-z0-9_.-]+", "_", str(value)).strip("._")
    return value or "case"


def load_cases(prompt_file):
    if prompt_file is None:
        return [{"id": "basic_ipv4_forwarding", "prompt": DEFAULT_PROMPT}]

    cases = []
    for line_number, line in enumerate(prompt_file.read_text(encoding="utf-8").splitlines(), 1):
        if not line.strip():
            continue
        try:
            record = json.loads(line)
        except json.JSONDecodeError as exc:
            raise ValueError("{}:{} is not valid JSON: {}".format(prompt_file, line_number, exc))
        if not isinstance(record, dict) or not isinstance(record.get("prompt"), str):
            raise ValueError("{}:{} needs a string 'prompt' field".format(prompt_file, line_number))
        cases.append(
            {
                "id": str(record.get("id", "case_{:03d}".format(line_number))),
                "prompt": record["prompt"],
            }
        )
    if not cases:
        raise ValueError("{} contains no prompt records".format(prompt_file))
    return cases


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def redacted(text, api_key):
    if not text:
        return ""
    return text.replace(api_key, "<redacted>") if api_key else text


def request_with_urllib(args, url, body):
    request = urllib.request.Request(
        url,
        data=body,
        headers={
            "Authorization": "Bearer " + args.api_key,
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=args.request_timeout) as response:
            return response.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(
            "API returned HTTP {}: {}".format(
                exc.code, redacted(detail, args.api_key)
            )
        )
    except urllib.error.URLError as exc:
        raise RuntimeError("API request failed: {}".format(exc.reason))
    except TimeoutError:
        raise RuntimeError(
            "API request timed out after {} seconds".format(args.request_timeout)
        )


def request_with_curl(args, url, body):
    command = [
        "curl",
        "--silent",
        "--show-error",
        "--connect-timeout",
        str(min(args.request_timeout, 15.0)),
        "--max-time",
        str(args.request_timeout),
    ]
    if args.proxy:
        command.extend(["--proxy", args.proxy])
    else:
        command.extend(["--noproxy", "*"])
    command.extend(
        [
            "--request",
            "POST",
            "--url",
            url,
            "--header",
            "Authorization: Bearer " + args.api_key,
            "--header",
            "Content-Type: application/json",
            "--data-binary",
            "@-",
            "--write-out",
            CURL_HTTP_STATUS_MARKER.decode("ascii") + "%{http_code}",
        ]
    )
    try:
        completed = subprocess.run(
            command,
            input=body,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=args.request_timeout + 5.0,
            check=False,
        )
    except subprocess.TimeoutExpired:
        raise RuntimeError(
            "API request timed out after {} seconds".format(args.request_timeout)
        )

    stdout = completed.stdout
    stderr = redacted(
        completed.stderr.decode("utf-8", errors="replace").strip(), args.api_key
    )
    marker_position = stdout.rfind(CURL_HTTP_STATUS_MARKER)
    if marker_position == -1:
        detail = stderr or redacted(
            stdout.decode("utf-8", errors="replace"), args.api_key
        )
        raise RuntimeError("API request failed: {}".format(detail or "no response"))

    response_body = stdout[:marker_position].decode("utf-8", errors="replace")
    status_text = stdout[
        marker_position + len(CURL_HTTP_STATUS_MARKER) :
    ].decode("ascii", errors="replace")
    try:
        http_status = int(status_text)
    except ValueError:
        http_status = 0
    if completed.returncode != 0:
        raise RuntimeError(
            "API request failed (curl exit {}): {}".format(
                completed.returncode, stderr or "HTTP {}".format(http_status)
            )
        )
    if not 200 <= http_status < 300:
        raise RuntimeError(
            "API returned HTTP {}: {}".format(
                http_status, redacted(response_body, args.api_key)
            )
        )
    return response_body


def call_model(args, prompt):
    request_payload = {
        "model": args.model,
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": prompt},
        ],
        "temperature": args.temperature,
        "max_tokens": args.max_tokens,
        "stream": False,
    }
    body = json.dumps(request_payload).encode("utf-8")
    url = args.base_url + "/chat/completions"
    if args.transport == "curl":
        response_body = request_with_curl(args, url, body)
    else:
        response_body = request_with_urllib(args, url, body)

    try:
        parsed = json.loads(response_body)
        message = parsed["choices"][0]["message"]
        content = message["content"]
    except (KeyError, IndexError, TypeError, json.JSONDecodeError) as exc:
        raise RuntimeError("API response did not contain choices[0].message.content: {}".format(exc))
    if not isinstance(content, str) or not content.strip():
        raise RuntimeError("API returned an empty message.content")
    return request_payload, parsed, content


def extract_p4(text):
    """Use the largest fenced block when a model ignores the no-Markdown request."""
    blocks = re.findall(r"```[^\n]*\n(.*?)```", text, flags=re.DOTALL)
    if blocks:
        return max(blocks, key=len).strip() + "\n", "markdown_fence"
    return text.strip() + "\n", "raw_response"


def compile_p4(args, candidate, case_dir):
    # ``case_dir`` may be relative when ``--output-dir`` is relative.  The
    # compiler runs with ``cwd=case_dir``; passing a relative candidate path in
    # that situation makes p4c look for ``case_dir/case_dir/candidate.p4``.
    # Resolve all compiler-facing paths before changing cwd.
    case_dir = Path(case_dir).resolve()
    candidate = Path(candidate).resolve()
    output_json = case_dir / "program.json"
    p4info = case_dir / "program.p4info.txt"
    command = [
        args.compiler,
        "--p4v",
        "16",
        "--p4runtime-files",
        str(p4info),
        "-o",
        str(output_json),
        str(candidate),
    ]
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
        }


    except subprocess.TimeoutExpired as exc:
        (case_dir / "compiler.stdout.txt").write_text(exc.stdout or "", encoding="utf-8")
        (case_dir / "compiler.stderr.txt").write_text(exc.stderr or "", encoding="utf-8")
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
    """Return aggregate and per-case pass@k values for grouped attempts."""
    grouped = {}
    for result in results:
        grouped.setdefault(result.get(group_field), []).append(result)

    aggregate = {}
    eligible_cases = {}
    per_case = {}
    for group_id, group_results in grouped.items():
        sample_count = len(group_results)
        success_count = sum(1 for result in group_results if result.get("compiled"))
        per_case[str(group_id)] = {
            "samples": sample_count,
            "compiled": success_count,
            "pass_at_k": {},
        }
        for k in range(1, max_k + 1):
            estimate = estimate_pass_at_k(sample_count, success_count, k)
            per_case[str(group_id)]["pass_at_k"][str(k)] = estimate

    for k in range(1, max_k + 1):
        estimates = [
            case["pass_at_k"][str(k)]
            for case in per_case.values()
            if case["pass_at_k"][str(k)] is not None
        ]
        aggregate[str(k)] = sum(estimates) / len(estimates) if estimates else None
        eligible_cases[str(k)] = len(estimates)

    return aggregate, eligible_cases, per_case


def main():
    args = parse_args()
    try:
        cases = load_cases(args.prompts)
    except (OSError, ValueError) as exc:
        print("Input error: {}".format(exc), file=sys.stderr)
        return 2

    if shutil.which(args.compiler) is None:
        print("Compiler not found: {}".format(args.compiler), file=sys.stderr)
        return 2
    if args.transport == "curl" and shutil.which("curl") is None:
        print("curl is required for PCL/SOCKS requests", file=sys.stderr)
        return 2

    run_stamp = dt.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    run_dir = args.output_dir / (run_stamp + "_" + str(os.getpid()))
    run_dir.mkdir(parents=True, exist_ok=False)
    write_json(
        run_dir / "run.json",
        {
            "provider": args.provider,
            "base_url": args.base_url,
            "model": args.model,
            "transport": args.transport,
            "proxy_enabled": bool(args.proxy),
            "temperature": args.temperature,
            "max_tokens": args.max_tokens,
            "compiler": args.compiler,
            "compile_timeout": args.compile_timeout,
            "request_timeout": args.request_timeout,
            "case_count": len(cases),
            "attempts_per_case": args.attempts,
            "api_key_written": False,
        },
    )

    print(
        "Provider: {} | Model: {} | Transport: {}{}".format(
            args.provider,
            args.model,
            args.transport,
            " via proxy" if args.proxy else " direct",
        )
    )

    results = []
    total = len(cases) * args.attempts
    for case_index, case in enumerate(cases, 1):
        for attempt in range(1, args.attempts + 1):
            case_dir = run_dir / "{:03d}_{}_attempt_{:03d}".format(
                case_index, safe_name(case["id"]), attempt
            )
            case_dir.mkdir()
            result = {
                "case_id": case["id"],
                "attempt": attempt,
                "compiled": False,
                "status": "request_failed",
            }
            try:
                request_payload, response, content = call_model(args, case["prompt"])
                write_json(case_dir / "request.json", request_payload)
                write_json(case_dir / "response.json", response)
                (case_dir / "model_output.txt").write_text(content, encoding="utf-8")
                extracted, extraction_method = extract_p4(content)
                (case_dir / "candidate.p4").write_text(extracted, encoding="utf-8")
                result.update(compile_p4(args, case_dir / "candidate.p4", case_dir))
                result["extraction_method"] = extraction_method
                result["status"] = "compiled" if result["compiled"] else "compile_failed"
            except RuntimeError as exc:
                result["error"] = str(exc)
            write_json(case_dir / "result.json", result)
            results.append(result)
            progress = "PASS" if result["compiled"] else result["status"].upper()
            print("[{}/{}] {} attempt {}: {}".format(total and len(results), total, case["id"], attempt, progress))

    compiled = sum(1 for result in results if result["compiled"])
    pass_at_k, pass_at_k_eligible_cases, pass_at_k_by_case = summarize_pass_at_k(
        results, "case_id", args.attempts
    )
    summary = {
        "total": len(results),
        "compiled": compiled,
        "compile_rate": compiled / len(results) if results else 0.0,
        "request_failures": sum(1 for result in results if result["status"] == "request_failed"),
        "compile_failures": sum(1 for result in results if result["status"] == "compile_failed"),
        "attempts_per_case": args.attempts,
        "pass_at_k_definition": "mean over cases of 1 - C(n-c, k) / C(n, k), where n is the number of recorded attempts and c is the number compiled",
        "pass_at_k": pass_at_k,
        "pass_at_k_eligible_cases": pass_at_k_eligible_cases,
    }
    write_json(run_dir / "summary.json", summary)
    write_json(run_dir / "pass_at_k_by_case.json", pass_at_k_by_case)
    with (run_dir / "results.jsonl").open("w", encoding="utf-8") as handle:
        for result in results:
            handle.write(json.dumps(result, ensure_ascii=False) + "\n")

    print("Run directory: {}".format(run_dir))
    print(
        "Compile rate: {}/{} ({:.1%})".format(
            summary["compiled"], summary["total"], summary["compile_rate"]
        )
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
