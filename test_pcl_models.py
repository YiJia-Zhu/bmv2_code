#!/usr/bin/env python3
"""List and smoke-test every model exposed by the PCL API.
- 聊天：DeepSeek-V4-Flash-0731、DeepSeek-V4-Pro、GLM-5.2、PCNL本地大模型、Qwen3.6-35B
- 向量：bge-m3
- 重排：bge-reranker
- 语音转写：whisper-1

Configuration is loaded from the .env file next to this script.  The API key
is never printed.  The script only depends on Python's standard library and
curl (curl is used because this project reaches PCL through a SOCKS5 proxy).
"""

import argparse
import json
import math
import os
import re
import shutil
import struct
import subprocess
import sys
import tempfile
import time
import wave
from pathlib import Path


DEFAULT_ENV_FILE = Path(__file__).resolve().with_name(".env")
DEFAULT_PROXY = "socks5h://127.0.0.1:11451"
HTTP_STATUS_MARKER = "\n__PCL_HTTP_STATUS__="


def parse_args():
    parser = argparse.ArgumentParser(
        description="List PCL models and send one small request to each model."
    )
    parser.add_argument(
        "--env-file",
        type=Path,
        default=DEFAULT_ENV_FILE,
        help="dotenv file containing PCL_BASE_URL and PCL_API_KEY",
    )
    parser.add_argument("--base-url", help="override PCL_BASE_URL")
    parser.add_argument(
        "--proxy",
        help="override PCL_PROXY (project default: {})".format(DEFAULT_PROXY),
        default=None,
    )
    parser.add_argument(
        "--direct",
        action="store_true",
        help="connect directly instead of using a proxy",
    )
    parser.add_argument(
        "--model",
        action="append",
        dest="models",
        help="test only this model; may be supplied more than once",
    )
    parser.add_argument(
        "--list-only",
        action="store_true",
        help="only authenticate and list models",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=120.0,
        help="timeout in seconds for each request (default: %(default)s)",
    )
    parser.add_argument(
        "--max-tokens",
        type=int,
        default=32,
        help="maximum output tokens for chat smoke tests (default: %(default)s)",
    )
    parser.add_argument(
        "--show-response",
        action="store_true",
        help="show a short, sanitized response excerpt for successful requests",
    )
    args = parser.parse_args()
    if args.timeout <= 0:
        parser.error("--timeout must be greater than zero")
    if args.max_tokens < 1:
        parser.error("--max-tokens must be at least 1")
    return args


def load_dotenv(path):
    if not path.is_file():
        raise ValueError("环境文件不存在: {}".format(path))

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
                    "{}:{} 的引号没有闭合".format(path, line_number)
                )
            value = value[1:closing_quote]
        else:
            value = re.split(r"\s+#", value, maxsplit=1)[0].strip()
        os.environ.setdefault(key, value)


def sanitize(text, api_key):
    if not text:
        return ""
    sanitized = text.replace(api_key, "<redacted>") if api_key else text
    return re.sub(r"\s+", " ", sanitized).strip()


def run_curl(url, api_key, proxy, timeout, payload=None, extra_args=None):
    command = [
        "curl",
        "--silent",
        "--show-error",
        "--connect-timeout",
        str(min(timeout, 15.0)),
        "--max-time",
        str(timeout),
    ]
    if proxy:
        command.extend(["--proxy", proxy])
    command.extend(
        [
            "--request",
            "POST" if payload is not None or extra_args else "GET",
            "--url",
            url,
            "--header",
            "Authorization: Bearer {}".format(api_key),
            "--header",
            "Accept: application/json",
        ]
    )

    input_bytes = None
    if payload is not None:
        command.extend(
            [
                "--header",
                "Content-Type: application/json",
                "--data-binary",
                "@-",
            ]
        )
        input_bytes = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    if extra_args:
        command.extend(extra_args)
    command.extend(["--write-out", HTTP_STATUS_MARKER + "%{http_code}"])

    started = time.monotonic()
    process = subprocess.run(
        command,
        input=input_bytes,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    elapsed = time.monotonic() - started
    stdout = process.stdout.decode("utf-8", errors="replace")
    stderr = sanitize(
        process.stderr.decode("utf-8", errors="replace"), api_key
    )
    marker_position = stdout.rfind(HTTP_STATUS_MARKER)
    if marker_position == -1:
        return {
            "http_status": 0,
            "body": sanitize(stdout, api_key),
            "stderr": stderr or "curl 未返回 HTTP 状态码",
            "curl_exit": process.returncode,
            "elapsed": elapsed,
        }

    body = stdout[:marker_position]
    status_text = stdout[marker_position + len(HTTP_STATUS_MARKER) :].strip()
    try:
        http_status = int(status_text)
    except ValueError:
        http_status = 0
    return {
        "http_status": http_status,
        "body": sanitize(body, api_key),
        "stderr": stderr,
        "curl_exit": process.returncode,
        "elapsed": elapsed,
    }


def decode_json(result):
    try:
        return json.loads(result["body"])
    except (TypeError, json.JSONDecodeError):
        return None


def response_error(result):
    parsed = decode_json(result)
    if isinstance(parsed, dict):
        error = parsed.get("error")
        if isinstance(error, dict):
            message = error.get("message") or error.get("code")
            if message:
                return str(message)
        if isinstance(error, str):
            return error
        for key in ("message", "msg", "detail"):
            if parsed.get(key):
                return str(parsed[key])
    if result["stderr"]:
        return result["stderr"]
    if result["body"]:
        return result["body"][:240]
    return "没有响应内容"


def model_kind(model_id):
    lowered = model_id.lower()
    if "whisper" in lowered:
        return "语音转写"
    if "rerank" in lowered:
        return "文本重排"
    if "embed" in lowered or lowered.startswith("bge-"):
        return "文本向量"
    return "聊天生成"


def validate_model_response(kind, result):
    if not 200 <= result["http_status"] < 300:
        return False, response_error(result)

    parsed = decode_json(result)
    if not isinstance(parsed, dict):
        return False, "HTTP 成功，但响应不是 JSON 对象"
    if parsed.get("error"):
        return False, response_error(result)

    if kind == "聊天生成":
        valid = isinstance(parsed.get("choices"), list) and bool(parsed["choices"])
        detail = "返回 choices" if valid else "响应缺少 choices"
    elif kind == "文本向量":
        data = parsed.get("data")
        valid = isinstance(data, list) and bool(data)
        detail = "返回 {} 条向量".format(len(data)) if valid else "响应缺少向量 data"
    elif kind == "文本重排":
        results = parsed.get("results", parsed.get("data"))
        valid = isinstance(results, list) and bool(results)
        detail = "返回 {} 条排序结果".format(len(results)) if valid else "响应缺少排序结果"
    else:
        valid = isinstance(parsed.get("text"), str)
        detail = "返回转写文本" if valid else "响应缺少 text"
    return valid, detail


def create_test_wav(path):
    sample_rate = 16000
    sample_count = sample_rate // 2
    frames = b"".join(
        struct.pack(
            "<h", int(5000 * math.sin(2.0 * math.pi * 440.0 * index / sample_rate))
        )
        for index in range(sample_count)
    )
    with wave.open(str(path), "wb") as audio_file:
        audio_file.setnchannels(1)
        audio_file.setsampwidth(2)
        audio_file.setframerate(sample_rate)
        audio_file.writeframes(frames)


def test_model(base_url, api_key, proxy, timeout, max_tokens, model_id):
    kind = model_kind(model_id)
    if kind == "聊天生成":
        result = run_curl(
            base_url + "/chat/completions",
            api_key,
            proxy,
            timeout,
            payload={
                "model": model_id,
                "messages": [{"role": "user", "content": "只回复 OK"}],
                "max_tokens": max_tokens,
                "temperature": 0,
                "stream": False,
            },
        )
    elif kind == "文本向量":
        result = run_curl(
            base_url + "/embeddings",
            api_key,
            proxy,
            timeout,
            payload={"model": model_id, "input": ["这是一个接口测试。"]},
        )
    elif kind == "文本重排":
        result = run_curl(
            base_url + "/rerank",
            api_key,
            proxy,
            timeout,
            payload={
                "model": model_id,
                "query": "中国的首都是哪里？",
                "documents": ["北京是中国的首都。", "苹果是一种水果。"],
                "top_n": 2,
            },
        )
    else:
        with tempfile.TemporaryDirectory(prefix="pcl_model_test_") as temp_dir:
            audio_path = Path(temp_dir) / "test.wav"
            create_test_wav(audio_path)
            result = run_curl(
                base_url + "/audio/transcriptions",
                api_key,
                proxy,
                timeout,
                extra_args=[
                    "--form",
                    "file=@{};type=audio/wav".format(audio_path),
                    "--form",
                    "model={}".format(model_id),
                    "--form",
                    "response_format=json",
                ],
            )

    usable, detail = validate_model_response(kind, result)
    return {
        "model": model_id,
        "kind": kind,
        "usable": usable,
        "http_status": result["http_status"],
        "elapsed": result["elapsed"],
        "detail": detail,
        "body": result["body"],
    }


def response_excerpt(body):
    parsed = None
    try:
        parsed = json.loads(body)
    except (TypeError, json.JSONDecodeError):
        pass
    if isinstance(parsed, dict):
        if isinstance(parsed.get("data"), list) and parsed["data"]:
            first = parsed["data"][0]
            if isinstance(first, dict) and "embedding" in first:
                parsed = dict(parsed)
                parsed["data"] = ["<向量数据已省略>"]
    excerpt = json.dumps(parsed, ensure_ascii=False) if parsed is not None else body
    return re.sub(r"\s+", " ", excerpt).strip()[:500]


def main():
    args = parse_args()
    try:
        load_dotenv(args.env_file.resolve())
    except (OSError, ValueError) as exc:
        print("配置错误: {}".format(exc), file=sys.stderr)
        return 2

    base_url = (args.base_url or os.environ.get("PCL_BASE_URL", "")).rstrip("/")
    api_key = os.environ.get("PCL_API_KEY", "").strip()
    if not base_url:
        print("配置错误: 未设置 PCL_BASE_URL", file=sys.stderr)
        return 2
    if not api_key:
        print("配置错误: 未设置 PCL_API_KEY", file=sys.stderr)
        return 2
    if shutil.which("curl") is None:
        print("依赖错误: 找不到 curl", file=sys.stderr)
        return 2

    if args.direct:
        proxy = None
    elif args.proxy is not None:
        proxy = args.proxy.strip() or None
    else:
        proxy = os.environ.get("PCL_PROXY", DEFAULT_PROXY).strip() or None

    print("PCL 地址: {}".format(base_url))
    print("API Key: 已从 {} 加载（不会显示）".format(args.env_file.resolve()))
    print("连接方式: {}".format("代理 " + proxy if proxy else "直连"))
    print("正在验证密钥并读取模型列表……")

    list_result = run_curl(base_url + "/models", api_key, proxy, args.timeout)
    parsed = decode_json(list_result)
    if not 200 <= list_result["http_status"] < 300:
        print(
            "验证失败: HTTP {} - {}".format(
                list_result["http_status"], response_error(list_result)
            ),
            file=sys.stderr,
        )
        return 2
    if not isinstance(parsed, dict) or not isinstance(parsed.get("data"), list):
        print("验证失败: /models 响应格式异常", file=sys.stderr)
        return 2

    available_models = [
        item.get("id")
        for item in parsed["data"]
        if isinstance(item, dict) and isinstance(item.get("id"), str)
    ]
    print(
        "认证成功: HTTP {}，发现 {} 个模型（{:.2f}s）".format(
            list_result["http_status"], len(available_models), list_result["elapsed"]
        )
    )
    for model_id in available_models:
        print("  - {} [{}]".format(model_id, model_kind(model_id)))

    if args.list_only:
        return 0

    selected_models = args.models or available_models
    unknown_models = [model for model in selected_models if model not in available_models]
    if unknown_models:
        print(
            "警告: 以下模型不在 /models 列表中，仍将尝试: {}".format(
                ", ".join(unknown_models)
            )
        )

    print("\n开始逐个发送最小测试请求：")
    results = []
    for model_id in selected_models:
        result = test_model(
            base_url,
            api_key,
            proxy,
            args.timeout,
            args.max_tokens,
            model_id,
        )
        results.append(result)
        status = "可用" if result["usable"] else "不可用"
        print(
            "  [{}] {} | {} | HTTP {} | {:.2f}s | {}".format(
                status,
                result["model"],
                result["kind"],
                result["http_status"],
                result["elapsed"],
                result["detail"],
            )
        )
        if args.show_response and result["usable"]:
            print("        响应: {}".format(response_excerpt(result["body"])))

    usable_models = [result["model"] for result in results if result["usable"]]
    failed_models = [result["model"] for result in results if not result["usable"]]
    print("\n测试完成: {}/{} 个模型可用".format(len(usable_models), len(results)))
    if usable_models:
        print("可用模型: {}".format(", ".join(usable_models)))
    if failed_models:
        print("不可用模型: {}".format(", ".join(failed_models)))
    return 0 if not failed_models else 1


if __name__ == "__main__":
    sys.exit(main())
