#!/usr/bin/env bash
# Run the SFU P4 continuation compile-through benchmark against the local
# Qwen3-14B vLLM server.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SMOKE_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
REPO_ROOT="$(cd "$SMOKE_DIR/.." && pwd)"
DATASET="${SFU_CONTINUATION_DATASET:-$REPO_ROOT/data/normalized/hf/hf_positive_compile_clean.jsonl}"
BASE_URL="${QWEN3_VLLM_BASE_URL:-http://127.0.0.1:18001/v1}"
API_KEY="${QWEN3_VLLM_API_KEY:-${VLLM_API_KEY:-local-qwen3-14b}}"
MODEL="${QWEN3_MODEL:-qwen3-14b}"
MAX_TASKS="${SFU_MAX_TASKS:-0}"
ATTEMPTS="${SFU_ATTEMPTS:-1}"
TARGET_MIN_LINES="${SFU_TARGET_MIN_LINES:-2}"
TARGET_MAX_LINES="${SFU_TARGET_MAX_LINES:-10}"
MAX_TOKENS="${SFU_MAX_TOKENS:-8192}"
REQUEST_TIMEOUT="${SFU_REQUEST_TIMEOUT:-300}"
COMPILE_TIMEOUT="${SFU_COMPILE_TIMEOUT:-30}"
OUTPUT_DIR="${SFU_OUTPUT_DIR:-$SMOKE_DIR/runs/benchmark_runs}"

if [[ ! -f "$DATASET" ]]; then
    echo "Error: SFU dataset file does not exist: $DATASET" >&2
    exit 2
fi

exec python3 "$SMOKE_DIR/tools/bmv2_p4_continuation_benchmark.py" \
    --dataset "$DATASET" \
    --base-url "$BASE_URL" \
    --api-key "$API_KEY" \
    --model "$MODEL" \
    --max-tasks "$MAX_TASKS" \
    --attempts "$ATTEMPTS" \
    --target-min-lines "$TARGET_MIN_LINES" \
    --target-max-lines "$TARGET_MAX_LINES" \
    --max-tokens "$MAX_TOKENS" \
    --request-timeout "$REQUEST_TIMEOUT" \
    --compile-timeout "$COMPILE_TIMEOUT" \
    --output-dir "$OUTPUT_DIR" \
    "$@"
