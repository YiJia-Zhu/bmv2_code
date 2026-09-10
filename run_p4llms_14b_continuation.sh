#!/usr/bin/env bash
# Run the p4llms-internal continuation test split against local Qwen3-14B.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATASET="${P4LLMS_CONTINUATION_DATASET:-$SCRIPT_DIR/data/finetune/p4llms_p4_code_test.jsonl}"
BASE_URL="${QWEN3_VLLM_BASE_URL:-http://127.0.0.1:18001/v1}"
API_KEY="${QWEN3_VLLM_API_KEY:-${VLLM_API_KEY:-local-qwen3-14b}}"
MODEL="${QWEN3_MODEL:-qwen3-14b}"
MAX_TASKS="${P4LLMS_MAX_TASKS:-0}"
ATTEMPTS="${P4LLMS_ATTEMPTS:-1}"
TARGET_MIN_LINES="${P4LLMS_TARGET_MIN_LINES:-2}"
TARGET_MAX_LINES="${P4LLMS_TARGET_MAX_LINES:-10}"
MAX_TOKENS="${P4LLMS_MAX_TOKENS:-8192}"
REQUEST_TIMEOUT="${P4LLMS_REQUEST_TIMEOUT:-300}"
COMPILE_TIMEOUT="${P4LLMS_COMPILE_TIMEOUT:-30}"
OUTPUT_DIR="${P4LLMS_OUTPUT_DIR:-$SCRIPT_DIR/benchmark_runs}"

if [[ ! -f "$DATASET" ]]; then
    echo "Error: p4llms test split does not exist: $DATASET" >&2
    echo "Run: python3 data/tools/prepare_p4llms_finetune.py" >&2
    exit 2
fi

exec python3 "$SCRIPT_DIR/bmv2_p4_continuation_benchmark.py" \
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
