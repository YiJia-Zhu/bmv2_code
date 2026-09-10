#!/usr/bin/env bash
# Start a local OpenAI-compatible vLLM server for Qwen3-14B.
#
# The defaults match bmv2_llm_compile_benchmark.py's local provider:
#   http://127.0.0.1:18001/v1
#   model name: qwen3-14b

set -euo pipefail

MODEL_PATH="${QWEN3_14B_MODEL_PATH:-/mnt/8T/xgr/zhuyijia/huggingface_models/Qwen3-14B}"
# LORA_PATH="${QWEN3_LORA_PATH:-}"
LORA_PATH="${QWEN3_LORA_PATH:-/mnt/8T/xgr/zhuyijia/bmv2_code/training_runs/qwen3_14b_p4_pt_3ep}"


# LORA_NAME="${QWEN3_LORA_NAME:-qwen3-14b-p4pt}"
LORA_NAME="${QWEN3_LORA_NAME:-qwen3-14b-p4pt-3ep}"



# A LoRA name must not also be the base model's served name.  vLLM treats a
# request matching a base served alias as the base model and consequently
# passes lora_request=None.  Use a distinct base alias automatically when an
# adapter is configured, while still allowing an explicit override.
if [[ -n "${QWEN3_SERVED_MODEL_NAME:-}" ]]; then
    SERVED_MODEL_NAME="$QWEN3_SERVED_MODEL_NAME"
elif [[ -n "$LORA_PATH" ]]; then
    SERVED_MODEL_NAME="qwen3-14b-base"
else
    SERVED_MODEL_NAME="qwen3-14b"
fi

if [[ -n "${VLLM_BIN:-}" ]]; then
    VLLM_BIN="$VLLM_BIN"
elif [[ -x /mnt/8T/xgr/zhuyijia/envs/effthink/bin/vllm ]]; then
    VLLM_BIN="/mnt/8T/xgr/zhuyijia/envs/effthink/bin/vllm"
else
    VLLM_BIN="vllm"
fi

VLLM_HOST="${VLLM_HOST:-127.0.0.1}"
# 18001 is the default endpoint used by bmv2_llm_compile_benchmark.py.
VLLM_PORT="${VLLM_PORT:-18001}"
VLLM_API_KEY="${QWEN3_VLLM_API_KEY:-${VLLM_API_KEY:-local-qwen3-14b}}"
VLLM_TENSOR_PARALLEL_SIZE="${VLLM_TENSOR_PARALLEL_SIZE:-1}"
VLLM_GPU_MEMORY_UTILIZATION="${VLLM_GPU_MEMORY_UTILIZATION:-0.90}"
VLLM_MAX_MODEL_LEN="${VLLM_MAX_MODEL_LEN:-32768}"
VLLM_DTYPE="${VLLM_DTYPE:-auto}"
VLLM_MAX_LORA_RANK="${VLLM_MAX_LORA_RANK:-8}"

LORA_ARGS=()
BENCHMARK_MODEL_NAME="$SERVED_MODEL_NAME"
if [[ -n "$LORA_PATH" ]]; then
    if [[ ! -d "$LORA_PATH" ]]; then
        echo "Error: LoRA adapter directory does not exist: $LORA_PATH" >&2
        exit 2
    fi
    if [[ "$SERVED_MODEL_NAME" == "$LORA_NAME" ]]; then
        echo "Error: base served name and LoRA name must differ: $SERVED_MODEL_NAME" >&2
        echo "Set QWEN3_SERVED_MODEL_NAME to a base alias such as qwen3-14b-base." >&2
        exit 2
    fi
    LORA_ARGS=(
        --enable-lora
        --max-lora-rank "$VLLM_MAX_LORA_RANK"
        --lora-modules "${LORA_NAME}=${LORA_PATH}"
    )
    BENCHMARK_MODEL_NAME="$LORA_NAME"
fi

if [[ ! -d "$MODEL_PATH" ]]; then
    echo "Error: Qwen3-14B model directory does not exist: $MODEL_PATH" >&2
    exit 2
fi

if ! command -v "$VLLM_BIN" >/dev/null 2>&1; then
    echo "Error: vLLM executable not found: $VLLM_BIN" >&2
    echo "Activate the environment that provides vLLM or set VLLM_BIN=/path/to/vllm." >&2
    exit 127
fi

cat <<EOF
Starting Qwen3-14B vLLM server
  model path:              $MODEL_PATH
  served name:             $SERVED_MODEL_NAME
  LoRA adapter:             ${LORA_NAME} (${LORA_PATH:-disabled})
  endpoint:                http://$VLLM_HOST:$VLLM_PORT/v1
  tensor parallel size:    $VLLM_TENSOR_PARALLEL_SIZE
  GPU memory utilization:  $VLLM_GPU_MEMORY_UTILIZATION
  max model length:        $VLLM_MAX_MODEL_LEN

Use it with the BMv2 benchmark:
  QWEN3_VLLM_BASE_URL=http://$VLLM_HOST:$VLLM_PORT/v1 \
  ./bmv2_llm_compile_benchmark.py --provider local --model $BENCHMARK_MODEL_NAME

If you changed the API key, export the same value as QWEN3_VLLM_API_KEY for the benchmark.
EOF

exec "$VLLM_BIN" serve "$MODEL_PATH" \
    --served-model-name "$SERVED_MODEL_NAME" \
    --host "$VLLM_HOST" \
    --port "$VLLM_PORT" \
    --api-key "$VLLM_API_KEY" \
    --tensor-parallel-size "$VLLM_TENSOR_PARALLEL_SIZE" \
    --gpu-memory-utilization "$VLLM_GPU_MEMORY_UTILIZATION" \
    --max-model-len "$VLLM_MAX_MODEL_LEN" \
    --dtype "$VLLM_DTYPE" \
    --trust-remote-code \
    "${LORA_ARGS[@]}"
