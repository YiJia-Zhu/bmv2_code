#!/usr/bin/env bash
# Staged p4llms -> Qwen3-14B training and BMv2 evaluation pipeline.
#
# The stages are deliberately explicit because a 14B training job and a vLLM
# server cannot safely occupy the same H100 at the same time:
#
#   prepare  : build repository-level p4llms train/dev/test and SFT windows
#   baseline : evaluate the currently running base-model vLLM server
#   train    : run continued pretraining (PT) followed by continuation SFT
#   test     : evaluate a vLLM server serving the trained LoRA adapter
#   status   : print the expected artifacts and commands
#
# Typical flow:
#   ./run_p4llms_qwen3_pipeline.sh prepare
#   ./run_p4llms_qwen3_pipeline.sh baseline
#   # stop the base vLLM server before continuing
#   ./run_p4llms_qwen3_pipeline.sh train
#   # start vLLM with the resulting LoRA adapter
#   ./run_p4llms_qwen3_pipeline.sh test

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHON_BIN="${PYTHON_BIN:-/mnt/8T/xgr/zhuyijia/envs/effthink/bin/python}"
LLAMAFACTORY_BIN="${LLAMAFACTORY_BIN:-/mnt/8T/xgr/zhuyijia/envs/effthink/bin/llamafactory-cli}"

INPUT_DATASET="${P4LLMS_INPUT_DATASET:-$SCRIPT_DIR/data/normalized/hf_p4llms/hf_p4llms_bmv2_eligible.jsonl}"
DATA_ROOT="${P4LLMS_DATA_ROOT:-$SCRIPT_DIR/data/finetune}"
PT_CONFIG="${P4LLMS_PT_CONFIG:-$SCRIPT_DIR/training/qwen3_14b_p4_pt_10k.yaml}"
SFT_CONFIG="${P4LLMS_SFT_CONFIG:-$SCRIPT_DIR/training/qwen3_14b_p4_sft_10k.yaml}"
PT_OUTPUT="${P4LLMS_PT_OUTPUT:-$SCRIPT_DIR/training_runs/qwen3_14b_p4_pt_10k}"
SFT_OUTPUT="${P4LLMS_SFT_OUTPUT:-$SCRIPT_DIR/training_runs/qwen3_14b_p4_pt_sft_10k}"

BASE_URL="${QWEN3_VLLM_BASE_URL:-http://127.0.0.1:18001/v1}"
API_KEY="${QWEN3_VLLM_API_KEY:-${VLLM_API_KEY:-local-qwen3-14b}}"
BASE_MODEL_NAME="${P4LLMS_BASE_MODEL_NAME:-qwen3-14b}"
TEST_MODEL_NAME="${P4LLMS_TEST_MODEL_NAME:-qwen3-14b-p4}"
OUTPUT_ROOT="${P4LLMS_OUTPUT_DIR:-$SCRIPT_DIR/benchmark_runs}"
MAX_WINDOWS="${P4LLMS_MAX_WINDOWS_PER_FILE:-8}"

die() {
    echo "Error: $*" >&2
    exit 2
}

require_file() {
    [[ -f "$1" ]] || die "file not found: $1"
}

require_executable() {
    [[ -x "$1" ]] || die "executable not found: $1"
}

latest_adapter() {
    local output_dir="$1"
    local checkpoint=""
    if [[ -f "$output_dir/adapter_config.json" ]]; then
        printf '%s\n' "$output_dir"
        return 0
    fi
    checkpoint="$(find "$output_dir" -maxdepth 1 -type d -name 'checkpoint-*' 2>/dev/null | sort -V | tail -n 1)"
    [[ -n "$checkpoint" ]] || return 1
    [[ -f "$checkpoint/adapter_config.json" ]] || return 1
    printf '%s\n' "$checkpoint"
}

local_port_open() {
    "$PYTHON_BIN" - "$1" <<'PY'
import socket
import sys

port = int(sys.argv[1])
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.settimeout(0.5)
try:
    sock.connect(("127.0.0.1", port))
except OSError:
    sys.exit(1)
finally:
    sock.close()
PY
}

ensure_no_local_vllm() {
    local port="${BASE_URL##*:}"
    port="${port%%/*}"
    if local_port_open "$port" && [[ "${P4LLMS_ALLOW_ACTIVE_VLLM:-0}" != "1" ]]; then
        die "local vLLM appears active on port $port; stop it before 14B training, or set P4LLMS_ALLOW_ACTIVE_VLLM=1"
    fi
}

prepare_data() {
    require_executable "$PYTHON_BIN"
    require_file "$INPUT_DATASET"
    "$PYTHON_BIN" "$SCRIPT_DIR/data/tools/prepare_p4llms_finetune.py" \
        --input "$INPUT_DATASET" \
        --output-root "$DATA_ROOT" \
        --min-target-lines 2 \
        --max-target-lines 10 \
        --max-windows-per-file "$MAX_WINDOWS"
}

run_benchmark() {
    local model_name="$1"
    local run_name="$2"
    local dataset="$DATA_ROOT/p4llms_p4_code_test.jsonl"
    require_file "$dataset"
    QWEN3_MODEL="$model_name" \
    QWEN3_VLLM_BASE_URL="$BASE_URL" \
    QWEN3_VLLM_API_KEY="$API_KEY" \
    P4LLMS_OUTPUT_DIR="$OUTPUT_ROOT/$run_name" \
        "$SCRIPT_DIR/run_p4llms_14b_continuation.sh"
}

run_baseline() {
    echo "Running base-model p4llms test against $BASE_URL"
    run_benchmark "$BASE_MODEL_NAME" "p4llms_baseline_qwen3_14b"
}

run_pt() {
    require_executable "$LLAMAFACTORY_BIN"
    require_file "$PT_CONFIG"
    ensure_no_local_vllm
    require_file "$DATA_ROOT/p4llms_p4_code_train.jsonl"
    require_file "$DATA_ROOT/p4llms_p4_code_dev.jsonl"
    "$LLAMAFACTORY_BIN" train "$PT_CONFIG" \
        dataset_dir="$DATA_ROOT" \
        output_dir="$PT_OUTPUT" \
        logging_dir="$PT_OUTPUT/tensorboard" \
        overwrite_output_dir=true
}

run_sft() {
    require_executable "$LLAMAFACTORY_BIN"
    require_file "$SFT_CONFIG"
    ensure_no_local_vllm
    require_file "$DATA_ROOT/p4llms_continuation_train.jsonl"
    require_file "$DATA_ROOT/p4llms_continuation_dev.jsonl"
    local pt_adapter
    pt_adapter="$(latest_adapter "$PT_OUTPUT")" || die "cannot find PT adapter under $PT_OUTPUT"
    echo "Loading PT adapter: $pt_adapter"
    "$LLAMAFACTORY_BIN" train "$SFT_CONFIG" \
        dataset_dir="$DATA_ROOT" \
        adapter_name_or_path="$pt_adapter" \
        lora_rank=8 \
        output_dir="$SFT_OUTPUT" \
        logging_dir="$SFT_OUTPUT/tensorboard" \
        overwrite_output_dir=true
}

run_train() {
    [[ -f "$DATA_ROOT/p4llms_p4_code_train.jsonl" ]] || prepare_data
    run_pt
    run_sft
    local sft_adapter
    sft_adapter="$(latest_adapter "$SFT_OUTPUT")" || die "cannot find SFT adapter under $SFT_OUTPUT"
    echo
    echo "Training complete. SFT adapter: $sft_adapter"
    echo "Start vLLM with --enable-lora and this adapter, then run:"
    echo "  P4LLMS_TEST_MODEL_NAME=qwen3-14b-p4 $SCRIPT_DIR/run_p4llms_qwen3_pipeline.sh test"
}

run_test() {
    local sft_adapter
    sft_adapter="$(latest_adapter "$SFT_OUTPUT")" || die "cannot find SFT adapter under $SFT_OUTPUT"
    echo "Testing model '$TEST_MODEL_NAME' at $BASE_URL"
    echo "The server must already be configured with LoRA adapter: $sft_adapter"
    run_benchmark "$TEST_MODEL_NAME" "p4llms_qwen3_14b_pt_sft"
}

show_status() {
    echo "Repository:       $SCRIPT_DIR"
    echo "Input dataset:    $INPUT_DATASET"
    echo "Data root:        $DATA_ROOT"
    echo "PT output:        $PT_OUTPUT"
    echo "SFT output:       $SFT_OUTPUT"
    echo "Benchmark URL:    $BASE_URL"
    echo "Base model name:  $BASE_MODEL_NAME"
    echo "Test model name:  $TEST_MODEL_NAME"
    echo
    if [[ -f "$DATA_ROOT/p4llms_finetune_manifest.json" ]]; then
        echo "Prepared data: yes"
    else
        echo "Prepared data: no"
    fi
    if adapter="$(latest_adapter "$PT_OUTPUT" 2>/dev/null)"; then
        echo "PT adapter:      $adapter"
    else
        echo "PT adapter:      not found"
    fi
    if adapter="$(latest_adapter "$SFT_OUTPUT" 2>/dev/null)"; then
        echo "SFT adapter:     $adapter"
    else
        echo "SFT adapter:     not found"
    fi
}

usage() {
    cat <<EOF
Usage: $0 <stage>

Stages:
  prepare   Split p4llms by repository and generate PT/SFT JSONL.
  baseline  Run the original Qwen3-14B against p4llms test (vLLM required).
  pt        Run paper-style 10k-context QLoRA continued pretraining.
  sft       Continue from the PT adapter with continuation SFT.
  train     Run pt followed by sft.
  test      Run the p4llms test split against a vLLM LoRA server.
  status    Show data, adapter, endpoint, and output paths.

Important: stop the base vLLM process before 'pt' or 'train'.  After training,
start a vLLM server with --enable-lora, then run 'test'.

Useful overrides:
  P4LLMS_MAX_WINDOWS_PER_FILE=4
  QWEN3_VLLM_BASE_URL=http://127.0.0.1:18001/v1
  P4LLMS_TEST_MODEL_NAME=qwen3-14b-p4
  P4LLMS_ALLOW_ACTIVE_VLLM=1  # bypass the training safety check
EOF
}

stage="${1:-}"
case "$stage" in
    prepare)
        prepare_data
        ;;
    baseline)
        [[ -f "$DATA_ROOT/p4llms_p4_code_test.jsonl" ]] || prepare_data
        run_baseline
        ;;
    pt)
        run_pt
        ;;
    sft)
        run_sft
        ;;
    train)
        run_train
        ;;
    test)
        run_test
        ;;
    status)
        show_status
        ;;
    -h|--help|help)
        usage
        ;;
    *)
        usage >&2
        exit 2
        ;;
esac
