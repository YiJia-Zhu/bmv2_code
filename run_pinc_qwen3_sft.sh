#!/usr/bin/env bash
# Prepare and train Qwen3-14B on the PINC intent-to-P4 dataset.
#
# This pipeline is separate from the existing p4llms/Generating pipeline:
#   prepare : raw SFU/PINC JSONL -> repository-disjoint ShareGPT JSONL
#   train   : QLoRA SFT, intent -> complete P4_16 program
#   status  : print the configured paths and generated artifacts

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

PYTHON_BIN="${PINC_PYTHON_BIN:-/mnt/8T/xgr/zhuyijia/envs/effthink/bin/python}"
LLAMAFACTORY_BIN="${LLAMAFACTORY_BIN:-/mnt/8T/xgr/zhuyijia/envs/effthink/bin/llamafactory-cli}"
MODEL_PATH="${PINC_MODEL_PATH:-/mnt/8T/xgr/zhuyijia/huggingface_models/Qwen3-14B}"
INPUT_DATASET="${PINC_INPUT_DATASET:-$SCRIPT_DIR/data/raw/hf/SFU_p4_dataset/p4_dataset.jsonl}"
DATA_ROOT="${PINC_DATA_ROOT:-$SCRIPT_DIR/data/finetune/pinc}"
CONFIG="${PINC_CONFIG:-$SCRIPT_DIR/training/qwen3_14b_pinc_sft.yaml}"
OUTPUT_DIR="${PINC_OUTPUT_DIR:-$SCRIPT_DIR/training_runs/qwen3_14b_pinc_sft}"
MAX_SEQ_LEN="${PINC_MAX_SEQ_LEN:-16384}"
VLLM_BASE_URL="${PINC_VLLM_BASE_URL:-http://127.0.0.1:18001/v1}"

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
    local port="${VLLM_BASE_URL##*:}"
    port="${port%%/*}"
    if local_port_open "$port" && [[ "${PINC_ALLOW_ACTIVE_VLLM:-0}" != "1" ]]; then
        die "local vLLM appears active on port $port; stop it before PINC training, or set PINC_ALLOW_ACTIVE_VLLM=1"
    fi
}

prepare_data() {
    require_executable "$PYTHON_BIN"
    require_file "$INPUT_DATASET"
    require_file "$MODEL_PATH/tokenizer_config.json"
    "$PYTHON_BIN" "$SCRIPT_DIR/data/tools/prepare_pinc_finetune.py" \
        --input "$INPUT_DATASET" \
        --output-root "$DATA_ROOT" \
        --tokenizer "$MODEL_PATH" \
        --max-seq-len "$MAX_SEQ_LEN"
}

data_matches_config() {
    [[ -f "$DATA_ROOT/pinc_finetune_manifest.json" ]] || return 1
    "$PYTHON_BIN" - "$DATA_ROOT/pinc_finetune_manifest.json" "$MAX_SEQ_LEN" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as handle:
    manifest = json.load(handle)
sys.exit(0 if manifest.get("max_seq_len") == int(sys.argv[2]) else 1)
PY
}

run_train() {
    require_executable "$PYTHON_BIN"
    require_executable "$LLAMAFACTORY_BIN"
    require_file "$CONFIG"
    require_file "$DATA_ROOT/pinc_sft_train.jsonl"
    require_file "$DATA_ROOT/pinc_sft_dev.jsonl"
    data_matches_config || prepare_data
    ensure_no_local_vllm

    "$LLAMAFACTORY_BIN" train "$CONFIG" \
        model_name_or_path="$MODEL_PATH" \
        dataset_dir="$DATA_ROOT" \
        output_dir="$OUTPUT_DIR" \
        logging_dir="$OUTPUT_DIR/tensorboard" \
        overwrite_output_dir=true \
        cutoff_len="$MAX_SEQ_LEN" \
        "$@"
}

show_status() {
    echo "PINC SFT pipeline"
    echo "  input dataset: $INPUT_DATASET"
    echo "  model path:    $MODEL_PATH"
    echo "  data root:     $DATA_ROOT"
    echo "  config:        $CONFIG"
    echo "  output dir:    $OUTPUT_DIR"
    echo "  max seq len:   $MAX_SEQ_LEN"
    echo
    if [[ -f "$DATA_ROOT/pinc_finetune_manifest.json" ]]; then
        echo "  prepared data: yes"
        "$PYTHON_BIN" - "$DATA_ROOT/pinc_finetune_manifest.json" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as handle:
    manifest = json.load(handle)
print("  records:       {}".format(manifest.get("written_records")))
print("  repositories:   {}".format(manifest.get("written_repository_counts")))
print("  dropped long:   {}".format(manifest.get("dropped_overlength")))
PY
    else
        echo "  prepared data: no"
    fi
    if [[ -f "$OUTPUT_DIR/adapter_config.json" ]]; then
        echo "  adapter:        $OUTPUT_DIR"
    else
        echo "  adapter:        not found"
    fi
}

usage() {
    cat <<EOF
Usage: $0 <stage> [additional llamafactory key=value overrides]

Stages:
  prepare   Build repository-disjoint PINC ShareGPT train/dev/test files.
  train     Run Qwen3-14B 4-bit LoRA SFT on intent -> complete P4.
  status    Show paths and generated artifacts.

Useful overrides:
  PINC_MODEL_PATH=/path/to/Qwen3-14B
  PINC_MAX_SEQ_LEN=10240
  PINC_ALLOW_ACTIVE_VLLM=1
  LLAMAFACTORY_BIN=/path/to/llamafactory-cli
EOF
}

stage="${1:-status}"
if [[ $# -gt 0 ]]; then
    shift
fi

case "$stage" in
    prepare)
        [[ $# -eq 0 ]] || die "prepare does not accept LlamaFactory overrides"
        prepare_data
        ;;
    train)
        if [[ ! -f "$DATA_ROOT/pinc_sft_train.jsonl" ]]; then
            prepare_data
        fi
        run_train "$@"
        ;;
    status)
        [[ $# -eq 0 ]] || die "status does not accept arguments"
        show_status
        ;;
    *)
        usage
        exit 2
        ;;
esac
