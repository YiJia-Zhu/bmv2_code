#!/usr/bin/env bash
# Run one of the LlamaFactory P4 training stages.
# Prepare data first with:
#   python3 training_smoke/tools/prepare_p4llms_finetune.py

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SMOKE_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
REPO_ROOT="$(cd "$SMOKE_DIR/.." && pwd)"
cd "$REPO_ROOT"
MODE="${1:-pt}"
if [[ $# -gt 0 ]]; then
    shift
fi

if [[ -n "${LLAMAFACTORY_BIN:-}" ]]; then
    LLAMAFACTORY_BIN="$LLAMAFACTORY_BIN"
elif [[ -x /mnt/8T/xgr/zhuyijia/envs/effthink/bin/llamafactory-cli ]]; then
    LLAMAFACTORY_BIN="/mnt/8T/xgr/zhuyijia/envs/effthink/bin/llamafactory-cli"
else
    LLAMAFACTORY_BIN="llamafactory-cli"
fi

case "$MODE" in
    pt)
        CONFIG="$SMOKE_DIR/configs/qwen3_14b_p4_pt_10k.yaml"
        ;;
    sft)
        CONFIG="$SMOKE_DIR/configs/qwen3_14b_p4_sft_10k.yaml"
        ;;
    *)
        echo "Usage: $0 [pt|sft] [additional llamafactory arguments...]" >&2
        exit 2
        ;;
esac

if ! command -v "$LLAMAFACTORY_BIN" >/dev/null 2>&1; then
    echo "Error: LlamaFactory CLI not found: $LLAMAFACTORY_BIN" >&2
    exit 127
fi

exec "$LLAMAFACTORY_BIN" train "$CONFIG" "$@"
