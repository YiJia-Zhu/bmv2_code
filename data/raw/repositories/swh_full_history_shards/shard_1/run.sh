#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
python3 -u "$ROOT/collector/download_swh_history.py" \
  --input "$ROOT/input.csv" --output "$ROOT" \
  --shard-count 4 --shard-index 1 \
  --proxy "${SWH_HISTORY_PROXY:-}" \
  --env-file "${SWH_HISTORY_ENV_FILE:-$ROOT/.env}" \
  "$@" >> "$ROOT/run.log" 2>&1
