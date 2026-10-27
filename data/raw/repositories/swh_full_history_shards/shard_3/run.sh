#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
for REQUIRED in input.csv manifests/state.sqlite3; do
  if [[ ! -f "$ROOT/$REQUIRED" ]]; then
    printf 'Missing %s; copy this shard checkpoint from the source machine.\n' "$ROOT/$REQUIRED" >&2
    exit 2
  fi
done
python3 -u "$ROOT/collector/download_swh_history.py" \
  --input "$ROOT/input.csv" --output "$ROOT" \
  --shard-count 4 --shard-index 3 \
  --proxy "${SWH_HISTORY_PROXY:-}" \
  --env-file "${SWH_HISTORY_ENV_FILE:-$ROOT/.env}" \
  "$@" >> "$ROOT/run.log" 2>&1
