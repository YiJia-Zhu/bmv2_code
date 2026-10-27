#!/usr/bin/env bash
set -u
TASK_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)"
cd -- "$TASK_ROOT"
OUTPUT_ROOT="${SWH_HISTORY_OUTPUT:-$TASK_ROOT/data/raw/repositories/swh_full_history}"
mkdir -p -- "$OUTPUT_ROOT"
if [[ -f "$OUTPUT_ROOT/exit_code.txt" ]]; then
    mv -- "$OUTPUT_ROOT/exit_code.txt" "$OUTPUT_ROOT/previous_exit_code.txt"
fi
python3 -u data/tools/download_swh_history.py --output "$OUTPUT_ROOT" "$@" >> "$OUTPUT_ROOT/run.log" 2>&1
RESULT=$?
printf '%s\n' "$RESULT" > "$OUTPUT_ROOT/exit_code.txt"
exit "$RESULT"
