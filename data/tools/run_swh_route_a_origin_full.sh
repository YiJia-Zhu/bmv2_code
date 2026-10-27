#!/usr/bin/env bash
set -euo pipefail

TASK_REPO=/mnt/8T/xgr/zhuyijia/bmv2_code
TASK_PYTHON=/mnt/8T/xgr/zhuyijia/envs/effthink/bin/python
TASK_ROOT=/mnt/8T/xgr/swh-graph-poc-20260604/route-a-revision-origin-v1/full
mkdir -p "$TASK_ROOT"
cd "$TASK_REPO"
export PYTHONUNBUFFERED=1
TASK_REMAINING_SECONDS=$("$TASK_PYTHON" - "$TASK_ROOT/control.json" <<'PY'
import datetime as dt
import json
from pathlib import Path
import sys
p = Path(sys.argv[1])
remaining = 12 * 3600
if p.exists():
    control = json.loads(p.read_text())
    remaining = (dt.datetime.fromisoformat(control['deadline_utc']) - dt.datetime.now(dt.timezone.utc)).total_seconds()
print(max(1, int(remaining)))
PY
)
if test -f "$TASK_ROOT/exit-code.txt"; then
  mv "$TASK_ROOT/exit-code.txt" "$TASK_ROOT/exit-code.previous.txt"
fi

set +e
timeout --signal=TERM --kill-after=30s "${TASK_REMAINING_SECONDS}s" \
  "$TASK_PYTHON" -u data/tools/run_swh_route_a_origin_full.py \
  --graph /mnt/8T/xgr/swh-graph-poc-20260604 \
  --merge /mnt/8T/xgr/swh-graph-poc-20260604/route-a-revision-origin-v1/merge \
  --output "$TASK_ROOT" \
  --binary /mnt/8T/xgr/swh-graph-poc-20260604/route-a-revision-origin-v1/build/release/origin \
  --workers 32 --batch-size 512 --seed-batch 512 \
  --max-hours 12 --aws-budget-usd 100 \
  2>&1 | tee -a "$TASK_ROOT/run.log"
TASK_RESULT=${PIPESTATUS[0]}
set -e
printf '%s\n' "$TASK_RESULT" > "$TASK_ROOT/exit-code.txt"
exit "$TASK_RESULT"
