#!/usr/bin/env bash
set -euo pipefail

TASK_REPO=/mnt/8T/xgr/zhuyijia/bmv2_code
TASK_PYTHON=/mnt/8T/xgr/zhuyijia/envs/effthink/bin/python
TASK_WORK=/mnt/8T/xgr/swh-graph-poc-20260604/route-a-d2-d8-v1
mkdir -p "$TASK_WORK"
cd "$TASK_REPO"
export PYTHONUNBUFFERED=1

# The Python control file preserves the first deadline across resumed runs.
set +e
timeout --signal=TERM --kill-after=30s 10h \
    "$TASK_PYTHON" -u data/tools/run_swh_route_a_layers.py --max-hours 10 \
    2>&1 | tee -a "$TASK_WORK/run.log"
TASK_RESULT=${PIPESTATUS[0]}
set -e
printf '%s\n' "$TASK_RESULT" > "$TASK_WORK/exit-code.txt"
exit "$TASK_RESULT"
