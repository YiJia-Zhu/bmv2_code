#!/usr/bin/env bash
set -euo pipefail

TASK_REPO=/mnt/8T/xgr/zhuyijia/bmv2_code
TASK_PYTHON=/mnt/8T/xgr/zhuyijia/envs/effthink/bin/python
TASK_ROOT=/mnt/8T/xgr/swh-graph-poc-20260604/route-a-revision-origin-v1/sample8
mkdir -p "$TASK_ROOT"
cd "$TASK_REPO"
export PYTHONUNBUFFERED=1
set +e
timeout --signal=TERM --kill-after=30s 10h \
    "$TASK_PYTHON" -u data/tools/run_swh_route_a_origin_sample.py \
    --max-hours 10 --aws-budget-usd 100 --max-nodes 1000000 --max-edges 10000000 \
    2>&1 | tee -a "$TASK_ROOT/run.log"
TASK_RESULT=${PIPESTATUS[0]}
set -e
printf '%s\n' "$TASK_RESULT" > "$TASK_ROOT/exit-code.txt"
exit "$TASK_RESULT"
