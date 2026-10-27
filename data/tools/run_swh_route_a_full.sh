#!/usr/bin/env bash
set -euo pipefail

TASK_REPO=/mnt/8T/xgr/zhuyijia/bmv2_code
TASK_PYTHON=/mnt/8T/xgr/zhuyijia/envs/effthink/bin/python
TASK_GRAPH=/mnt/8T/xgr/swh-graph-poc-20260604
TASK_WORK="$TASK_GRAPH/route-a-d0-d1-v1/full-d0-d1-v1"
mkdir -p "$TASK_WORK"
cd "$TASK_REPO"
unset HTTP_PROXY HTTPS_PROXY ALL_PROXY http_proxy https_proxy all_proxy
export PYTHONUNBUFFERED=1 AWS_PAGER=''

# The driver also persists the ORIGINAL deadline and cumulative budget on resume.
set +e
timeout --signal=TERM --kill-after=15s 10h \
    "$TASK_PYTHON" -u data/tools/run_swh_route_a_full.py \
    --graph-dir "$TASK_GRAPH" --workers 32 --aws-budget-usd 18.36 --max-hours 10 \
    2>&1 | tee -a "$TASK_WORK/run.log"
TASK_RESULT=${PIPESTATUS[0]}
set -e
printf '%s\n' "$TASK_RESULT" > "$TASK_WORK/exit-code.txt"
exit "$TASK_RESULT"
