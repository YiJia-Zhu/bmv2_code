#!/usr/bin/env bash
set -euo pipefail

TASK_REPO=/mnt/8T/xgr/zhuyijia/bmv2_code
TASK_PYTHON=/mnt/8T/xgr/zhuyijia/envs/effthink/bin/python
TASK_ROOT=/mnt/8T/xgr/swh-graph-poc-20260604/route-a-revision-origin-v1/revision-resolve-v1
mkdir -p "$TASK_ROOT"
cd "$TASK_REPO"
export PYTHONUNBUFFERED=1
"$TASK_PYTHON" -u data/tools/run_swh_route_a_revision_resolve.py \
  --output "$TASK_ROOT" --workers 32 --batch-spans 128 \
  2>&1 | tee -a "$TASK_ROOT/run.log"
