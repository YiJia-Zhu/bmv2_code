#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd -- "$ROOT/../../../../.." && pwd)"

# The Python collector reads selected token keys; never source or print .env.
# Request pacing is shared by this process's four GitHub API workers.
export SWH_HISTORY_ENV_FILE="${SWH_HISTORY_ENV_FILE:-$PROJECT_ROOT/.env}"
export SWH_HISTORY_PROXY="${SWH_HISTORY_PROXY-http://127.0.0.1:7892}"
exec bash "$ROOT/run.sh" \
  --api-only \
  --github-api-rph 4500 \
  --github-api-concurrency 4 \
  --report-seconds 30 \
  "$@"
