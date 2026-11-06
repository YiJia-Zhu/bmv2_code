#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd -- "$ROOT/../../../.." && pwd)"
export SWH_HISTORY_ENV_FILE="${SWH_HISTORY_ENV_FILE:-$PROJECT_ROOT/.env}"

for required_command in setsid flock; do
  if ! command -v "$required_command" >/dev/null 2>&1; then
    printf '%s is required.\n' "$required_command" >&2
    exit 2
  fi
done

exec 9>"$ROOT/run_all.lock"
if ! flock -n 9; then
  printf 'run_all.sh is already running.\n' >&2
  exit 2
fi

for index in 0 1 2 3; do
  for required in input.csv manifests/state.sqlite3; do
    if [[ ! -f "$ROOT/shard_$index/$required" ]]; then
      printf 'Missing shard_%s/%s\n' "$index" "$required" >&2
      exit 2
    fi
  done
done

pids=()
stop_children() {
  local pid
  trap '' INT TERM
  printf 'Stopping shard workers; waiting for in-flight requests to finish...\n' >&2
  for pid in "${pids[@]}"; do
    kill -TERM -- "-$pid" 2>/dev/null || true
  done
  for pid in "${pids[@]}"; do
    wait "$pid" 2>/dev/null || true
  done
}
trap 'stop_children; exit 130' INT
trap 'stop_children; exit 143' TERM

for index in 0 1 2 3; do
  setsid bash "$ROOT/shard_$index/run.sh" "$@" 9>&- &
  pid=$!
  pids+=("$pid")
  printf 'Started shard_%s PID=%s log=%s/shard_%s/run.log\n' \
    "$index" "$pid" "$ROOT" "$index"
done

result=0
for index in 0 1 2 3; do
  if wait "${pids[$index]}"; then
    printf 'shard_%s finished successfully.\n' "$index"
  else
    code=$?
    printf 'shard_%s exited with status %s; see run.log.\n' "$index" "$code" >&2
    result=1
  fi
done
exit "$result"
