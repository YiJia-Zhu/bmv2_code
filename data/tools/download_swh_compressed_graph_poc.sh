#!/usr/bin/env bash
set -euo pipefail

BASE_URL="https://softwareheritage.s3.amazonaws.com/graph/2026-06-04/compressed"
DEST_DIR="${SWH_GRAPH_POC_DIR:-/mnt/8T/xgr/swh-graph-poc-20260604}"
MAX_PARALLEL="${SWH_GRAPH_DOWNLOAD_JOBS:-4}"
CONNECTIONS_PER_FILE="${SWH_GRAPH_CONNECTIONS_PER_FILE:-4}"
PROGRESS_INTERVAL="${SWH_GRAPH_PROGRESS_INTERVAL:-10}"
PROXY="${SWH_GRAPH_PROXY:-}"
PROXYCHAINS_CONFIG="${SWH_GRAPH_PROXYCHAINS_CONFIG:-}"

KEYS=(
    graph-transposed.graph
    graph-transposed.ef
    graph.fmphgo
    graph.node2type.bin.zst
)
SIZES=(
    196157270424
    51015688144
    16823249121
    8575434753
)

if [[ ! "$MAX_PARALLEL" =~ ^[1-4]$ ]]; then
    printf 'SWH_GRAPH_DOWNLOAD_JOBS must be an integer from 1 to 4\n' >&2
    exit 2
fi
if [[ ! "$CONNECTIONS_PER_FILE" =~ ^[1-8]$ ]] || \
    (( MAX_PARALLEL * CONNECTIONS_PER_FILE > 16 )); then
    printf 'SWH_GRAPH_CONNECTIONS_PER_FILE must be 1..8, with at most 16 total connections\n' >&2
    exit 2
fi
if [[ ! "$PROGRESS_INTERVAL" =~ ^[1-9][0-9]*$ ]]; then
    printf 'SWH_GRAPH_PROGRESS_INTERVAL must be a positive integer in seconds\n' >&2
    exit 2
fi
if ! command -v aria2c >/dev/null 2>&1; then
    printf 'aria2c is required for segmented downloads\n' >&2
    exit 2
fi
if [[ -n "$PROXY" && ! "$PROXY" =~ ^https?://[^/]+$ ]]; then
    printf 'SWH_GRAPH_PROXY must be an HTTP(S) proxy URL without a path\n' >&2
    exit 2
fi
if [[ -n "$PROXY" && -n "$PROXYCHAINS_CONFIG" ]]; then
    printf 'Use only one of SWH_GRAPH_PROXY and SWH_GRAPH_PROXYCHAINS_CONFIG\n' >&2
    exit 2
fi
if [[ -n "$PROXYCHAINS_CONFIG" ]]; then
    if ! command -v proxychains4 >/dev/null 2>&1 || [[ ! -r "$PROXYCHAINS_CONFIG" ]]; then
        printf 'proxychains4 and a readable SWH_GRAPH_PROXYCHAINS_CONFIG are required\n' >&2
        exit 2
    fi
fi

mkdir -p "$DEST_DIR"
LOG_FILE="$DEST_DIR/download.log"
exec > >(tee -a "$LOG_FILE") 2>&1

TOTAL_BYTES=0
for size in "${SIZES[@]}"; do
    TOTAL_BYTES=$((TOTAL_BYTES + size))
done

declare -a PIDS=()
declare -a PID_KEYS=()
next_file=0
active_jobs=0
failed_jobs=0
reaped_jobs=0

current_bytes() {
    local i path blocks block_size size total=0
    for i in "${!KEYS[@]}"; do
        path="$DEST_DIR/${KEYS[$i]}"
        size=0
        if [[ -f "$path" ]]; then
            # aria2 writes ranges out of order, so apparent file length is not progress.
            read -r blocks block_size < <(stat -c '%b %B' "$path")
            size=$((blocks * block_size))
        fi
        if (( size > SIZES[$i] )); then
            size="${SIZES[$i]}"
        fi
        total=$((total + size))
    done
    printf '%s' "$total"
}

active_file_list() {
    local i result=""
    for i in "${!PIDS[@]}"; do
        if [[ -n "${PIDS[$i]:-}" ]]; then
            if [[ -n "$result" ]]; then
                result+=", "
            fi
            result+="${PID_KEYS[$i]}"
        fi
    done
    printf '%s' "${result:-none}"
}

report_progress() {
    local now="$1"
    local downloaded="$2"
    local elapsed=$((now - last_report_time))
    local delta=$((downloaded - last_report_bytes))
    if (( elapsed < 1 )); then elapsed=1; fi
    if (( delta < 0 )); then delta=0; fi

    awk -v done="$downloaded" -v total="$TOTAL_BYTES" \
        -v delta="$delta" -v elapsed="$elapsed" '
        function human_bytes(n, units, i) {
            split("B KiB MiB GiB TiB", units, " ")
            i = 1
            while (n >= 1024 && i < 5) { n /= 1024; i++ }
            return sprintf("%.2f %s", n, units[i])
        }
        function human_time(s, days, hours, minutes) {
            if (s < 0) return "unknown"
            s = int(s)
            days = int(s / 86400); s %= 86400
            hours = int(s / 3600); s %= 3600
            minutes = int(s / 60); s %= 60
            if (days > 0) return sprintf("%dd %02d:%02d:%02d", days, hours, minutes, s)
            return sprintf("%02d:%02d:%02d", hours, minutes, s)
        }
        BEGIN {
            rate = delta / elapsed
            remaining = total - done
            eta = rate > 0 ? remaining / rate : -1
            printf "Progress (allocated data, approx.): %.2f%% (%s / %s) | %.2f MiB/s | ETA %s\n", \
                100 * done / total, human_bytes(done), human_bytes(total), \
                rate / 1048576, human_time(eta)
        }
    '
    if (( active_jobs > 0 )); then
        printf 'Active files: %s\n' "$(active_file_list)"
    fi

    last_report_time="$now"
    last_report_bytes="$downloaded"
}

download_one() {
    local key="$1"
    local expected_size="$2"
    local destination="$DEST_DIR/$key"
    local error_log="$DEST_DIR/$key.aria2.log"
    local actual_size aria2_pid="" aria2_status
    local -a proxy_args=() download_cmd=(aria2c)
    if [[ -n "$PROXY" ]]; then
        proxy_args=(--all-proxy="$PROXY")
    elif [[ -n "$PROXYCHAINS_CONFIG" ]]; then
        download_cmd=(proxychains4 -q -f "$PROXYCHAINS_CONFIG" aria2c)
    fi

    worker_signal() {
        if [[ -n "$aria2_pid" ]]; then
            kill -TERM "$aria2_pid" 2>/dev/null || true
            wait "$aria2_pid" 2>/dev/null || true
        fi
        exit 143
    }
    trap worker_signal INT TERM

    if [[ -f "$destination" ]]; then
        actual_size="$(stat -c '%s' "$destination")"
        if [[ "$actual_size" == "$expected_size" && ! -f "$destination.aria2" ]]; then
            printf '%s already complete; skipping\n' "$key"
            return 0
        fi
        if (( actual_size > expected_size )); then
            printf 'Unexpected local size for %s: expected %s, got %s\n' \
                "$key" "$expected_size" "$actual_size" >&2
            return 1
        fi
    fi

    printf '%s downloading/resuming %s\n' "$(date -Is)" "$key"
    "${download_cmd[@]}" --no-conf --continue=true --always-resume=true \
        "${proxy_args[@]}" \
        --auto-file-renaming=false --allow-overwrite=false \
        --file-allocation=none --split="$CONNECTIONS_PER_FILE" \
        --max-connection-per-server="$CONNECTIONS_PER_FILE" \
        --min-split-size=16M --max-tries=8 --retry-wait=5 \
        --show-console-readout=false --summary-interval="$PROGRESS_INTERVAL" \
        --dir="$DEST_DIR" --out="$key" "$BASE_URL/$key" \
        >>"$error_log" 2>&1 &
    aria2_pid="$!"
    if wait "$aria2_pid"; then
        aria2_pid=""
    else
        aria2_status="$?"
        aria2_pid=""
        printf '%s aria2 failed for %s (exit %s); details: %s\n' \
            "$(date -Is)" "$key" "$aria2_status" "$error_log" >&2
        return "$aria2_status"
    fi
    trap - INT TERM

    actual_size="$(stat -c '%s' "$destination")"
    if [[ "$actual_size" != "$expected_size" ]]; then
        printf 'Size check failed for %s: expected %s, got %s\n' \
            "$key" "$expected_size" "$actual_size" >&2
        return 1
    fi
    if [[ -f "$destination.aria2" ]]; then
        printf 'Incomplete aria2 control file remains for %s\n' "$key" >&2
        return 1
    fi
    printf '%s verified %s (%s bytes)\n' "$(date -Is)" "$key" "$actual_size"
}

launch_pending() {
    while (( next_file < ${#KEYS[@]} && active_jobs < MAX_PARALLEL )); do
        download_one "${KEYS[$next_file]}" "${SIZES[$next_file]}" &
        PIDS[$next_file]="$!"
        PID_KEYS[$next_file]="${KEYS[$next_file]}"
        active_jobs=$((active_jobs + 1))
        next_file=$((next_file + 1))
    done
}

reap_finished() {
    local i pid status
    reaped_jobs=0
    for i in "${!PIDS[@]}"; do
        pid="${PIDS[$i]:-}"
        if [[ -n "$pid" ]] && ! kill -0 "$pid" 2>/dev/null; then
            if wait "$pid"; then
                :
            else
                status="$?"
                failed_jobs=$((failed_jobs + 1))
                printf '%s download failed for %s (exit %s)\n' \
                    "$(date -Is)" "${PID_KEYS[$i]}" "$status" >&2
            fi
            unset 'PIDS[i]'
            unset 'PID_KEYS[i]'
            active_jobs=$((active_jobs - 1))
            reaped_jobs=$((reaped_jobs + 1))
        fi
    done
}

stop_workers() {
    local exit_status="${1:-130}"
    local reason="${2:-interrupted}"
    trap - INT TERM
    printf '%s %s; stopping downloads. Partial files are kept for resume.\n' \
        "$(date -Is)" "$reason" >&2
    local pid
    for pid in "${PIDS[@]}"; do
        if [[ -n "$pid" ]]; then kill -TERM "$pid" 2>/dev/null || true; fi
    done
    for pid in "${PIDS[@]}"; do
        if [[ -n "$pid" ]]; then wait "$pid" 2>/dev/null || true; fi
    done
    exit "$exit_status"
}
trap 'stop_workers 130 interrupted' INT TERM

printf 'Destination: %s\nLog: %s\n' "$DEST_DIR" "$LOG_FILE"
printf 'Parallel files: %s; connections per file: %s; max connections: %s\n' \
    "$MAX_PARALLEL" "$CONNECTIONS_PER_FILE" "$((MAX_PARALLEL * CONNECTIONS_PER_FILE))"
if [[ -n "$PROXY" ]]; then
    printf 'HTTP proxy enabled for aria2c downloads\n'
elif [[ -n "$PROXYCHAINS_CONFIG" ]]; then
    printf 'SOCKS proxy enabled through proxychains4\n'
fi
printf 'Total: %s bytes (about 272.6 GB). Progress interval: %s seconds.\n' \
    "$TOTAL_BYTES" "$PROGRESS_INTERVAL"

last_report_time="$(date +%s)"
last_report_bytes="$(current_bytes)"
launch_pending
report_progress "$last_report_time" "$last_report_bytes"

while (( active_jobs > 0 )); do
    sleep 1
    reap_finished
    if (( failed_jobs > 0 )); then
        stop_workers 1 'a file download failed'
    fi
    now="$(date +%s)"
    if (( reaped_jobs > 0 || now - last_report_time >= PROGRESS_INTERVAL )); then
        downloaded="$(current_bytes)"
        report_progress "$now" "$downloaded"
    fi
    launch_pending
done

if (( failed_jobs > 0 )); then
    printf '%s %s download(s) failed; rerun this script to resume incomplete files.\n' \
        "$(date -Is)" "$failed_jobs" >&2
    exit 1
fi

printf '%s all four files downloaded and size-verified\n' "$(date -Is)"
