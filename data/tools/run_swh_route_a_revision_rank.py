#!/usr/bin/env python3
"""Join revision dates to origin edges and rank candidates per origin.

This is a local, deterministic stage.  It only orders candidates; it does not
claim that a revision still contains a P4_16 file or that it compiles.
"""

import argparse
import datetime as dt
import hashlib
import json
import os
import time
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


GRAPH_ROOT = Path("/mnt/8T/xgr/swh-graph-poc-20260604")
RESOLVE_ROOT = GRAPH_ROOT / "route-a-revision-origin-v1/revision-resolve-v1"
DATES_ROOT = GRAPH_ROOT / "route-a-revision-origin-v1/revision-dates-v1"
OUTPUT_ROOT = GRAPH_ROOT / "route-a-revision-origin-v1/revision-rank-v1"
ORIGIN_INPUT = RESOLVE_ROOT / "revision_origin_with_swhid.parquet"
DATE_INPUT = DATES_ROOT / "shards"

JOIN_SCHEMA = pa.schema([
    ("candidate_revision_node_id", pa.int64()),
    ("candidate_revision_swhid", pa.string()),
    ("candidate_revision_id", pa.string()),
    ("origin_node_id", pa.int64()),
    ("origin_id", pa.string()),
    ("origin_url", pa.string()),
    ("revision_date", pa.string()),
    ("committer_date", pa.string()),
    ("directory", pa.string()),
    ("source_path", pa.string()),
    ("date_status", pa.string()),
])
LATEST_SCHEMA = pa.schema(list(JOIN_SCHEMA) + [("candidate_rank", pa.int32()),
                                                ("selection_status", pa.string())])


def sha256_file(path):
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def write_json(path, value):
    temporary = path.with_suffix(path.suffix + ".partial")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n",
                         encoding="utf-8")
    os.replace(str(temporary), str(path))


def parse_timestamp(value):
    if not value:
        return None
    try:
        return dt.datetime.strptime(value, "%Y-%m-%d %H:%M:%S.%f")
    except ValueError:
        try:
            parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
            if parsed.tzinfo is not None:
                parsed = parsed.astimezone(dt.timezone.utc).replace(tzinfo=None)
            return parsed
        except ValueError:
            return None


def candidate_key(row):
    revision = parse_timestamp(row.get("revision_date"))
    committer = parse_timestamp(row.get("committer_date"))
    # Revision date is the primary ordering field.  A committer date is only a
    # deterministic tie breaker and is never used to claim P4_16 validity.
    return (revision is not None, revision or dt.datetime.min,
            committer or dt.datetime.min, row.get("candidate_revision_id") or "")


def load_dates(date_root):
    dates = {}
    source_count = 0
    for path in sorted(date_root.glob("part-*.parquet")):
        source_count += 1
        for row in pq.ParquetFile(path).iter_batches(batch_size=100000):
            for item in pa.Table.from_batches([row]).to_pylist():
                revision_id = item["id"]
                if revision_id in dates:
                    raise RuntimeError("duplicate revision date: " + revision_id)
                dates[revision_id] = item
    if source_count != 128 or len(dates) != 317804:
        raise RuntimeError("expected 128 date shards and 317,804 unique rows")
    return dates, source_count


def identity_for(origin_input, date_root):
    date_files = sorted(date_root.glob("part-*.parquet"))
    if len(date_files) != 128:
        raise RuntimeError("expected 128 revision date shards")
    return {
        "version": "route-a-revision-rank-v1",
        "origin_input": str(origin_input),
        "origin_input_sha256": sha256_file(origin_input),
        "origin_input_rows": pq.read_metadata(origin_input).num_rows,
        "date_shards": [
            {"name": path.name, "bytes": path.stat().st_size,
             "sha256": sha256_file(path)} for path in date_files
        ],
    }


def run(origin_input, date_root, output, batch_size):
    output.mkdir(parents=True, exist_ok=True)
    identity = identity_for(origin_input, date_root)
    identity_path = output / "identity.json"
    done_path = output / "complete.json"
    if done_path.exists() and identity_path.exists():
        done = json.loads(done_path.read_text(encoding="utf-8"))
        if done.get("identity") == identity:
            return done
        raise RuntimeError("existing rank output belongs to different inputs")

    dates, date_shards = load_dates(date_root)
    write_json(identity_path, identity)
    joined_path = output / "revision_origin_with_dates.parquet"
    latest_path = output / "origin_latest_candidates.parquet"
    joined_partial = joined_path.with_suffix(".parquet.partial")
    latest_partial = latest_path.with_suffix(".parquet.partial")
    for path in (joined_partial, latest_partial):
        if path.exists():
            path.unlink()

    latest = {}
    joined_rows = 0
    missing_dates = 0
    started = time.monotonic()
    parquet = pq.ParquetFile(origin_input)
    with pq.ParquetWriter(joined_partial, JOIN_SCHEMA, compression="zstd") as writer:
        for batch_number, batch in enumerate(parquet.iter_batches(batch_size=batch_size)):
            rows = []
            for source in pa.Table.from_batches([batch]).to_pylist():
                date_row = dates.get(source["candidate_revision_id"])
                if date_row is None:
                    missing_dates += 1
                    source.update({"revision_date": None, "committer_date": None,
                                   "directory": None, "source_path": None,
                                   "date_status": "missing_revision_date"})
                else:
                    source.update({
                        "revision_date": date_row["date"],
                        "committer_date": date_row["committer_date"],
                        "directory": date_row["directory"],
                        "source_path": date_row["source_path"],
                        "date_status": "revision_date_available"
                        if date_row["date"] else "revision_date_null",
                    })
                rows.append(source)
                key = source["origin_id"]
                previous = latest.get(key)
                if previous is None or candidate_key(source) > candidate_key(previous):
                    latest[key] = dict(source)
            writer.write_table(pa.Table.from_pylist(rows, schema=JOIN_SCHEMA))
            joined_rows += len(rows)
            elapsed = max(time.monotonic() - started, 0.001)
            rate = joined_rows / elapsed
            print(json.dumps({"event": "join_progress", "batch": batch_number,
                              "rows": joined_rows, "origins": len(latest),
                              "rate_rows_per_second": round(rate, 1),
                              "timestamp_utc": dt.datetime.now(dt.timezone.utc).isoformat()},
                             sort_keys=True), flush=True)
    os.replace(str(joined_partial), str(joined_path))

    latest_rows = []
    for row in latest.values():
        row["candidate_rank"] = 1
        row["selection_status"] = (
            "ordered_unconfirmed" if row["date_status"] == "revision_date_available"
            else "ordered_with_missing_revision_date")
        latest_rows.append(row)
    latest_rows.sort(key=lambda row: (row["origin_id"], row["candidate_revision_id"]))
    with pq.ParquetWriter(latest_partial, LATEST_SCHEMA, compression="zstd") as writer:
        writer.write_table(pa.Table.from_pylist(latest_rows, schema=LATEST_SCHEMA))
    os.replace(str(latest_partial), str(latest_path))

    summary = {
        "status": "passed",
        "identity": identity,
        "date_shards": date_shards,
        "joined_rows": joined_rows,
        "unique_origins": len(latest_rows),
        "missing_date_join_rows": missing_dates,
        "ordered_unconfirmed_rows": sum(
            row["selection_status"] == "ordered_unconfirmed" for row in latest_rows),
        "completed_at_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "files": [
            {"path": joined_path.name, "rows": joined_rows,
             "bytes": joined_path.stat().st_size, "sha256": sha256_file(joined_path)},
            {"path": latest_path.name, "rows": len(latest_rows),
             "bytes": latest_path.stat().st_size, "sha256": sha256_file(latest_path)},
        ],
    }
    write_json(done_path, summary)
    return summary


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--origin-input", type=Path, default=ORIGIN_INPUT)
    parser.add_argument("--date-root", type=Path, default=DATE_INPUT)
    parser.add_argument("--output", type=Path, default=OUTPUT_ROOT)
    parser.add_argument("--batch-size", type=int, default=100000)
    args = parser.parse_args()
    if args.batch_size < 1:
        parser.error("--batch-size must be positive")
    print(json.dumps(run(args.origin_input, args.date_root, args.output,
                         args.batch_size), sort_keys=True), flush=True)


if __name__ == "__main__":
    raise SystemExit(main())
