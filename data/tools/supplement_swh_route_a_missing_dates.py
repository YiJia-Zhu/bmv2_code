#!/usr/bin/env python3
"""Correct ranking of undated synthetic opam revisions using package versions."""

import argparse
import datetime as dt
import hashlib
import json
import os
import re
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq


GRAPH_ROOT = Path("/mnt/8T/xgr/swh-graph-poc-20260604")
RANK_ROOT = GRAPH_ROOT / "route-a-revision-origin-v1/revision-rank-v1"
OUTPUT_ROOT = GRAPH_ROOT / "route-a-revision-origin-v1/revision-rank-v2"
SUPPLEMENT = Path("data/raw/swh_p4/petr4_opam_synthetic_revision_metadata.json")
ORIGIN_ID = "8e140ee02c9734a85b99e05b37a9200351367f36"


def sha256(path):
    h = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def numeric_package_version(message):
    if not isinstance(message, str) or not re.fullmatch(r"[0-9]+(?:\.[0-9]+)*", message):
        raise ValueError("unsupported package version: {!r}".format(message))
    return tuple(int(part) for part in message.split("."))


def apply_supplement(latest_rows, relation_rows, metadata):
    if metadata.get("origin_id") != ORIGIN_ID or not metadata.get("origin_url", "").startswith("opam+"):
        raise RuntimeError("unexpected synthetic origin")
    revision_metadata = {row["id"]: row for row in metadata["revisions"]}
    if len(revision_metadata) != 2:
        raise RuntimeError("expected exactly two synthetic revisions")
    for row in revision_metadata.values():
        if (row.get("type") != "tar" or row.get("synthetic") is not True or
                row.get("date") is not None or row.get("committer_date") is not None):
            raise RuntimeError("supplement contains a dated or non-synthetic revision")
        numeric_package_version(row.get("message"))

    matches = [row for row in relation_rows if row["origin_id"] == ORIGIN_ID]
    if len(matches) != 2 or {row["candidate_revision_id"] for row in matches} != set(revision_metadata):
        raise RuntimeError("opam origin revision set differs from SWH API evidence")
    for row in matches:
        evidence = revision_metadata[row["candidate_revision_id"]]
        if (row["revision_date"] is not None or row["committer_date"] is not None or
                row["directory"] != evidence["directory"] or
                row["origin_url"] != metadata["origin_url"]):
            raise RuntimeError("date or directory differs from SWH API evidence")
    selected = max(matches, key=lambda row: numeric_package_version(
        revision_metadata[row["candidate_revision_id"]]["message"]))
    result = []
    corrected = 0
    for original in latest_rows:
        if original["origin_id"] == ORIGIN_ID:
            row = dict(selected)
            row["candidate_rank"] = 1
            row["selection_status"] = "ordered_by_package_version_unconfirmed"
            row["ordering_basis"] = "package_version"
            row["package_version"] = revision_metadata[row["candidate_revision_id"]]["message"]
            corrected += 1
        else:
            row = dict(original)
            if row.get("selection_status") != "ordered_unconfirmed":
                raise RuntimeError("unexpected additional undated origin")
            row["ordering_basis"] = "revision_date"
            row["package_version"] = None
        result.append(row)
    if corrected != 1 or len(result) != 10107:
        raise RuntimeError("expected one corrected origin among 10,107")
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rank-root", type=Path, default=RANK_ROOT)
    parser.add_argument("--supplement", type=Path, default=SUPPLEMENT)
    parser.add_argument("--output", type=Path, default=OUTPUT_ROOT)
    args = parser.parse_args()
    latest_input = args.rank_root / "origin_latest_candidates.parquet"
    relation_input = args.rank_root / "revision_origin_with_dates.parquet"
    metadata = json.loads(args.supplement.read_text(encoding="utf-8"))
    latest = pq.read_table(latest_input)
    relations = []
    for batch in pq.ParquetFile(relation_input).iter_batches(batch_size=100000):
        table = pa.Table.from_batches([batch])
        matches = table.filter(pc.equal(table["origin_id"], ORIGIN_ID))
        relations.extend(matches.to_pylist())
    rows = apply_supplement(latest.to_pylist(), relations, metadata)
    schema = pa.schema(list(latest.schema) + [
        ("ordering_basis", pa.string()), ("package_version", pa.string())])
    identity = {
        "version": "route-a-revision-rank-v2",
        "latest_input_sha256": sha256(latest_input),
        "relation_input_sha256": sha256(relation_input),
        "supplement_sha256": sha256(args.supplement),
    }
    args.output.mkdir(parents=True, exist_ok=True)
    done_path = args.output / "complete.json"
    output_path = args.output / "origin_latest_candidates.parquet"
    if done_path.exists():
        done = json.loads(done_path.read_text(encoding="utf-8"))
        if done.get("identity") != identity or not output_path.exists() or sha256(output_path) != done.get("sha256"):
            raise RuntimeError("existing supplement output differs from current inputs")
        print(json.dumps(done, sort_keys=True), flush=True)
        return
    partial = output_path.with_suffix(".parquet.partial")
    pq.write_table(pa.Table.from_pylist(rows, schema=schema), partial, compression="zstd")
    os.replace(str(partial), str(output_path))
    done = {
        "status": "passed",
        "identity": identity,
        "rows": len(rows),
        "corrected_origins": 1,
        "corrected_origin_id": ORIGIN_ID,
        "selected_revision_id": next(row["candidate_revision_id"] for row in rows
                                     if row["origin_id"] == ORIGIN_ID),
        "selected_package_version": "0.1.1",
        "revision_date_status": "unknown_synthetic_tar",
        "sha256": sha256(output_path),
        "completed_at_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
    }
    temporary = done_path.with_suffix(".json.partial")
    temporary.write_text(json.dumps(done, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(str(temporary), str(done_path))
    print(json.dumps(done, sort_keys=True), flush=True)


if __name__ == "__main__":
    main()
