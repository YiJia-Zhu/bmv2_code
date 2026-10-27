#!/usr/bin/env python3
"""Audit release/snapshot neighbors for every expanded SWH candidate directory."""

import argparse
import datetime as dt
import hashlib
import json
import os
import subprocess
import time
from collections import defaultdict
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


GRAPH_ROOT = Path("/mnt/8T/xgr/swh-graph-poc-20260604")
ROUTE_ROOT = GRAPH_ROOT / "route-a-revision-origin-v1"
MERGE = ROUTE_ROOT / "merge"
OUTPUT = ROUTE_ROOT / "directory-snapshot-audit-v1"
READER = ROUTE_ROOT / "build/release/origin"
VERSION = "directory-snapshot-audit-v1"
PART_SIZE = 4096
EDGE_SCHEMA = pa.schema([
    ("directory_node_id", pa.int64()),
    ("target_node_id", pa.int64()),
    ("target_type", pa.int8()),
])


def digest(path):
    hasher = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            block = handle.read(1024 * 1024)
            if not block:
                break
            hasher.update(block)
    return hasher.hexdigest()


def save_json(path, value):
    temporary = path.with_suffix(path.suffix + ".partial")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(str(temporary), str(path))


def load_expected(path, source, target):
    edges = defaultdict(set)
    for batch in pq.ParquetFile(path).iter_batches(batch_size=32768):
        for row in batch.to_pylist():
            edges[row[source]].add(row[target])
    return edges


def query_reader(reader, nodes):
    reader.stdin.write(json.dumps({"op": "neighbors", "nodes": nodes}) + "\n")
    reader.stdin.flush()
    line = reader.stdout.readline()
    if not line:
        raise RuntimeError("graph reader exited; see reader.stderr.log")
    result = json.loads(line)
    if result.get("event") != "neighbors" or [row["node_id"] for row in result["results"]] != nodes:
        raise RuntimeError("graph reader returned the wrong batch")
    return result["results"]


def checked_part(path, marker, identity, expected_nodes):
    if not marker.exists():
        return None
    meta = json.loads(marker.read_text(encoding="utf-8"))
    if (meta.get("identity") != identity or meta.get("directory_nodes") != expected_nodes
            or not path.is_file() or path.stat().st_size != meta["bytes"]
            or digest(path) != meta["sha256"] or pq.read_metadata(path).num_rows != meta["edges"]):
        raise RuntimeError("checkpoint changed: " + str(path))
    return meta


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=OUTPUT)
    parser.add_argument("--max-parts", type=int, default=0,
                        help="stop after N new parts for a bounded smoke run")
    args = parser.parse_args()
    started = time.monotonic()
    args.output.mkdir(parents=True, exist_ok=True)
    parts_root = args.output / "parts"
    parts_root.mkdir(exist_ok=True)
    merge_marker = json.loads((MERGE / "complete.json").read_text(encoding="utf-8"))
    if merge_marker.get("status") != "passed" or merge_marker.get("directory_nodes") != 523435:
        raise RuntimeError("merged directory input is incomplete")
    identity = {"version": VERSION, "merge_complete_sha256": digest(MERGE / "complete.json"),
                "reader_sha256": digest(READER), "part_size": PART_SIZE}
    identity_path = args.output / "identity.json"
    if identity_path.exists() and json.loads(identity_path.read_text(encoding="utf-8")) != identity:
        raise RuntimeError("audit identity changed")
    save_json(identity_path, identity)
    nodes = sorted(pq.read_table(MERGE / "directory_nodes.parquet").column("node_id").to_pylist())
    if len(nodes) != 523435 or len(set(nodes)) != len(nodes):
        raise RuntimeError("invalid directory node set")
    parents = load_expected(MERGE / "parent_edges.parquet", "child_node_id", "parent_node_id")
    revisions = load_expected(MERGE / "directory_revision.parquet", "directory_node_id", "revision_node_id")
    parts = []
    pending = []
    for offset in range(0, len(nodes), PART_SIZE):
        index = offset // PART_SIZE
        segment = nodes[offset:offset + PART_SIZE]
        path = parts_root / "part-{:04d}.parquet".format(index)
        marker = path.with_suffix(".json")
        done = checked_part(path, marker, identity, len(segment))
        if done:
            parts.append(done)
        else:
            if path.exists():
                raise RuntimeError("orphaned checkpoint: " + str(path))
            pending.append((index, segment, path, marker))
    print(json.dumps({"event": "audit_start", "directories": len(nodes),
                      "completed_parts": len(parts), "pending_parts": len(pending),
                      "started_at_utc": dt.datetime.now(dt.timezone.utc).isoformat()}), flush=True)
    if pending:
        stderr_path = args.output / "reader.stderr.log"
        with stderr_path.open("a", encoding="utf-8") as stderr:
            reader = subprocess.Popen([str(READER), str(GRAPH_ROOT)], stdin=subprocess.PIPE,
                                      stdout=subprocess.PIPE, stderr=stderr, text=True, bufsize=1)
            try:
                while True:
                    line = reader.stdout.readline()
                    if not line:
                        raise RuntimeError("graph reader exited before ready")
                    event = json.loads(line)
                    if event.get("event") == "ready":
                        print(json.dumps(event), flush=True)
                        break
                for index, segment, path, marker in pending:
                    rows = []
                    direct_release = 0
                    direct_snapshot = 0
                    for offset in range(0, len(segment), 512):
                        for row in query_reader(reader, segment[offset:offset + 512]):
                            node = row["node_id"]
                            if row["node_type"] != 1 or row["truncated"]:
                                raise RuntimeError("invalid or truncated directory: {}".format(node))
                            typed = defaultdict(set)
                            for item in row["neighbors"]:
                                typed[item["node_type"]].add(item["node_id"])
                            if typed[1] != parents.get(node, set()) or typed[4] != revisions.get(node, set()):
                                raise RuntimeError("saved parent/revision edges disagree: {}".format(node))
                            if any(kind not in (1, 3, 4, 5) for kind in typed):
                                raise RuntimeError("unexpected directory edge type: {}".format(node))
                            for kind in (3, 5):
                                for target in sorted(typed[kind]):
                                    rows.append((node, target, kind))
                                    if kind == 3:
                                        direct_release += 1
                                    else:
                                        direct_snapshot += 1
                    table = pa.Table.from_pylist([
                        {"directory_node_id": a, "target_node_id": b, "target_type": c}
                        for a, b, c in rows], schema=EDGE_SCHEMA)
                    temporary = path.with_suffix(".partial.parquet")
                    pq.write_table(table, temporary, compression="zstd")
                    os.replace(str(temporary), str(path))
                    meta = {"identity": identity, "part": index, "directory_nodes": len(segment),
                            "edges": len(rows), "release_edges": direct_release,
                            "snapshot_edges": direct_snapshot, "bytes": path.stat().st_size,
                            "sha256": digest(path)}
                    save_json(marker, meta)
                    parts.append(meta)
                    elapsed = time.monotonic() - started
                    completed = min((index + 1) * PART_SIZE, len(nodes))
                    print(json.dumps({"event": "audit_progress", "directories": completed,
                                      "total": len(nodes), "parts": len(parts),
                                      "elapsed_seconds": round(elapsed, 1),
                                      "eta_seconds": round(elapsed / completed * (len(nodes) - completed), 1)}),
                          flush=True)
                    if args.max_parts and len(parts) >= args.max_parts:
                        break
            finally:
                if reader.poll() is None:
                    reader.stdin.write('{"op":"quit"}\n')
                    reader.stdin.flush()
                    reader.wait(timeout=30)
                reader.stdin.close()
                reader.stdout.close()
    if len(parts) != (len(nodes) + PART_SIZE - 1) // PART_SIZE:
        print(json.dumps({"event": "audit_paused", "completed_parts": len(parts)}), flush=True)
        return
    summary = {"status": "passed", "directory_nodes": len(nodes),
               "parts": len(parts), "direct_release_edges": sum(x["release_edges"] for x in parts),
               "direct_snapshot_edges": sum(x["snapshot_edges"] for x in parts),
               "elapsed_seconds": round(time.monotonic() - started, 2),
               "completed_at_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
               "identity": identity}
    save_json(args.output / "summary.json", summary)
    print(json.dumps(summary, sort_keys=True), flush=True)


if __name__ == "__main__":
    main()
