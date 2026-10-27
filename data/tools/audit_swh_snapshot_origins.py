#!/usr/bin/env python3
"""Trace audited directory release/snapshot entries to SWH origins."""

import csv
import datetime as dt
import json
import subprocess
import time
from collections import defaultdict, deque

import pyarrow as pa
import pyarrow.parquet as pq

from audit_swh_directory_snapshot_edges import (GRAPH_ROOT, MERGE, OUTPUT,
                                                READER, digest, query_reader, save_json)
from probe_swh_missing_revision_orc import missing_closure


ROUTE_ROOT = GRAPH_ROOT / "route-a-revision-origin-v1"
TRANSIT_TYPES = (3, 4, 5)
MAX_TRANSIT_NODES = 1000000
MAX_TRANSIT_EDGES = 5000000
DIRECT_SCHEMA = pa.schema([
    ("directory_node_id", pa.int64()), ("target_node_id", pa.int64()),
    ("target_type", pa.int8()),
])
TRANSIT_SCHEMA = pa.schema([
    ("source_node_id", pa.int64()), ("target_node_id", pa.int64()),
    ("source_type", pa.int8()), ("target_type", pa.int8()),
])
ENTRY_ORIGIN_SCHEMA = pa.schema([
    ("anchor_directory_node_id", pa.int64()), ("entry_node_id", pa.int64()),
    ("entry_type", pa.int8()), ("origin_node_id", pa.int64()),
    ("transit_path_nodes", pa.list_(pa.int64())),
    ("transit_path_types", pa.list_(pa.int8())),
    ("origin_id", pa.string()), ("origin_url", pa.string()),
])
SEED_ANCHOR_SCHEMA = pa.schema([
    ("seed_directory_id", pa.string()), ("seed_node_id", pa.int64()),
    ("anchor_directory_node_id", pa.int64()), ("directory_hops", pa.int32()),
])
SEED_ORIGIN_SCHEMA = pa.schema([
    ("seed_directory_id", pa.string()), ("seed_node_id", pa.int64()),
    ("anchor_directory_node_id", pa.int64()), ("origin_node_id", pa.int64()),
    ("origin_id", pa.string()), ("origin_url", pa.string()),
    ("directory_hops", pa.int32()), ("mapping_basis", pa.string()),
])


def read_entries():
    summary = json.loads((OUTPUT / "summary.json").read_text(encoding="utf-8"))
    if summary.get("status") != "passed" or summary["directory_nodes"] != 523435:
        raise RuntimeError("directory audit is incomplete")
    rows = []
    for path in sorted((OUTPUT / "parts").glob("part-*.parquet")):
        marker = json.loads(path.with_suffix(".json").read_text(encoding="utf-8"))
        if marker["identity"] != summary["identity"] or digest(path) != marker["sha256"]:
            raise RuntimeError("entry checkpoint mismatch")
        rows.extend(pq.read_table(path).to_pylist())
    if len(rows) != summary["direct_release_edges"] + summary["direct_snapshot_edges"]:
        raise RuntimeError("entry count mismatch")
    return rows


def traverse(entries):
    entry_nodes = {row["target_node_id"] for row in entries}
    discovered = set(entry_nodes)
    pending = set(entry_nodes)
    visited = set()
    adjacency = {}
    node_types = {row["target_node_id"]: row["target_type"] for row in entries}
    edge_rows = []
    with (OUTPUT / "transit-reader.stderr.log").open("a", encoding="utf-8") as stderr:
        reader = subprocess.Popen([str(READER), str(GRAPH_ROOT)],
                                  stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                  stderr=stderr, text=True, bufsize=1)
        try:
            while True:
                line = reader.stdout.readline()
                if not line:
                    raise RuntimeError("graph reader exited before ready")
                event = json.loads(line)
                if event.get("event") == "ready":
                    print(json.dumps(event), flush=True)
                    break
            while pending:
                current = sorted(pending)
                pending = set()
                if len(visited) + len(current) > MAX_TRANSIT_NODES:
                    raise RuntimeError("transit node cap exceeded")
                for offset in range(0, len(current), 512):
                    for row in query_reader(reader, current[offset:offset + 512]):
                        node = row["node_id"]
                        kind = row["node_type"]
                        if kind not in TRANSIT_TYPES or row["truncated"]:
                            raise RuntimeError("invalid or truncated transit node: {}".format(node))
                        if node_types[node] != kind:
                            raise RuntimeError("inconsistent transit node type")
                        neighbors = []
                        for item in row["neighbors"]:
                            target = item["node_id"]
                            target_type = item["node_type"]
                            if target_type not in TRANSIT_TYPES + (2,):
                                raise RuntimeError("unexpected transit edge type")
                            neighbors.append((target, target_type))
                            edge_rows.append((node, target, kind, target_type))
                            old_type = node_types.setdefault(target, target_type)
                            if old_type != target_type:
                                raise RuntimeError("inconsistent target node type")
                            if target_type in TRANSIT_TYPES and target not in discovered:
                                discovered.add(target)
                                pending.add(target)
                        adjacency[node] = neighbors
                        visited.add(node)
                    if len(edge_rows) > MAX_TRANSIT_EDGES:
                        raise RuntimeError("transit edge cap exceeded")
                print(json.dumps({"event": "transit_progress", "visited": len(visited),
                                  "pending": len(pending), "edges": len(edge_rows)}), flush=True)
        finally:
            if reader.poll() is None:
                reader.stdin.write('{"op":"quit"}\n')
                reader.stdin.flush()
                reader.wait(timeout=30)
            reader.stdin.close()
            reader.stdout.close()
    if visited != discovered:
        raise RuntimeError("transit traversal incomplete")
    return adjacency, node_types, edge_rows


def entry_origins(entries, adjacency, node_types):
    paths_by_entry = {}
    result = []
    for entry in sorted({(row["target_node_id"], row["target_type"]) for row in entries}):
        start, _kind = entry
        queue = deque([start])
        previous = {start: None}
        while queue:
            source = queue.popleft()
            for target, target_kind in sorted(adjacency.get(source, ())):
                if target in previous:
                    continue
                previous[target] = source
                if target_kind in TRANSIT_TYPES:
                    queue.append(target)
        paths = []
        for origin in sorted(node for node in previous if node_types[node] == 2):
            path = []
            current = origin
            while current is not None:
                path.append(current)
                current = previous[current]
            path.reverse()
            paths.append((origin, path, [node_types[node] for node in path]))
        paths_by_entry[entry] = paths
    for row in entries:
        for origin, path, types in paths_by_entry[(row["target_node_id"], row["target_type"])]:
            result.append({"anchor_directory_node_id": row["directory_node_id"],
                           "entry_node_id": row["target_node_id"],
                           "entry_type": row["target_type"], "origin_node_id": origin,
                           "transit_path_nodes": path, "transit_path_types": types,
                           "origin_id": None, "origin_url": None})
    return result


def seed_anchors(entries):
    children = defaultdict(set)
    for batch in pq.ParquetFile(MERGE / "parent_edges.parquet").iter_batches(batch_size=32768):
        for row in batch.to_pylist():
            children[row["parent_node_id"]].add(row["child_node_id"])
    seeds = {row["seed_node_id"]: row["seed_directory_id"] for row in
             pq.read_table(MERGE / "seed_nodes.parquet").to_pylist()}
    anchors = sorted({row["directory_node_id"] for row in entries})
    result = []
    for index, anchor in enumerate(anchors):
        queue = deque([(anchor, 0)])
        seen = {anchor}
        while queue:
            node, hops = queue.popleft()
            if node in seeds:
                result.append({"seed_directory_id": seeds[node], "seed_node_id": node,
                               "anchor_directory_node_id": anchor, "directory_hops": hops})
            for child in children.get(node, ()):
                if child not in seen:
                    seen.add(child)
                    queue.append((child, hops + 1))
        if index == 0 or (index + 1) % 50 == 0:
            print(json.dumps({"event": "seed_progress", "anchors": index + 1,
                              "total": len(anchors), "relations": len(result)}), flush=True)
    return result


def known_urls():
    lookup = {}
    for row in pq.read_table(ROUTE_ROOT / "full/results/origin_urls.parquet").to_pylist():
        if row["status"] != "mapped":
            raise RuntimeError("historic URL is unresolved")
        lookup[row["origin_node_id"]] = (row["origin_id"], row["origin_url"])
    with (ROUTE_ROOT / "nonrevision-origin-v1/origin_urls.csv").open(
            encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            node = int(row["origin_node_id"])
            value = (row["origin_id"], row["origin_url"])
            if node in lookup and lookup[node] != value:
                raise RuntimeError("historic URL conflict")
            lookup[node] = value
    return lookup


def save_table(path, rows, schema):
    temporary = path.with_suffix(".partial.parquet")
    pq.write_table(pa.Table.from_pylist(rows, schema=schema), temporary, compression="zstd")
    temporary.replace(path)


def main():
    started = time.monotonic()
    entries = read_entries()
    _missing, old_closure, _roots = missing_closure()
    old_edges = [row for row in entries if row["directory_node_id"] in old_closure]
    if len(old_edges) != 310:
        raise RuntimeError("337-seed closure no longer has 310 direct entries")
    adjacency, node_types, transit = traverse(entries)
    entry_rows = entry_origins(entries, adjacency, node_types)
    seed_rows = seed_anchors(entries)
    urls = known_urls()
    new_origins = sorted({row["origin_node_id"] for row in entry_rows} - set(urls))
    for row in entry_rows:
        if row["origin_node_id"] in urls:
            row["origin_id"], row["origin_url"] = urls[row["origin_node_id"]]
    by_anchor = defaultdict(list)
    for row in entry_rows:
        by_anchor[row["anchor_directory_node_id"]].append(row)
    pairs = {}
    for row in seed_rows:
        for entry in by_anchor.get(row["anchor_directory_node_id"], ()):
            key = (row["seed_directory_id"], entry["origin_node_id"])
            candidate = {"seed_directory_id": row["seed_directory_id"],
                         "seed_node_id": row["seed_node_id"],
                         "anchor_directory_node_id": row["anchor_directory_node_id"],
                         "origin_node_id": entry["origin_node_id"],
                         "origin_id": entry["origin_id"], "origin_url": entry["origin_url"],
                         "directory_hops": row["directory_hops"],
                         "mapping_basis": "directory_release_or_snapshot"}
            if key not in pairs or candidate["directory_hops"] < pairs[key]["directory_hops"]:
                pairs[key] = candidate
    old_pairs = {(row["seed_directory_id"], row["origin_node_id"]) for row in
                 pq.read_table(ROUTE_ROOT / "nonrevision-origin-v1/seed_origin.parquet").to_pylist()}
    if not old_pairs.issubset(pairs):
        raise RuntimeError("full audit lost earlier 337-seed evidence")
    save_table(OUTPUT / "direct_edges.parquet", entries, DIRECT_SCHEMA)
    save_table(OUTPUT / "transit_edges.parquet", [
        {"source_node_id": a, "target_node_id": b, "source_type": c, "target_type": d}
        for a, b, c, d in sorted(set(transit))], TRANSIT_SCHEMA)
    save_table(OUTPUT / "entry_origin.parquet", entry_rows, ENTRY_ORIGIN_SCHEMA)
    save_table(OUTPUT / "seed_anchor.parquet", seed_rows, SEED_ANCHOR_SCHEMA)
    save_table(OUTPUT / "seed_origin.parquet", list(pairs.values()), SEED_ORIGIN_SCHEMA)
    summary = {"status": "graph_passed" if new_origins else "passed",
               "directory_nodes": 523435,
               "direct_release_edges": sum(row["target_type"] == 3 for row in entries),
               "direct_snapshot_edges": sum(row["target_type"] == 5 for row in entries),
               "old_closure_edges": len(old_edges), "new_direct_edges": len(entries) - len(old_edges),
               "entry_nodes": len({row["target_node_id"] for row in entries}),
               "transit_nodes": len(adjacency), "transit_edges": len(set(transit)),
               "origin_nodes": len({row["origin_node_id"] for row in entry_rows}),
               "new_origin_nodes": new_origins,
               "entry_origin_rows": len(entry_rows), "seed_anchor_rows": len(seed_rows),
               "seed_origin_pairs": len(pairs),
               "mapped_seeds": len({row["seed_directory_id"] for row in pairs.values()}),
               "old_seed_origin_pairs_verified": len(old_pairs),
               "elapsed_seconds": round(time.monotonic() - started, 2),
               "completed_at_utc": dt.datetime.now(dt.timezone.utc).isoformat()}
    save_json(OUTPUT / "origin-summary.json", summary)
    print(json.dumps(summary, sort_keys=True), flush=True)


if __name__ == "__main__":
    main()
