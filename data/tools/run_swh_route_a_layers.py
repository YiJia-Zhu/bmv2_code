#!/usr/bin/env python3
"""Extend the verified local reverse graph from D1 through D8.

Traversal uses graph node IDs and makes no network requests. Directory hashes
already verified in D0/D1 remain available through known_directory_ids.parquet.
Each committed layer publishes an archive request for the signed S3 uploader.
Revision neighbors are cached as evidence; revision/origin mapping is separate.
"""

import argparse
import datetime as dt
import fcntl
import json
import os
from pathlib import Path
import queue
import resource
import signal
import subprocess
import sys
import threading
import time

import pyarrow as pa
import pyarrow.parquet as pq

from run_swh_route_a_full import Checkpoints, chunks, digest_json, put_parquet
from run_swh_route_a_sample import sha256, utc_now, write_json


VERSION = "route-a-directory-layers-v1"
NODES = 58675740295
NEIGHBOR_SCHEMA = pa.schema([
    ("node_id", pa.int64()), ("parent_nodes", pa.list_(pa.int64())),
    ("revision_nodes", pa.list_(pa.int64())), ("outdegree", pa.int64()),
    ("neighbor_type_counts", pa.list_(pa.int64())),
])
NODE_SCHEMA = pa.schema([("node_id", pa.int64())])
PAIR_SCHEMA = pa.schema([("child_node_id", pa.int64()), ("parent_node_id", pa.int64())])
REV_SCHEMA = pa.schema([("directory_node_id", pa.int64()), ("revision_node_id", pa.int64())])


def next_frontier(parents, expanded, current):
    """Deduplicate query work while retaining edges separately."""
    return sorted(set(parents) - set(expanded) - set(current))


def validate_neighbors(nodes, rows):
    if [row["node_id"] for row in rows] != nodes:
        raise RuntimeError("Reader batch membership mismatch")
    for row in rows:
        if row["node_type"] != 1:
            raise RuntimeError("Reader input is not a directory")
        counts = row["neighbor_type_counts"]
        if len(counts) != 6 or sum(counts) != row["outdegree"]:
            raise RuntimeError("Neighbor degree mismatch")
        for field, kind in (("parent_nodes", 1), ("revision_nodes", 4)):
            values = row[field]
            if values != sorted(set(values)) or any(node < 0 or node >= NODES for node in values):
                raise RuntimeError("Invalid or duplicate graph neighbors")
            if len(values) > counts[kind]:
                raise RuntimeError("Neighbor type count mismatch")


def layer_files_valid(root, summary, identity, inputs):
    if summary["run_identity"] != identity or summary["input_sha256"] != digest_json(inputs):
        raise RuntimeError("Completed layer has incompatible inputs or implementation")
    for entry in summary["files"]:
        path = root / entry["path"]
        if not path.is_file() or path.stat().st_size != entry["bytes"] or sha256(path) != entry["sha256"]:
            raise RuntimeError("Completed layer file is missing or corrupt: " + str(path))
        if pq.read_metadata(path).num_rows != entry["rows"]:
            raise RuntimeError("Completed layer row count mismatch")


class LayerRunner:
    def __init__(self, args):
        self.args = args
        self.output = args.output
        self.output.mkdir(parents=True, exist_ok=True)
        self.run_lock = (self.output / "run.lock").open("a")
        fcntl.flock(self.run_lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        self.started = time.monotonic()
        self.reader = None
        self.stderr = None
        self.messages = queue.Queue()
        self.stop = threading.Event()
        self.state = {"version": VERSION, "started_at_utc": utc_now(), "phase": "preparing",
                      "max_depth": args.max_depth, "aws_compute_jobs": 0,
                      "network_requests_by_traversal": 0,
                      "s3_prefix": args.s3_prefix, "pid": os.getpid()}
        control = self.output / "control.json"
        if control.exists():
            self.control = json.loads(control.read_text())
        else:
            now = dt.datetime.now(dt.timezone.utc)
            self.control = {"first_started_at_utc": now.isoformat(),
                            "deadline_utc": (now + dt.timedelta(hours=args.max_hours)).isoformat(),
                            "max_hours": args.max_hours, "max_depth": args.max_depth,
                            "scope": "Local directory traversal D1 to D8; no revision/origin expansion"}
            write_json(control, self.control)
        self.deadline = dt.datetime.fromisoformat(self.control["deadline_utc"])
        self.state.update(first_started_at_utc=self.control["first_started_at_utc"],
                          deadline_utc=self.control["deadline_utc"])

    def check(self):
        if self.stop.is_set():
            raise InterruptedError("Stop requested; committed batches are retained")
        if dt.datetime.now(dt.timezone.utc) >= self.deadline:
            raise TimeoutError("Original directory run deadline reached; checkpoints are retained")

    def emit(self, event, **fields):
        value = dict(fields, event=event, timestamp_utc=utc_now(),
                     elapsed_seconds=round(time.monotonic() - self.started, 3))
        self.state.update(value)
        line = json.dumps(value, sort_keys=True)
        print(line, flush=True)
        with (self.output / "events.jsonl").open("a", encoding="utf-8") as handle:
            handle.write(line + "\n")
        write_json(self.output / "status.json", self.state)

    def prepare(self):
        self.check()
        baseline = self.args.baseline
        complete = json.loads((baseline / "complete.json").read_text())
        comparison = json.loads((baseline / "comparison.json").read_text())
        original = json.loads((baseline / "identity.json").read_text())
        archive = json.loads((baseline / "archive.json").read_text())
        if (complete["status"] != comparison["status"] or complete["status"] != "passed"
                or complete["d0"] != 65321 or complete["new_d1_a"] != 114293
                or complete["unresolved"] != 0 or archive["status"] != "passed"
                or complete["identity"] != digest_json(original)):
            raise RuntimeError("D0/D1 baseline has not passed verification and archival")
        for name, metadata in original["graph_files"].items():
            path = self.args.graph_dir / name
            current = path.stat()
            if (current.st_size != metadata["size"] or current.st_mtime_ns != metadata["mtime_ns"]
                    or Path(str(path) + ".aria2").exists()):
                raise RuntimeError("Local graph changed: " + name)
        self.binary = self.args.graph_dir / "route-a-d0-d1-v1/build/release/swh-p4-graph-reader"
        if sha256(self.binary) != original["reader_sha256"]:
            raise RuntimeError("Reader differs from verified D1 reader")
        if sha256(self.args.graph_dir / "graph-transposed.properties") != original["properties_sha256"]:
            raise RuntimeError("Graph properties changed")
        source_names = ["complete.json", "comparison.json", "identity.json",
                        "results/d0_node_mapping.parquet", "results/parent_node_mapping.parquet",
                        "results/frontier_d1.parquet", "results/parent_pairs.parquet"]
        archived_files = {entry["key"].split("/d0-d1-v1/", 1)[-1]: entry for entry in archive["objects"]}
        source_hashes = {}
        for name in source_names:
            source_hashes[name] = sha256(baseline / name)
            if name in archived_files and source_hashes[name] != archived_files[name]["sha256"]:
                raise RuntimeError("D1 input changed since archive verification: " + name)
        implementation = {}
        for name in (Path(__file__).name, "run_swh_route_a_full.py", "run_swh_route_a_sample.py"):
            implementation[name] = sha256(Path(__file__).with_name(name))
        identity = {"version": VERSION, "source_identity": complete["identity"],
                    "source_sha256": source_hashes, "graph_release": "2026-06-04",
                    "graph_files": original["graph_files"], "reader_sha256": original["reader_sha256"],
                    "implementation_sha256": implementation, "max_depth": self.args.max_depth,
                    "batch_size": self.args.batch_size, "s3_prefix": self.args.s3_prefix,
                    "id_representation": "node_id in the pinned graph; directory hashes deferred"}
        target = self.output / "identity.json"
        if target.exists() and json.loads(target.read_text()) != identity:
            raise RuntimeError("Run identity changed; refusing incompatible checkpoint reuse")
        write_json(target, identity)
        self.identity = digest_json(identity)
        self.checkpoints = Checkpoints(self.output / "shards", self.identity)
        d0 = pq.read_table(baseline / "results/d0_node_mapping.parquet").to_pylist()
        parents = pq.read_table(baseline / "results/parent_node_mapping.parquet").to_pylist()
        known = {}
        for row in d0 + parents:
            node, directory = row["node_id"], row["directory_id"]
            if node in known and known[node] != directory:
                raise RuntimeError("Known directory node collision")
            known[node] = directory
        self.d0_nodes = {row["node_id"] for row in d0}
        parent_nodes = {row["node_id"] for row in parents}
        self.frontier = sorted(parent_nodes - self.d0_nodes)
        hashes = {row["directory_id"] for row in
                  pq.read_table(baseline / "results/frontier_d1.parquet").to_pylist()}
        if (len(self.d0_nodes) != 65321 or len(self.frontier) != 114293
                or {known[node] for node in self.frontier} != hashes):
            raise RuntimeError("D1 frontier and node mapping disagree")
        put_parquet(self.output / "known_directory_ids.parquet",
                    [{"node_id": node, "directory_id": known[node]} for node in sorted(known)],
                    pa.schema([("node_id", pa.int64()), ("directory_id", pa.string())]))
        self.depths = {node: 0 for node in self.d0_nodes}
        self.depths.update((node, 1) for node in self.frontier)
        self.expanded = set(self.d0_nodes)
        self.emit("preflight_complete", d0=len(self.d0_nodes), frontier_d1=len(self.frontier),
                  known_directory_hashes=len(known), run_identity=self.identity,
                  directory_ids_deferred=True)

    def receive(self, expected):
        waited = time.monotonic()
        while True:
            self.check()
            try:
                line = self.messages.get(timeout=5)
            except queue.Empty:
                if time.monotonic() - waited >= 30:
                    self.emit("reader_wait", expected=expected, wait_seconds=round(time.monotonic() - waited, 1))
                    waited = time.monotonic()
                continue
            if line is None:
                raise RuntimeError("Graph reader exited; inspect reader.stderr.log")
            value = json.loads(line)
            if value["event"] == expected:
                return value
            self.emit("reader_event", reader_event=value["event"])

    def start_reader(self):
        if self.reader is not None:
            return
        self.stderr = (self.output / "reader.stderr.log").open("a", encoding="utf-8")
        self.reader = subprocess.Popen([str(self.binary), str(self.args.graph_dir)],
                                       stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                       stderr=self.stderr, text=True, bufsize=1)

        def pump():
            try:
                for line in self.reader.stdout:
                    self.messages.put(line)
            finally:
                self.messages.put(None)

        threading.Thread(target=pump, daemon=True).start()
        ready = self.receive("ready")
        if ready["nodes"] != NODES:
            raise RuntimeError("Unexpected graph node count")
        self.emit("reader_ready", nodes=ready["nodes"], load_seconds=ready["load_seconds"])

    def query(self, nodes):
        self.start_reader()
        self.reader.stdin.write(json.dumps({"op": "neighbors", "nodes": nodes}) + "\n")
        self.reader.stdin.flush()
        rows = self.receive("neighbors")["results"]
        validate_neighbors(nodes, rows)
        return [{name: row[name] for name in NEIGHBOR_SCHEMA.names} for row in rows]

    def archive_request(self, layer_root, summary):
        paths = [layer_root / row["path"] for row in summary["files"]]
        paths += [self.output / "identity.json", self.output / "control.json",
                  self.output / "known_directory_ids.parquet", layer_root / "complete.json"]
        entries = [{"path": str(path.relative_to(self.output)), "bytes": path.stat().st_size,
                    "sha256": sha256(path), "key": self.args.s3_prefix + str(path.relative_to(self.output))}
                   for path in paths]
        write_json(layer_root / "archive-request.json", {
            "status": "pending", "run_identity": self.identity, "bucket": "swh2p4",
            "prefix": self.args.s3_prefix, "layer": summary["layer"], "objects": entries,
            "note": "Compute completion and S3 archive completion are separate."})

    def layer(self, depth, frontier):
        self.check()
        root = self.output / "layers" / "d{:02d}".format(depth)
        root.mkdir(parents=True, exist_ok=True)
        marker = root / "complete.json"
        if marker.exists():
            summary = json.loads(marker.read_text())
            layer_files_valid(root, summary, self.identity, frontier)
            output = pq.read_table(root / "frontier.parquet").column("node_id").to_pylist()
            self.archive_request(root, summary)
            self.emit("layer_resumed", layer=depth, input_nodes=len(frontier), next_frontier=len(output))
            return output
        self.state.update(phase="directory_traversal", layer=depth)
        self.emit("layer_started", layer=depth, input_nodes=len(frontier),
                  expanded_nodes=len(self.expanded), directory_depth=depth)
        started = time.monotonic()
        parent_nodes, without = set(), []
        completed = resumed = parent_edges = revision_edges = 0
        edge_path = root / "parent_edges.parquet"
        rev_path = root / "revision_neighbors.parquet"
        edge_tmp, rev_tmp = edge_path.with_suffix(".partial"), rev_path.with_suffix(".partial")
        try:
            with pq.ParquetWriter(edge_tmp, PAIR_SCHEMA, compression="zstd") as edge_writer, \
                    pq.ParquetWriter(rev_tmp, REV_SCHEMA, compression="zstd") as rev_writer:
                for number, nodes in chunks(frontier, self.args.batch_size):
                    self.check()
                    rows = self.checkpoints.load("d{:02d}".format(depth), number, nodes)
                    if rows is None:
                        rows = self.query(nodes)
                        self.checkpoints.save("d{:02d}".format(depth), number, nodes, rows, NEIGHBOR_SCHEMA)
                    else:
                        resumed += len(rows)
                    if [row["node_id"] for row in rows] != nodes:
                        raise RuntimeError("Neighbor checkpoint membership mismatch")
                    edges, revs = [], []
                    for row in rows:
                        node = row["node_id"]
                        parent_nodes.update(row["parent_nodes"])
                        if not row["parent_nodes"]:
                            without.append(node)
                        edges.extend({"child_node_id": node, "parent_node_id": parent}
                                     for parent in row["parent_nodes"])
                        revs.extend({"directory_node_id": node, "revision_node_id": revision}
                                    for revision in row["revision_nodes"])
                    edge_writer.write_table(pa.Table.from_pylist(edges, schema=PAIR_SCHEMA))
                    rev_writer.write_table(pa.Table.from_pylist(revs, schema=REV_SCHEMA))
                    completed += len(rows)
                    parent_edges += len(edges)
                    revision_edges += len(revs)
                    seconds = time.monotonic() - started
                    rate = (completed - resumed) / seconds if seconds else 0
                    self.emit("progress", layer=depth, completed=completed, total=len(frontier),
                              percent=round(completed * 100 / len(frontier), 3) if frontier else 100,
                              nodes_per_second=round(rate, 2), resumed_nodes=resumed,
                              eta_seconds=round((len(frontier) - completed) / rate, 1) if rate else None,
                              parent_edges=parent_edges, revision_neighbors=revision_edges,
                              unique_parent_nodes=len(parent_nodes), layer_elapsed_seconds=round(seconds, 2))
            self.check()
            os.replace(str(edge_tmp), str(edge_path))
            os.replace(str(rev_tmp), str(rev_path))
            output = next_frontier(parent_nodes, self.expanded, frontier)
            put_parquet(root / "frontier.parquet", [{"node_id": node} for node in output], NODE_SCHEMA)
            put_parquet(root / "no_directory_parent.parquet",
                        [{"node_id": node} for node in sorted(without)], NODE_SCHEMA)
            files = []
            for path in (edge_path, rev_path, root / "frontier.parquet", root / "no_directory_parent.parquet"):
                files.append({"path": path.name, "bytes": path.stat().st_size,
                              "sha256": sha256(path), "rows": pq.read_metadata(path).num_rows})
            summary = {"version": VERSION, "status": "passed", "run_identity": self.identity,
                       "layer": depth, "expanded_directory_depth": depth - 1,
                       "input_sha256": digest_json(frontier), "input_nodes": len(frontier),
                       "next_frontier_nodes": len(output), "unique_parent_nodes": len(parent_nodes),
                       "parent_edges": parent_edges, "revision_neighbors": revision_edges,
                       "without_directory_parent": len(without), "resumed_nodes": resumed,
                       "completed_at_utc": utc_now(), "elapsed_seconds": round(time.monotonic() - started, 3),
                       "files": files, "id_representation": "graph node IDs, not SHA1 directory IDs",
                       "seed_path_policy": "Retain all edges; reconstruct seed paths later with per-seed depth <=8"}
            write_json(marker, summary)
            self.archive_request(root, summary)
            self.emit("layer_complete", **{key: summary[key] for key in
                      ("layer", "input_nodes", "next_frontier_nodes", "parent_edges", "revision_neighbors", "elapsed_seconds")})
            return output
        except BaseException:
            # Only batch checkpoints and complete layer markers can be resumed.
            for path in (edge_tmp, rev_tmp):
                if path.exists():
                    path.unlink()
            raise

    def run(self):
        self.prepare()
        frontier = self.frontier
        for depth in range(2, self.args.max_depth + 1):
            output = self.layer(depth, frontier)
            self.expanded.update(frontier)
            for node in output:
                if node in self.depths:
                    raise RuntimeError("Visited node was scheduled as a new frontier")
                self.depths[node] = depth
            frontier = output
        self.check()
        path = self.output / "all_directory_nodes.parquet"
        put_parquet(path, [{"node_id": node, "minimum_depth": self.depths[node]}
                           for node in sorted(self.depths)],
                    pa.schema([("node_id", pa.int64()), ("minimum_depth", pa.int32())]))
        summary = {"version": VERSION, "status": "passed", "run_identity": self.identity,
                   "completed_at_utc": utc_now(), "max_depth": self.args.max_depth,
                   "unique_directory_nodes": len(self.depths), "expanded_nodes": len(self.expanded),
                   "boundary_nodes": len(frontier), "depth_limit_reached": bool(frontier),
                   "all_directory_nodes_sha256": sha256(path), "aws_compute_jobs": 0,
                   "network_requests_by_traversal": 0, "revision_origin_mapping_completed": False,
                   "archive_status": "See per-layer archive.json; pending uploads do not invalidate local computation",
                   "elapsed_seconds_this_run": round(time.monotonic() - self.started, 3),
                   "reader_peak_rss_kib": resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss,
                   "scope": "D0/D1 baseline plus local BFS to D8; no claim of complete repository coverage"}
        write_json(self.output / "complete.json", summary)
        self.state["phase"] = "complete"
        self.emit("run_complete", **summary)

    def close(self):
        self.stop.set()
        if self.reader is not None:
            if self.reader.poll() is None:
                self.reader.terminate()
            try:
                self.reader.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.reader.kill()
                self.reader.wait(timeout=5)
        if self.stderr is not None:
            self.stderr.close()
        self.state["reader_peak_rss_kib"] = resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss
        write_json(self.output / "status.json", self.state)
        self.run_lock.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    graph = Path("/mnt/8T/xgr/swh-graph-poc-20260604")
    parser.add_argument("--graph-dir", type=Path, default=graph)
    parser.add_argument("--baseline", type=Path, default=graph / "route-a-d0-d1-v1/full-d0-d1-v1")
    parser.add_argument("--output", type=Path, default=graph / "route-a-d2-d8-v1")
    parser.add_argument("--batch-size", type=int, default=512)
    parser.add_argument("--max-depth", type=int, default=8)
    parser.add_argument("--max-hours", type=float, default=10)
    parser.add_argument("--s3-prefix", default="new-account-588301174757/route-a/d2-d8-v1/")
    args = parser.parse_args()
    if not 2 <= args.max_depth <= 8 or not 1 <= args.batch_size <= 1024 or not 0 < args.max_hours <= 10:
        parser.error("Allowed: depth 2..8, batch size 1..1024, duration >0..10h")
    if not args.s3_prefix.startswith("new-account-588301174757/route-a/") or not args.s3_prefix.endswith("/"):
        parser.error("S3 output must use the approved account prefix")
    runner = LayerRunner(args)
    for signum in (signal.SIGINT, signal.SIGTERM):
        signal.signal(signum, lambda _signum, _frame: runner.stop.set())
    try:
        runner.run()
        return 0
    except (Exception, KeyboardInterrupt) as error:
        stopped = isinstance(error, (InterruptedError, TimeoutError, KeyboardInterrupt))
        runner.state["phase"] = "stopped" if stopped else "failed"
        runner.emit("run_stopped", error_type=type(error).__name__, error=str(error))
        return 2 if stopped else 1
    finally:
        runner.close()


if __name__ == "__main__":
    sys.exit(main())
