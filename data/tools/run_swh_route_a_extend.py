#!/usr/bin/env python3
"""Continue verified directory BFS from a completed depth to D16.

The earlier run and its checkpoint identity remain immutable. This extension
imports its complete visited set and expands only the saved boundary frontier.
"""

import argparse
import json
from pathlib import Path
import resource
import signal
import sys
import time

import pyarrow as pa
import pyarrow.parquet as pq

from run_swh_route_a_full import Checkpoints, digest_json, put_parquet
from run_swh_route_a_layers import LayerRunner, NODES
from run_swh_route_a_sample import sha256, utc_now, write_json


VERSION = "route-a-directory-extension-v1"


def source_state(source, complete):
    depth = complete["max_depth"]
    rows = pq.read_table(source / "all_directory_nodes.parquet").to_pylist()
    depths = {row["node_id"]: row["minimum_depth"] for row in rows}
    if (len(depths) != len(rows) or len(depths) != complete["unique_directory_nodes"]
            or any(node < 0 or node >= NODES or level < 0 or level > depth
                   for node, level in depths.items())):
        raise RuntimeError("Invalid source directory discovery table")
    boundary = pq.read_table(source / "layers" / "d{:02d}".format(depth) / "frontier.parquet").column("node_id").to_pylist()
    frontier = sorted(set(boundary))
    if (len(frontier) != len(boundary) or len(frontier) != complete["boundary_nodes"]
            or set(frontier) != {node for node, level in depths.items() if level == depth}):
        raise RuntimeError("Source boundary and discovery depths disagree")
    expanded = set(depths) - set(frontier)
    if len(expanded) != complete["expanded_nodes"]:
        raise RuntimeError("Source expanded-node count mismatch")
    return depths, expanded, frontier


class ExtensionRunner(LayerRunner):
    def __init__(self, args):
        super().__init__(args)
        scope = "Extend a verified directory boundary to D{}; no revision/origin expansion".format(args.max_depth)
        if self.control["scope"] != scope:
            self.control["scope"] = scope
            write_json(self.output / "control.json", self.control)
        self.state["version"] = VERSION

    def prepare(self):
        self.check()
        source = self.args.source
        complete = json.loads((source / "complete.json").read_text())
        prior = json.loads((source / "identity.json").read_text())
        archive = json.loads((source / "archive.json").read_text())
        if (complete["status"] != "passed" or archive["status"] != "passed"
                or complete["run_identity"] != digest_json(prior)
                or not complete["max_depth"] < self.args.max_depth):
            raise RuntimeError("Source traversal is incomplete, unarchived, or has incompatible depth")
        source_prefix = archive["prefix"]
        for entry in archive["objects"]:
            if not entry["key"].startswith(source_prefix):
                raise RuntimeError("Source archive key has an unexpected prefix")
            path = source / entry["key"][len(source_prefix):]
            if not path.is_file() or path.stat().st_size != entry["bytes"] or sha256(path) != entry["sha256"]:
                raise RuntimeError("Source archive input changed: " + str(path))
        if sha256(source / "all_directory_nodes.parquet") != complete["all_directory_nodes_sha256"]:
            raise RuntimeError("Source directory table SHA256 mismatch")
        for name, metadata in prior["graph_files"].items():
            path = self.args.graph_dir / name
            stat = path.stat()
            if (stat.st_size != metadata["size"] or stat.st_mtime_ns != metadata["mtime_ns"]
                    or Path(str(path) + ".aria2").exists()):
                raise RuntimeError("Graph changed since source run: " + name)
        self.binary = self.args.graph_dir / "route-a-d0-d1-v1/build/release/swh-p4-graph-reader"
        if sha256(self.binary) != prior["reader_sha256"]:
            raise RuntimeError("Reader changed since source run")
        original = json.loads((self.args.baseline / "identity.json").read_text())
        if sha256(self.args.graph_dir / "graph-transposed.properties") != original["properties_sha256"]:
            raise RuntimeError("Graph properties changed")
        for name, expected in prior["implementation_sha256"].items():
            if sha256(Path(__file__).with_name(name)) != expected:
                raise RuntimeError("Source implementation changed: " + name)
        names = (Path(__file__).name, "run_swh_route_a_layers.py", "run_swh_route_a_full.py", "run_swh_route_a_sample.py")
        identity = {"version": VERSION, "source_run_identity": complete["run_identity"],
                    "source_root": str(source.resolve()), "source_max_depth": complete["max_depth"],
                    "source_sha256": {name: sha256(source / name) for name in
                                      ("complete.json", "identity.json", "archive.json", "all_directory_nodes.parquet")},
                    "graph_release": prior["graph_release"], "graph_files": prior["graph_files"],
                    "reader_sha256": prior["reader_sha256"],
                    "implementation_sha256": {name: sha256(Path(__file__).with_name(name)) for name in names},
                    "max_depth": self.args.max_depth, "batch_size": self.args.batch_size,
                    "s3_prefix": self.args.s3_prefix, "id_representation": prior["id_representation"]}
        path = self.output / "identity.json"
        if path.exists() and json.loads(path.read_text()) != identity:
            raise RuntimeError("Extension identity changed; refusing incompatible checkpoint reuse")
        write_json(path, identity)
        self.identity = digest_json(identity)
        self.checkpoints = Checkpoints(self.output / "shards", self.identity)
        self.source_complete = complete
        self.source_depth = complete["max_depth"]
        self.depths, self.expanded, self.frontier = source_state(source, complete)
        known = pq.read_table(source / "known_directory_ids.parquet")
        put_parquet(self.output / "known_directory_ids.parquet", known.to_pylist(), known.schema)
        self.emit("preflight_complete", source_depth=self.source_depth,
                  source_unique_directory_nodes=len(self.depths), input_boundary_nodes=len(self.frontier),
                  already_expanded_nodes=len(self.expanded), run_identity=self.identity)

    def archive_request(self, layer_root, summary):
        # The inherited file format is shared; the path limit follows this run.
        summary["seed_path_policy"] = "Retain all edges; reconstruct seed paths later with per-seed depth <={}".format(self.args.max_depth)
        write_json(layer_root / "complete.json", summary)
        super().archive_request(layer_root, summary)

    def run(self):
        self.prepare()
        frontier = self.frontier
        for depth in range(self.source_depth + 1, self.args.max_depth + 1):
            output = self.layer(depth, frontier)
            self.expanded.update(frontier)
            for node in output:
                if node in self.depths:
                    raise RuntimeError("Visited directory incorrectly scheduled as new frontier")
                self.depths[node] = depth
            frontier = output
        self.check()
        path = self.output / "all_directory_nodes.parquet"
        put_parquet(path, [{"node_id": node, "minimum_depth": self.depths[node]} for node in sorted(self.depths)],
                    pa.schema([("node_id", pa.int64()), ("minimum_depth", pa.int32())]))
        # Obtain the reader's final process statistics before writing completion.
        if self.reader is not None:
            if self.reader.poll() is None:
                self.reader.terminate()
            self.reader.wait(timeout=10)
        summary = {"version": VERSION, "status": "passed", "run_identity": self.identity,
                   "completed_at_utc": utc_now(), "source_max_depth": self.source_depth,
                   "source_root": str(self.args.source), "source_run_identity": self.source_complete["run_identity"],
                   "source_unique_directory_nodes": self.source_complete["unique_directory_nodes"],
                   "max_depth": self.args.max_depth, "unique_directory_nodes": len(self.depths),
                   "new_directory_nodes": len(self.depths) - self.source_complete["unique_directory_nodes"],
                   "expanded_nodes": len(self.expanded), "boundary_nodes": len(frontier),
                   "depth_limit_reached": bool(frontier), "all_directory_nodes_sha256": sha256(path),
                   "aws_compute_jobs": 0, "network_requests_by_traversal": 0,
                   "revision_origin_mapping_completed": False,
                   "archive_status": "See archive.json for verified S3 upload status",
                   "elapsed_seconds_this_run": round(time.monotonic() - self.started, 3),
                   "reader_peak_rss_kib": resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss,
                   "scope": "Verified earlier directory BFS extended to D{}; no revision/origin expansion".format(self.args.max_depth)}
        write_json(self.output / "complete.json", summary)
        self.state["phase"] = "complete"
        self.emit("run_complete", **summary)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    graph = Path("/mnt/8T/xgr/swh-graph-poc-20260604")
    parser.add_argument("--graph-dir", type=Path, default=graph)
    parser.add_argument("--source", type=Path, default=graph / "route-a-d2-d8-v1")
    parser.add_argument("--baseline", type=Path, default=graph / "route-a-d0-d1-v1/full-d0-d1-v1")
    parser.add_argument("--output", type=Path, default=graph / "route-a-d9-d16-v1")
    parser.add_argument("--max-depth", type=int, default=16)
    parser.add_argument("--batch-size", type=int, default=512)
    parser.add_argument("--max-hours", type=float, default=10)
    parser.add_argument("--s3-prefix", default="new-account-588301174757/route-a/d9-d16-v1/")
    args = parser.parse_args()
    if not 2 <= args.max_depth <= 16 or not 1 <= args.batch_size <= 1024 or not 0 < args.max_hours <= 10:
        parser.error("Allowed: target depth 2..16, batch size 1..1024, duration >0..10h")
    if args.source.resolve() == args.output.resolve():
        parser.error("Extension output must be separate from source")
    if not args.s3_prefix.startswith("new-account-588301174757/route-a/") or not args.s3_prefix.endswith("/"):
        parser.error("S3 output must use the approved account prefix")
    runner = ExtensionRunner(args)
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
