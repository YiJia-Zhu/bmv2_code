#!/usr/bin/env python3
"""Verify the downloaded reverse graph against a bounded sample of route B.

This reads existing S3 results and small ranges from public mapping files. It
never submits an Athena/Glue job and never starts the full D0 traversal.
"""

import argparse
import concurrent.futures
import csv
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import queue
import resource
import subprocess
import sys
import threading
import time

import boto3
from botocore import UNSIGNED
from botocore.config import Config
from boto3.s3.transfer import TransferConfig
import pyarrow.parquet as pq

from swh_p4_content_screen import s3_client


VERSION = "route-a-sample-v1"
GRAPH_PREFIX = "graph/2026-06-04/compressed/"
BASELINE_PREFIX = "glue-results/swh-p4-20260604/d0-parent-scan-v2/"
INDEX_SHA256 = "58fa1b5ea1844ef19e6d15f8ad9e041065265673a8ec6a95308961ea56ee45f0"
PROBE_IDS = [
    "294ca7131a98ab63083208d45d2b85f81208154b",
    "488727636c23225350dce8ada7d07e81d0d0628e",
    "48a1e22a0b4effac0bba896fe776441cb03699b0",
    "5275b1cc402040045fc830d56daf7d1be4b2e7d1",
    "65e5cf2ed926d31d597e55e556cd5bc70376d759",
    "a03e2602eb9e8de2b12e33c5fe002bb6357a7fbb",
    "eb76378d6665f5549df1b5afa597ce86efebb1fd",
    "0c846910e03e5359c6c68cbb07561c978ac6d02f",
]
NODE_TYPES = ("cnt", "dir", "ori", "rel", "rev", "snp")


def utc_now():
    return dt.datetime.now(dt.timezone.utc).isoformat()


def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(str(temporary), str(path))


def sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def hex_id(value):
    return value.hex() if isinstance(value, bytes) else value.lower()


def merged_ranges(indices, width, max_gap=1024, max_span=65536):
    """Merge nearby records, retaining the index-to-offset relationship."""
    result = []
    for index in sorted(set(indices)):
        start, end = index * width, (index + 1) * width - 1
        if result and start - result[-1][1] - 1 <= max_gap and end - result[-1][0] + 1 <= max_span:
            result[-1][1] = end
            result[-1][2].append(index)
        else:
            result.append([start, end, [index]])
    return result


class Runner:
    def __init__(self, args):
        self.args = args
        self.output = args.graph_dir / "route-a-d0-d1-v1"
        self.output.mkdir(parents=True, exist_ok=True)
        self.started = time.monotonic()
        self.deadline = None
        self.state = {"version": VERSION, "started_at_utc": utc_now(), "phase": "preparing"}
        self.range_requests = 0
        self.range_bytes = 0
        self.range_reserved_bytes = 0
        self.range_cache_hits = 0
        self.lock = threading.Lock()
        self.reader = None
        self.messages = queue.Queue()
        self.public = boto3.Session(region_name="us-east-1").client(
            "s3", endpoint_url="https://s3.amazonaws.com",
            config=Config(signature_version=UNSIGNED, proxies={},
                          s3={"addressing_style": "virtual"},
                          retries={"total_max_attempts": 1, "mode": "standard"},
                          connect_timeout=8, read_timeout=20, max_pool_connections=8))
        self.mapping_identity = {}

    def emit(self, event, **fields):
        value = dict(fields, event=event, timestamp_utc=utc_now(),
                     elapsed_seconds=round(time.monotonic() - self.started, 3))
        print(json.dumps(value, sort_keys=True), flush=True)
        with (self.output / "events.jsonl").open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(value, sort_keys=True) + "\n")
        self.state.update(value)
        self.state.update(range_requests=self.range_requests, range_bytes=self.range_bytes,
                          range_reserved_bytes=self.range_reserved_bytes,
                          range_cache_hits=self.range_cache_hits)
        write_json(self.output / "status.json", self.state)

    def check_deadline(self):
        if self.deadline is not None and time.monotonic() >= self.deadline:
            raise TimeoutError("30-minute sample query deadline reached")

    def prepare(self):
        source = self.args.index
        if sha256(source) != INDEX_SHA256:
            raise RuntimeError("D0 index fingerprint mismatch")
        index = self.output / "inputs" / "d0.csv"
        index.parent.mkdir(parents=True, exist_ok=True)
        index.write_bytes(source.read_bytes())
        with index.open(encoding="utf-8", newline="") as handle:
            self.d0 = {row["directory_id"] for row in csv.DictReader(handle)}
        if len(self.d0) != 65321:
            raise RuntimeError("D0 directory count mismatch")
        private = s3_client("us-east-1")
        with private.get_object(Bucket="swh2p4", Key=BASELINE_PREFIX + "complete.json")["Body"] as body:
            complete = json.load(body)
        if complete["source_files_complete"] != 128 or complete["seed_count"] != len(self.d0):
            raise RuntimeError("route B is incomplete or uses a different D0 count")
        write_json(self.output / "inputs" / "route_b_complete.json", complete)
        objects = []
        for group in ("parent_edges", "frontier_d1", "d0_without_observed_parent"):
            for page in private.get_paginator("list_objects_v2").paginate(
                    Bucket="swh2p4", Prefix=BASELINE_PREFIX + group + "/"):
                for obj in page.get("Contents", []):
                    if obj["Key"].endswith(".parquet"):
                        objects.append({"key": obj["Key"], "size": obj["Size"],
                                        "etag": obj["ETag"], "group": group})
        self.emit("baseline_download_started", objects=len(objects),
                  total_bytes=sum(obj["size"] for obj in objects))

        def download(obj):
            target = self.output / "inputs" / obj["group"] / Path(obj["key"]).name
            marker = target.with_suffix(".verified.json")
            identity = {key: obj[key] for key in ("key", "size", "etag")}
            if target.exists() and marker.exists():
                saved = json.loads(marker.read_text())
                if (all(saved.get(key) == value for key, value in identity.items())
                        and target.stat().st_size == obj["size"] and sha256(target) == saved["sha256"]):
                    return target
            target.parent.mkdir(parents=True, exist_ok=True)
            temporary = target.with_suffix(".partial")
            private.download_file("swh2p4", obj["key"], str(temporary),
                                  Config=TransferConfig(use_threads=False))
            if temporary.stat().st_size != obj["size"]:
                raise RuntimeError("baseline download size mismatch")
            # Pin this result to the object inventory, including across downloads.
            current = private.head_object(Bucket="swh2p4", Key=obj["key"])
            if current["ETag"] != obj["etag"]:
                raise RuntimeError("baseline object changed while downloading")
            os.replace(str(temporary), str(target))
            write_json(marker, dict(identity, sha256=sha256(target)))
            return target

        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            list(pool.map(download, objects))
        write_json(self.output / "inputs" / "route_b_inventory.json", objects)
        self.emit("baseline_download_complete", objects=len(objects))
        self.baseline = {}
        edge_count = 0
        for path in sorted((self.output / "inputs" / "parent_edges").glob("*.parquet")):
            table = pq.read_table(path, columns=["child_id", "parent_id"])
            edge_count += table.num_rows
            for row in table.to_pylist():
                child, parent = hex_id(row["child_id"]), hex_id(row["parent_id"])
                if child not in self.d0:
                    raise RuntimeError("route B child is outside D0")
                self.baseline.setdefault(child, set()).add(parent)
        without = set()
        for path in (self.output / "inputs" / "d0_without_observed_parent").glob("*.parquet"):
            table = pq.read_table(path)
            if table.num_columns != 1:
                raise RuntimeError("unexpected no-parent schema")
            without.update(hex_id(value) for value in table.column(0).to_pylist())
        if (edge_count != complete["unique_parent_edge_rows"] or len(without) != 10525
                or without != self.d0 - set(self.baseline)):
            raise RuntimeError("route B baseline count or no-parent set mismatch")
        samples = [(directory, "previous_probe") for directory in PROBE_IDS]
        groups = [("no_parent", sorted(without), 2),
                  ("multiple_parents", sorted(key for key, parents in self.baseline.items()
                                              if len(parents) > 1), 3),
                  ("single_parent", sorted(key for key, parents in self.baseline.items()
                                            if len(parents) == 1), 3)]
        for group, ids, count in groups:
            for directory in (key for key in ids if key not in {s[0] for s in samples}):
                samples.append((directory, group))
                count -= 1
                if not count:
                    break
        if len(samples) != 16 or not all(directory in self.d0 for directory, _ in samples):
            raise RuntimeError("invalid sample selection")
        self.samples = [{"directory_id": directory, "group": group,
                         "route_b_parent_count": len(self.baseline.get(directory, set()))}
                        for directory, group in samples]
        write_json(self.output / "sample_inputs.json", self.samples)
        self.emit("sample_selected", samples=len(samples),
                  baseline_parent_pairs=sum(len(parents) for parents in self.baseline.values()))

    def range_read(self, name, start, end):
        self.check_deadline()
        identity = self.mapping_identity[name]
        cache_key = hashlib.sha256(json.dumps([identity, start, end], sort_keys=True).encode()).hexdigest()
        cache = self.output / "range_cache" / cache_key
        length = end - start + 1
        if cache.exists():
            data = cache.read_bytes()
            if len(data) != length:
                raise RuntimeError("range cache size mismatch")
            with self.lock:
                self.range_cache_hits += 1
            return data
        for attempt in range(3):
            self.check_deadline()
            with self.lock:
                if self.range_reserved_bytes + length > 1024 ** 3:
                    raise RuntimeError("1 GiB Range download budget exceeded")
                self.range_reserved_bytes += length
                self.range_requests += 1
            try:
                response = self.public.get_object(Bucket="softwareheritage", Key=GRAPH_PREFIX + name,
                                                  Range="bytes={}-{}".format(start, end),
                                                  IfMatch=identity["etag"])
                with response["Body"] as body:
                    if (response["ResponseMetadata"]["HTTPStatusCode"] != 206
                            or response["ContentLength"] != length
                            or response["ContentRange"] != "bytes {}-{}/{}".format(start, end, identity["size"])):
                        raise RuntimeError("invalid S3 Range response")
                    data = body.read(length + 1)
                if len(data) != length:
                    raise RuntimeError("truncated Range response")
                with self.lock:
                    self.range_bytes += len(data)
                cache.parent.mkdir(parents=True, exist_ok=True)
                temporary = cache.with_suffix(".tmp")
                temporary.write_bytes(data)
                os.replace(str(temporary), str(cache))
                return data
            except (OSError, RuntimeError):
                if attempt == 2:
                    raise
        raise RuntimeError("Range retry loop exhausted")

    def records(self, name, indices, width):
        result = {}
        ranges = merged_ranges(indices, width)

        def read(span):
            start, end, ids = span
            return span, self.range_read(name, start, end)

        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            for (start, _end, ids), data in pool.map(read, ranges):
                for index in ids:
                    offset = index * width - start
                    result[index] = data[offset:offset + width]
        return result

    def reverse(self, nodes):
        values = self.records("graph.node2swhid.bin", nodes, 22)
        result = {}
        for node, data in values.items():
            if data[0] != 1 or data[1] >= len(NODE_TYPES):
                raise RuntimeError("invalid binary SWHID at node {}".format(node))
            result[node] = "swh:1:{}:{}".format(NODE_TYPES[data[1]], data[2:].hex())
        return result

    def receive(self, expected):
        while True:
            self.check_deadline()
            try:
                line = self.messages.get(timeout=10)
            except queue.Empty:
                self.emit("reader_wait", expected=expected)
                continue
            if line is None:
                raise RuntimeError("reader exited unexpectedly; see reader.stderr.log")
            value = json.loads(line)
            self.emit("reader_event", reader=value)
            if value["event"] == expected:
                return value

    def request(self, value, expected):
        self.reader.stdin.write(json.dumps(value) + "\n")
        self.reader.stdin.flush()
        return self.receive(expected)

    def query(self):
        self.deadline = time.monotonic() + 1800
        self.state["phase"] = "querying"
        local_files = {}
        for name, size in (("graph-transposed.graph", 196157270424),
                           ("graph-transposed.ef", 51015688144),
                           ("graph.fmphgo", 16823249121),
                           ("graph.node2type.bin", 22003402616)):
            path = self.args.graph_dir / name
            metadata = path.stat()
            if metadata.st_size != size or Path(str(path) + ".aria2").exists():
                raise RuntimeError("local graph file incomplete: " + name)
            local_files[name] = {"size": size, "mtime_ns": metadata.st_mtime_ns}
        source_root = Path(__file__).parent / "swh_graph_reader"
        binary = self.output / "build" / "release" / "swh-p4-graph-reader"
        write_json(self.output / "run_identity.json", {
            "d0_sha256": INDEX_SHA256, "graph_release": "2026-06-04",
            "graph_files": local_files, "reader_sha256": sha256(binary),
            "cargo_lock_sha256": sha256(source_root / "Cargo.lock"),
            "properties_sha256": sha256(self.args.graph_dir / "graph-transposed.properties"),
            "driver_sha256": sha256(Path(__file__)),
            "sample_inputs_sha256": sha256(self.output / "sample_inputs.json")})
        for name, width in (("graph.fmphgo.order", 8), ("graph.node2swhid.bin", 22)):
            response = self.public.head_object(Bucket="softwareheritage", Key=GRAPH_PREFIX + name)
            if response["ContentLength"] != 58675740295 * width:
                raise RuntimeError("public mapping size mismatch")
            self.mapping_identity[name] = {"key": GRAPH_PREFIX + name, "etag": response["ETag"],
                                           "size": response["ContentLength"]}
        write_json(self.output / "mapping_identity.json", self.mapping_identity)
        self.stderr = (self.output / "reader.stderr.log").open("a", encoding="utf-8")
        self.reader = subprocess.Popen([str(binary), str(self.args.graph_dir)],
                                       stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                       stderr=self.stderr, text=True, bufsize=1)

        def pump():
            for line in self.reader.stdout:
                self.messages.put(line)
            self.messages.put(None)

        threading.Thread(target=pump, daemon=True).start()
        ready = self.receive("ready")
        self.state["reader_ready"] = ready
        swhids = ["swh:1:dir:" + sample["directory_id"] for sample in self.samples]
        hashes = self.request({"op": "hash", "swhids": swhids}, "hashes")["results"]
        if any(row["position"] is None for row in hashes):
            raise RuntimeError("sample SWHID MPHF lookup failed")
        positions = self.records("graph.fmphgo.order", [row["position"] for row in hashes], 8)
        self.nodes = {row["swhid"]: int.from_bytes(positions[row["position"]], "big") for row in hashes}
        if any(node >= ready["nodes"] for node in self.nodes.values()):
            raise RuntimeError("permuted node ID out of bounds")
        reverse = self.reverse(self.nodes.values())
        for swhid, node in self.nodes.items():
            if reverse[node] != swhid:
                raise RuntimeError("SWHID/node roundtrip mismatch: {} -> {} -> {}".format(swhid, node, reverse[node]))
        write_json(self.output / "sample_id_mapping.json", [dict(row, node_id=self.nodes[row["swhid"]],
                                                                  roundtrip_verified=True) for row in hashes])
        self.emit("roundtrip_verified", directories=len(self.nodes))
        # Check the first input before expanding to the remaining sample inputs.
        neighbors = []
        for group in (list(self.nodes.values())[:1], list(self.nodes.values())[1:]):
            neighbors.extend(self.request({"op": "neighbors", "nodes": group}, "neighbors")["results"])
        write_json(self.output / "sample_node_neighbors.json", neighbors)
        parents = {node for row in neighbors for node in row["parent_nodes"]}
        revisions = {node for row in neighbors for node in row["revision_nodes"]}
        reverse = self.reverse(parents | revisions)
        seed_by_node = {node: swhid[10:] for swhid, node in self.nodes.items()}
        rows = []
        pairs = set()
        mismatches = []
        for row in neighbors:
            directory = seed_by_node[row["node_id"]]
            if any(not reverse[node].startswith("swh:1:dir:") for node in row["parent_nodes"]):
                raise RuntimeError("parent node type disagrees with reverse SWHID")
            if any(not reverse[node].startswith("swh:1:rev:") for node in row["revision_nodes"]):
                raise RuntimeError("revision node type disagrees with reverse SWHID")
            actual = {reverse[node][10:] for node in row["parent_nodes"]}
            expected = self.baseline.get(directory, set())
            only_a, only_b = sorted(actual - expected), sorted(expected - actual)
            result = dict(row, directory_id=directory, parent_ids=sorted(actual),
                          revision_swhids=[reverse[node] for node in row["revision_nodes"]],
                          route_b_parent_count=len(expected), only_a=only_a, only_b=only_b,
                          status="matched" if not only_a and not only_b else "mismatch")
            rows.append(result)
            pairs.update((directory, parent) for parent in actual)
            if only_a or only_b:
                mismatches.append(result)
        write_json(self.output / "sample_results.json", rows)
        with (self.output / "sample_parent_pairs.csv").open("w", encoding="utf-8", newline="") as handle:
            writer = csv.writer(handle)
            writer.writerow(["child_id", "parent_id"])
            writer.writerows(sorted(pairs))
        expected_pairs = {(sample["directory_id"], parent) for sample in self.samples
                          for parent in self.baseline.get(sample["directory_id"], set())}
        actual_no_parent = sorted(row["directory_id"] for row in rows if not row["parent_ids"])
        expected_no_parent = sorted(sample["directory_id"] for sample in self.samples
                                    if not self.baseline.get(sample["directory_id"]))
        summary = {"version": VERSION, "completed_at_utc": utc_now(), "samples": len(rows),
                   "status": "passed" if not mismatches else "mismatch", "mismatched_directories":len(mismatches),
                   "parent_pairs_a":len(pairs), "parent_pairs_b":len(expected_pairs),
                   "only_a_pairs":sorted(pairs - expected_pairs), "only_b_pairs":sorted(expected_pairs - pairs),
                   "no_parent_a":actual_no_parent, "no_parent_b":expected_no_parent,
                   "no_parent_match":actual_no_parent == expected_no_parent,
                   "new_d1_a":sorted({parent for _, parent in pairs} - self.d0),
                   "new_d1_b":sorted({parent for _, parent in expected_pairs} - self.d0),
                   "unresolved":0, "range_requests":self.range_requests, "range_bytes":self.range_bytes,
                   "range_reserved_bytes":self.range_reserved_bytes, "range_cache_hits":self.range_cache_hits,
                   "elapsed_seconds":time.monotonic() - self.started,
                   "query_seconds":time.monotonic() - (self.deadline - 1800),
                   "reader_load_seconds":ready["load_seconds"], "d0_sha256":INDEX_SHA256,
                   "scope":"16 input directories, one reverse directory hop; no full D0 run or origin lookup"}
        if summary["new_d1_a"] != summary["new_d1_b"] or not summary["no_parent_match"]:
            summary["status"] = "mismatch"
        write_json(self.output / "comparison_sample.json", summary)
        self.emit("sample_comparison_complete", **summary)
        if summary["status"] != "passed":
            raise RuntimeError("sample differs from route B; stop before expansion")
        return summary

    def close(self):
        if self.reader is not None:
            if self.reader.poll() is None:
                self.reader.terminate()
            try:
                self.reader.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.reader.kill()
                self.reader.wait(timeout=10)
            self.stderr.close()
        self.state["reader_peak_rss_kib"] = resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss
        comparison = self.output / "comparison_sample.json"
        if comparison.exists() and self.state["phase"] == "complete":
            summary = json.loads(comparison.read_text())
            summary["reader_peak_rss_kib"] = self.state["reader_peak_rss_kib"]
            write_json(comparison, summary)
        write_json(self.output / "status.json", self.state)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--graph-dir", type=Path, default=Path("/mnt/8T/xgr/swh-graph-poc-20260604"))
    parser.add_argument("--index", type=Path, default=Path("/tmp/swh-candidate-parent-index-v1.csv"))
    args = parser.parse_args()
    runner = Runner(args)
    try:
        runner.prepare()
        runner.query()
        runner.state["phase"] = "complete"
        runner.emit("sample_finished")
        return 0
    except Exception as error:
        runner.state["phase"] = "failed"
        runner.emit("sample_failed", error_type=type(error).__name__, error=str(error))
        raise
    finally:
        runner.close()


if __name__ == "__main__":
    sys.exit(main())
