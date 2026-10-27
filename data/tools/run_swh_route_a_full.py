#!/usr/bin/env python3
"""Generate D1 from all D0 with the local graph; checkpoint and compare with B.

Only small anonymous public-S3 mapping reads and a bounded result archive are
allowed. This program cannot submit Glue, Athena, EMR, or EC2 work.
"""

import argparse
import concurrent.futures as futures
import csv
import datetime as dt
import hashlib
import json
import math
import os
from pathlib import Path
import queue
import resource
import signal
import sqlite3
import subprocess
import sys
import threading
import time

import boto3
from boto3.s3.transfer import TransferConfig
from botocore import UNSIGNED
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError
import pyarrow as pa
import pyarrow.parquet as pq

from run_swh_route_a_sample import (
    GRAPH_PREFIX, INDEX_SHA256, NODE_TYPES, Runner as SampleRunner,
    hex_id, merged_ranges, sha256, utc_now, write_json,
)
from swh_p4_content_screen import s3_client


VERSION = "route-a-full-d0-d1-v1"
NODES = 58675740295
ARCHIVE_PREFIX = "graph-results/swh-p4-20260604/route-a-d0-d1-v1/"
MAP_SCHEMA = pa.schema([("directory_id", pa.string()), ("node_id", pa.int64())])
EDGE_SCHEMA = pa.schema([
    ("directory_id", pa.string()), ("node_id", pa.int64()),
    ("parent_nodes", pa.list_(pa.int64())),
    ("revision_nodes", pa.list_(pa.int64())), ("outdegree", pa.int64()),
    ("neighbor_type_counts", pa.list_(pa.int64())),
])


def digest_json(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def chunks(values, size):
    for offset in range(0, len(values), size):
        yield offset // size, values[offset:offset + size]


def put_parquet(path, rows, schema):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".partial")
    pq.write_table(pa.Table.from_pylist(rows, schema=schema), temporary, compression="zstd")
    os.replace(str(temporary), str(path))


class BudgetExceeded(RuntimeError):
    pass


class Budget:
    """Durably reserve worst-case attempts BEFORE network I/O, including retries.

    Rates are deliberately conservative accounting assumptions, not AWS prices:
    $10/million attempts, $1/GiB including 16 KiB overhead per attempt, and $1
    reserved for the small private archive. No billing/credit balance is assumed.
    """
    def __init__(self, path, limit, max_bytes=1024 ** 3):
        self.path = path
        self.limit = limit
        self.max_bytes = max_bytes
        self.state = json.loads(path.read_text()) if path.exists() else {
            "reserved_attempts": 0, "reserved_payload_bytes": 0,
            "archive_allowance_usd": 1.0, "limit_usd": limit,
            "request_usd": 0.00001, "transfer_usd_per_gib": 1.0,
            "overhead_bytes_per_attempt": 16384,
            "accounting": "Conservative task model; not the AWS bill or available credits",
        }
        self.limit = min(limit, self.state["limit_usd"])
        if self.cost() > self.limit:
            raise BudgetExceeded("Saved task reservations exceed the allowed budget")
        write_json(self.path, self.state)

    def cost(self, attempts=0, payload=0):
        count = self.state["reserved_attempts"] + attempts
        size = self.state["reserved_payload_bytes"] + payload
        return (self.state["archive_allowance_usd"] + count * 0.00001
                + (size + count * 16384) / 1024 ** 3)

    def reserve(self, attempts, payload=0):
        if self.state["reserved_payload_bytes"] + payload > self.max_bytes:
            raise BudgetExceeded("Cumulative reserved Range payload would exceed 1 GiB")
        if self.cost(attempts, payload) > self.limit:
            raise BudgetExceeded("Cumulative conservative task cost would exceed ${}".format(self.limit))
        self.state["reserved_attempts"] += attempts
        self.state["reserved_payload_bytes"] += payload
        self.state["conservative_reserved_usd"] = self.cost()
        write_json(self.path, self.state)


class Checkpoints:
    def __init__(self, root, identity):
        self.root, self.identity = root, identity

    def load(self, stage, number, inputs):
        base = self.root / stage / "part-{:05d}".format(number)
        marker, data = base.with_suffix(".complete.json"), base.with_suffix(".parquet")
        if not marker.exists() or not data.exists():
            return None
        saved = json.loads(marker.read_text())
        if saved["identity"] != self.identity or saved["input_sha256"] != digest_json(inputs):
            raise RuntimeError("Checkpoint belongs to another input or implementation: " + str(marker))
        if sha256(data) != saved["sha256"]:
            raise RuntimeError("Committed checkpoint is corrupt: " + str(data))
        table = pq.read_table(data)
        if table.num_rows != saved["rows"]:
            raise RuntimeError("Checkpoint row count mismatch")
        return table.to_pylist()

    def save(self, stage, number, inputs, rows, schema):
        base = self.root / stage / "part-{:05d}".format(number)
        data = base.with_suffix(".parquet")
        put_parquet(data, rows, schema)
        write_json(base.with_suffix(".complete.json"), {
            "identity": self.identity, "input_sha256": digest_json(inputs),
            "sha256": sha256(data), "rows": len(rows), "completed_at_utc": utc_now(),
        })


class FullRunner(SampleRunner):
    def __init__(self, args):
        self.args = args
        self.sample_root = args.graph_dir / "route-a-d0-d1-v1"
        self.output = self.sample_root / "full-d0-d1-v1"
        self.output.mkdir(parents=True, exist_ok=True)
        # Never share a checkpoint DB or a reader between concurrent runs.
        import fcntl
        self.run_lock = (self.output / "run.lock").open("a")
        fcntl.flock(self.run_lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        self.started = time.monotonic()
        self.reader = None
        self.messages = queue.Queue()
        self.lock = threading.Lock()
        self.stop = threading.Event()
        self.range_requests = self.range_bytes = self.range_cache_hits = 0
        self.last_heartbeat = 0.0
        self.public = boto3.Session(region_name="us-east-1").client(
            "s3", endpoint_url="https://s3.amazonaws.com",
            config=Config(signature_version=UNSIGNED, proxies={}, s3={"addressing_style": "virtual"},
                          retries={"total_max_attempts": 1, "mode": "standard"},
                          connect_timeout=8, read_timeout=20, max_pool_connections=args.workers))
        self.state = {"version": VERSION, "started_at_utc": utc_now(), "phase": "preparing",
                      "aws_budget_usd": args.aws_budget_usd, "s3_reserve_usd": 10,
                      "workers": args.workers, "total_d0": 65321}
        control_path = self.output / "control.json"
        if control_path.exists():
            self.control = json.loads(control_path.read_text())
        else:
            now = dt.datetime.now(dt.timezone.utc)
            self.control = {"first_started_at_utc": now.isoformat(),
                            "deadline_utc": (now + dt.timedelta(hours=args.max_hours)).isoformat(),
                            "max_hours": args.max_hours, "aws_budget_usd": args.aws_budget_usd}
            write_json(control_path, self.control)
        remaining = (dt.datetime.fromisoformat(self.control["deadline_utc"])
                     - dt.datetime.now(dt.timezone.utc)).total_seconds()
        self.deadline = self.started + min(remaining, args.max_hours * 3600) - 30
        self.state.update(deadline_utc=self.control["deadline_utc"],
                          first_started_at_utc=self.control["first_started_at_utc"])
        self.budget = Budget(self.output / "budget.json", min(args.aws_budget_usd, self.control["aws_budget_usd"]))
        self.db = sqlite3.connect(str(self.output / "records.sqlite3"))
        self.db.execute("CREATE TABLE IF NOT EXISTS records (object_id TEXT, idx INTEGER, data BLOB, sha TEXT, PRIMARY KEY(object_id, idx))")
        self.db.commit()
        self.cache = {}
        self.pool = futures.ThreadPoolExecutor(max_workers=args.workers)

    def check_deadline(self):
        if self.stop.is_set():
            raise InterruptedError("Stop requested; committed shards are retained")
        if time.monotonic() >= self.deadline:
            raise TimeoutError("10-hour run deadline reached; committed shards are retained")

    def emit(self, event, **fields):
        if event == "reader_event":
            reader = fields["reader"]
            fields = {"reader_event": reader["event"], "reader_rows": len(reader.get("results", []))}
            if reader["event"] == "ready":
                fields["reader_ready"] = reader
        value = dict(fields, event=event, timestamp_utc=utc_now(),
                     elapsed_seconds=round(time.monotonic() - self.started, 3))
        self.state.update(value)
        self.state.update(range_requests_this_run=self.range_requests,
                          range_bytes_this_run=self.range_bytes, cache_records_hit_this_run=self.range_cache_hits,
                          conservative_reserved_usd=round(self.budget.cost(), 6))
        print(json.dumps(value, sort_keys=True), flush=True)
        with (self.output / "events.jsonl").open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(value, sort_keys=True) + "\n")
        write_json(self.output / "status.json", self.state)

    def progress(self, stage, done, total, resumed=0, force=False):
        now = time.monotonic()
        if not force and now - self.last_heartbeat < 30:
            return
        self.last_heartbeat = now
        seconds = now - self.stage_started
        rate = max(done - resumed, 0) / seconds if seconds else 0
        self.emit("progress", stage=stage, completed=done, total=total,
                  percent=round(done * 100 / total, 3) if total else 100,
                  resumed=resumed, units_per_second=round(rate, 3),
                  eta_seconds=round((total - done) / rate) if rate else None,
                  stage_elapsed_seconds=round(seconds, 3))

    def prepare(self):
        self.check_deadline()
        if sha256(self.args.index) != INDEX_SHA256:
            raise RuntimeError("D0 fingerprint mismatch")
        with self.args.index.open(encoding="utf-8", newline="") as handle:
            rows = list(csv.DictReader(handle))
        self.d0_ids = sorted(row["directory_id"] for row in rows)
        if len(self.d0_ids) != 65321 or len(set(self.d0_ids)) != 65321:
            raise RuntimeError("Expected exactly 65,321 distinct D0")
        self.d0 = set(self.d0_ids)
        self.inputs = self.sample_root / "inputs"
        inventory = json.loads((self.inputs / "route_b_inventory.json").read_text())
        self.baseline_complete = json.loads((self.inputs / "route_b_complete.json").read_text())
        if self.baseline_complete["source_files_complete"] != 128 or self.baseline_complete["seed_count"] != len(self.d0):
            raise RuntimeError("Incomplete route B baseline")
        for obj in inventory:
            self.check_deadline()
            path = self.inputs / obj["group"] / Path(obj["key"]).name
            saved = json.loads(path.with_suffix(".verified.json").read_text())
            if (any(saved[key] != obj[key] for key in ("key", "etag", "size"))
                    or path.stat().st_size != obj["size"] or sha256(path) != saved["sha256"]):
                raise RuntimeError("Local baseline verification failed: " + str(path))
        local = {}
        for name, size in (("graph-transposed.graph", 196157270424), ("graph-transposed.ef", 51015688144),
                           ("graph.fmphgo", 16823249121), ("graph.node2type.bin", 22003402616)):
            path = self.args.graph_dir / name
            stat = path.stat()
            if stat.st_size != size or Path(str(path) + ".aria2").exists():
                raise RuntimeError("Incomplete local graph: " + name)
            local[name] = {"size": size, "mtime_ns": stat.st_mtime_ns}
        self.binary = self.sample_root / "build" / "release" / "swh-p4-graph-reader"
        self.mapping_identity = {}
        self.budget.reserve(2)
        for name, width in (("graph.fmphgo.order", 8), ("graph.node2swhid.bin", 22)):
            self.check_deadline()
            response = self.public.head_object(Bucket="softwareheritage", Key=GRAPH_PREFIX + name)
            if response["ContentLength"] != NODES * width:
                raise RuntimeError("Mapping object length mismatch")
            self.mapping_identity[name] = {"key": GRAPH_PREFIX + name, "etag": response["ETag"],
                                           "size": response["ContentLength"]}
        identity = {"version": VERSION, "d0_sha256": INDEX_SHA256, "graph_release": "2026-06-04",
                    "graph_files": local, "mapping_objects": self.mapping_identity,
                    "reader_sha256": sha256(self.binary), "driver_sha256": sha256(Path(__file__)),
                    "helpers_sha256": sha256(Path(__file__).with_name("run_swh_route_a_sample.py")),
                    "properties_sha256": sha256(self.args.graph_dir / "graph-transposed.properties"),
                    "baseline_inventory_sha256": sha256(self.inputs / "route_b_inventory.json"),
                    "baseline_complete_sha256": sha256(self.inputs / "route_b_complete.json"),
                    "map_batch": self.args.map_batch, "edge_batch": self.args.edge_batch,
                    "parent_batch": self.args.parent_batch}
        path = self.output / "identity.json"
        if path.exists() and json.loads(path.read_text()) != identity:
            raise RuntimeError("Run identity changed; refusing to reuse incompatible checkpoints")
        write_json(path, identity)
        self.checkpoints = Checkpoints(self.output / "shards", digest_json(identity))
        self.object_keys = {name: digest_json(value) for name, value in self.mapping_identity.items()}
        for obj, index, data, expected in self.db.execute("SELECT object_id, idx, data, sha FROM records"):
            if hashlib.sha256(data).hexdigest() != expected:
                raise RuntimeError("Persistent mapping cache is corrupt")
            self.cache[(obj, index)] = data
        self.emit("preflight_complete", local_baseline_files=len(inventory), cached_records=len(self.cache),
                  aws_compute_jobs=0, archive_prefix="s3://swh2p4/" + ARCHIVE_PREFIX)

    def range_read(self, name, span):
        start, end, indices = span
        length = end - start + 1
        identity = self.mapping_identity[name]
        last = None
        for attempt in range(3):
            self.check_deadline()
            try:
                with self.lock:
                    self.range_requests += 1
                response = self.public.get_object(Bucket="softwareheritage", Key=identity["key"],
                                                  Range="bytes={}-{}".format(start, end), IfMatch=identity["etag"])
                with response["Body"] as body:
                    if (response["ResponseMetadata"]["HTTPStatusCode"] != 206
                            or response["ContentLength"] != length
                            or response["ContentRange"] != "bytes {}-{}/{}".format(start, end, identity["size"])):
                        raise ValueError("Invalid S3 Range response; refusing full-object download")
                    data = body.read(length + 1)
                if len(data) != length:
                    raise OSError("Truncated S3 Range response")
                with self.lock:
                    self.range_bytes += len(data)
                return span, data
            except ClientError as error:
                if error.response.get("ResponseMetadata", {}).get("HTTPStatusCode") not in (429, 500, 502, 503, 504):
                    raise
                last = error
            except (OSError, BotoCoreError) as error:
                last = error
            if attempt < 2:
                self.stop.wait(0.25 * (2 ** attempt))
        raise RuntimeError("Range read failed after 3 reserved attempts") from last

    def records(self, name, indices, width):
        self.check_deadline()
        object_id = self.object_keys[name]
        indices = sorted(set(indices))
        if any(index < 0 or index >= NODES for index in indices):
            raise RuntimeError("Mapping record index is out of bounds")
        result = {index: self.cache[(object_id, index)] for index in indices if (object_id, index) in self.cache}
        self.range_cache_hits += len(result)
        missing = [index for index in indices if index not in result]
        spans = merged_ranges(missing, width, max_gap=4096, max_span=65536)
        # Reserve all possible retries durably; unused reservations are not refunded.
        self.budget.reserve(len(spans) * 3, sum(end - start + 1 for start, end, _ in spans) * 3)
        pending = {self.pool.submit(self.range_read, name, span) for span in spans}
        completed = 0
        try:
            while pending:
                self.check_deadline()
                done, pending = futures.wait(pending, timeout=2, return_when=futures.FIRST_COMPLETED)
                for task in done:
                    (start, _end, members), data = task.result()
                    for index in members:
                        offset = index * width - start
                        record = data[offset:offset + width]
                        if len(record) != width:
                            raise RuntimeError("Mapping record is truncated")
                        self.cache[(object_id, index)] = result[index] = record
                        self.db.execute("INSERT OR REPLACE INTO records VALUES (?, ?, ?, ?)",
                                        (object_id, index, record, hashlib.sha256(record).hexdigest()))
                    completed += 1
                if done:
                    self.db.commit()
                if time.monotonic() - self.last_heartbeat >= 30:
                    self.last_heartbeat = time.monotonic()
                    self.emit("mapping_progress", mapping=name, spans_completed=completed, spans_total=len(spans))
        finally:
            for task in pending:
                task.cancel()
            self.db.commit()
        return result

    def start_reader(self):
        self.stderr = (self.output / "reader.stderr.log").open("a", encoding="utf-8")
        self.reader = subprocess.Popen([str(self.binary), str(self.args.graph_dir)], stdin=subprocess.PIPE,
                                       stdout=subprocess.PIPE, stderr=self.stderr, text=True, bufsize=1)

        def pump():
            for line in self.reader.stdout:
                self.messages.put(line)
            self.messages.put(None)

        threading.Thread(target=pump, daemon=True).start()
        ready = self.receive("ready")
        if ready["nodes"] != NODES:
            raise RuntimeError("Reader graph version mismatch")
        self.state["reader_load_seconds"] = ready["load_seconds"]

    def map_d0(self):
        self.state["phase"] = "mapping_d0"
        self.stage_started = time.monotonic()
        all_rows, resumed = [], 0
        for number, ids in chunks(self.d0_ids, self.args.map_batch):
            self.check_deadline()
            rows = self.checkpoints.load("d0_map", number, ids)
            if rows is None:
                self.emit("shard_started", stage="mapping_d0", shard=number, directories=len(ids))
                swhids = ["swh:1:dir:" + directory for directory in ids]
                hashes = self.request({"op": "hash", "swhids": swhids}, "hashes")["results"]
                if len(hashes) != len(ids) or [row["swhid"] for row in hashes] != swhids or any(row["position"] is None for row in hashes):
                    raise RuntimeError("Incomplete MPHF result")
                positions = self.records("graph.fmphgo.order", [row["position"] for row in hashes], 8)
                nodes = [int.from_bytes(positions[row["position"]], "big") for row in hashes]
                reverse = self.reverse(nodes)
                if any(reverse[node] != swhid for node, swhid in zip(nodes, swhids)):
                    raise RuntimeError("D0 SWHID roundtrip mismatch")
                rows = [dict(directory_id=directory, node_id=node) for directory, node in zip(ids, nodes)]
                self.checkpoints.save("d0_map", number, ids, rows, MAP_SCHEMA)
            else:
                resumed += len(rows)
            if [row["directory_id"] for row in rows] != ids:
                raise RuntimeError("D0 checkpoint membership mismatch")
            all_rows.extend(rows)
            self.progress("mapping_d0", len(all_rows), len(self.d0_ids), resumed, force=True)
        if len({row["node_id"] for row in all_rows}) != len(self.d0_ids):
            raise RuntimeError("Distinct D0 mapped to the same graph node")
        return sorted(all_rows, key=lambda row: row["node_id"])

    def parent_edges(self, mapped):
        self.state["phase"] = "reading_parent_edges"
        self.stage_started = time.monotonic()
        all_rows, resumed = [], 0
        for number, seeds in chunks(mapped, self.args.edge_batch):
            self.check_deadline()
            rows = self.checkpoints.load("node_edges", number, seeds)
            if rows is None:
                nodes = [row["node_id"] for row in seeds]
                neighbors = self.request({"op": "neighbors", "nodes": nodes}, "neighbors")["results"]
                if [row["node_id"] for row in neighbors] != nodes or any(row["node_type"] != 1 for row in neighbors):
                    raise RuntimeError("Incomplete directory neighbor result")
                rows = [dict(directory_id=seed["directory_id"], **{key: row[key] for key in
                             ("node_id", "parent_nodes", "revision_nodes", "outdegree", "neighbor_type_counts")})
                        for seed, row in zip(seeds, neighbors)]
                self.checkpoints.save("node_edges", number, seeds, rows, EDGE_SCHEMA)
            else:
                resumed += len(rows)
            if [(row["directory_id"], row["node_id"]) for row in rows] != [(row["directory_id"], row["node_id"]) for row in seeds]:
                raise RuntimeError("Parent checkpoint membership mismatch")
            all_rows.extend(rows)
            self.progress("reading_parent_edges", len(all_rows), len(mapped), resumed, force=True)
        return all_rows

    def map_parents(self, neighbors):
        self.state["phase"] = "mapping_parent_ids"
        self.stage_started = time.monotonic()
        parents = sorted({node for row in neighbors for node in row["parent_nodes"]})
        mapped, resumed = {}, 0
        for number, nodes in chunks(parents, self.args.parent_batch):
            self.check_deadline()
            rows = self.checkpoints.load("parent_map", number, nodes)
            if rows is None:
                reverse = self.reverse(nodes)
                if any(not value.startswith("swh:1:dir:") for value in reverse.values()):
                    raise RuntimeError("Parent type and reverse SWHID disagree")
                rows = [dict(node_id=node, directory_id=reverse[node][10:]) for node in nodes]
                self.checkpoints.save("parent_map", number, nodes, rows, MAP_SCHEMA)
            else:
                resumed += len(rows)
            if [row["node_id"] for row in rows] != nodes:
                raise RuntimeError("Parent mapping checkpoint membership mismatch")
            mapped.update((row["node_id"], row["directory_id"]) for row in rows)
            self.progress("mapping_parent_ids", len(mapped), len(parents), resumed, force=True)
        return mapped

    def export_compare(self, mapped, neighbors, parents):
        self.state["phase"] = "comparing"
        self.check_deadline()
        pairs = {(row["directory_id"], parents[node]) for row in neighbors for node in row["parent_nodes"]}
        without = {row["directory_id"] for row in neighbors if not row["parent_nodes"]}
        parent_ids = set(parents.values())
        frontier = parent_ids - self.d0
        if len(neighbors) != len(self.d0) or without != self.d0 - {child for child, _ in pairs}:
            raise RuntimeError("Full traversal coverage check failed")
        final = self.output / "results"
        pair_rows = [dict(child_id=child, parent_id=parent) for child, parent in sorted(pairs)]
        pair_schema = pa.schema([("child_id", pa.string()), ("parent_id", pa.string())])
        directory_schema = pa.schema([("directory_id", pa.string())])
        put_parquet(final / "parent_pairs.parquet", pair_rows, pair_schema)
        put_parquet(final / "frontier_d1.parquet", [dict(directory_id=value) for value in sorted(frontier)], directory_schema)
        put_parquet(final / "all_parent_ids.parquet", [dict(directory_id=value) for value in sorted(parent_ids)], directory_schema)
        put_parquet(final / "no_parent.parquet", [dict(directory_id=value) for value in sorted(without)], directory_schema)
        put_parquet(final / "unresolved.parquet", [], pa.schema([("directory_id", pa.string()), ("reason", pa.string())]))
        put_parquet(final / "d0_node_mapping.parquet", mapped, MAP_SCHEMA)
        put_parquet(final / "parent_node_mapping.parquet", [dict(node_id=node, directory_id=value) for node, value in sorted(parents.items())], MAP_SCHEMA)
        with (final / "parent_pairs.csv").open("w", encoding="utf-8", newline="") as handle:
            writer = csv.writer(handle)
            writer.writerow(["child_id", "parent_id"])
            writer.writerows(sorted(pairs))
        # Route B enters only here, after A's independently derived output exists.
        expected, labelled_count = set(), 0
        for path in sorted((self.inputs / "parent_edges").glob("*.parquet")):
            self.check_deadline()
            table = pq.read_table(path, columns=["child_id", "parent_id"])
            labelled_count += table.num_rows
            expected.update((hex_id(row["child_id"]), hex_id(row["parent_id"])) for row in table.to_pylist())
        expected_no_parent, expected_frontier = set(), set()
        for group, target in (("d0_without_observed_parent", expected_no_parent), ("frontier_d1", expected_frontier)):
            for path in sorted((self.inputs / group).glob("*.parquet")):
                table = pq.read_table(path)
                if table.num_columns != 1:
                    raise RuntimeError("Unexpected baseline directory schema")
                target.update(hex_id(value) for value in table.column(0).to_pylist())
        if (labelled_count != 345693 or len(expected) != 345515 or len(expected_no_parent) != 10525
                or len(expected_frontier) != 114293 or expected_no_parent != self.d0 - {child for child, _ in expected}
                or expected_frontier != {parent for _, parent in expected} - self.d0):
            raise RuntimeError("Route B baseline has inconsistent counts or membership")
        only_a, only_b = sorted(pairs - expected), sorted(expected - pairs)
        for name, values in (("only_a_parent_pairs", only_a), ("only_b_parent_pairs", only_b)):
            put_parquet(final / (name + ".parquet"), [dict(child_id=a, parent_id=b) for a, b in values], pair_schema)
        summary = {"version": VERSION, "status": "passed" if not only_a and not only_b and without == expected_no_parent and frontier == expected_frontier else "mismatch",
                   "completed_at_utc": utc_now(), "d0": len(self.d0), "roundtrip_verified": len(mapped),
                   "parent_pairs_a": len(pairs), "parent_pairs_b": len(expected), "labelled_edges_b": labelled_count,
                   "unique_parent_ids": len(parent_ids), "new_d1_a": len(frontier), "new_d1_b": len(expected_frontier),
                   "no_parent_a": len(without), "no_parent_b": len(expected_no_parent), "unresolved": 0,
                   "only_a_pairs": len(only_a), "only_b_pairs": len(only_b),
                   "only_a_examples": only_a[:20], "only_b_examples": only_b[:20],
                   "no_parent_only_a": sorted(without - expected_no_parent), "no_parent_only_b": sorted(expected_no_parent - without),
                   "new_d1_only_a": sorted(frontier - expected_frontier), "new_d1_only_b": sorted(expected_frontier - frontier),
                   "range_requests_this_run": self.range_requests, "range_bytes_this_run": self.range_bytes,
                   "record_cache_hits_this_run": self.range_cache_hits,
                   "conservative_reserved_usd": self.budget.cost(), "aws_budget_usd": self.budget.limit,
                   "reader_load_seconds": self.state.get("reader_load_seconds"),
                   "elapsed_seconds_this_run": time.monotonic() - self.started,
                   "first_started_at_utc": self.control["first_started_at_utc"],
                   "reader_peak_rss_kib": resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss,
                   "identity": self.checkpoints.identity,
                   "scope": "All D0, one reverse directory hop. No deeper traversal or origin lookup."}
        write_json(self.output / "comparison.json", summary)
        self.emit("full_comparison_complete", **summary)
        if summary["status"] != "passed":
            raise RuntimeError("A differs from B; preserve differences and stop before deeper traversal")
        write_json(self.output / "complete.json", summary)

    def archive(self):
        self.state["phase"] = "archiving"
        self.emit("archive_started", destination="s3://swh2p4/" + ARCHIVE_PREFIX)
        files = sorted((self.output / "results").glob("*"))
        files += [self.output / name for name in ("identity.json", "control.json", "budget.json", "comparison.json")]
        files.append(self.output / "complete.json")
        total = sum(path.stat().st_size for path in files)
        if total > 100 * 1024 ** 2:
            raise BudgetExceeded("Result archive exceeds the reserved 100 MiB allowance")
        client = s3_client("us-east-1")
        archived = []
        for path in files:
            self.check_deadline()
            key = ARCHIVE_PREFIX + str(path.relative_to(self.output))
            checksum = sha256(path)
            client.upload_file(str(path), "swh2p4", key,
                               ExtraArgs={"Metadata": {"sha256": checksum}, "ServerSideEncryption": "AES256"},
                               Config=TransferConfig(use_threads=False, multipart_threshold=128 * 1024 ** 2))
            response = client.head_object(Bucket="swh2p4", Key=key)
            if response["ContentLength"] != path.stat().st_size or response["Metadata"].get("sha256") != checksum:
                raise RuntimeError("S3 archive verification failed")
            archived.append({"key": key, "bytes": path.stat().st_size, "sha256": checksum})
        write_json(self.output / "archive.json", {"completed_at_utc": utc_now(), "bytes": total, "objects": archived})
        self.emit("archive_complete", archive_bytes=total, objects=len(archived))

    def close(self):
        self.stop.set()
        if self.reader is not None:
            if self.reader.poll() is None:
                self.reader.terminate()
            try:
                self.reader.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.reader.kill()
                self.reader.wait(timeout=5)
            self.stderr.close()
        self.pool.shutdown(wait=True)
        self.db.close()
        self.state["reader_peak_rss_kib"] = resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss
        write_json(self.output / "status.json", self.state)
        self.run_lock.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--graph-dir", type=Path, default=Path("/mnt/8T/xgr/swh-graph-poc-20260604"))
    parser.add_argument("--index", type=Path, default=Path("/mnt/8T/xgr/swh-graph-poc-20260604/route-a-d0-d1-v1/inputs/d0.csv"))
    parser.add_argument("--workers", type=int, default=32)
    parser.add_argument("--map-batch", type=int, default=1024)
    parser.add_argument("--edge-batch", type=int, default=512)
    parser.add_argument("--parent-batch", type=int, default=4096)
    parser.add_argument("--aws-budget-usd", type=float, default=25)
    parser.add_argument("--max-hours", type=float, default=10)
    parser.add_argument("--no-upload", action="store_true")
    args = parser.parse_args()
    if (not 1 <= args.workers <= 64 or not 1 <= args.aws_budget_usd <= 25
            or not 0 < args.max_hours <= 10 or not all(size > 0 for size in (args.map_batch, args.edge_batch, args.parent_batch))):
        parser.error("Allowed: 1..64 workers, budget $1..25, duration >0..10h, positive batch sizes")
    runner = FullRunner(args)
    for signum in (signal.SIGINT, signal.SIGTERM):
        signal.signal(signum, lambda _signum, _frame: runner.stop.set())
    try:
        runner.prepare()
        runner.start_reader()
        mapped = runner.map_d0()
        neighbors = runner.parent_edges(mapped)
        parents = runner.map_parents(neighbors)
        # Release the reader before computing final process peak RSS and archiving.
        runner.reader.terminate()
        runner.reader.wait(timeout=10)
        runner.export_compare(mapped, neighbors, parents)
        if not args.no_upload:
            runner.archive()
        runner.state["phase"] = "complete"
        runner.emit("run_complete", status="passed", uploaded=not args.no_upload)
        return 0
    except (Exception, KeyboardInterrupt) as error:
        phase = runner.state["phase"]
        runner.state["phase"] = "stopped" if isinstance(error, (TimeoutError, BudgetExceeded, InterruptedError, KeyboardInterrupt)) else "failed"
        runner.emit("run_stopped", failed_stage=phase, error_type=type(error).__name__, error=str(error))
        return 2 if runner.state["phase"] == "stopped" else 1
    finally:
        runner.close()


if __name__ == "__main__":
    sys.exit(main())
