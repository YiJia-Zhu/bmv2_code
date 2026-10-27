#!/usr/bin/env python3
"""Map all merged candidate revisions to SWH origins and URLs.

The graph traversal is local because the downloaded SWH graph is a compressed
random-access graph rather than an ORC table.  AWS is used only for bounded
Range reads of origin SWHIDs, one Athena origin lookup, and result archival.
Every expensive phase is checkpointed in SQLite or an immutable JSON marker.
"""

import argparse
import base64
import concurrent.futures
import csv
import datetime as dt
import fcntl
import hashlib
import json
import os
import re
from collections import deque
from pathlib import Path
import queue
import resource
import signal
import shutil
import sqlite3
import subprocess
import sys
import threading
import time

import boto3
import botocore.session
from boto3.s3.transfer import TransferConfig
from botocore import UNSIGNED
from botocore.config import Config
from botocore.credentials import RefreshableCredentials
from botocore.exceptions import BotoCoreError, ClientError
import pyarrow as pa
import pyarrow.parquet as pq

from run_swh_route_a_full import Budget, BudgetExceeded, digest_json, put_parquet
from run_swh_route_a_sample import GRAPH_PREFIX, NODE_TYPES, merged_ranges, sha256, utc_now, write_json


VERSION = "route-a-origin-full-v1"
GRAPH_RELEASE = "2026-06-04"
GRAPH_NODES = 58675740295
GRAPH_FILES = {
    "graph-transposed.graph": 196157270424,
    "graph-transposed.ef": 51015688144,
    "graph.fmphgo": 16823249121,
    "graph.node2type.bin": 22003402616,
}
MERGE_ROOT = Path("/mnt/8T/xgr/swh-graph-poc-20260604/route-a-revision-origin-v1/merge")
GRAPH_ROOT = Path("/mnt/8T/xgr/swh-graph-poc-20260604")
OUTPUT_ROOT = GRAPH_ROOT / "route-a-revision-origin-v1/full"
BUCKET = "swh2p4"
S3_PREFIX = "new-account-588301174757/route-a/revision-origin-v1/full/"
WORKGROUP = "swh-route-a-origin-full-bounded"
SCAN_LIMIT = 32 * 1024 ** 3
EXPECTED_ACCOUNT = "588301174757"
BUCKET_OWNER = "728516300718"
PUBLIC_BUCKET = "softwareheritage"
ORIGIN_MAPPING_KEY = GRAPH_PREFIX + "graph.node2swhid.bin"
ATHENA_DATABASE = "swh_p4_20260604"
ATHENA_ORIGIN_TABLE = "origin_20260604"
ORIGIN_NODE_TYPE = 2
TRAVERSE_TYPES = (3, 4, 5)

SEED_REV_SCHEMA = pa.schema([
    ("seed_directory_id", pa.string()), ("seed_node_id", pa.int64()),
    ("root_directory_node_id", pa.int64()), ("candidate_revision_node_id", pa.int64()),
    ("directory_hops", pa.int32()),
])
EDGE_SCHEMA = pa.schema([
    ("source_node_id", pa.int64()), ("target_node_id", pa.int64()),
    ("source_type", pa.int8()), ("target_type", pa.int8()),
])
ORIGIN_SCHEMA = pa.schema([
    ("origin_node_id", pa.int64()), ("origin_id", pa.string()),
    ("origin_swhid", pa.string()),
    ("origin_url", pa.string()), ("status", pa.string()),
])
SEED_ORIGIN_SCHEMA = pa.schema([
    ("seed_directory_id", pa.string()), ("seed_node_id", pa.int64()),
    ("root_directory_node_id", pa.int64()), ("candidate_revision_node_id", pa.int64()),
    ("origin_node_id", pa.int64()), ("origin_id", pa.string()),
    ("origin_url", pa.string()), ("status", pa.string()),
])
UNRESOLVED_SCHEMA = pa.schema([
    ("kind", pa.string()), ("node_id", pa.int64()), ("reason", pa.string()),
])


def authenticated_session():
    """Use normal boto3 credentials, or refresh the active aws-login session."""
    session = boto3.Session(region_name="us-east-1")
    if session.get_credentials() is not None:
        return session

    def refresh():
        result = subprocess.run(
            ["aws", "configure", "export-credentials", "--format", "process"],
            capture_output=True, text=True, check=True, timeout=30)
        values = json.loads(result.stdout)
        return {"access_key": values["AccessKeyId"],
                "secret_key": values["SecretAccessKey"],
                "token": values.get("SessionToken"),
                "expiry_time": values["Expiration"]}

    raw = botocore.session.get_session()
    credentials = RefreshableCredentials.create_from_metadata(
        metadata=refresh(), refresh_using=refresh, method="aws-cli-login")
    credentials._advisory_refresh_timeout = 60
    credentials._mandatory_refresh_timeout = 30
    raw._credentials = credentials
    return boto3.Session(botocore_session=raw, region_name="us-east-1")


def chunks(values, size):
    for offset in range(0, len(values), size):
        yield offset // size, values[offset:offset + size]


def record_sha(path):
    return sha256(path)


def origin_hash(value):
    raw = value.removeprefix("swh:1:ori:") if hasattr(value, "removeprefix") else value.split(":")[-1]
    if not re.fullmatch("[0-9a-f]{40}", raw):
        raise ValueError("Invalid origin identifier: " + value)
    return raw


class FullOriginRun:
    def __init__(self, args):
        self.args = args
        self.root = args.output
        self.root.mkdir(parents=True, exist_ok=True)
        self.run_lock = (self.root / "run.lock").open("a")
        fcntl.flock(self.run_lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        self.started = time.monotonic()
        self.stop = threading.Event()
        self.reader = None
        self.reader_stderr = None
        self.reader_messages = queue.Queue()
        self.reader_thread = None
        self.last_progress = 0.0
        self.phase_started = self.started
        self.lock = threading.Lock()
        self.range_requests = 0
        self.range_bytes = 0
        self.range_cache_hits = 0
        self.public = boto3.Session(region_name="us-east-1").client(
            "s3", endpoint_url="https://s3.amazonaws.com",
            config=Config(signature_version=UNSIGNED, proxies={},
                          s3={"addressing_style": "virtual"},
                          retries={"total_max_attempts": 1, "mode": "standard"},
                          connect_timeout=8, read_timeout=30,
                          max_pool_connections=args.workers))
        private_session = authenticated_session()
        self.private = private_session.client(
            "s3", config=Config(retries={"total_max_attempts": 3, "mode": "standard"},
                                 max_pool_connections=args.workers))
        self.athena = private_session.client(
            "athena", config=Config(retries={"total_max_attempts": 3, "mode": "standard"},
                                     max_pool_connections=args.workers))
        self.sts = private_session.client("sts")
        self.state = {"version": VERSION, "phase": "preparing", "started_at_utc": utc_now(),
                      "aws_budget_usd": args.aws_budget_usd, "s3_reserve_usd": 10,
                      "workers": args.workers}
        self.control_path = self.root / "control.json"
        if self.control_path.exists():
            self.control = json.loads(self.control_path.read_text(encoding="utf-8"))
            if self.control["aws_budget_usd"] > args.aws_budget_usd:
                raise RuntimeError("Saved budget is larger than this invocation")
        else:
            now = dt.datetime.now(dt.timezone.utc)
            self.control = {
                "version": VERSION,
                "first_started_at_utc": now.isoformat(),
                "deadline_utc": (now + dt.timedelta(hours=args.max_hours)).isoformat(),
                "max_hours": args.max_hours,
                "aws_budget_usd": args.aws_budget_usd,
                "s3_reserve_usd": 10,
                "batch_size": args.batch_size,
            }
            write_json(self.control_path, self.control)
        self.deadline = dt.datetime.fromisoformat(self.control["deadline_utc"])
        self.state.update(first_started_at_utc=self.control["first_started_at_utc"],
                          deadline_utc=self.control["deadline_utc"])
        self.budget = Budget(self.root / "budget.json", min(args.aws_budget_usd,
                                                             self.control["aws_budget_usd"]))
        self.db = sqlite3.connect(str(self.root / "state.sqlite3"), timeout=120)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=NORMAL")
        self._create_tables()
        self.db.set_progress_handler(self.sql_interrupted, 100000)

    def sql_interrupted(self):
        return int(self.stop.is_set() or dt.datetime.now(dt.timezone.utc) >= self.deadline)

    def _create_tables(self):
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS seed_status (
                seed_index INTEGER PRIMARY KEY, seed_directory_id TEXT NOT NULL,
                seed_node_id INTEGER NOT NULL, complete INTEGER NOT NULL DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS seed_revision (
                seed_directory_id TEXT NOT NULL, seed_node_id INTEGER NOT NULL,
                root_directory_node_id INTEGER NOT NULL,
                candidate_revision_node_id INTEGER NOT NULL, directory_hops INTEGER NOT NULL,
                PRIMARY KEY(seed_directory_id, root_directory_node_id, candidate_revision_node_id)
            );
            CREATE TABLE IF NOT EXISTS graph_nodes (
                node_id INTEGER PRIMARY KEY, node_type INTEGER NOT NULL,
                status TEXT NOT NULL, outdegree INTEGER, truncated INTEGER NOT NULL DEFAULT 0,
                error TEXT
            );
            CREATE INDEX IF NOT EXISTS graph_nodes_pending ON graph_nodes(status, node_id);
            CREATE TABLE IF NOT EXISTS traversal_edges (
                source_node_id INTEGER NOT NULL, target_node_id INTEGER NOT NULL,
                source_type INTEGER NOT NULL, target_type INTEGER NOT NULL,
                PRIMARY KEY(source_node_id, target_node_id)
            );
            CREATE INDEX IF NOT EXISTS traversal_edges_target ON traversal_edges(target_node_id);
            CREATE TABLE IF NOT EXISTS origins (origin_node_id INTEGER PRIMARY KEY);
            CREATE TABLE IF NOT EXISTS origin_ids (
                origin_node_id INTEGER PRIMARY KEY, origin_id TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS origin_urls (
                origin_id TEXT NOT NULL, origin_url TEXT NOT NULL,
                PRIMARY KEY(origin_id, origin_url)
            );
            CREATE TABLE IF NOT EXISTS checkpoints (
                stage TEXT NOT NULL, batch_no INTEGER NOT NULL, input_sha256 TEXT NOT NULL,
                rows INTEGER NOT NULL, completed_at_utc TEXT NOT NULL,
                PRIMARY KEY(stage, batch_no)
            );
        """)
        self.db.commit()

    def check(self):
        if self.stop.is_set():
            raise InterruptedError("stop requested; committed checkpoints retained")
        if dt.datetime.now(dt.timezone.utc) >= self.deadline:
            raise TimeoutError("12-hour deadline reached; committed checkpoints retained")

    def emit(self, event, **fields):
        row = dict(fields, event=event, timestamp_utc=utc_now(),
                   elapsed_seconds=round(time.monotonic() - self.started, 3))
        self.state.update(row)
        self.state.update(range_requests=self.range_requests, range_bytes=self.range_bytes,
                          range_cache_hits=self.range_cache_hits,
                          conservative_reserved_usd=round(self.budget.cost(), 6))
        print(json.dumps(row, sort_keys=True), flush=True)
        with (self.root / "events.jsonl").open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(row, sort_keys=True) + "\n")
        write_json(self.root / "status.json", self.state)

    def progress(self, stage, completed, total, extra=None, force=False):
        now = time.monotonic()
        if not force and now - self.last_progress < 30:
            return
        self.last_progress = now
        elapsed = max(now - self.phase_started, 0.001)
        rate = completed / elapsed
        fields = {"stage": stage, "completed": completed, "total": total,
                  "percent": round(100 * completed / total, 3) if total else 100,
                  "rate_per_second": round(rate, 3),
                  "eta_seconds": round((total - completed) / rate) if rate else None}
        if extra:
            fields.update(extra)
        self.emit("progress", **fields)

    def preflight(self):
        self.check()
        account = self.sts.get_caller_identity()["Account"]
        if account != EXPECTED_ACCOUNT:
            raise RuntimeError("AWS account changed: " + account)
        merge_root = self.args.merge
        complete_path = merge_root / "complete.json"
        if not complete_path.exists():
            raise RuntimeError("missing merge complete.json")
        complete = json.loads(complete_path.read_text(encoding="utf-8"))
        if complete.get("status") != "passed" or complete.get("unique_revision_nodes") != 317804:
            raise RuntimeError("merge input is not the verified 317,804-revision result")
        expected_files = {item["path"]: item for item in complete["files"]}
        for name, metadata in expected_files.items():
            path = merge_root / name
            if path.stat().st_size != metadata["bytes"] or record_sha(path) != metadata["sha256"]:
                raise RuntimeError("merge input changed: " + name)
        for name, expected_size in GRAPH_FILES.items():
            path = self.args.graph / name
            if not path.exists() or path.stat().st_size != expected_size or Path(str(path) + ".aria2").exists():
                raise RuntimeError("incomplete graph file: " + name)
        binary = self.args.binary
        if not binary.exists():
            raise RuntimeError("missing graph reader: " + str(binary))
        mapping = self.public.head_object(Bucket=PUBLIC_BUCKET, Key=ORIGIN_MAPPING_KEY)
        expected_mapping_size = GRAPH_NODES * 22
        if mapping["ContentLength"] != expected_mapping_size:
            raise RuntimeError("public origin mapping size mismatch")
        identity = {
            "version": VERSION,
            "merge_complete_sha256": record_sha(complete_path),
            "graph_release": GRAPH_RELEASE,
            "graph_files": {name: {"size": self.args.graph.joinpath(name).stat().st_size,
                                   "mtime_ns": self.args.graph.joinpath(name).stat().st_mtime_ns}
                            for name in GRAPH_FILES},
            "graph_reader_sha256": record_sha(binary),
            "origin_mapping": {"key": ORIGIN_MAPPING_KEY, "etag": mapping["ETag"],
                                "size": mapping["ContentLength"]},
            "driver_sha256": record_sha(Path(__file__)),
            "batch_size": self.args.batch_size,
            "workers": self.args.workers,
        }
        identity_path = self.root / "identity.json"
        if identity_path.exists():
            previous = json.loads(identity_path.read_text(encoding="utf-8"))
            if previous != identity:
                immutable = ["version", "merge_complete_sha256", "graph_release", "graph_files",
                             "origin_mapping", "batch_size", "workers", "graph_reader_sha256"]
                if not all(previous.get(key) == identity.get(key) for key in immutable):
                    raise RuntimeError("run identity changed; refusing incompatible resume")
                migration = self.root / "approved-driver-migration.json"
                expected_migration = {"from_sha256": previous.get("driver_sha256"),
                                      "to_sha256": identity["driver_sha256"]}
                if not migration.exists() or json.loads(migration.read_text()) != expected_migration:
                    raise RuntimeError("Driver changed without an explicit checkpoint migration record")
                write_json(self.root / "identity-migration.json", {
                    "from_driver_sha256": previous.get("driver_sha256"),
                    "to_driver_sha256": identity["driver_sha256"],
                    "migrated_at_utc": utc_now(),
                    "reason": "credential refresh and exception handling fix",
                })
        write_json(identity_path, identity)
        self.identity = digest_json(identity)
        self.data_identity = digest_json({key: identity[key] for key in (
            "version", "merge_complete_sha256", "graph_release", "graph_files",
            "graph_reader_sha256", "origin_mapping")})
        self.mapping_identity = identity["origin_mapping"]
        # Schema migration: the ORC origin.id column contains a raw SHA-1.
        # The former driver stored full SWHIDs; retain the graph bytes and
        # canonicalize only their representation, without network requests.
        for node, value in self.db.execute("SELECT origin_node_id,origin_id FROM origin_ids").fetchall():
            raw = origin_hash(value)
            if raw != value:
                self.db.execute("UPDATE origin_ids SET origin_id=? WHERE origin_node_id=?", (raw, node))
        self.db.commit()
        seeds = pq.read_table(merge_root / "seed_nodes.parquet").to_pylist()
        if len(seeds) != 65321 or len({row["seed_directory_id"] for row in seeds}) != len(seeds):
            raise RuntimeError("seed_nodes does not contain 65,321 unique seeds")
        self.seeds = sorted(seeds, key=lambda row: row["seed_directory_id"])
        parents = pq.read_table(merge_root / "parent_edges.parquet").to_pylist()
        revisions = pq.read_table(merge_root / "directory_revision.parquet").to_pylist()
        self.parents = {}
        for row in parents:
            self.parents.setdefault(row["child_node_id"], set()).add(row["parent_node_id"])
        self.revisions = {}
        for row in revisions:
            self.revisions.setdefault(row["directory_node_id"], set()).add(row["revision_node_id"])
        self.emit("preflight_complete", seeds=len(self.seeds), parent_edges=len(parents),
                   directory_revision_edges=len(revisions), unique_revision_nodes=complete["unique_revision_nodes"],
                   aws_compute_jobs=0)

    def seed_revision_stage(self):
        self.state["phase"] = "seed_revision"
        self.phase_started = time.monotonic()
        total = len(self.seeds)
        for batch_no, batch in chunks(self.seeds, self.args.seed_batch):
            self.check()
            marker_input = [{"seed_directory_id": row["seed_directory_id"],
                             "seed_node_id": row["seed_node_id"]} for row in batch]
            marker_hash = digest_json(marker_input)
            done = self.db.execute(
                "SELECT COUNT(*) FROM seed_status WHERE seed_index BETWEEN ? AND ? AND complete=1",
                (batch_no * self.args.seed_batch, batch_no * self.args.seed_batch + len(batch) - 1)).fetchone()[0]
            checkpoint = self.db.execute(
                "SELECT input_sha256 FROM checkpoints WHERE stage='seed_revision' AND batch_no=?",
                (batch_no,)).fetchone()
            if done == len(batch) and checkpoint and checkpoint[0] == marker_hash:
                continue
            self.db.execute("BEGIN")
            try:
                for offset, seed in enumerate(batch):
                    seed_index = batch_no * self.args.seed_batch + offset
                    existing = self.db.execute("SELECT complete FROM seed_status WHERE seed_index=?",
                                               (seed_index,)).fetchone()
                    if existing and existing[0]:
                        continue
                    seed_node = seed["seed_node_id"]
                    queue_nodes = deque([(seed_node, 0)])
                    seen = {seed_node}
                    rows = []
                    while queue_nodes:
                        directory, depth = queue_nodes.popleft()
                        for revision in sorted(self.revisions.get(directory, ())):
                            rows.append((seed["seed_directory_id"], seed_node, directory, revision, depth))
                        for parent in sorted(self.parents.get(directory, ())):
                            if parent not in seen:
                                seen.add(parent)
                                queue_nodes.append((parent, depth + 1))
                    for row in rows:
                        self.db.execute("INSERT OR IGNORE INTO seed_revision VALUES (?,?,?,?,?)", row)
                    self.db.execute("INSERT OR REPLACE INTO seed_status VALUES (?,?,?,1)",
                                    (seed_index, seed["seed_directory_id"], seed_node))
                self.db.execute("INSERT OR REPLACE INTO checkpoints VALUES (?,?,?,?,?)",
                                ("seed_revision", batch_no, marker_hash, len(batch), utc_now()))
                self.db.commit()
            except BaseException:
                self.db.rollback()
                raise
            done_total = self.db.execute("SELECT COUNT(*) FROM seed_status WHERE complete=1").fetchone()[0]
            revision_count = self.db.execute("SELECT COUNT(DISTINCT candidate_revision_node_id) FROM seed_revision").fetchone()[0]
            self.progress("seed_revision", done_total, total,
                          {"seed_revision_rows": self.db.execute("SELECT COUNT(*) FROM seed_revision").fetchone()[0],
                           "candidate_revision_nodes": revision_count}, force=True)
        seed_count = self.db.execute("SELECT COUNT(*) FROM seed_status WHERE complete=1").fetchone()[0]
        if seed_count != total:
            raise RuntimeError("seed revision stage incomplete")
        rows = self.db.execute("SELECT seed_directory_id,seed_node_id,root_directory_node_id,"
                               "candidate_revision_node_id,directory_hops FROM seed_revision "
                               "ORDER BY seed_directory_id, candidate_revision_node_id, root_directory_node_id").fetchall()
        put_parquet(self.root / "seed_revision.parquet", [dict(zip(SEED_REV_SCHEMA.names, row)) for row in rows], SEED_REV_SCHEMA)
        revisions = sorted({row[3] for row in rows})
        self.db.executemany("INSERT OR IGNORE INTO graph_nodes(node_id,node_type,status) VALUES (?,4,'pending')",
                            [(node,) for node in revisions])
        self.db.commit()
        self.emit("seed_revision_complete", seeds=seed_count, seed_revision_rows=len(rows),
                  candidate_revision_nodes=len(revisions))

    def _reader_pump(self):
        for line in self.reader.stdout:
            self.reader_messages.put(line)
        self.reader_messages.put(None)

    def start_reader(self):
        self.reader_stderr = (self.root / "reader.stderr.log").open("a", encoding="utf-8")
        self.reader = subprocess.Popen([str(self.args.binary), str(self.args.graph)],
                                       stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                       stderr=self.reader_stderr, text=True, bufsize=1)
        self.reader_thread = threading.Thread(target=self._reader_pump, daemon=True)
        self.reader_thread.start()
        while True:
            self.check()
            try:
                line = self.reader_messages.get(timeout=10)
            except queue.Empty:
                self.emit("reader_wait")
                continue
            if line is None:
                raise RuntimeError("graph reader exited before ready")
            value = json.loads(line)
            if value.get("event") == "ready":
                if value.get("nodes") != GRAPH_NODES:
                    raise RuntimeError("graph reader node count mismatch")
                self.state["reader_load_seconds"] = value["load_seconds"]
                self.emit("reader_ready", nodes=value["nodes"], arcs=value.get("arcs"),
                          load_seconds=value["load_seconds"])
                return

    def query_reader(self, nodes):
        self.check()
        self.reader.stdin.write(json.dumps({"op": "neighbors", "nodes": nodes}) + "\n")
        self.reader.stdin.flush()
        while True:
            self.check()
            try:
                line = self.reader_messages.get(timeout=30)
            except queue.Empty:
                self.emit("reader_wait", expected="neighbors", batch_nodes=len(nodes))
                continue
            if line is None:
                raise RuntimeError("graph reader exited during traversal")
            value = json.loads(line)
            if value.get("event") == "neighbors":
                results = value.get("results", [])
                if [row.get("node_id") for row in results] != nodes:
                    raise RuntimeError("graph reader returned a different batch")
                return results

    def traversal_stage(self):
        self.state["phase"] = "graph_traversal"
        self.phase_started = time.monotonic()
        if self.db.execute("SELECT 1 FROM graph_nodes WHERE status='pending' LIMIT 1").fetchone():
            self.start_reader()
        batches = 0
        while True:
            self.check()
            pending = self.db.execute("SELECT node_id FROM graph_nodes WHERE status='pending' "
                                      "ORDER BY node_id LIMIT ?", (self.args.batch_size,)).fetchall()
            if not pending:
                break
            nodes = [row[0] for row in pending]
            results = self.query_reader(nodes)
            self.db.execute("BEGIN")
            try:
                for result in results:
                    node = result["node_id"]
                    kind = result["node_type"]
                    if kind not in TRAVERSE_TYPES:
                        self.db.execute("UPDATE graph_nodes SET status='error', error=? WHERE node_id=?",
                                        ("unexpected node type {}".format(kind), node))
                        continue
                    if result.get("truncated"):
                        self.db.execute("UPDATE graph_nodes SET status='truncated',outdegree=?,truncated=1 WHERE node_id=?",
                                        (result.get("outdegree"), node))
                        continue
                    neighbors = result.get("neighbors", [])
                    if len(neighbors) != result.get("outdegree"):
                        raise RuntimeError("non-truncated reader result has incomplete neighbors")
                    for target in neighbors:
                        target_node = target["node_id"]
                        target_kind = target["node_type"]
                        self.db.execute("INSERT OR IGNORE INTO traversal_edges VALUES (?,?,?,?)",
                                        (node, target_node, kind, target_kind))
                        if target_kind == ORIGIN_NODE_TYPE:
                            self.db.execute("INSERT OR IGNORE INTO origins VALUES (?)", (target_node,))
                        elif target_kind in TRAVERSE_TYPES:
                            self.db.execute("INSERT OR IGNORE INTO graph_nodes(node_id,node_type,status) "
                                            "VALUES (?,?,'pending')", (target_node, target_kind))
                    self.db.execute("UPDATE graph_nodes SET status='done',outdegree=?,truncated=0 WHERE node_id=?",
                                    (result.get("outdegree"), node))
                self.db.commit()
            except BaseException:
                self.db.rollback()
                raise
            batches += 1
            if batches == 1 or time.monotonic() - self.last_progress >= 30:
                done = self.db.execute("SELECT COUNT(*) FROM graph_nodes WHERE status='done'").fetchone()[0]
                all_nodes = self.db.execute("SELECT COUNT(*) FROM graph_nodes").fetchone()[0]
                edges = self.db.execute("SELECT COUNT(*) FROM traversal_edges").fetchone()[0]
                origins = self.db.execute("SELECT COUNT(*) FROM origins").fetchone()[0]
                self.progress("graph_traversal", done, max(all_nodes, done),
                              {"queued_nodes": all_nodes, "pending_nodes": all_nodes - done,
                               "edges": edges, "origin_nodes": origins,
                               "rss_kib": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
                               "eta_seconds": None, "percent": None}, force=True)
        done = self.db.execute("SELECT COUNT(*) FROM graph_nodes WHERE status='done'").fetchone()[0]
        pending = self.db.execute("SELECT COUNT(*) FROM graph_nodes WHERE status='pending'").fetchone()[0]
        errors = self.db.execute("SELECT COUNT(*) FROM graph_nodes WHERE status IN ('error','truncated')").fetchone()[0]
        edges = self.db.execute("SELECT COUNT(*) FROM traversal_edges").fetchone()[0]
        origins = self.db.execute("SELECT COUNT(*) FROM origins").fetchone()[0]
        status = "passed" if pending == 0 and errors == 0 else "limited"
        write_json(self.root / "traversal-complete.json", {
            "status": status, "done_nodes": done, "pending_nodes": pending,
            "error_or_truncated_nodes": errors, "edges": edges, "origin_nodes": origins,
            "completed_at_utc": utc_now(),
        })
        self.emit("graph_traversal_complete", status=status, queried_nodes=done,
                  pending_nodes=pending, error_or_truncated_nodes=errors,
                  edges=edges, origin_nodes=origins)
        if self.reader is not None:
            self.reader.terminate()
            self.reader.wait(timeout=30)
            self.reader = None

    def map_origin_ids(self):
        self.state["phase"] = "origin_id_mapping"
        self.phase_started = time.monotonic()
        nodes = [row[0] for row in self.db.execute("SELECT origin_node_id FROM origins ORDER BY origin_node_id")]
        missing = [node for node in nodes if not self.db.execute(
            "SELECT 1 FROM origin_ids WHERE origin_node_id=?", (node,)).fetchone()]
        if not missing:
            self.emit("origin_id_mapping_complete", origin_nodes=len(nodes), mapped=len(nodes), missing=0)
            return
        spans = merged_ranges(missing, 22, max_gap=4096, max_span=1024 * 1024)
        self.budget.reserve(len(spans) * 3, sum(span[1] - span[0] + 1 for span in spans) * 3)
        identity = self.mapping_identity

        def fetch(span):
            start, end, members = span
            length = end - start + 1
            last_error = None
            for attempt in range(3):
                self.check()
                try:
                    with self.lock:
                        self.range_requests += 1
                    response = self.public.get_object(
                        Bucket=PUBLIC_BUCKET, Key=identity["key"],
                        Range="bytes={}-{}".format(start, end), IfMatch=identity["etag"])
                    with response["Body"] as body:
                        if (response["ResponseMetadata"]["HTTPStatusCode"] != 206 or
                                response["ContentLength"] != length or
                                response["ContentRange"] != "bytes {}-{}/{}".format(start, end, identity["size"])):
                            raise RuntimeError("invalid origin mapping Range response")
                        data = body.read(length + 1)
                    if len(data) != length:
                        raise RuntimeError("truncated origin mapping Range response")
                    with self.lock:
                        self.range_bytes += len(data)
                    return span, data
                except (BotoCoreError, ClientError, OSError, RuntimeError) as error:
                    last_error = error
                    if attempt < 2:
                        time.sleep(0.25 * (2 ** attempt))
            raise RuntimeError("origin mapping Range failed") from last_error

        results = {}
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.args.workers) as pool:
            pending = [pool.submit(fetch, span) for span in spans]
            for index, task in enumerate(concurrent.futures.as_completed(pending), 1):
                span, payload = task.result()
                start, end, members = span
                for node in members:
                    offset = node * 22 - start
                    record = payload[offset:offset + 22]
                    if len(record) != 22 or record[:2] != b"\x01\x02":
                        raise RuntimeError("origin node has invalid SWHID record: {}".format(node))
                    results[node] = record[2:].hex()
                self.db.executemany("INSERT OR REPLACE INTO origin_ids VALUES (?,?)",
                                    [(node, results[node]) for node in members])
                self.db.commit()
                self.progress("origin_id_mapping", len(results), len(missing),
                              {"spans_completed": index, "spans_total": len(spans)})
        self.db.execute("BEGIN")
        try:
            self.db.executemany("INSERT OR REPLACE INTO origin_ids VALUES (?,?)", sorted(results.items()))
            self.db.commit()
        except BaseException:
            self.db.rollback()
            raise
        self.emit("origin_id_mapping_complete", origin_nodes=len(nodes), mapped=len(results), missing=0)

    def configure_query_limit(self):
        config = {"BytesScannedCutoffPerQuery": SCAN_LIMIT,
                  # CTAS external_location is rejected when centralized
                  # output is enforced. The scan cutoff is a workgroup-only
                  # limit and remains effective with output overrides enabled.
                  "EnforceWorkGroupConfiguration": False,
                  "PublishCloudWatchMetricsEnabled": False,
                  "ResultConfiguration": {
                      "OutputLocation": "s3://" + BUCKET + "/" + S3_PREFIX + "athena-results/",
                      "ExpectedBucketOwner": BUCKET_OWNER}}
        try:
            current = self.athena.get_work_group(WorkGroup=WORKGROUP)["WorkGroup"]["Configuration"]
        except self.athena.exceptions.InvalidRequestException as error:
            message = error.response.get("Error", {}).get("Message", "").lower()
            if "not found" not in message and "does not exist" not in message:
                raise
            self.athena.create_work_group(Name=WORKGROUP, Configuration=config,
                                         Description="SWH origin mapping, 32 GiB per query")
        else:
            if (current.get("BytesScannedCutoffPerQuery") != SCAN_LIMIT or
                    current.get("EnforceWorkGroupConfiguration", False) or
                    current.get("ResultConfiguration", {}).get("OutputLocation") !=
                    config["ResultConfiguration"]["OutputLocation"]):
                raise RuntimeError("Dedicated Athena workgroup configuration changed")
        self.emit("query_limits_ready", workgroup=WORKGROUP, scan_limit_bytes=SCAN_LIMIT)

    def athena_query(self, name, sql, database):
        state_path = self.root / ("athena-" + name + ".json")
        output_location = "s3://" + BUCKET + "/" + S3_PREFIX + "athena-results/"
        digest = digest_json([database, sql, output_location, WORKGROUP])
        saved = json.loads(state_path.read_text(encoding="utf-8")) if state_path.exists() else None
        if saved and saved.get("sql_sha256") != digest:
            raise RuntimeError("Athena query changed during resume: " + name)
        if not saved:
            self.check()
            # A durable $1 allowance covers <=32 GiB at $5/TB, result storage
            # and request overhead. Never refund a failed query reservation.
            ledger_path = self.root / "query-budget.json"
            ledger = json.loads(ledger_path.read_text()) if ledger_path.exists() else {"reservations": {}}
            if digest not in ledger["reservations"]:
                if self.budget.cost() + sum(ledger["reservations"].values()) + 1 > self.budget.limit:
                    raise BudgetExceeded("Athena allowance would exceed authorized $100 budget")
                ledger["reservations"][digest] = 1.0
                write_json(ledger_path, ledger)
            self.budget.reserve(1)
            response = self.athena.start_query_execution(
                QueryString=sql, ClientRequestToken=digest,
                QueryExecutionContext={"Database": database}, WorkGroup=WORKGROUP,
                ResultConfiguration={"OutputLocation": output_location,
                                     "ExpectedBucketOwner": BUCKET_OWNER})
            saved = {"sql_sha256": digest, "execution_id": response["QueryExecutionId"],
                     "submitted_at_utc": utc_now()}
            write_json(state_path, saved)
        while True:
            try:
                self.check()
            except (TimeoutError, InterruptedError):
                self.athena.stop_query_execution(QueryExecutionId=saved["execution_id"])
                raise
            execution = self.athena.get_query_execution(QueryExecutionId=saved["execution_id"])["QueryExecution"]
            status = execution["Status"]["State"]
            if status in ("SUCCEEDED", "FAILED", "CANCELLED"):
                saved.update({"status": status, "completed_at_utc": utc_now(),
                              "scanned_bytes": execution.get("Statistics", {}).get("DataScannedInBytes", 0),
                              "reason": execution["Status"].get("StateChangeReason")})
                write_json(state_path, saved)
                self.emit("athena_query_done", name=name, status=status,
                          scanned_bytes=saved["scanned_bytes"], execution_id=saved["execution_id"])
                if status != "SUCCEEDED":
                    raise RuntimeError("Athena {} query {}".format(name, status))
                return saved["execution_id"]
            time.sleep(5)

    def resolve_urls(self):
        self.state["phase"] = "origin_url_lookup"
        self.phase_started = time.monotonic()
        pairs = self.db.execute("SELECT origin_node_id,origin_id FROM origin_ids ORDER BY origin_node_id").fetchall()
        if not pairs:
            raise RuntimeError("no origin IDs were resolved")
        run_token = self.data_identity[:16]
        self.configure_query_limit()
        input_path = self.root / "origin_hashes.parquet"
        put_parquet(input_path, [{"id": origin_hash(origin_id)} for _node, origin_id in pairs],
                    pa.schema([("id", pa.string())]))
        input_key = S3_PREFIX + "athena-input-sha1-v2/origin_hashes.parquet"
        self.private.upload_file(str(input_path), BUCKET, input_key,
                                 ExtraArgs={"ContentType": "application/octet-stream"},
                                 Config=TransferConfig(use_threads=False))
        self.emit("athena_input_uploaded", origin_ids=len(pairs), key=input_key)
        self.athena_query("database_v2", "CREATE DATABASE IF NOT EXISTS {}".format(ATHENA_DATABASE), "default")
        self.athena_query("origin_table_v2",
                          "CREATE EXTERNAL TABLE IF NOT EXISTS {}.{} (id string, url string) "
                          "STORED AS ORC LOCATION 's3://softwareheritage/graph/2026-06-04/orc/origin/'"
                          .format(ATHENA_DATABASE, ATHENA_ORIGIN_TABLE), ATHENA_DATABASE)
        stage_table = "origin_hashes_" + run_token
        self.athena_query("stage_hashes_v2",
                          "CREATE EXTERNAL TABLE IF NOT EXISTS {}.{} (id string) STORED AS PARQUET "
                          "LOCATION 's3://{}/{}athena-input-sha1-v2/'".format(ATHENA_DATABASE, stage_table, BUCKET, S3_PREFIX),
                          ATHENA_DATABASE)
        query = ("SELECT o.id, o.url FROM {db}.{origin} o JOIN {db}.{stage} i ON o.id=i.id"
                 .format(db=ATHENA_DATABASE, origin=ATHENA_ORIGIN_TABLE, stage=stage_table))
        query_id = self.athena_query("urls_sha1_v2", query, ATHENA_DATABASE)
        urls = []
        paginator = self.athena.get_paginator("get_query_results")
        for page in paginator.paginate(QueryExecutionId=query_id):
            for row in page["ResultSet"]["Rows"]:
                values = [cell.get("VarCharValue", "") for cell in row["Data"]]
                if len(values) == 2 and re.fullmatch("[0-9a-f]{40}", values[0]):
                    raw, url = values
                    if hashlib.sha1(url.encode("utf-8")).hexdigest() != raw:
                        raise RuntimeError("Origin URL SHA-1 does not match its origin ID")
                    urls.append((raw, url))
        if not urls:
            raise RuntimeError("URL query returned no matches; refusing to mark full mapping complete")
        self.db.executemany("INSERT OR IGNORE INTO origin_urls VALUES (?,?)", urls)
        self.db.commit()
        self.emit("origin_url_lookup_complete", origin_ids=len(pairs), origin_url_rows=len(urls))

    def export_sql(self, relative_path, sql, schema):
        """Bound memory while exporting potentially millions of evidence rows."""
        self.check()
        target = self.root / relative_path
        target.parent.mkdir(parents=True, exist_ok=True)
        marker = target.with_suffix(".complete.json")
        query_digest = digest_json([self.data_identity, sql, str(schema)])
        if marker.exists() and target.exists():
            saved = json.loads(marker.read_text())
            if saved["query_sha256"] == query_digest and saved["sha256"] == record_sha(target):
                return saved["rows"]
        temporary = target.with_suffix(".partial")
        cursor = self.db.execute(sql)
        rows = 0
        with pq.ParquetWriter(temporary, schema, compression="zstd") as writer:
            while True:
                self.check()
                batch = cursor.fetchmany(32768)
                if not batch:
                    break
                writer.write_table(pa.Table.from_pylist([dict(zip(schema.names, row)) for row in batch], schema=schema))
                rows += len(batch)
                if time.monotonic() - self.last_progress >= 30:
                    self.last_progress = time.monotonic()
                    self.emit("export_progress", path=relative_path, rows=rows, eta_seconds=None)
        os.replace(str(temporary), str(target))
        write_json(marker, {"query_sha256": query_digest, "sha256": record_sha(target), "rows": rows})
        return rows

    def materialize(self):
        self.state["phase"] = "materializing"
        output = self.root / "results"
        output.mkdir(parents=True, exist_ok=True)
        self.db.execute("CREATE INDEX IF NOT EXISTS seed_revision_revision ON seed_revision(candidate_revision_node_id)")
        self.db.execute("CREATE INDEX IF NOT EXISTS origin_urls_id ON origin_urls(origin_id)")
        self.db.commit()
        count_seed_origin = self.export_sql(
            "results/seed_origin/part-00000.parquet",
            "SELECT sr.seed_directory_id,sr.seed_node_id,sr.root_directory_node_id,"
            "sr.candidate_revision_node_id,n.origin_node_id,oi.origin_id,ou.origin_url,"
            "CASE WHEN ou.origin_url IS NULL THEN 'url_unresolved' ELSE 'mapped' END "
            "FROM seed_revision sr JOIN node_origins n ON sr.candidate_revision_node_id=n.node_id "
            "LEFT JOIN origin_ids oi ON n.origin_node_id=oi.origin_node_id "
            "LEFT JOIN origin_urls ou ON oi.origin_id=ou.origin_id", SEED_ORIGIN_SCHEMA)
        self.export_sql("results/origin_urls.parquet",
                       "SELECT oi.origin_node_id,oi.origin_id,'swh:1:ori:'||oi.origin_id,ou.origin_url,"
                       "CASE WHEN ou.origin_url IS NULL THEN 'url_unresolved' ELSE 'mapped' END "
                       "FROM origin_ids oi LEFT JOIN origin_urls ou ON oi.origin_id=ou.origin_id", ORIGIN_SCHEMA)
        self.export_sql("results/traversal_edges.parquet",
                       "SELECT source_node_id,target_node_id,source_type,target_type FROM traversal_edges", EDGE_SCHEMA)
        self.export_sql("results/revision_origin.parquet",
                       "SELECT n.node_id,n.origin_node_id FROM node_origins n "
                       "WHERE EXISTS (SELECT 1 FROM seed_revision sr WHERE sr.candidate_revision_node_id=n.node_id)",
                       pa.schema([("candidate_revision_node_id", pa.int64()), ("origin_node_id", pa.int64())]))
        with (output / "repository_urls.csv").open("w", encoding="utf-8", newline="") as handle:
            writer = csv.writer(handle)
            writer.writerow(["origin_id", "origin_url"])
            for row in self.db.execute("SELECT DISTINCT origin_id,origin_url FROM origin_urls ORDER BY origin_url"):
                writer.writerow(row)
        unresolved = []
        for node, reason in self.db.execute("SELECT node_id,status FROM graph_nodes WHERE status!='done'"):
            unresolved.append({"kind": "graph_node", "node_id": node, "reason": reason})
        for node in self.db.execute("SELECT origin_node_id FROM origins WHERE origin_node_id NOT IN "
                                    "(SELECT origin_node_id FROM origin_ids)"):
            unresolved.append({"kind": "origin_node", "node_id": node[0], "reason": "id_decode_failed"})
        for node, in self.db.execute("SELECT oi.origin_node_id FROM origin_ids oi WHERE NOT EXISTS "
                                    "(SELECT 1 FROM origin_urls ou WHERE ou.origin_id=oi.origin_id)"):
            unresolved.append({"kind": "origin_node", "node_id": node, "reason": "url_unresolved"})
        put_parquet(output / "unresolved.parquet", unresolved, UNRESOLVED_SCHEMA)
        seed_summary_schema = pa.schema([
            ("seed_directory_id", pa.string()), ("seed_node_id", pa.int64()),
            ("origin_count", pa.int64()), ("mapping_status", pa.string())])
        # Collapse URLs only for the per-seed status index. All revision/origin
        # evidence stays in seed_origin, including forks and shared content.
        self.db.executescript("""
            DROP TABLE IF EXISTS seed_summary;
            CREATE TABLE seed_summary AS
            SELECT s.seed_directory_id,s.seed_node_id,COUNT(DISTINCT ou.origin_url) AS origin_count,
                CASE WHEN COUNT(DISTINCT ou.origin_url)>0 THEN 'mapped'
                     WHEN MAX(sr.candidate_revision_node_id) IS NULL THEN 'no_revision_entry'
                     WHEN MAX(n.origin_node_id) IS NULL THEN 'exhausted_without_origin'
                     ELSE 'url_unresolved' END AS mapping_status
            FROM seed_status s LEFT JOIN seed_revision sr ON s.seed_directory_id=sr.seed_directory_id
            LEFT JOIN node_origins n ON sr.candidate_revision_node_id=n.node_id
            LEFT JOIN origin_ids oi ON n.origin_node_id=oi.origin_node_id
            LEFT JOIN origin_urls ou ON oi.origin_id=ou.origin_id
            GROUP BY s.seed_directory_id,s.seed_node_id;
        """)
        self.db.commit()
        self.export_sql("results/seed_summary/part-00000.parquet",
                       "SELECT seed_directory_id,seed_node_id,origin_count,mapping_status FROM seed_summary",
                       seed_summary_schema)
        by_status = dict(self.db.execute("SELECT mapping_status,COUNT(*) FROM seed_summary GROUP BY mapping_status"))
        scans = {}
        for path in self.root.glob("athena-*.json"):
            query = json.loads(path.read_text())
            if "execution_id" in query:
                scans[query["execution_id"]] = query.get("scanned_bytes", 0)
        summary = {
            "version": VERSION,
            "status": "passed" if not unresolved else "limited",
            "seed_revision_rows": self.db.execute("SELECT COUNT(*) FROM seed_revision").fetchone()[0],
            "candidate_revision_nodes": self.db.execute("SELECT COUNT(DISTINCT candidate_revision_node_id) FROM seed_revision").fetchone()[0],
            "graph_nodes": self.db.execute("SELECT COUNT(*) FROM graph_nodes").fetchone()[0],
            "traversal_edges": self.db.execute("SELECT COUNT(*) FROM traversal_edges").fetchone()[0],
            "origin_nodes": self.db.execute("SELECT COUNT(*) FROM origins").fetchone()[0],
            "origin_url_rows": self.db.execute("SELECT COUNT(*) FROM origin_urls").fetchone()[0],
            "seed_origin_rows": count_seed_origin,
            "seed_status_counts": by_status,
            "unresolved_rows": len(unresolved),
            "athena_scanned_bytes": sum(scans.values()),
            "athena_scan_estimate_usd_at_5_per_TB": round(sum(scans.values()) / 10**12 * 5, 6),
            "range_requests": self.range_requests,
            "range_bytes": self.range_bytes,
            "completed_at_utc": utc_now(),
            "identity": self.identity,
            "data_identity": self.data_identity,
            "first_started_at_utc": self.control["first_started_at_utc"],
            "deadline_utc": self.control["deadline_utc"],
            "elapsed_seconds_total": (dt.datetime.now(dt.timezone.utc) - dt.datetime.fromisoformat(
                self.control["first_started_at_utc"])).total_seconds(),
        }
        write_json(self.root / "complete.json", summary)
        self.emit("materialize_complete", **summary)
        return summary

    def build_node_origins(self):
        """Propagate origin reachability backwards through saved graph edges."""
        self.state["phase"] = "origin_reachability"
        self.phase_started = time.monotonic()
        marker_path = self.root / "reachability-complete.json"
        if marker_path.exists():
            marker = json.loads(marker_path.read_text())
            if (marker["pairs"] == self.db.execute("SELECT COUNT(*) FROM node_origins").fetchone()[0]
                    and marker["edges"] == self.db.execute("SELECT COUNT(*) FROM traversal_edges").fetchone()[0]
                    and not self.db.execute("SELECT 1 FROM origin_frontier LIMIT 1").fetchone()):
                self.emit("origin_reachability_resumed", node_origin_pairs=marker["pairs"])
                return
            raise RuntimeError("Completed origin reachability cache changed")
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS node_origins (
                node_id INTEGER NOT NULL, origin_node_id INTEGER NOT NULL,
                PRIMARY KEY(node_id, origin_node_id)
            );
            CREATE INDEX IF NOT EXISTS node_origins_origin ON node_origins(origin_node_id);
            CREATE TABLE IF NOT EXISTS origin_frontier (
                node_id INTEGER NOT NULL, origin_node_id INTEGER NOT NULL,
                PRIMARY KEY(node_id, origin_node_id)
            );
            CREATE TEMP TABLE IF NOT EXISTS origin_candidates (
                node_id INTEGER NOT NULL, origin_node_id INTEGER NOT NULL,
                PRIMARY KEY(node_id, origin_node_id)
            );
        """)
        existing_pairs = self.db.execute("SELECT COUNT(*) FROM node_origins").fetchone()[0]
        if not existing_pairs:
            self.db.execute("INSERT OR IGNORE INTO node_origins SELECT origin_node_id,origin_node_id FROM origins")
            self.db.execute("INSERT OR IGNORE INTO origin_frontier SELECT origin_node_id,origin_node_id FROM origins")
        self.db.commit()
        iteration = 0
        while self.db.execute("SELECT 1 FROM origin_frontier LIMIT 1").fetchone():
            self.check()
            self.db.execute("BEGIN")
            try:
                self.db.execute("CREATE TEMP TABLE IF NOT EXISTS next_frontier "
                                "(node_id INTEGER NOT NULL, origin_node_id INTEGER NOT NULL, "
                                "PRIMARY KEY(node_id,origin_node_id))")
                self.db.execute("DELETE FROM next_frontier")
                self.db.execute("DELETE FROM origin_candidates")
                self.db.execute("INSERT OR IGNORE INTO origin_candidates "
                                "SELECT e.source_node_id,f.origin_node_id FROM traversal_edges e "
                                "JOIN origin_frontier f ON e.target_node_id=f.node_id")
                self.db.execute("INSERT OR IGNORE INTO next_frontier "
                                "SELECT c.node_id,c.origin_node_id FROM origin_candidates c "
                                "WHERE NOT EXISTS (SELECT 1 FROM node_origins n "
                                "WHERE n.node_id=c.node_id AND n.origin_node_id=c.origin_node_id)")
                added = self.db.execute("SELECT COUNT(*) FROM next_frontier").fetchone()[0]
                self.db.execute("INSERT OR IGNORE INTO node_origins SELECT node_id,origin_node_id FROM next_frontier")
                self.db.execute("DELETE FROM origin_frontier")
                self.db.execute("INSERT INTO origin_frontier SELECT node_id,origin_node_id FROM next_frontier")
                self.db.commit()
            except BaseException:
                self.db.rollback()
                raise
            iteration += 1
            reached = self.db.execute("SELECT COUNT(*) FROM node_origins").fetchone()[0]
            if time.monotonic() - self.last_progress >= 30 or added == 0:
                self.last_progress = time.monotonic()
                self.emit("reachability_progress", iterations=iteration,
                          new_pairs=added, node_origin_pairs=reached, eta_seconds=None)
            if added == 0:
                break
        # Validate every saved edge before adopting the pre-fix completed cache
        # or writing a completion marker. CROSS JOIN retains the small frontier
        # as the outer loop instead of scanning all edges on every iteration.
        missing = self.db.execute(
            "SELECT e.source_node_id FROM node_origins n CROSS JOIN traversal_edges e "
            "ON e.target_node_id=n.node_id WHERE NOT EXISTS (SELECT 1 FROM node_origins p "
            "WHERE p.node_id=e.source_node_id AND p.origin_node_id=n.origin_node_id) LIMIT 1").fetchone()
        missing_base = self.db.execute(
            "SELECT origin_node_id FROM origins o WHERE NOT EXISTS (SELECT 1 FROM node_origins n "
            "WHERE n.node_id=o.origin_node_id AND n.origin_node_id=o.origin_node_id) LIMIT 1").fetchone()
        if missing or missing_base:
            raise RuntimeError("Origin reachability does not cover all saved edges")
        write_json(marker_path, {"pairs": self.db.execute("SELECT COUNT(*) FROM node_origins").fetchone()[0],
                                 "edges": self.db.execute("SELECT COUNT(*) FROM traversal_edges").fetchone()[0],
                                 "completed_at_utc": utc_now()})
        self.emit("origin_reachability_complete", iterations=iteration,
                  node_origin_pairs=self.db.execute("SELECT COUNT(*) FROM node_origins").fetchone()[0])

    def query_rows(self, query_id):
        first = True
        for page in self.athena.get_paginator("get_query_results").paginate(QueryExecutionId=query_id):
            for row in page["ResultSet"]["Rows"]:
                if first:
                    first = False
                    continue
                yield [cell.get("VarCharValue") for cell in row["Data"]]

    def file_seed_stage(self, summary):
        """Preserve all candidate rows and expose their full origin join as a view.

        Physically multiplying 13M files by every historical revision would
        waste storage. Store files and evidence separately with exact D0 keys;
        the view reconstructs all file/revision/origin rows without information
        loss or content-based deduplication. Athena limits CTAS to 100 partitions,
        so produce two independently resumable groups of 64 source CSVs.
        """
        self.state["phase"] = "candidate_file_mapping"
        self.emit("file_mapping_started", source_csvs=128, expected_rows=13465757)
        inventory_path = self.root / "candidate-source-inventory.json"
        inventory = json.loads(inventory_path.read_text())
        source_prefix = "file-screen/swh-p4-20260604/full-128-20261020/filtered_v2/candidate_csv/"
        current = []
        for page in self.private.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=source_prefix):
            current.extend({"key": obj["Key"], "bytes": obj["Size"], "etag": obj["ETag"]}
                           for obj in page.get("Contents", []) if obj["Key"].endswith(".csv"))
        if sorted(current, key=lambda row: row["key"]) != sorted(inventory, key=lambda row: row["key"]):
            raise RuntimeError("Candidate CSV inventory changed since the verified input inventory")
        shards = sorted(int(re.search(r"source_shard=(\d+)", obj["key"]).group(1)) for obj in current)
        if shards != list(range(128)):
            raise RuntimeError("Candidate input must have exactly one CSV for each shard 0..127")
        columns = json.loads((self.root / "candidate-source-columns.json").read_text())
        if not columns or any(not re.fullmatch("[a-z_][a-z_0-9]*", name) for name in columns):
            raise RuntimeError("Invalid candidate CSV schema")
        token = self.data_identity[:16]
        source_table = "p4_candidates_" + token
        summary_table = "p4_seed_summary_" + token
        summary_path = self.root / "results/seed_summary/part-00000.parquet"
        self.private.upload_file(str(summary_path), BUCKET, S3_PREFIX + "athena-seed-summary/part-00000.parquet",
                                 ExtraArgs={"ChecksumAlgorithm": "SHA256", "ExpectedBucketOwner": BUCKET_OWNER})
        source_ddl = (
            "CREATE EXTERNAL TABLE IF NOT EXISTS {db}.{table} ({columns}) "
            "ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' "
            "WITH SERDEPROPERTIES ('separatorChar'= ',', 'quoteChar'= '\"') "
            "STORED AS TEXTFILE LOCATION 's3://{bucket}/{prefix}' TBLPROPERTIES ('skip.header.line.count'='1')"
        ).format(db=ATHENA_DATABASE, table=source_table, columns=",".join("`" + name + "` string" for name in columns),
                 bucket=BUCKET, prefix=source_prefix)
        self.athena_query("candidate_source_table_v2", source_ddl, ATHENA_DATABASE)
        self.athena_query("seed_summary_table_v2",
                          "CREATE EXTERNAL TABLE IF NOT EXISTS {db}.{table} "
                          "(seed_directory_id string, seed_node_id bigint, origin_count bigint, mapping_status string) "
                          "STORED AS PARQUET LOCATION 's3://{bucket}/{prefix}athena-seed-summary/'".format(
                              db=ATHENA_DATABASE, table=summary_table, bucket=BUCKET, prefix=S3_PREFIX), ATHENA_DATABASE)
        tables = []
        for start, end in ((0, 63), (64, 127)):
            table = "p4_file_seed_{}_{}_{}".format(start, end, token)
            location = "s3://" + BUCKET + "/" + S3_PREFIX + "file_seed/batch_{:03d}_{:03d}/".format(start, end)
            shard_expr = "CAST(regexp_extract(c.\"$path\", 'source_shard=([0-9]+)', 1) AS integer)"
            sql = (
                "CREATE TABLE {db}.{out} WITH (format='PARQUET', write_compression='ZSTD', "
                "external_location='{location}', partitioned_by=ARRAY['source_shard']) AS "
                "SELECT c.*, s.seed_node_id, s.origin_count, "
                "COALESCE(s.mapping_status, 'seed_not_in_index') AS mapping_status, {shard} AS source_shard "
                "FROM {db}.{source} c LEFT JOIN {db}.{seeds} s ON c.directory_id=s.seed_directory_id "
                "WHERE {shard} BETWEEN {start} AND {end}"
            ).format(db=ATHENA_DATABASE, out=table, location=location, shard=shard_expr,
                     source=source_table, seeds=summary_table, start=start, end=end)
            self.athena_query("file_seed_{}_{}_v2".format(start, end), sql, ATHENA_DATABASE)
            tables.append(table)
            self.emit("file_mapping_batch_complete", first_shard=start, last_shard=end, location=location)
        union = " UNION ALL ".join("SELECT * FROM {}.{}".format(ATHENA_DATABASE, table) for table in tables)
        stats = self.athena_query("file_mapping_counts_v2",
                                 "SELECT source_shard,mapping_status,COUNT(*) FROM (" + union + ") "
                                 "GROUP BY source_shard,mapping_status", ATHENA_DATABASE)
        counts = list(self.query_rows(stats))
        total_rows = sum(int(row[2]) for row in counts)
        if total_rows != 13465757 or {int(row[0]) for row in counts} != set(range(128)):
            raise RuntimeError("Full candidate row count or 128-shard coverage differs from input")
        if any(row[1] == "seed_not_in_index" for row in counts):
            raise RuntimeError("Candidate D0 is absent from the verified seed index")
        evidence_table = "p4_seed_origin_" + token
        self.athena_query("seed_origin_table_v2",
                          "CREATE EXTERNAL TABLE IF NOT EXISTS {db}.{table} "
                          "(seed_directory_id string,seed_node_id bigint,root_directory_node_id bigint,"
                          "candidate_revision_node_id bigint,origin_node_id bigint,origin_id string,"
                          "origin_url string,status string) STORED AS PARQUET "
                          "LOCATION 's3://{bucket}/{prefix}results/seed_origin/'".format(
                              db=ATHENA_DATABASE, table=evidence_table, bucket=BUCKET, prefix=S3_PREFIX), ATHENA_DATABASE)
        view_name = "p4_file_origin_" + token
        view_sql = (
            "CREATE OR REPLACE VIEW {db}.{view} AS SELECT f.*,e.root_directory_node_id,"
            "e.candidate_revision_node_id,e.origin_node_id,e.origin_id,e.origin_url "
            "FROM ({union}) f LEFT JOIN {db}.{evidence} e ON f.directory_id=e.seed_directory_id"
        ).format(db=ATHENA_DATABASE, view=view_name, union=union, evidence=evidence_table)
        self.athena_query("file_origin_view_v2", view_sql, ATHENA_DATABASE)
        (self.root / "results/file_origin_view.sql").write_text(view_sql + ";\n")
        summary.update(candidate_file_rows=total_rows, candidate_file_csv_shards=128,
                       file_origin_view=ATHENA_DATABASE + "." + view_name,
                       file_seed_s3_prefix="s3://" + BUCKET + "/" + S3_PREFIX + "file_seed/",
                       file_shard_status_counts=counts)
        summary["athena_scanned_bytes"] = self.total_scanned_bytes()
        summary["athena_scan_estimate_usd_at_5_per_TB"] = round(summary["athena_scanned_bytes"] / 10**12 * 5, 6)
        summary["completed_at_utc"] = utc_now()
        write_json(self.root / "complete.json", summary)
        self.emit("file_mapping_complete", candidate_rows=total_rows, source_csvs=128,
                  athena_view=summary["file_origin_view"])

    def total_scanned_bytes(self):
        scans = {}
        for path in self.root.glob("athena-*.json"):
            record = json.loads(path.read_text())
            if "execution_id" in record:
                scans[record["execution_id"]] = record.get("scanned_bytes", 0)
        return sum(scans.values())

    def archive(self, status, enforce_deadline=True):
        self.state["phase"] = "archiving"
        self.db.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        self.db.commit()
        snapshot = self.root / "archive-snapshot"
        snapshot.mkdir(exist_ok=True)
        for name in ("status.json", "events.jsonl", "run.log", "reader.stderr.log"):
            path = self.root / name
            if path.exists():
                shutil.copyfile(path, snapshot / name)
        files = sorted(path for path in self.root.rglob("*") if path.is_file() and (
            path.suffix in (".parquet", ".json", ".sql", ".md") or
            snapshot in path.parents or path.name == "state.sqlite3") and
            path.name not in ("status.json", "events.jsonl", "archive.json", "complete.json") and
            not path.name.endswith(".tmp"))
        files.extend(path for path in snapshot.iterdir() if path.name in ("status.json", "events.jsonl"))
        if sum(path.stat().st_size for path in files) > 50 * 1024**3:
            raise BudgetExceeded("Archive exceeds the 50 GiB task storage allowance")
        archived = []
        transfer = TransferConfig(max_concurrency=8, multipart_threshold=64 * 1024**2,
                                  multipart_chunksize=16 * 1024**2)
        for path in files:
            if not path.exists():
                continue
            if enforce_deadline:
                self.check()
            key = S3_PREFIX + path.relative_to(self.root).as_posix()
            digest = record_sha(path)
            # Each artifact is immutable during this phase. Completed uploads
            # are reused on resume when their size and SHA metadata match.
            self.private.upload_file(str(path), BUCKET, key,
                                     ExtraArgs={"Metadata": {"sha256": digest},
                                                "ServerSideEncryption": "AES256",
                                                "ExpectedBucketOwner": BUCKET_OWNER,
                                                "ChecksumAlgorithm": "SHA256"}, Config=transfer)
            head = self.private.head_object(Bucket=BUCKET, Key=key, ChecksumMode="ENABLED",
                                            ExpectedBucketOwner=BUCKET_OWNER)
            remote = head.get("ChecksumSHA256", "")
            expected = base64.b64encode(bytes.fromhex(digest)).decode("ascii")
            if "-" in remote:
                pieces = []
                with path.open("rb") as handle:
                    for block in iter(lambda: handle.read(16 * 1024**2), b""):
                        pieces.append(hashlib.sha256(block).digest())
                expected = base64.b64encode(hashlib.sha256(b"".join(pieces)).digest()).decode("ascii")
                expected += "-" + str(len(pieces))
            if (head["ContentLength"] != path.stat().st_size or
                    head.get("Metadata", {}).get("sha256") != digest or remote != expected):
                raise RuntimeError("archive size or checksum mismatch: " + str(path))
            archived.append({"key": key, "bytes": path.stat().st_size, "sha256": digest,
                             "remote_checksum_verified": True})
            self.emit("archive_progress", uploaded=len(archived), total=len(files), status=status)
        write_json(self.root / "archive.json", {"status": status, "completed_at_utc": utc_now(),
                                                 "objects": archived})
        for name in ("archive.json", "complete.json"):
            path = self.root / name
            if path.exists():
                self.private.upload_file(str(path), BUCKET, S3_PREFIX + name,
                                         ExtraArgs={"ChecksumAlgorithm": "SHA256", "ExpectedBucketOwner": BUCKET_OWNER})
                with self.private.get_object(Bucket=BUCKET, Key=S3_PREFIX + name,
                                             ExpectedBucketOwner=BUCKET_OWNER)["Body"] as body:
                    if body.read() != path.read_bytes():
                        raise RuntimeError("Final archive marker readback failed: " + name)

    def close(self):
        if self.reader is not None:
            if self.reader.poll() is None:
                self.reader.terminate()
                try:
                    self.reader.wait(timeout=30)
                except subprocess.TimeoutExpired:
                    self.reader.kill()
                    self.reader.wait(timeout=30)
            if self.reader_stderr is not None:
                self.reader_stderr.close()
            self.reader = None
        self.db.commit()
        self.db.close()
        self.run_lock.close()

    def run(self):
        self.preflight()
        self.seed_revision_stage()
        self.traversal_stage()
        if self.db.execute("SELECT COUNT(*) FROM graph_nodes WHERE status!='done'").fetchone()[0]:
            raise RuntimeError("graph traversal has unresolved nodes")
        self.map_origin_ids()
        self.resolve_urls()
        self.build_node_origins()
        summary = self.materialize()
        self.file_seed_stage(summary)
        self.archive(summary["status"])
        self.state["phase"] = "complete"
        self.emit("run_complete", status=summary["status"], uploaded=True)
        return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--graph", type=Path, default=GRAPH_ROOT)
    parser.add_argument("--merge", type=Path, default=MERGE_ROOT)
    parser.add_argument("--output", type=Path, default=OUTPUT_ROOT)
    parser.add_argument("--binary", type=Path,
                        default=GRAPH_ROOT / "route-a-revision-origin-v1/build/release/origin")
    parser.add_argument("--workers", type=int, default=32)
    parser.add_argument("--batch-size", type=int, default=512)
    parser.add_argument("--seed-batch", type=int, default=512)
    parser.add_argument("--max-hours", type=float, default=12)
    parser.add_argument("--aws-budget-usd", type=float, default=100)
    args = parser.parse_args()
    if not 1 <= args.workers <= 64 or not 1 <= args.batch_size <= 512:
        parser.error("workers must be 1..64 and batch-size must be 1..512")
    if not 0 < args.max_hours <= 12 or not 0 < args.aws_budget_usd <= 100 or args.seed_batch <= 0:
        parser.error("max-hours must be <=12, aws-budget-usd <=100 and seed-batch positive")
    runner = FullOriginRun(args)
    for signum in (signal.SIGINT, signal.SIGTERM):
        signal.signal(signum, lambda _signum, _frame: runner.stop.set())
    exit_code = 0
    try:
        exit_code = runner.run()
    except (TimeoutError, BudgetExceeded, InterruptedError, KeyboardInterrupt) as error:
        runner.state["phase"] = "stopped"
        runner.emit("run_stopped", error_type=type(error).__name__, error=str(error))
        # Do not start another expensive phase after the original deadline.
        # SQLite commits and all exported shards remain locally resumable.
        exit_code = 2
    except Exception as error:
        runner.state["phase"] = "failed"
        runner.emit("run_failed", error_type=type(error).__name__, error=str(error))
        exit_code = 1
    finally:
        runner.close()
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
