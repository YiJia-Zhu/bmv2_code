#!/usr/bin/env python3
"""Resolve candidate revision node IDs to SWHIDs with resumable S3 ranges.

The full origin run deliberately kept graph node IDs separate from SWHIDs.
This stage reads only the 22-byte records needed for the 317,804 candidate
revision nodes from the public ``node2swhid.bin`` object.  Nearby records are
coalesced into bounded ranges; completed range batches are committed in
SQLite before the derived Parquet files are written.
"""

import argparse
import base64
import concurrent.futures
import datetime as dt
import fcntl
import hashlib
import json
import os
import sqlite3
import subprocess
import sys
import threading
import time
from pathlib import Path

import boto3
from boto3.s3.transfer import TransferConfig
from botocore import UNSIGNED
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError
import pyarrow as pa
import pyarrow.parquet as pq

from run_swh_route_a_sample import GRAPH_PREFIX, NODE_TYPES, merged_ranges, sha256, utc_now, write_json


VERSION = "route-a-revision-resolve-v1"
GRAPH_RELEASE = "2026-06-04"
GRAPH_NODES = 58675740295
NODE_WIDTH = 22
NODE2SWHID_KEY = GRAPH_PREFIX + "graph.node2swhid.bin"
PUBLIC_BUCKET = "softwareheritage"
BUCKET = "swh2p4"
BUCKET_OWNER = "728516300718"
AWS_ACCOUNT = "588301174757"
GRAPH_ROOT = Path("/mnt/8T/xgr/swh-graph-poc-20260604")
MERGE_ROOT = GRAPH_ROOT / "route-a-revision-origin-v1/merge"
INPUT_ROOT = GRAPH_ROOT / "route-a-revision-origin-v1/full"
OUTPUT_ROOT = GRAPH_ROOT / "route-a-revision-origin-v1/revision-resolve-v1"
S3_PREFIX = "new-account-588301174757/route-a/revision-resolve-v1/"

REVISION_SCHEMA = pa.schema([
    ("revision_node_id", pa.int64()),
    ("revision_swhid", pa.string()),
    ("revision_id", pa.string()),
    ("mapping_status", pa.string()),
])
DIRECTORY_REVISION_SCHEMA = pa.schema([
    ("directory_node_id", pa.int64()),
    ("revision_node_id", pa.int64()),
    ("revision_swhid", pa.string()),
    ("revision_id", pa.string()),
])
SEED_REVISION_SCHEMA = pa.schema([
    ("seed_directory_id", pa.string()),
    ("seed_node_id", pa.int64()),
    ("root_directory_node_id", pa.int64()),
    ("candidate_revision_node_id", pa.int64()),
    ("candidate_revision_swhid", pa.string()),
    ("candidate_revision_id", pa.string()),
    ("directory_hops", pa.int32()),
])
REVISION_ORIGIN_SCHEMA = pa.schema([
    ("candidate_revision_node_id", pa.int64()),
    ("candidate_revision_swhid", pa.string()),
    ("candidate_revision_id", pa.string()),
    ("origin_node_id", pa.int64()),
    ("origin_id", pa.string()),
    ("origin_url", pa.string()),
])


def digest_json(value):
    payload = json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def historical_range_counters(path):
    """Recover prior process counters from the append-only event log."""
    if not path.exists():
        return 0, 0
    totals = [0, 0]
    current = [0, 0]
    started = False
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            event = json.loads(line)
            if event.get("event") == "mapping_start":
                if started:
                    totals[0] += current[0]
                    totals[1] += current[1]
                current = [0, 0]
                started = True
            elif started and event.get("event") in ("progress", "mapping_complete"):
                requests = event.get("range_requests_run", event.get("range_requests", 0))
                bytes_read = event.get("range_bytes_run", event.get("range_bytes", 0))
                current[0] = max(current[0], requests)
                current[1] = max(current[1], bytes_read)
    if started:
        totals[0] += current[0]
        totals[1] += current[1]
    return tuple(totals)


def private_session():
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

    import botocore.session
    from botocore.credentials import RefreshableCredentials
    raw = botocore.session.get_session()
    credentials = RefreshableCredentials.create_from_metadata(
        metadata=refresh(), refresh_using=refresh, method="aws-cli-login")
    credentials._advisory_refresh_timeout = 60
    credentials._mandatory_refresh_timeout = 30
    raw._credentials = credentials
    return boto3.Session(botocore_session=raw, region_name="us-east-1")


def put_parquet(path, rows, schema):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".partial")
    table = pa.Table.from_pylist(rows, schema=schema)
    pq.write_table(table, temporary, compression="zstd")
    os.replace(str(temporary), str(path))


def check_record(node_id, record):
    if len(record) != NODE_WIDTH:
        raise RuntimeError("node {} returned {} bytes, expected 22".format(node_id, len(record)))
    if record[0] != 1:
        raise RuntimeError("node {} has unsupported SWHID version {}".format(node_id, record[0]))
    kind = record[1]
    if kind != NODE_TYPES.index("rev"):
        label = NODE_TYPES[kind] if kind < len(NODE_TYPES) else "unknown"
        raise RuntimeError("node {} is {}, expected revision".format(node_id, label))
    revision_id = record[2:].hex()
    return "swh:1:rev:" + revision_id, revision_id


class RevisionResolver:
    def __init__(self, args):
        self.args = args
        self.root = args.output
        self.root.mkdir(parents=True, exist_ok=True)
        self.lock = (self.root / "run.lock").open("a")
        fcntl.flock(self.lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        self.prior_request_count, self.prior_range_bytes = historical_range_counters(
            self.root / "events.jsonl")
        self.started = time.monotonic()
        self.last_progress = 0.0
        self.request_count = 0
        self.range_bytes = 0
        self.request_lock = threading.Lock()
        self.public = boto3.Session(region_name="us-east-1").client(
            "s3", endpoint_url="https://s3.amazonaws.com",
            config=Config(signature_version=UNSIGNED, proxies={},
                          s3={"addressing_style": "virtual"},
                          retries={"total_max_attempts": 1, "mode": "standard"},
                          connect_timeout=8, read_timeout=30,
                          max_pool_connections=args.workers))
        # The mapping itself is public-S3-only. Delay private credentials until
        # the optional archive phase so an expired login cannot block resume.
        self.private = None
        self.db = sqlite3.connect(str(self.root / "state.sqlite3"), timeout=120)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=NORMAL")
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS revision_mapping (
                revision_node_id INTEGER PRIMARY KEY,
                revision_swhid TEXT NOT NULL,
                revision_id TEXT NOT NULL,
                mapping_status TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS range_batches (
                batch_no INTEGER PRIMARY KEY,
                start_byte INTEGER NOT NULL,
                end_byte INTEGER NOT NULL,
                record_count INTEGER NOT NULL,
                input_sha256 TEXT NOT NULL,
                completed_at_utc TEXT NOT NULL
            );
        """)
        self.db.commit()
        self.control_path = self.root / "control.json"
        if self.control_path.exists():
            self.control = json.loads(self.control_path.read_text())
            if self.control.get("workers") != args.workers or self.control.get("batch_spans") != args.batch_spans:
                raise RuntimeError("resume parameters changed; use the original worker and batch settings")
        else:
            self.control = {
                "version": VERSION, "graph_release": GRAPH_RELEASE,
                "started_at_utc": utc_now(), "workers": args.workers,
                "batch_spans": args.batch_spans,
            }
            write_json(self.control_path, self.control)

    def emit(self, event, **fields):
        row = dict(fields, event=event, timestamp_utc=utc_now(),
                   elapsed_seconds=round(time.monotonic() - self.started, 3))
        print(json.dumps(row, sort_keys=True), flush=True)
        with (self.root / "events.jsonl").open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(row, sort_keys=True) + "\n")
        write_json(self.root / "status.json", row)

    def source_identity(self):
        response = self.public.head_object(Bucket=PUBLIC_BUCKET, Key=NODE2SWHID_KEY)
        expected_size = GRAPH_NODES * NODE_WIDTH
        if response["ContentLength"] != expected_size:
            raise RuntimeError("node2swhid size mismatch")
        return {"bucket": PUBLIC_BUCKET, "key": NODE2SWHID_KEY,
                "etag": response["ETag"], "size": response["ContentLength"]}

    def load_inputs(self):
        source = self.source_identity()
        merge_marker = json.loads((MERGE_ROOT / "complete.json").read_text())
        if merge_marker.get("status") != "passed" or merge_marker.get("unique_revision_nodes") != 317804:
            raise RuntimeError("merge input is not the verified 317,804-revision result")
        directory_revision = pq.read_table(MERGE_ROOT / "directory_revision.parquet").to_pylist()
        revision_nodes = sorted({row["revision_node_id"] for row in directory_revision})
        if len(revision_nodes) != 317804:
            raise RuntimeError("directory_revision does not contain 317,804 unique revisions")
        identity = {"version": VERSION, "graph_release": GRAPH_RELEASE,
                    "source": source, "merge_complete_sha256": sha256(MERGE_ROOT / "complete.json"),
                    "directory_revision_sha256": sha256(MERGE_ROOT / "directory_revision.parquet"),
                    "revision_nodes": len(revision_nodes), "workers": self.args.workers,
                    "batch_spans": self.args.batch_spans}
        path = self.root / "identity.json"
        if path.exists() and json.loads(path.read_text()) != identity:
            raise RuntimeError("revision resolve input identity changed")
        write_json(path, identity)
        self.identity = digest_json(identity)
        self.directory_revision = directory_revision
        return revision_nodes, source

    def fetch_range(self, span, source):
        start, end, members = span
        length = end - start + 1
        last_error = None
        for attempt in range(3):
            try:
                with self.request_lock:
                    self.request_count += 1
                response = self.public.get_object(
                    Bucket=source["bucket"], Key=source["key"],
                    Range="bytes={}-{}".format(start, end), IfMatch=source["etag"])
                with response["Body"] as body:
                    if (response["ResponseMetadata"]["HTTPStatusCode"] != 206 or
                            response["ContentLength"] != length or
                            response["ContentRange"] != "bytes {}-{}/{}".format(start, end, source["size"])):
                        raise RuntimeError("invalid node2swhid Range response")
                    payload = body.read(length + 1)
                if len(payload) != length:
                    raise RuntimeError("truncated node2swhid Range response")
                with self.request_lock:
                    self.range_bytes += len(payload)
                result = []
                for node in members:
                    offset = node * NODE_WIDTH - start
                    result.append((node, payload[offset:offset + NODE_WIDTH]))
                return span, result
            except (BotoCoreError, ClientError, OSError, RuntimeError) as error:
                last_error = error
                if attempt < 2:
                    time.sleep(0.25 * (2 ** attempt))
        raise RuntimeError("node2swhid Range request failed for {}-{}".format(start, end)) from last_error

    def progress(self, completed, total, spans_done, spans_total):
        now = time.monotonic()
        if now - self.last_progress < 15 and completed < total:
            return
        self.last_progress = now
        elapsed = max(now - self.started, 0.001)
        rate = completed / elapsed
        eta = (total - completed) / rate if rate else None
        self.emit("progress", stage="revision_id_mapping", completed=completed,
                  total=total, percent=round(100 * completed / total, 3),
                  rate_per_second=round(rate, 3), eta_seconds=round(eta) if eta else None,
                  spans_completed=spans_done, spans_total=spans_total,
                  range_requests=self.prior_request_count + self.request_count,
                  range_bytes=self.prior_range_bytes + self.range_bytes,
                  range_requests_run=self.request_count, range_bytes_run=self.range_bytes)

    def resolve(self, nodes, source):
        spans = merged_ranges(nodes, NODE_WIDTH, max_gap=self.args.max_gap,
                              max_span=self.args.max_span)
        self.emit("mapping_start", revision_nodes=len(nodes), ranges=len(spans),
                  estimated_range_bytes=sum(span[1] - span[0] + 1 for span in spans),
                  max_gap=self.args.max_gap, max_span=self.args.max_span)
        total = len(nodes)
        done_nodes = self.db.execute("SELECT COUNT(*) FROM revision_mapping").fetchone()[0]
        done_batches = {row[0] for row in self.db.execute("SELECT batch_no FROM range_batches")}
        for batch_no in range(0, len(spans), self.args.batch_spans):
            batch = spans[batch_no:batch_no + self.args.batch_spans]
            if batch_no in done_batches:
                continue
            expected_hash = digest_json([[span[0], span[1], len(span[2])] for span in batch])
            results = []
            with concurrent.futures.ThreadPoolExecutor(max_workers=self.args.workers) as pool:
                futures = [pool.submit(self.fetch_range, span, source) for span in batch]
                for future in concurrent.futures.as_completed(futures):
                    _span, records = future.result()
                    results.extend(records)
            rows = []
            for node, record in results:
                swhid, revision_id = check_record(node, record)
                rows.append((node, swhid, revision_id, "mapped"))
            rows.sort()
            self.db.execute("BEGIN")
            try:
                self.db.executemany("INSERT OR REPLACE INTO revision_mapping VALUES (?,?,?,?)", rows)
                self.db.execute("INSERT INTO range_batches VALUES (?,?,?,?,?,?)",
                                (batch_no, batch[0][0], batch[-1][1], len(rows), expected_hash, utc_now()))
                self.db.commit()
            except BaseException:
                self.db.rollback()
                raise
            done_nodes += len(rows)
            self.progress(done_nodes, total, batch_no + len(batch), len(spans))
        mapped = self.db.execute("SELECT COUNT(*) FROM revision_mapping WHERE mapping_status='mapped'").fetchone()[0]
        if mapped != total:
            raise RuntimeError("revision mapping incomplete: {}/{}".format(mapped, total))
        self.emit("mapping_complete", revision_nodes=total, mapped=mapped,
                  ranges=len(spans),
                  range_requests=self.prior_request_count + self.request_count,
                  range_bytes=self.prior_range_bytes + self.range_bytes,
                  range_requests_run=self.request_count, range_bytes_run=self.range_bytes)
        return {row[0]: (row[1], row[2]) for row in self.db.execute(
            "SELECT revision_node_id,revision_swhid,revision_id FROM revision_mapping")}

    def export(self, mapping):
        revision_rows = [{"revision_node_id": node, "revision_swhid": values[0],
                          "revision_id": values[1], "mapping_status": "mapped"}
                         for node, values in sorted(mapping.items())]
        put_parquet(self.root / "revision_nodes.parquet", revision_rows, REVISION_SCHEMA)
        directory_rows = [{"directory_node_id": row["directory_node_id"],
                           "revision_node_id": row["revision_node_id"],
                           "revision_swhid": mapping[row["revision_node_id"]][0],
                           "revision_id": mapping[row["revision_node_id"]][1]}
                          for row in self.directory_revision]
        directory_rows.sort(key=lambda row: (row["directory_node_id"], row["revision_node_id"]))
        put_parquet(self.root / "directory_revision_with_swhid.parquet", directory_rows,
                    DIRECTORY_REVISION_SCHEMA)
        seed_table = pq.read_table(INPUT_ROOT / "seed_revision.parquet").to_pylist()
        seed_rows = [{"seed_directory_id": row["seed_directory_id"],
                      "seed_node_id": row["seed_node_id"],
                      "root_directory_node_id": row["root_directory_node_id"],
                      "candidate_revision_node_id": row["candidate_revision_node_id"],
                      "candidate_revision_swhid": mapping[row["candidate_revision_node_id"]][0],
                      "candidate_revision_id": mapping[row["candidate_revision_node_id"]][1],
                      "directory_hops": row["directory_hops"]} for row in seed_table]
        put_parquet(self.root / "seed_revision_with_swhid.parquet", seed_rows, SEED_REVISION_SCHEMA)
        origin_table = pq.read_table(INPUT_ROOT / "results/revision_origin.parquet").to_pylist()
        origin_urls = {row["origin_node_id"]: row for row in pq.read_table(
            INPUT_ROOT / "results/origin_urls.parquet").to_pylist()}
        origin_rows = []
        for row in origin_table:
            url = origin_urls.get(row["origin_node_id"], {})
            origin_rows.append({"candidate_revision_node_id": row["candidate_revision_node_id"],
                                "candidate_revision_swhid": mapping[row["candidate_revision_node_id"]][0],
                                "candidate_revision_id": mapping[row["candidate_revision_node_id"]][1],
                                "origin_node_id": row["origin_node_id"],
                                "origin_id": url.get("origin_id", ""),
                                "origin_url": url.get("origin_url", "")})
        put_parquet(self.root / "revision_origin_with_swhid.parquet", origin_rows, REVISION_ORIGIN_SCHEMA)
        summary = {"status": "passed", "version": VERSION, "identity": self.identity,
                   "completed_at_utc": utc_now(), "revision_nodes": len(revision_rows),
                   "directory_revision_rows": len(directory_rows),
                   "seed_revision_rows": len(seed_rows), "revision_origin_rows": len(origin_rows),
                   "mapped_revision_nodes": len(revision_rows),
                   "unique_revision_swhids": len({row["revision_swhid"] for row in revision_rows}),
                   "range_requests": self.prior_request_count + self.request_count,
                   "range_bytes": self.prior_range_bytes + self.range_bytes,
                   "source_key": NODE2SWHID_KEY}
        write_json(self.root / "complete.json", summary)
        self.emit("export_complete", **summary)
        return summary

    def archive(self, summary):
        if self.private is None:
            self.private = private_session().client(
                "s3", config=Config(retries={"total_max_attempts": 3, "mode": "standard"},
                                     max_pool_connections=8))
        snapshot = self.root / "archive-snapshot"
        snapshot.mkdir(exist_ok=True)
        for name in ("run.log", "events.jsonl", "status.json"):
            path = self.root / name
            if path.exists():
                temporary = snapshot / (name + ".partial")
                temporary.write_bytes(path.read_bytes())
                os.replace(str(temporary), str(snapshot / name))
        live_excluded = {self.root / name for name in
                         ("run.log", "events.jsonl", "status.json", "archive.json", "run_complete.json",
                          "run.lock", "state.sqlite3", "state.sqlite3-shm", "state.sqlite3-wal")}
        files = sorted(path for path in self.root.rglob("*") if path.is_file() and
                       path not in live_excluded and not path.name.endswith(".partial"))
        # All current outputs are below 256 MiB. A single PUT gives a normal
        # SHA256 response and makes resume verification deterministic.
        transfer = TransferConfig(max_concurrency=4, multipart_threshold=256 * 1024**2,
                                  multipart_chunksize=16 * 1024**2)
        objects = []
        for path in files:
            key = S3_PREFIX + path.relative_to(self.root).as_posix()
            digest = sha256(path)
            try:
                head = self.private.head_object(Bucket=BUCKET, Key=key,
                                                ExpectedBucketOwner=BUCKET_OWNER,
                                                ChecksumMode="ENABLED")
            except (ClientError, BotoCoreError):
                head = None
            expected = base64.b64encode(bytes.fromhex(digest)).decode("ascii")
            if (head is not None and head.get("ContentLength") == path.stat().st_size
                    and head.get("Metadata", {}).get("sha256") == digest
                    and head.get("ChecksumSHA256") == expected):
                objects.append({"key": key, "bytes": path.stat().st_size, "sha256": digest,
                                "remote_checksum_verified": True, "resumed": True})
                self.emit("archive_reuse", path=path.name, total=len(files),
                          uploaded=len(objects))
                continue
            self.private.upload_file(str(path), BUCKET, key, Config=transfer,
                                     ExtraArgs={"ExpectedBucketOwner": BUCKET_OWNER,
                                                "ServerSideEncryption": "AES256",
                                                "Metadata": {"sha256": digest},
                                                "ChecksumAlgorithm": "SHA256"})
            head = self.private.head_object(Bucket=BUCKET, Key=key,
                                            ExpectedBucketOwner=BUCKET_OWNER,
                                            ChecksumMode="ENABLED")
            if head.get("ChecksumSHA256") != expected or head["ContentLength"] != path.stat().st_size:
                raise RuntimeError("S3 archive verification failed: " + path.name)
            objects.append({"key": key, "bytes": path.stat().st_size, "sha256": digest,
                            "remote_checksum_verified": True})
            self.emit("archive_progress", uploaded=len(objects), total=len(files), path=path.name)
        archive = {"status": "passed", "completed_at_utc": utc_now(), "objects": objects,
                   "summary": summary}
        archive_path = self.root / "archive.json"
        write_json(archive_path, archive)
        self.private.upload_file(str(archive_path), BUCKET, S3_PREFIX + "archive.json",
                                 ExtraArgs={"ExpectedBucketOwner": BUCKET_OWNER,
                                            "ServerSideEncryption": "AES256",
                                            "ChecksumAlgorithm": "SHA256"})
        with self.private.get_object(Bucket=BUCKET, Key=S3_PREFIX + "archive.json",
                                     ExpectedBucketOwner=BUCKET_OWNER)["Body"] as body:
            if body.read() != archive_path.read_bytes():
                raise RuntimeError("archive marker readback failed")
        self.emit("archive_complete", objects=len(objects), bytes=sum(item["bytes"] for item in objects))

    def run(self):
        nodes, source = self.load_inputs()
        mapping = self.resolve(nodes, source)
        summary = self.export(mapping)
        self.archive(summary)
        write_json(self.root / "run_complete.json", {"status": "passed", "completed_at_utc": utc_now(),
                                                       "summary": summary})
        self.emit("run_complete", status="passed", uploaded=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, default=OUTPUT_ROOT)
    parser.add_argument("--workers", type=int, default=32)
    parser.add_argument("--batch-spans", type=int, default=128)
    parser.add_argument("--max-gap", type=int, default=4096)
    parser.add_argument("--max-span", type=int, default=1024 * 1024)
    args = parser.parse_args()
    resolver = RevisionResolver(args)
    try:
        resolver.run()
    finally:
        resolver.db.commit()
        resolver.db.close()
        resolver.lock.close()


if __name__ == "__main__":
    main()
