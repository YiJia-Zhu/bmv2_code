#!/usr/bin/env python3
"""Recover dates for candidate revisions by scanning public ORC shards once."""

import argparse
import concurrent.futures
import datetime as dt
import hashlib
import json
import os
import subprocess
import threading
import time
from pathlib import Path

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.config import Config
from botocore.exceptions import ClientError


ACCOUNT = "588301174757"
BUCKET = "swh2p4"
BUCKET_OWNER = "728516300718"
DATABASE = "swh_p4_20260604"
WORKGROUP = "swh-route-a-origin-full-bounded"
GRAPH_PREFIX = "graph/2026-06-04/orc/revision/"
REVISION_TABLE = "revision_20260604_route_a_v1"
IDS_TABLE = "route_a_revision_ids_v1"
GRAPH_ROOT = Path("/mnt/8T/xgr/swh-graph-poc-20260604")
RESOLVE_ROOT = GRAPH_ROOT / "route-a-revision-origin-v1/revision-resolve-v1"
INVENTORY_PATH = Path("data/raw/swh_p4/discovery/probe8_spark_v1/inventory.json")
OUTPUT_ROOT = GRAPH_ROOT / "route-a-revision-origin-v1/revision-dates-v1"
S3_PREFIX = "new-account-588301174757/route-a/revision-dates-v1/"
ID_SCHEMA = pa.schema([("id", pa.string())])
DATE_SCHEMA = pa.schema([
    ("id", pa.string()),
    ("date", pa.string()),
    ("committer_date", pa.string()),
    ("directory", pa.string()),
    ("source_path", pa.string()),
])


def utc_now():
    return dt.datetime.now(dt.timezone.utc).isoformat()


def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".partial")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n",
                         encoding="utf-8")
    os.replace(str(temporary), str(path))


def sha256_bytes(data):
    return hashlib.sha256(data).hexdigest()


def quote_sql(value):
    return "'" + value.replace("'", "''") + "'"


def query_for_shard(path):
    return (
        'SELECT r.id, r."date", r.committer_date, r.directory, r."$path" AS source_path '
        'FROM "{}"."{}" r '
        'JOIN "{}"."{}" ids ON r.id = ids.id '
        'WHERE r."$path" = {}'
    ).format(DATABASE, REVISION_TABLE, DATABASE, IDS_TABLE, quote_sql(path))


def private_session():
    session = boto3.Session(region_name="us-east-1")
    if session.get_credentials() is not None:
        return session

    # `aws login` stores short-lived credentials behind the CLI process
    # provider rather than exporting them into the shell environment.
    def refresh():
        result = subprocess.run(
            ["aws", "configure", "export-credentials", "--format", "process"],
            capture_output=True, text=True, check=True, timeout=30)
        values = json.loads(result.stdout)
        return {
            "access_key": values["AccessKeyId"],
            "secret_key": values["SecretAccessKey"],
            "token": values.get("SessionToken"),
            "expiry_time": values["Expiration"],
        }

    import botocore.session
    from botocore.credentials import RefreshableCredentials
    raw = botocore.session.get_session()
    credentials = RefreshableCredentials.create_from_metadata(
        metadata=refresh(), refresh_using=refresh, method="aws-cli-login")
    credentials._advisory_refresh_timeout = 60
    credentials._mandatory_refresh_timeout = 30
    raw._credentials = credentials
    return boto3.Session(botocore_session=raw, region_name="us-east-1")


class RevisionDateRunner:
    def __init__(self, args):
        self.args = args
        self.root = args.output
        self.root.mkdir(parents=True, exist_ok=True)
        self.state_root = self.root / "state"
        self.shard_root = self.root / "shards"
        self.state_root.mkdir(exist_ok=True)
        self.shard_root.mkdir(exist_ok=True)
        self.session = private_session()
        self.glue = self.session.client("glue")
        self.s3 = self.session.client(
            "s3", config=Config(retries={"total_max_attempts": 3, "mode": "standard"},
                                max_pool_connections=max(8, args.workers * 2)))
        self.athena = self.session.client(
            "athena", config=Config(retries={"total_max_attempts": 3, "mode": "standard"},
                                    max_pool_connections=max(8, args.workers * 2)))
        self.output_lock = threading.Lock()
        self.started = time.monotonic()
        self.completed = 0
        self.failed = 0
        self.rows = 0
        self.scanned_bytes = 0
        self.completed_durations = []
        self.shards = self.load_shards()
        self.revision_ids = self.load_revision_ids()
        self.ensure_inputs()
        self.ensure_tables()
        self.write_identity()

    def load_shards(self):
        inventory = json.loads(INVENTORY_PATH.read_text(encoding="utf-8"))
        rows = inventory["tables"]["revision"]["objects"]
        shards = []
        for item in rows:
            key = item["key"]
            if not key.startswith(GRAPH_PREFIX) or not key.endswith(".orc"):
                raise RuntimeError("unexpected revision object: " + key)
            shards.append({"key": key, "size": int(item["size"])})
        if len(shards) != 128:
            raise RuntimeError("expected 128 revision ORC objects")
        return sorted(shards, key=lambda row: row["key"])

    def load_revision_ids(self):
        table = pq.read_table(RESOLVE_ROOT / "revision_nodes.parquet",
                              columns=["revision_id"])
        ids = sorted(set(table.column("revision_id").to_pylist()))
        if len(ids) != 317804 or not all(len(value) == 40 for value in ids):
            raise RuntimeError("expected 317,804 unique revision IDs")
        return ids

    def ensure_inputs(self):
        self.ids_path = self.root / "revision_ids.parquet"
        if not self.ids_path.exists():
            temporary = self.ids_path.with_suffix(".parquet.partial")
            pq.write_table(pa.Table.from_pylist([{"id": value} for value in self.revision_ids],
                                                 schema=ID_SCHEMA),
                           temporary, compression="zstd")
            os.replace(str(temporary), str(self.ids_path))
        payload_digest = hashlib.sha256(self.ids_path.read_bytes()).hexdigest()
        key = S3_PREFIX + "athena-input/revision_ids.parquet"
        try:
            head = self.s3.head_object(Bucket=BUCKET, Key=key,
                                       ExpectedBucketOwner=BUCKET_OWNER)
        except self.s3.exceptions.ClientError as error:
            if error.response.get("Error", {}).get("Code") not in ("404", "NoSuchKey", "NotFound"):
                raise
            head = None
        if head is None or head.get("Metadata", {}).get("sha256") != payload_digest:
            self.s3.upload_file(
                str(self.ids_path), BUCKET, key,
                ExtraArgs={"ExpectedBucketOwner": BUCKET_OWNER,
                           "ServerSideEncryption": "AES256",
                           "Metadata": {"sha256": payload_digest},
                           "ChecksumAlgorithm": "SHA256"},
            )
            head = self.s3.head_object(Bucket=BUCKET, Key=key,
                                       ExpectedBucketOwner=BUCKET_OWNER,
                                       ChecksumMode="ENABLED")
        if head.get("ContentLength") != self.ids_path.stat().st_size:
            raise RuntimeError("staged revision ID object has an unexpected size")
        self.ids_location = "s3://{}/{}".format(BUCKET, S3_PREFIX + "athena-input/")
        self.ids_sha256 = payload_digest

    def ensure_tables(self):
        try:
            db = self.glue.get_database(Name=DATABASE)["Database"]
        except self.glue.exceptions.EntityNotFoundException:
            raise RuntimeError("Glue database not found: " + DATABASE)
        del db
        revision_columns = [
            ("id", "string"), ("message", "binary"), ("author", "binary"),
            ("date", "timestamp"), ("date_offset", "smallint"),
            ("date_raw_offset_bytes", "binary"), ("committer", "binary"),
            ("committer_date", "timestamp"), ("committer_offset", "smallint"),
            ("committer_date_raw_offset_bytes", "binary"), ("directory", "string"),
            ("type", "string"), ("raw_manifest", "binary"),
        ]
        self.ensure_glue_table(
            REVISION_TABLE,
            revision_columns,
            "s3://softwareheritage/graph/2026-06-04/orc/revision/",
            "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat",
            "org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat",
            "org.apache.hadoop.hive.ql.io.orc.OrcSerde",
            "orc",
        )
        self.ensure_glue_table(
            IDS_TABLE, [("id", "string")], self.ids_location,
            "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
            "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
            "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
            "parquet",
        )

    def ensure_glue_table(self, name, columns, location, input_format,
                          output_format, serde, classification):
        try:
            existing = self.glue.get_table(DatabaseName=DATABASE, Name=name)["Table"]
        except self.glue.exceptions.EntityNotFoundException:
            existing = None
        if existing:
            actual = existing["StorageDescriptor"]["Location"].rstrip("/") + "/"
            if actual != location.rstrip("/") + "/":
                raise RuntimeError("existing Glue table has unexpected location: " + name)
            return
        self.glue.create_table(
            DatabaseName=DATABASE,
            TableInput={
                "Name": name,
                "TableType": "EXTERNAL_TABLE",
                "Parameters": {"EXTERNAL": "TRUE", "classification": classification},
                "StorageDescriptor": {
                    "Columns": [{"Name": column, "Type": kind}
                                for column, kind in columns],
                    "Location": location,
                    "InputFormat": input_format,
                    "OutputFormat": output_format,
                    "SerdeInfo": {
                        "SerializationLibrary": serde,
                        "Parameters": {"serialization.format": "1"},
                    },
                },
            },
        )
        loaded = self.glue.get_table(DatabaseName=DATABASE, Name=name)["Table"]
        if loaded["StorageDescriptor"]["Location"].rstrip("/") + "/" != location.rstrip("/") + "/":
            raise RuntimeError("created Glue table location did not read back")

    def write_identity(self):
        identity = {
            "version": "route-a-revision-dates-v1",
            "account": ACCOUNT,
            "database": DATABASE,
            "workgroup": WORKGROUP,
            "revision_ids": len(self.revision_ids),
            "revision_ids_sha256": self.ids_sha256,
            "revision_orc_objects": len(self.shards),
            "revision_orc_bytes_upper_bound": sum(row["size"] for row in self.shards),
            "per_query_bytes_cutoff": 34359738368,
            "created_at_utc": utc_now(),
        }
        path = self.root / "identity.json"
        if path.exists():
            existing = json.loads(path.read_text(encoding="utf-8"))
            existing_comparison = dict(existing)
            identity_comparison = dict(identity)
            for key in ("created_at_utc",):
                existing_comparison.pop(key, None)
                identity_comparison.pop(key, None)
            if existing_comparison != identity_comparison:
                raise RuntimeError("revision-date run identity changed")
            if existing.get("created_at_utc"):
                identity["created_at_utc"] = existing["created_at_utc"]
        write_json(path, identity)

    def event(self, name, **fields):
        event = dict(fields, event=name, timestamp_utc=utc_now(),
                     elapsed_seconds=round(time.monotonic() - self.started, 3))
        line = json.dumps(event, sort_keys=True)
        with self.output_lock:
            print(line, flush=True)
            with (self.root / "events.jsonl").open("a", encoding="utf-8") as handle:
                handle.write(line + "\n")
            write_json(self.root / "status.json", event)

    def state_path(self, index):
        return self.state_root / ("shard_{:03d}.json".format(index))

    def shard_output_path(self, index):
        return self.shard_root / ("part-{:03d}.parquet".format(index))

    def upload_local(self, path, key, content_type="application/octet-stream"):
        payload = path.read_bytes()
        self.s3.put_object(
            Bucket=BUCKET,
            Key=key,
            Body=payload,
            ContentType=content_type,
            ExpectedBucketOwner=BUCKET_OWNER,
            ServerSideEncryption="AES256",
            Metadata={"sha256": hashlib.sha256(payload).hexdigest()},
        )

    def save_shard(self, index, rows, source_path, scanned_bytes, query_id):
        path = self.shard_output_path(index)
        temporary = path.with_suffix(".parquet.partial")
        for row in rows:
            row["source_path"] = source_path
        pq.write_table(pa.Table.from_pylist(rows, schema=DATE_SCHEMA),
                       temporary, compression="zstd")
        os.replace(str(temporary), str(path))
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        key = S3_PREFIX + "results/" + path.name
        self.s3.upload_file(
            str(path), BUCKET, key,
            ExtraArgs={"ExpectedBucketOwner": BUCKET_OWNER,
                       "ServerSideEncryption": "AES256",
                       "Metadata": {"sha256": digest},
                       "ChecksumAlgorithm": "SHA256"},
        )
        head = self.s3.head_object(Bucket=BUCKET, Key=key,
                                   ExpectedBucketOwner=BUCKET_OWNER,
                                   ChecksumMode="ENABLED")
        expected_checksum = __import__("base64").b64encode(bytes.fromhex(digest)).decode("ascii")
        if (head["ContentLength"] != path.stat().st_size or
                head.get("ChecksumSHA256") != expected_checksum):
            raise RuntimeError("S3 revision-date shard failed verification")
        state = {
            "status": "passed", "shard_index": index, "source_path": source_path,
            "query_id": query_id, "scanned_bytes": scanned_bytes, "rows": len(rows),
            "output_key": key, "output_bytes": path.stat().st_size,
            "output_sha256": digest, "completed_at_utc": utc_now(),
        }
        write_json(self.state_path(index), state)
        self.upload_local(self.state_path(index), S3_PREFIX + "metadata/state/" +
                          self.state_path(index).name, "application/json")
        return state

    def archive_metadata(self):
        files = [
            (self.root / "identity.json", "application/json"),
            (self.root / "summary.json", "application/json"),
            (self.root / "status.json", "application/json"),
            (self.root / "events.jsonl", "application/x-ndjson"),
        ]
        for path, content_type in files:
            if path.exists():
                self.upload_local(path, S3_PREFIX + "metadata/" + path.name,
                                  content_type)
        for path in sorted(self.state_root.glob("shard_*.json")):
            self.upload_local(path, S3_PREFIX + "metadata/state/" + path.name,
                              "application/json")
        marker = {
            "status": json.loads((self.root / "summary.json").read_text(encoding="utf-8")),
            "archived_at_utc": utc_now(),
            "prefix": "s3://{}/{}".format(BUCKET, S3_PREFIX),
        }
        self.s3.put_object(
            Bucket=BUCKET,
            Key=S3_PREFIX + "complete.json",
            Body=(json.dumps(marker, indent=2, sort_keys=True) + "\n").encode("utf-8"),
            ContentType="application/json",
            ExpectedBucketOwner=BUCKET_OWNER,
            ServerSideEncryption="AES256",
        )

    def result_rows(self, query_id):
        rows = []
        paginator = self.athena.get_paginator("get_query_results")
        first = True
        for page in paginator.paginate(QueryExecutionId=query_id):
            for item in page["ResultSet"].get("Rows", []):
                if first:
                    first = False
                    continue
                values = [cell.get("VarCharValue") for cell in item.get("Data", [])]
                values += [None] * (5 - len(values))
                rows.append({
                    "id": values[0], "date": values[1], "committer_date": values[2],
                    "directory": values[3], "source_path": values[4],
                })
        return rows

    def process_shard(self, index, item):
        state_path = self.state_path(index)
        output_path = self.shard_output_path(index)
        if state_path.exists():
            state = json.loads(state_path.read_text(encoding="utf-8"))
            if state.get("status") == "passed" and output_path.exists():
                self.record_completed(state, resumed=True)
                return state
        else:
            state = {}
        source_path = "s3://softwareheritage/" + item["key"]
        query_id = state.get("query_id")
        if state.get("status") in ("failed", "cancelled"):
            query_id = None
        if not query_id:
            query = query_for_shard(source_path)
            response = self.athena.start_query_execution(
                QueryString=query,
                QueryExecutionContext={"Database": DATABASE, "Catalog": "AwsDataCatalog"},
                WorkGroup=WORKGROUP,
                ClientRequestToken=hashlib.sha256(
                    (self.ids_sha256 + "\n" + item["key"] + "\n" + query).encode("utf-8")
                ).hexdigest()[:64],
            )
            query_id = response["QueryExecutionId"]
            state = {"status": "submitted", "shard_index": index, "query_id": query_id,
                     "source_path": source_path, "submitted_at_utc": utc_now()}
            write_json(state_path, state)
            self.event("query_submitted", shard=index, shard_count=len(self.shards),
                       query_id=query_id, source_bytes=item["size"])
        last_log = time.monotonic()
        while True:
            execution = self.athena.get_query_execution(QueryExecutionId=query_id)["QueryExecution"]
            status = execution["Status"]["State"]
            if status in ("SUCCEEDED", "FAILED", "CANCELLED"):
                break
            if time.monotonic() - last_log >= 15:
                self.event("query_wait", shard=index, query_id=query_id, state=status)
                last_log = time.monotonic()
            time.sleep(3)
        if status != "SUCCEEDED":
            state.update(status=status.lower(), reason=execution["Status"].get("StateChangeReason", ""))
            write_json(state_path, state)
            raise RuntimeError("Athena shard {} {}: {}".format(
                index, status, state.get("reason", "")))
        statistics = execution.get("Statistics", {})
        scanned = int(statistics.get("DataScannedInBytes", 0))
        rows = self.result_rows(query_id)
        saved = self.save_shard(index, rows, source_path, scanned, query_id)
        self.record_completed(saved, resumed=False,
                              duration=statistics.get("EngineExecutionTimeInMillis", 0) / 1000.0)
        return saved

    def record_completed(self, state, resumed, duration=0):
        with self.output_lock:
            self.completed += 1
            self.rows += int(state.get("rows", 0))
            self.scanned_bytes += int(state.get("scanned_bytes", 0))
            if duration:
                self.completed_durations.append(duration)
            elapsed = max(time.monotonic() - self.started, 0.001)
            rate = self.completed / elapsed
            eta = (len(self.shards) - self.completed) / rate if rate else None
            print(json.dumps({
                "event": "progress", "completed_shards": self.completed,
                "total_shards": len(self.shards),
                "percent": round(100.0 * self.completed / len(self.shards), 2),
                "rows": self.rows, "scanned_bytes": self.scanned_bytes,
                "eta_seconds": round(eta) if eta is not None else None,
                "resumed": resumed, "timestamp_utc": utc_now(),
            }, sort_keys=True), flush=True)

    def run(self):
        self.event("run_start", shard_count=len(self.shards),
                   revision_ids=len(self.revision_ids), workers=self.args.workers,
                   requested_shards=self.args.limit_shards)
        selected = list(enumerate(self.shards))
        if self.args.limit_shards:
            pending = []
            for index, item in selected:
                state_path = self.state_path(index)
                state = json.loads(state_path.read_text(encoding="utf-8")) if state_path.exists() else {}
                if state.get("status") != "passed" or not self.shard_output_path(index).exists():
                    pending.append((index, item))
            selected = pending[:self.args.limit_shards]
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.args.workers) as pool:
            futures = [pool.submit(self.process_shard, index, item) for index, item in selected]
            for future in concurrent.futures.as_completed(futures):
                try:
                    future.result()
                except Exception:
                    self.failed += 1
                    for other in futures:
                        other.cancel()
                    raise
        states = []
        for index, _item in enumerate(self.shards):
            path = self.state_path(index)
            if path.exists():
                states.append(json.loads(path.read_text(encoding="utf-8")))
        complete_count = sum(row.get("status") == "passed" for row in states)
        summary = {
            "status": "passed" if complete_count == len(self.shards) else "partial",
            "completed_shards": complete_count,
            "total_shards": len(self.shards),
            "rows": sum(int(row.get("rows", 0)) for row in states
                        if row.get("status") == "passed"),
            "scanned_bytes": sum(int(row.get("scanned_bytes", 0)) for row in states
                                 if row.get("status") == "passed"),
            "revision_ids": len(self.revision_ids),
            "updated_at_utc": utc_now(),
        }
        write_json(self.root / "summary.json", summary)
        self.event("run_complete" if summary["status"] == "passed" else "run_partial",
                   **summary)
        self.archive_metadata()
        return summary


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, default=OUTPUT_ROOT)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--limit-shards", type=int, default=0,
                        help="process this many pending shards; 0 means all")
    args = parser.parse_args()
    if args.workers < 1 or args.workers > 8:
        parser.error("--workers must be between 1 and 8")
    if args.limit_shards < 0:
        parser.error("--limit-shards must be >= 0")
    summary = RevisionDateRunner(args).run()
    return 0 if summary["status"] in ("passed", "partial") else 1


if __name__ == "__main__":
    raise SystemExit(main())
