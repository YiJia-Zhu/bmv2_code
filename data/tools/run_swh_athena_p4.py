#!/usr/bin/env python3
"""Run resumable per-ORC Athena discovery queries for SWH P4 files.

Athena names SELECT results with query UUIDs.  This runner retains those raw
objects, then copies every successful CSV to a deterministic shard key.  A
manifest is written atomically both locally and to S3 so an interrupted run
can recover submitted queries without paying for duplicate scans.
"""

from __future__ import print_function

import argparse
import concurrent.futures
import datetime as dt
import hashlib
import json
import os
import subprocess
import sys
import time
import urllib.parse
from pathlib import Path


SCRIPT_VERSION = "1.0.0"
GRAPH_RELEASE = "2026-06-04"
SOURCE_BUCKET = "softwareheritage"
SOURCE_PREFIX = "graph/2026-06-04/orc/directory_entry/"
DEFAULT_REGION = "us-east-1"
DEFAULT_OUTPUT_BUCKET = "swh2p4"
DEFAULT_OUTPUT_PREFIX = "athena-results/swh-p4-20260604"
DEFAULT_WORKGROUP = "swh-p4-pilot"
DEFAULT_DATABASE = "swh_p4"
DEFAULT_TABLE = "directory_entry_20260604"
DEFAULT_LOCAL_MANIFEST = Path(
    "data/raw/swh_p4/discovery/athena_directory_entry_shards.json"
)
DEFAULT_PILOT_MANIFEST = Path(
    "data/raw/swh_p4/discovery/athena_p4_hit_pilot_20261016.json"
)
QUERY_VERSION = "p4-basename-like-v1"
TERMINAL_QUERY_STATES = {"SUCCEEDED", "FAILED", "CANCELLED"}
ACTIVE_STATES = {"submitted", "running"}


class AwsCliError(RuntimeError):
    pass


def utc_now():
    return dt.datetime.now(dt.timezone.utc).isoformat()


def write_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    with temporary.open("w", encoding="utf-8") as handle:
        json.dump(value, handle, ensure_ascii=False, indent=2, sort_keys=True)
        handle.write("\n")
    os.replace(str(temporary), str(path))


def read_json(path):
    with Path(path).open("r", encoding="utf-8") as handle:
        return json.load(handle)


def aws_json(arguments, region, check=True, timeout=60, attempts=3):
    command = ["aws"] + list(arguments)
    if "--region" not in command:
        command.extend(["--region", region])
    if "--output" not in command:
        command.extend(["--output", "json"])
    command.append("--no-cli-pager")
    environment = dict(os.environ)
    environment["AWS_PAGER"] = ""
    last_error = ""
    for attempt in range(attempts):
        try:
            result = subprocess.run(
                command,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                env=environment,
                timeout=timeout,
            )
        except subprocess.TimeoutExpired:
            last_error = "{} timed out after {} seconds".format(
                " ".join(command[:3]), timeout
            )
        else:
            if result.returncode == 0:
                output = result.stdout.strip()
                return (json.loads(output) if output else {}), ""
            last_error = result.stderr.strip()
            if not check:
                return None, last_error
        if attempt + 1 < attempts:
            time.sleep(2 ** attempt)
    if check:
        raise AwsCliError(last_error)
    return None, last_error


def s3_uri(bucket, key):
    return "s3://{}/{}".format(bucket, key.lstrip("/"))


def parse_s3_uri(value):
    parsed = urllib.parse.urlparse(value)
    if parsed.scheme != "s3" or not parsed.netloc or not parsed.path:
        raise ValueError("invalid S3 URI: {}".format(value))
    return parsed.netloc, parsed.path.lstrip("/")


def source_object_id(source_key):
    name = source_key.rsplit("/", 1)[-1]
    if name.startswith("directory_entry-"):
        name = name[len("directory_entry-") :]
    if name.endswith(".orc"):
        name = name[:-4]
    return name


def stable_result_key(output_prefix, index, source_key):
    return "{}/p4-directory-hits/part-{:05d}-{}.csv".format(
        output_prefix.rstrip("/"), index, source_object_id(source_key)
    )


def query_for_source(database, table, source_uri):
    escaped_uri = source_uri.replace("'", "''")
    return (
        "SELECT directory_id, from_utf8(name) AS name, type, "
        "target AS content_id, \"$path\" AS source_path "
        "FROM \"{}\".\"{}\" "
        "WHERE \"$path\" = '{}' "
        "AND type = 'file' "
        "AND lower(from_utf8(name)) LIKE '%.p4'"
    ).format(database, table, escaped_uri)


def client_token(source_key, attempt=0):
    digest = hashlib.sha256(
        "{}\n{}\n{}\n{}".format(
            GRAPH_RELEASE, QUERY_VERSION, source_key, attempt
        ).encode("utf-8")
    ).hexdigest()
    return "swh-p4-{}".format(digest)


class Runner(object):
    def __init__(self, args):
        self.args = args
        self.region = args.region
        self.output_bucket = args.output_bucket
        self.output_prefix = args.output_prefix.rstrip("/")
        self.raw_result_prefix = self.output_prefix + "/_athena-query-results/"
        self.manifest_key = self.output_prefix + "/manifests/athena-directory-entry-shards-v1.json"
        self.summary_key = self.output_prefix + "/manifests/run-summary.json"
        self.local_manifest = args.local_manifest
        self.local_summary = self.local_manifest.with_name("athena_run_summary.json")
        self.manifest = None

    def list_source_objects(self):
        value, _ = aws_json(
            [
                "s3api",
                "list-objects-v2",
                "--no-sign-request",
                "--bucket",
                SOURCE_BUCKET,
                "--prefix",
                SOURCE_PREFIX,
            ],
            self.region,
        )
        objects = []
        for item in value.get("Contents", []):
            key = item.get("Key", "")
            if not key.endswith(".orc"):
                continue
            objects.append(
                {
                    "source_key": key,
                    "source_size_bytes": int(item["Size"]),
                    "source_etag": str(item.get("ETag", "")).strip('"'),
                }
            )
        objects.sort(key=lambda item: item["source_key"])
        if len(objects) != self.args.expected_objects and not self.args.allow_object_count_mismatch:
            raise RuntimeError(
                "expected {} ORC objects, found {}; use --allow-object-count-mismatch "
                "only after reviewing the source listing".format(
                    self.args.expected_objects, len(objects)
                )
            )
        return objects

    def head_object(self, bucket, key):
        value, error = aws_json(
            ["s3api", "head-object", "--bucket", bucket, "--key", key],
            self.region,
            check=False,
        )
        if value is None:
            lowered = error.lower()
            if "not found" in lowered or "404" in lowered or "nosuchkey" in lowered:
                return None
            raise AwsCliError(error)
        return value

    def list_stable_results(self):
        prefix = self.output_prefix + "/p4-directory-hits/"
        value, _ = aws_json(
            [
                "s3api",
                "list-objects-v2",
                "--bucket",
                self.output_bucket,
                "--prefix",
                prefix,
            ],
            self.region,
        )
        return {
            item["Key"]: {
                "ContentLength": int(item["Size"]),
                "ETag": item.get("ETag", ""),
            }
            for item in value.get("Contents", [])
        }

    def download_remote_manifest(self):
        if self.head_object(self.output_bucket, self.manifest_key) is None:
            return False
        temporary = self.local_manifest.with_name(self.local_manifest.name + ".download")
        temporary.parent.mkdir(parents=True, exist_ok=True)
        aws_json(
            [
                "s3api",
                "get-object",
                "--bucket",
                self.output_bucket,
                "--key",
                self.manifest_key,
                str(temporary),
            ],
            self.region,
        )
        os.replace(str(temporary), str(self.local_manifest))
        return True

    def new_manifest(self, source_objects):
        rows = []
        for index, item in enumerate(source_objects):
            row = dict(item)
            row.update(
                {
                    "index": index,
                    "source_uri": s3_uri(SOURCE_BUCKET, item["source_key"]),
                    "stable_result_key": stable_result_key(
                        self.output_prefix, index, item["source_key"]
                    ),
                    "status": "pending",
                    "attempt": 0,
                    "client_request_token": client_token(item["source_key"], 0),
                }
            )
            rows.append(row)
        return {
            "schema_version": 1,
            "script_version": SCRIPT_VERSION,
            "query_version": QUERY_VERSION,
            "graph_release": GRAPH_RELEASE,
            "created_at": utc_now(),
            "updated_at": utc_now(),
            "region": self.region,
            "source_bucket": SOURCE_BUCKET,
            "source_prefix": SOURCE_PREFIX,
            "output_bucket": self.output_bucket,
            "output_prefix": self.output_prefix,
            "raw_result_prefix": self.raw_result_prefix,
            "manifest_s3_uri": s3_uri(self.output_bucket, self.manifest_key),
            "workgroup": self.args.workgroup,
            "database": self.args.database,
            "table": self.args.table,
            "concurrency": self.args.concurrency,
            "objects": rows,
        }

    def import_pilot(self):
        if not self.args.pilot_manifest.is_file():
            return 0
        pilot = read_json(self.args.pilot_manifest)
        pilot_by_key = {item["source_key"]: item for item in pilot.get("objects", [])}
        imported = 0
        for row in self.manifest["objects"]:
            previous = pilot_by_key.get(row["source_key"])
            if not previous or previous.get("status") != "SUCCEEDED":
                continue
            row.update(
                {
                    "status": "query_succeeded",
                    "query_execution_id": previous["query_execution_id"],
                    "data_scanned_bytes": previous.get("data_scanned_bytes", 0),
                    "engine_execution_ms": previous.get("engine_execution_ms", 0),
                    "total_execution_ms": previous.get("total_execution_ms", 0),
                    "query_queue_ms": previous.get("query_queue_ms", 0),
                    "raw_result_s3_uri": previous["result_s3_uri"],
                    "raw_result_size_bytes": previous.get("result_size_bytes"),
                    "imported_from_pilot": str(self.args.pilot_manifest),
                }
            )
            imported += 1
        return imported

    def reconcile_inventory(self, source_objects):
        source_by_key = {item["source_key"]: item for item in source_objects}
        manifest_by_key = {
            item["source_key"]: item for item in self.manifest.get("objects", [])
        }
        if set(source_by_key) != set(manifest_by_key):
            raise RuntimeError("source inventory differs from the saved manifest")
        for index, source in enumerate(source_objects):
            row = manifest_by_key[source["source_key"]]
            if (
                row.get("source_size_bytes") != source["source_size_bytes"]
                or row.get("source_etag") != source["source_etag"]
            ):
                raise RuntimeError(
                    "source object changed since manifest creation: {}".format(
                        source["source_key"]
                    )
                )
            row["index"] = index
            row["stable_result_key"] = stable_result_key(
                self.output_prefix, index, source["source_key"]
            )
        self.manifest["objects"] = [
            manifest_by_key[item["source_key"]] for item in source_objects
        ]

    def load_or_create_manifest(self, source_objects):
        if self.args.resume and not self.local_manifest.is_file():
            self.download_remote_manifest()
        if self.local_manifest.is_file():
            self.manifest = read_json(self.local_manifest)
            self.reconcile_inventory(source_objects)
            return 0
        self.manifest = self.new_manifest(source_objects)
        return self.import_pilot()

    def upload_json(self, local_path, key):
        aws_json(
            [
                "s3api",
                "put-object",
                "--bucket",
                self.output_bucket,
                "--key",
                key,
                "--body",
                str(local_path),
                "--content-type",
                "application/json",
                "--server-side-encryption",
                "AES256",
                "--checksum-algorithm",
                "SHA256",
            ],
            self.region,
        )

    def summary(self):
        counts = {}
        total_scanned = 0
        total_result = 0
        total_execution = 0
        for row in self.manifest["objects"]:
            status = row.get("status", "pending")
            counts[status] = counts.get(status, 0) + 1
            total_scanned += int(row.get("data_scanned_bytes") or 0)
            total_result += int(row.get("stable_result_size_bytes") or 0)
            total_execution += int(row.get("total_execution_ms") or 0)
        return {
            "schema_version": 1,
            "updated_at": utc_now(),
            "graph_release": GRAPH_RELEASE,
            "workgroup": self.args.workgroup,
            "region": self.region,
            "object_count": len(self.manifest["objects"]),
            "status_counts": counts,
            "total_scanned_bytes": total_scanned,
            "total_result_bytes": total_result,
            "sum_query_execution_ms": total_execution,
            "manifest_s3_uri": s3_uri(self.output_bucket, self.manifest_key),
            "result_prefix": s3_uri(
                self.output_bucket, self.output_prefix + "/p4-directory-hits/"
            ),
        }

    def persist(self):
        self.manifest["updated_at"] = utc_now()
        write_json(self.local_manifest, self.manifest)
        write_json(self.local_summary, self.summary())
        self.upload_json(self.local_manifest, self.manifest_key)
        self.upload_json(self.local_summary, self.summary_key)

    def verify_workgroup(self):
        value, _ = aws_json(
            ["athena", "get-work-group", "--work-group", self.args.workgroup],
            self.region,
        )
        workgroup = value["WorkGroup"]
        configuration = workgroup.get("Configuration", {})
        actual_output = (
            configuration.get("ResultConfiguration", {}).get("OutputLocation", "")
        )
        expected_output = s3_uri(self.output_bucket, self.raw_result_prefix)
        if actual_output.rstrip("/") != expected_output.rstrip("/"):
            raise RuntimeError(
                "workgroup output is {!r}, expected {!r}".format(
                    actual_output, expected_output
                )
            )
        if not configuration.get("EnforceWorkGroupConfiguration"):
            raise RuntimeError("workgroup must enforce its result configuration")
        cutoff = configuration.get("BytesScannedCutoffPerQuery")
        if not cutoff:
            raise RuntimeError("workgroup has no per-query scan cutoff")

    def fetch_query_states(self, query_ids):
        states = {}
        ids = list(query_ids)
        for start in range(0, len(ids), 50):
            value, _ = aws_json(
                ["athena", "batch-get-query-execution", "--query-execution-ids"]
                + ids[start : start + 50],
                self.region,
            )
            for query in value.get("QueryExecutions", []):
                states[query["QueryExecutionId"]] = query
        return states

    def apply_query_state(self, row, query):
        status = query.get("Status", {})
        statistics = query.get("Statistics", {})
        state = status.get("State")
        row["athena_state"] = state
        row["data_scanned_bytes"] = statistics.get("DataScannedInBytes", 0)
        row["engine_execution_ms"] = statistics.get("EngineExecutionTimeInMillis", 0)
        row["total_execution_ms"] = statistics.get("TotalExecutionTimeInMillis", 0)
        row["query_queue_ms"] = statistics.get("QueryQueueTimeInMillis", 0)
        row["submission_time"] = status.get("SubmissionDateTime")
        row["completion_time"] = status.get("CompletionDateTime")
        output = query.get("ResultConfiguration", {}).get("OutputLocation")
        if output:
            row["raw_result_s3_uri"] = output
        if state == "SUCCEEDED":
            row["status"] = "query_succeeded"
            row.pop("failure_reason", None)
        elif state in ("FAILED", "CANCELLED"):
            row["status"] = "failed"
            row["failure_reason"] = status.get("StateChangeReason", state)
        else:
            row["status"] = "running"

    def recover_remote_states(self):
        active = [
            row
            for row in self.manifest["objects"]
            if row.get("status") in ACTIVE_STATES and row.get("query_execution_id")
        ]
        if not active:
            return False
        states = self.fetch_query_states(
            [row["query_execution_id"] for row in active]
        )
        changed = False
        for row in active:
            query = states.get(row["query_execution_id"])
            if query:
                previous = row.get("status")
                self.apply_query_state(row, query)
                changed = changed or previous != row.get("status")
        return changed

    def copy_result(self, row):
        raw_bucket, raw_key = parse_s3_uri(row["raw_result_s3_uri"])
        copy_source = urllib.parse.quote(
            "{}/{}".format(raw_bucket, raw_key), safe="/"
        )
        result, _ = aws_json(
            [
                "s3api",
                "copy-object",
                "--bucket",
                self.output_bucket,
                "--key",
                row["stable_result_key"],
                "--copy-source",
                copy_source,
                "--server-side-encryption",
                "AES256",
                "--metadata-directive",
                "COPY",
            ],
            self.region,
        )
        row.update(
            {
                "status": "copied",
                "stable_result_s3_uri": s3_uri(
                    self.output_bucket, row["stable_result_key"]
                ),
                "copy_result_etag": str(
                    result.get("CopyObjectResult", {}).get("ETag", "")
                ).strip('"'),
                "copied_at": utc_now(),
            }
        )

    def confirm_copied(self, stable_results=None):
        copied = [
            row for row in self.manifest["objects"] if row.get("status") == "copied"
        ]
        if not copied:
            return 0
        stable_results = stable_results or self.list_stable_results()
        confirmed = 0
        for row in copied:
            stable = stable_results.get(row["stable_result_key"])
            if stable is None:
                continue
            expected_size = row.get("raw_result_size_bytes")
            if expected_size is not None and int(stable["ContentLength"]) != int(
                expected_size
            ):
                raise RuntimeError("stable result size differs from the Athena result")
            row.update(
                {
                    "status": "complete",
                    "stable_result_size_bytes": int(stable["ContentLength"]),
                    "stable_result_etag": str(stable.get("ETag", "")).strip('"'),
                    "completed_at": utc_now(),
                }
            )
            confirmed += 1
        return confirmed

    def finalize_succeeded(self):
        count = 0
        stable_results = self.list_stable_results()
        self.confirm_copied(stable_results)
        for row in self.manifest["objects"]:
            if row.get("status") != "query_succeeded":
                continue
            existing = stable_results.get(row["stable_result_key"])
            if existing is not None and (
                row.get("raw_result_size_bytes") is None
                or int(existing["ContentLength"])
                == int(row.get("raw_result_size_bytes"))
            ):
                row.update(
                    {
                        "status": "complete",
                        "stable_result_s3_uri": s3_uri(
                            self.output_bucket, row["stable_result_key"]
                        ),
                        "stable_result_size_bytes": int(existing["ContentLength"]),
                        "stable_result_etag": str(existing.get("ETag", "")).strip('"'),
                        "completed_at": row.get("completed_at") or utc_now(),
                    }
                )
            else:
                self.copy_result(row)
            count += 1
        if count:
            self.confirm_copied()
        return count

    def verify_completed(self):
        changed = False
        stable_results = self.list_stable_results()
        for row in self.manifest["objects"]:
            if row.get("status") != "complete":
                continue
            head = stable_results.get(row["stable_result_key"])
            if head is None or int(head["ContentLength"]) != int(
                row.get("stable_result_size_bytes", -1)
            ):
                if row.get("raw_result_s3_uri"):
                    row["status"] = "query_succeeded"
                else:
                    row["status"] = "pending"
                changed = True
        return changed

    def start_query(self, row):
        query = query_for_source(
            self.args.database, self.args.table, row["source_uri"]
        )
        value, _ = aws_json(
            [
                "athena",
                "start-query-execution",
                "--query-string",
                query,
                "--work-group",
                self.args.workgroup,
                "--client-request-token",
                row["client_request_token"],
                "--result-reuse-configuration",
                "ResultReuseByAgeConfiguration={Enabled=false}",
            ],
            self.region,
        )
        return value["QueryExecutionId"]

    def submit_pending(self, capacity):
        pending = [
            row for row in self.manifest["objects"] if row.get("status") == "pending"
        ][:capacity]
        if not pending:
            return 0
        failures = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(pending)) as executor:
            future_rows = {
                executor.submit(self.start_query, row): row for row in pending
            }
            for future in concurrent.futures.as_completed(future_rows):
                row = future_rows[future]
                try:
                    row["query_execution_id"] = future.result()
                    row["status"] = "submitted"
                    row["submitted_at"] = utc_now()
                    row.pop("submission_error", None)
                except Exception as exc:
                    row["submission_error"] = str(exc)
                    failures.append(row)
        if failures and len(failures) == len(pending):
            raise RuntimeError("all query submissions failed; first error: {}".format(
                failures[0]["submission_error"]
            ))
        return len(pending) - len(failures)

    def print_progress(self):
        summary = self.summary()
        counts = summary["status_counts"]
        print(
            "progress complete={}/{} running={} pending={} failed={} scanned_bytes={}".format(
                counts.get("complete", 0),
                summary["object_count"],
                counts.get("running", 0) + counts.get("submitted", 0),
                counts.get("pending", 0),
                counts.get("failed", 0),
                summary["total_scanned_bytes"],
            ),
            flush=True,
        )

    def run(self):
        self.verify_workgroup()
        source_objects = self.list_source_objects()
        imported = self.load_or_create_manifest(source_objects)
        if imported:
            print("imported {} completed pilot queries".format(imported), flush=True)
        if self.args.retry_failed:
            for row in self.manifest["objects"]:
                if row.get("status") == "failed":
                    attempt = int(row.get("attempt", 0)) + 1
                    row["status"] = "pending"
                    row["attempt"] = attempt
                    row["client_request_token"] = client_token(
                        row["source_key"], attempt
                    )
                    row.pop("query_execution_id", None)
                    row.pop("failure_reason", None)
        self.verify_completed()
        self.recover_remote_states()
        self.finalize_succeeded()
        self.persist()
        self.print_progress()

        while True:
            self.recover_remote_states()
            self.finalize_succeeded()
            active_count = sum(
                1
                for row in self.manifest["objects"]
                if row.get("status") in ACTIVE_STATES
            )
            capacity = max(0, self.args.concurrency - active_count)
            submitted = self.submit_pending(capacity) if capacity else 0
            self.persist()
            self.print_progress()

            counts = self.summary()["status_counts"]
            if (
                counts.get("pending", 0) == 0
                and counts.get("submitted", 0) == 0
                and counts.get("running", 0) == 0
                and counts.get("query_succeeded", 0) == 0
                and counts.get("copied", 0) == 0
            ):
                break
            if not submitted:
                time.sleep(self.args.poll_seconds)

        final_summary = self.summary()
        write_json(self.local_summary, final_summary)
        self.upload_json(self.local_summary, self.summary_key)
        print(json.dumps(final_summary, ensure_ascii=False, indent=2), flush=True)
        return 0 if final_summary["status_counts"].get("failed", 0) == 0 else 2


def build_parser():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--region", default=DEFAULT_REGION)
    parser.add_argument("--output-bucket", default=DEFAULT_OUTPUT_BUCKET)
    parser.add_argument("--output-prefix", default=DEFAULT_OUTPUT_PREFIX)
    parser.add_argument("--workgroup", default=DEFAULT_WORKGROUP)
    parser.add_argument("--database", default=DEFAULT_DATABASE)
    parser.add_argument("--table", default=DEFAULT_TABLE)
    parser.add_argument("--local-manifest", type=Path, default=DEFAULT_LOCAL_MANIFEST)
    parser.add_argument("--pilot-manifest", type=Path, default=DEFAULT_PILOT_MANIFEST)
    parser.add_argument("--concurrency", type=int, default=8)
    parser.add_argument("--poll-seconds", type=float, default=3.0)
    parser.add_argument("--expected-objects", type=int, default=128)
    parser.add_argument("--allow-object-count-mismatch", action="store_true")
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--retry-failed", action="store_true")
    return parser


def main(argv=None):
    args = build_parser().parse_args(argv)
    if args.concurrency < 1 or args.poll_seconds < 0 or args.expected_objects < 1:
        raise SystemExit("concurrency/expected-objects must be positive and poll-seconds non-negative")
    runner = Runner(args)
    try:
        return runner.run()
    except KeyboardInterrupt:
        if runner.manifest is not None:
            write_json(runner.local_manifest, runner.manifest)
        print("interrupted; local manifest saved for --resume", file=sys.stderr)
        return 130
    except Exception as exc:
        if runner.manifest is not None:
            write_json(runner.local_manifest, runner.manifest)
        print("failed; local manifest saved for --resume: {}".format(exc), file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
