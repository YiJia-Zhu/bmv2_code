#!/usr/bin/env python3
"""Cross-check unmapped SWH directories against the public revision ORC."""

import argparse
import concurrent.futures
import datetime as dt
import hashlib
import json
import os
import threading
import time
from collections import defaultdict, deque
from pathlib import Path

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from botocore import UNSIGNED
from botocore.config import Config

from run_swh_route_a_revision_dates import private_session
from run_swh_route_a_sample import GRAPH_PREFIX, merged_ranges


ROOT = Path("/mnt/8T/xgr/swh-graph-poc-20260604/route-a-revision-origin-v1")
OUTPUT = ROOT / "missing-revision-orc-probe-v1"
BUCKET = "swh2p4"
BUCKET_OWNER = "728516300718"
PREFIX = "new-account-588301174757/route-a/missing-revision-orc-probe-v1/"
DATABASE = "swh_p4_20260604"
REVISION_TABLE = "revision_20260604_route_a_v1"
LOOKUP_TABLE = "missing_revision_dirs_route_a_v1"
WORKGROUP = "swh-route-a-origin-full-bounded"
PUBLIC_KEY = GRAPH_PREFIX + "graph.node2swhid.bin"
INVENTORY = Path("data/raw/swh_p4/discovery/probe8_spark_v1/inventory.json")


def save_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".partial")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(str(temporary), str(path))


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def missing_closure():
    summary = pq.read_table(ROOT / "full/results/seed_summary/part-00000.parquet").to_pylist()
    missing = [row for row in summary if row["mapping_status"] == "no_revision_entry"]
    if len(missing) != 337:
        raise RuntimeError("unmapped seed count changed")
    parents = defaultdict(set)
    for row in pq.read_table(ROOT / "merge/parent_edges.parquet").to_pylist():
        parents[row["child_node_id"]].add(row["parent_node_id"])
    revision_nodes = {row["directory_node_id"] for row in
                      pq.read_table(ROOT / "merge/directory_revision.parquet").to_pylist()}
    closure = set()
    roots = set()
    for row in missing:
        queue = deque([row["seed_node_id"]])
        seen = set(queue)
        while queue:
            node = queue.popleft()
            closure.add(node)
            if node in revision_nodes:
                raise RuntimeError("unmapped seed reaches a cached revision")
            if not parents.get(node):
                roots.add(node)
            for parent in parents.get(node, ()):
                if parent not in seen:
                    seen.add(parent)
                    queue.append(parent)
    return missing, closure, roots


def directory_hashes(nodes, public):
    known = {row["node_id"]: row["directory_id"] for row in pq.read_table(
        ROOT.parent / "route-a-d9-d16-v1/known_directory_ids.parquet").to_pylist()}
    missing = sorted(nodes - known.keys())
    head = public.head_object(Bucket="softwareheritage", Key=PUBLIC_KEY)
    requests = 0
    bytes_read = 0
    for start, end, members in merged_ranges(missing, 22, max_gap=4096, max_span=1048576):
        response = public.get_object(Bucket="softwareheritage", Key=PUBLIC_KEY,
                                     Range="bytes={}-{}".format(start, end),
                                     IfMatch=head["ETag"])
        with response["Body"] as body:
            payload = body.read()
        if response["ResponseMetadata"]["HTTPStatusCode"] != 206 or len(payload) != end - start + 1:
            raise RuntimeError("invalid public graph range response")
        for node in members:
            record = payload[node * 22 - start:node * 22 - start + 22]
            if len(record) != 22 or record[:2] != b"\x01\x01":
                raise RuntimeError("node {} is not a SWH directory".format(node))
            known[node] = record[2:].hex()
        requests += 1
        bytes_read += len(payload)
    values = {node: known[node] for node in nodes}
    if len(set(values.values())) != len(nodes):
        raise RuntimeError("directory hash collision or duplicate graph node")
    return values, {"range_requests": requests, "range_bytes": bytes_read,
                    "mapping_etag": head["ETag"], "reused_known": len(nodes) - len(missing)}


def ensure_lookup(glue, s3, hashes):
    path = OUTPUT / "directory_ids.parquet"
    if not path.exists():
        pq.write_table(pa.Table.from_pylist([{"directory": value} for value in sorted(hashes.values())],
                                          schema=pa.schema([("directory", pa.string())])), path,
                       compression="zstd")
    staged = PREFIX + "input/directory_ids.parquet"
    checksum = digest(path)
    s3.upload_file(str(path), BUCKET, staged,
                   ExtraArgs={"ExpectedBucketOwner": BUCKET_OWNER,
                              "ServerSideEncryption": "AES256",
                              "Metadata": {"sha256": checksum},
                              "ChecksumAlgorithm": "SHA256"})
    head = s3.head_object(Bucket=BUCKET, Key=staged, ExpectedBucketOwner=BUCKET_OWNER)
    if head["ContentLength"] != path.stat().st_size or head.get("Metadata", {}).get("sha256") != checksum:
        raise RuntimeError("staged directory lookup failed verification")
    location = "s3://{}/{}/input/".format(BUCKET, PREFIX.rstrip("/"))
    try:
        table = glue.get_table(DatabaseName=DATABASE, Name=LOOKUP_TABLE)["Table"]
    except glue.exceptions.EntityNotFoundException:
        glue.create_table(DatabaseName=DATABASE, TableInput={
            "Name": LOOKUP_TABLE, "TableType": "EXTERNAL_TABLE",
            "Parameters": {"EXTERNAL": "TRUE", "classification": "parquet"},
            "StorageDescriptor": {
                "Columns": [{"Name": "directory", "Type": "string"}], "Location": location,
                "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                "SerdeInfo": {"SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"},
            },
        })
    else:
        if table["StorageDescriptor"]["Location"].rstrip("/") != location.rstrip("/"):
            raise RuntimeError("lookup table points to an unexpected location")
    return checksum


def query_shard(index, item, athena, lock):
    state_path = OUTPUT / "state" / "shard_{:03d}.json".format(index)
    state = json.loads(state_path.read_text(encoding="utf-8")) if state_path.exists() else {}
    if state.get("status") == "passed":
        return state
    source = "s3://softwareheritage/" + item["key"]
    sql = ('SELECT r.id, r.directory, r."$path" FROM "{}"."{}" r '
           'JOIN "{}"."{}" d ON r.directory=d.directory '
           'WHERE r."$path"=\'{}\'').format(
               DATABASE, REVISION_TABLE, DATABASE, LOOKUP_TABLE, source)
    query_id = state.get("query_id")
    if not query_id:
        response = athena.start_query_execution(
            QueryString=sql, QueryExecutionContext={"Database": DATABASE},
            WorkGroup=WORKGROUP,
            ClientRequestToken=hashlib.sha256(sql.encode("utf-8")).hexdigest())
        query_id = response["QueryExecutionId"]
        state = {"status": "submitted", "query_id": query_id, "source": source,
                 "submitted_at_utc": dt.datetime.now(dt.timezone.utc).isoformat()}
        save_json(state_path, state)
    while True:
        execution = athena.get_query_execution(QueryExecutionId=query_id)["QueryExecution"]
        status = execution["Status"]["State"]
        if status in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(3)
    if status != "SUCCEEDED":
        state.update(status=status.lower(), reason=execution["Status"].get("StateChangeReason"))
        save_json(state_path, state)
        raise RuntimeError("Athena shard {} failed: {}".format(index, state["reason"]))
    matches = []
    first = True
    for page in athena.get_paginator("get_query_results").paginate(QueryExecutionId=query_id):
        for row in page["ResultSet"].get("Rows", []):
            if first:
                first = False
                continue
            values = [cell.get("VarCharValue") for cell in row["Data"]]
            matches.append({"revision_id": values[0], "directory_id": values[1],
                            "source": values[2]})
    state.update(status="passed", scanned_bytes=int(execution.get("Statistics", {}).get(
        "DataScannedInBytes", 0)), matches=matches,
        completed_at_utc=dt.datetime.now(dt.timezone.utc).isoformat())
    save_json(state_path, state)
    with lock:
        print(json.dumps({"shard": index, "status": "passed", "matches": len(matches),
                          "scanned_bytes": state["scanned_bytes"]}), flush=True)
    return state


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--max-scan-usd", type=float, default=5.0)
    args = parser.parse_args()
    OUTPUT.mkdir(parents=True, exist_ok=True)
    session = private_session()
    account = session.client("sts").get_caller_identity()["Account"]
    if account != "588301174757":
        raise RuntimeError("unexpected AWS account: " + account)
    public = boto3.client("s3", region_name="us-east-1", config=Config(signature_version=UNSIGNED))
    missing, closure, roots = missing_closure()
    hashes, mapping = directory_hashes(closure, public)
    inventory = json.loads(INVENTORY.read_text(encoding="utf-8"))
    shards = sorted(inventory["tables"]["revision"]["objects"], key=lambda row: row["key"])
    if len(shards) != 128:
        raise RuntimeError("revision ORC inventory is incomplete")
    maximum = sum(row["size"] for row in shards) * 5.0 / 10 ** 12
    if maximum > args.max_scan_usd:
        raise RuntimeError("compressed ORC scan ceiling ${:.2f} exceeds limit".format(maximum))
    cohort = {"missing_seeds": len(missing), "closure_nodes": len(closure),
              "terminal_nodes": len(roots), "lookup_hashes": len(hashes),
              "mapping": mapping, "compressed_scan_usd_ceiling": maximum}
    save_json(OUTPUT / "cohort.json", cohort)
    s3 = session.client("s3")
    glue = session.client("glue")
    lookup_sha = ensure_lookup(glue, s3, hashes)
    athena = session.client("athena", config=Config(max_pool_connections=args.workers + 4))
    lock = threading.Lock()
    started = time.monotonic()
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = [pool.submit(query_shard, index, item, athena, lock)
                   for index, item in enumerate(shards)]
        states = [future.result() for future in concurrent.futures.as_completed(futures)]
    matches = [match for state in states for match in state["matches"]]
    summary = dict(cohort, status="passed", lookup_sha256=lookup_sha,
                   completed_shards=len(states), matching_revision_rows=len(matches),
                   matching_directories=len({row["directory_id"] for row in matches}),
                   scanned_bytes=sum(state["scanned_bytes"] for state in states),
                   elapsed_seconds=round(time.monotonic() - started, 2))
    summary["athena_scan_usd_estimate"] = round(summary["scanned_bytes"] * 5.0 / 10 ** 12, 4)
    save_json(OUTPUT / "matches.json", matches)
    save_json(OUTPUT / "summary.json", summary)
    for name in ("cohort.json", "matches.json", "summary.json"):
        path = OUTPUT / name
        s3.put_object(Bucket=BUCKET, Key=PREFIX + "results/" + name,
                      Body=path.read_bytes(), ExpectedBucketOwner=BUCKET_OWNER,
                      ServerSideEncryption="AES256", Metadata={"sha256": digest(path)})
    print(json.dumps(summary, sort_keys=True), flush=True)


if __name__ == "__main__":
    main()
