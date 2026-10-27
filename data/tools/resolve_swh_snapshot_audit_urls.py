#!/usr/bin/env python3
"""Resolve newly found snapshot-path origins and archive the full audit."""

import csv
import datetime as dt
import hashlib
import json
import os
import time

import boto3
import pyarrow.parquet as pq
from botocore import UNSIGNED
from botocore.config import Config

from audit_swh_directory_snapshot_edges import OUTPUT, save_json
from audit_swh_snapshot_origins import (ENTRY_ORIGIN_SCHEMA, SEED_ORIGIN_SCHEMA,
                                        known_urls, save_table)
from resolve_swh_nonrevision_origins import (BUCKET, BUCKET_OWNER, DATABASE,
                                             ORIGIN_TABLE, WORKGROUP, origin_hashes)
from run_swh_route_a_revision_dates import private_session


PREFIX = "new-account-588301174757/route-a/directory-snapshot-audit-v1/"
QUERY_STATE = OUTPUT / "origin-url-query.json"


def lookup_urls(hashes, session):
    table = session.client("glue").get_table(
        DatabaseName=DATABASE, Name=ORIGIN_TABLE)["Table"]
    expected = "s3://softwareheritage/graph/2026-06-04/orc/origin/"
    if table["StorageDescriptor"]["Location"].rstrip("/") != expected.rstrip("/"):
        raise RuntimeError("origin table location changed")
    sql = "SELECT id, url FROM {}.{} WHERE id IN ({})".format(
        DATABASE, ORIGIN_TABLE, ",".join(
            "'{}'".format(value) for value in sorted(hashes.values())))
    sql_hash = hashlib.sha256(sql.encode("utf-8")).hexdigest()
    state = json.loads(QUERY_STATE.read_text(encoding="utf-8")) if QUERY_STATE.exists() else {}
    if state and state.get("sql_sha256") != sql_hash:
        raise RuntimeError("saved Athena query has a different input")
    athena = session.client("athena")
    query_id = state.get("query_id")
    if not query_id:
        response = athena.start_query_execution(
            QueryString=sql, QueryExecutionContext={"Database": DATABASE},
            WorkGroup=WORKGROUP, ClientRequestToken=sql_hash)
        query_id = response["QueryExecutionId"]
        state = {"query_id": query_id, "sql_sha256": sql_hash, "status": "submitted"}
        save_json(QUERY_STATE, state)
    while True:
        execution = athena.get_query_execution(QueryExecutionId=query_id)["QueryExecution"]
        status = execution["Status"]["State"]
        if status in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(2)
    if status != "SUCCEEDED":
        state.update(status=status.lower(),
                     reason=execution["Status"].get("StateChangeReason"))
        save_json(QUERY_STATE, state)
        raise RuntimeError("origin URL query failed: " + str(state["reason"]))
    urls = {}
    first = True
    for page in athena.get_paginator("get_query_results").paginate(QueryExecutionId=query_id):
        for row in page["ResultSet"].get("Rows", []):
            if first:
                first = False
                continue
            raw, url = [cell.get("VarCharValue") for cell in row["Data"]]
            if hashlib.sha1(url.encode("utf-8")).hexdigest() != raw:
                raise RuntimeError("origin URL SHA1 mismatch")
            urls[raw] = url
    scanned = int(execution.get("Statistics", {}).get("DataScannedInBytes", 0))
    state.update(status="passed", scanned_bytes=scanned, origin_urls=len(urls))
    save_json(QUERY_STATE, state)
    if set(urls) != set(hashes.values()):
        raise RuntimeError("origin ORC did not resolve all new origin nodes")
    return urls, state


def archive(paths, s3):
    objects = []
    for path in paths:
        key = PREFIX + path.name
        payload = path.read_bytes()
        checksum = hashlib.sha256(payload).hexdigest()
        s3.put_object(Bucket=BUCKET, Key=key, Body=payload,
                      ExpectedBucketOwner=BUCKET_OWNER, ServerSideEncryption="AES256",
                      Metadata={"sha256": checksum})
        head = s3.head_object(Bucket=BUCKET, Key=key,
                              ExpectedBucketOwner=BUCKET_OWNER)
        if head["ContentLength"] != len(payload) or head.get("Metadata", {}).get("sha256") != checksum:
            raise RuntimeError("S3 archive verification failed: " + key)
        objects.append({"key": key, "bytes": len(payload), "sha256": checksum})
    marker = {"status": "passed", "objects": objects,
              "completed_at_utc": dt.datetime.now(dt.timezone.utc).isoformat()}
    save_json(OUTPUT / "complete.json", marker)
    marker_bytes = (OUTPUT / "complete.json").read_bytes()
    s3.put_object(Bucket=BUCKET, Key=PREFIX + "complete.json", Body=marker_bytes,
                  ExpectedBucketOwner=BUCKET_OWNER, ServerSideEncryption="AES256",
                  Metadata={"sha256": hashlib.sha256(marker_bytes).hexdigest()})
    readback = s3.get_object(Bucket=BUCKET, Key=PREFIX + "complete.json",
                             ExpectedBucketOwner=BUCKET_OWNER)
    with readback["Body"] as body:
        if body.read() != marker_bytes:
            raise RuntimeError("S3 completion marker readback mismatch")
    return marker


def main():
    started = time.monotonic()
    summary_path = OUTPUT / "origin-summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    if summary["status"] not in ("graph_passed", "passed") or summary["directory_nodes"] != 523435:
        raise RuntimeError("local snapshot graph audit is incomplete")
    new_nodes = set(summary["new_origin_nodes"])
    public = boto3.client("s3", region_name="us-east-1",
                          config=Config(signature_version=UNSIGNED))
    hashes, range_stats = origin_hashes(new_nodes, public) if new_nodes else ({}, {})
    session = private_session()
    if session.client("sts").get_caller_identity()["Account"] != "588301174757":
        raise RuntimeError("unexpected AWS account")
    urls, query = lookup_urls(hashes, session) if hashes else ({}, {})
    known = known_urls()
    for node, raw in hashes.items():
        known[node] = (raw, urls[raw])
    entry_rows = pq.read_table(OUTPUT / "entry_origin.parquet").to_pylist()
    seed_rows = pq.read_table(OUTPUT / "seed_origin.parquet").to_pylist()
    for rows in (entry_rows, seed_rows):
        for row in rows:
            row["origin_id"], row["origin_url"] = known[row["origin_node_id"]]
    save_table(OUTPUT / "entry_origin.parquet", entry_rows, ENTRY_ORIGIN_SCHEMA)
    save_table(OUTPUT / "seed_origin.parquet", seed_rows, SEED_ORIGIN_SCHEMA)
    old_urls = {url for _raw, url in known_urls().values()}
    all_urls = {row["origin_url"] for row in entry_rows}
    if len(all_urls - old_urls) != len(new_nodes):
        raise RuntimeError("new URL count disagrees with new origin node count")
    csv_path = OUTPUT / "new_origin_urls.csv"
    temporary = csv_path.with_suffix(".partial.csv")
    with temporary.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["origin_node_id", "origin_id", "origin_url"])
        for node in sorted(new_nodes):
            writer.writerow([node, known[node][0], known[node][1]])
    os.replace(str(temporary), str(csv_path))
    summary.update(status="passed", new_origin_urls=len(new_nodes),
                   known_origin_urls_total=len(old_urls) + len(new_nodes),
                   origin_range=range_stats, athena_query_id=query.get("query_id"),
                   athena_scanned_bytes=query.get("scanned_bytes", 0),
                   athena_scan_usd_estimate=round(
                       query.get("scanned_bytes", 0) * 5 / 10 ** 12, 4),
                   resolution_elapsed_seconds=round(time.monotonic() - started, 2))
    save_json(summary_path, summary)
    paths = [OUTPUT / name for name in (
        "summary.json", "origin-summary.json", "direct_edges.parquet",
        "transit_edges.parquet", "entry_origin.parquet", "seed_anchor.parquet",
        "seed_origin.parquet", "new_origin_urls.csv", "origin-url-query.json")]
    archive(paths, session.client("s3"))
    print(json.dumps(summary, sort_keys=True), flush=True)


if __name__ == "__main__":
    main()
