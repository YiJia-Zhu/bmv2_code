#!/usr/bin/env python3
"""Recover origin URLs reached through directory -> release/snapshot edges."""

import csv
import datetime as dt
import hashlib
import json
import os
import subprocess
import time
from collections import defaultdict, deque
from pathlib import Path

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from botocore import UNSIGNED
from botocore.config import Config

from probe_swh_missing_revision_orc import ROOT, missing_closure
from run_swh_route_a_revision_dates import private_session
from run_swh_route_a_sample import GRAPH_PREFIX, merged_ranges


GRAPH_ROOT = ROOT.parent
OUTPUT = ROOT / "nonrevision-origin-v1"
BUCKET = "swh2p4"
BUCKET_OWNER = "728516300718"
PREFIX = "new-account-588301174757/route-a/nonrevision-origin-v1/"
DATABASE = "swh_p4_20260604"
ORIGIN_TABLE = "origin_20260604"
WORKGROUP = "swh-route-a-origin-full-bounded"
NODE2SWHID_KEY = GRAPH_PREFIX + "graph.node2swhid.bin"
TRANSIT_TYPES = (3, 4, 5)

EVIDENCE_SCHEMA = pa.schema([
    ("seed_directory_id", pa.string()),
    ("seed_node_id", pa.int64()),
    ("anchor_directory_node_id", pa.int64()),
    ("origin_node_id", pa.int64()),
    ("origin_id", pa.string()),
    ("origin_url", pa.string()),
    ("directory_hops", pa.int32()),
    ("graph_path_nodes", pa.list_(pa.int64())),
    ("graph_path_types", pa.list_(pa.int8())),
    ("mapping_basis", pa.string()),
])


def save_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".partial")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(str(temporary), str(path))


def query_reader(reader, nodes):
    reader.stdin.write(json.dumps({"op": "neighbors", "nodes": nodes}) + "\n")
    reader.stdin.flush()
    result = json.loads(reader.stdout.readline())
    if result.get("event") != "neighbors" or [row["node_id"] for row in result["results"]] != nodes:
        raise RuntimeError("graph reader returned an unexpected batch")
    if any(row["truncated"] for row in result["results"]):
        raise RuntimeError("graph reader truncated a needed adjacency list")
    return result["results"]


def graph_edges(closure, parent_edges):
    binary = ROOT / "build/release/origin"
    reader = subprocess.Popen([str(binary), str(GRAPH_ROOT)],
                              stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                              stderr=subprocess.PIPE, text=True, bufsize=1)
    adjacency = {}
    types = {node: 1 for node in closure}
    try:
        while True:
            line = reader.stdout.readline()
            if not line:
                raise RuntimeError("graph reader exited before ready: " + reader.stderr.read())
            event = json.loads(line)
            if event.get("event") == "ready":
                break
        directory_nodes = sorted(closure)
        anchors = set()
        for offset in range(0, len(directory_nodes), 512):
            for row in query_reader(reader, directory_nodes[offset:offset + 512]):
                node = row["node_id"]
                if row["node_type"] != 1:
                    raise RuntimeError("closure contains a non-directory node")
                neighbors = [(item["node_id"], item["node_type"]) for item in row["neighbors"]]
                if {target for target, kind in neighbors if kind == 1} != parent_edges.get(node, set()):
                    raise RuntimeError("directory parent edges disagree with saved closure")
                if any(kind not in (1, 3, 5) for _target, kind in neighbors):
                    raise RuntimeError("unexpected directory neighbor type")
                adjacency[node] = neighbors
                anchors.update(target for target, kind in neighbors if kind in (3, 5))
        frontier = set(anchors)
        visited = set()
        while frontier:
            batch_nodes = sorted(frontier - visited)
            frontier = set()
            if len(visited) + len(batch_nodes) > 100000:
                raise RuntimeError("non-directory traversal exceeded node cap")
            for offset in range(0, len(batch_nodes), 512):
                for row in query_reader(reader, batch_nodes[offset:offset + 512]):
                    node = row["node_id"]
                    kind = row["node_type"]
                    if kind not in TRANSIT_TYPES:
                        raise RuntimeError("unexpected non-directory node type")
                    visited.add(node)
                    types[node] = kind
                    neighbors = [(item["node_id"], item["node_type"]) for item in row["neighbors"]]
                    adjacency[node] = neighbors
                    for target, target_kind in neighbors:
                        if target_kind == 2:
                            types[target] = 2
                        elif target_kind in TRANSIT_TYPES and target not in visited:
                            frontier.add(target)
        return adjacency, types, len(anchors), len(visited)
    finally:
        if reader.poll() is None:
            reader.stdin.write('{"op":"quit"}\n')
            reader.stdin.flush()
            reader.wait(timeout=15)
        reader.stdin.close()
        reader.stdout.close()
        reader.stderr.close()


def shortest_paths(seeds, adjacency, types):
    paths = []
    for seed in seeds:
        start = seed["seed_node_id"]
        queue = deque([start])
        previous = {start: None}
        while queue:
            node = queue.popleft()
            for target, kind in sorted(adjacency.get(node, ())):
                if target in previous:
                    continue
                previous[target] = node
                if kind != 2:
                    queue.append(target)
        origins = sorted(node for node in previous if types.get(node) == 2)
        for origin in origins:
            chain = []
            node = origin
            while node is not None:
                chain.append(node)
                node = previous[node]
            chain.reverse()
            kinds = [types[node] for node in chain]
            anchor_index = next(index for index, kind in enumerate(kinds) if kind != 1) - 1
            if anchor_index < 0 or kinds[-1] != 2:
                raise RuntimeError("invalid directory-to-origin path")
            paths.append({"seed_directory_id": seed["seed_directory_id"],
                          "seed_node_id": start, "anchor_directory_node_id": chain[anchor_index],
                          "origin_node_id": origin, "directory_hops": anchor_index,
                          "graph_path_nodes": chain, "graph_path_types": kinds,
                          "mapping_basis": "directory_release_or_snapshot"})
    return paths


def origin_hashes(nodes, public):
    head = public.head_object(Bucket="softwareheritage", Key=NODE2SWHID_KEY)
    result = {}
    requests = 0
    bytes_read = 0
    for start, end, members in merged_ranges(sorted(nodes), 22, max_gap=4096, max_span=1048576):
        response = public.get_object(Bucket="softwareheritage", Key=NODE2SWHID_KEY,
                                     Range="bytes={}-{}".format(start, end), IfMatch=head["ETag"])
        with response["Body"] as body:
            payload = body.read()
        if response["ResponseMetadata"]["HTTPStatusCode"] != 206 or len(payload) != end - start + 1:
            raise RuntimeError("invalid origin hash Range response")
        for node in members:
            record = payload[node * 22 - start:node * 22 - start + 22]
            if len(record) != 22 or record[:2] != b"\x01\x02":
                raise RuntimeError("origin node has an unexpected SWHID record")
            result[node] = record[2:].hex()
        requests += 1
        bytes_read += len(payload)
    if len(result) != len(nodes):
        raise RuntimeError("not all origin nodes were decoded")
    return result, {"range_requests": requests, "range_bytes": bytes_read,
                    "mapping_etag": head["ETag"]}


def lookup_urls(hashes, session):
    glue = session.client("glue")
    table = glue.get_table(DatabaseName=DATABASE, Name=ORIGIN_TABLE)["Table"]
    expected = "s3://softwareheritage/graph/2026-06-04/orc/origin/"
    if table["StorageDescriptor"]["Location"].rstrip("/") != expected.rstrip("/"):
        raise RuntimeError("origin table points to an unexpected dataset")
    sql = "SELECT id, url FROM {}.{} WHERE id IN ({})".format(
        DATABASE, ORIGIN_TABLE, ",".join("'{}'".format(value) for value in sorted(hashes.values())))
    state_path = OUTPUT / "athena-query.json"
    state = json.loads(state_path.read_text(encoding="utf-8")) if state_path.exists() else {}
    sql_hash = hashlib.sha256(sql.encode("utf-8")).hexdigest()
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
        save_json(state_path, state)
    while True:
        execution = athena.get_query_execution(QueryExecutionId=query_id)["QueryExecution"]
        status = execution["Status"]["State"]
        if status in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(2)
    if status != "SUCCEEDED":
        state.update(status=status.lower(), reason=execution["Status"].get("StateChangeReason"))
        save_json(state_path, state)
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
                raise RuntimeError("origin URL does not match SHA-1 ID")
            urls[raw] = url
    state.update(status="passed", scanned_bytes=int(execution.get("Statistics", {}).get(
        "DataScannedInBytes", 0)), origin_urls=len(urls))
    save_json(state_path, state)
    if set(urls) != set(hashes.values()):
        raise RuntimeError("origin ORC did not resolve every graph origin")
    return urls, state


def archive(paths, s3):
    manifest = []
    for path in paths:
        key = PREFIX + path.name
        payload = path.read_bytes()
        checksum = hashlib.sha256(payload).hexdigest()
        s3.put_object(Bucket=BUCKET, Key=key, Body=payload,
                      ExpectedBucketOwner=BUCKET_OWNER, ServerSideEncryption="AES256",
                      Metadata={"sha256": checksum})
        head = s3.head_object(Bucket=BUCKET, Key=key, ExpectedBucketOwner=BUCKET_OWNER)
        if head["ContentLength"] != len(payload) or head.get("Metadata", {}).get("sha256") != checksum:
            raise RuntimeError("S3 archive verification failed: " + key)
        manifest.append({"key": key, "bytes": len(payload), "sha256": checksum})
    marker = {"status": "passed", "objects": manifest,
              "completed_at_utc": dt.datetime.now(dt.timezone.utc).isoformat()}
    save_json(OUTPUT / "complete.json", marker)
    s3.put_object(Bucket=BUCKET, Key=PREFIX + "complete.json",
                  Body=(OUTPUT / "complete.json").read_bytes(),
                  ExpectedBucketOwner=BUCKET_OWNER, ServerSideEncryption="AES256")


def main():
    started = time.monotonic()
    OUTPUT.mkdir(parents=True, exist_ok=True)
    seeds, closure, _roots = missing_closure()
    parents = defaultdict(set)
    for row in pq.read_table(ROOT / "merge/parent_edges.parquet").to_pylist():
        if row["child_node_id"] in closure:
            parents[row["child_node_id"]].add(row["parent_node_id"])
    adjacency, types, anchors, transit = graph_edges(closure, parents)
    paths = shortest_paths(seeds, adjacency, types)
    origin_nodes = {row["origin_node_id"] for row in paths}
    print(json.dumps({"event": "graph_resolved", "seeds": len(seeds),
                      "anchors": anchors, "transit_nodes": transit,
                      "origins": len(origin_nodes), "seed_origin_pairs": len(paths)}), flush=True)
    if len({row["seed_directory_id"] for row in paths}) != 337 or not origin_nodes:
        raise RuntimeError("not all missing seeds reached an origin")
    public = boto3.client("s3", region_name="us-east-1",
                          config=Config(signature_version=UNSIGNED))
    hashes, range_stats = origin_hashes(origin_nodes, public)
    session = private_session()
    if session.client("sts").get_caller_identity()["Account"] != "588301174757":
        raise RuntimeError("unexpected AWS account")
    urls, query = lookup_urls(hashes, session)
    for row in paths:
        row["origin_id"] = hashes[row["origin_node_id"]]
        row["origin_url"] = urls[row["origin_id"]]
    paths.sort(key=lambda row: (row["seed_directory_id"], row["origin_url"]))
    evidence = OUTPUT / "seed_origin.parquet"
    pq.write_table(pa.Table.from_pylist(paths, schema=EVIDENCE_SCHEMA), evidence,
                   compression="zstd")
    origin_rows = sorted([(node, hashes[node], urls[hashes[node]]) for node in origin_nodes],
                         key=lambda row: row[2])
    url_csv = OUTPUT / "origin_urls.csv"
    with url_csv.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["origin_node_id", "origin_id", "origin_url"])
        writer.writerows(origin_rows)
    summary = {"status": "passed", "missing_seeds": len(seeds),
               "mapped_seeds": len({row["seed_directory_id"] for row in paths}),
               "closure_directories": len(closure), "anchor_nodes": anchors,
               "transit_nodes": transit, "origin_nodes": len(origin_nodes),
               "origin_urls": len(urls), "seed_origin_pairs": len(paths),
               "mapping_basis": "directory_release_or_snapshot",
               "origin_range": range_stats, "athena_query_id": query["query_id"],
               "athena_scanned_bytes": query["scanned_bytes"],
               "athena_scan_usd_estimate": round(query["scanned_bytes"] * 5.0 / 10 ** 12, 4),
               "elapsed_seconds": round(time.monotonic() - started, 2)}
    save_json(OUTPUT / "summary.json", summary)
    archive([evidence, url_csv, OUTPUT / "summary.json", OUTPUT / "athena-query.json"],
            session.client("s3"))
    print(json.dumps(summary, sort_keys=True), flush=True)


if __name__ == "__main__":
    main()
