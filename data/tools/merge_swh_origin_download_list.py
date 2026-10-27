#!/usr/bin/env python3
"""Publish a unified SWH origin view and a local origin download list."""

import csv
import datetime as dt
import hashlib
import json
import os
import time
from collections import defaultdict
from pathlib import Path
from urllib.parse import urlparse

import pyarrow.parquet as pq
from botocore.exceptions import ClientError

from audit_swh_directory_snapshot_edges import GRAPH_ROOT, digest, save_json
from run_swh_route_a_revision_dates import private_session


ROOT = GRAPH_ROOT / "route-a-revision-origin-v1"
OUTPUT = ROOT / "unified-origin-v1"
LOCAL_DOWNLOAD = Path("data/raw/swh_p4/download_lists/swh_20260604_origin_download_list_v1.csv")
BUCKET = "swh2p4"
BUCKET_OWNER = "728516300718"
DATABASE = "swh_p4_20260604"
WORKGROUP = "swh-route-a-origin-full-bounded"
OLD_TOKEN = "425ca2322f7030ec"
CSV_COLUMNS = (
    "origin_id", "origin_node_id", "origin_url", "origin_host",
    "origin_kind_hint", "has_revision_path", "has_release_snapshot_path",
    "release_snapshot_seed_count", "first_candidate_revision_id",
    "first_candidate_revision_swhid", "first_candidate_revision_date",
    "first_candidate_ordering_basis", "first_candidate_package_version",
    "first_candidate_selection_status", "download_status",
)


def origin_kind(url):
    parsed = urlparse(url)
    host = parsed.netloc.lower()
    if host in ("github.com", "gitlab.com", "bitbucket.org", "gitee.com"):
        return "forge"
    if host in ("pkg.go.dev", "pypi.org", "opam.ocaml.org") or url.startswith("opam+"):
        return "package_registry"
    if host in ("doi.org", "cache.nixos.org") or parsed.scheme == "deb":
        return "archive_or_package"
    return "other"


def inputs():
    markers = {
        "revision_mapping": ROOT / "full/complete.json",
        "snapshot_audit": ROOT / "directory-snapshot-audit-v1/complete.json",
        "revision_rank": ROOT / "revision-rank-v2/complete.json",
    }
    identity = {"version": "swh-unified-origin-v1", "graph_release": "2026-06-04",
                "inputs": {name: digest(path) for name, path in markers.items()}}
    for name, path in markers.items():
        marker = json.loads(path.read_text(encoding="utf-8"))
        if marker.get("status", "passed") != "passed":
            raise RuntimeError("input is incomplete: " + name)
    return identity


def manifest_rows():
    rank_path = ROOT / "revision-rank-v2/origin_latest_candidates.parquet"
    direct_path = ROOT / "directory-snapshot-audit-v1/seed_origin.parquet"
    ranks = pq.read_table(rank_path).to_pylist()
    directs = pq.read_table(direct_path).to_pylist()
    if len(ranks) != 10107 or len(directs) != 966:
        raise RuntimeError("input row count changed")
    by_id = {}
    for row in ranks:
        raw = row["origin_id"]
        if raw in by_id or hashlib.sha1(row["origin_url"].encode("utf-8")).hexdigest() != raw:
            raise RuntimeError("revision origin identity invalid")
        by_id[raw] = row
    direct_seeds = defaultdict(set)
    direct_nodes = {}
    for row in directs:
        raw = row["origin_id"]
        if hashlib.sha1(row["origin_url"].encode("utf-8")).hexdigest() != raw:
            raise RuntimeError("snapshot origin identity invalid")
        existing = by_id.get(raw)
        if existing is not None and (existing["origin_url"] != row["origin_url"]
                                     or existing["origin_node_id"] != row["origin_node_id"]):
            raise RuntimeError("origin differs between paths")
        previous = direct_nodes.setdefault(raw, (row["origin_node_id"], row["origin_url"]))
        if previous != (row["origin_node_id"], row["origin_url"]):
            raise RuntimeError("snapshot origin identity conflict")
        direct_seeds[raw].add(row["seed_directory_id"])
    if len(direct_nodes) != 97 or len(set(direct_nodes) - set(by_id)) != 94:
        raise RuntimeError("snapshot origin coverage changed")
    rows = []
    for raw in sorted(set(by_id) | set(direct_nodes),
                      key=lambda key: (by_id.get(key) or
                                       {"origin_url": direct_nodes[key][1]})["origin_url"]):
        rank = by_id.get(raw)
        node, url = ((rank["origin_node_id"], rank["origin_url"]) if rank else direct_nodes[raw])
        rows.append({
            "origin_id": raw, "origin_node_id": node, "origin_url": url,
            "origin_host": urlparse(url).netloc.lower(),
            "origin_kind_hint": origin_kind(url),
            "has_revision_path": bool(rank),
            "has_release_snapshot_path": raw in direct_nodes,
            "release_snapshot_seed_count": len(direct_seeds[raw]),
            "first_candidate_revision_id": rank["candidate_revision_id"] if rank else "",
            "first_candidate_revision_swhid": rank["candidate_revision_swhid"] if rank else "",
            "first_candidate_revision_date": rank["revision_date"] or "" if rank else "",
            "first_candidate_ordering_basis": rank["ordering_basis"] or "" if rank else "",
            "first_candidate_package_version": rank["package_version"] or "" if rank else "",
            "first_candidate_selection_status": rank["selection_status"] if rank else
            "release_snapshot_version_pending",
            "download_status": "not_started",
        })
    if len(rows) != 10201 or len({row["origin_url"] for row in rows}) != len(rows):
        raise RuntimeError("download list identity or count changed")
    return rows


def write_csv(path, rows):
    temporary = path.with_suffix(".partial.csv")
    with temporary.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)
    os.replace(str(temporary), str(path))
    with path.open(newline="", encoding="utf-8") as handle:
        readback = list(csv.DictReader(handle))
    if len(readback) != len(rows) or [row["origin_id"] for row in readback] != [
            row["origin_id"] for row in rows]:
        raise RuntimeError("download list CSV readback mismatch")


def put_verified(s3, local, key):
    data = local.read_bytes()
    checksum = hashlib.sha256(data).hexdigest()
    s3.put_object(Bucket=BUCKET, Key=key, Body=data,
                  ExpectedBucketOwner=BUCKET_OWNER, ServerSideEncryption="AES256",
                  Metadata={"sha256": checksum})
    head = s3.head_object(Bucket=BUCKET, Key=key, ExpectedBucketOwner=BUCKET_OWNER)
    if head["ContentLength"] != len(data) or head.get("Metadata", {}).get("sha256") != checksum:
        raise RuntimeError("S3 object verification failed: " + key)
    return {"key": key, "bytes": len(data), "sha256": checksum}


def copy_revision_evidence(s3, destination):
    source_key = ("new-account-588301174757/route-a/revision-origin-v1/full/"
                  "results/seed_origin/part-00000.parquet")
    local = ROOT / "full/results/seed_origin/part-00000.parquet"
    checksum = digest(local)
    size = local.stat().st_size
    source = s3.head_object(Bucket=BUCKET, Key=source_key,
                            ExpectedBucketOwner=BUCKET_OWNER)
    if source["ContentLength"] != size:
        raise RuntimeError("revision evidence source size changed")
    try:
        target = s3.head_object(Bucket=BUCKET, Key=destination,
                                ExpectedBucketOwner=BUCKET_OWNER)
    except ClientError as error:
        if error.response.get("Error", {}).get("Code") not in ("404", "NoSuchKey", "NotFound"):
            raise
    else:
        if target["ContentLength"] != size or target.get("Metadata", {}).get("sha256") != checksum:
            raise RuntimeError("existing clean revision copy differs")
        return {"key": destination, "bytes": size, "sha256": checksum}
    s3.copy_object(Bucket=BUCKET, Key=destination,
                   CopySource={"Bucket": BUCKET, "Key": source_key},
                   ExpectedBucketOwner=BUCKET_OWNER,
                   ExpectedSourceBucketOwner=BUCKET_OWNER,
                   ServerSideEncryption="AES256", MetadataDirective="REPLACE",
                   Metadata={"sha256": checksum})
    target = s3.head_object(Bucket=BUCKET, Key=destination,
                            ExpectedBucketOwner=BUCKET_OWNER)
    if target["ContentLength"] != size or target.get("Metadata", {}).get("sha256") != checksum:
        raise RuntimeError("clean revision evidence copy verification failed")
    return {"key": destination, "bytes": size, "sha256": checksum}


def query(athena, sql, label, output):
    sql_hash = hashlib.sha256(sql.encode("utf-8")).hexdigest()
    state_path = output / (label + ".json")
    state = json.loads(state_path.read_text(encoding="utf-8")) if state_path.exists() else {}
    if state and state["sql_sha256"] != sql_hash:
        raise RuntimeError("Athena checkpoint query differs: " + label)
    if not state:
        response = athena.start_query_execution(
            QueryString=sql, QueryExecutionContext={"Database": DATABASE},
            WorkGroup=WORKGROUP, ClientRequestToken=sql_hash)
        state = {"query_id": response["QueryExecutionId"], "sql_sha256": sql_hash}
        save_json(state_path, state)
    while True:
        execution = athena.get_query_execution(QueryExecutionId=state["query_id"])["QueryExecution"]
        status = execution["Status"]["State"]
        if status in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(2)
    if status != "SUCCEEDED":
        state.update(status=status, reason=execution["Status"].get("StateChangeReason"))
        save_json(state_path, state)
        raise RuntimeError("Athena {} failed: {}".format(label, state["reason"]))
    state.update(status=status, scanned_bytes=int(execution.get("Statistics", {}).get(
        "DataScannedInBytes", 0)))
    save_json(state_path, state)
    print(json.dumps({"event": "query_complete", "label": label,
                      "scanned_bytes": state["scanned_bytes"]}), flush=True)
    return state


def ddl_sql(token, prefix):
    revision_table = "p4_seed_origin_revision_clean_" + token
    direct_table = "p4_seed_origin_direct_" + token
    origin_view = "p4_seed_origin_unified_" + token
    file_view = "p4_file_origin_unified_" + token
    revision_ddl = (
        "CREATE EXTERNAL TABLE IF NOT EXISTS {db}.{table} "
        "(seed_directory_id string,seed_node_id bigint,root_directory_node_id bigint,"
        "candidate_revision_node_id bigint,origin_node_id bigint,origin_id string,"
        "origin_url string,status string) STORED AS PARQUET "
        "LOCATION 's3://{bucket}/{prefix}revision_seed_origin/'"
    ).format(db=DATABASE, table=revision_table, bucket=BUCKET, prefix=prefix)
    direct_ddl = (
        "CREATE EXTERNAL TABLE IF NOT EXISTS {db}.{table} "
        "(seed_directory_id string,seed_node_id bigint,anchor_directory_node_id bigint,"
        "origin_node_id bigint,origin_id string,origin_url string,directory_hops int,"
        "mapping_basis string) STORED AS PARQUET "
        "LOCATION 's3://{bucket}/{prefix}direct_seed_origin/'"
    ).format(db=DATABASE, table=direct_table, bucket=BUCKET, prefix=prefix)
    origin_sql = (
        "CREATE OR REPLACE VIEW {db}.{view} AS "
        "SELECT seed_directory_id,seed_node_id,root_directory_node_id,"
        "candidate_revision_node_id,CAST(NULL AS bigint) anchor_directory_node_id,"
        "origin_node_id,origin_id,origin_url,CAST(NULL AS integer) directory_hops,"
        "'directory_revision' mapping_basis,status "
        "FROM {db}.{revision} "
        "UNION ALL "
        "SELECT seed_directory_id,seed_node_id,CAST(NULL AS bigint) root_directory_node_id,"
        "CAST(NULL AS bigint) candidate_revision_node_id,anchor_directory_node_id,"
        "origin_node_id,origin_id,origin_url,directory_hops,mapping_basis,"
        "'mapped' status FROM {db}.{direct}"
    ).format(db=DATABASE, view=origin_view, revision=revision_table,
             direct=direct_table)
    union_files = " UNION ALL ".join(
        "SELECT * FROM {db}.p4_file_seed_{start}_{end}_{old}".format(
            db=DATABASE, start=start, end=end, old=OLD_TOKEN)
        for start, end in ((0, 63), (64, 127)))
    file_sql = (
        "CREATE OR REPLACE VIEW {db}.{view} AS SELECT f.*,e.root_directory_node_id,"
        "e.candidate_revision_node_id,e.anchor_directory_node_id,e.origin_node_id,"
        "e.origin_id,e.origin_url,e.directory_hops,e.mapping_basis,e.status "
        "FROM ({files}) f LEFT JOIN {db}.{origin_view} e "
        "ON f.directory_id=e.seed_directory_id"
    ).format(db=DATABASE, view=file_view, files=union_files, origin_view=origin_view)
    return revision_table, direct_table, origin_view, file_view, (
        revision_ddl, direct_ddl, origin_sql, file_sql)


def download_readback(s3, key, destination, expected_sha):
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_suffix(".partial.csv")
    response = s3.get_object(Bucket=BUCKET, Key=key, ExpectedBucketOwner=BUCKET_OWNER)
    with response["Body"] as body, temporary.open("wb") as handle:
        while True:
            chunk = body.read(1024 * 1024)
            if not chunk:
                break
            handle.write(chunk)
    if digest(temporary) != expected_sha:
        raise RuntimeError("local S3 readback checksum mismatch")
    os.replace(str(temporary), str(destination))


def main():
    started = time.monotonic()
    identity = inputs()
    token = hashlib.sha256(json.dumps(identity, sort_keys=True).encode("utf-8")).hexdigest()[:12]
    prefix = "new-account-588301174757/route-a/unified-origin-v1/{}/".format(token)
    OUTPUT.mkdir(parents=True, exist_ok=True)
    identity_path = OUTPUT / "identity.json"
    if identity_path.exists() and json.loads(identity_path.read_text(encoding="utf-8")) != identity:
        raise RuntimeError("unified output belongs to different inputs")
    save_json(identity_path, identity)
    rows = manifest_rows()
    manifest = OUTPUT / "origin_download_list.csv"
    write_csv(manifest, rows)
    direct_file = ROOT / "directory-snapshot-audit-v1/seed_origin.parquet"
    session = private_session()
    if session.client("sts").get_caller_identity()["Account"] != "588301174757":
        raise RuntimeError("unexpected AWS account")
    s3 = session.client("s3")
    objects = [copy_revision_evidence(
        s3, prefix + "revision_seed_origin/part-00000.parquet"),
        put_verified(s3, direct_file, prefix + "direct_seed_origin/part-00000.parquet"),
        put_verified(s3, manifest, prefix + "origin_download_list.csv")]
    revision_table, direct_table, origin_view, file_view, sqls = ddl_sql(token, prefix)
    athena = session.client("athena")
    for name, sql in zip(("revision_clean_table", "direct_table",
                          "origin_view_clean", "file_view"), sqls):
        (OUTPUT / (name + ".sql")).write_text(sql + ";\n", encoding="utf-8")
        query(athena, sql, name, OUTPUT)
    for name in (revision_table, direct_table, origin_view, file_view):
        session.client("glue").get_table(DatabaseName=DATABASE, Name=name)
    count_query = query(athena, "SELECT COUNT(*) FROM {}.{}".format(
        DATABASE, direct_table), "direct_count", OUTPUT)
    count_rows = athena.get_query_results(QueryExecutionId=count_query["query_id"])
    count = int(count_rows["ResultSet"]["Rows"][1]["Data"][0]["VarCharValue"])
    if count != 966:
        raise RuntimeError("Athena direct table count differs from local evidence")
    sample_seed = pq.read_table(
        ROOT / "nonrevision-origin-v1/seed_origin.parquet",
        columns=["seed_directory_id"]).column("seed_directory_id")[0].as_py()
    sample_sql = (
        "SELECT seed_directory_id,origin_url,mapping_basis "
        "FROM {}.{} WHERE seed_directory_id='{}' "
        "AND mapping_basis='directory_release_or_snapshot' LIMIT 3"
    ).format(DATABASE, origin_view, sample_seed)
    sample_query = query(athena, sample_sql, "origin_view_sample_clean", OUTPUT)
    sample_results = athena.get_query_results(QueryExecutionId=sample_query["query_id"])
    sample_rows = sample_results["ResultSet"]["Rows"][1:]
    if not sample_rows or any(
            row["Data"][2].get("VarCharValue") != "directory_release_or_snapshot"
            for row in sample_rows):
        raise RuntimeError("unified origin view sample did not return direct evidence")
    explain = query(athena, "EXPLAIN SELECT origin_id, origin_url, mapping_basis "
                    "FROM {}.{} LIMIT 1".format(DATABASE, file_view),
                    "file_view_explain", OUTPUT)
    file_sql = (
        "SELECT directory_id,name,content_id,origin_url,mapping_basis "
        "FROM {}.{} WHERE directory_id='{}' "
        "AND mapping_basis='directory_release_or_snapshot' LIMIT 1"
    ).format(DATABASE, file_view, sample_seed)
    file_query = query(athena, file_sql, "file_view_sample_clean", OUTPUT)
    file_results = athena.get_query_results(QueryExecutionId=file_query["query_id"])
    file_rows = file_results["ResultSet"]["Rows"][1:]
    if not file_rows:
        raise RuntimeError("unified file view sample is empty")
    cells = [cell.get("VarCharValue") for cell in file_rows[0]["Data"]]
    if (cells[0] != sample_seed or not cells[1].endswith(".p4")
            or len(cells[2]) != 40 or not cells[3]
            or cells[4] != "directory_release_or_snapshot"):
        raise RuntimeError("unified file view sample columns are invalid")
    download_readback(s3, prefix + "origin_download_list.csv", LOCAL_DOWNLOAD,
                      objects[2]["sha256"])
    summary = {
        "status": "passed", "graph_release": "2026-06-04",
        "origins": len(rows), "revision_origins": sum(row["has_revision_path"] for row in rows),
        "release_snapshot_origins": sum(row["has_release_snapshot_path"] for row in rows),
        "release_snapshot_relations": 966,
        "revision_table": DATABASE + "." + revision_table,
        "direct_table": DATABASE + "." + direct_table,
        "origin_view": DATABASE + "." + origin_view,
        "file_view": DATABASE + "." + file_view,
        "source_candidate_file_rows": 13465757,
        "download_list_local": str(LOCAL_DOWNLOAD.resolve()),
        "download_list_sha256": objects[2]["sha256"],
        "s3_prefix": "s3://" + BUCKET + "/" + prefix,
        "athena_scanned_bytes": (count_query["scanned_bytes"]
                                 + sample_query["scanned_bytes"]
                                 + explain["scanned_bytes"]
                                 + file_query["scanned_bytes"]),
        "elapsed_seconds": round(time.monotonic() - started, 2),
        "completed_at_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "identity": identity,
    }
    save_json(OUTPUT / "summary.json", summary)
    objects.append(put_verified(s3, OUTPUT / "summary.json", prefix + "summary.json"))
    marker = {"status": "passed", "objects": objects,
              "download_list_sha256": objects[2]["sha256"]}
    save_json(OUTPUT / "complete.json", marker)
    put_verified(s3, OUTPUT / "complete.json", prefix + "complete.json")
    response = s3.get_object(Bucket=BUCKET, Key=prefix + "complete.json",
                             ExpectedBucketOwner=BUCKET_OWNER)
    with response["Body"] as body:
        if body.read() != (OUTPUT / "complete.json").read_bytes():
            raise RuntimeError("S3 completion marker readback mismatch")
    print(json.dumps(summary, sort_keys=True), flush=True)


if __name__ == "__main__":
    main()
