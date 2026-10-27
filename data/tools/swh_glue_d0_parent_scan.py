#!/usr/bin/env python3
"""Find every direct parent of the existing candidate-directory D0 frontier.

Each public ORC object is committed independently. A later run with the same
configuration skips committed objects and can finish the D1 materialization.
"""

import datetime as dt
import hashlib
import json
import re
import sys
import time


VERSION = "d0-parent-scan-v2"
SOURCE_PREFIX = "graph/2026-06-04/orc/directory_entry/"
EXPECTED_SOURCES = 128
EXPECTED_SEEDS = 65321
SHA1_RE = re.compile(r"^[0-9a-f]{40}$")


def utc_now():
    return dt.datetime.now(dt.timezone.utc).isoformat()


def fingerprint(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True).encode("utf-8")).hexdigest()


def s3_parts(uri):
    if not uri.startswith("s3://") or "/" not in uri[5:]:
        raise ValueError("expected an S3 object URI")
    return uri[5:].split("/", 1)


def emit(event, **fields):
    print(json.dumps(dict(fields, event=event, timestamp_utc=utc_now()), sort_keys=True), flush=True)


def validate_config(config):
    if config.get("version") != VERSION:
        raise ValueError("unexpected scan version")
    expected_id = fingerprint({key: value for key, value in config.items() if key != "config_id"})
    if config.get("config_id") != expected_id:
        raise ValueError("configuration fingerprint mismatch")
    if config.get("output_uri", "").rstrip("/") != (
            "s3://swh2p4/glue-results/swh-p4-20260604/d0-parent-scan-v2"):
        raise ValueError("unexpected output URI")
    if config.get("seed_count") != EXPECTED_SEEDS:
        raise ValueError("unexpected D0 count")
    if config.get("seed_uri", "").rstrip("/") != (
            "s3://swh2p4/file-screen/swh-p4-20260604/"
            "full-128-20261020/candidate-parent-index-v1/parent_directory_parquet"):
        raise ValueError("unexpected D0 input URI")
    objects = config.get("source_objects", [])
    keys = [item.get("key", "") for item in objects]
    if len(objects) != EXPECTED_SOURCES or len(set(keys)) != EXPECTED_SOURCES:
        raise ValueError("expected 128 distinct directory_entry ORC objects")
    if any(not key.startswith(SOURCE_PREFIX) or not key.endswith(".orc") for key in keys):
        raise ValueError("unexpected ORC source key")
    if any(not isinstance(item.get("size"), int) or item["size"] <= 0 or
           not item.get("etag") for item in objects):
        raise ValueError("source object inventory lacks size or ETag")
    if not config.get("seed_object", {}).get("etag"):
        raise ValueError("D0 input ETag is missing")
    return objects


class Store(object):
    def __init__(self, client, root):
        self.client = client
        self.root = root.rstrip("/")

    def read(self, name):
        bucket, key = s3_parts(self.root + "/" + name)
        try:
            response = self.client.get_object(Bucket=bucket, Key=key)
        except self.client.exceptions.NoSuchKey:
            return None
        with response["Body"] as body:
            return json.load(body)

    def put(self, name, value):
        bucket, key = s3_parts(self.root + "/" + name)
        self.client.put_object(Bucket=bucket, Key=key,
                               Body=json.dumps(value, sort_keys=True).encode("utf-8"),
                               ContentType="application/json")

    def completed_parquet(self, uri):
        # Glue's S3 committer does not necessarily create a _SUCCESS object.
        # This is called only after write() returns, or for a committed marker.
        bucket, prefix = s3_parts(uri.rstrip("/") + "/")
        response = self.client.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1000)
        return any(item["Key"].endswith(".parquet") and item["Size"] > 0
                   for item in response.get("Contents", []))


def read_seeds(spark, uri):
    from pyspark.sql import functions as F

    source = spark.read.parquet(uri)
    if "directory_id" not in source.columns:
        raise RuntimeError("D0 index has no directory_id column")
    seeds = source.select(F.col("directory_id").alias("seed_id")).dropDuplicates().cache()
    count = seeds.count()
    if count != EXPECTED_SEEDS:
        raise RuntimeError("D0 index has {} unique directories, expected {}".format(
            count, EXPECTED_SEEDS))
    if seeds.where(F.col("seed_id").isNull() |
                   ~F.col("seed_id").rlike("^[0-9a-f]{40}$")).limit(1).count():
        raise RuntimeError("D0 index contains an invalid directory ID")
    return seeds


def matched_edges(spark, source_uri, seeds):
    from pyspark.sql import functions as F

    entries = spark.read.orc(source_uri)
    required = {"directory_id", "name", "type", "target", "perms"}
    if not required.issubset(entries.columns):
        raise RuntimeError("directory_entry ORC is missing required columns")
    return (entries.where(F.col("type") == "dir")
            .join(F.broadcast(seeds), F.col("target") == F.col("seed_id"), "inner")
            .select(F.col("target").alias("child_id"),
                    F.col("directory_id").alias("parent_id"),
                    F.col("name"), F.col("perms")))


def checkpoint_matches(marker, config_id, source):
    return (marker is not None and marker.get("config_id") == config_id and
            marker.get("source") == source and isinstance(marker.get("edge_rows"), int)
            and marker["edge_rows"] >= 0 and isinstance(marker.get("data_uri"), str))


def scan(spark, store, config, shard_limit):
    from pyspark.sql import functions as F

    sources = validate_config(config)
    if shard_limit < 1 or shard_limit > len(sources):
        raise ValueError("shard limit must be between 1 and 128")
    seed_object = config["seed_object"]
    bucket, key = s3_parts(seed_object["uri"])
    actual = store.client.head_object(Bucket=bucket, Key=key)
    if actual["ETag"] != seed_object["etag"] or actual["ContentLength"] != seed_object["size"]:
        raise RuntimeError("D0 input changed after configuration was recorded")
    seeds = read_seeds(spark, config["seed_uri"])
    started = time.monotonic()
    started_at_utc = utc_now()
    fresh = 0
    completed = 0
    rows_total = 0
    emit("d0_parent_scan_started", config_id=config["config_id"],
         started_at_utc=started_at_utc,
         seed_count=EXPECTED_SEEDS, source_files_total=len(sources), shard_limit=shard_limit)
    store.put("status.json", {"version": VERSION, "config_id": config["config_id"],
                              "phase": "scanning", "started_at_utc": started_at_utc,
                              "updated_at_utc": utc_now(), "source_files_complete": 0,
                              "source_files_total": len(sources), "shard_limit": shard_limit})
    for index, source in enumerate(sources[:shard_limit]):
        base = "batches/shard-{:03d}".format(index)
        marker = store.read(base + "/complete.json")
        resumed = marker is not None
        if marker is not None and not checkpoint_matches(marker, config["config_id"], source):
            raise RuntimeError("checkpoint conflicts with current configuration at shard {}".format(index))
        if marker is not None and (marker["data_uri"] != store.root + "/" + base + "/edges"
                                   or not store.completed_parquet(marker["data_uri"])):
            raise RuntimeError("checkpoint data is missing at shard {}".format(index))
        if marker is None:
            shard_started = time.monotonic()
            data_uri = store.root + "/" + base + "/edges"
            source_uri = "s3://softwareheritage/" + source["key"]
            emit("source_scan_started", shard=index, source_uri=source_uri,
                 source_bytes=source["size"])
            matched_edges(spark, source_uri, seeds).repartition(8).write.mode(
                "overwrite").parquet(data_uri)
            edge_rows = spark.read.parquet(data_uri).count()
            if not store.completed_parquet(data_uri):
                raise RuntimeError("Parquet write did not complete at shard {}".format(index))
            marker = {"config_id": config["config_id"], "source": source,
                      "edge_rows": edge_rows, "data_uri": data_uri,
                      "completed_at_utc": utc_now(),
                      "elapsed_seconds": round(time.monotonic() - shard_started, 1)}
            store.put(base + "/complete.json", marker)
            fresh += 1
        completed += 1
        rows_total += marker["edge_rows"]
        elapsed = time.monotonic() - started
        rate = elapsed / fresh if fresh else None
        remaining = len(sources) - completed
        eta_seconds = round(rate * remaining) if rate is not None else None
        status = {"version": VERSION, "config_id": config["config_id"],
                  "updated_at_utc": utc_now(), "source_files_complete": completed,
                  "source_files_total": len(sources), "shard_limit": shard_limit,
                  "source_bytes_covered": sum(item["size"] for item in sources[:completed]),
                  "raw_parent_edge_rows": rows_total,
                  "started_at_utc": started_at_utc,
                  "elapsed_seconds_this_run": round(elapsed, 1), "eta_seconds": eta_seconds,
                  "phase": "scanning"}
        store.put("status.json", status)
        emit("source_scan_completed", shard=index, resumed=resumed,
             edge_rows=marker["edge_rows"], source_files_complete=completed,
             source_files_total=len(sources), eta_seconds=eta_seconds)
    if shard_limit < len(sources):
        emit("bounded_smoke_completed", source_files_complete=completed,
             source_files_total=len(sources))
        seeds.unpersist()
        return None
    completed = store.read("complete.json")
    if completed is not None:
        if completed.get("config_id") != config["config_id"] or not all(
                store.completed_parquet(completed[key]) for key in
                ("parent_edges_uri", "frontier_d1_uri", "d0_without_observed_parent_uri")):
            raise RuntimeError("D1 completion marker or output is inconsistent")
        emit("d1_already_materialized")
        seeds.unpersist()
        return completed

    store.put("status.json", {"version": VERSION, "config_id": config["config_id"],
                              "phase": "materializing_d1", "updated_at_utc": utc_now(),
                              "source_files_complete": len(sources),
                              "source_files_total": len(sources),
                              "raw_parent_edge_rows": rows_total})
    paths = [store.root + "/batches/shard-{:03d}/edges".format(i)
             for i in range(len(sources))]
    edges = spark.read.parquet(*paths).dropDuplicates(
        ["child_id", "parent_id", "name", "perms"])
    edge_uri = store.root + "/parent_edges"
    edges.write.mode("overwrite").parquet(edge_uri)
    if not store.completed_parquet(edge_uri):
        raise RuntimeError("parent edge output is incomplete")
    saved_edges = spark.read.parquet(edge_uri).cache()
    edge_count = saved_edges.count()
    parents = saved_edges.select(F.col("parent_id").alias("directory_id")).dropDuplicates()
    parent_count = parents.count()
    unseen = parents.join(seeds, parents.directory_id == seeds.seed_id, "leftanti")
    next_count = unseen.count()
    frontier_uri = store.root + "/frontier_d1"
    unseen.write.mode("overwrite").parquet(frontier_uri)
    if not store.completed_parquet(frontier_uri):
        raise RuntimeError("D1 frontier output is incomplete")
    without = seeds.join(saved_edges.select(F.col("child_id").alias("seed_id")).dropDuplicates(),
                         "seed_id", "leftanti").select(F.col("seed_id").alias("directory_id"))
    without_count = without.count()
    without_uri = store.root + "/d0_without_observed_parent"
    without.write.mode("overwrite").parquet(without_uri)
    if not store.completed_parquet(without_uri):
        raise RuntimeError("unobserved-parent output is incomplete")
    summary = {"version": VERSION, "config_id": config["config_id"],
               "completed_at_utc": utc_now(), "seed_count": EXPECTED_SEEDS,
               "source_files_complete": len(sources), "raw_parent_edge_rows": rows_total,
               "unique_parent_edge_rows": edge_count, "unique_parent_directories": parent_count,
               "d1_new_frontier_count": next_count,
               "d0_without_observed_parent_count": without_count,
               "parent_edges_uri": edge_uri, "frontier_d1_uri": frontier_uri,
               "d0_without_observed_parent_uri": without_uri,
               "elapsed_seconds_this_run": round(time.monotonic() - started, 1)}
    store.put("complete.json", summary)
    store.put("status.json", dict(summary, phase="complete"))
    emit("d1_materialized", **summary)
    saved_edges.unpersist()
    seeds.unpersist()
    return summary


def main():
    import boto3
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.utils import getResolvedOptions
    from pyspark.context import SparkContext

    args = getResolvedOptions(sys.argv, ["JOB_NAME", "CONFIG_URI", "SHARD_LIMIT"])
    s3 = boto3.client("s3", region_name="us-east-1")
    bucket, key = s3_parts(args["CONFIG_URI"])
    with s3.get_object(Bucket=bucket, Key=key)["Body"] as body:
        config = json.load(body)
    validate_config(config)
    context = GlueContext(SparkContext.getOrCreate())
    job = Job(context)
    job.init(args["JOB_NAME"], args)
    scan(context.spark_session, Store(s3, config["output_uri"]),
         config, int(args["SHARD_LIMIT"]))
    job.commit()


if __name__ == "__main__":
    main()
