#!/usr/bin/env python3
"""Build a global direct-parent index from all 128 candidate CSV shards."""

import datetime as dt
import json
import sys


SOURCE_URI = (
    "s3://swh2p4/file-screen/swh-p4-20260604/"
    "full-128-20261020/filtered_v2/candidate_csv/"
)
OUTPUT_URI = (
    "s3://swh2p4/file-screen/swh-p4-20260604/"
    "full-128-20261020/candidate-parent-index-v1/"
)
EXPECTED_SHARDS = 128
EXPECTED_ROWS = 13465757
EXPECTED_DIRECT_PARENTS = 65321


def validate_inventory(input_files, shard_ids):
    if len(input_files) != EXPECTED_SHARDS:
        raise RuntimeError("expected {} candidate CSVs, found {}".format(
            EXPECTED_SHARDS, len(input_files)))
    if set(shard_ids) != set(range(EXPECTED_SHARDS)):
        raise RuntimeError("candidate source_shard inventory is incomplete")


def build_index(spark, source_uri, output_uri):
    from pyspark.sql import functions as F
    from pyspark.storagelevel import StorageLevel

    if source_uri.rstrip("/") != SOURCE_URI.rstrip("/"):
        raise ValueError("unexpected candidate input prefix")
    if output_uri.rstrip("/") != OUTPUT_URI.rstrip("/"):
        raise ValueError("unexpected parent-index output prefix")

    source = (spark.read.option("header", True).option("multiLine", True)
              .option("escape", '"').csv(source_uri))
    required = {"directory_id", "discovery_disposition", "source_shard"}
    if not required.issubset(source.columns):
        raise RuntimeError("candidate CSV is missing required columns: {}".format(
            sorted(required.difference(source.columns))))
    input_files = source.inputFiles()
    parents = source.select("directory_id", "discovery_disposition", "source_shard").persist(
        StorageLevel.MEMORY_AND_DISK)
    row_count = parents.count()
    shard_ids = [int(row[0]) for row in parents.select("source_shard").distinct().collect()]
    validate_inventory(input_files, shard_ids)
    if row_count != EXPECTED_ROWS:
        raise RuntimeError("expected {} candidate rows, found {}".format(
            EXPECTED_ROWS, row_count))
    if parents.where(
            F.col("directory_id").isNull() |
            ~F.col("directory_id").rlike("^[0-9a-f]{40}$") |
            (F.col("discovery_disposition") != "p4_16_candidate") |
            F.col("discovery_disposition").isNull()).limit(1).count():
        raise RuntimeError("candidate rows contain an invalid parent ID or disposition")
    print(json.dumps({"event": "candidate_input_validated", "csv_files": len(input_files),
                      "rows": row_count, "shards": len(shard_ids)}), flush=True)

    index = parents.groupBy("directory_id").agg(
        F.count(F.lit(1)).alias("candidate_hit_count")).persist(StorageLevel.MEMORY_AND_DISK)
    parent_count = index.count()
    if parent_count != EXPECTED_DIRECT_PARENTS:
        raise RuntimeError("expected {} direct parents, found {}".format(
            EXPECTED_DIRECT_PARENTS, parent_count))
    if index.agg(F.sum("candidate_hit_count")).first()[0] != row_count:
        raise RuntimeError("parent hit counts do not add up to the candidate row count")

    csv_uri = output_uri.rstrip("/") + "/parent_directory_csv"
    parquet_uri = output_uri.rstrip("/") + "/parent_directory_parquet"
    index.coalesce(1).write.mode("errorifexists").option("header", True).csv(csv_uri)
    index.coalesce(1).write.mode("errorifexists").parquet(parquet_uri)
    if spark.read.option("header", True).csv(csv_uri).count() != parent_count:
        raise RuntimeError("saved parent CSV row count changed")
    if spark.read.parquet(parquet_uri).count() != parent_count:
        raise RuntimeError("saved parent Parquet row count changed")

    metrics = {
        "completed_at_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "source_uri": source_uri,
        "output_uri": output_uri,
        "candidate_csv_files": len(input_files),
        "candidate_rows": row_count,
        "unique_direct_parent_directories": parent_count,
        "parent_directory_csv_uri": csv_uri,
        "parent_directory_parquet_uri": parquet_uri,
        "repository_mapping_completed": False,
    }
    spark.createDataFrame([(json.dumps(metrics, sort_keys=True),)], ["json"]).coalesce(
        1).write.mode("errorifexists").text(output_uri.rstrip("/") + "/metrics")
    print(json.dumps({"event": "parent_index_completed", **metrics}, sort_keys=True), flush=True)
    index.unpersist()
    parents.unpersist()
    return metrics


def main():
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.utils import getResolvedOptions
    from pyspark.context import SparkContext

    args = getResolvedOptions(sys.argv, ["JOB_NAME", "SOURCE_URI", "OUTPUT_URI"])
    context = GlueContext(SparkContext.getOrCreate())
    job = Job(context)
    job.init(args["JOB_NAME"], args)
    build_index(context.spark_session, args["SOURCE_URI"], args["OUTPUT_URI"])
    job.commit()


if __name__ == "__main__":
    main()
