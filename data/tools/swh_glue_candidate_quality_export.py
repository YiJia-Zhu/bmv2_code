#!/usr/bin/env python3
"""Join local quality annotations to all candidate SWH hits on AWS Glue."""

import datetime as dt
import json
import sys


EXPECTED_ROWS = 13465757
EXPECTED_IDS = 122392
EXPECTED_SHARDS = 128
EXPECTED_STATUS_COUNTS = {"pass": 121840, "excluded": 552}
OUTPUT_URI = "s3://swh2p4/file-screen/swh-p4-20260604/full-128-20261020/candidate_quality_glue_v1"
RULE_VERSION = "p4-candidate-quality-v1"


def export(spark, source_uri, annotation_uri, output_uri):
    from pyspark.sql import functions as F
    from pyspark.storagelevel import StorageLevel

    annotations = spark.read.json(annotation_uri).select(
        "content_id", F.col("quality_status").alias("new_quality_status"),
        F.col("quality_flags").alias("new_quality_flags"),
        F.col("quality_rule_version").alias("new_quality_rule_version"))
    if annotations.count() != EXPECTED_IDS or annotations.select("content_id").distinct().count() != EXPECTED_IDS:
        raise RuntimeError("quality annotation count or IDs changed")
    if {row["new_quality_status"]: row["count"] for row in annotations.groupBy(
            "new_quality_status").count().collect()} != EXPECTED_STATUS_COUNTS:
        raise RuntimeError("quality annotation status counts changed")
    if annotations.where(F.col("new_quality_rule_version") != RULE_VERSION).limit(1).count():
        raise RuntimeError("wrong quality rule version")

    source = spark.read.parquet(source_uri).where(
        F.col("discovery_disposition") == "p4_16_candidate")
    if source.count() != EXPECTED_ROWS or source.select("source_shard").distinct().count() != EXPECTED_SHARDS:
        raise RuntimeError("candidate hit count or shard inventory changed")
    joined = source.join(F.broadcast(annotations), "content_id", "inner").persist(
        StorageLevel.MEMORY_AND_DISK)
    if joined.count() != EXPECTED_ROWS or joined.select("content_id").distinct().count() != EXPECTED_IDS:
        raise RuntimeError("quality join lost or duplicated candidate hits")

    hit_counts = {row["new_quality_status"]: row["count"] for row in joined.groupBy(
        "new_quality_status").count().collect()}
    directory_counts = {row["new_quality_status"]: row["directories"] for row in joined.groupBy(
        "new_quality_status").agg(F.countDistinct("directory_id").alias("directories")).collect()}
    all_directories = joined.select("directory_id").distinct().count()
    export_frame = (joined.withColumn("quality_v1_status", F.col("new_quality_status"))
                    .withColumn("quality_v1_flags", F.concat_ws("|", F.col("new_quality_flags")))
                    .withColumn("quality_v1_training_eligible", F.when(
                        F.col("new_quality_status") == "pass", "true").otherwise("false"))
                    .withColumn("quality_v1_rule_version", F.col("new_quality_rule_version"))
                    .drop("new_quality_status", "new_quality_flags", "new_quality_rule_version"))
    for column in ("review_evidence_p4_16", "review_evidence_p4_14", "review_reasons"):
        export_frame = export_frame.withColumn(column, F.concat_ws("|", F.col(column)))
    for status in ("pass", "excluded", "unknown"):
        path = output_uri + "/quality_{}_csv".format(status)
        part = export_frame.where(F.col("quality_v1_status") == status)
        part.repartition(EXPECTED_SHARDS, "source_shard").write.mode("errorifexists").option(
            "header", True).option("escape", '"').option("compression", "gzip").partitionBy(
                "source_shard").csv(path)
        if hit_counts.get(status, 0):
            saved = spark.read.option("header", True).option("escape", '"').option(
                "multiLine", True).csv(path)
            if saved.count() != hit_counts[status]:
                raise RuntimeError("exported CSV count changed for " + status)
        print(json.dumps({"event": "quality_csv_written", "status": status,
                          "rows": hit_counts.get(status, 0), "uri": path}), flush=True)

    metrics = {
        "rule_version": RULE_VERSION,
        "completed_at_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "source_uri": source_uri, "annotation_uri": annotation_uri, "output_uri": output_uri,
        "export_engine": "aws_glue_spark",
        "source_candidate_rows": EXPECTED_ROWS, "unique_content_ids": EXPECTED_IDS,
        "content_status_counts": EXPECTED_STATUS_COUNTS, "hit_status_counts": hit_counts,
        "unique_parent_directories": directory_counts,
        "all_candidate_parent_directories": all_directories,
        "size_500kb_is_diagnostic": True, "duplicate_line_ratio_is_diagnostic": True,
        "p4_16_compiler_confirmed": False, "repository_mapping_completed": False,
    }
    spark.createDataFrame([(json.dumps(metrics, sort_keys=True),)], ["json"]).coalesce(
        1).write.mode("errorifexists").text(output_uri + "/metrics")
    print(json.dumps({"event": "quality_export_completed", **metrics}, sort_keys=True), flush=True)
    joined.unpersist()
    return metrics


def main():
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.utils import getResolvedOptions
    from pyspark.context import SparkContext

    args = getResolvedOptions(sys.argv, ["JOB_NAME", "SOURCE_URI", "ANNOTATION_URI", "OUTPUT_URI"])
    if args["OUTPUT_URI"] != OUTPUT_URI:
        raise ValueError("Glue output must use its dedicated quality prefix")
    context = GlueContext(SparkContext.getOrCreate())
    job = Job(context)
    job.init(args["JOB_NAME"], args)
    export(context.spark_session, args["SOURCE_URI"], args["ANNOTATION_URI"], args["OUTPUT_URI"])
    job.commit()


if __name__ == "__main__":
    main()
