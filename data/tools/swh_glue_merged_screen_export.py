#!/usr/bin/env python3
"""Merge both content screens while retaining every original SWH hit."""

import datetime as dt
import json
import sys


EXPECTED_CONTENT_IDS = 26826
EXPECTED_SOURCE_ROWS = 14060834
EXPECTED_REVIEW_ROWS = 2537870
EXPECTED_SHARDS = 128
OUTPUT_URI = "s3://swh2p4/file-screen/swh-p4-20260604/full-128-20261020/filtered_v2"
REVIEW_CATEGORIES = ("p4_16_candidate", "p4_14_likely", "non_source", "unresolved")


def merge(spark, source_uri, annotation_uri, output_uri,
          expected_ids=EXPECTED_CONTENT_IDS, expected_rows=EXPECTED_SOURCE_ROWS,
          expected_review_rows=EXPECTED_REVIEW_ROWS, expected_shards=EXPECTED_SHARDS):
    from pyspark.sql import functions as F
    from pyspark.sql import types as T

    schema = T.StructType([
        T.StructField("content_id", T.StringType()),
        T.StructField("review_rule_version", T.StringType()),
        T.StructField("review_version_status", T.StringType()),
        T.StructField("review_disposition", T.StringType()),
        T.StructField("review_evidence_p4_16", T.ArrayType(T.StringType())),
        T.StructField("review_evidence_p4_14", T.ArrayType(T.StringType())),
        T.StructField("review_reasons", T.ArrayType(T.StringType())),
        T.StructField("review_encoding", T.StringType()),
        T.StructField("review_fetch_status", T.StringType()),
        T.StructField("review_sha1_git_verified", T.BooleanType()),
        T.StructField("review_raw_bytes", T.LongType()),
    ])
    annotations = spark.read.schema(schema).json(annotation_uri).cache()
    annotation_count = annotations.count()
    distinct_ids = annotations.select("content_id").distinct().count()
    if annotation_count != expected_ids or distinct_ids != expected_ids:
        raise RuntimeError("review annotations incomplete or duplicated: {} / {}".format(
            annotation_count, distinct_ids))
    if annotations.where(
        F.col("content_id").isNull() |
        (F.col("review_rule_version") != "p4-review-screen-v2") |
        ~F.col("review_disposition").isin(*REVIEW_CATEGORIES) |
        ((F.col("review_disposition") != "unresolved") &
         ~F.coalesce(F.col("review_sha1_git_verified"), F.lit(False)))
    ).limit(1).count():
        raise RuntimeError("invalid review annotation")

    source = spark.read.parquet(source_uri)
    source_count = source.count()
    review = source.where(F.col("screen_disposition") == "needs_review")
    review_count = review.count()
    shards = source.select("source_shard").distinct().count()
    if (source_count != expected_rows or review_count != expected_review_rows
            or shards != expected_shards):
        raise RuntimeError("original hit counts changed: {} / {} / {}".format(
            source_count, review_count, shards))
    review_ids = review.select("content_id").distinct()
    if review_ids.join(annotations.select("content_id"), "content_id", "left_anti").limit(1).count():
        raise RuntimeError("review hit is missing its second-pass annotation")
    if annotations.select("content_id").join(review_ids, "content_id", "left_anti").limit(1).count():
        raise RuntimeError("second-pass annotation was not in the original review set")

    joined = source.join(F.broadcast(annotations), "content_id", "left")
    old_review = F.col("screen_disposition") == "needs_review"
    verified = F.col("fetch_status").isin("fetched", "cached") & F.coalesce(
        F.col("sha1_git_verified"), F.lit(False))
    disposition = (
        F.when(old_review & (F.col("review_disposition") == "p4_16_candidate"),
               "p4_16_candidate")
         .when(old_review & (F.col("review_disposition") == "p4_14_likely"),
               "excluded_p4_14_likely")
         .when(old_review & (F.col("review_disposition") == "non_source"),
               "excluded_non_source")
         .when(old_review & (F.col("review_disposition") == "unresolved"),
               "needs_review")
         .when(verified & (F.col("version_status") == "p4_16_likely"),
               "p4_16_candidate")
         .when(F.col("screen_disposition").isin("excluded_p4_14", "excluded_non_source"),
               F.col("screen_disposition"))
    )
    joined = joined.withColumn("discovery_disposition", disposition)
    if joined.where(F.col("discovery_disposition").isNull()).limit(1).count():
        raise RuntimeError("an original hit has no discovery disposition")
    joined.write.mode("overwrite").parquet(output_uri + "/all_annotated_hits")
    saved = spark.read.parquet(output_uri + "/all_annotated_hits")
    counts = {row.discovery_disposition: row["count"] for row in saved.groupBy(
        "discovery_disposition").count().collect()}
    if sum(counts.values()) != expected_rows:
        raise RuntimeError("merged row count differs from original source")

    groups = (
        ("candidate_csv", saved.where(F.col("discovery_disposition") == "p4_16_candidate")),
        ("review_csv", saved.where(F.col("discovery_disposition") == "needs_review")),
        ("excluded_csv", saved.where(F.col("discovery_disposition").startswith("excluded_"))),
    )
    for name, frame in groups:
        for column in ("review_evidence_p4_16", "review_evidence_p4_14", "review_reasons"):
            frame = frame.withColumn(column, F.concat_ws("|", F.col(column)))
        frame.repartition(expected_shards, "source_shard").write.mode("overwrite").option(
            "header", True).option("escape", '"').partitionBy("source_shard").csv(
                output_uri + "/" + name)
        print(json.dumps({"event": "merged_csv_written", "name": name}), flush=True)

    metrics = {
        "completed_at_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "source_rows": source_count, "source_csv_count": shards,
        "original_review_rows": review_count, "review_content_ids": annotation_count,
        "disposition_counts": counts, "output_uri": output_uri,
        "discovery_rule": "verified P4_16 syntax evidence; training quality is a separate field",
        "p4_16_repository_confirmed": False,
    }
    spark.createDataFrame([(json.dumps(metrics, sort_keys=True),)], ["json"]).coalesce(
        1).write.mode("overwrite").text(output_uri + "/metrics")
    print(json.dumps(dict(event="merged_screen_completed", **metrics), sort_keys=True), flush=True)
    return metrics


def main():
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.utils import getResolvedOptions
    from pyspark.context import SparkContext

    args = getResolvedOptions(sys.argv, ["JOB_NAME", "SOURCE_URI", "ANNOTATION_URI", "OUTPUT_URI"])
    if args["OUTPUT_URI"] != OUTPUT_URI:
        raise ValueError("merged export output must use its dedicated prefix")
    context = GlueContext(SparkContext.getOrCreate())
    job = Job(context)
    job.init(args["JOB_NAME"], args)
    merge(context.spark_session, args["SOURCE_URI"], args["ANNOTATION_URI"], args["OUTPUT_URI"])
    job.commit()


if __name__ == "__main__":
    main()
