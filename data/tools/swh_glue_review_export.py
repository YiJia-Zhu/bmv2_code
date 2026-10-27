#!/usr/bin/env python3
"""Join second-pass content labels to every original review hit."""

import datetime as dt
import json
import sys


EXPECTED_IDS = 26826
EXPECTED_ROWS = 2537870
OUTPUT_URI = "s3://swh2p4/file-screen/swh-p4-20260604/review-v2-20261020/filtered"
CATEGORIES = ("p4_16_candidate", "p4_14_likely", "non_source", "unresolved")


def export(spark, source_uri, annotation_uri, output_uri,
           expected_ids=EXPECTED_IDS, expected_rows=EXPECTED_ROWS):
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
    count = annotations.count()
    distinct = annotations.select("content_id").distinct().count()
    if count != expected_ids or distinct != expected_ids:
        raise RuntimeError("review annotations incomplete or duplicated: {} / {}".format(count, distinct))
    invalid = annotations.where(
        F.col("content_id").isNull() |
        (F.col("review_rule_version") != "p4-review-screen-v2") |
        ~F.col("review_disposition").isin(*CATEGORIES)
    ).limit(1).count()
    if invalid:
        raise RuntimeError("invalid review annotation")
    source = spark.read.parquet(source_uri).where(F.col("screen_disposition") == "needs_review")
    source_count = source.count()
    if source_count != expected_rows:
        raise RuntimeError("review source row count changed: {}".format(source_count))
    missing = source.select("content_id").distinct().join(
        annotations.select("content_id"), "content_id", "left_anti").limit(1).count()
    extra = annotations.select("content_id").join(
        source.select("content_id").distinct(), "content_id", "left_anti").limit(1).count()
    if missing or extra:
        raise RuntimeError("review content IDs do not match original review rows")
    joined = source.join(F.broadcast(annotations), "content_id", "inner")
    joined.write.mode("overwrite").parquet(output_uri + "/all_review_hits")
    saved = spark.read.parquet(output_uri + "/all_review_hits")
    counts = {row.review_disposition: row["count"] for row in saved.groupBy(
        "review_disposition").count().collect()}
    if sum(counts.values()) != expected_rows:
        raise RuntimeError("review join changed original row count")
    for name in CATEGORIES:
        frame = saved.where(F.col("review_disposition") == name)
        for column in ("review_evidence_p4_16", "review_evidence_p4_14", "review_reasons"):
            frame = frame.withColumn(column, F.concat_ws("|", F.col(column)))
        frame.repartition(128, "source_shard").write.mode("overwrite").option(
            "header", True).option("escape", '"').partitionBy("source_shard").csv(
                output_uri + "/" + name + "_csv")
        print(json.dumps({"event": "review_csv_written", "name": name,
                          "rows": counts.get(name, 0)}), flush=True)
    metrics = {"completed_at_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
               "review_content_ids": count, "original_review_rows": source_count,
               "disposition_counts": counts, "output_uri": output_uri,
               "p4_16_repository_confirmed": False,
               "rule_version": "p4-review-screen-v2"}
    spark.createDataFrame([(json.dumps(metrics, sort_keys=True),)], ["json"]).coalesce(
        1).write.mode("overwrite").text(output_uri + "/metrics")
    print(json.dumps(dict(event="review_export_completed", **metrics), sort_keys=True), flush=True)
    return metrics


def main():
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.utils import getResolvedOptions
    from pyspark.context import SparkContext

    args = getResolvedOptions(sys.argv, ["JOB_NAME", "SOURCE_URI", "ANNOTATION_URI", "OUTPUT_URI"])
    if args["OUTPUT_URI"] != OUTPUT_URI:
        raise ValueError("review export output must use its dedicated prefix")
    context = GlueContext(SparkContext.getOrCreate())
    job = Job(context)
    job.init(args["JOB_NAME"], args)
    export(context.spark_session, args["SOURCE_URI"], args["ANNOTATION_URI"], args["OUTPUT_URI"])
    job.commit()


if __name__ == "__main__":
    main()
