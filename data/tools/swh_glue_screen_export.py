#!/usr/bin/env python3
"""Join unique-content annotations to every original P4 hit and export CSVs."""

import datetime as dt
import json
import sys
import threading
import time


class Heartbeat:
    def __init__(self, stage):
        self.stage = stage
        self.stopped = threading.Event()
        self.thread = None
        self.started = None

    def __enter__(self):
        self.started = time.monotonic()
        self.thread = threading.Thread(target=self.report, daemon=True)
        self.thread.start()
        print(json.dumps({"event": "stage_started", "stage": self.stage}), flush=True)
        return self

    def report(self):
        while not self.stopped.wait(30):
            print(json.dumps({"event": "stage_heartbeat", "stage": self.stage,
                              "elapsed_seconds": round(time.monotonic() - self.started)}), flush=True)

    def __exit__(self, error_type, error, trace):
        self.stopped.set()
        self.thread.join(timeout=2)
        print(json.dumps({"event": "stage_finished" if error is None else "stage_failed",
                          "stage": self.stage,
                          "elapsed_seconds": round(time.monotonic() - self.started),
                          "error": str(error)[:500] if error else None}), flush=True)
        return False


def classify_rows(frame):
    from pyspark.sql import functions as F

    fetched = F.col("fetch_status").isin("fetched", "cached") & F.coalesce(
        F.col("sha1_git_verified"), F.lit(False))
    disposition = (F.when(~fetched, "needs_review")
                   .when(F.col("version_status").isin("unknown", "uninspected", "mixed_or_conflicted"),
                         "needs_review")
                   .when(F.col("version_status") == "p4_14_likely", "excluded_p4_14")
                   .when(F.col("version_status") == "non_source", "excluded_non_source")
                   .when((F.col("version_status") == "p4_16_likely") &
                         (F.col("quality_status") == "pass"), "selected")
                   .when(F.col("quality_status") == "excluded", "excluded_quality")
                   .otherwise("needs_review"))
    return frame.withColumn("screen_disposition", disposition)


def export(spark, source_uri, annotation_uri, output_uri, expected_ids=143211,
           expected_rows=14060834, expected_shards=128):
    from pyspark.sql import functions as F
    from pyspark.sql import types as T

    schema = T.StructType([
        T.StructField("content_id", T.StringType()),
        T.StructField("sha1", T.StringType()),
        T.StructField("rule_version", T.StringType()),
        T.StructField("fetch_status", T.StringType()),
        T.StructField("sha1_git_verified", T.BooleanType()),
        T.StructField("version_status", T.StringType()),
        T.StructField("quality_status", T.StringType()),
        T.StructField("quality_flags", T.ArrayType(T.StringType())),
        T.StructField("positive_evidence", T.ArrayType(T.StringType())),
        T.StructField("negative_evidence", T.ArrayType(T.StringType())),
        T.StructField("raw_bytes", T.LongType()),
        T.StructField("line_count", T.LongType()),
        T.StructField("average_line_length", T.DoubleType()),
        T.StructField("longest_line_length", T.LongType()),
        T.StructField("letter_ratio", T.DoubleType()),
    ])
    annotations = spark.read.schema(schema).json(annotation_uri).cache()
    with Heartbeat("validate_annotations"):
        annotation_count = annotations.count()
        distinct_ids = annotations.select("content_id").distinct().count()
    if annotation_count != expected_ids or distinct_ids != expected_ids:
        raise RuntimeError("annotations incomplete or duplicated: {} rows / {} IDs".format(
            annotation_count, distinct_ids))
    invalid = annotations.where(F.col("content_id").isNull() |
                                (F.col("rule_version") != "p4-content-screen-v1") |
                                F.col("rule_version").isNull()).limit(1).count()
    if invalid:
        raise RuntimeError("invalid annotation identity or rule version")
    annotations = classify_rows(annotations)
    for column in ("quality_flags", "positive_evidence", "negative_evidence"):
        annotations = annotations.withColumn(column, F.concat_ws("|", F.col(column)))
    source = spark.read.parquet(source_uri)
    with Heartbeat("validate_source_rows"):
        source_count = source.count()
        shards = source.select("source_shard").distinct().count()
    if source_count != expected_rows or shards != expected_shards:
        raise RuntimeError("source rows/shards changed: {} rows / {} shards".format(source_count, shards))
    missing = source.select("content_id").distinct().join(
        annotations.select("content_id"), "content_id", "left_anti").limit(1).count()
    if missing:
        raise RuntimeError("source content IDs missing from annotations")
    joined = source.join(F.broadcast(annotations), "content_id", "left")
    joined = joined.withColumn("source_csv_index", F.col("source_shard"))
    with Heartbeat("join_and_write_all_hits"):
        joined.write.mode("overwrite").parquet(output_uri + "/all_annotated_hits")
    saved = spark.read.parquet(output_uri + "/all_annotated_hits")
    counts = {row.screen_disposition: row["count"] for row in saved.groupBy(
        "screen_disposition").count().collect()}
    if sum(counts.values()) != expected_rows:
        raise RuntimeError("joined row count differs from source")
    selected = saved.where(F.col("screen_disposition") == "selected")
    review = saved.where(F.col("screen_disposition") == "needs_review")
    excluded = saved.where(F.col("screen_disposition").startswith("excluded_"))
    for name, frame in (("selected_csv", selected), ("review_csv", review),
                        ("excluded_csv", excluded)):
        print(json.dumps({"event": "csv_export_started", "name": name}), flush=True)
        with Heartbeat(name):
            frame.repartition(expected_shards, "source_shard").write.mode("overwrite").option(
                "header", True).option("escape", '"').partitionBy("source_shard").csv(output_uri + "/" + name)
    annotations.coalesce(1).write.mode("overwrite").option("header", True).csv(
        output_uri + "/content_annotations_csv")
    metrics = {"completed_at_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
               "annotation_count": annotation_count, "source_rows": source_count,
               "source_csv_count": shards, "disposition_counts": counts,
               "output_uri": output_uri,
               "selection_rule": "p4_16_likely and quality_status=pass with verified bytes",
               "p4_16_repository_confirmed": False}
    spark.createDataFrame([(json.dumps(metrics, sort_keys=True),)], ["json"]).coalesce(
        1).write.mode("overwrite").text(output_uri + "/metrics")
    print(json.dumps(dict(event="screen_export_completed", **metrics), sort_keys=True), flush=True)
    return metrics


def main():
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.utils import getResolvedOptions
    from pyspark.context import SparkContext

    args = getResolvedOptions(sys.argv, ["JOB_NAME", "SOURCE_URI", "ANNOTATION_URI", "OUTPUT_URI"])
    allowed = "s3://swh2p4/file-screen/swh-p4-20260604/full-128-20261020/filtered"
    if args["OUTPUT_URI"] != allowed:
        raise ValueError("export output must be the dedicated full-screen prefix")
    context = GlueContext(SparkContext.getOrCreate())
    job = Job(context)
    job.init(args["JOB_NAME"], args)
    export(context.spark_session, args["SOURCE_URI"], args["ANNOTATION_URI"], args["OUTPUT_URI"])
    job.commit()


if __name__ == "__main__":
    main()
