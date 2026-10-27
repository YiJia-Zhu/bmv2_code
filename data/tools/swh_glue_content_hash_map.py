#!/usr/bin/env python3
"""Map candidate Git blob IDs to raw SHA-1 keys in the SWH content table."""

import datetime as dt
import json
import re
import sys
import threading
import time


INPUT_URI = ("s3://swh2p4/preprocess/swh-p4-20260604/dedup-v1/runs/"
             "full-128-20261019-v1/unique_content_ids")
CONTENT_ROOT = "s3://softwareheritage/graph/2026-06-04/orc/content/"
OUTPUT_ROOT = "s3://swh2p4/preprocess/swh-p4-20260604/content-hash-map-v1/runs/"
RUN_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]*$")

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
        return self

    def report(self):
        while not self.stopped.wait(30):
            print(json.dumps({"event": "stage_heartbeat", "stage": self.stage,
                              "elapsed_seconds": round(time.monotonic() - self.started, 1),
                              "at_utc": dt.datetime.now(dt.timezone.utc).isoformat()}),
                  flush=True)

    def __exit__(self, error_type, error, trace):
        self.stopped.set()
        self.thread.join(timeout=2)
        print(json.dumps({"event": "stage_finished" if error is None else "stage_failed",
                          "stage": self.stage,
                          "elapsed_seconds": round(time.monotonic() - self.started, 1),
                          "error": str(error) if error else None}), flush=True)
        return False


def argument(argv, name, default=None):
    flag = "--" + name
    for index, value in enumerate(argv):
        if value == flag:
            return argv[index + 1]
        if value.startswith(flag + "="):
            return value[len(flag) + 1:]
    return default


def validate(input_uri, content_uri, output_uri, run_id, confirm):
    if input_uri.rstrip("/") != INPUT_URI:
        raise ValueError("unexpected candidate input")
    if not content_uri.startswith(CONTENT_ROOT) or not (
        content_uri == CONTENT_ROOT or content_uri.endswith(".orc")
    ):
        raise ValueError("content URI must be the release root or one ORC file")
    if output_uri.rstrip("/") != OUTPUT_ROOT + run_id:
        raise ValueError("unexpected output URI")
    if not RUN_RE.fullmatch(run_id):
        raise ValueError("invalid run ID")
    if confirm != "true":
        raise ValueError("--CONFIRM_SCAN=true is required")


def map_hashes(spark, input_uri, content_uri, output_uri, expected_count):
    from pyspark.sql import functions as F

    start = time.monotonic()
    candidates = spark.read.parquet(input_uri).select(
        F.lower("content_id").alias("content_id")
    ).distinct().cache()
    with Heartbeat("load_candidates"):
        candidate_count = candidates.count()
    if candidate_count != expected_count:
        raise RuntimeError("expected {} candidate IDs, found {}".format(
            expected_count, candidate_count))
    print(json.dumps({"event": "candidates_loaded", "at_utc": dt.datetime.now(
        dt.timezone.utc).isoformat(), "count": candidate_count}), flush=True)

    content = spark.read.orc(content_uri).select(
        F.lower("sha1_git").alias("content_id"),
        F.lower("sha1").alias("sha1"),
    ).where(F.col("content_id").rlike("^[0-9a-f]{40}$") &
            F.col("sha1").rlike("^[0-9a-f]{40}$"))
    matches = content.join(F.broadcast(candidates), "content_id", "inner")
    matches = matches.dropDuplicates(["content_id", "sha1"])
    with Heartbeat("scan_content_and_write_matches"):
        matches.write.mode("errorifexists").parquet(output_uri + "/matches")
    saved = spark.read.parquet(output_uri + "/matches")
    with Heartbeat("count_matches"):
        match_count = saved.count()
        distinct_count = saved.select("content_id").distinct().count()
    metrics = {
        "schema_version": 1,
        "at_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "input_uri": input_uri,
        "content_uri": content_uri,
        "candidate_count": candidate_count,
        "matching_rows": match_count,
        "matching_content_ids": distinct_count,
        "elapsed_seconds": round(time.monotonic() - start, 1),
    }
    spark.createDataFrame([(json.dumps(metrics, sort_keys=True),)], ["json"]).coalesce(
        1).write.mode("errorifexists").text(output_uri + "/metrics")
    print(json.dumps(dict(event="hash_map_completed", **metrics), sort_keys=True), flush=True)
    return metrics


def main(argv=None):
    argv = sys.argv if argv is None else argv
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.utils import getResolvedOptions
    from pyspark.context import SparkContext

    args = getResolvedOptions(argv, ["JOB_NAME", "INPUT_URI", "CONTENT_URI",
                                     "OUTPUT_URI", "RUN_ID", "CONFIRM_SCAN"])
    validate(args["INPUT_URI"], args["CONTENT_URI"], args["OUTPUT_URI"],
             args["RUN_ID"], args["CONFIRM_SCAN"])
    glue_context = GlueContext(SparkContext.getOrCreate())
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)
    map_hashes(glue_context.spark_session, args["INPUT_URI"], args["CONTENT_URI"],
               args["OUTPUT_URI"], int(argument(argv, "EXPECTED_COUNT", "143211")))
    job.commit()


if __name__ == "__main__":
    main()
