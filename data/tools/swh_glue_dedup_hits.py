#!/usr/bin/env python3
"""Deduplicate Athena P4 hit CSVs without losing their source rows."""

import csv
import datetime as dt
import io
import json
import re
import sys
import threading
import time


INPUT_PREFIX = "s3://swh2p4/athena-results/swh-p4-20260604/p4-directory-hits/"
OUTPUT_PREFIX = "s3://swh2p4/preprocess/swh-p4-20260604/dedup-v1/runs/"
CSV_COLUMNS = ("directory_id", "name", "type", "content_id", "source_path")
SHA1_RE = re.compile(r"^[0-9a-f]{40}$")
SHARD_RE = re.compile(r"^part-(\d{5})-[0-9a-f-]+\.csv$")
RUN_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]*$")


def utc_now():
    return dt.datetime.now(dt.timezone.utc).isoformat()


def log_event(event, **fields):
    print(json.dumps(dict(event=event, at_utc=utc_now(), **fields), sort_keys=True), flush=True)


def parse_csv_file(item):
    """Emit one row per CSV record, with a stable 1-based record number."""
    source_csv, content = item
    filename = source_csv.rsplit("/", 1)[-1]
    match = SHARD_RE.fullmatch(filename)
    if not match:
        raise ValueError("unexpected Athena CSV name: {}".format(source_csv))
    shard = int(match.group(1))
    csv.field_size_limit(10 * 1024 * 1024)
    reader = csv.reader(io.StringIO(content, newline=""), strict=True)
    try:
        header = next(reader)
    except StopIteration:
        raise ValueError("empty Athena CSV: {}".format(source_csv))
    header[0] = header[0].lstrip("\ufeff")
    if tuple(header) != CSV_COLUMNS:
        raise ValueError("unexpected Athena CSV columns in {}: {}".format(source_csv, header))

    for source_row, columns in enumerate(reader, 1):
        if len(columns) != len(CSV_COLUMNS):
            raise ValueError("invalid column count in {} row {}".format(source_csv, source_row))
        directory_id, name, entry_type, content_id, source_path = columns
        directory_id = directory_id.lower()
        content_id = content_id.lower()
        if not (
            SHA1_RE.fullmatch(directory_id)
            and SHA1_RE.fullmatch(content_id)
            and name.lower().endswith(".p4")
            and entry_type == "file"
            and source_path.startswith(
                "s3://softwareheritage/graph/2026-06-04/orc/directory_entry/"
            )
        ):
            raise ValueError("invalid P4 hit in {} row {}".format(source_csv, source_row))
        yield (shard, source_csv, source_row, directory_id, name, entry_type,
               content_id, source_path)


def validate_run(input_glob, output_root, expected_shards):
    if not input_glob.startswith(INPUT_PREFIX) or not input_glob.endswith(".csv"):
        raise ValueError("input must be Athena CSVs under {}".format(INPUT_PREFIX))
    if input_glob != INPUT_PREFIX + "part-*.csv" and "*" in input_glob:
        raise ValueError("only the complete part-*.csv input glob is supported")
    if not output_root.startswith(OUTPUT_PREFIX) or not RUN_ID_RE.fullmatch(
        output_root[len(OUTPUT_PREFIX):].rstrip("/")
    ):
        raise ValueError("output must name one run under {}".format(OUTPUT_PREFIX))
    if expected_shards < 1 or expected_shards > 128:
        raise ValueError("expected_shards must be between 1 and 128")


class StageMonitor:
    def __init__(self, spark, name, interval=30):
        self.tracker = spark.sparkContext.statusTracker()
        self.name = name
        self.interval = interval
        self.stop_event = threading.Event()
        self.thread = None
        self.started_at = None
        self.previous = {}

    def __enter__(self):
        self.started_at = time.monotonic()
        log_event("stage_started", stage=self.name)
        self.thread = threading.Thread(target=self._heartbeat, daemon=True)
        self.thread.start()
        return self

    def _heartbeat(self):
        while not self.stop_event.wait(self.interval):
            try:
                observed_at = time.monotonic()
                active = []
                for stage_id in sorted(self.tracker.getActiveStageIds()):
                    info = self.tracker.getStageInfo(stage_id)
                    if info is None:
                        continue
                    key = (info.stageId, info.currentAttemptId)
                    previous = self.previous.get(key)
                    rate = None
                    eta = None
                    if previous:
                        advanced = info.numCompletedTasks - previous[0]
                        seconds = observed_at - previous[1]
                        if advanced > 0 and seconds > 0:
                            rate = round(advanced / seconds, 3)
                            eta = round((info.numTasks - info.numCompletedTasks) / rate)
                    active.append({
                        "stage_id": info.stageId,
                        "tasks_completed": info.numCompletedTasks,
                        "tasks_total": info.numTasks,
                        "task_rate_per_second": rate,
                        "eta_seconds_current_spark_stage_only": eta,
                    })
                    self.previous[key] = (info.numCompletedTasks, observed_at)
                log_event("stage_progress", stage=self.name,
                          elapsed_seconds=round(observed_at - self.started_at, 1),
                          spark_stages=active)
            except Exception as exc:
                log_event("stage_monitor_error", stage=self.name, error=str(exc))

    def __exit__(self, exception_type, exception, traceback):
        self.stop_event.set()
        self.thread.join(timeout=2)
        log_event("stage_failed" if exception else "stage_completed",
                  stage=self.name,
                  elapsed_seconds=round(time.monotonic() - self.started_at, 1),
                  error=str(exception) if exception else None)
        return False


def run_dedup(spark, input_glob, output_root, expected_shards):
    from pyspark.sql import functions as functions
    from pyspark.sql import types

    validate_run(input_glob, output_root, expected_shards)
    output_root = output_root.rstrip("/")
    source_uri = output_root + "/source_rows"
    hit_uri = output_root + "/unique_hits"
    content_uri = output_root + "/unique_content_ids"
    directory_uri = output_root + "/unique_parent_directory_ids"

    schema = types.StructType([
        types.StructField("source_shard", types.IntegerType(), False),
        types.StructField("source_csv", types.StringType(), False),
        types.StructField("source_row", types.LongType(), False),
        types.StructField("directory_id", types.StringType(), False),
        types.StructField("name", types.StringType(), False),
        types.StructField("type", types.StringType(), False),
        types.StructField("content_id", types.StringType(), False),
        types.StructField("source_path", types.StringType(), False),
    ])
    records = spark.sparkContext.wholeTextFiles(
        input_glob, minPartitions=expected_shards
    ).flatMap(parse_csv_file)
    source_frame = spark.createDataFrame(records, schema)

    with StageMonitor(spark, "source_rows"):
        source_frame.write.mode("errorifexists").parquet(source_uri)
    source_frame = spark.read.parquet(source_uri)
    with StageMonitor(spark, "source_metrics"):
        source_count = source_frame.count()
        source_files = [row.source_csv for row in source_frame.select(
            "source_csv"
        ).distinct().collect()]
        source_shards = [row.source_shard for row in source_frame.select(
            "source_shard"
        ).distinct().collect()]
    if len(source_files) != expected_shards or len(source_shards) != expected_shards:
        raise RuntimeError("expected {} CSV shards, found {} files / {} indices".format(
            expected_shards, len(source_files), len(source_shards)
        ))
    if expected_shards == 128 and set(source_shards) != set(range(128)):
        raise RuntimeError("full input did not include all shard indices 0..127")
    if source_count == 0:
        raise RuntimeError("the candidate CSVs contained no P4 hits")
    log_event("source_metrics", rows=source_count, shards=len(source_files))

    keys = ["directory_id", "name", "content_id"]
    with StageMonitor(spark, "unique_hits"):
        source_frame.groupBy(*keys).agg(
            functions.count("*").alias("source_occurrences")
        ).write.mode("errorifexists").parquet(hit_uri)
    hit_frame = spark.read.parquet(hit_uri)
    with StageMonitor(spark, "unique_content_ids"):
        hit_frame.select("content_id").distinct().write.mode(
            "errorifexists"
        ).parquet(content_uri)
    with StageMonitor(spark, "unique_parent_directory_ids"):
        hit_frame.select("directory_id").distinct().write.mode(
            "errorifexists"
        ).parquet(directory_uri)

    with StageMonitor(spark, "final_metrics"):
        hit_count = hit_frame.count()
        content_count = spark.read.parquet(content_uri).count()
        directory_count = spark.read.parquet(directory_uri).count()
    metrics = {
        "schema_version": 1,
        "completed_at_utc": utc_now(),
        "input_glob": input_glob,
        "output_root": output_root,
        "source_row_count": source_count,
        "source_csv_count": len(source_files),
        "unique_hit_count": hit_count,
        "duplicate_hit_occurrences": source_count - hit_count,
        "unique_content_id_count": content_count,
        "unique_parent_directory_id_count": directory_count,
        "source_files": sorted(source_files),
    }
    with StageMonitor(spark, "write_metrics"):
        spark.createDataFrame(
            [(json.dumps(metrics, sort_keys=True),)], ["metrics_json"]
        ).coalesce(1).write.mode("errorifexists").text(output_root + "/metrics")
    log_event("dedup_completed", **{key: value for key, value in metrics.items()
                                    if key != "source_files"})
    return metrics


def main():
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.utils import getResolvedOptions
    from pyspark.context import SparkContext

    args = getResolvedOptions(sys.argv, [
        "JOB_NAME", "INPUT_GLOB", "OUTPUT_ROOT", "EXPECTED_SHARDS"
    ])
    spark_context = SparkContext.getOrCreate()
    glue_context = GlueContext(spark_context)
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)
    run_dedup(glue_context.spark_session, args["INPUT_GLOB"],
              args["OUTPUT_ROOT"], int(args["EXPECTED_SHARDS"]))
    job.commit()


if __name__ == "__main__":
    main()
