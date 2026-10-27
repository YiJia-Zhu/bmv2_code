#!/usr/bin/env python3
"""Probe whether Spark can find parents of one SWH directory efficiently."""

import re
import sys


GRAPH_DIRECTORY_PREFIX = (
    "s3://softwareheritage/graph/2026-06-04/orc/directory_entry/"
)
OUTPUT_ROOT = (
    "s3://swh2p4/glue-results/swh-p4-20260604/graph-map-v1/parent-probes/"
)
SHA1_RE = re.compile(r"^[0-9a-f]{40}$")


def get_cli_argument(argv, name):
    flag = "--{}".format(name)
    for index, value in enumerate(argv):
        if value == flag:
            if index + 1 >= len(argv):
                break
            return argv[index + 1]
        if value.startswith(flag + "="):
            return value[len(flag) + 1 :]
    raise ValueError("missing required argument {}".format(flag))


def validate_directory_id(value):
    if not SHA1_RE.fullmatch(value or ""):
        raise ValueError("TARGET_DIRECTORY_ID must be a lowercase 40-character SHA1")
    return value


def validate_output_root(value):
    normalized = value.rstrip("/") + "/"
    if normalized != OUTPUT_ROOT:
        raise ValueError("OUTPUT_ROOT must be exactly {}".format(OUTPUT_ROOT))
    return normalized


def run_output_uri(output_root, directory_id, job_run_id):
    if not job_run_id.startswith("jr_") or "/" in job_run_id:
        raise ValueError("invalid Glue JOB_RUN_ID")
    return "{}{}/attempts/{}/".format(output_root, directory_id, job_run_id)


def main():
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.utils import getResolvedOptions
    from pyspark import StorageLevel
    from pyspark.context import SparkContext
    from pyspark.sql import functions

    args = getResolvedOptions(
        sys.argv,
        [
            "JOB_NAME",
            "TARGET_DIRECTORY_ID",
            "OUTPUT_ROOT",
            "PROBE_ID",
        ],
    )
    job_run_id = get_cli_argument(sys.argv, "JOB_RUN_ID")
    target_directory_id = validate_directory_id(args["TARGET_DIRECTORY_ID"])
    output_root = validate_output_root(args["OUTPUT_ROOT"])
    run_output = run_output_uri(output_root, target_directory_id, job_run_id)

    spark_context = SparkContext.getOrCreate()
    glue_context = GlueContext(spark_context)
    spark = glue_context.spark_session
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    spark.conf.set("spark.sql.orc.filterPushdown", "true")
    spark.conf.set("spark.sql.orc.impl", "native")

    entries = spark.read.format("orc").load(GRAPH_DIRECTORY_PREFIX)
    required_columns = {"directory_id", "target", "type"}
    missing_columns = required_columns.difference(entries.columns)
    if missing_columns:
        raise RuntimeError(
            "directory_entry ORC is missing columns: {}".format(
                ", ".join(sorted(missing_columns))
            )
        )

    parents = (
        entries.where(
            (functions.col("type") == functions.lit("dir"))
            & (functions.col("target") == functions.lit(target_directory_id))
        )
        .select(
            functions.col("directory_id").alias("parent_directory_id"),
            functions.col("target").alias("child_directory_id"),
            functions.col("name").alias("child_name"),
            functions.col("perms"),
            functions.input_file_name().alias("source_orc"),
        )
        .dropDuplicates(
            ["parent_directory_id", "child_directory_id", "child_name", "source_orc"]
        )
        .persist(StorageLevel.MEMORY_AND_DISK)
    )

    parent_count = parents.count()
    parents.write.mode("errorifexists").parquet(run_output + "parents/")

    metrics = spark.createDataFrame(
        [
            {
                "graph_release": "2026-06-04",
                "job_run_id": job_run_id,
                "parent_count": parent_count,
                "probe_id": args["PROBE_ID"],
                "stage": "single-directory-parent-probe",
                "target_directory_id": target_directory_id,
            }
        ]
    )
    metrics.coalesce(1).write.mode("errorifexists").json(run_output + "metrics/")
    parents.unpersist()
    job.commit()


if __name__ == "__main__":
    main()
