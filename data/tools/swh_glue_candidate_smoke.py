#!/usr/bin/env python3
"""Normalize one Athena P4-hit CSV in a small AWS Glue smoke job."""

import re
import sys


CANDIDATE_PREFIX = (
    "s3://swh2p4/athena-results/swh-p4-20260604/p4-directory-hits/"
)
OUTPUT_PREFIX = "s3://swh2p4/glue-results/swh-p4-20260604/smoke/"
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


def validate_candidate_input(uri):
    if not uri.startswith(CANDIDATE_PREFIX):
        raise ValueError("candidate input must be under {}".format(CANDIDATE_PREFIX))
    relative_key = uri[len(CANDIDATE_PREFIX) :]
    if not relative_key or "/" in relative_key or not relative_key.endswith(".csv"):
        raise ValueError("smoke input must name exactly one CSV object")
    if any(character in relative_key for character in "*?[]"):
        raise ValueError("wildcards are not allowed in the smoke input")
    return uri


def validate_output_prefix(uri):
    normalized = uri.rstrip("/") + "/"
    if not normalized.startswith(OUTPUT_PREFIX):
        raise ValueError("output must be under {}".format(OUTPUT_PREFIX))
    return normalized


def main():
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.utils import getResolvedOptions
    from pyspark import StorageLevel
    from pyspark.context import SparkContext
    from pyspark.sql import functions as functions
    from pyspark.sql import types

    args = getResolvedOptions(
        sys.argv,
        [
            "JOB_NAME",
            "CANDIDATE_INPUT",
            "OUTPUT_PREFIX",
        ],
    )
    job_run_id = get_cli_argument(sys.argv, "JOB_RUN_ID")
    candidate_input = validate_candidate_input(args["CANDIDATE_INPUT"])
    output_prefix = validate_output_prefix(args["OUTPUT_PREFIX"])
    run_output = "{}runs/{}/".format(output_prefix, job_run_id)

    spark_context = SparkContext.getOrCreate()
    glue_context = GlueContext(spark_context)
    spark = glue_context.spark_session
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    schema = types.StructType(
        [
            types.StructField("directory_id", types.StringType(), True),
            types.StructField("name", types.StringType(), True),
            types.StructField("type", types.StringType(), True),
            types.StructField("content_id", types.StringType(), True),
            types.StructField("source_path", types.StringType(), True),
        ]
    )
    raw = (
        spark.read.option("header", "true")
        .option("mode", "FAILFAST")
        .schema(schema)
        .csv(candidate_input)
        .withColumn("source_csv", functions.input_file_name())
        .persist(StorageLevel.MEMORY_AND_DISK)
    )

    valid_condition = (
        (functions.col("type") == "file")
        & functions.lower(functions.col("name")).endswith(".p4")
        & functions.col("directory_id").rlike(SHA1_RE.pattern)
        & functions.col("content_id").rlike(SHA1_RE.pattern)
        & functions.col("source_path").startswith(
            "s3://softwareheritage/graph/2026-06-04/orc/directory_entry/"
        )
    )
    valid = raw.filter(valid_condition).dropDuplicates(
        ["directory_id", "name", "content_id", "source_path"]
    )

    metrics_row = raw.agg(
        functions.count("*").alias("input_rows"),
        functions.sum(functions.when(valid_condition, 1).otherwise(0)).alias(
            "valid_rows_before_dedup"
        ),
    ).collect()[0]
    valid_rows = valid.count()
    if valid_rows == 0:
        raise RuntimeError("the candidate CSV contains no valid P4 rows")

    valid.write.mode("errorifexists").parquet(run_output + "candidates/")
    metrics = spark.createDataFrame(
        [
            {
                "candidate_input": candidate_input,
                "input_rows": metrics_row["input_rows"],
                "invalid_rows": (
                    metrics_row["input_rows"]
                    - metrics_row["valid_rows_before_dedup"]
                ),
                "output_rows": valid_rows,
                "run_id": job_run_id,
                "stage": "candidate-normalize-smoke",
            }
        ]
    )
    metrics.coalesce(1).write.mode("errorifexists").json(run_output + "metrics/")
    raw.unpersist()
    job.commit()


if __name__ == "__main__":
    main()
