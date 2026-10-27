#!/usr/bin/env python3
"""Read SWH Graph ORC footers and publish a schema manifest.

This bounded Glue job only asks Spark for ORC schemas.  It does not execute a
full table scan and is intended to run before ``swh_glue_graph_mapper.py`` so
column-name or release changes are caught cheaply.
"""

from __future__ import print_function

import json
import os
import sys


TABLE_ARGUMENTS = (
    ("DIRECTORY_ENTRY_URI", True),
    ("REVISION_URI", True),
    ("REVISION_HISTORY_URI", False),
    ("SNAPSHOT_BRANCH_URI", True),
    ("ORIGIN_VISIT_STATUS_URI", True),
    ("ORIGIN_URI", True),
    ("RELEASE_URI", False),
)


def get_cli_argument(argv, name, default=None):
    flag = "--{}".format(name)
    for index, value in enumerate(argv):
        if value == flag:
            if index + 1 >= len(argv):
                raise ValueError("missing value for {}".format(flag))
            return argv[index + 1]
        if value.startswith(flag + "="):
            return value[len(flag) + 1 :]
    return default


def join_uri(root, child):
    return "{}/{}".format(str(root).rstrip("/"), str(child).lstrip("/"))


def collect_table_schemas(spark, table_uris):
    result = []
    for name, uri, required in table_uris:
        if not uri:
            result.append(
                {
                    "table": name,
                    "uri": None,
                    "required": required,
                    "status": "omitted",
                }
            )
            continue
        frame = spark.read.format("orc").load(uri).limit(0)
        result.append(
            {
                "table": name,
                "uri": uri,
                "required": required,
                "status": "ok",
                "columns": [
                    {
                        "name": field.name,
                        "type": field.dataType.simpleString(),
                        "nullable": field.nullable,
                    }
                    for field in frame.schema.fields
                ],
            }
        )
    return result


def main(argv=None):
    argv = sys.argv if argv is None else argv
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.utils import getResolvedOptions
    from pyspark.context import SparkContext

    required_args = [
        "JOB_NAME",
        "OUTPUT_ROOT",
    ] + [name for name, required in TABLE_ARGUMENTS if required]
    args = getResolvedOptions(argv, required_args)
    run_id = get_cli_argument(argv, "JOB_RUN_ID", "manual-{}".format(os.getpid()))
    table_uris = []
    for name, required in TABLE_ARGUMENTS:
        table_uris.append(
            (name, args.get(name) or get_cli_argument(argv, name), required)
        )

    spark_context = SparkContext.getOrCreate()
    glue_context = GlueContext(spark_context)
    spark = glue_context.spark_session
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)
    tables = collect_table_schemas(spark, table_uris)
    manifest = {
        "schema_version": 1,
        "run_id": run_id,
        "tables": tables,
    }
    output = join_uri(args["OUTPUT_ROOT"], "runs/{}/schema_manifest".format(run_id))
    spark.createDataFrame([(json.dumps(manifest, ensure_ascii=False),)], ["manifest"]).coalesce(
        1
    ).write.mode("overwrite").text(output)
    job.commit()
    print(json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    main()
