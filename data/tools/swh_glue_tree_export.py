#!/usr/bin/env python3
"""Export complete SWH directory trees for selected revision roots.

Run this after P4_16 verification and the graph mapper's
``latest_verified_candidates`` output.  It reads the directory_entry ORC once
for the selected roots, then walks child directories in Spark.  Both Parquet
and line-oriented JSON output are written because the latter can be downloaded
and passed to the existing local collector without a Spark runtime.
"""

from __future__ import print_function

import json
import os
import sys


DEFAULT_MAX_DEPTH = 128


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


def parse_bool(value):
    lowered = str(value).strip().lower()
    if lowered in ("1", "true", "yes", "y", "on"):
        return True
    if lowered in ("0", "false", "no", "n", "off"):
        return False
    raise ValueError("invalid boolean value: {}".format(value))


def positive_int(value, name):
    try:
        result = int(value)
    except (TypeError, ValueError):
        raise ValueError("{} must be positive".format(name))
    if result <= 0:
        raise ValueError("{} must be positive".format(name))
    return result


def join_uri(root, child):
    return "{}/{}".format(str(root).rstrip("/"), str(child).lstrip("/"))


def find_column(frame, aliases):
    names = {str(column).lower(): column for column in frame.columns}
    for alias in aliases:
        if str(alias).lower() in names:
            return names[str(alias).lower()]
    return None


def require_column(frame, aliases, label):
    column = find_column(frame, aliases)
    if column is None:
        raise RuntimeError(
            "{} is missing; accepted {}; found {}".format(
                label, ", ".join(aliases), ", ".join(frame.columns)
            )
        )
    return column


def id_expression(frame, column, functions, types):
    field = next(field for field in frame.schema.fields if field.name == column)
    if isinstance(field.dataType, types.BinaryType):
        expression = functions.lower(functions.hex(functions.col(column)))
    else:
        expression = functions.lower(functions.trim(functions.col(column).cast("string")))
    return functions.regexp_replace(expression, r"^swh:1:[a-z]+:", "")


def text_expression(frame, column, functions, types):
    field = next(field for field in frame.schema.fields if field.name == column)
    if isinstance(field.dataType, types.BinaryType):
        return functions.decode(functions.col(column), "UTF-8")
    return functions.col(column).cast("string")


def read_selected_roots(spark, input_uri, input_format="parquet"):
    from pyspark.sql import functions
    from pyspark.sql import types

    if input_format.lower() == "parquet":
        frame = spark.read.parquet(input_uri)
    elif input_format.lower() == "json":
        frame = spark.read.json(input_uri)
    elif input_format.lower() == "csv":
        frame = spark.read.option("header", "true").option("mode", "FAILFAST").csv(input_uri)
    else:
        raise ValueError("candidate format must be parquet, json, or csv")
    root_column = require_column(
        frame,
        ["root_directory_id", "directory_id", "root_directory"],
        "selected candidate root directory",
    )
    rank_column = find_column(frame, ["origin_rank"])
    status_column = find_column(frame, ["selection_status", "p4_16_confirmed"])
    selected = frame.select(
        id_expression(frame, root_column, functions, types).alias("root_directory_id")
    )
    if rank_column:
        selected = selected.where(functions.col(rank_column) == functions.lit(1))
    if status_column and status_column != rank_column:
        selected = selected.where(
            functions.lower(functions.trim(functions.col(status_column).cast("string"))).isin(
                "verified", "true", "1", "yes", "compiled"
            )
        )
    return selected.where(functions.col("root_directory_id").isNotNull()).dropDuplicates()


def read_entries(spark, uri):
    from pyspark.sql import functions
    from pyspark.sql import types

    raw = spark.read.format("orc").load(uri)
    directory_column = require_column(raw, ["directory_id", "parent_directory_id"], "entry directory")
    target_column = require_column(raw, ["target", "target_id"], "entry target")
    name_column = require_column(raw, ["name", "entry_name"], "entry name")
    type_column = require_column(raw, ["type", "entry_type"], "entry type")
    perms_column = find_column(raw, ["perms", "permissions"])
    length_column = find_column(raw, ["length", "content_length", "size"])
    selected = [
        id_expression(raw, directory_column, functions, types).alias("directory_id"),
        text_expression(raw, name_column, functions, types).alias("name"),
        functions.lower(
            functions.trim(text_expression(raw, type_column, functions, types))
        ).alias("type"),
        id_expression(raw, target_column, functions, types).alias("target"),
    ]
    if perms_column:
        selected.append(functions.col(perms_column).cast("string").alias("perms"))
    else:
        selected.append(functions.lit(None).cast("string").alias("perms"))
    if length_column:
        selected.append(functions.col(length_column).cast("long").alias("length"))
    else:
        selected.append(functions.lit(None).cast("long").alias("length"))
    return raw.select(*selected).where(
        functions.col("directory_id").isNotNull()
        & functions.col("name").isNotNull()
        & functions.col("target").isNotNull()
    ).dropDuplicates(["directory_id", "name", "type", "target"])


def export_tree(spark, roots, entries, max_depth):
    from pyspark import StorageLevel
    from pyspark.sql import functions

    frontier = roots.select(
        functions.col("root_directory_id").alias("directory_id"),
        functions.col("root_directory_id").alias("root_directory_id"),
        functions.lit("").alias("parent_path"),
        functions.lit(0).cast("int").alias("depth"),
    ).persist(StorageLevel.MEMORY_AND_DISK)
    emitted = None
    visited = frontier.select("root_directory_id", "directory_id", "parent_path")
    hit_depth_limit = False
    for depth in range(max_depth + 1):
        current = (
            frontier.alias("frontier")
            .join(
                entries.alias("entry"),
                functions.col("frontier.directory_id")
                == functions.col("entry.directory_id"),
                "inner",
            )
            .select(
                functions.col("frontier.root_directory_id").alias("root_directory_id"),
                functions.col("entry.directory_id").alias("directory_id"),
                functions.col("entry.name").alias("name"),
                functions.col("entry.type").alias("type"),
                functions.col("entry.target").alias("target"),
                functions.col("entry.perms").alias("perms"),
                functions.col("entry.length").alias("length"),
                functions.when(
                    functions.col("frontier.parent_path") == "",
                    functions.col("entry.name"),
                )
                .otherwise(
                    functions.concat_ws(
                        "/",
                        functions.col("frontier.parent_path"),
                        functions.col("entry.name"),
                    )
                )
                .alias("path"),
                functions.lit(depth).cast("int").alias("depth"),
            )
            .dropDuplicates(["root_directory_id", "path", "target", "type"])
        )
        emitted = current if emitted is None else emitted.unionByName(current)
        next_frontier = (
            current.where(functions.lower(functions.col("type")).isin("dir", "directory", "folder"))
            .select(
                functions.col("target").alias("directory_id"),
                functions.col("root_directory_id"),
                functions.col("path").alias("parent_path"),
                (functions.col("depth") + functions.lit(1)).cast("int").alias("depth"),
            )
            .join(
                visited,
                ["root_directory_id", "directory_id", "parent_path"],
                "leftanti",
            )
            .dropDuplicates(["root_directory_id", "directory_id", "parent_path"])
            .persist(StorageLevel.MEMORY_AND_DISK)
        )
        if next_frontier.limit(1).count() == 0:
            next_frontier.unpersist()
            break
        visited = visited.unionByName(
            next_frontier.select("root_directory_id", "directory_id", "parent_path")
        )
        frontier.unpersist()
        frontier = next_frontier
    else:
        hit_depth_limit = True
    frontier.unpersist()
    return emitted, {"tree_depth_limit_reached": hit_depth_limit}


def main(argv=None):
    argv = sys.argv if argv is None else argv
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.utils import getResolvedOptions
    from pyspark.context import SparkContext

    args = getResolvedOptions(
        argv,
        [
            "JOB_NAME",
            "CANDIDATE_INPUT",
            "CANDIDATE_FORMAT",
            "DIRECTORY_ENTRY_URI",
            "OUTPUT_ROOT",
            "CONFIRM_FULL_SCAN",
        ],
    )
    if not parse_bool(args["CONFIRM_FULL_SCAN"]):
        raise ValueError("CONFIRM_FULL_SCAN=true is required")
    run_id = get_cli_argument(argv, "JOB_RUN_ID", "manual-{}".format(os.getpid()))
    max_depth = positive_int(
        get_cli_argument(argv, "MAX_DIRECTORY_DEPTH", DEFAULT_MAX_DEPTH),
        "MAX_DIRECTORY_DEPTH",
    )
    output = join_uri(args["OUTPUT_ROOT"], "runs/{}/tree_export".format(run_id))

    spark_context = SparkContext.getOrCreate()
    glue_context = GlueContext(spark_context)
    spark = glue_context.spark_session
    spark.conf.set("spark.sql.orc.filterPushdown", "true")
    spark.conf.set("spark.sql.orc.impl", "native")
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)
    roots = read_selected_roots(
        spark, args["CANDIDATE_INPUT"], args["CANDIDATE_FORMAT"]
    ).persist()
    if roots.limit(1).count() == 0:
        raise RuntimeError(
            "candidate input contains no origin_rank=1 verified revision roots"
        )
    entries = read_entries(spark, args["DIRECTORY_ENTRY_URI"]).persist()
    tree, metrics = export_tree(spark, roots, entries, max_depth)
    if tree is None:
        raise RuntimeError("selected roots have no directory_entry rows")
    tree.write.mode("overwrite").parquet(join_uri(output, "tree_entries"))
    tree.write.mode("overwrite").json(join_uri(output, "tree_entries_json"))
    roots.write.mode("overwrite").parquet(join_uri(output, "root_directories"))
    metrics.update(
        {
            "run_id": run_id,
            "root_count": roots.count(),
            "tree_entry_count": tree.count(),
            "output": output,
        }
    )
    spark.createDataFrame(
        [(json.dumps(metrics, ensure_ascii=False),)], ["metrics"]
    ).coalesce(1).write.mode("overwrite").text(join_uri(output, "metrics"))
    job.commit()
    print(json.dumps(metrics, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
