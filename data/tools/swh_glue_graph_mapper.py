#!/usr/bin/env python3
"""Map SWH ``.p4`` hits to revisions, snapshots, and origins with Spark.

This is the graph-mapping stage between the Athena ``directory_entry`` scan and
the existing repository collector.  It deliberately runs against the public
SWH ORC export in Glue/EMR; it does not make one REST request per hit.

The job performs four bounded stages:

1. normalize the Athena CSV/Parquet hit manifest;
2. build the reverse directory-edge closure for every hit directory;
3. join ancestor directories to revision roots;
4. resolve snapshot branches, revision parents, origin visits, and origin URLs.

The SWH export has changed column encodings between releases (IDs may be
binary and names are commonly binary), so all IDs and names are normalized in
one place.  A missing required column raises an error instead of producing a
plausible but incorrect repository mapping.

The script is a Glue job.  It imports ``awsglue`` only inside ``main`` so the
pure validation and DataFrame helpers remain importable by the offline tests.
"""

from __future__ import print_function

import argparse
import datetime as datetime_module
import hashlib
import json
import os
import re
import sys
import threading
import time
import uuid


GRAPH_RELEASE = "2026-06-04"
MAPPER_VERSION = "graph-map-resume-v1"
DEFAULT_GRAPH_ROOT = "s3://softwareheritage/graph/{}/orc".format(GRAPH_RELEASE)
DEFAULT_MAX_DIRECTORY_DEPTH = 128
DEFAULT_MAX_REVISION_DEPTH = 10000
DEFAULT_PROGRESS_INTERVAL_SECONDS = 30
DEFAULT_OUTPUT_ROOT = "s3://swh2p4/glue-results/swh-p4-{}/graph-map-v2".format(
    GRAPH_RELEASE.replace("-", "")
)
SHA1_RE = re.compile(r"^[0-9a-f]{40}$")
SWHID_PREFIX_RE = re.compile(r"^swh:1:[a-z]+:", re.IGNORECASE)


def utc_now():
    return datetime_module.datetime.now(datetime_module.timezone.utc).isoformat()


def log_progress(event, **fields):
    record = {"event": event, "timestamp_utc": utc_now()}
    record.update(fields)
    print(json.dumps(record, ensure_ascii=False, sort_keys=True), flush=True)


def task_progress(info, previous=None, observed_at=None):
    """Estimate the remaining time of one Spark stage from two task samples."""

    observed_at = time.monotonic() if observed_at is None else observed_at
    total = info.numTasks
    completed = info.numCompletedTasks
    result = {
        "spark_stage_id": info.stageId,
        "spark_stage_attempt": info.currentAttemptId,
        "spark_stage_name": info.name,
        "tasks_completed": completed,
        "tasks_total": total,
        "tasks_active": info.numActiveTasks,
        "tasks_failed": info.numFailedTasks,
        "task_percent": round(100.0 * completed / total, 2) if total else None,
        "task_rate_per_second": None,
        "eta_seconds": None,
        "estimated_finish_utc": None,
    }
    if previous is not None and total > completed:
        earlier_completed, earlier_at = previous
        elapsed = observed_at - earlier_at
        advanced = completed - earlier_completed
        if elapsed > 0 and advanced > 0:
            rate = advanced / elapsed
            eta = (total - completed) / rate
            result["task_rate_per_second"] = round(rate, 3)
            result["eta_seconds"] = round(eta)
            result["estimated_finish_utc"] = (
                datetime_module.datetime.now(datetime_module.timezone.utc)
                + datetime_module.timedelta(seconds=eta)
            ).isoformat()
    return result


class StageProgress(object):
    """Report pipeline and active Spark-stage progress without Spark actions."""

    def __init__(self, spark, name, mapping_id, interval_seconds):
        self.tracker = spark.sparkContext.statusTracker()
        self.name = name
        self.mapping_id = mapping_id
        self.interval_seconds = interval_seconds
        self.started_at = None
        self.started_at_utc = None
        self.stop_event = threading.Event()
        self.thread = None
        self.previous = {}
        self.reported_error = False

    def start(self):
        self.started_at = time.monotonic()
        self.started_at_utc = utc_now()
        log_progress(
            "pipeline_stage_started", mapping_id=self.mapping_id,
            pipeline_stage=self.name, started_at_utc=self.started_at_utc,
            progress_interval_seconds=self.interval_seconds,
        )
        self.thread = threading.Thread(target=self._heartbeat, daemon=True)
        self.thread.start()

    def _heartbeat(self):
        while not self.stop_event.wait(self.interval_seconds):
            try:
                observed_at = time.monotonic()
                active = []
                for stage_id in sorted(self.tracker.getActiveStageIds()):
                    info = self.tracker.getStageInfo(stage_id)
                    if info is None:
                        continue
                    key = (info.stageId, info.currentAttemptId)
                    previous = self.previous.get(key)
                    active.append(task_progress(info, previous, observed_at))
                    self.previous[key] = (info.numCompletedTasks, observed_at)
                log_progress(
                    "pipeline_stage_progress", mapping_id=self.mapping_id,
                    pipeline_stage=self.name,
                    started_at_utc=self.started_at_utc,
                    elapsed_seconds=round(observed_at - self.started_at, 1),
                    spark_stages=active,
                    eta_scope="current_spark_stage_only",
                )
            except Exception as exc:
                if not self.reported_error:
                    log_progress(
                        "progress_monitor_error", mapping_id=self.mapping_id,
                        pipeline_stage=self.name, error=str(exc),
                    )
                    self.reported_error = True

    def stop(self, state, metrics=None, error=None):
        self.stop_event.set()
        if self.thread is not None:
            self.thread.join(timeout=2)
        fields = {
            "mapping_id": self.mapping_id,
            "pipeline_stage": self.name,
            "started_at_utc": self.started_at_utc,
            "elapsed_seconds": round(time.monotonic() - self.started_at, 1),
            "metrics": metrics or {},
        }
        if error is not None:
            fields["error"] = error
        log_progress("pipeline_stage_{}".format(state), **fields)


def get_cli_argument(argv, name, default=None):
    """Read a Glue-style ``--NAME value`` or ``--NAME=value`` argument."""

    flag = "--{}".format(name)
    for index, value in enumerate(argv):
        if value == flag:
            if index + 1 >= len(argv):
                raise ValueError("missing value for {}".format(flag))
            return argv[index + 1]
        if value.startswith(flag + "="):
            return value[len(flag) + 1 :]
    return default


def parse_bool(value, default=False):
    if value is None:
        return default
    lowered = str(value).strip().lower()
    if lowered in ("1", "true", "t", "yes", "y", "on"):
        return True
    if lowered in ("0", "false", "f", "no", "n", "off"):
        return False
    raise ValueError("invalid boolean value: {}".format(value))


def positive_int(value, name):
    try:
        number = int(value)
    except (TypeError, ValueError):
        raise ValueError("{} must be a positive integer".format(name))
    if number <= 0:
        raise ValueError("{} must be a positive integer".format(name))
    return number


def join_uri(root, child):
    """Join an S3 URI or local path without introducing a double slash."""

    if not root:
        raise ValueError("URI root is required")
    return "{}/{}".format(str(root).rstrip("/"), str(child).lstrip("/"))


def stable_fingerprint(value):
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def parse_shard_indices(value):
    if value is None:
        raise ValueError("SHARD_INDICES is required with CANDIDATE_MANIFEST_URI")
    if value.strip().lower() == "all":
        return None
    parts = [part.strip() for part in value.split(",")]
    if not parts or any(not part.isdigit() for part in parts):
        raise ValueError("SHARD_INDICES must be 'all' or comma-separated integers")
    indices = [int(part) for part in parts]
    if len(indices) != len(set(indices)):
        raise ValueError("SHARD_INDICES contains duplicate indices")
    return indices


def select_manifest_shards(manifest, indices):
    if manifest.get("graph_release") != GRAPH_RELEASE:
        raise ValueError("candidate manifest graph release differs from mapper")
    objects = manifest.get("objects", [])
    by_index = {item["index"]: item for item in objects}
    if len(by_index) != len(objects):
        raise ValueError("candidate manifest contains duplicate shard indices")
    selected = sorted(by_index) if indices is None else sorted(indices)
    if not selected:
        raise ValueError("no candidate shards selected")
    shards = []
    for index in selected:
        item = by_index.get(index)
        if item is None or item.get("status") != "complete":
            raise ValueError("candidate shard {} is absent or incomplete".format(index))
        for key in ("stable_result_s3_uri", "stable_result_size_bytes", "stable_result_etag"):
            if not item.get(key):
                raise ValueError("candidate shard {} lacks {}".format(index, key))
        shards.append({
            "index": index,
            "uri": item["stable_result_s3_uri"],
            "size": int(item["stable_result_size_bytes"]),
            "etag": item["stable_result_etag"],
        })
    return shards


def hadoop_path_exists(spark, uri):
    path = spark._jvm.org.apache.hadoop.fs.Path(uri)
    filesystem = path.getFileSystem(spark._jsc.hadoopConfiguration())
    return filesystem.exists(path)


def write_json_file(spark, uri, value):
    path = spark._jvm.org.apache.hadoop.fs.Path(uri)
    filesystem = path.getFileSystem(spark._jsc.hadoopConfiguration())
    filesystem.mkdirs(path.getParent())
    output = filesystem.create(path, False)
    try:
        output.write(bytearray(json.dumps(value, sort_keys=True).encode("utf-8")))
    finally:
        output.close()


def read_stage_record(spark, run_uri, stage):
    data_uri = join_uri(run_uri, stage)
    meta_uri = join_uri(run_uri, "_stage_meta/{}".format(stage))
    path = spark._jvm.org.apache.hadoop.fs.Path(meta_uri)
    filesystem = path.getFileSystem(spark._jsc.hadoopConfiguration())
    if not filesystem.exists(path):
        return None
    records = sorted(
        status.getPath().toString()
        for status in filesystem.listStatus(path)
        if status.getPath().getName().endswith(".json")
    )
    if len(records) > 1:
        raise RuntimeError("stage {} has multiple completion records".format(stage))
    if records:
        rows = spark.read.text(records[0]).take(2)
    elif hadoop_path_exists(spark, join_uri(meta_uri, "_COMPLETE")):
        # A completed pre-marker pilot stage can be explicitly certified once.
        rows = spark.read.text(meta_uri).take(2)
    else:
        return None
    if len(rows) != 1:
        raise RuntimeError("stage {} metadata is not a single record".format(stage))
    record = json.loads(rows[0].value)
    if not hadoop_path_exists(spark, record.get("data_uri", data_uri)):
        raise RuntimeError("stage {} has metadata but no data".format(stage))
    return record


def materialize_stage(
    spark, run_uri, stage, fingerprint, build, resume, output_format="parquet",
    mapping_id=None, progress_interval_seconds=DEFAULT_PROGRESS_INTERVAL_SECONDS,
):
    """Persist a stage and reuse it only after a matching completion marker."""

    data_uri = join_uri(run_uri, stage)
    meta_uri = join_uri(run_uri, "_stage_meta/{}".format(stage))
    previous = read_stage_record(spark, run_uri, stage)
    if previous is not None:
        if previous.get("fingerprint") != fingerprint:
            raise RuntimeError("stage {} input/configuration changed; use a new MAPPING_ID".format(stage))
        if not resume:
            raise RuntimeError("stage {} already exists; pass RESUME=true".format(stage))
        log_progress(
            "pipeline_stage_resumed", mapping_id=mapping_id,
            pipeline_stage=stage, metrics=previous.get("metrics", {}),
        )
        previous_uri = previous.get("data_uri", data_uri)
        frame = spark.read.parquet(previous_uri) if output_format == "parquet" else None
        return frame, previous.get("metrics", {}), True

    attempt_id = uuid.uuid4().hex
    if hadoop_path_exists(spark, data_uri):
        if not resume:
            raise RuntimeError("stage {} has incomplete data; pass RESUME=true".format(stage))
        data_uri = join_uri(run_uri, "_stage_attempts/{}/{}".format(stage, attempt_id))
    progress = StageProgress(spark, stage, mapping_id, progress_interval_seconds)
    progress.start()
    try:
        frame, metrics = build()
        try:
            writer = frame.write.mode("errorifexists")
            if output_format == "parquet":
                writer.parquet(data_uri)
            elif output_format == "csv":
                writer.option("header", "true").csv(data_uri)
            else:
                raise ValueError("unsupported stage output format: {}".format(output_format))
        finally:
            frame.unpersist()
        record = {
            "stage": stage,
            "fingerprint": fingerprint,
            "data_uri": data_uri,
            "mapper_version": MAPPER_VERSION,
            "completed_at": utc_now(),
            "metrics": metrics,
        }
        write_json_file(spark, join_uri(meta_uri, "{}.json".format(attempt_id)), record)
    except BaseException as exc:
        progress.stop("failed", error="{}: {}".format(type(exc).__name__, exc))
        raise
    progress.stop("completed", metrics=metrics)
    return (spark.read.parquet(data_uri) if output_format == "parquet" else None), metrics, False


def strip_swhid_prefix(value):
    value = "" if value is None else str(value).strip().lower()
    return SWHID_PREFIX_RE.sub("", value)


def normalize_kind(value):
    value = "" if value is None else str(value).strip().lower()
    if value in ("file", "blob", "content"):
        return "file"
    if value in ("dir", "directory", "folder"):
        return "dir"
    if value in ("rev", "revision", "commit"):
        return "revision"
    if value in ("rel", "release"):
        return "release"
    return value


def is_github_url(url):
    """Return whether an origin URL is hosted by GitHub."""

    from urllib.parse import urlparse

    parsed = urlparse((url or "").strip())
    return (parsed.hostname or "").lower() in ("github.com", "www.github.com")


def _normalized_column_name(value):
    return str(value).strip().lower().replace("`", "")


def find_column(df, aliases):
    """Return the actual DataFrame column matching the first alias."""

    columns = {
        _normalized_column_name(column): column for column in df.columns
    }
    for alias in aliases:
        actual = columns.get(_normalized_column_name(alias))
        if actual is not None:
            return actual
    return None


def require_column(df, aliases, label):
    actual = find_column(df, aliases)
    if actual is None:
        raise RuntimeError(
            "{} is missing; accepted columns: {}; found: {}".format(
                label, ", ".join(aliases), ", ".join(df.columns)
            )
        )
    return actual


def _spark_id_expression(df, actual, functions, types):
    """Build a lower-case ID expression for string or binary SWH IDs."""

    field = next(field for field in df.schema.fields if field.name == actual)
    if isinstance(field.dataType, types.BinaryType):
        expression = functions.lower(functions.hex(functions.col(actual)))
    else:
        expression = functions.lower(functions.trim(functions.col(actual).cast("string")))
    return functions.regexp_replace(expression, r"^swh:1:[a-z]+:", "")


def _spark_text_expression(df, actual, functions, types):
    field = next(field for field in df.schema.fields if field.name == actual)
    if isinstance(field.dataType, types.BinaryType):
        return functions.decode(functions.col(actual), "UTF-8")
    return functions.col(actual).cast("string")


def select_canonical_column(
    df,
    aliases,
    output_name,
    functions,
    types,
    required=False,
    kind="id",
):
    """Return a canonical Spark expression or a NULL expression."""

    actual = find_column(df, aliases)
    if actual is None:
        if required:
            raise RuntimeError(
                "{} is missing; accepted columns: {}; found: {}".format(
                    output_name, ", ".join(aliases), ", ".join(df.columns)
                )
            )
        return functions.lit(None).cast("string").alias(output_name)

    if kind == "id":
        expression = _spark_id_expression(df, actual, functions, types)
    elif kind == "text":
        expression = _spark_text_expression(df, actual, functions, types)
    elif kind == "kind":
        expression = functions.lower(
            functions.trim(_spark_text_expression(df, actual, functions, types))
        )
    else:
        expression = functions.col(actual).cast("string")
    return expression.alias(output_name)


def normalize_candidate_frame(spark, input_uri, input_format="auto"):
    """Read an Athena hit manifest and return canonical columns.

    The output has one row per distinct ``(directory_id, name, content_id)``.
    Repository fields are retained when the input already contains them.
    """

    from pyspark.sql import functions
    from pyspark.sql import types

    selected_format = input_format.lower()
    if selected_format == "auto":
        lower = input_uri.lower()
        if lower.endswith((".parquet", ".parquet/")):
            selected_format = "parquet"
        elif lower.endswith((".json", ".jsonl", ".json/", ".jsonl/")):
            selected_format = "json"
        else:
            # Athena's result prefix normally ends in '/'; its objects are CSV.
            selected_format = "csv"
    if selected_format not in ("csv", "parquet", "json"):
        raise ValueError("input format must be csv, parquet, or json")

    reader = spark.read
    if selected_format == "csv":
        raw = (
            reader.option("header", "true")
            .option("mode", "FAILFAST")
            .csv(input_uri)
        )
    elif selected_format == "json":
        raw = reader.json(input_uri)
    else:
        raw = reader.parquet(input_uri)

    canonical = raw.select(
        select_canonical_column(
            raw,
            ["directory_id", "containing_directory_id", "parent_directory_id"],
            "directory_id",
            functions,
            types,
            required=True,
        ),
        select_canonical_column(
            raw,
            ["name", "file_name", "path", "file_path"],
            "name",
            functions,
            types,
            required=True,
            kind="text",
        ),
        select_canonical_column(
            raw,
            ["type", "entry_type", "target_type"],
            "entry_type",
            functions,
            types,
            required=False,
            kind="kind",
        ),
        select_canonical_column(
            raw,
            ["content_id", "target_id", "target", "sha1_git"],
            "content_id",
            functions,
            types,
            required=False,
        ),
        select_canonical_column(
            raw,
            ["origin_id", "origin"],
            "origin_id",
            functions,
            types,
            required=False,
        ),
        select_canonical_column(
            raw,
            ["origin_url", "url"],
            "origin_url",
            functions,
            types,
            required=False,
            kind="text",
        ),
        select_canonical_column(
            raw,
            ["revision_id", "revision", "commit_id"],
            "revision_id",
            functions,
            types,
            required=False,
        ),
        select_canonical_column(
            raw,
            ["root_directory_id", "revision_directory_id", "root_directory"],
            "root_directory_id",
            functions,
            types,
            required=False,
        ),
        select_canonical_column(
            raw,
            ["p4_16_confirmed", "contains_p4_16"],
            "p4_16_confirmed",
            functions,
            types,
            required=False,
            kind="text",
        ),
        functions.input_file_name().alias("candidate_source_file"),
    )
    entry_type = functions.lower(functions.trim(functions.col("entry_type")))
    is_file = functions.col("entry_type").isNull() | entry_type.isin(
        "file", "blob", "content"
    )
    return (
        canonical.where(
            functions.col("directory_id").isNotNull()
            & functions.col("name").isNotNull()
            & functions.lower(functions.col("name")).endswith(".p4")
            & is_file
        )
        .dropDuplicates(["directory_id", "name", "content_id"])
    )


def read_directory_edges(spark, directory_entry_uri):
    """Read parent -> child directory edges from the Graph ORC table."""

    from pyspark.sql import functions
    from pyspark.sql import types

    entries = spark.read.format("orc").load(directory_entry_uri)
    parent_column = require_column(
        entries, ["directory_id", "parent_directory_id"], "directory_entry parent directory"
    )
    target_column = require_column(
        entries, ["target", "target_id", "child_directory_id"], "directory_entry target"
    )
    type_column = require_column(entries, ["type", "entry_type"], "directory_entry type")
    name_column = find_column(entries, ["name", "entry_name"])

    parent = _spark_id_expression(entries, parent_column, functions, types)
    child = _spark_id_expression(entries, target_column, functions, types)
    entry_type = functions.lower(
        functions.trim(_spark_text_expression(entries, type_column, functions, types))
    )
    selected = [
        parent.alias("parent_directory_id"),
        child.alias("child_directory_id"),
    ]
    if name_column:
        selected.append(
            _spark_text_expression(entries, name_column, functions, types).alias(
                "child_name"
            )
        )
    else:
        selected.append(functions.lit(None).cast("string").alias("child_name"))
    return (
        entries.where(entry_type.isin("dir", "directory", "folder"))
        .select(*selected)
        .where(
            functions.col("parent_directory_id").isNotNull()
            & functions.col("child_directory_id").isNotNull()
        )
    )


def _empty_like(spark, schema):
    return spark.createDataFrame([], schema)


def build_directory_closure(
    seed_frame, edge_frame, max_depth, checkpoint_dir=None, mapping_id=None
):
    """Return ``seed_directory_id -> ancestor_directory_id`` closure.

    The reverse edge join is iterative and cycle-safe.  The returned metrics
    distinguish a natural fixed point from a depth-limit stop so callers can
    avoid presenting a truncated closure as complete.
    """

    from pyspark import StorageLevel
    from pyspark.sql import functions

    seeds = (
        seed_frame.select(functions.col("directory_id").alias("seed_directory_id"))
        .where(functions.col("seed_directory_id").isNotNull())
        .dropDuplicates()
    )
    closure = seeds.select(
        "seed_directory_id",
        functions.col("seed_directory_id").alias("ancestor_directory_id"),
        functions.lit(0).cast("int").alias("depth"),
        functions.lit("").alias("path_from_ancestor"),
    )
    frontier = closure.persist(StorageLevel.MEMORY_AND_DISK)
    closure = closure.persist(StorageLevel.MEMORY_AND_DISK)
    seed_count = closure.count()
    log_progress(
        "traversal_started", mapping_id=mapping_id, traversal="directory",
        seed_count=seed_count, max_depth=max_depth,
    )
    reached_depth = 0
    stopped_at_depth_limit = False

    for depth in range(1, max_depth + 1):
        depth_started = time.monotonic()
        log_progress(
            "traversal_depth_started", mapping_id=mapping_id,
            traversal="directory", depth=depth, max_depth=max_depth,
        )
        next_frontier = (
            frontier.alias("frontier")
            .join(
                edge_frame.alias("edge"),
                functions.col("frontier.ancestor_directory_id")
                == functions.col("edge.child_directory_id"),
                "inner",
            )
            .select(
                functions.col("frontier.seed_directory_id").alias(
                    "seed_directory_id"
                ),
                functions.col("edge.parent_directory_id").alias(
                    "ancestor_directory_id"
                ),
                functions.lit(depth).cast("int").alias("depth"),
                functions.when(
                    functions.col("edge.child_name").isNull()
                    | (functions.trim(functions.col("edge.child_name")) == ""),
                    functions.col("frontier.path_from_ancestor"),
                )
                .when(
                    functions.col("frontier.path_from_ancestor") == "",
                    functions.col("edge.child_name"),
                )
                .otherwise(
                    functions.concat_ws(
                        "/",
                        functions.col("edge.child_name"),
                        functions.col("frontier.path_from_ancestor"),
                    )
                )
                .alias("path_from_ancestor"),
            )
            .dropDuplicates(
                ["seed_directory_id", "ancestor_directory_id", "path_from_ancestor"]
            )
            .join(
                closure.select(
                    "seed_directory_id", "ancestor_directory_id", "path_from_ancestor"
                ),
                ["seed_directory_id", "ancestor_directory_id", "path_from_ancestor"],
                "leftanti",
            )
            .persist(StorageLevel.MEMORY_AND_DISK)
        )
        has_next = next_frontier.limit(1).count() > 0
        log_progress(
            "traversal_depth_completed", mapping_id=mapping_id,
            traversal="directory", depth=depth, max_depth=max_depth,
            has_next_frontier=has_next,
            elapsed_seconds=round(time.monotonic() - depth_started, 1),
        )
        if not has_next:
            next_frontier.unpersist()
            break
        closure = closure.unionByName(next_frontier)
        if checkpoint_dir:
            closure = closure.checkpoint(eager=True).persist(
                StorageLevel.MEMORY_AND_DISK
            )
        frontier.unpersist()
        frontier = next_frontier
        reached_depth = depth
    else:
        stopped_at_depth_limit = True

    frontier.unpersist()
    log_progress(
        "traversal_completed", mapping_id=mapping_id,
        traversal="directory", depth_reached=reached_depth,
        stopped_at_depth_limit=stopped_at_depth_limit,
    )
    return closure, {
        "directory_seed_count": seed_count,
        "directory_closure_count": closure.count(),
        "directory_depth_reached": reached_depth,
        "directory_stopped_at_depth_limit": stopped_at_depth_limit,
    }


def read_revisions(spark, revision_uri):
    from pyspark.sql import functions
    from pyspark.sql import types

    revisions = spark.read.format("orc").load(revision_uri)
    revision_id_column = require_column(
        revisions, ["id", "revision_id", "sha1_git"], "revision ID"
    )
    directory_column = require_column(
        revisions,
        ["directory", "directory_id", "root_directory", "root_directory_id"],
        "revision root directory",
    )
    date_column = find_column(
        revisions,
        ["committer_date", "date", "revision_date", "author_date", "timestamp"],
    )
    selected = [
        _spark_id_expression(revisions, revision_id_column, functions, types).alias(
            "revision_id"
        ),
        _spark_id_expression(revisions, directory_column, functions, types).alias(
            "root_directory_id"
        ),
    ]
    if date_column:
        selected.append(
            functions.col(date_column).cast("string").alias("revision_date")
        )
    else:
        selected.append(functions.lit(None).cast("string").alias("revision_date"))
    return revisions.select(*selected).dropDuplicates(["revision_id"])


def read_snapshot_branches(spark, snapshot_branch_uri):
    from pyspark.sql import functions
    from pyspark.sql import types

    branches = spark.read.format("orc").load(snapshot_branch_uri)
    snapshot_column = require_column(
        branches, ["snapshot_id", "snapshot"], "snapshot branch snapshot ID"
    )
    target_column = require_column(
        branches, ["target", "target_id", "target_revision_id"], "snapshot branch target"
    )
    # Without target_type an alias/directory target can be mistaken for a
    # revision.  Fail closed instead of silently mapping the wrong object.
    type_column = require_column(
        branches,
        ["target_type", "type", "target_kind"],
        "snapshot branch target type",
    )
    name_column = find_column(branches, ["name", "branch_name", "branch"])
    selected = [
        _spark_id_expression(branches, snapshot_column, functions, types).alias(
            "snapshot_id"
        ),
        _spark_id_expression(branches, target_column, functions, types).alias(
            "target_id"
        ),
    ]
    if type_column:
        selected.append(
            functions.lower(
                functions.trim(
                    _spark_text_expression(branches, type_column, functions, types)
                )
            ).alias("target_type")
        )
    # ``type_column`` is required above; this branch is retained only for
    # defensive clarity if the helper is adapted for a non-SWH table.
    else:
        selected.append(functions.lit(None).cast("string").alias("target_type"))
    if name_column:
        selected.append(
            _spark_text_expression(branches, name_column, functions, types).alias(
                "branch_name"
            )
        )
    else:
        selected.append(functions.lit(None).cast("string").alias("branch_name"))
    return branches.select(*selected).dropDuplicates(
        ["snapshot_id", "target_id", "target_type", "branch_name"]
    )


def read_releases(spark, release_uri):
    from pyspark.sql import functions
    from pyspark.sql import types

    releases = spark.read.format("orc").load(release_uri)
    release_id_column = require_column(
        releases, ["id", "release_id", "sha1_git"], "release ID"
    )
    target_column = require_column(
        releases, ["target", "target_id"], "release target"
    )
    target_type_column = require_column(
        releases,
        ["target_type", "type", "target_kind"],
        "release target type",
    )
    selected = [
        _spark_id_expression(releases, release_id_column, functions, types).alias(
            "release_id"
        ),
        _spark_id_expression(releases, target_column, functions, types).alias(
            "release_target_id"
        ),
    ]
    if target_type_column:
        selected.append(
            functions.lower(
                functions.trim(
                    _spark_text_expression(
                        releases, target_type_column, functions, types
                    )
                )
            ).alias("release_target_type")
        )
    return releases.select(*selected).dropDuplicates(["release_id"])


def resolve_revision_branch_targets(branches, releases=None):
    """Convert snapshot branch targets to revision targets.

    Direct revision targets are retained.  Release targets are followed when a
    release table is supplied and the release points at a revision.  Alias and
    directory targets remain visible in the ``unmapped`` output.
    """

    from pyspark.sql import functions

    target_type = functions.lower(functions.trim(functions.col("target_type")))
    direct = branches.where(
        target_type.isin("rev", "revision", "commit")
    ).select(
        "snapshot_id",
        "branch_name",
        functions.col("target_id").alias("branch_tip_revision_id"),
        functions.lit("branch_tip").alias("branch_target_relation"),
    )
    if releases is None:
        return direct.dropDuplicates(
            ["snapshot_id", "branch_name", "branch_tip_revision_id"]
        )

    release_targets = (
        branches.where(target_type.isin("rel", "release"))
        .join(
            releases,
            functions.col("target_id") == functions.col("release_id"),
            "inner",
        )
        .where(
            functions.col("release_target_type").isNull()
            | functions.lower(functions.col("release_target_type")).isin(
                "rev", "revision", "commit"
            )
        )
        .select(
            functions.col("snapshot_id"),
            functions.col("branch_name"),
            functions.col("release_target_id").alias("branch_tip_revision_id"),
            functions.lit("release_target").alias("branch_target_relation"),
        )
    )
    return direct.unionByName(release_targets).dropDuplicates(
        ["snapshot_id", "branch_name", "branch_tip_revision_id"]
    )


def read_revision_history(spark, history_uri):
    from pyspark.sql import functions
    from pyspark.sql import types

    history = spark.read.format("orc").load(history_uri)
    child_column = require_column(
        history, ["revision_id", "id", "child_revision_id"], "revision history child"
    )
    parent_column = require_column(
        history, ["parent_id", "parent", "parent_revision_id"], "revision history parent"
    )
    parent_field = next(field for field in history.schema.fields if field.name == parent_column)
    if isinstance(parent_field.dataType, types.ArrayType):
        exploded = history.select(
            _spark_id_expression(history, child_column, functions, types).alias(
                "child_revision_id"
            ),
            functions.explode_outer(functions.col(parent_column)).alias("_parent_id"),
        )
        parent_value = exploded.schema["_parent_id"].dataType
        if isinstance(parent_value, types.BinaryType):
            parent_expression = functions.lower(functions.hex(functions.col("_parent_id")))
        else:
            parent_expression = functions.lower(
                functions.trim(functions.col("_parent_id").cast("string"))
            )
        return exploded.select(
            "child_revision_id",
            functions.regexp_replace(
                parent_expression, r"^swh:1:[a-z]+:", ""
            ).alias("parent_revision_id"),
        ).where(functions.col("parent_revision_id").isNotNull()).dropDuplicates(
            ["child_revision_id", "parent_revision_id"]
        )
    return history.select(
        _spark_id_expression(history, child_column, functions, types).alias(
            "child_revision_id"
        ),
        _spark_id_expression(history, parent_column, functions, types).alias(
            "parent_revision_id"
        ),
    ).dropDuplicates(["child_revision_id", "parent_revision_id"])


def build_revision_reachability(
    candidate_revisions, tips, history, max_depth, checkpoint_dir=None,
    mapping_id=None,
):
    """Follow only candidate revisions towards descendant snapshot branch tips.

    The state is keyed by candidate and reached revision, so a merge or a
    cycle cannot repeatedly add the same pair.  Branch/snapshot fanout is
    applied only after this candidate-scoped traversal.
    """

    from pyspark import StorageLevel
    from pyspark.sql import functions

    seeds = candidate_revisions.select("revision_id").where(
        functions.col("revision_id").isNotNull()
    ).dropDuplicates()
    reach = seeds.select(
        functions.col("revision_id").alias("candidate_revision_id"),
        functions.col("revision_id").alias("reachable_revision_id"),
        functions.lit(0).cast("int").alias("revision_depth"),
    )
    frontier = reach.persist(StorageLevel.MEMORY_AND_DISK)
    reach = reach.persist(StorageLevel.MEMORY_AND_DISK)
    seed_count = reach.count()
    log_progress(
        "traversal_started", mapping_id=mapping_id, traversal="revision",
        seed_count=seed_count, max_depth=max_depth,
        history_available=history is not None,
    )
    reached_depth = 0
    stopped_at_depth_limit = False

    depths = range(1, max_depth + 1) if history is not None else ()
    for depth in depths:
        depth_started = time.monotonic()
        log_progress(
            "traversal_depth_started", mapping_id=mapping_id,
            traversal="revision", depth=depth, max_depth=max_depth,
        )
        next_frontier = (
            frontier.alias("frontier")
            .join(
                history.alias("history"),
                functions.col("frontier.reachable_revision_id")
                == functions.col("history.parent_revision_id"),
                "inner",
            )
            .select(
                functions.col("frontier.candidate_revision_id").alias(
                    "candidate_revision_id"
                ),
                functions.col("history.child_revision_id").alias(
                    "reachable_revision_id"
                ),
                functions.lit(depth).cast("int").alias("revision_depth"),
            )
            .dropDuplicates(["candidate_revision_id", "reachable_revision_id"])
            .join(
                reach.select(
                    "candidate_revision_id", "reachable_revision_id"
                ),
                ["candidate_revision_id", "reachable_revision_id"],
                "leftanti",
            )
            .persist(StorageLevel.MEMORY_AND_DISK)
        )
        has_next = next_frontier.limit(1).count() > 0
        log_progress(
            "traversal_depth_completed", mapping_id=mapping_id,
            traversal="revision", depth=depth, max_depth=max_depth,
            has_next_frontier=has_next,
            elapsed_seconds=round(time.monotonic() - depth_started, 1),
        )
        if not has_next:
            next_frontier.unpersist()
            break
        previous_reach = reach
        reach = reach.unionByName(next_frontier)
        if checkpoint_dir:
            reach = reach.checkpoint(eager=True).persist(
                StorageLevel.MEMORY_AND_DISK
            )
            reach.count()
            previous_reach.unpersist()
        frontier.unpersist()
        frontier = next_frontier
        reached_depth = depth
    else:
        stopped_at_depth_limit = history is not None
    frontier.unpersist()
    reach_count = reach.count()
    log_progress(
        "traversal_completed", mapping_id=mapping_id,
        traversal="revision", depth_reached=reached_depth,
        stopped_at_depth_limit=stopped_at_depth_limit,
        reach_count=reach_count,
    )
    branch_reach = reach.alias("reach").join(
        tips.alias("tip"),
        functions.col("reach.reachable_revision_id")
        == functions.col("tip.branch_tip_revision_id"),
        "inner",
    ).select(
        functions.col("tip.snapshot_id").alias("snapshot_id"),
        functions.col("tip.branch_name").alias("branch_name"),
        functions.col("tip.branch_tip_revision_id").alias("branch_tip_revision_id"),
        functions.col("reach.candidate_revision_id").alias("revision_id"),
        functions.col("reach.revision_depth").alias("revision_depth"),
    )
    return branch_reach, {
        "revision_reachability_count": reach_count,
        "revision_depth_reached": reached_depth,
        "revision_stopped_at_depth_limit": stopped_at_depth_limit,
        "revision_history_available": history is not None,
        "revision_traversal": "candidate_to_descendant_branch_tip",
    }


def read_origin_visits(spark, status_uri):
    from pyspark.sql import functions
    from pyspark.sql import types

    statuses = spark.read.format("orc").load(status_uri)
    snapshot_column = require_column(
        statuses, ["snapshot_id", "snapshot"], "origin visit snapshot"
    )
    origin_column = find_column(statuses, ["origin_id", "origin"])
    url_column = find_column(statuses, ["origin_url", "url"])
    if origin_column is None and url_column is None:
        raise RuntimeError(
            "origin visit status needs origin_id/origin or origin_url/url"
        )
    visit_column = find_column(statuses, ["visit_id", "visit", "id"])
    date_column = find_column(statuses, ["visit_date", "date", "timestamp"])
    state_column = find_column(statuses, ["status", "visit_status"])
    selected = [
        _spark_id_expression(statuses, snapshot_column, functions, types).alias(
            "snapshot_id"
        )
    ]
    if origin_column:
        selected.append(
            _spark_id_expression(statuses, origin_column, functions, types).alias(
                "origin_id"
            )
        )
    else:
        selected.append(functions.lit(None).cast("string").alias("origin_id"))
    if url_column:
        selected.append(
            _spark_text_expression(statuses, url_column, functions, types).alias(
                "origin_url"
            )
        )
    else:
        selected.append(functions.lit(None).cast("string").alias("origin_url"))
    if visit_column:
        selected.append(
            _spark_text_expression(statuses, visit_column, functions, types).alias(
                "visit_id"
            )
        )
    else:
        selected.append(functions.lit(None).cast("string").alias("visit_id"))
    if date_column:
        selected.append(functions.col(date_column).cast("string").alias("visit_date"))
    else:
        selected.append(functions.lit(None).cast("string").alias("visit_date"))
    if state_column:
        selected.append(
            functions.lower(
                functions.trim(
                    _spark_text_expression(statuses, state_column, functions, types)
                )
            ).alias("visit_status")
        )
    else:
        selected.append(functions.lit(None).cast("string").alias("visit_status"))
    # Keep separate visits even when an export omits visit_id.  Collapsing on
    # only (snapshot, origin, NULL) would discard visit timestamps and status.
    return statuses.select(*selected).dropDuplicates()


def read_origins(spark, origin_uri):
    from pyspark.sql import functions
    from pyspark.sql import types

    origins = spark.read.format("orc").load(origin_uri)
    origin_column = require_column(origins, ["origin_id", "id"], "origin ID")
    url_column = require_column(origins, ["url", "origin_url"], "origin URL")
    return origins.select(
        _spark_id_expression(origins, origin_column, functions, types).alias(
            "origin_id"
        ),
        _spark_text_expression(origins, url_column, functions, types)
        .cast("string")
        .alias("origin_url"),
    ).dropDuplicates(["origin_id"])


def map_candidates_to_origins(
    revision_candidates,
    branches,
    visits,
    origins,
    history=None,
    releases=None,
    max_revision_depth=DEFAULT_MAX_REVISION_DEPTH,
    checkpoint_dir=None,
    mapping_id=None,
):
    """Attach origin metadata to revision candidates.

    ``history`` is optional for a bounded smoke run.  When supplied, an older
    revision is emitted with ``revision_relation=ancestor_of_branch_tip``.
    Without it, only branch tips can be mapped and the returned metrics make
    that limitation explicit.
    """

    from pyspark.sql import functions

    branch_tips = resolve_revision_branch_targets(branches, releases)
    reach, reach_metrics = build_revision_reachability(
        revision_candidates.select("revision_id"),
        branch_tips,
        history,
        max_revision_depth,
        checkpoint_dir=checkpoint_dir,
        mapping_id=mapping_id,
    )
    mapped = (
        revision_candidates.alias("candidate")
        .join(
            reach.alias("reach"),
            functions.col("candidate.revision_id")
            == functions.col("reach.revision_id"),
            "inner",
        )
        .join(
            visits.alias("visit"),
            functions.col("reach.snapshot_id") == functions.col("visit.snapshot_id"),
            "inner",
        )
    )
    if origins is not None:
        mapped = mapped.join(
            origins.alias("origin"),
            functions.col("visit.origin_id") == functions.col("origin.origin_id"),
            "left",
        )
        origin_url_expression = functions.coalesce(
            functions.col("visit.origin_url"), functions.col("origin.origin_url")
        )
    else:
        origin_url_expression = functions.col("visit.origin_url")
    mapped = mapped.select(
        functions.col("candidate.p4_directory_id").alias("p4_directory_id"),
        functions.col("candidate.p4_name").alias("p4_name"),
        functions.col("candidate.p4_content_id").alias("p4_content_id"),
        functions.col("candidate.p4_16_confirmed").alias("p4_16_confirmed"),
        functions.col("candidate.p4_path").alias("p4_path"),
        functions.col("candidate.candidate_source_file").alias(
            "candidate_source_file"
        ),
        functions.col("candidate.root_directory_id").alias("root_directory_id"),
        functions.col("candidate.revision_id").alias("revision_id"),
        functions.col("candidate.revision_date").alias("revision_date"),
        functions.col("reach.snapshot_id").alias("snapshot_id"),
        functions.col("reach.branch_name").alias("branch_name"),
        functions.col("reach.branch_tip_revision_id").alias("branch_tip_revision_id"),
        functions.col("reach.revision_depth").alias("revision_depth"),
        functions.when(
            functions.col("reach.revision_depth") == functions.lit(0),
            functions.lit("branch_tip"),
        )
        .otherwise(functions.lit("ancestor_of_branch_tip"))
        .alias("revision_relation"),
        functions.col("visit.origin_id").alias("origin_id"),
        origin_url_expression.alias("origin_url"),
        functions.col("visit.visit_id").alias("visit_id"),
        functions.col("visit.visit_date").alias("visit_date"),
        functions.col("visit.visit_status").alias("visit_status"),
    ).dropDuplicates(
        [
            "p4_directory_id",
            "p4_name",
            "p4_content_id",
            "p4_path",
            "revision_id",
            "snapshot_id",
            "origin_id",
            "visit_id",
            "branch_name",
        ]
    )
    return mapped, reach_metrics


def attach_revision_roots(seed_hits, closure, revisions):
    """Join each hit directory to every revision whose root contains it."""

    from pyspark.sql import functions

    return (
        seed_hits.alias("hit")
        .join(
            closure.alias("closure"),
            functions.col("hit.directory_id")
            == functions.col("closure.seed_directory_id"),
            "inner",
        )
        .join(
            revisions.alias("revision"),
            functions.col("closure.ancestor_directory_id")
            == functions.col("revision.root_directory_id"),
            "inner",
        )
        .select(
            functions.col("hit.directory_id").alias("p4_directory_id"),
            functions.col("hit.name").alias("p4_name"),
            functions.col("hit.content_id").alias("p4_content_id"),
            functions.col("hit.p4_16_confirmed").alias("p4_16_confirmed"),
            functions.when(
                functions.col("closure.path_from_ancestor") == "",
                functions.col("hit.name"),
            )
            .otherwise(
                functions.concat_ws(
                    "/",
                    functions.col("closure.path_from_ancestor"),
                    functions.col("hit.name"),
                )
            )
            .alias("p4_path"),
            functions.col("hit.candidate_source_file").alias(
                "candidate_source_file"
            ),
            functions.col("revision.root_directory_id").alias("root_directory_id"),
            functions.col("revision.revision_id").alias("revision_id"),
            functions.col("revision.revision_date").alias("revision_date"),
            functions.col("closure.depth").alias("directory_depth"),
        )
        .dropDuplicates(
            ["p4_directory_id", "p4_name", "p4_content_id", "revision_id", "p4_path"]
        )
    )


def add_latest_verified_candidates(frame):
    """Rank verified candidates without pretending unverified rows are valid."""

    from pyspark.sql import Window
    from pyspark.sql import functions

    confirmed = functions.lower(functions.trim(functions.col("p4_16_confirmed")))
    with_status = frame.withColumn(
        "selection_status",
        functions.when(confirmed.isin("1", "true", "yes", "compiled"), "verified")
        .otherwise("awaiting_p4_16_verification"),
    )
    verified = with_status.where(functions.col("selection_status") == "verified")
    if verified.limit(1).count() == 0:
        return with_status.withColumn("origin_rank", functions.lit(None).cast("int"))
    order_date = functions.coalesce(
        functions.to_timestamp("revision_date"),
        functions.to_timestamp("visit_date"),
    )
    origin_partition = functions.coalesce(
        functions.col("origin_id"), functions.col("origin_url")
    )
    revision_window = Window.partitionBy(origin_partition, "revision_id")
    verified = verified.withColumn(
        "_revision_order_date", functions.max(order_date).over(revision_window)
    )
    window = Window.partitionBy(origin_partition).orderBy(
        functions.col("_revision_order_date").desc_nulls_last(),
        functions.col("revision_id").desc(),
    )
    # Multiple .p4 paths and snapshot branches can describe the same revision.
    # dense_rank keeps every row belonging to the newest revision at rank 1;
    # row_number would arbitrarily retain only one path.
    ranked = verified.withColumn("origin_rank", functions.dense_rank().over(window)).drop(
        "_revision_order_date"
    )
    unverified = with_status.where(functions.col("selection_status") != "verified").withColumn(
        "origin_rank", functions.lit(None).cast("int")
    )
    return ranked.unionByName(unverified, allowMissingColumns=True)


def build_repository_candidates(mapped):
    """Make a download-friendly, unique origin URL list without a P4_16 gate."""

    from pyspark.sql import functions

    url = functions.trim(functions.col("origin_url"))
    clean_path = functions.regexp_replace(
        functions.regexp_replace(url, r"/+$", ""), r"(?i)\.git$", ""
    )
    return (
        mapped.select(url.alias("origin_url"))
        .where(functions.col("origin_url").isNotNull() & (functions.col("origin_url") != ""))
        .dropDuplicates(["origin_url"])
        .withColumn("repository_name", functions.regexp_extract(clean_path, r"([^/]+)$", 1))
        .withColumn(
            "origin_is_github",
            functions.lower(functions.col("origin_url")).rlike(
                r"^https?://(www\.)?github\.com/"
            ),
        )
    )


def write_json_metrics(spark, output_uri, values):
    metrics = {key: (None if value is None else str(value)) for key, value in values.items()}
    write_json_file(spark, join_uri(output_uri, "{}.json".format(uuid.uuid4().hex)), metrics)


def load_manifest_shards(spark, config):
    manifest_uri = config.get("candidate_manifest_uri")
    if not manifest_uri:
        return None
    manifest_rows = spark.read.option("multiLine", "true").json(manifest_uri).take(2)
    if len(manifest_rows) != 1:
        raise RuntimeError("candidate manifest must contain one JSON object")
    return select_manifest_shards(
        manifest_rows[0].asDict(recursive=True), config["candidate_shard_indices"]
    )


def run_mapping(spark, config):
    """Run a mapping ID through durable, fingerprinted Spark stages."""

    from pyspark.sql import functions

    output_uri = config["output_uri"].rstrip("/")
    run_uri = join_uri(output_uri, "runs/{}".format(config["run_id"]))
    resume = config.get("resume", False)
    if config.get("checkpoint_uri"):
        spark.sparkContext.setCheckpointDir(config["checkpoint_uri"])
    resumed_stages = []

    def stage(name, fingerprint, build, output_format="parquet"):
        frame, stage_metrics, was_resumed = materialize_stage(
            spark, run_uri, name, fingerprint, build, resume, output_format,
            mapping_id=config["run_id"],
            progress_interval_seconds=config.get(
                "progress_interval_seconds", DEFAULT_PROGRESS_INTERVAL_SECONDS
            ),
        )
        if was_resumed:
            resumed_stages.append(name)
        return frame, stage_metrics

    shards = load_manifest_shards(spark, config)
    if shards is not None:
        shard_uris = []
        for shard_number, shard in enumerate(shards, 1):
            name = "normalized_hit_shards/{:05d}".format(shard["index"])
            fingerprint = stable_fingerprint({"version": MAPPER_VERSION, "shard": shard})

            def build_shard(uri=shard["uri"]):
                frame = normalize_candidate_frame(spark, uri, "csv").persist()
                count = frame.count()
                return frame, {"candidate_hit_count": count}

            stage(name, fingerprint, build_shard)
            log_progress(
                "candidate_shards_progress", mapping_id=config["run_id"],
                shards_completed=shard_number, shards_total=len(shards),
                shard_index=shard["index"],
                percent=round(100.0 * shard_number / len(shards), 2),
            )
            record = read_stage_record(spark, run_uri, name)
            shard_uris.append(record.get("data_uri", join_uri(run_uri, name)))
        input_identity = {"manifest": config["candidate_manifest_uri"], "shards": shards}

        def build_hits():
            frame = spark.read.parquet(*shard_uris).dropDuplicates(
                ["directory_id", "name", "content_id"]
            ).persist()
            count = frame.count()
            if count == 0:
                raise RuntimeError("selected candidate shards contain no .p4 file rows")
            return frame, {"candidate_hit_count": count}

    else:
        input_identity = {
            "candidate_input": config["candidate_input"],
            "candidate_format": config.get("candidate_format", "auto"),
        }

        def build_hits():
            frame = normalize_candidate_frame(
                spark, config["candidate_input"], config.get("candidate_format", "auto")
            ).persist()
            count = frame.count()
            if count == 0:
                raise RuntimeError("candidate input contains no .p4 file rows")
            return frame, {"candidate_hit_count": count}

    fingerprint = stable_fingerprint({
        "version": MAPPER_VERSION,
        "graph_release": config.get("graph_release", GRAPH_RELEASE),
        "input": input_identity,
        "directory_entry_uri": config["directory_entry_uri"],
        "revision_uri": config["revision_uri"],
        "revision_history_uri": config.get("revision_history_uri"),
        "snapshot_branch_uri": config["snapshot_branch_uri"],
        "origin_visit_status_uri": config["origin_visit_status_uri"],
        "origin_uri": config.get("origin_uri"),
        "release_uri": config.get("release_uri"),
        "max_directory_depth": config["max_directory_depth"],
        "max_revision_depth": config["max_revision_depth"],
    })
    hits, hit_metrics = stage("normalized_hits", fingerprint, build_hits)
    hit_count = hit_metrics["candidate_hit_count"]
    if config.get("stop_after_stage") == "normalized_hits":
        metrics = {
            "schema_version": 1,
            "run_id": config["run_id"],
            "status": "partial",
            "last_completed_stage": "normalized_hits",
            "candidate_hit_count": hit_count,
            "mapping_fingerprint": fingerprint,
            "resumed_stages": ",".join(resumed_stages),
        }
        write_json_metrics(spark, join_uri(run_uri, "metrics"), metrics)
        return metrics

    def build_closure():
        edges = read_directory_edges(spark, config["directory_entry_uri"]).persist()
        return build_directory_closure(
            hits.select("directory_id"),
            edges,
            config["max_directory_depth"],
            checkpoint_dir=config.get("checkpoint_uri"),
            mapping_id=config["run_id"],
        )

    closure, closure_metrics = stage("directory_closure", fingerprint, build_closure)

    def build_revisions():
        revisions = read_revisions(spark, config["revision_uri"])
        frame = attach_revision_roots(hits, closure, revisions).persist()
        return frame, {"revision_candidate_count": frame.count()}

    revision_candidates, revision_metrics = stage(
        "revision_candidates", fingerprint, build_revisions
    )
    revision_candidate_count = revision_metrics["revision_candidate_count"]

    def build_mapped():
        branches = read_snapshot_branches(spark, config["snapshot_branch_uri"])
        releases = (
            read_releases(spark, config["release_uri"])
            if config.get("release_uri") else None
        )
        visits = read_origin_visits(spark, config["origin_visit_status_uri"])
        origins = (
            read_origins(spark, config["origin_uri"])
            if config.get("origin_uri") else None
        )
        history = (
            read_revision_history(spark, config["revision_history_uri"])
            if config.get("revision_history_uri") else None
        )
        frame, reach_metrics = map_candidates_to_origins(
            revision_candidates,
            branches,
            visits,
            origins,
            history=history,
            releases=releases,
            max_revision_depth=config["max_revision_depth"],
            checkpoint_dir=config.get("checkpoint_uri"),
            mapping_id=config["run_id"],
        )
        frame = frame.withColumn(
            "origin_is_github",
            functions.lower(functions.col("origin_url")).rlike(
                r"^https?://(www\.)?github\.com/"
            ),
        )
        return add_latest_verified_candidates(frame), reach_metrics

    mapped, reach_metrics = stage("origin_revision_candidates", fingerprint, build_mapped)
    mapped_count = mapped.count()

    def build_repositories():
        frame = build_repository_candidates(mapped).persist()
        return frame, {"repository_candidate_count": frame.count()}

    repositories, repository_metrics = stage(
        "repository_candidates", fingerprint, build_repositories
    )
    repository_count = repository_metrics["repository_candidate_count"]
    stage(
        "repository_candidates_csv", fingerprint,
        lambda: (repositories, {"repository_candidate_count": repository_count}),
        output_format="csv",
    )

    # Keep a schema compatible with collect_swh_p4.py.  The graph mapper keeps
    # all origins in its primary output; the existing collector may apply its
    # GitHub-only policy downstream.  ``directory_id`` is deliberately the
    # revision root here, never the direct parent directory of the .p4 file.
    def build_collector_candidates():
        frame = (
            mapped.where(
                functions.col("origin_url").isNotNull()
                & functions.col("revision_id").isNotNull()
                & functions.col("root_directory_id").isNotNull()
            )
            .select(
                "origin_id",
                "origin_url",
                "revision_id",
                functions.col("root_directory_id").alias("directory_id"),
                "branch_name",
                "visit_id",
                "visit_date",
                "revision_date",
                "snapshot_id",
                "origin_is_github",
                "selection_status",
                "origin_rank",
            )
            .dropDuplicates(["origin_url", "revision_id", "directory_id"])
        )
        return frame, {"collector_candidate_count": frame.count()}

    collector_candidates, collector_metrics = stage(
        "collector_candidates", fingerprint, build_collector_candidates
    )

    def build_latest_verified():
        frame = collector_candidates.where(
            functions.col("origin_rank") == functions.lit(1)
        )
        return frame, {"latest_verified_candidate_count": frame.count()}

    latest_verified, latest_metrics = stage(
        "latest_verified_candidates", fingerprint, build_latest_verified
    )

    def build_unmapped():
        mapped_keys = mapped.select(
            "p4_directory_id", "p4_name", "p4_content_id", "p4_path", "revision_id"
        ).dropDuplicates()
        frame = revision_candidates.join(
            mapped_keys,
            ["p4_directory_id", "p4_name", "p4_content_id", "p4_path", "revision_id"],
            "leftanti",
        ).withColumn("unmapped_reason", functions.lit("no_snapshot_origin_mapping"))
        return frame, {"unmapped_revision_candidate_count": frame.count()}

    unmapped, unmapped_metrics = stage(
        "unmapped_revision_candidates", fingerprint, build_unmapped
    )

    metrics = {
        "schema_version": 1,
        "graph_release": config.get("graph_release", GRAPH_RELEASE),
        "run_id": config["run_id"],
        "created_at": utc_now(),
        "candidate_hit_count": hit_count,
        "revision_candidate_count": revision_candidate_count,
        "mapped_origin_candidate_count": mapped_count,
        "repository_candidate_count": repository_count,
        "collector_candidate_count": collector_metrics["collector_candidate_count"],
        "latest_verified_candidate_count": latest_metrics["latest_verified_candidate_count"],
        "unmapped_revision_candidate_count": unmapped_metrics["unmapped_revision_candidate_count"],
        "mapping_scope": "github_and_non_github_origins; filter origin_is_github downstream",
        "mapping_fingerprint": fingerprint,
        "resumed_stages": ",".join(resumed_stages),
        "status": "complete",
        "last_completed_stage": "unmapped_revision_candidates",
    }
    metrics.update(closure_metrics)
    metrics.update(reach_metrics)
    write_json_metrics(spark, join_uri(run_uri, "metrics"), metrics)
    return metrics


def _glue_config(argv):
    from awsglue.utils import getResolvedOptions

    required = [
        "JOB_NAME",
        "OUTPUT_ROOT",
        "DIRECTORY_ENTRY_URI",
        "REVISION_URI",
        "ORIGIN_URI",
        "SNAPSHOT_BRANCH_URI",
        "ORIGIN_VISIT_STATUS_URI",
        "CONFIRM_FULL_SCAN",
    ]
    args = getResolvedOptions(argv, required)
    job_run_id = get_cli_argument(argv, "JOB_RUN_ID", "manual-{}".format(os.getpid()))
    release_uri = get_cli_argument(argv, "RELEASE_URI")
    origin_uri = args["ORIGIN_URI"]
    history_uri = get_cli_argument(argv, "REVISION_HISTORY_URI")
    checkpoint_uri = get_cli_argument(argv, "CHECKPOINT_URI")
    candidate_manifest_uri = get_cli_argument(argv, "CANDIDATE_MANIFEST_URI")
    candidate_input = get_cli_argument(argv, "CANDIDATE_INPUT")
    if bool(candidate_manifest_uri) == bool(candidate_input):
        raise ValueError("provide exactly one of CANDIDATE_MANIFEST_URI or CANDIDATE_INPUT")
    mapping_id = get_cli_argument(argv, "MAPPING_ID", job_run_id)
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,80}", mapping_id):
        raise ValueError("MAPPING_ID must contain 1-80 letters, digits, underscores or hyphens")
    resume = parse_bool(get_cli_argument(argv, "RESUME", "false"))
    if resume and get_cli_argument(argv, "MAPPING_ID") is None:
        raise ValueError("RESUME=true requires an explicit stable MAPPING_ID")
    stop_after_stage = get_cli_argument(argv, "STOP_AFTER_STAGE")
    if stop_after_stage not in (None, "normalized_hits"):
        raise ValueError("STOP_AFTER_STAGE currently supports normalized_hits only")
    if not parse_bool(args["CONFIRM_FULL_SCAN"]):
        raise ValueError(
            "CONFIRM_FULL_SCAN=true is required; this job reads the public Graph ORC"
        )
    return {
        "job_name": args["JOB_NAME"],
        "run_id": mapping_id,
        "candidate_input": candidate_input,
        "candidate_manifest_uri": candidate_manifest_uri,
        "candidate_shard_indices": (
            parse_shard_indices(get_cli_argument(argv, "SHARD_INDICES"))
            if candidate_manifest_uri else None
        ),
        "candidate_format": get_cli_argument(argv, "CANDIDATE_FORMAT", "auto"),
        "resume": resume,
        "stop_after_stage": stop_after_stage,
        "output_uri": args["OUTPUT_ROOT"],
        "directory_entry_uri": args["DIRECTORY_ENTRY_URI"],
        "revision_uri": args["REVISION_URI"],
        "snapshot_branch_uri": args["SNAPSHOT_BRANCH_URI"],
        "origin_visit_status_uri": args["ORIGIN_VISIT_STATUS_URI"],
        "origin_uri": origin_uri,
        "release_uri": release_uri,
        "revision_history_uri": history_uri,
        "checkpoint_uri": checkpoint_uri,
        "progress_interval_seconds": positive_int(
            get_cli_argument(
                argv, "PROGRESS_INTERVAL_SECONDS", DEFAULT_PROGRESS_INTERVAL_SECONDS
            ),
            "PROGRESS_INTERVAL_SECONDS",
        ),
        "max_directory_depth": positive_int(
            get_cli_argument(argv, "MAX_DIRECTORY_DEPTH", DEFAULT_MAX_DIRECTORY_DEPTH),
            "MAX_DIRECTORY_DEPTH",
        ),
        "max_revision_depth": positive_int(
            get_cli_argument(argv, "MAX_REVISION_DEPTH", DEFAULT_MAX_REVISION_DEPTH),
            "MAX_REVISION_DEPTH",
        ),
        "graph_release": get_cli_argument(argv, "GRAPH_RELEASE", GRAPH_RELEASE),
    }


def main(argv=None):
    argv = sys.argv if argv is None else argv
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from pyspark.context import SparkContext

    config = _glue_config(argv)
    spark_context = SparkContext.getOrCreate()
    glue_context = GlueContext(spark_context)
    spark = glue_context.spark_session
    spark.conf.set("spark.sql.orc.filterPushdown", "true")
    spark.conf.set("spark.sql.orc.impl", "native")
    job = Job(glue_context)
    job.init(config["job_name"], {"mapping_id": config["run_id"]})
    started_at = time.monotonic()
    started_at_utc = utc_now()
    log_progress(
        "mapping_started", mapping_id=config["run_id"],
        started_at_utc=started_at_utc, resume=config["resume"],
        selected_shards=(
            "all" if config["candidate_shard_indices"] is None
            else config["candidate_shard_indices"]
        ),
    )
    try:
        metrics = run_mapping(spark, config)
        job.commit()
    except BaseException as exc:
        log_progress(
            "mapping_failed", mapping_id=config["run_id"],
            started_at_utc=started_at_utc,
            elapsed_seconds=round(time.monotonic() - started_at, 1),
            error="{}: {}".format(type(exc).__name__, exc),
        )
        raise
    log_progress(
        "mapping_completed", mapping_id=config["run_id"],
        started_at_utc=started_at_utc,
        elapsed_seconds=round(time.monotonic() - started_at, 1),
        metrics=metrics,
    )
    print(json.dumps(metrics, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    main()
