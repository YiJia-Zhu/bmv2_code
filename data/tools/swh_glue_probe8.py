#!/usr/bin/env python3
"""Find evidenced origins for eight directory probes, with durable scan batches.

This is a first-origin probe, not an exhaustive enumeration of every origin.
The traversal engine also runs against an in-memory scanner in offline tests.
All public reads are ORC/S3 reads; no Software Heritage HTTP API is used.
"""

import base64
import csv
import datetime as dt
import hashlib
import io
import json
import re
import sys
import time


VERSION = "probe8-v1.1"
TABLE_COLUMNS = {
    "directory_entry": ["directory_id", "name", "type", "target"],
    "revision": ["id", "directory"],
    "revision_history": ["id", "parent_id", "parent_rank"],
    "release": ["id", "target", "target_type"],
    "snapshot_branch": ["snapshot_id", "name", "target", "target_type"],
    "origin_visit_status": ["origin", "visit", "date", "status", "snapshot", "type"],
    "origin": ["id", "url"],
}
KINDS = {"rev": "revision", "commit": "revision", "dir": "directory", "rel": "release"}
SHA1_RE = re.compile(r"^[0-9a-f]{40}$")


def utc_now():
    return dt.datetime.now(dt.timezone.utc).isoformat()


def fingerprint(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True).encode("utf-8")).hexdigest()


def json_safe(value):
    if isinstance(value, (bytes, bytearray)):
        return {"base64": base64.b64encode(value).decode("ascii")}
    if isinstance(value, (dt.datetime, dt.date)):
        return value.isoformat()
    if isinstance(value, dict):
        return {key: json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_safe(item) for item in value]
    return value


def kind(value):
    return KINDS.get(value, value)


def uri_parts(uri):
    if not uri.startswith("s3://"):
        raise ValueError("expected s3 URI")
    return uri[5:].split("/", 1)


def log(event, **fields):
    fields.update(event=event, timestamp_utc=utc_now())
    print(json.dumps(json_safe(fields), sort_keys=True), flush=True)


class Store(object):
    def __init__(self, client, root):
        self.client = client
        self.root = root.rstrip("/")

    def read(self, uri):
        bucket, key = uri_parts(uri)
        try:
            response = self.client.get_object(Bucket=bucket, Key=key)
        except self.client.exceptions.NoSuchKey:
            return None
        with response["Body"] as body:
            return json.load(body)

    def put(self, name, value):
        self.put_text(name, json.dumps(json_safe(value), ensure_ascii=False, indent=2),
                      "application/json")

    def put_text(self, name, text, content_type):
        bucket, key = uri_parts(self.root + "/" + name)
        self.client.put_object(Bucket=bucket, Key=key, Body=text.encode("utf-8"),
                               ContentType=content_type)


class Scanner(object):
    """One predicate per query; commit each source batch after output is durable."""

    def __init__(self, spark, store, inventory, config):
        self.spark = spark
        self.store = store
        self.inventory = inventory
        self.config = config
        self.coverage = {}
        self.max_rows = config.get("max_driver_rows", 500000)
        self.predicates = {}
        self.index_frames = {}

    def lookup_index(self, table, column, values):
        """Build only configured small-table indexes, once, then prune hash buckets.

        Directory entries are deliberately excluded: their full reverse index would
        be much larger than the budget for this probe. Snapshot branches use bounded
        source batches so a failed attempt only redoes its uncommitted batch.
        """
        from pyspark.sql import functions as F
        from swh_glue_graph_mapper import StageProgress

        spec = self.config.get("lookup_indexes", {}).get(table)
        if spec is None or spec["column"] != column:
            return None
        if table == "directory_entry":
            raise ValueError("full directory reverse index is outside this probe")
        if not all(SHA1_RE.fullmatch(value) for value in values):
            raise ValueError("hash lookup index requires SHA-1 values")
        key = (table, column)
        predicates = self.predicates.setdefault(key, set())
        predicates.add(fingerprint(values))
        objects = self.inventory["tables"][table]["objects"]
        identity = {"version": VERSION, "table": table, "column": column,
                    "prefix_length": spec.get("prefix_length", 3), "objects": objects}
        if table == "snapshot_branch":
            # The previous full-table shuffle exhausted Glue's local disks.
            # A new identity keeps its uncommitted output out of this index.
            identity.update(prefix_length=2, layout="source-batches-v2")
        index_id = fingerprint(identity)
        base = "lookup-indexes/{}/{}".format(table, index_id)
        marker = self.store.read(self.store.root + "/" + base + "/complete.json")
        if marker is None and len(predicates) < spec.get("after_distinct_predicates", 2):
            return None
        data_uri = self.store.root + "/" + base + "/data"
        if marker is None:
            started = time.monotonic()
            self.store.put("current-scan.json", {"phase": "building_lookup_index", "table": table,
                                                "started_at": utc_now(), "index_id": index_id})
            monitor = StageProgress(self.spark, "index/" + table, "probe8", 30)
            monitor.start()
            try:
                if table == "snapshot_branch":
                    batch_files = spec.get("index_batch_files", 4)
                    if batch_files < 1:
                        raise ValueError("index_batch_files must be positive")
                    for offset in range(0, len(objects), batch_files):
                        batch = objects[offset:offset + batch_files]
                        batch_name = base + "/batch-{:03d}/complete.json".format(offset)
                        batch_uri = self.store.root + "/" + batch_name
                        committed = self.store.read(batch_uri)
                        if committed is not None:
                            if committed.get("index_id") != index_id or committed.get("objects") != batch:
                                raise RuntimeError("index batch checkpoint mismatch")
                            log("lookup_index_batch_resumed", table=table, index_id=index_id,
                                batch_offset=offset, source_files_complete=min(offset + len(batch), len(objects)))
                            continue
                        paths = ["s3://softwareheritage/" + item["key"] for item in batch]
                        frame = self.spark.read.orc(paths).select(
                            *[F.col(name) for name in TABLE_COLUMNS[table]],
                            F.input_file_name().alias("source_orc"))
                        frame = frame.where(F.col(column).rlike("^[0-9a-f]{40}$"))
                        frame = frame.withColumn("lookup_bucket", F.substring(
                            F.col(column), 1, identity["prefix_length"]))
                        batch_data_uri = data_uri + "/batch={:03d}".format(offset)
                        frame.repartition(spec.get("shuffle_partitions", 128), "lookup_bucket").write.mode(
                            "overwrite").partitionBy("lookup_bucket").parquet(batch_data_uri)
                        self.store.put(batch_name, {"index_id": index_id, "objects": batch,
                                                    "data_uri": batch_data_uri, "completed_at": utc_now()})
                        log("lookup_index_batch_committed", table=table, index_id=index_id,
                            batch_offset=offset, source_files_complete=min(offset + len(batch), len(objects)),
                            source_files_total=len(objects), elapsed_seconds=round(time.monotonic() - started, 2))
                else:
                    paths = ["s3://softwareheritage/" + item["key"] for item in objects]
                    frame = self.spark.read.orc(paths).select(
                        *[F.col(name) for name in TABLE_COLUMNS[table]],
                        F.input_file_name().alias("source_orc"))
                    frame = frame.withColumn("lookup_bucket", F.substring(
                        F.col(column), 1, identity["prefix_length"]))
                    frame.repartition(spec.get("shuffle_partitions", 512), "lookup_bucket").write.mode(
                        "overwrite").partitionBy("lookup_bucket").parquet(data_uri)
                marker = dict(identity, index_id=index_id, completed_at=utc_now(), data_uri=data_uri,
                              build_seconds=round(time.monotonic() - started, 2))
                self.store.put(base + "/complete.json", marker)
                monitor.stop("completed", {"source_files": len(objects), "index_uri": data_uri})
                log("lookup_index_committed", **marker)
            except Exception as exc:
                monitor.stop("failed", error=str(exc))
                raise
        if key not in self.index_frames:
            # Reuse the file listing in the DataFrame; Spark prunes these partitions
            # for every subsequent predicate instead of reopening all public ORCs.
            self.index_frames[key] = self.spark.read.parquet(data_uri)
        buckets = sorted(set(value[:identity["prefix_length"]] for value in values))
        log("lookup_index_query", table=table, index_id=index_id, bucket_count=len(buckets),
            values_count=len(values), index_uri=data_uri)
        return self.index_frames[key].where(F.col("lookup_bucket").isin(buckets))

    def scan(self, table, column, values, label, allowed_types=None):
        from pyspark.sql import functions as F
        from swh_glue_graph_mapper import StageProgress

        values = sorted(set(values))
        if not values:
            return []
        if len(values) > self.max_rows:
            raise RuntimeError("frontier too large; checkpoint retained, not a negative result")
        objects = self.inventory["tables"][table]["objects"]
        query = {"version": VERSION, "table": table, "column": column,
                 "values": values, "allowed_types": allowed_types, "objects": objects}
        query_id = fingerprint(query)
        query_root = "scans/{}/{}".format(table, query_id)
        batch_size = self.config.get("directory_batch_files", 4) if table == "directory_entry" else 128
        rows = []
        started = time.monotonic()
        coverage = {"table": table, "query_id": query_id, "label": label,
                    "files_total": len(objects), "files_complete": 0,
                    "object_bytes_covered": 0, "output_rows": 0,
                    "query": {key: value for key, value in query.items() if key != "objects"}}
        self.coverage[query_id] = coverage
        indexed_frame = None
        for offset in range(0, len(objects), batch_size):
            batch = objects[offset:offset + batch_size]
            base = query_root + "/batch-{:03d}".format(offset)
            marker_uri = self.store.root + "/" + base + "/complete.json"
            marker = self.store.read(marker_uri)
            data_uri = self.store.root + "/" + base + "/data"
            if marker is not None and marker["query_id"] != query_id:
                raise RuntimeError("checkpoint fingerprint mismatch")
            if marker is None:
                self.store.put("current-scan.json", dict(coverage, batch_offset=offset,
                                                        started_at=utc_now()))
                monitor = StageProgress(self.spark, label + "/{}".format(offset),
                                        "probe8", 30)
                monitor.start()
                try:
                    if indexed_frame is None:
                        indexed_frame = self.lookup_index(table, column, values)
                    if indexed_frame is None:
                        paths = ["s3://softwareheritage/" + item["key"] for item in batch]
                        frame = self.spark.read.orc(paths).withColumn("source_orc", F.input_file_name())
                    else:
                        # Configured index tables use a single 128-file query batch.
                        frame = indexed_frame
                    predicate = F.col(column).isin(values)
                    if allowed_types:
                        predicate = predicate & F.col("type" if table == "directory_entry"
                                                       else "target_type").isin(allowed_types)
                    frame = frame.where(predicate).select(
                        *[F.col(name) for name in TABLE_COLUMNS[table]],
                        F.col("source_orc"))
                    # Shuffle AFTER filtering, so all input partitions read in parallel.
                    frame.repartition(1).write.mode("overwrite").parquet(data_uri)
                    count = self.spark.read.parquet(data_uri).count()
                    marker = {"query_id": query_id, "completed_at": utc_now(),
                              "objects": batch, "output_rows": count, "data_uri": data_uri}
                    self.store.put(base + "/complete.json", marker)
                    monitor.stop("completed", {"output_rows": count, "source_files": len(batch)})
                except Exception as exc:
                    monitor.stop("failed", error=str(exc))
                    raise
            else:
                log("scan_batch_resumed", table=table, batch_offset=offset, query_id=query_id)
            if len(rows) + marker["output_rows"] > self.max_rows:
                raise RuntimeError("filtered output exceeds driver bound; retained at " + data_uri)
            rows.extend(json_safe(row.asDict()) for row in self.spark.read.parquet(data_uri).collect())
            coverage["files_complete"] += len(batch)
            coverage["object_bytes_covered"] += sum(item["size"] for item in batch)
            coverage["output_rows"] += marker["output_rows"]
            elapsed = time.monotonic() - started
            remaining = len(objects) - coverage["files_complete"]
            # Coverage ETA includes resumed batches; task ETA is the live useful estimate on resume.
            eta = elapsed * remaining / coverage["files_complete"]
            log("scan_coverage", **dict(coverage, elapsed_seconds=round(elapsed, 2),
                                        coverage_eta_seconds=round(eta),
                                        byte_meaning="full_object_sizes_not_actual_read_bytes"))
            self.store.put("coverage.json", self.coverage)
        self.store.put(query_root + "/complete.json", coverage)
        return rows


def node_key(node):
    return (node["seed_directory_id"], node.get("candidate_revision_id"),
            node["object_type"], node["object_id"])


class Traversal(object):
    def __init__(self, scanner, store, config):
        self.scanner = scanner
        self.store = store
        self.config = config
        self.seeds = config["seeds"]
        self.results = []
        self.result_keys = set()
        self.resolved = set()
        self.reasons = {}
        self.started_at = utc_now()

    def progress(self, phase, **extra):
        status = {"version": VERSION, "started_at": self.started_at, "updated_at": utc_now(),
                  "phase": phase, "resolved_seeds": len(self.resolved), "total_seeds": len(self.seeds),
                  "evidence_rows": len(self.results), "origin_urls": len(set(
                      row["origin_url"] for row in self.results)),
                  "scope": "at_least_one_evidenced_origin_per_seed_not_exhaustive_all_origins",
                  "seeds": [{"directory_id": seed["directory_id"],
                             "status": "mapped" if seed["directory_id"] in self.resolved else
                             self.reasons.get(seed["directory_id"], "pending")}
                            for seed in self.seeds]}
        status.update(extra)
        self.store.put("status.json", status)
        self.store.put("origin-evidence.json", self.results)
        output = io.StringIO()
        columns = ["seed_directory_id", "origin_url", "origin_id", "candidate_revision_id",
                   "root_directory_id", "directory_depth", "object_type", "object_id",
                   "snapshot_id", "visit", "visit_date", "visit_status", "branch_name_base64",
                   "revision_history_depth", "release_depth"]
        writer = csv.DictWriter(output, columns, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(self.results)
        self.store.put_text("origin-mapping.csv", output.getvalue(), "text/csv")
        urls = sorted(set(row["origin_url"] for row in self.results))
        self.store.put_text("repository-urls.txt", "\n".join(urls) + ("\n" if urls else ""), "text/plain")
        log("probe_progress", **status)
        return status

    def find_origins(self, initial_nodes, label):
        nodes = {node_key(node): node for node in initial_nodes}
        frontier = list(nodes.values())
        release_depth = 0
        while frontier:
            if release_depth >= self.config.get("max_release_depth", 128):
                raise RuntimeError("release depth limit reached; probe remains incomplete")
            releases = self.scanner.scan("release", "target", [n["object_id"] for n in frontier],
                                         label + "/release-{}".format(release_depth))
            next_frontier = []
            by_target = {}
            for node in frontier:
                by_target.setdefault((node["object_type"], node["object_id"]), []).append(node)
            for release in releases:
                for node in by_target.get((kind(release["target_type"]), release["target"]), []):
                    child = dict(node, object_type="release", object_id=release["id"],
                                 release_depth=node.get("release_depth", 0) + 1,
                                 release_chain=node.get("release_chain", []) + [release])
                    if node_key(child) not in nodes:
                        nodes[node_key(child)] = child
                        next_frontier.append(child)
            frontier = next_frontier
            release_depth += 1
        branches = self.scanner.scan("snapshot_branch", "target", [n["object_id"] for n in nodes.values()],
                                     label + "/snapshot", ["revision", "rev", "commit", "release", "rel", "directory", "dir"])
        by_target = {}
        for node in nodes.values():
            by_target.setdefault((node["object_type"], node["object_id"]), []).append(node)
        matched_branches = []
        for branch in branches:
            for node in by_target.get((kind(branch["target_type"]), branch["target"]), []):
                matched_branches.append((node, branch))
        visits = self.scanner.scan("origin_visit_status", "snapshot", [b["snapshot_id"] for _, b in matched_branches],
                                   label + "/visits")
        # The export normally uses origin IDs here. Support URL-valued exports too.
        refs = sorted(set(v["origin"] for v in visits))
        origin_rows = self.scanner.scan("origin", "id", [r for r in refs if SHA1_RE.fullmatch(r)],
                                        label + "/origin-id")
        url_refs = [r for r in refs if not SHA1_RE.fullmatch(r)]
        origin_rows += self.scanner.scan("origin", "url", url_refs, label + "/origin-url")
        origin_map = {}
        for row in origin_rows:
            origin_map[row["id"]] = row
            origin_map[row["url"]] = row
        by_snapshot = {}
        for visit in visits:
            by_snapshot.setdefault(visit["snapshot"], []).append(visit)
        unresolved_visit_origins = set()
        for node, branch in matched_branches:
            for visit in by_snapshot.get(branch["snapshot_id"], []):
                origin = origin_map.get(visit["origin"])
                if origin is None:
                    unresolved_visit_origins.add(visit["origin"])
                    continue
                name = branch["name"]
                record = dict(node, origin_url=origin["url"], origin_id=origin["id"],
                              snapshot_id=branch["snapshot_id"], branch_name_base64=name.get("base64")
                              if isinstance(name, dict) else name,
                              visit=visit["visit"], visit_date=visit["date"], visit_status=visit["status"],
                              branch_evidence=branch, visit_evidence=visit, origin_evidence=origin)
                key = fingerprint(record)
                if key not in self.result_keys:
                    self.result_keys.add(key)
                    self.results.append(record)
                self.resolved.add(node["seed_directory_id"])
        if unresolved_visit_origins:
            raise RuntimeError("visit origin IDs missing in full origin table; evidence retained")
        self.progress(label + "/origins-complete")

    def revision_origins(self, roots, directory_nodes, label):
        frontier = list(roots)
        seen = {node_key(n) for n in frontier}
        self.find_origins(frontier + directory_nodes, label + "/history-0")
        for depth in range(1, self.config.get("max_revision_depth", 10000) + 1):
            frontier = [n for n in frontier if n["seed_directory_id"] not in self.resolved]
            if not frontier:
                return
            edges = self.scanner.scan("revision_history", "parent_id", [n["object_id"] for n in frontier],
                                      label + "/history-{}".format(depth))
            by_parent = {}
            for node in frontier:
                by_parent.setdefault(node["object_id"], []).append(node)
            next_frontier = []
            for edge in edges:
                for node in by_parent.get(edge["parent_id"], []):
                    child = dict(node, object_id=edge["id"], revision_history_depth=depth,
                                 revision_history_path=node.get("revision_history_path", []) + [edge])
                    if node_key(child) not in seen:
                        seen.add(node_key(child))
                        next_frontier.append(child)
            self.store.put(label + "/history-{:05d}.json".format(depth), next_frontier)
            if not next_frontier:
                return
            frontier = next_frontier
            self.find_origins(frontier, label + "/history-{}".format(depth))
        if any(n["seed_directory_id"] not in self.resolved for n in frontier):
            raise RuntimeError("revision depth limit reached; probe remains incomplete")

    def run(self):
        frontier = [{"seed_directory_id": s["directory_id"], "root_directory_id": s["directory_id"],
                     "object_id": s["directory_id"], "object_type": "directory", "directory_depth": 0,
                     "directory_path": [], "revision_history_depth": 0, "release_depth": 0}
                    for s in self.seeds]
        seen = {(n["seed_directory_id"], n["object_id"]) for n in frontier}
        self.progress("started")
        for depth in range(self.config.get("max_directory_depth", 128) + 1):
            if not frontier:
                break
            label = "directory-{:03d}".format(depth)
            if depth == 0:
                roots = self.config["initial_roots"]
                self.store.put("reused-revision-root-query.json", self.config["initial_root_query"])
            else:
                roots = self.scanner.scan("revision", "directory", [n["object_id"] for n in frontier], label + "/roots")
            by_directory = {}
            for node in frontier:
                by_directory.setdefault(node["object_id"], []).append(node)
            revision_nodes = []
            for root in roots:
                for node in by_directory.get(root["directory"], []):
                    revision_nodes.append(dict(node, object_type="revision", object_id=root["id"],
                                               candidate_revision_id=root["id"], root_evidence=root))
            self.revision_origins(revision_nodes, frontier, label)
            pending = [n for n in frontier if n["seed_directory_id"] not in self.resolved]
            if not pending:
                break
            if depth == self.config.get("max_directory_depth", 128):
                raise RuntimeError("directory depth limit reached; probe remains incomplete")
            edges = self.scanner.scan("directory_entry", "target", [n["object_id"] for n in pending],
                                      label + "/parents", ["dir", "directory"])
            by_child = {}
            for node in pending:
                by_child.setdefault(node["object_id"], []).append(node)
            next_frontier = []
            for edge in edges:
                for node in by_child.get(edge["target"], []):
                    key = (node["seed_directory_id"], edge["directory_id"])
                    if key in seen:
                        continue
                    seen.add(key)
                    next_frontier.append(dict(node, object_id=edge["directory_id"],
                                              root_directory_id=edge["directory_id"], directory_depth=depth + 1,
                                              directory_path=node["directory_path"] + [edge]))
            next_seeds = {n["seed_directory_id"] for n in next_frontier}
            for node in pending:
                if node["seed_directory_id"] not in next_seeds:
                    self.reasons[node["seed_directory_id"]] = "reachable_graph_exhausted_without_origin"
            frontier = next_frontier
            self.store.put("directory-frontier-{:03d}.json".format(depth + 1), frontier)
            self.progress(label + "/parents-complete")
        return self.progress("finished", all_eight_mapped=len(self.resolved) == len(self.seeds),
                             finished_at=utc_now())


def main():
    import boto3
    from botocore.config import Config
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.utils import getResolvedOptions
    from pyspark.context import SparkContext

    args = getResolvedOptions(sys.argv, ["JOB_NAME", "CONFIG_URI", "OUTPUT_ROOT"])
    client = boto3.client("s3", region_name="us-east-1", config=Config(
        retries={"mode": "standard", "total_max_attempts": 4}, max_pool_connections=32))
    store = Store(client, args["OUTPUT_ROOT"])
    config = store.read(args["CONFIG_URI"])
    inventory = config["inventory"]
    if inventory["graph_release"] != "2026-06-04":
        raise ValueError("unexpected graph release")
    for table in TABLE_COLUMNS:
        objects = inventory["tables"][table]["objects"]
        if len(objects) != 128 or len({o["key"] for o in objects}) != 128:
            raise ValueError("incomplete inventory for " + table)
    previous = store.read(store.root + "/config-fingerprint.json")
    identity = {"fingerprint": fingerprint(config)}
    if previous is not None and previous != identity:
        raise RuntimeError("configuration changed: use a new output root")
    store.put("config-fingerprint.json", identity)
    context = GlueContext(SparkContext.getOrCreate())
    spark = context.spark_session
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.orc.impl", "native")
    spark.conf.set("spark.sql.orc.filterPushdown", "true")
    spark.conf.set("spark.sql.files.maxPartitionBytes", str(512 * 1024 * 1024))
    spark.conf.set("spark.sql.shuffle.partitions", "76")
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    job = Job(context)
    job.init(args["JOB_NAME"], args)
    traversal = Traversal(Scanner(spark, store, inventory, config), store, config)
    try:
        traversal.run()
    except Exception as exc:
        traversal.progress("failed_incomplete", error=str(exc))
        raise
    job.commit()


if __name__ == "__main__":
    main()
