import copy
import io
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest import mock

from data.tools.swh_glue_probe8 import Scanner, Traversal


class MemoryStore(object):
    def __init__(self, root="memory://test"):
        self.root = root
        self.values = {}
        self.interrupt_once = False

    def read(self, uri):
        return copy.deepcopy(self.values.get(uri))

    def put(self, name, value):
        if self.interrupt_once and name.endswith("batch-000/complete.json"):
            self.interrupt_once = False
            raise RuntimeError("simulated interruption before checkpoint commit")
        self.values[self.root + "/" + name] = copy.deepcopy(value)

    def put_text(self, name, value, content_type):
        self.values[self.root + "/" + name] = value


class MemoryScanner(object):
    def __init__(self, tables):
        self.tables = tables
        self.calls = []

    def scan(self, table, column, values, label, allowed_types=None):
        self.calls.append((table, column, set(values)))
        type_column = "type" if table == "directory_entry" else "target_type"
        return [copy.deepcopy(row) for row in self.tables.get(table, [])
                if row[column] in values and (not allowed_types or row[type_column] in allowed_types)]


def branch(target, target_type, snapshot="snapshot1"):
    return {"snapshot_id": snapshot, "target": target, "target_type": target_type,
            "name": {"base64": "cmVmcy9oZWFkcy9tYWlu"}}


def add_origin(tables, url="https://github.com/example/p4", use_id=False):
    origin_id = "a" * 40
    tables["origin"] = [{"id": origin_id, "url": url}]
    tables["origin_visit_status"] = [{"snapshot": "snapshot1", "origin": origin_id if use_id else url,
                                      "visit": 1, "date": "2026-01-01", "status": "full", "type": "git"}]


class TraversalTest(unittest.TestCase):
    def run_graph(self, tables, seeds=None, roots=None, **limits):
        config = {"seeds": [{"directory_id": s} for s in (seeds or ["dir1"])],
                  "initial_roots": roots or [], "initial_root_query": {"query_id": "paid-query"}}
        config.update(limits)
        scanner, store = MemoryScanner(tables), MemoryStore()
        traversal = Traversal(scanner, store, config)
        with mock.patch("sys.stdout", new=io.StringIO()):
            status = traversal.run()
        return traversal, scanner, store, status

    def test_history_direction_and_nested_releases_preserve_candidate_revision(self):
        tables = {
            "revision_history": [{"id": "rev2", "parent_id": "rev1", "parent_rank": 0},
                                 {"id": "older", "parent_id": "unrelated", "parent_rank": 0}],
            "release": [{"id": "tag1", "target": "rev2", "target_type": "revision"},
                        {"id": "tag2", "target": "tag1", "target_type": "release"}],
            "snapshot_branch": [branch("tag2", "release")],
        }
        add_origin(tables)
        traversal, scanner, store, status = self.run_graph(
            tables, roots=[{"id": "rev1", "directory": "dir1"}])
        self.assertTrue(status["all_eight_mapped"])
        result = traversal.results[0]
        self.assertEqual("rev1", result["candidate_revision_id"])
        self.assertEqual("rev2", result["revision_history_path"][0]["id"])
        self.assertEqual(2, result["release_depth"])
        self.assertEqual("dir1", result["root_directory_id"])
        self.assertIn(("revision_history", "parent_id", {"rev1"}), scanner.calls)
        self.assertIn("https://github.com/example/p4", store.values[store.root + "/repository-urls.txt"])

    def test_parent_directory_and_origin_id_resolution(self):
        tables = {
            "directory_entry": [{"directory_id": "root", "target": "dir1", "name": {"base64": "c3Jj"}, "type": "dir"}],
            "revision": [{"id": "rev1", "directory": "root"}],
            "snapshot_branch": [branch("rev1", "revision")],
        }
        add_origin(tables, use_id=True)
        traversal, _, _, status = self.run_graph(tables)
        self.assertTrue(status["all_eight_mapped"])
        self.assertEqual(1, traversal.results[0]["directory_depth"])
        self.assertEqual("root", traversal.results[0]["directory_path"][0]["directory_id"])

    def test_shared_root_keeps_both_seeds_and_non_http_origins(self):
        tables = {
            "directory_entry": [{"directory_id": "root", "target": s, "name": {}, "type": "dir"}
                                for s in ["dir1", "dir2"]],
            "revision": [{"id": "rev1", "directory": "root"}],
            "snapshot_branch": [branch("rev1", "revision")],
        }
        add_origin(tables, "git://example.org/p4.git")
        traversal, _, _, status = self.run_graph(tables, seeds=["dir1", "dir2"])
        self.assertTrue(status["all_eight_mapped"])
        self.assertEqual({"dir1", "dir2"}, {r["seed_directory_id"] for r in traversal.results})
        self.assertEqual(1, status["origin_urls"])

    def test_direct_directory_release_requires_no_revision(self):
        tables = {"release": [{"id": "tag", "target": "dir1", "target_type": "directory"}],
                  "snapshot_branch": [branch("tag", "release")]}
        add_origin(tables)
        traversal, scanner, _, status = self.run_graph(tables)
        self.assertTrue(status["all_eight_mapped"])
        self.assertNotIn("candidate_revision_id", traversal.results[0])
        self.assertFalse(any(call[0] == "directory_entry" for call in scanner.calls))

    def test_exhausted_graph_is_explicit_and_history_cycle_terminates(self):
        tables = {"revision_history": [{"id": "rev2", "parent_id": "rev1", "parent_rank": 0},
                                        {"id": "rev1", "parent_id": "rev2", "parent_rank": 0}]}
        _, scanner, _, status = self.run_graph(tables, roots=[{"id": "rev1", "directory": "dir1"}])
        self.assertFalse(status["all_eight_mapped"])
        self.assertEqual("reachable_graph_exhausted_without_origin", status["seeds"][0]["status"])
        self.assertEqual(2, len([c for c in scanner.calls if c[0] == "revision_history"]))

    def test_depth_limit_raises_incomplete_instead_of_negative_result(self):
        tables = {"revision_history": [{"id": "rev2", "parent_id": "rev1", "parent_rank": 0}]}
        with self.assertRaisesRegex(RuntimeError, "remains incomplete"):
            self.run_graph(tables, roots=[{"id": "rev1", "directory": "dir1"}], max_revision_depth=1)

    def test_missing_origin_row_raises_instead_of_silent_mapping_loss(self):
        tables = {"snapshot_branch": [branch("dir1", "directory")]}
        add_origin(tables)
        tables["origin"] = []
        with self.assertRaisesRegex(RuntimeError, "origin IDs missing"):
            self.run_graph(tables)


class ScannerCheckpointTest(unittest.TestCase):
    def test_snapshot_index_resumes_committed_source_batches(self):
        from pyspark.sql import SparkSession
        from pyspark.sql.readwriter import DataFrameReader
        import data.tools.swh_glue_graph_mapper as helper

        with tempfile.TemporaryDirectory(prefix="swh-probe8-snapshot-") as temporary:
            os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
            spark = (SparkSession.builder.master("local[2]").appName("probe8-snapshot-batches")
                     .config("spark.ui.enabled", "false").getOrCreate())
            spark.sparkContext.setLogLevel("ERROR")
            try:
                source = Path(temporary) / "source"
                targets = [letter * 40 for letter in "abcd"]
                for offset, target in enumerate(targets):
                    spark.createDataFrame(
                        [("f" * 40, b"main", target, "revision")],
                        "snapshot_id string, name binary, target string, target_type string"
                    ).coalesce(1).write.orc(str(source / str(offset)))
                objects = [{"key": str(path), "size": path.stat().st_size, "etag": "fixture"}
                           for path in sorted(source.rglob("*.orc"))]
                inventory = {"tables": {"snapshot_branch": {"objects": objects}}}
                config = {"lookup_indexes": {"snapshot_branch": {
                    "column": "target", "after_distinct_predicates": 1,
                    "index_batch_files": 2, "shuffle_partitions": 2}}}
                store = MemoryStore(str(Path(temporary) / "output"))
                original_orc, original_put = DataFrameReader.orc, store.put
                reads, interrupted = [], [False]

                def local_orc(reader, paths, **kwargs):
                    reads.extend(paths)
                    return original_orc(reader, [p.replace("s3://softwareheritage/", "", 1)
                                                 for p in paths], **kwargs)

                def interrupt_second_batch(name, value):
                    if name.endswith("batch-002/complete.json") and not interrupted[0]:
                        interrupted[0] = True
                        raise RuntimeError("interrupted second index batch")
                    original_put(name, value)

                with mock.patch.dict(sys.modules, {"swh_glue_graph_mapper": helper}), \
                        mock.patch.object(DataFrameReader, "orc", local_orc), \
                        mock.patch.object(store, "put", interrupt_second_batch), \
                        mock.patch("sys.stdout", new=io.StringIO()):
                    scanner = Scanner(spark, store, inventory, config)
                    with self.assertRaisesRegex(RuntimeError, "interrupted second index batch"):
                        scanner.lookup_index("snapshot_branch", "target", targets)
                    restarted = Scanner(spark, store, inventory, config)
                    selected = restarted.lookup_index("snapshot_branch", "target", targets)
                    self.assertEqual(set(targets), {row["target"] for row in selected.collect()})
                    again = Scanner(spark, store, inventory, config).lookup_index(
                        "snapshot_branch", "target", targets[:1])
                    self.assertEqual(targets[:1], [row["target"] for row in again.collect()])
                self.assertEqual(6, len(reads))
                self.assertEqual(1, sum("batch-000/complete.json" in key for key in store.values))
                plan = again._jdf.queryExecution().executedPlan().toString()
                self.assertIn("PartitionFilters:", plan)
                self.assertIn("aa", plan)
            finally:
                spark.stop()

    def test_lookup_index_rebuilds_uncommitted_data_then_reuses_it_after_restart(self):
        from pyspark.sql import SparkSession
        from pyspark.sql.readwriter import DataFrameReader
        import data.tools.swh_glue_graph_mapper as helper

        with tempfile.TemporaryDirectory(prefix="swh-probe8-index-") as temporary:
            os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
            spark = (SparkSession.builder.master("local[2]").appName("probe8-offline-index")
                     .config("spark.ui.enabled", "false").getOrCreate())
            spark.sparkContext.setLogLevel("ERROR")
            try:
                source = str(Path(temporary) / "source")
                spark.createDataFrame([("1" * 40, "a" * 40, 0),
                                       ("2" * 40, "b" * 40, 0)],
                                      "id string, parent_id string, parent_rank int").write.orc(source)
                objects = [{"key": str(p), "size": p.stat().st_size, "etag": "fixture"}
                           for p in sorted(Path(source).glob("*.orc"))]
                inventory = {"tables": {"revision_history": {"objects": objects}}}
                config = {"lookup_indexes": {"revision_history": {
                    "column": "parent_id", "after_distinct_predicates": 1,
                    "shuffle_partitions": 2}}}
                store = MemoryStore(str(Path(temporary) / "output"))
                original_orc, original_put = DataFrameReader.orc, store.put
                reads, interrupted = [], [False]

                def local_orc(reader, paths, **kwargs):
                    reads.append(paths)
                    return original_orc(reader, [p.replace("s3://softwareheritage/", "", 1)
                                                 for p in paths], **kwargs)

                def interrupt_commit(name, value):
                    if name.startswith("lookup-indexes/") and not interrupted[0]:
                        interrupted[0] = True
                        raise RuntimeError("interrupted index commit")
                    original_put(name, value)

                with mock.patch.dict(sys.modules, {"swh_glue_graph_mapper": helper}), \
                        mock.patch.object(DataFrameReader, "orc", local_orc), \
                        mock.patch.object(store, "put", interrupt_commit), \
                        mock.patch("sys.stdout", new=io.StringIO()):
                    scanner = Scanner(spark, store, inventory, config)
                    with self.assertRaisesRegex(RuntimeError, "interrupted index commit"):
                        scanner.scan("revision_history", "parent_id", ["a" * 40], "fixture")
                    first = scanner.scan("revision_history", "parent_id", ["a" * 40], "fixture")
                    restarted = Scanner(spark, store, inventory, config)
                    second = restarted.scan("revision_history", "parent_id", ["b" * 40], "fixture")
                    pruned = restarted.lookup_index("revision_history", "parent_id", ["a" * 40])
                self.assertEqual("1" * 40, first[0]["id"])
                self.assertEqual("2" * 40, second[0]["id"])
                self.assertEqual(2, len(reads))  # Failed build and one committed rebuild only.
                self.assertEqual(1, pruned.count())
                plan = pruned._jdf.queryExecution().executedPlan().toString()
                self.assertIn("PartitionFilters:", plan)
                self.assertIn("aaa", plan)
            finally:
                spark.stop()

    def test_real_local_spark_replays_uncommitted_batch_and_skips_committed_batches(self):
        from pyspark.sql import SparkSession
        from pyspark.sql.readwriter import DataFrameReader
        import data.tools.swh_glue_graph_mapper as helper

        with tempfile.TemporaryDirectory(prefix="swh-probe8-test-") as temporary:
            os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
            spark = (SparkSession.builder.master("local[2]").appName("probe8-offline-checkpoint")
                     .config("spark.ui.enabled", "false").getOrCreate())
            spark.sparkContext.setLogLevel("ERROR")
            try:
                source = str(Path(temporary) / "source")
                spark.createDataFrame([("parent", b"src", "dir", "wanted"),
                                       ("irrelevant", b"file", "file", "wanted")],
                                      "directory_id string, name binary, type string, target string").write.orc(source)
                parts = sorted(Path(source).glob("*.orc"))
                objects = [{"key": str(p), "size": p.stat().st_size, "etag": "fixture"} for p in parts]
                inventory = {"tables": {"directory_entry": {"objects": objects}}}
                store = MemoryStore(str(Path(temporary) / "output"))
                store.interrupt_once = True
                scanner = Scanner(spark, store, inventory, {"directory_batch_files": 1})
                original = DataFrameReader.orc
                calls = []

                def local_orc(reader, paths, **kwargs):
                    calls.append(paths)
                    return original(reader, [p.replace("s3://softwareheritage/", "", 1) for p in paths], **kwargs)

                with mock.patch.dict(sys.modules, {"swh_glue_graph_mapper": helper}), \
                        mock.patch.object(DataFrameReader, "orc", local_orc), \
                        mock.patch("sys.stdout", new=io.StringIO()):
                    with self.assertRaisesRegex(RuntimeError, "simulated interruption"):
                        scanner.scan("directory_entry", "target", ["wanted"], "fixture", ["dir"])
                    first = scanner.scan("directory_entry", "target", ["wanted"], "fixture", ["dir"])
                    reads_after_resume = len(calls)
                    second = scanner.scan("directory_entry", "target", ["wanted"], "fixture", ["dir"])
                self.assertEqual(first, second)
                self.assertEqual(1, len(first))
                self.assertEqual("parent", first[0]["directory_id"])
                self.assertEqual(len(objects) + 1, reads_after_resume)
                self.assertEqual(reads_after_resume, len(calls))
                coverage = next(iter(scanner.coverage.values()))
                self.assertEqual(len(objects), coverage["files_complete"])
            finally:
                spark.stop()


if __name__ == "__main__":
    unittest.main()
