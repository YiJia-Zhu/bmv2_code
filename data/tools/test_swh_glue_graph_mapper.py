#!/usr/bin/env python3
"""Offline tests for the Spark graph-mapper's dependency-free helpers."""

import json
import unittest
import os
import tempfile
import time
from contextlib import redirect_stdout
from io import StringIO
from collections import namedtuple

from data.tools import swh_glue_graph_mapper as mapper


class SwhGlueGraphMapperTest(unittest.TestCase):
    def test_task_progress_estimates_only_current_spark_stage(self):
        Stage = namedtuple(
            "Stage",
            "stageId currentAttemptId name numTasks numCompletedTasks "
            "numActiveTasks numFailedTasks",
        )
        info = Stage(7, 0, "read directory edges", 100, 40, 8, 1)
        first = mapper.task_progress(info, observed_at=10.0)
        self.assertEqual(40.0, first["task_percent"])
        self.assertIsNone(first["eta_seconds"])
        second = mapper.task_progress(info, previous=(20, 0.0), observed_at=10.0)
        self.assertEqual(2.0, second["task_rate_per_second"])
        self.assertEqual(30, second["eta_seconds"])
        self.assertIsNotNone(second["estimated_finish_utc"])
        stalled = mapper.task_progress(info, previous=(40, 0.0), observed_at=10.0)
        self.assertIsNone(stalled["eta_seconds"])

    def test_reads_glue_style_arguments(self):
        argv = ["job.py", "--JOB_RUN_ID", "jr_one", "--FLAG=true"]
        self.assertEqual("jr_one", mapper.get_cli_argument(argv, "JOB_RUN_ID"))
        self.assertEqual("true", mapper.get_cli_argument(argv, "FLAG"))
        self.assertEqual("fallback", mapper.get_cli_argument(argv, "MISSING", "fallback"))

    def test_rejects_missing_argument_value(self):
        with self.assertRaises(ValueError):
            mapper.get_cli_argument(["job.py", "--FLAG"], "FLAG")

    def test_boolean_and_integer_validation(self):
        self.assertTrue(mapper.parse_bool("TRUE"))
        self.assertFalse(mapper.parse_bool("0"))
        self.assertEqual(4, mapper.positive_int("4", "DEPTH"))
        with self.assertRaises(ValueError):
            mapper.parse_bool("maybe")
        with self.assertRaises(ValueError):
            mapper.positive_int("0", "DEPTH")

    def test_swhid_and_uri_normalization(self):
        self.assertEqual(
            "0123456789abcdef0123456789abcdef01234567",
            mapper.strip_swhid_prefix(
                "swh:1:dir:0123456789abcdef0123456789abcdef01234567"
            ),
        )
        self.assertEqual(
            "s3://swh2p4/results/run-1",
            mapper.join_uri("s3://swh2p4/results/", "/run-1"),
        )

    def test_origin_url_policy(self):
        self.assertTrue(mapper.is_github_url("https://github.com/org/repo.git"))
        self.assertTrue(mapper.is_github_url("https://www.github.com/org/repo"))
        self.assertFalse(mapper.is_github_url("https://gitlab.com/org/repo"))

    def test_column_alias_selection_is_case_insensitive(self):
        class Frame(object):
            columns = ["Target", "directory_id"]

        frame = Frame()
        self.assertEqual("Target", mapper.find_column(frame, ["target"]))
        self.assertEqual(
            "directory_id", mapper.find_column(frame, ["directory", "directory_id"])
        )
        self.assertIsNone(mapper.find_column(frame, ["origin_url"]))

    def test_manifest_shard_selection_and_validation(self):
        manifest = {
            "graph_release": mapper.GRAPH_RELEASE,
            "objects": [
                {"index": 1, "status": "complete", "stable_result_s3_uri": "s3://b/1.csv",
                 "stable_result_size_bytes": 20, "stable_result_etag": "etag-1"},
                {"index": 0, "status": "complete", "stable_result_s3_uri": "s3://b/0.csv",
                 "stable_result_size_bytes": 10, "stable_result_etag": "etag-0"},
            ],
        }
        self.assertEqual([0, 1], [x["index"] for x in mapper.select_manifest_shards(manifest, [1, 0])])
        self.assertEqual([0, 1], [x["index"] for x in mapper.select_manifest_shards(manifest, None)])
        self.assertEqual([85, 94], sorted(mapper.parse_shard_indices("94,85")))
        with self.assertRaises(ValueError):
            mapper.parse_shard_indices("85,85")
        with self.assertRaises(ValueError):
            mapper.select_manifest_shards(manifest, [2])
        manifest["objects"][0]["status"] = "failed"
        with self.assertRaises(ValueError):
            mapper.select_manifest_shards(manifest, [1])


try:
    from pyspark.sql import SparkSession
except ImportError:
    SparkSession = None


@unittest.skipIf(SparkSession is None, "pyspark is unavailable")
class SwhGlueGraphMapperSparkTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder.master("local[2]")
            .appName("swh-graph-mapper-test")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "2")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")
        cls.tempdir = tempfile.TemporaryDirectory()
        cls.spark.sparkContext.setCheckpointDir(
            os.path.join(cls.tempdir.name, "checkpoints")
        )

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
        cls.tempdir.cleanup()

    def test_live_spark_progress_heartbeat(self):
        output = StringIO()
        monitor = mapper.StageProgress(self.spark, "heartbeat-test", "test", 0.1)
        with redirect_stdout(output):
            monitor.start()
            try:
                self.spark.sparkContext.parallelize([1, 2], 2).map(
                    lambda value: (time.sleep(1), value)[1]
                ).count()
            finally:
                monitor.stop("completed")
        events = [json.loads(line) for line in output.getvalue().splitlines()]
        self.assertFalse(any(row["event"] == "progress_monitor_error" for row in events))
        self.assertTrue(any(
            row["event"] == "pipeline_stage_progress" and row["spark_stages"]
            for row in events
        ))

    def test_nested_old_revision_maps_to_each_origin_without_p4_16(self):
        from pyspark.sql import types

        csv_path = os.path.join(self.tempdir.name, "hits.csv")
        with open(csv_path, "w", encoding="utf-8") as stream:
            stream.write("directory_id,name,type,content_id,source_path\n")
            stream.write("dir_pkg,router.p4,file,blob1,shard.orc\n")
            stream.write("dir_pkg,notes.txt,file,blob2,shard.orc\n")
        hits = mapper.normalize_candidate_frame(self.spark, csv_path, "csv")
        self.assertEqual(1, hits.count())

        entry_schema = types.StructType(
            [
                types.StructField("directory_id", types.StringType()),
                types.StructField("name", types.BinaryType()),
                types.StructField("type", types.StringType()),
                types.StructField("target", types.StringType()),
            ]
        )
        entries_path = os.path.join(self.tempdir.name, "entries")
        self.spark.createDataFrame(
            [
                ("root_old", b"src", "dir", "dir_src"),
                ("dir_src", b"pkg", "dir", "dir_pkg"),
                ("dir_pkg", b"router.p4", "file", "blob1"),
            ],
            entry_schema,
        ).write.orc(entries_path)
        edges = mapper.read_directory_edges(self.spark, entries_path)
        closure, closure_metrics = mapper.build_directory_closure(
            hits.select("directory_id"), edges, 4, checkpoint_dir=self.tempdir.name
        )
        self.assertEqual(2, closure_metrics["directory_depth_reached"])

        revisions_path = os.path.join(self.tempdir.name, "revisions")
        self.spark.createDataFrame(
            [("rev_old", "root_old", "2020-01-01")],
            ["id", "directory", "date"],
        ).write.orc(revisions_path)
        revisions = mapper.read_revisions(self.spark, revisions_path)
        candidates = mapper.attach_revision_roots(hits, closure, revisions)
        self.assertEqual("src/pkg/router.p4", candidates.first().p4_path)

        branches = self.spark.createDataFrame(
            [
                ("snap_main", "main", "rev_tip", "revision"),
                ("snap_feature", "feature", "rev_old", "revision"),
                ("snap_other", "main", "rev_far_tip", "revision"),
            ],
            ["snapshot_id", "branch_name", "target_id", "target_type"],
        )
        visits = self.spark.createDataFrame(
            [
                ("snap_main", "origin_a", None, "visit_a", "2022-01-01", "full"),
                ("snap_feature", "origin_b", None, "visit_b", "2021-01-01", "full"),
                ("snap_other", "origin_c", None, "visit_c", "2023-01-01", "full"),
            ],
            types.StructType(
                [
                    types.StructField(name, types.StringType())
                    for name in (
                        "snapshot_id",
                        "origin_id",
                        "origin_url",
                        "visit_id",
                        "visit_date",
                        "visit_status",
                    )
                ]
            ),
        )
        origins = self.spark.createDataFrame(
            [
                ("origin_a", "https://github.com/team/router.git"),
                ("origin_b", "https://gitlab.com/team/fork"),
                ("origin_c", "https://github.com/other/unrelated"),
            ],
            ["origin_id", "origin_url"],
        )
        history = self.spark.createDataFrame(
            [
                ("rev_mid", "rev_old"),
                ("rev_tip", "rev_mid"),
                ("rev_far_tip", "rev_far_old"),
            ],
            ["child_revision_id", "parent_revision_id"],
        )
        mapped, metrics = mapper.map_candidates_to_origins(
            candidates,
            branches,
            visits,
            origins,
            history=history,
            max_revision_depth=4,
            checkpoint_dir=self.tempdir.name,
        )
        rows = {row.origin_id: row for row in mapped.collect()}
        self.assertEqual({"origin_a", "origin_b"}, set(rows))
        self.assertEqual(2, rows["origin_a"].revision_depth)
        self.assertEqual("ancestor_of_branch_tip", rows["origin_a"].revision_relation)
        self.assertEqual(0, rows["origin_b"].revision_depth)
        self.assertIsNone(rows["origin_a"].p4_16_confirmed)
        self.assertTrue(metrics["revision_history_available"])
        self.assertEqual(3, metrics["revision_reachability_count"])

        repositories = {
            row.origin_url: row.repository_name
            for row in mapper.build_repository_candidates(mapped).collect()
        }
        self.assertEqual(
            {
                "https://github.com/team/router.git": "router",
                "https://gitlab.com/team/fork": "fork",
            },
            repositories,
        )
        self.assertEqual(
            2,
            mapper.build_repository_candidates(mapped.unionByName(mapped)).count(),
        )

        bounded, bounded_metrics = mapper.map_candidates_to_origins(
            candidates,
            branches,
            visits,
            origins,
            history=history,
            max_revision_depth=1,
            checkpoint_dir=self.tempdir.name,
        )
        self.assertEqual(["origin_b"], [row.origin_id for row in bounded.collect()])
        self.assertTrue(bounded_metrics["revision_stopped_at_depth_limit"])

        direct, direct_metrics = mapper.map_candidates_to_origins(
            candidates, branches, visits, origins, max_revision_depth=4
        )
        self.assertEqual(["origin_b"], [row.origin_id for row in direct.collect()])
        self.assertFalse(direct_metrics["revision_history_available"])

        branch_path = os.path.join(self.tempdir.name, "branches")
        visit_path = os.path.join(self.tempdir.name, "visits")
        origin_path = os.path.join(self.tempdir.name, "origins")
        history_path = os.path.join(self.tempdir.name, "history")
        branches.write.orc(branch_path)
        visits.write.orc(visit_path)
        origins.write.orc(origin_path)
        history.write.orc(history_path)
        output_root = os.path.join(self.tempdir.name, "results")
        run_metrics = mapper.run_mapping(
            self.spark,
            {
                "run_id": "local-fixture",
                "candidate_input": csv_path,
                "candidate_format": "csv",
                "output_uri": output_root,
                "directory_entry_uri": entries_path,
                "revision_uri": revisions_path,
                "snapshot_branch_uri": branch_path,
                "origin_visit_status_uri": visit_path,
                "origin_uri": origin_path,
                "revision_history_uri": history_path,
                "checkpoint_uri": os.path.join(self.tempdir.name, "checkpoints"),
                "max_directory_depth": 4,
                "max_revision_depth": 4,
            },
        )
        self.assertEqual(2, run_metrics["repository_candidate_count"])
        repository_csv = os.path.join(
            output_root, "runs", "local-fixture", "repository_candidates_csv"
        )
        csv_repositories = self.spark.read.option("header", "true").csv(
            repository_csv
        )
        self.assertEqual(
            {"https://github.com/team/router.git", "https://gitlab.com/team/fork"},
            {row.origin_url for row in csv_repositories.collect()},
        )

        resumed_config = {
            "run_id": "local-fixture",
            "candidate_input": csv_path,
            "candidate_format": "csv",
            "output_uri": output_root,
            "directory_entry_uri": entries_path,
            "revision_uri": revisions_path,
            "snapshot_branch_uri": branch_path,
            "origin_visit_status_uri": visit_path,
            "origin_uri": origin_path,
            "revision_history_uri": history_path,
            "checkpoint_uri": os.path.join(self.tempdir.name, "checkpoints"),
            "max_directory_depth": 4,
            "max_revision_depth": 4,
            "resume": True,
        }
        resumed = mapper.run_mapping(self.spark, resumed_config)
        self.assertEqual(2, resumed["repository_candidate_count"])
        self.assertIn("directory_closure", resumed["resumed_stages"])
        with self.assertRaisesRegex(RuntimeError, "configuration changed"):
            mapper.run_mapping(self.spark, dict(resumed_config, max_directory_depth=5))

    def test_two_manifest_csv_shards_resume_as_one_batch(self):
        first_path = os.path.join(self.tempdir.name, "shard-0.csv")
        second_path = os.path.join(self.tempdir.name, "shard-1.csv")
        for path, name in ((first_path, "a.p4"), (second_path, "b.p4")):
            with open(path, "w", encoding="utf-8") as stream:
                stream.write("directory_id,name,type,content_id,source_path\n")
                stream.write("dir_pkg,{},file,blob1,source.orc\n".format(name))
        manifest_path = os.path.join(self.tempdir.name, "manifest.json")
        manifest = {
            "graph_release": mapper.GRAPH_RELEASE,
            "objects": [
                {
                    "index": index,
                    "status": "complete",
                    "stable_result_s3_uri": path,
                    "stable_result_size_bytes": os.path.getsize(path),
                    "stable_result_etag": "etag-{}".format(index),
                }
                for index, path in enumerate((first_path, second_path))
            ],
        }
        with open(manifest_path, "w", encoding="utf-8") as stream:
            json.dump(manifest, stream)
        config = {
            "run_id": "two-shards",
            "candidate_manifest_uri": manifest_path,
            "candidate_shard_indices": [1, 0],
            "output_uri": os.path.join(self.tempdir.name, "results"),
            "directory_entry_uri": "unused-entry",
            "revision_uri": "unused-revision",
            "snapshot_branch_uri": "unused-branch",
            "origin_visit_status_uri": "unused-visit",
            "origin_uri": "unused-origin",
            "max_directory_depth": 4,
            "max_revision_depth": 4,
            "stop_after_stage": "normalized_hits",
            "resume": True,
        }
        stale_shard = os.path.join(
            config["output_uri"], "runs", config["run_id"],
            "normalized_hit_shards", "00000",
        )
        os.makedirs(stale_shard)
        with open(os.path.join(stale_shard, "partial"), "w", encoding="utf-8") as stream:
            stream.write("incomplete")
        first = mapper.run_mapping(self.spark, config)
        self.assertEqual(2, first["candidate_hit_count"])
        self.assertEqual("partial", first["status"])
        shard_record = mapper.read_stage_record(
            self.spark,
            os.path.join(config["output_uri"], "runs", config["run_id"]),
            "normalized_hit_shards/00000",
        )
        self.assertIn("_stage_attempts/normalized_hit_shards/00000/", shard_record["data_uri"])
        resumed = mapper.run_mapping(self.spark, dict(config, resume=True))
        self.assertEqual(2, resumed["candidate_hit_count"])
        self.assertIn("normalized_hit_shards/00000", resumed["resumed_stages"])
        self.assertIn("normalized_hit_shards/00001", resumed["resumed_stages"])

    def test_incomplete_stage_retries_without_overwriting_existing_data(self):
        run_uri = os.path.join(self.tempdir.name, "interrupted-stage")
        stale_uri = os.path.join(run_uri, "normalized_hits")
        os.makedirs(stale_uri)
        with open(os.path.join(stale_uri, "partial"), "w", encoding="utf-8") as stream:
            stream.write("incomplete")

        def build():
            return self.spark.createDataFrame([("dir_one",)], ["directory_id"]), {"count": 1}

        with self.assertRaisesRegex(RuntimeError, "pass RESUME=true"):
            mapper.materialize_stage(
                self.spark, run_uri, "normalized_hits", "same-input", build, False
            )
        frame, metrics, resumed = mapper.materialize_stage(
            self.spark, run_uri, "normalized_hits", "same-input", build, True
        )
        self.assertFalse(resumed)
        self.assertEqual(1, metrics["count"])
        self.assertEqual("dir_one", frame.first().directory_id)
        record = mapper.read_stage_record(self.spark, run_uri, "normalized_hits")
        self.assertIn("_stage_attempts/normalized_hits/", record["data_uri"])
        self.assertTrue(os.path.exists(os.path.join(stale_uri, "partial")))

        reused, _, resumed = mapper.materialize_stage(
            self.spark, run_uri, "normalized_hits", "same-input", build, True
        )
        self.assertTrue(resumed)
        self.assertEqual("dir_one", reused.first().directory_id)


if __name__ == "__main__":
    unittest.main()
