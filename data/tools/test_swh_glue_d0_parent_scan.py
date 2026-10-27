"""Offline checks for the bounded D0 parent scan."""

import tempfile
import unittest

import run_swh_d0_parent_scan as runner
import swh_glue_d0_parent_scan as scan


class D0ParentScanTest(unittest.TestCase):
    def test_budget_allows_only_remaining_compute(self):
        self.assertEqual(runner.allowed_timeout([]), (720, 0.0))
        timeout, spent = runner.allowed_timeout([{"ExecutionTime": 43200}])
        self.assertAlmostEqual(spent, 84.48)
        self.assertEqual(timeout, 84)

    def test_glue_parquet_without_success_marker_is_complete(self):
        class FakeS3(object):
            def list_objects_v2(self, **kwargs):
                self.kwargs = kwargs
                return {"Contents": [{"Key": kwargs["Prefix"] + "part-00000.parquet",
                                      "Size": 100}]}

        client = FakeS3()
        store = scan.Store(client, "s3://bucket/output")
        self.assertTrue(store.completed_parquet("s3://bucket/output/edges"))
        self.assertEqual(client.kwargs["Prefix"], "output/edges/")

    def test_checkpoint_requires_full_source_identity(self):
        source = {"key": "a.orc", "etag": '"a"', "size": 100}
        marker = {"config_id": "id", "source": source, "edge_rows": 0,
                  "data_uri": "s3://bucket/data"}
        self.assertTrue(scan.checkpoint_matches(marker, "id", source))
        self.assertFalse(scan.checkpoint_matches(marker, "other", source))
        self.assertFalse(scan.checkpoint_matches(marker, "id", dict(source, size=101)))

    def test_binary_names_and_multiple_parents_survive_scan(self):
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F

        with tempfile.TemporaryDirectory() as directory:
            spark = (SparkSession.builder.master("local[2]")
                     .appName("d0-parent-scan-test").getOrCreate())
            spark.sparkContext.setLogLevel("ERROR")
            try:
                rows = [("parent1", b"src", "dir", "child", 16384),
                        ("parent2", b"\xff", "dir", "child", 16384),
                        ("other", b"x", "dir", "other-child", 16384),
                        ("other", b"x", "file", "child", 33188)]
                path = directory + "/entries"
                spark.createDataFrame(rows, ["directory_id", "name", "type",
                                             "target", "perms"]).write.orc(path)
                seeds = spark.createDataFrame([("child",)], ["seed_id"])
                edges = scan.matched_edges(spark, path, seeds)
                self.assertEqual(edges.count(), 2)
                self.assertEqual({row["parent_id"] for row in edges.collect()},
                                 {"parent1", "parent2"})
                self.assertEqual(edges.select(F.col("name")).dropDuplicates().count(), 2)
            finally:
                spark.stop()


if __name__ == "__main__":
    unittest.main()
