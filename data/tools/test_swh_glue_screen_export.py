"""Exercise provenance-preserving CSV export on a small local Spark fixture."""

import json
import tempfile
import unittest
from pathlib import Path

from data.tools import swh_glue_screen_export as EXPORT


class ScreenExportTests(unittest.TestCase):
    def test_join_retains_repeated_content_and_all_dispositions(self):
        from pyspark.sql import SparkSession

        spark = (SparkSession.builder.master("local[2]").appName("swh-screen-export-test")
                 .config("spark.sql.shuffle.partitions", "2").getOrCreate())
        try:
            spark.sparkContext.setLogLevel("ERROR")
            with tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                rows = [
                    (0, "csv0", 1, "dir1", "main,quoted.p4", "file", "a" * 40, "orc1"),
                    (1, "csv1", 1, "dir2", "shared.p4", "file", "a" * 40, "orc1"),
                    (0, "csv0", 2, "dir1", "old.p4", "file", "b" * 40, "orc1"),
                    (1, "csv1", 2, "dir2", "unknown.p4", "file", "c" * 40, "orc1"),
                    (1, "csv1", 3, "dir2", "bad.p4", "file", "d" * 40, "orc1"),
                    (1, "csv1", 4, "dir2", "failed.p4", "file", "e" * 40, "orc1"),
                ]
                source = str(root / "source")
                spark.createDataFrame(rows, ["source_shard", "source_csv", "source_row",
                                             "directory_id", "name", "type", "content_id",
                                             "source_path"]).write.parquet(source)
                annotations = []
                for content_id, version, quality in (
                    ("a", "p4_16_likely", "pass"), ("b", "p4_14_likely", "pass"),
                    ("c", "unknown", "pass"), ("d", "p4_16_likely", "excluded"),
                    ("e", "uninspected", "unknown"),
                ):
                    annotations.append({"content_id": content_id * 40,
                                        "rule_version": "p4-content-screen-v1",
                                        "fetch_status": "network_error" if content_id == "e" else "fetched",
                                        "sha1_git_verified": content_id != "e",
                                        "version_status": version, "quality_status": quality,
                                        "quality_flags": ["long_average_line"] if content_id == "d" else []})
                annotation_path = root / "annotations.jsonl"
                annotation_path.write_text("".join(json.dumps(row) + "\n" for row in annotations))
                output = str(root / "export")
                metrics = EXPORT.export(spark, source, str(annotation_path), output,
                                        expected_ids=5, expected_rows=6, expected_shards=2)
                self.assertEqual(metrics["disposition_counts"], {
                    "selected": 2, "excluded_p4_14": 1, "excluded_quality": 1,
                    "needs_review": 2,
                })
                selected = spark.read.option("header", True).csv(output + "/selected_csv").collect()
                self.assertEqual({row.directory_id for row in selected}, {"dir1", "dir2"})
                self.assertIn("main,quoted.p4", {row.name for row in selected})
                self.assertEqual({row.source_csv_index for row in selected}, {"0", "1"})
        finally:
            spark.stop()


if __name__ == "__main__":
    unittest.main()
