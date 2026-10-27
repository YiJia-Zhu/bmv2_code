"""Preserve every original review hit in the second-pass Glue export."""

import json
import tempfile
import unittest
from pathlib import Path

from data.tools import swh_glue_review_export as EXPORT


class ReviewExportTests(unittest.TestCase):
    def test_repeated_content_retains_source_rows(self):
        from pyspark.sql import SparkSession

        spark = (SparkSession.builder.master("local[2]").appName("swh-review-export-test")
                 .config("spark.sql.shuffle.partitions", "2").getOrCreate())
        try:
            spark.sparkContext.setLogLevel("ERROR")
            with tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                source = str(root / "source")
                rows = [(0, "csv0", 1, "a" * 40, "needs_review"),
                        (1, "csv1", 2, "a" * 40, "needs_review"),
                        (1, "csv1", 3, "b" * 40, "needs_review"),
                        (0, "csv0", 4, "c" * 40, "needs_review"),
                        (0, "csv0", 5, "d" * 40, "selected")]
                spark.createDataFrame(rows, ["source_shard", "source_csv", "source_row",
                                             "content_id", "screen_disposition"]).write.parquet(source)
                labels = [("a", "p4_16_candidate"), ("b", "p4_14_likely"),
                          ("c", "unresolved")]
                annotations = root / "annotations.jsonl"
                annotations.write_text("".join(json.dumps({
                    "content_id": content_id * 40, "review_rule_version": "p4-review-screen-v2",
                    "review_disposition": disposition, "review_version_status": "unknown",
                }) + "\n" for content_id, disposition in labels))
                output = str(root / "output")
                metrics = EXPORT.export(spark, source, str(annotations), output,
                                        expected_ids=3, expected_rows=4)
                self.assertEqual(metrics["disposition_counts"], {
                    "p4_16_candidate": 2, "p4_14_likely": 1, "unresolved": 1})
                selected = spark.read.option("header", True).csv(
                    output + "/p4_16_candidate_csv").collect()
                self.assertEqual({row.source_csv for row in selected}, {"csv0", "csv1"})
        finally:
            spark.stop()


if __name__ == "__main__":
    unittest.main()
