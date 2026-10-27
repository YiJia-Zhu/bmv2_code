"""Check full-hit coverage when merging both P4 content screens."""

import json
import tempfile
import unittest
from pathlib import Path

from data.tools import swh_glue_merged_screen_export as EXPORT


class MergedScreenTests(unittest.TestCase):
    def test_review_promotions_and_quality_exclusions_retain_source_rows(self):
        from pyspark.sql import SparkSession

        spark = (SparkSession.builder.master("local[2]").appName("swh-merged-screen-test")
                 .config("spark.sql.shuffle.partitions", "2").getOrCreate())
        try:
            spark.sparkContext.setLogLevel("ERROR")
            with tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                source = str(root / "source")
                rows = [
                    (0, "csv0", 1, "a" * 40, "selected", "p4_16_likely", "pass"),
                    (1, "csv1", 2, "a" * 40, "selected", "p4_16_likely", "pass"),
                    (0, "csv0", 3, "b" * 40, "excluded_quality", "p4_16_likely", "excluded"),
                    (1, "csv1", 4, "c" * 40, "needs_review", "unknown", "pass"),
                    (0, "csv0", 5, "c" * 40, "needs_review", "unknown", "pass"),
                    (1, "csv1", 6, "d" * 40, "needs_review", "unknown", "pass"),
                    (0, "csv0", 7, "e" * 40, "needs_review", "unknown", "pass"),
                    (1, "csv1", 8, "f" * 40, "excluded_p4_14", "p4_14_likely", "pass"),
                ]
                frame = spark.createDataFrame(rows, ["source_shard", "source_csv", "source_row",
                                                     "content_id", "screen_disposition",
                                                     "version_status", "quality_status"])
                frame = frame.withColumn("fetch_status", __import__("pyspark").sql.functions.lit("fetched"))
                frame = frame.withColumn("sha1_git_verified", __import__("pyspark").sql.functions.lit(True))
                frame.write.parquet(source)
                labels = (("c", "p4_16_candidate"), ("d", "p4_14_likely"),
                          ("e", "unresolved"))
                annotations = root / "annotations.jsonl"
                annotations.write_text("".join(json.dumps({
                    "content_id": content_id * 40, "review_rule_version": "p4-review-screen-v2",
                    "review_disposition": disposition, "review_sha1_git_verified": True,
                }) + "\n" for content_id, disposition in labels))
                output = str(root / "merged")
                metrics = EXPORT.merge(spark, source, str(annotations), output,
                                       expected_ids=3, expected_rows=8,
                                       expected_review_rows=4, expected_shards=2)
                self.assertEqual(metrics["disposition_counts"], {
                    "p4_16_candidate": 5, "excluded_p4_14_likely": 1,
                    "excluded_p4_14": 1, "needs_review": 1,
                })
                candidates = spark.read.option("header", True).csv(
                    output + "/candidate_csv").collect()
                self.assertEqual(len(candidates), 5)
                self.assertEqual({row.source_csv for row in candidates}, {"csv0", "csv1"})
                self.assertEqual({row.source_row for row in candidates}, {"1", "2", "3", "4", "5"})
        finally:
            spark.stop()


if __name__ == "__main__":
    unittest.main()
