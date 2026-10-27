import csv
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from data.tools import swh_glue_dedup_hits as dedup


SOURCE_PATH = (
    "s3://softwareheritage/graph/2026-06-04/orc/directory_entry/part.orc"
)
DIR_A = "a" * 40
DIR_B = "b" * 40
CONTENT_A = "c" * 40
CONTENT_B = "d" * 40
FILE_0 = "part-00000-aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa.csv"
FILE_1 = "part-00001-bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb.csv"


def write_csv(path, rows):
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(dedup.CSV_COLUMNS)
        writer.writerows(rows)


class DedupHitsTest(unittest.TestCase):
    def test_parse_csv_keeps_record_index_and_quoted_names(self):
        content = (
            '"directory_id","name","type","content_id","source_path"\n'
            '"{}","name,with,commas.p4","file","{}","{}"\n'
        ).format(DIR_A, CONTENT_A, SOURCE_PATH)
        rows = list(dedup.parse_csv_file((
            "s3://swh2p4/" + FILE_0, content
        )))
        self.assertEqual(1, len(rows))
        self.assertEqual(0, rows[0][0])
        self.assertEqual(1, rows[0][2])
        self.assertEqual("name,with,commas.p4", rows[0][4])

    def test_invalid_file_is_rejected(self):
        content = (
            "directory_id,name,type,content_id,source_path\n"
            "{},no.txt,file,{},{}\n"
        ).format(DIR_A, CONTENT_A, SOURCE_PATH)
        with self.assertRaises(ValueError):
            list(dedup.parse_csv_file((FILE_0, content)))

    def test_local_spark_outputs_keep_all_source_rows(self):
        try:
            from pyspark.sql import SparkSession
        except ImportError:
            self.skipTest("PySpark is not installed")
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            inputs = root / "inputs"
            inputs.mkdir()
            write_csv(inputs / FILE_0, [
                (DIR_A, "main.p4", "file", CONTENT_A, SOURCE_PATH),
                (DIR_A, "main.p4", "file", CONTENT_A, SOURCE_PATH),
                (DIR_B, "include.p4", "file", CONTENT_A, SOURCE_PATH),
            ])
            write_csv(inputs / FILE_1, [
                (DIR_A, "main.p4", "file", CONTENT_A, SOURCE_PATH),
                (DIR_B, "other.p4", "file", CONTENT_B, SOURCE_PATH),
            ])
            output_prefix = str(root / "outputs") + "/"
            spark = (SparkSession.builder.master("local[2]")
                     .appName("swh-glue-dedup-test")
                     .config("spark.sql.shuffle.partitions", "2")
                     .getOrCreate())
            try:
                spark.sparkContext.setLogLevel("ERROR")
                with mock.patch.object(dedup, "INPUT_PREFIX", str(inputs) + "/"), \
                     mock.patch.object(dedup, "OUTPUT_PREFIX", output_prefix):
                    metrics = dedup.run_dedup(
                        spark, str(inputs / "part-*.csv"),
                        output_prefix + "test-run", 2
                    )
                self.assertEqual(5, metrics["source_row_count"])
                self.assertEqual(3, metrics["unique_hit_count"])
                self.assertEqual(2, metrics["unique_content_id_count"])
                self.assertEqual(2, metrics["unique_parent_directory_id_count"])
                source_rows = spark.read.parquet(
                    output_prefix + "test-run/source_rows"
                ).collect()
                self.assertEqual([1, 2, 3], sorted(
                    row.source_row for row in source_rows if row.source_shard == 0
                ))
                hits = spark.read.parquet(
                    output_prefix + "test-run/unique_hits"
                ).collect()
                self.assertEqual(3, next(
                    row.source_occurrences for row in hits
                    if row.directory_id == DIR_A and row.name == "main.p4"
                ))
                metric_file = next((root / "outputs/test-run/metrics").glob("part-*"))
                self.assertEqual(metrics, json.loads(metric_file.read_text()))
            finally:
                spark.stop()


if __name__ == "__main__":
    unittest.main()
