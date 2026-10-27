#!/usr/bin/env python3
"""Offline tests for the budget-limited SWH Graph HTTP mapper."""

import csv
import os
import tempfile
import unittest
from unittest.mock import patch

from data.tools import map_swh_candidates_via_graph_api as mapper


class GraphHttpMapperTest(unittest.TestCase):
    def test_candidate_preparation_keeps_only_candidate_rows(self):
        with tempfile.TemporaryDirectory() as root:
            uri = "s3://swh2p4/candidate_csv/source_shard=0/part.csv"
            local = os.path.join(root, "source.csv")
            fields = ["directory_id", "name", "content_id", "discovery_disposition",
                      "source_csv", "source_row"]
            directory_id = "a" * 40
            with open(local, "w", newline="", encoding="utf-8") as output:
                writer = csv.DictWriter(output, fieldnames=fields)
                writer.writeheader()
                writer.writerow(dict(directory_id=directory_id, name="main.p4",
                                     content_id="b" * 40,
                                     discovery_disposition="p4_16_candidate",
                                     source_csv="original.csv", source_row="12"))
                writer.writerow(dict(directory_id=directory_id, name="old.p4",
                                     content_id="c" * 40,
                                     discovery_disposition="p4_14_likely"))

            class FakeS3:
                def head_object(self, Bucket, Key):
                    return {"ContentLength": os.path.getsize(local)}

                def download_file(self, Bucket, Key, Filename):
                    with open(local, "rb") as source, open(Filename, "wb") as target:
                        target.write(source.read())

            with patch.object(mapper, "s3_client", return_value=FakeS3()):
                path, directories, count = mapper.load_candidate_csvs([uri], root, "us-east-1")
            self.assertEqual(1, count)
            self.assertEqual([directory_id], directories)
            with open(path, newline="", encoding="utf-8") as source:
                rows = list(csv.DictReader(source))
            self.assertEqual(1, len(rows))
            self.assertEqual("original.csv", rows[0]["source_csv"])
            self.assertEqual("12", rows[0]["source_row"])

    def test_rows_with_no_origin_are_audited(self):
        with tempfile.TemporaryDirectory() as root:
            source_path = os.path.join(root, "hits.csv")
            directory_a = "a" * 40
            directory_b = "b" * 40
            with open(source_path, "w", newline="", encoding="utf-8") as output:
                writer = csv.DictWriter(output, fieldnames=mapper.MAPPING_COLUMNS[:8])
                writer.writeheader()
                for directory_id in (directory_a, directory_b):
                    writer.writerow(dict(source_shard="0", source_csv="part.csv",
                                         source_row="1", directory_id=directory_id,
                                         name="main.p4", content_id="c" * 40,
                                         version_status="p4_16_candidate",
                                         quality_status="excluded"))
            origin = "d" * 40
            cache = {
                directory_a: {"status": "complete", "origin_ids": [origin]},
                directory_b: {"status": "complete", "origin_ids": []},
            }
            mapping, repositories, unmapped = mapper.write_outputs(
                source_path, [directory_a, directory_b], cache,
                {origin: "https://github.com/team/p4repo.git"}, root)
            with open(mapping, newline="", encoding="utf-8") as source:
                rows = list(csv.DictReader(source))
            self.assertEqual(1, len(rows))
            self.assertEqual("p4repo", rows[0]["repository_name"])
            self.assertEqual("excluded", rows[0]["quality_status"])
            with open(repositories, newline="", encoding="utf-8") as source:
                self.assertEqual(1, len(list(csv.DictReader(source))))
            with open(unmapped, newline="", encoding="utf-8") as source:
                rows = list(csv.DictReader(source))
            self.assertEqual("no_origin_in_graph", rows[0]["unmapped_reason"])


if __name__ == "__main__":
    unittest.main()
