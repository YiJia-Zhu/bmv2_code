#!/usr/bin/env python3
"""Offline tests for SWH S3/Athena discovery input normalization."""

from __future__ import print_function

import csv
import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import discover_swh_p4


class DiscoverSwhP4Test(unittest.TestCase):
    def test_iter_p4_hits_filters_type_and_suffix(self):
        rows = [
            ("dir-a", b"main.P4", "file", "blob-a"),
            ("dir-a", b"p4-not-source.txt", "file", "blob-b"),
            ("dir-a", b"fake.p4", "dir", "dir-b"),
        ]
        hits, row_count, complete = discover_swh_p4.iter_p4_hits(rows, "test")
        self.assertEqual(row_count, 3)
        self.assertTrue(complete)
        self.assertEqual(
            hits,
            [
                {
                    "directory_id": "dir-a",
                    "name": "main.P4",
                    "type": "file",
                    "content_id": "blob-a",
                    "source_kind": "test",
                }
            ],
        )

    def test_athena_import_emits_hits_and_mapped_repository_candidates(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "athena.csv"
            output = root / "out"
            rows = [
                {
                    "directory_id": "dir-src",
                    "name": "main.p4",
                    "type": "file",
                    "content_id": "blob-p4",
                    "origin_url": "https://github.com/example/repo.git",
                    "revision_id": "revision-1",
                    "root_directory_id": "dir-root",
                    "branch_name": "main",
                },
                {
                    "directory_id": "dir-src",
                    "name": "README.md",
                    "type": "file",
                    "content_id": "blob-readme",
                    "origin_url": "https://github.com/example/repo.git",
                    "revision_id": "revision-1",
                    "root_directory_id": "dir-root",
                    "branch_name": "main",
                },
            ]
            with source.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
                writer.writeheader()
                writer.writerows(rows)

            status = discover_swh_p4.main(
                [
                    "athena-import",
                    "--input",
                    str(source),
                    "--output-root",
                    str(output),
                    "--require-mapped-repositories",
                ]
            )
            self.assertEqual(status, 0)
            hits = [
                json.loads(line)
                for line in (output / "p4_directory_hits.jsonl").read_text(encoding="utf-8").splitlines()
            ]
            candidates = [
                json.loads(line)
                for line in (output / "swh_candidate_revisions.jsonl").read_text(encoding="utf-8").splitlines()
            ]
            self.assertEqual(len(hits), 1)
            self.assertEqual(len(candidates), 1)
            self.assertEqual(candidates[0]["directory_id"], "dir-root")
            summary = json.loads(
                (output / "athena_import_summary.json").read_text(encoding="utf-8")
            )
            self.assertTrue(summary["candidate_manifest_ready"])
            self.assertFalse(summary["collector_ready"])

    def test_hit_only_athena_export_is_not_collector_ready(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "hits.jsonl"
            output = root / "out"
            source.write_text(
                json.dumps(
                    {
                        "directory_id": "dir-src",
                        "name": "main.p4",
                        "type": "file",
                        "content_id": "blob-p4",
                    }
                )
                + "\n",
                encoding="utf-8",
            )
            status = discover_swh_p4.main(
                [
                    "athena-import",
                    "--input",
                    str(source),
                    "--output-root",
                    str(output),
                    "--require-mapped-repositories",
                ]
            )
            self.assertEqual(status, 2)
            summary = json.loads(
                (output / "athena_import_summary.json").read_text(encoding="utf-8")
            )
            self.assertFalse(summary["collector_ready"])

    def test_partially_mapped_hits_are_not_collector_ready(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "mixed.jsonl"
            output = root / "out"
            rows = [
                {
                    "directory_id": "dir-a",
                    "name": "a.p4",
                    "type": "file",
                    "content_id": "blob-a",
                    "origin_url": "https://github.com/example/a.git",
                    "revision_id": "rev-a",
                    "root_directory_id": "root-a",
                },
                {
                    "directory_id": "dir-b",
                    "name": "b.p4",
                    "type": "file",
                    "content_id": "blob-b",
                },
            ]
            source.write_text(
                "\n".join(json.dumps(row) for row in rows) + "\n",
                encoding="utf-8",
            )
            status = discover_swh_p4.main(
                [
                    "athena-import",
                    "--input",
                    str(source),
                    "--output-root",
                    str(output),
                    "--require-mapped-repositories",
                ]
            )
            self.assertEqual(status, 2)
            summary = json.loads(
                (output / "athena_import_summary.json").read_text(encoding="utf-8")
            )
            self.assertEqual(summary["p4_hit_rows_without_repository_mapping"], 1)
            self.assertFalse(summary["collector_ready"])


if __name__ == "__main__":
    unittest.main()
