#!/usr/bin/env python3
"""Offline tests for the SWH P4 repository collector."""

from __future__ import print_function

import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import collect_swh_p4


class CollectSwhP4Test(unittest.TestCase):
    def test_keeps_complete_tree_and_deduplicates_directory(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            candidates = root / "candidates.jsonl"
            entries = root / "directory_entries.jsonl"
            content_root = root / "content"
            output = root / "out"
            content_root.mkdir()

            candidate_rows = [
                {
                    "origin_url": "https://github.com/example/one.git",
                    "revision_id": "old",
                    "directory_id": "dir-old",
                    "branch_name": "main",
                    "visit_date": "2023-01-01T00:00:00Z",
                },
                {
                    "origin_url": "https://github.com/example/one.git",
                    "revision_id": "new",
                    "directory_id": "dir-shared",
                    "branch_name": "main",
                    "visit_date": "2024-01-01T00:00:00Z",
                },
                {
                    "origin_url": "https://github.com/example/two.git",
                    "revision_id": "same-tree",
                    "directory_id": "dir-shared",
                    "branch_name": "master",
                    "visit_date": "2024-02-01T00:00:00Z",
                },
                {
                    "origin_url": "https://gitlab.com/example/not-selected.git",
                    "revision_id": "other",
                    "directory_id": "dir-other",
                    "branch_name": "main",
                },
            ]
            candidates.write_text(
                "\n".join(json.dumps(row) for row in candidate_rows) + "\n",
                encoding="utf-8",
            )
            entry_rows = [
                {"directory_id": "dir-shared", "name": "README.md", "type": "file", "target": "readme"},
                {"directory_id": "dir-shared", "name": "src", "type": "dir", "target": "dir-src"},
                {"directory_id": "dir-src", "name": "main.p4", "type": "file", "target": "main-p4"},
                {"directory_id": "dir-src", "name": "helper.txt", "type": "file", "target": "helper"},
            ]
            entries.write_text(
                "\n".join(json.dumps(row) for row in entry_rows) + "\n",
                encoding="utf-8",
            )
            (content_root / "main-p4").write_text(
                '#include <v1model.p4>\nparser P(packet_in b) { state start { transition accept; } }\n',
                encoding="utf-8",
            )
            (content_root / "readme").write_text("repository context\n", encoding="utf-8")

            status = collect_swh_p4.main(
                [
                    "--candidates",
                    str(candidates),
                    "--directory-entries",
                    str(entries),
                    "--content-root",
                    str(content_root),
                    "--output-root",
                    str(output),
                    "--materialize",
                    "--content-mode",
                    "all",
                    "--request-delay",
                    "0",
                ]
            )
            self.assertEqual(status, 0)

            summary = json.loads(
                (output / "discovery" / "discovery_summary.json").read_text(encoding="utf-8")
            )
            self.assertEqual(summary["candidate_count"], 2)
            self.assertEqual(summary["p4_repositories"], 1)
            self.assertEqual(summary["aliases"], 1)
            self.assertEqual(summary["errors"], 0)

            repositories = [
                json.loads(line)
                for line in (output / "manifests" / "repositories.jsonl").read_text(encoding="utf-8").splitlines()
                if line
            ]
            self.assertEqual(len(repositories), 1)
            self.assertEqual(repositories[0]["revision_id"], "new")
            self.assertTrue(repositories[0]["contains_p4"])
            self.assertTrue(repositories[0]["contains_p4_16"])
            self.assertEqual(repositories[0]["file_count"], 3)

            tree_path = output / repositories[0]["tree_manifest_path"]
            tree_paths = {
                json.loads(line)["path"]
                for line in tree_path.read_text(encoding="utf-8").splitlines()
                if line
            }
            self.assertEqual(tree_paths, {"README.md", "src", "src/main.p4", "src/helper.txt"})
            self.assertTrue(
                (output / "snapshots" / "swh_latest" / "example__one.git" / "new" / "README.md").is_file()
            )

            resumed_status = collect_swh_p4.main(
                [
                    "--candidates",
                    str(candidates),
                    "--directory-entries",
                    str(entries),
                    "--content-root",
                    str(content_root),
                    "--output-root",
                    str(output),
                    "--materialize",
                    "--content-mode",
                    "all",
                    "--request-delay",
                    "0",
                ]
            )
            self.assertEqual(resumed_status, 0)
            resumed_summary = json.loads(
                (output / "discovery" / "discovery_summary.json").read_text(encoding="utf-8")
            )
            self.assertEqual(resumed_summary["resumed_skips"], 2)
            self.assertEqual(
                len((output / "manifests" / "repositories.jsonl").read_text(encoding="utf-8").splitlines()),
                1,
            )

    def test_p4_16_gate_rejects_p4_14_before_full_materialization(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            candidates = root / "candidates.jsonl"
            entries = root / "entries.jsonl"
            content_root = root / "content"
            output = root / "out"
            content_root.mkdir()
            candidates.write_text(
                json.dumps(
                    {
                        "origin_url": "https://github.com/example/p4-14.git",
                        "revision_id": "legacy",
                        "directory_id": "root",
                        "branch_name": "main",
                    }
                )
                + "\n",
                encoding="utf-8",
            )
            entries.write_text(
                "\n".join(
                    json.dumps(row)
                    for row in [
                        {"directory_id": "root", "name": "main.p4", "type": "file", "target": "p4"},
                        {"directory_id": "root", "name": "README.md", "type": "file", "target": "readme"},
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            (content_root / "p4").write_text(
                "header_type ethernet_t { fields { dstAddr : 48; } }\n",
                encoding="utf-8",
            )
            (content_root / "readme").write_text("must not be fetched\n", encoding="utf-8")

            status = collect_swh_p4.main(
                [
                    "--candidates",
                    str(candidates),
                    "--directory-entries",
                    str(entries),
                    "--content-root",
                    str(content_root),
                    "--output-root",
                    str(output),
                    "--content-mode",
                    "all",
                    "--materialize",
                    "--request-delay",
                    "0",
                ]
            )
            self.assertEqual(status, 0)
            self.assertEqual(
                (output / "manifests" / "repositories.jsonl").read_text(encoding="utf-8"),
                "",
            )
            rejected = json.loads(
                (output / "manifests" / "rejected_repositories.jsonl")
                .read_text(encoding="utf-8")
                .strip()
            )
            self.assertFalse(rejected["accepted"])
            self.assertTrue(rejected["contains_p4"])
            self.assertFalse(rejected["contains_p4_16"])
            materialized_root = output / "snapshots" / "swh_latest" / "example__p4-14.git"
            self.assertFalse(materialized_root.exists())
            file_rows = [
                json.loads(line)
                for line in (output / "manifests" / "files.jsonl").read_text(encoding="utf-8").splitlines()
            ]
            readme = next(row for row in file_rows if row["path"] == "README.md")
            self.assertEqual(readme["payload_status"], "not_requested")


if __name__ == "__main__":
    unittest.main()
