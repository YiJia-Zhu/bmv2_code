#!/usr/bin/env python3
"""Offline tests for collect_gharchive_p4.py (no network access)."""

import io
import json
import sys
import tarfile
import tempfile
import unittest
from types import SimpleNamespace
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parent))
import collect_gharchive_p4 as collector  # noqa: E402


class FakeHttp:
    def __init__(self, commit_payload, tree_payload, blobs=None, archive_bytes=None):
        self.commit_payload = commit_payload
        self.tree_payload = tree_payload
        self.blobs = blobs or {}
        self.archive_bytes = archive_bytes

    def get_json(self, url):
        if "/git/commits/" in url:
            return self.commit_payload, {}
        if "/git/trees/" in url:
            return self.tree_payload, {}
        raise AssertionError("unexpected fake JSON URL: {}".format(url))

    def get_bytes(self, url, headers=None):
        path = url.split("/", 5)[-1]
        if "/" in path:
            path = path.split("/", 1)[1]
        if path in self.blobs:
            return self.blobs[path]
        raise AssertionError("unexpected fake blob URL: {}".format(url))

    def download(self, url, destination, headers=None):
        if self.archive_bytes is None:
            raise AssertionError("archive was not expected")
        destination = Path(destination)
        destination.parent.mkdir(parents=True, exist_ok=True)
        partial = destination.with_name(destination.name + ".partial")
        partial.write_bytes(self.archive_bytes)
        partial.replace(destination)
        return destination.stat().st_size, collector.sha256_file(destination)


class RecursiveTreeHttp(FakeHttp):
    def __init__(self, commit_payload, trees):
        super().__init__(commit_payload, {})
        self.trees = trees

    def get_json(self, url):
        if "/git/commits/" in url:
            return self.commit_payload, {}
        if "/git/trees/" in url:
            tree_sha = url.split("/git/trees/", 1)[1].split("?", 1)[0]
            return self.trees[tree_sha], {}
        raise AssertionError("unexpected fake JSON URL: {}".format(url))


def make_tar(files, prefix="demo-abc"):
    stream = io.BytesIO()
    with tarfile.open(fileobj=stream, mode="w:gz") as archive:
        root = tarfile.TarInfo(prefix)
        root.type = tarfile.DIRTYPE
        root.mode = 0o755
        archive.addfile(root)
        for path, data in files.items():
            info = tarfile.TarInfo(prefix + "/" + path)
            info.size = len(data)
            info.mode = 0o644
            archive.addfile(info, io.BytesIO(data))
    return stream.getvalue()


class CollectorTests(unittest.TestCase):
    def test_select_history_records_is_deterministic_and_bounded(self):
        events = [
            {"repository": "Owner/Repo", "head_sha": "old", "ref": "refs/heads/main", "created_at": "2026-01-01T00:00:00Z"},
            {"repository": "owner/repo", "head_sha": "new", "ref": "refs/heads/main", "created_at": "2026-01-02T00:00:00Z"},
            {"repository": "Owner/Repo", "head_sha": "tag", "ref": "refs/tags/v1", "created_at": "2026-01-03T00:00:00Z"},
        ]
        selected = collector.select_history_records(
            events,
            mode="window",
            max_per_repository=1,
            repositories=[{"repository": "Owner/Repo"}],
        )
        self.assertEqual(len(selected), 1)
        self.assertEqual(selected[0]["commit_sha"], "new")
        self.assertEqual(selected[0]["source_kind"], "gharchive_history")
        self.assertEqual(
            collector.select_history_records(
                events,
                mode="all",
                repositories=[{"repository": "other/repo"}],
            ),
            [],
        )

    def test_repository_normalization_and_numeric_p4_count(self):
        self.assertEqual(collector.normalize_repository("https://github.com/Owner/Repo.git/"), "Owner/Repo")
        merged = collector.merge_repository_records(
            [{"source_kind": "bigquery_github_repos", "repository": "Owner/Repo", "p4_file_count": "2"}]
        )
        self.assertTrue(merged[0]["contains_p4"])
        self.assertEqual(merged[0]["p4_verification_status"], "external_index")

    def test_gharchive_candidates_are_unverified_until_snapshot(self):
        candidates = collector.build_gharchive_candidates(
            [
                {
                    "event_key": "event-2",
                    "event_id": "event-2",
                    "event_type": "PushEvent",
                    "created_at": "2026-09-10T02:00:00Z",
                    "repository_id": 7,
                    "repository": "Owner/Repo",
                    "source_url": "https://data.gharchive.org/2026-09-10-2.json.gz",
                },
                {
                    "event_key": "event-1",
                    "event_id": "event-1",
                    "event_type": "IssuesEvent",
                    "created_at": "2026-09-10T01:00:00Z",
                    "repository_id": 7,
                    "repository": "owner/repo",
                    "source_url": "https://data.gharchive.org/2026-09-10-1.json.gz",
                },
            ],
            event_manifest="manifests/gharchive_repository_events.jsonl",
            discovered_at="2026-09-10T03:00:00Z",
        )
        self.assertEqual(len(candidates), 1)
        candidate = candidates[0]
        self.assertEqual(candidate["source_kind"], "gharchive_events")
        self.assertEqual(candidate["discovery_basis"], "gharchive_repository_event")
        self.assertIsNone(candidate["contains_p4"])
        self.assertEqual(candidate["p4_verification_status"], "unverified")
        self.assertEqual(candidate["gharchive_event_count"], 2)
        self.assertEqual(candidate["gharchive_event_types"], ["IssuesEvent", "PushEvent"])
        self.assertEqual(candidate["gharchive_first_seen"], "2026-09-10T01:00:00Z")
        self.assertEqual(candidate["gharchive_last_seen"], "2026-09-10T02:00:00Z")

    def test_source_aliases_and_snapshot_namespace(self):
        record = {
            "source_kind": "gharchive_events",
            "discovery_sources": ["gharchive_events"],
        }
        self.assertTrue(collector.source_matches(record, "gharchive_latest"))
        self.assertEqual(collector.snapshot_source_kind(record), "gharchive_latest")
        self.assertEqual(
            collector.snapshot_source_kind(record, "gharchive_events"),
            "gharchive_latest",
        )

    def test_gharchive_and_bigquery_merge_keeps_both_evidence_types(self):
        merged = collector.merge_repository_records(
            [
                {
                    "source_kind": "gharchive_events",
                    "repository": "Owner/Repo",
                    "contains_p4": None,
                    "p4_verification_status": "unverified",
                    "gharchive_event_count": 3,
                },
                {
                    "source_kind": "bigquery_github_repos",
                    "repository": "owner/repo",
                    "contains_p4": True,
                    "p4_file_count": 2,
                    "p4_verification_status": "external_index",
                },
            ]
        )
        self.assertEqual(merged[0]["discovery_sources"], ["bigquery_github_repos", "gharchive_events"])
        self.assertTrue(merged[0]["contains_p4"])
        self.assertEqual(merged[0]["p4_verification_status"], "external_index")

    def test_recursive_tree_fallback_reconstructs_paths(self):
        fake = RecursiveTreeHttp(
            {"tree": {"sha": "root"}},
            {
                "root": {
                    "tree": [
                        {"path": "src", "type": "tree", "sha": "src-tree"},
                        {"path": "README.md", "type": "blob", "sha": "readme", "size": 3},
                    ],
                    "truncated": True,
                },
                "src-tree": {
                    "tree": [{"path": "main.p4", "type": "blob", "sha": "p4", "size": 1}],
                },
            },
        )
        entries, truncated = collector.recursive_tree(fake, "Owner/Repo", "root")
        self.assertTrue(truncated)
        self.assertEqual(sorted(item["path"] for item in entries), ["README.md", "src", "src/main.p4"])

    def test_layered_lfs_pointer_p4_is_not_readable(self):
        pointer = b"version https://git-lfs.github.com/spec/v1\noid sha256:abc\nsize 10\n"
        entries = [{
            "path": "main.p4",
            "type": "blob",
            "mode": "100644",
            "sha": collector.git_blob_sha1(pointer),
            "size": len(pointer),
        }]
        fake = FakeHttp(
            {"tree": {"sha": "tree-root"}},
            {"tree": entries, "truncated": False},
            blobs={"main.p4": pointer},
        )
        with tempfile.TemporaryDirectory() as directory:
            result = collector.SnapshotCollector(Path(directory), fake, 1, 1000, 50 * 1024 * 1024).collect_one(
                {"repository": "Owner/Repo", "latest_commit": "abc", "default_branch": "main", "contains_p4": True},
                source_kind="test",
            )
            self.assertEqual(result["project_status"], "no_readable_p4")
            self.assertFalse(result["code_complete"])
            self.assertEqual(result["readable_p4_paths"], [])

    def test_same_tree_from_second_source_is_deduplicated_alias(self):
        data = b"header H {}\n"
        entries = [{
            "path": "main.p4",
            "type": "blob",
            "mode": "100644",
            "sha": collector.git_blob_sha1(data),
            "size": len(data),
        }]
        fake = FakeHttp(
            {"tree": {"sha": "tree-root"}},
            {"tree": entries, "truncated": False},
            archive_bytes=make_tar({"main.p4": data}),
        )
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            snapshots = collector.SnapshotCollector(root, fake, 10**9, 1000, 50 * 1024 * 1024)
            first = snapshots.collect_one(
                {"repository": "Owner/Repo", "latest_commit": "abc", "default_branch": "main", "contains_p4": True},
                source_kind="github_candidates",
            )
            alias = snapshots.collect_one(
                {"repository": "Owner/Repo", "latest_commit": "abc", "default_branch": "main", "contains_p4": True},
                source_kind="p4lang_latest",
            )
            self.assertEqual(first["status"], "downloaded")
            self.assertEqual(alias["status"], "deduplicated_alias")
            self.assertEqual(alias["duplicate_of_snapshot_id"], first["snapshot_id"])

    def test_failed_snapshot_marker_is_retryable(self):
        data = b"header H {}\n"
        entries = [{
            "path": "main.p4",
            "type": "blob",
            "mode": "100644",
            "sha": collector.git_blob_sha1(data),
            "size": len(data),
        }]
        fake = FakeHttp(
            {"tree": {"sha": "tree-root"}},
            {"tree": entries, "truncated": False},
            archive_bytes=make_tar({"main.p4": data}),
        )
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            destination = root / "snapshots/test/Owner__Repo/abc"
            destination.mkdir(parents=True)
            (destination / "snapshot.json").write_text(
                json.dumps({"status": "code_incomplete"}), encoding="utf-8"
            )
            result = collector.SnapshotCollector(root, fake, 10**9, 1000, 50 * 1024 * 1024).collect_one(
                {"repository": "Owner/Repo", "latest_commit": "abc", "default_branch": "main", "contains_p4": True},
                source_kind="test",
            )
            self.assertEqual(result["status"], "downloaded")
            self.assertEqual(result["project_status"], "eligible")


    def test_path_classification_keeps_non_p4_code_context(self):
        self.assertEqual(collector.classify_path("src/main.p4", 100), "code")
        self.assertEqual(collector.classify_path("control/test.py", 100), "code")
        self.assertEqual(collector.classify_path("p4info/demo.json", 100), "code")
        self.assertEqual(collector.classify_path("README.md", 100), "context")
        self.assertEqual(collector.classify_path("data/results.json", 100), "optional")
        self.assertEqual(collector.classify_path("build/generated.py", 100), "optional")
        self.assertEqual(collector.classify_path("build/main.p4", 100), "code")
        self.assertEqual(collector.classify_path("images/logo.png", 100), "optional")

    def test_push_event_and_repository_merge(self):
        event = {
            "id": "event-1",
            "type": "PushEvent",
            "created_at": "2026-01-01T00:00:00Z",
            "repo": {"id": 7, "name": "Owner/Repo"},
            "payload": {"ref": "refs/heads/main", "head": "abc", "before": "def", "size": 1},
        }
        push = collector.extract_push_event(event)
        self.assertEqual(push["repository"], "Owner/Repo")
        merged = collector.merge_repository_records(
            [
                {"source_kind": "bigquery_github_repos", "repository": "Owner/Repo", "p4_file_count": 1, "example_paths": ["main.p4"]},
                {"source_kind": "p4lang_org", "repository": "Owner/Repo", "default_branch": "main"},
            ]
        )
        self.assertEqual(len(merged), 1)
        self.assertEqual(merged[0]["discovery_sources"], ["bigquery_github_repos", "p4lang_org"])
        self.assertTrue(merged[0]["contains_p4"])

    def test_full_archive_snapshot_is_project_eligible(self):
        files = {
            "main.p4": b'#include "support.p4"\nheader H {}\n',
            "support.p4": b"header S {}\n",
            "control/test.py": b"print('ok')\n",
            "p4info/demo.json": b'{"actions": []}\n',
            "images/logo.png": b"\x89PNG\r\n",
        }
        entries = []
        for path, data in files.items():
            entries.append({"path": path, "type": "blob", "mode": "100644", "sha": collector.git_blob_sha1(data), "size": len(data)})
        entries.append({"path": "control", "type": "tree", "mode": "040000", "sha": "tree-control"})
        entries.append({"path": "p4info", "type": "tree", "mode": "040000", "sha": "tree-p4info"})
        entries.append({"path": "images", "type": "tree", "mode": "040000", "sha": "tree-images"})
        archive = make_tar(files)
        fake = FakeHttp(
            {"tree": {"sha": "tree-root"}},
            {"tree": entries, "truncated": False},
            archive_bytes=archive,
        )
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            result = collector.SnapshotCollector(root, fake, 10**9, 1000, 50 * 1024 * 1024).collect_one(
                {"repository": "Owner/Repo", "latest_commit": "abc", "default_branch": "main", "contains_p4": True},
                source_kind="test",
            )
            self.assertEqual(result["status"], "downloaded")
            self.assertEqual(result["project_status"], "eligible")
            self.assertTrue(result["full_content"])
            self.assertTrue(result["code_complete"])
            self.assertEqual(result["p4_paths"], ["main.p4", "support.p4"])
            self.assertTrue((root / result["tree_manifest_path"]).is_file())
            self.assertTrue((root / "manifests/files.jsonl").is_file())

    def test_layered_mode_fetches_code_and_include_context(self):
        files = {
            "main.p4": b'#include "support.p4"\nheader H {}\n',
            "support.p4": b"header S {}\n",
            "control/test.py": b"print('ok')\n",
            "p4info/demo.json": b'{"actions": []}\n',
            "data/results.json": b"[1, 2, 3]\n",
        }
        entries = [
            {"path": path, "type": "blob", "mode": "100644", "sha": collector.git_blob_sha1(data), "size": len(data)}
            for path, data in files.items()
        ]
        fake = FakeHttp(
            {"tree": {"sha": "tree-root"}},
            {"tree": entries, "truncated": False},
            blobs=files,
        )
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            result = collector.SnapshotCollector(root, fake, 1, 1000, 50 * 1024 * 1024).collect_one(
                {"repository": "Owner/Repo", "latest_commit": "abc", "default_branch": "main", "contains_p4": True},
                source_kind="test",
            )
            self.assertEqual(result["content_mode"], "layered_code_payload")
            self.assertEqual(result["project_status"], "eligible")
            self.assertFalse(result["full_content"])
            self.assertTrue(result["code_complete"])
            self.assertTrue((root / "snapshots/test/Owner__Repo/abc/code_payload/main.p4").is_file())
            self.assertTrue((root / "snapshots/test/Owner__Repo/abc/code_payload/support.p4").is_file())
            data_result = json.loads((root / "manifests/files.jsonl").read_text(encoding="utf-8").splitlines()[-1])
            self.assertEqual(data_result["path"], "data/results.json")
            self.assertEqual(data_result["content_status"], "omitted_optional")
            collector.command_build_context(
                SimpleNamespace(
                    output_root=root,
                    snapshots=root / "manifests/snapshots.jsonl",
                    replace=False,
                )
            )
            self.assertTrue((root / "derived/p4_context/test__Owner__Repo__abc/main.p4").is_file())
            self.assertFalse((root / "derived/p4_context/test__Owner__Repo__abc/data/results.json").exists())
            collector.command_build_context(
                SimpleNamespace(
                    output_root=root,
                    snapshots=root / "manifests/snapshots.jsonl",
                    replace=False,
                )
            )
            project_lines = (root / "manifests/derived_projects.jsonl").read_text(encoding="utf-8").splitlines()
            self.assertEqual(len(project_lines), 1)


if __name__ == "__main__":
    unittest.main()
