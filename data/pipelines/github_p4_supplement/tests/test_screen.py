"""Offline checks for resumable GitHub P4 file screening."""

import hashlib
import json
from pathlib import Path
import sqlite3
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from data.pipelines.github_p4_supplement import screen


class FakePool:
    def __init__(self, responses):
        self.responses = responses
        self.calls = []

    def get_json(self, path):
        self.calls.append(path)
        return self.responses[path]


class ScreenTests(unittest.TestCase):
    def test_truncated_recursive_tree_falls_back_to_subtrees(self):
        pool = FakePool({
            "/repos/org/repo/git/trees/main?recursive=1": {
                "sha": "root", "truncated": True, "tree": []},
            "/repos/org/repo/git/trees/root": {
                "sha": "root", "truncated": False, "tree": [
                    {"path": "src", "type": "tree", "sha": "child"},
                    {"path": "README.md", "type": "blob", "sha": "readme"}]},
            "/repos/org/repo/git/trees/child": {
                "sha": "child", "truncated": False, "tree": [
                    {"path": "main.P4", "type": "blob", "sha": "blob",
                     "mode": "100644"}]},
        })
        sha, files = screen.tree_files(pool, "org/repo", "main")
        self.assertEqual(sha, "root")
        self.assertEqual(files, [("src/main.P4", "blob", "100644")])
        self.assertEqual(len(pool.calls), 3)

    def test_large_tree_is_left_for_review(self):
        pool = FakePool({
            "/repos/org/repo/git/trees/main?recursive=1": {
                "sha": "root", "truncated": True, "tree": []},
            "/repos/org/repo/git/trees/root": {
                "sha": "root", "truncated": False, "tree": [
                    {"path": "src", "type": "tree", "sha": "child"}]},
        })
        with patch.object(screen, "MAX_TREE_REQUESTS_PER_REPO", 2):
            with self.assertRaises(screen.ApiError) as context:
                screen.tree_files(pool, "org/repo", "main")
        self.assertEqual(context.exception.status, "tree_limit")

    def test_blob_reuses_verified_cache_and_keeps_quality_separate(self):
        data = b"#include <core.p4>\nparser P(packet_in p) { }\n"
        oid = screen.git_blob_sha1(data)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = screen.blob_cache_path(root, oid)
            path.parent.mkdir(parents=True)
            path.write_bytes(data)
            result = screen.fetch_blob(FakePool({}), oid, ["org/repo"], root)
            self.assertEqual(result["screen"]["version_status"], "p4_16_likely")
            self.assertEqual(result["quality"]["quality_status"], "pass")
            connection = screen.connect(root / "screen.sqlite3")
            try:
                connection.execute("INSERT INTO blobs(oid) VALUES(?)", (oid,))
                screen.save_blob(connection, oid, result, None)
                row = connection.execute("SELECT status,screen_json FROM blobs WHERE oid=?",
                                         (oid,)).fetchone()
                self.assertEqual(row["status"], "complete")
                self.assertEqual(json.loads(row["screen_json"])["content_id"], oid)
            finally:
                connection.close()

    def test_init_and_tree_checkpoint_preserve_source_hits(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            search_db = root / "search.sqlite3"
            source = sqlite3.connect(str(search_db))
            source.executescript("""
                CREATE TABLE jobs(status TEXT);
                CREATE TABLE repositories(github_id INTEGER,full_name TEXT,
                    swh_name_match INTEGER);
                CREATE TABLE hits(seed TEXT,github_id INTEGER,path TEXT,blob_sha TEXT);
                INSERT INTO jobs VALUES('complete');
                INSERT INTO repositories VALUES(42,'org/repo',0);
            """)
            data = b"#include <core.p4>\n"
            oid = hashlib.sha1(b"blob " + str(len(data)).encode() + b"\0" + data).hexdigest()
            source.execute("INSERT INTO hits VALUES(?,?,?,?)",
                           ("extension_p4", 42, "old/main.p4", oid))
            source.commit()
            source.close()
            connection = screen.connect(root / "screen.sqlite3")
            try:
                screen.initialize(connection, search_db)
                row = connection.execute("SELECT * FROM repositories WHERE github_id=42").fetchone()
                screen.save_tree(connection, row, {
                    "name": "org/renamed", "status": "complete", "tree_sha": "tree",
                    "head_sha": "commit", "files": [("new/main.p4", oid, "100644")]}, None)
                self.assertEqual(connection.execute(
                    "SELECT COUNT(*) FROM files WHERE github_id=42").fetchone()[0], 2)
                self.assertEqual(connection.execute(
                    "SELECT COUNT(*) FROM blobs").fetchone()[0], 1)
                self.assertEqual(tuple(connection.execute(
                    "SELECT tree_status,full_name FROM repositories WHERE github_id=42"
                    ).fetchone()), ("complete", "org/renamed"))
            finally:
                connection.close()

    def test_transient_error_is_attempted_once_per_run(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            connection = screen.connect(root / "screen.sqlite3")
            try:
                with connection:
                    for github_id in range(3):
                        connection.execute(
                            "INSERT INTO repositories(github_id,full_name,swh_name_match)"
                            " VALUES(?,?,0)", (github_id, "org/repo{}".format(github_id)))
                pool = FakePool({})
                pool.requests = {}
                with patch.object(screen, "fetch_tree",
                                  side_effect=screen.ApiError("network", "timeout")), \
                     patch.object(screen.shutil, "disk_usage",
                                  return_value=SimpleNamespace(free=101 * 1024 ** 3)):
                    screen.run_phase(connection, root, pool, "trees", 2, 0)
                self.assertEqual(connection.execute(
                    "SELECT SUM(attempts) FROM repositories").fetchone()[0], 3)
                self.assertEqual(connection.execute(
                    "SELECT COUNT(*) FROM repositories WHERE tree_status='pending'"
                    ).fetchone()[0], 3)
            finally:
                connection.close()


if __name__ == "__main__":
    unittest.main()
