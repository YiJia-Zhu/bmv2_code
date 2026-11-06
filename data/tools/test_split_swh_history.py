#!/usr/bin/env python3
"""Offline tests for four-machine checkpoint handoff."""

import csv
import hashlib
import sqlite3
import subprocess
import tempfile
import unittest
from pathlib import Path

from split_swh_history import prepare, write_runner
from swh_history_common import State, atomic_json, stable_id


class SplitHistoryTests(unittest.TestCase):
    def test_complete_checkpoint_partition_and_resume(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source"
            source.mkdir()
            input_path = root / "input.csv"
            with input_path.open("w", newline="") as output:
                writer = csv.DictWriter(output, fieldnames=("origin_id", "origin_url"))
                writer.writeheader()
                for number in range(12):
                    writer.writerow({"origin_id": "{:040x}".format(number + 1),
                                     "origin_url": "https://github.com/org/repo{}".format(number)})
                writer.writerow({"origin_id": "{:040x}".format(99),
                                 "origin_url": "https://pkg.go.dev/github.com/org/package"})
            state = State(source)
            state.import_input(input_path, 0, 1)
            repos = state.rows("SELECT * FROM repos ORDER BY repo_id")
            complete, failed, inaccessible = repos[:3]
            for repo, status in ((complete, "complete"), (failed, "failed"),
                                 (inaccessible, "inaccessible")):
                state.execute("UPDATE repos SET status=?,attempts=5 WHERE repo_id=?",
                              (status, repo["repo_id"]))
                folder = source / "repos" / repo["repo_id"]
                (folder / "mirror.git").mkdir(parents=True)
                (folder / "mirror.git/config").write_text("[core]\n")
                if status == "complete":
                    (folder / "mirror_complete.json").write_text("{}\n")
                state.enqueue_api(repo, "https://api.github.com/repos/" +
                                  repo["repo_path"] + "/issues", "issues")
            task = state.rows("SELECT * FROM api_tasks WHERE repo_id=?", (complete["repo_id"],))[0]
            state.execute("UPDATE api_tasks SET status='complete' WHERE task_id=?", (task["task_id"],))
            failed_task = state.rows("SELECT * FROM api_tasks WHERE repo_id=?", (failed["repo_id"],))[0]
            state.execute("UPDATE api_tasks SET status='failed',attempts=5 WHERE task_id=?",
                          (failed_task["task_id"],))
            page = source / "trackers" / complete["repo_id"] / "pages" / (task["task_id"] + ".json")
            page.parent.mkdir(parents=True)
            page.write_text("{}\n")
            origin_id = "{:040x}".format(99)
            asset_id = stable_id("https://example.org/archive.tar.gz")
            state.execute("INSERT INTO assets(asset_id,origin_id,url,filename,status) VALUES (?,?,?,?,?)",
                          (asset_id, origin_id, "https://example.org/archive.tar.gz", "archive.tar.gz", "retry_wait"))
            archive = source / "packages" / origin_id / "archives" / (asset_id[:16] + "-archive.tar.gz.part")
            archive.parent.mkdir(parents=True)
            archive.write_bytes(b"partial")
            state.db.close()
            digest = hashlib.sha256(input_path.read_bytes()).hexdigest()
            atomic_json(source / "run_config.json", {"input": str(input_path),
                        "shard_index": 0, "shard_count": 1})
            atomic_json(source / "status.json", {"final": True, "updated_at": "2026-10-27T00:00:00+00:00"})
            atomic_json(source / "manifests/input.json", {"sha256": digest, "shard_index": 0,
                        "shard_count": 1, "raw_input_rows": 13})
            manifest = prepare(source, root / "shards")

            self.assertEqual(len(manifest["shards"]), 4)
            seen_repos = set()
            seen_origins = set()
            for index in range(4):
                shard = root / "shards" / "shard_{}".format(index)
                db = sqlite3.connect(str(shard / "manifests/state.sqlite3"))
                repo_ids = {row[0] for row in db.execute("SELECT repo_id FROM repos")}
                origin_ids = {row[0] for row in db.execute("SELECT origin_id FROM origins")}
                self.assertFalse(seen_repos & repo_ids)
                self.assertFalse(seen_origins & origin_ids)
                seen_repos.update(repo_ids)
                seen_origins.update(origin_ids)
                self.assertTrue(all(int(repo_id, 16) % 4 == index for repo_id in repo_ids))
                self.assertTrue((shard / "run.sh").exists())
                subprocess.run(["bash", "-n", str(shard / "run.sh")], check=True)
                runner = (shard / "run.sh").read_text()
                self.assertIn('PROJECT_ROOT="$(cd -- "$ROOT/../../../../.." && pwd)"', runner)
                self.assertIn('${SWH_HISTORY_ENV_FILE:-$PROJECT_ROOT/.env}', runner)
                subprocess.run(["python3", str(shard / "collector/download_swh_history.py"),
                                "--help"], check=True, stdout=subprocess.DEVNULL)
                self.assertIn('"GITHUB_API_TOKEN"',
                              (shard / "collector/swh_history_api.py").read_text())
                db.close()
                resume = State(shard)
                resume.import_input(shard / "input.csv", index, 4)
                resume.recover()
                resume.db.close()
            self.assertEqual(len(seen_repos), 12)
            self.assertEqual(len(seen_origins), 13)
            complete_shard = root / "shards" / "shard_{}".format(int(complete["repo_id"], 16) % 4)
            self.assertTrue((complete_shard / "repos" / complete["repo_id"] /
                             "mirror_complete.json").exists())
            self.assertTrue((complete_shard / "trackers" / complete["repo_id"] / "pages" /
                             (task["task_id"] + ".json")).exists())
            failed_shard = root / "shards" / "shard_{}".format(int(failed["repo_id"], 16) % 4)
            db = sqlite3.connect(str(failed_shard / "manifests/state.sqlite3"))
            self.assertEqual(db.execute("SELECT status,attempts FROM api_tasks WHERE task_id=?",
                                        (failed_task["task_id"],)).fetchone(), ("retry_wait", 0))
            db.close()
            package_shard = root / "shards" / "shard_{}".format(int(origin_id, 16) % 4)
            self.assertTrue((package_shard / "packages" / origin_id / "archives" / archive.name).exists())
            for repo in (failed, inaccessible):
                shard = root / "shards" / "shard_{}".format(int(repo["repo_id"], 16) % 4)
                db = sqlite3.connect(str(shard / "manifests/state.sqlite3"))
                self.assertEqual(db.execute("SELECT status,attempts FROM repos WHERE repo_id=?",
                                            (repo["repo_id"],)).fetchone(), ("retry_wait", 0))
                db.close()

    def test_refuses_live_checkpoint(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source"
            source.mkdir()
            atomic_json(source / "status.json", {"final": False})
            with self.assertRaises(ValueError):
                prepare(source, root / "shards")

    def test_runner_requires_checkpoint(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            write_runner(root, 2, 4)
            (root / "input.csv").write_text("origin_id,origin_url\n")
            result = subprocess.run(["bash", str(root / "run.sh")],
                                    stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                    universal_newlines=True)
            self.assertEqual(result.returncode, 2)
            self.assertIn("manifests/state.sqlite3", result.stderr)


if __name__ == "__main__":
    unittest.main()
