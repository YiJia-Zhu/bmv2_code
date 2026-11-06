#!/usr/bin/env python3
"""Offline checks for durable history capture and deterministic partitioning."""

import argparse
import csv
import json
import io
import sqlite3
import subprocess
import tempfile
import threading
import time
import unittest
import urllib.error
from pathlib import Path
from unittest import mock

from download_swh_history import Collector
from swh_history_api import API, RetryRequest
from swh_history_common import State, git_source


class HistoryTests(unittest.TestCase):
    def make_input(self, root, urls):
        destination = root / "input.csv"
        with destination.open("w", newline="") as output:
            writer = csv.DictWriter(output, fieldnames=["origin_id", "origin_url"])
            writer.writeheader()
            for number, url in enumerate(urls):
                writer.writerow({"origin_id": "{:040x}".format(number + 1), "origin_url": url})
        return destination

    def test_conservative_aliases(self):
        self.assertEqual(git_source("https://github.com/Org/Repo.git")["repo_id"],
                         git_source("http://github.com/org/repo/")["repo_id"])
        self.assertIsNone(git_source("https://pkg.go.dev/github.com/org/repo"))
        self.assertIsNone(git_source("https://github.com/org/repo/tree/main"))
        self.assertIsNone(git_source("https://password@github.com/org/repo"))
        self.assertEqual(git_source("https://git.launchpad.net/freerouter")["clone_url"],
                         "https://git.launchpad.net/freerouter")

    def test_partition_and_resume(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = self.make_input(root, ["https://github.com/org/repo{}".format(n) for n in range(20)] +
                                     ["https://github.com/ORG/REPO1.git"])
            whole = State(root / "whole")
            whole.import_input(source, 0, 1)
            expected = {row["repo_id"] for row in whole.rows("SELECT repo_id FROM repos")}
            pieces = []
            for shard in range(3):
                state = State(root / str(shard))
                state.import_input(source, shard, 3)
                state.import_input(source, shard, 3)
                pieces.append({row["repo_id"] for row in state.rows("SELECT repo_id FROM repos")})
            self.assertEqual(set.union(*pieces), expected)
            self.assertFalse(pieces[0] & pieces[1] or pieces[1] & pieces[2] or pieces[0] & pieces[2])
            repo_id = next(iter(expected))
            whole.execute("UPDATE repos SET status='running',attempts=1 WHERE repo_id=?", (repo_id,))
            whole.recover()
            self.assertEqual(whole.rows("SELECT status FROM repos WHERE repo_id=?", (repo_id,))[0]["status"], "retry_wait")
            with self.assertRaises(ValueError):
                whole.import_input(source, 0, 2)

    def test_api_page_replay_keeps_children(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            state = State(root)
            source = self.make_input(root, ["https://github.com/org/repo"])
            state.import_input(source, 0, 1)
            repo = state.rows("SELECT * FROM repos")[0]
            api = API(state, "", root / ".env")
            state.enqueue_api(repo, "https://api.github.com/repos/org/repo/issues?state=all&page=1", "issues")
            task = state.rows("SELECT * FROM api_tasks")[0]
            task["attempts"] = 1
            def response(url):
                return ([{"number": 4}], {"headers": {"Link": '<https://api.github.com/repos/org/repo/issues?state=all&page=2>; rel="next"'}, "http_status": 200})
            api.request = response
            api.run_task(task)
            self.assertEqual(len(state.rows("SELECT * FROM api_tasks")), 5)
            api.request = lambda url: self.fail("completed page replay must not download again")
            api.run_task(task)
            self.assertEqual(len(state.rows("SELECT * FROM api_tasks")), 5)
            self.assertEqual(state.rows("SELECT status FROM api_tasks WHERE task_id=?", (task["task_id"],))[0]["status"], "complete")

    def test_secondary_limit_and_token_reload(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            state = State(root)
            env = root / ".env"
            api = API(state, "", env)
            class Limited:
                def open(self, request, timeout):
                    raise urllib.error.HTTPError(request.full_url, 403, "Forbidden", {},
                                                 io.BytesIO(b'{"message":"secondary rate limit"}'))
            api.opener = Limited()
            with self.assertRaises(RetryRequest):
                api.request("https://api.github.com/repos/offline/example")
            self.assertGreater(api.next_at["api.github.com"], 0)
            api.refresh_credentials()
            env.write_text("GITHUB_TOKEN=offline-dummy-test-token\n")
            api.refresh_credentials()
            self.assertNotIn("api.github.com", api.next_at)
            self.assertNotIn("offline-dummy-test-token", (root / "events.jsonl").read_text())

    def test_github_api_token_alias_and_shared_request_pacing(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            state = State(root)
            env = root / ".env"
            env.write_text("GITHUB_API_TOKEN=offline-dummy-test-token\n")
            api = API(state, "", env, github_requests_per_hour=4500)
            response = mock.MagicMock()
            response.__enter__.return_value = response
            response.read.return_value = b'{}'
            response.headers = {"Content-Type": "application/json", "X-RateLimit-Remaining": "4998"}
            response.status = 200
            response.geturl.return_value = "https://api.github.com/repos/offline/example"
            api.opener = mock.Mock()
            api.opener.open.return_value = response
            with mock.patch.dict("os.environ", {"GITHUB_TOKEN": "", "GH_TOKEN": "", "GITHUB_API_TOKEN": ""}):
                with mock.patch("swh_history_api.time.time", return_value=1000) as clock:
                    api.request(response.geturl())
                    request = api.opener.open.call_args[0][0]
                    self.assertEqual(request.get_header("Authorization"), "Bearer offline-dummy-test-token")
                    self.assertAlmostEqual(api.next_at["api.github.com"], 1000.8)
                    clock.return_value = 1000.5
                    with self.assertRaises(RetryRequest):
                        api.request(response.geturl())
                    self.assertEqual(api.opener.open.call_count, 1)
                    clock.return_value = 1000.8
                    api.request(response.geturl())
                    self.assertEqual(api.opener.open.call_count, 2)
                    # An in-flight response must not shorten another worker's cooldown.
                    api.next_at.clear()
                    response.headers.update({"X-RateLimit-Remaining": "0", "X-RateLimit-Reset": "1200"})
                    def other_worker_wait(*args, **kwargs):
                        api.next_at["api.github.com"] = 2000
                        return response
                    api.opener.open.side_effect = other_worker_wait
                    api.request(response.geturl())
                    self.assertEqual(api.next_at["api.github.com"], 2000)
            state.db.close()

    def test_initial_github_token_requeues_unauthorized_pages(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            state = State(root)
            source = self.make_input(root, ["https://github.com/offline/example"])
            state.import_input(source, 0, 1)
            repo = state.rows("SELECT * FROM repos")[0]
            env = root / ".env"
            env.write_text("GITHUB_API_TOKEN=offline-dummy-test-token\n")
            for status in ("unavailable_http_401", "unavailable_http_403"):
                state.enqueue_api(repo, "https://api.github.com/repos/offline/example/" + status, "issues")
                state.execute("UPDATE api_tasks SET status=? WHERE url LIKE ?",
                              (status, "%" + status))
            api = API(state, "", env)
            api.refresh_credentials()
            self.assertEqual(state.rows("SELECT status,attempts FROM api_tasks ORDER BY url"),
                             [{"status": "pending", "attempts": 0},
                              {"status": "pending", "attempts": 0}])
            self.assertNotIn("offline-dummy-test-token", (root / "events.jsonl").read_text())
            state.db.close()

    def test_api_only_recovery_leaves_git_and_assets_unchanged(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            state = State(root)
            source = self.make_input(root, ["https://github.com/offline/example"])
            state.import_input(source, 0, 1)
            repo = state.rows("SELECT * FROM repos")[0]
            state.execute("UPDATE repos SET status='complete',lfs_status='error',attempts=1")
            state.enqueue_api(repo, "https://api.github.com/repos/offline/example", "repository")
            state.execute("UPDATE api_tasks SET status='running',attempts=1")
            state.execute("INSERT INTO assets(asset_id,status,attempts) VALUES ('offline','running',1)")
            state.recover(api_only=True)
            self.assertEqual(state.rows("SELECT status FROM repos")[0]["status"], "complete")
            self.assertEqual(state.rows("SELECT status FROM assets")[0]["status"], "running")
            task = state.rows("SELECT * FROM api_tasks")[0]
            self.assertEqual((task["status"], task["attempts"], task["next_at"]), ("retry_wait", 0, 0))
            state.db.close()

    def test_api_only_finishes_queue_with_four_workers_without_git(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = self.make_input(root, ["https://github.com/offline/example", "https://pkg.go.dev/offline/example"])
            args = argparse.Namespace(input=source, output=root / "capture", proxy="", env_file=root / ".env",
                                      shard_index=0, shard_count=1, github_start=1, github_max=2,
                                      git_max=2, min_free_gib=0, retries=5, report_seconds=30,
                                      api_only=True, github_api_rph=4500, github_api_concurrency=4)
            collector = Collector(args)
            activity_lock = threading.Lock()
            activity = {"running": 0, "maximum": 0}
            def complete_page(task):
                with activity_lock:
                    activity["running"] += 1
                    activity["maximum"] = max(activity["maximum"], activity["running"])
                time.sleep(0.6)
                collector.state.execute("UPDATE api_tasks SET status='complete' WHERE task_id=?", (task["task_id"],))
                with activity_lock:
                    activity["running"] -= 1
            collector.api.run_task = complete_page
            collector.mirror = mock.Mock()
            try:
                with mock.patch("swh_history_packages.Packages") as packages:
                    collector.run()
                    collector.mirror.assert_not_called()
                    packages.return_value.resolve.assert_not_called()
                    packages.return_value.download.assert_not_called()
                self.assertEqual(activity["maximum"], 4)
                self.assertEqual(collector.state.rows("SELECT COUNT(*) n FROM api_tasks WHERE status='complete'")[0]["n"], 5)
                self.assertEqual(collector.state.rows("SELECT status FROM repos")[0]["status"], "pending")
                self.assertEqual(collector.state.rows("SELECT status FROM packages")[0]["status"], "pending")
                record = json.loads((collector.root / "status.json").read_text())
                self.assertTrue(record["api_only"])
                self.assertEqual(record["api_active"], 0)
                self.assertEqual(record["api_completed_this_run"], 5)
            finally:
                collector.process_lock.close()
                collector.state.db.close()

    def test_full_local_history_and_reuse_objects(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            work = root / "upstream"
            work.mkdir()
            def git(*args):
                return subprocess.check_output(["git", "-C", str(work)] + list(args), stderr=subprocess.DEVNULL).decode().strip()
            git("init")
            git("config", "user.email", "offline@example.invalid")
            git("config", "user.name", "Offline Test")
            (work / "main.p4").write_text("control Example() { apply {} }\n")
            git("add", ".")
            git("commit", "-m", "first revision")
            first = git("rev-parse", "HEAD")
            git("tag", "v1")
            (work / "main.p4").write_text("control Example2() { apply {} }\n")
            git("commit", "-am", "second revision")
            source = self.make_input(root, ["https://github.com/offline/example"])
            args = argparse.Namespace(input=source, output=root / "capture", proxy="", env_file=root / ".env",
                                      shard_index=0, shard_count=1, github_start=1, github_max=2,
                                      git_max=2, min_free_gib=0, retries=5, report_seconds=30)
            collector = Collector(args)
            repo = collector.state.rows("SELECT * FROM repos")[0]
            repo.update(clone_url=str(work), attempts=1)
            self.assertEqual(collector.mirror(repo), "success")
            destination = collector.root / "repos" / repo["repo_id"]
            commits = [json.loads(line) for line in (destination / "commits.jsonl").read_text().splitlines()]
            self.assertEqual(len(commits), 2)
            self.assertIn(first, {row["commit_oid"] for row in commits})
            self.assertIn("refs/tags/v1", (destination / "refs.tsv").read_text())
            self.assertEqual(collector.mirror(repo), "success")
            self.assertEqual(len((destination / "commits.jsonl").read_text().splitlines()), 2)
            collector.process_lock.close()


if __name__ == "__main__":
    unittest.main()
