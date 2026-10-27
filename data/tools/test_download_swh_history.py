#!/usr/bin/env python3
"""Offline checks for durable history capture and deterministic partitioning."""

import argparse
import csv
import json
import io
import sqlite3
import subprocess
import tempfile
import unittest
import urllib.error
from pathlib import Path

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
