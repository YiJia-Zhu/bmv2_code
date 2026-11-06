"""Coverage and resume checks for the GitHub search supplement."""

import gzip
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from data.pipelines.github_p4_supplement import search


class FakeClient:
    def __init__(self, payloads):
        self.payloads = payloads
        self.calls = []

    def request(self, kind, query, page):
        self.calls.append((kind, query, page))
        return self.payloads.pop(0), {"url": "https://api.github.com/search/test",
                                        "http_status": 200, "fetched_at": search.utc_now()}


class SearchTests(unittest.TestCase):
    def test_partition_exactly_covers_dates_and_file_sizes(self):
        for qualifier, low, high in (("created", "2026-01-01", "2026-01-02"),
                                     ("size", "0", "3")):
            pieces = search.split_job({"qualifier": qualifier, "low": low, "high": high})
            self.assertEqual(pieces[0][0], low if qualifier == "created" else 0)
            self.assertEqual(pieces[1][1], high if qualifier == "created" else 3)
            self.assertIsNone(search.split_job({"qualifier": qualifier,
                                                "low": low, "high": low}))

    def test_large_query_splits_and_cached_page_is_replayed(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            db = search.connect(root)
            search.add_job(db, "language_p4", "repository", "language:P4",
                           "created", "2026-01-01", "2026-01-02")
            db.commit()
            job = db.execute("SELECT * FROM jobs").fetchone()
            client = FakeClient([{"total_count": 1200, "incomplete_results": False,
                                  "items": []}])
            self.assertEqual(search.process_job(db, root, client, job, set()), "split")
            self.assertEqual(db.execute("SELECT COUNT(*) FROM jobs WHERE status='pending'").fetchone()[0], 2)
            self.assertEqual(len(client.calls), 1)
            self.assertTrue(search.page_path(root, job, 1).is_file())
            db.close()

    def test_code_hit_joins_swh_name_and_resumes_saved_page(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            db = search.connect(root)
            search.add_job(db, "extension_p4", "code", "extension:p4", "size", 0, 1)
            db.commit()
            item = {"path": "src/main.p4", "sha": "abc",
                    "repository": {"id": 42, "full_name": "Example/Program",
                                   "html_url": "https://github.com/Example/Program"}}
            payload = {"total_count": 1, "incomplete_results": False, "items": [item]}
            job = db.execute("SELECT * FROM jobs").fetchone()
            page = search.page_path(root, job, 1)
            search.atomic_page(page, payload, {"fetched_at": search.utc_now()})
            client = FakeClient([])
            self.assertEqual(search.process_job(db, root, client, job,
                                                {"example/program"}), "complete")
            self.assertEqual(client.calls, [])
            self.assertEqual(search.status(db)["swh_name_matches"], 1)
            self.assertEqual(search.status(db)["code_evidence_repos"], 1)
            export = search.export(db, root).read_text()
            self.assertIn("file_search_evidence", export)
            self.assertIn("src/main.p4", json.dumps(payload))
            db.close()

    def test_exact_size_overflow_is_audited(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            db = search.connect(root)
            search.add_job(db, "extension_p4", "code", "extension:p4", "size", 10, 10)
            db.commit()
            job = db.execute("SELECT * FROM jobs").fetchone()
            client = FakeClient([{"total_count": 1500,
                                  "incomplete_results": False, "items": []}])
            self.assertEqual(search.process_job(db, root, client, job, set()), "unresolved")
            self.assertEqual(db.execute("SELECT status FROM jobs").fetchone()[0], "unresolved")
            db.close()

    def test_query_growth_splits_and_preserves_prior_hits(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            db = search.connect(root)
            search.add_job(db, "extension_p4", "code", "extension:p4", "size", 0, 3)
            db.commit()
            first = {"path": "a.p4", "sha": "a", "repository":
                     {"id": 1, "full_name": "example/a"}}
            second = {"path": "b.p4", "sha": "b", "repository":
                      {"id": 2, "full_name": "example/b"}}
            client = FakeClient([
                {"total_count": 200, "incomplete_results": False, "items": [first]},
                {"total_count": 1200, "incomplete_results": False, "items": [second]},
            ])
            job = db.execute("SELECT * FROM jobs").fetchone()
            self.assertEqual(search.process_job(db, root, client, job, set()), "page")
            job = db.execute("SELECT * FROM jobs WHERE job_id=?", (job["job_id"],)).fetchone()
            self.assertEqual(search.process_job(db, root, client, job, set()), "split")
            self.assertEqual(db.execute("SELECT COUNT(*) FROM jobs WHERE status='pending'").fetchone()[0], 2)
            self.assertEqual(db.execute("SELECT COUNT(*) FROM hits").fetchone()[0], 2)
            self.assertEqual(db.execute("SELECT fetched_count FROM jobs WHERE status='split'").fetchone()[0], 2)
            db.close()

    def test_empty_page_splits_and_old_failures_are_requeued(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            db = search.connect(root)
            search.add_job(db, "language_p4", "repository", "language:P4",
                           "created", "2026-01-01", "2026-01-02")
            db.commit()
            job = db.execute("SELECT * FROM jobs").fetchone()
            db.execute("UPDATE jobs SET status='failed',total_count=100,"
                       "next_page=2,error='empty page before expected total for language:P4'")
            db.commit()
            search.requeue_recoverable_jobs(db)
            job = db.execute("SELECT * FROM jobs WHERE job_id=?", (job["job_id"],)).fetchone()
            self.assertEqual(job["status"], "pending")
            client = FakeClient([{"total_count": 0, "incomplete_results": False,
                                  "items": []}])
            self.assertEqual(search.process_job(db, root, client, job, set()), "split")
            self.assertEqual(db.execute("SELECT COUNT(*) FROM jobs WHERE status='pending'").fetchone()[0], 2)
            db.close()

    def test_old_growth_failure_is_requeued(self):
        with tempfile.TemporaryDirectory() as directory:
            db = search.connect(Path(directory))
            search.add_job(db, "extension_p4", "code", "extension:p4", "size", 0, 1)
            db.execute("UPDATE jobs SET status='failed',attempts=5,"
                       "error='query grew beyond 1000 results: extension:p4 size:0..1'")
            db.commit()
            search.requeue_recoverable_jobs(db)
            job = db.execute("SELECT * FROM jobs").fetchone()
            self.assertEqual((job["status"], job["attempts"], job["error"]),
                             ("pending", 0, None))
            db.close()


if __name__ == "__main__":
    unittest.main()
