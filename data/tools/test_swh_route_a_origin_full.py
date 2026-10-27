"""Offline checks for URL identity, shared reachability and final evidence."""

import datetime as dt
import hashlib
from pathlib import Path
import sqlite3
import tempfile
import time
import unittest
from types import SimpleNamespace
from unittest.mock import Mock

import pyarrow.parquet as pq

from run_swh_route_a_origin_full import FullOriginRun, origin_hash


class OriginFullTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.run = object.__new__(FullOriginRun)
        self.run.root = Path(self.directory.name)
        self.run.db = sqlite3.connect(str(self.run.root / "state.sqlite3"))
        self.addCleanup(self.run.db.close)
        self.run.state = {}
        self.run.started = time.monotonic()
        self.run.phase_started = self.run.started
        self.run.last_progress = 0
        self.run.check = lambda: None
        self.run.emit = Mock()
        self.run.data_identity = "data-identity"
        self.run.identity = "run-identity"
        self.run.range_requests = 0
        self.run.range_bytes = 0
        now = dt.datetime.now(dt.timezone.utc)
        self.run.control = {"first_started_at_utc": now.isoformat(), "deadline_utc": now.isoformat()}
        self.run._create_tables()
        self.url = "https://github.com/example/p4"
        self.sha = hashlib.sha1(self.url.encode()).hexdigest()

    def fixture(self):
        self.run.db.executemany("INSERT INTO seed_status VALUES (?,?,?,1)", [(0, "a" * 40, 1), (1, "b" * 40, 2)])
        self.run.db.execute("INSERT INTO seed_revision VALUES (?,?,?,?,?)", ("a" * 40, 1, 100, 10, 2))
        self.run.db.executemany("INSERT INTO graph_nodes VALUES (?,4,'done',1,0,NULL)", [(10,), (11,), (12,)])
        # Both candidate revisions share the same later revision. Preserve the
        # complete graph while querying and propagating each node/origin once.
        self.run.db.executemany("INSERT INTO traversal_edges VALUES (?,?,?,?)",
                                [(10, 11, 4, 4), (12, 11, 4, 4), (11, 90, 4, 2)])
        self.run.db.execute("INSERT INTO origins VALUES (90)")
        self.run.db.execute("INSERT INTO origin_ids VALUES (?,?)", (90, self.sha))
        self.run.db.execute("INSERT INTO origin_urls VALUES (?,?)", (self.sha, self.url))
        self.run.db.commit()

    def test_orc_hash_is_separate_from_swhid(self):
        self.assertEqual(origin_hash("swh:1:ori:" + self.sha), self.sha)
        self.assertEqual(origin_hash(self.sha), self.sha)
        with self.assertRaises(ValueError):
            origin_hash("swh:1:rev:" + self.sha)

    def test_shared_reachability_and_resume_after_closure(self):
        self.fixture()
        self.run.build_node_origins()
        self.assertEqual(self.run.db.execute("SELECT node_id FROM node_origins ORDER BY node_id").fetchall(),
                         [(10,), (11,), (12,), (90,)])
        self.run.build_node_origins()
        self.assertEqual(self.run.db.execute("SELECT COUNT(*) FROM origin_frontier").fetchone()[0], 0)
        self.assertEqual(self.run.db.execute("SELECT COUNT(*) FROM node_origins").fetchone()[0], 4)

    def test_materialize_preserves_candidate_and_reports_seed_without_revision(self):
        self.fixture()
        self.run.build_node_origins()
        summary = self.run.materialize()
        self.assertEqual(summary["seed_status_counts"], {"mapped": 1, "no_revision_entry": 1})
        rows = pq.read_table(self.run.root / "results/seed_origin/part-00000.parquet").to_pylist()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["candidate_revision_node_id"], 10)
        self.assertEqual(rows[0]["origin_url"], self.url)
        self.assertEqual(rows[0]["status"], "mapped")
        # Re-export is idempotent and validates the committed artifact hash.
        self.assertEqual(self.run.materialize()["seed_origin_rows"], 1)

    def test_unresolved_url_is_not_mapped(self):
        self.fixture()
        self.run.db.execute("DELETE FROM origin_urls")
        self.run.db.commit()
        self.run.build_node_origins()
        result = self.run.materialize()
        self.assertEqual(result["status"], "limited")
        rows = pq.read_table(self.run.root / "results/seed_origin/part-00000.parquet").to_pylist()
        self.assertEqual(rows[0]["status"], "url_unresolved")

    def test_empty_url_query_is_rejected(self):
        self.fixture()
        self.run.configure_query_limit = Mock()
        self.run.private = Mock()
        self.run.athena_query = Mock(return_value="query-id")
        paginator = Mock()
        paginator.paginate.return_value = [{"ResultSet": {"Rows": [{"Data": [
            {"VarCharValue": "id"}, {"VarCharValue": "url"}]}]}}]
        self.run.athena = Mock()
        self.run.athena.get_paginator.return_value = paginator
        with self.assertRaisesRegex(RuntimeError, "no matches"):
            self.run.resolve_urls()


if __name__ == "__main__":
    unittest.main()
