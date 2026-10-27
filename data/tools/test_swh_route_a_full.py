"""Offline checks for cumulative budget safety and committed-shard recovery."""

import json
from pathlib import Path
import tempfile
import threading
import unittest

from run_swh_route_a_full import Budget, BudgetExceeded, Checkpoints, FullRunner, MAP_SCHEMA


class Body:
    def __init__(self):
        self.closed = False

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        self.closed = True


class FullTests(unittest.TestCase):
    def test_budget_reservations_survive_restart_and_rejected_call_does_not_mutate(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "budget.json"
            budget = Budget(path, 1.01)
            budget.reserve(3, 3000)
            saved = path.read_bytes()
            restored = Budget(path, 25)
            self.assertEqual(restored.limit, 1.01)
            self.assertEqual(restored.state["reserved_attempts"], 3)
            with self.assertRaises(BudgetExceeded):
                restored.reserve(1000, 3000)
            self.assertEqual(path.read_bytes(), saved)

    def test_checkpoint_requires_commit_identity_membership_and_output_integrity(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            checkpoints = Checkpoints(root, "original-reader-and-input")
            rows = [{"directory_id": "a" * 40, "node_id": 123}]
            self.assertIsNone(checkpoints.load("map", 0, ["a"]))
            checkpoints.save("map", 0, ["a"], rows, MAP_SCHEMA)
            self.assertEqual(checkpoints.load("map", 0, ["a"]), rows)
            with self.assertRaisesRegex(RuntimeError, "another input"):
                checkpoints.load("map", 0, ["b"])
            with self.assertRaisesRegex(RuntimeError, "another input"):
                Checkpoints(root, "changed-reader").load("map", 0, ["a"])
            (root / "map" / "part-00000.parquet").write_bytes(b"corrupt")
            with self.assertRaisesRegex(RuntimeError, "corrupt"):
                checkpoints.load("map", 0, ["a"])

    def test_full_range_reader_rejects_200_and_closes_body_without_reading(self):
        runner = FullRunner.__new__(FullRunner)
        runner.check_deadline = lambda: None
        runner.mapping_identity = {"map": {"key": "map", "etag": "etag", "size": 100}}
        runner.lock = threading.Lock()
        runner.range_requests = runner.range_bytes = 0
        body = Body()

        class Public:
            def get_object(self, **_kwargs):
                return {"Body": body, "ResponseMetadata": {"HTTPStatusCode": 200}, "ContentLength": 100}

        runner.public = Public()
        with self.assertRaisesRegex(ValueError, "Invalid S3 Range"):
            runner.range_read("map", (0, 7, [0]))
        self.assertTrue(body.closed)
        self.assertEqual(runner.range_requests, 1)
        self.assertEqual(runner.range_bytes, 0)


if __name__ == "__main__":
    unittest.main()
