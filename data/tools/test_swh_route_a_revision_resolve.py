import tempfile
import unittest
import json
from pathlib import Path

from run_swh_route_a_revision_resolve import (
    check_record, digest_json, historical_range_counters)


class RevisionResolveTests(unittest.TestCase):
    def test_revision_record_decodes(self):
        revision_id = bytes.fromhex("ab" * 20)
        swhid, raw = check_record(12, bytes([1, 4]) + revision_id)
        self.assertEqual(swhid, "swh:1:rev:" + ("ab" * 20))
        self.assertEqual(raw, "ab" * 20)

    def test_non_revision_record_rejected(self):
        with self.assertRaisesRegex(RuntimeError, "expected revision"):
            check_record(12, bytes([1, 2]) + bytes(20))

    def test_digest_is_stable(self):
        self.assertEqual(digest_json({"b": 2, "a": 1}), digest_json({"a": 1, "b": 2}))

    def test_resume_recovers_previous_request_counters(self):
        events = [
            {"event": "mapping_start"},
            {"event": "progress", "range_requests": 4, "range_bytes": 40},
            {"event": "mapping_complete", "range_requests": 5, "range_bytes": 50},
            {"event": "mapping_start"},
            {"event": "mapping_complete", "range_requests": 0, "range_bytes": 0},
            {"event": "mapping_start"},
            {"event": "progress", "range_requests": 6, "range_bytes": 60,
             "range_requests_run": 1, "range_bytes_run": 10},
        ]
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "events.jsonl"
            path.write_text("".join(json.dumps(row) + "\n" for row in events))
            self.assertEqual(historical_range_counters(path), (6, 60))


if __name__ == "__main__":
    unittest.main()
