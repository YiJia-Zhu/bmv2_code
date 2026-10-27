import csv
import json
import os
import tempfile
import unittest
from unittest.mock import patch

from data.tools import swh_graph_directory_probe as probe


class DirectoryProbeTest(unittest.TestCase):
    def test_prepare_selects_stable_representatives_and_checks_totals(self):
        with tempfile.TemporaryDirectory() as root:
            path = os.path.join(root, "index.csv")
            counts = (1, 2, 10, 100, 500, 1000, 10000)
            with open(path, "w", newline="", encoding="utf-8") as output:
                writer = csv.writer(output)
                writer.writerow(("directory_id", "candidate_hit_count"))
                for index, count in enumerate(counts):
                    writer.writerow(("{:040x}".format(index + 1), count))
            with patch.object(probe, "EXPECTED_DIRECTORIES", len(counts)), \
                    patch.object(probe, "EXPECTED_HITS", sum(counts)):
                manifest = probe.prepare(path, os.path.join(root, "prepared"))
            self.assertEqual(7, len(manifest["samples"]))
            self.assertEqual("extreme", manifest["samples"][-1]["group"])
            with open(os.path.join(root, "prepared", "directory_ids.txt"),
                      encoding="ascii") as source:
                self.assertEqual(7, len(source.read().splitlines()))

    def test_probe_stops_after_http_error_and_keeps_checkpoint(self):
        with tempfile.TemporaryDirectory() as root:
            with open(os.path.join(root, "manifest.json"), "w", encoding="utf-8") as output:
                json.dump({"samples": [
                    {"group": "one", "directory_id": "a" * 40, "candidate_hit_count": 1},
                    {"group": "few", "directory_id": "b" * 40, "candidate_hit_count": 2},
                ]}, output)
            with patch.dict(os.environ, {"SWH_API_TOKEN": "test-token"}), \
                    patch.object(probe, "request_origins", return_value={
                        "status": "http_error", "http_status": 401, "elapsed_seconds": 0.1,
                    }) as request:
                count = probe.probe(root, None, 4, 30, 2000000, False, 0)
            self.assertEqual(1, count)
            self.assertEqual(1, request.call_count)
            with open(os.path.join(root, "probe_results.jsonl"), encoding="utf-8") as source:
                saved = json.loads(source.readline())
            self.assertEqual(401, saved["http_status"])
            self.assertNotIn("test-token", json.dumps(saved))

    def test_unexpected_graph_response_is_not_accepted(self):
        class FakeResponse:
            status = 200
            headers = {}

            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return False

            def read(self, _limit):
                return ("swh:1:ori:" + "a" * 40 + "\n" + "truncated\n").encode("ascii")

        with patch.object(probe.urllib.request, "urlopen", return_value=FakeResponse()):
            result = probe.request_origins("b" * 40, "test-token", 30, 1000)
        self.assertEqual("unexpected_response", result["status"])
        self.assertEqual(1, result["origin_count"])


if __name__ == "__main__":
    unittest.main()
