"""Offline checks for record coalescing and strict partial-response handling."""

import tempfile
import threading
import unittest
from pathlib import Path

from run_swh_route_a_sample import Runner, merged_ranges


class Body:
    def __init__(self, data):
        self.data = data
        self.closed = False

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.closed = True

    def read(self, size):
        return self.data[:size]


class PublicS3:
    def __init__(self, response):
        self.response = response

    def get_object(self, **kwargs):
        return self.response


class SampleTests(unittest.TestCase):
    def test_merged_ranges_preserve_record_offsets_and_span_limit(self):
        indices = [100, 0, 1, 1, 4, 10_000]
        spans = merged_ranges(indices, 22, max_gap=44, max_span=128)
        recovered = []
        for start, end, records in spans:
            self.assertLessEqual(end - start + 1, 128)
            for record in records:
                self.assertGreaterEqual(record * 22, start)
                self.assertLessEqual((record + 1) * 22 - 1, end)
            recovered.extend(records)
        self.assertEqual(recovered, sorted(set(indices)))

    def test_range_rejects_ignored_range_and_closes_body(self):
        with tempfile.TemporaryDirectory() as temporary:
            runner = Runner.__new__(Runner)
            runner.output = Path(temporary)
            runner.deadline = None
            runner.lock = threading.Lock()
            runner.range_requests = runner.range_reserved_bytes = 0
            runner.range_bytes = runner.range_cache_hits = 0
            runner.mapping_identity = {"map": {"etag": "test", "size": 100}}
            body = Body(b"x" * 100)
            runner.public = PublicS3({"Body": body,
                                      "ResponseMetadata": {"HTTPStatusCode": 200},
                                      "ContentLength": 100})
            with self.assertRaisesRegex(RuntimeError, "invalid S3 Range response"):
                runner.range_read("map", 0, 7)
            self.assertTrue(body.closed)
            self.assertEqual(runner.range_bytes, 0)
            self.assertFalse((runner.output / "range_cache").exists())


if __name__ == "__main__":
    unittest.main()
