#!/usr/bin/env python3
import unittest

from data.tools import swh_glue_graph_schema_probe as probe


class GraphSchemaProbeTest(unittest.TestCase):
    def test_reads_equals_and_pair_arguments(self):
        argv = ["probe.py", "--A=one", "--B", "two"]
        self.assertEqual("one", probe.get_cli_argument(argv, "A"))
        self.assertEqual("two", probe.get_cli_argument(argv, "B"))
        self.assertEqual("fallback", probe.get_cli_argument(argv, "C", "fallback"))

    def test_missing_argument_value_is_rejected(self):
        with self.assertRaises(ValueError):
            probe.get_cli_argument(["probe.py", "--A"], "A")

    def test_uri_join(self):
        self.assertEqual("s3://bucket/root/schema", probe.join_uri("s3://bucket/root/", "/schema"))


if __name__ == "__main__":
    unittest.main()
