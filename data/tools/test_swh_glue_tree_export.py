#!/usr/bin/env python3
import unittest

from data.tools import swh_glue_tree_export as exporter


class TreeExportTest(unittest.TestCase):
    def test_argument_and_uri_helpers(self):
        argv = ["job.py", "--A=one", "--B", "two"]
        self.assertEqual("one", exporter.get_cli_argument(argv, "A"))
        self.assertEqual("two", exporter.get_cli_argument(argv, "B"))
        self.assertEqual("s3://bucket/a/b", exporter.join_uri("s3://bucket/a/", "/b"))

    def test_safe_boolean_and_depth(self):
        self.assertTrue(exporter.parse_bool("yes"))
        self.assertFalse(exporter.parse_bool("false"))
        self.assertEqual(3, exporter.positive_int("3", "DEPTH"))
        with self.assertRaises(ValueError):
            exporter.parse_bool("unknown")
        with self.assertRaises(ValueError):
            exporter.positive_int("0", "DEPTH")


if __name__ == "__main__":
    unittest.main()
