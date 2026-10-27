"""Offline guardrails for the SWH content hash join."""

import importlib.util
import unittest
from pathlib import Path


SPEC = importlib.util.spec_from_file_location(
    "swh_glue_content_hash_map", Path(__file__).with_name("swh_glue_content_hash_map.py")
)
MAP = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MAP)


class HashMapTests(unittest.TestCase):
    def test_bounded_content_uri_and_explicit_confirmation(self):
        output = MAP.OUTPUT_ROOT + "pilot-one-file"
        MAP.validate(MAP.INPUT_URI, MAP.CONTENT_ROOT + "part.orc", output,
                     "pilot-one-file", "true")
        with self.assertRaises(ValueError):
            MAP.validate(MAP.INPUT_URI, "s3://other-bucket/content/", output,
                         "pilot-one-file", "true")
        with self.assertRaises(ValueError):
            MAP.validate(MAP.INPUT_URI, MAP.CONTENT_ROOT, output,
                         "pilot-one-file", "false")


if __name__ == "__main__":
    unittest.main()
