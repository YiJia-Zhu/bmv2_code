import unittest
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import run_swh_athena_p4 as runner


class AthenaRunnerHelpersTest(unittest.TestCase):
    def test_stable_result_key_is_ordered_and_readable(self):
        key = runner.stable_result_key(
            "athena-results/swh-p4-20260604",
            7,
            "graph/2026-06-04/orc/directory_entry/"
            "directory_entry-06c5fbd4-efe9-45b2-a650-51d0cd02f88c.orc",
        )
        self.assertEqual(
            key,
            "athena-results/swh-p4-20260604/p4-directory-hits/"
            "part-00007-06c5fbd4-efe9-45b2-a650-51d0cd02f88c.csv",
        )

    def test_query_limits_the_hidden_path(self):
        query = runner.query_for_source(
            "swh_p4",
            "directory_entry_20260604",
            "s3://softwareheritage/path/object.orc",
        )
        self.assertIn('WHERE "$path" = \'s3://softwareheritage/path/object.orc\'', query)
        self.assertIn("lower(from_utf8(name)) LIKE '%.p4'", query)
        self.assertNotIn("ends_with", query)

    def test_client_token_is_stable_per_source(self):
        first = runner.client_token("one.orc")
        second = runner.client_token("one.orc")
        other = runner.client_token("two.orc")
        retry = runner.client_token("one.orc", attempt=1)
        self.assertEqual(first, second)
        self.assertNotEqual(first, other)
        self.assertNotEqual(first, retry)
        self.assertGreaterEqual(len(first), 32)

    def test_parse_s3_uri(self):
        self.assertEqual(
            runner.parse_s3_uri("s3://bucket/a/b.csv"), ("bucket", "a/b.csv")
        )
        with self.assertRaises(ValueError):
            runner.parse_s3_uri("https://bucket/a.csv")


if __name__ == "__main__":
    unittest.main()
