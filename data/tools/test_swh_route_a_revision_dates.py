import sys
import unittest
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parent))

from run_swh_route_a_revision_dates import query_for_shard


class RevisionDateQueryTests(unittest.TestCase):
    def test_query_filters_exact_public_orc_path(self):
        path = "s3://softwareheritage/graph/2026-06-04/orc/revision/revision-one.orc"
        sql = query_for_shard(path)
        self.assertIn('JOIN "swh_p4_20260604"."route_a_revision_ids_v1" ids', sql)
        self.assertIn('WHERE r."$path" =', sql)
        self.assertIn('r."$path" AS source_path', sql)
        self.assertIn("'s3://softwareheritage/graph/2026-06-04/orc/revision/revision-one.orc'", sql)

    def test_query_escapes_literals(self):
        path = "s3://example/revision'quoted.orc"
        self.assertIn("'s3://example/revision''quoted.orc'", query_for_shard(path))


if __name__ == "__main__":
    unittest.main()
