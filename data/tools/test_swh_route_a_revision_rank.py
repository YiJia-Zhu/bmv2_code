import sys
import unittest
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parent))

from run_swh_route_a_revision_rank import candidate_key, parse_timestamp


class RevisionRankTests(unittest.TestCase):
    def test_parse_swh_timestamp(self):
        self.assertIsNotNone(parse_timestamp("2026-04-17 07:49:41.000"))

    def test_revision_date_precedes_committer_tie_breaker(self):
        older = {"revision_date": "2025-01-01 00:00:00.000",
                 "committer_date": "2026-01-01 00:00:00.000",
                 "candidate_revision_id": "b"}
        newer = {"revision_date": "2026-01-01 00:00:00.000",
                 "committer_date": "2025-01-01 00:00:00.000",
                 "candidate_revision_id": "a"}
        self.assertGreater(candidate_key(newer), candidate_key(older))


if __name__ == "__main__":
    unittest.main()
