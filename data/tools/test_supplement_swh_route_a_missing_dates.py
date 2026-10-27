import sys
import unittest
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parent))

from supplement_swh_route_a_missing_dates import (
    ORIGIN_ID, apply_supplement, numeric_package_version)


class MissingDateSupplementTests(unittest.TestCase):
    def test_numeric_version_order(self):
        self.assertGreater(numeric_package_version("0.1.1"), numeric_package_version("0.1"))
        with self.assertRaises(ValueError):
            numeric_package_version("0.1-rc")

    def test_synthetic_version_corrects_latest_without_inventing_date(self):
        metadata = {
            "origin_id": ORIGIN_ID, "origin_url": "opam+https://example/petr4/",
            "revisions": [
                {"id": "a", "message": "0.1", "directory": "dir-a", "type": "tar",
                 "synthetic": True, "date": None, "committer_date": None},
                {"id": "b", "message": "0.1.1", "directory": "dir-b", "type": "tar",
                 "synthetic": True, "date": None, "committer_date": None},
            ],
        }
        relations = [
            {"origin_id": ORIGIN_ID, "origin_url": metadata["origin_url"],
             "candidate_revision_id": "a", "revision_date": None,
             "committer_date": None, "directory": "dir-a"},
            {"origin_id": ORIGIN_ID, "origin_url": metadata["origin_url"],
             "candidate_revision_id": "b", "revision_date": None,
             "committer_date": None, "directory": "dir-b"},
        ]
        # The production stage requires exactly 10,107 origins.
        latest = [dict(relations[0], candidate_rank=1, selection_status="ordered_with_missing_revision_date")]
        latest += [{"origin_id": "other-" + str(i), "candidate_revision_id": "x",
                    "selection_status": "ordered_unconfirmed"}
                   for i in range(10106)]
        rows = apply_supplement(latest, relations, metadata)
        selected = rows[0]
        self.assertEqual(selected["candidate_revision_id"], "b")
        self.assertIsNone(selected["revision_date"])
        self.assertEqual(selected["ordering_basis"], "package_version")
        self.assertEqual(selected["selection_status"], "ordered_by_package_version_unconfirmed")


if __name__ == "__main__":
    unittest.main()
