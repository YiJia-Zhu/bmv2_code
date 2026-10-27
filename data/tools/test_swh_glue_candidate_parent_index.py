import unittest

from data.tools.swh_glue_candidate_parent_index import validate_inventory


class CandidateParentInventoryTest(unittest.TestCase):
    def test_accepts_complete_inventory(self):
        validate_inventory(["file-{}".format(i) for i in range(128)], range(128))

    def test_rejects_missing_or_duplicate_shard(self):
        with self.assertRaisesRegex(RuntimeError, "inventory is incomplete"):
            validate_inventory(["file-{}".format(i) for i in range(128)],
                               list(range(127)) + [0])

    def test_rejects_missing_csv(self):
        with self.assertRaisesRegex(RuntimeError, "candidate CSVs"):
            validate_inventory(["file-{}".format(i) for i in range(127)], range(128))


if __name__ == "__main__":
    unittest.main()
