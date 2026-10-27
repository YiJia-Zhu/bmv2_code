import unittest

from data.tools.swh_glue_candidate_smoke import (
    CANDIDATE_PREFIX,
    OUTPUT_PREFIX,
    get_cli_argument,
    validate_candidate_input,
    validate_output_prefix,
)


class GlueCandidateSmokeTest(unittest.TestCase):
    def test_reads_glue_job_run_id_without_registering_it(self):
        argv = ["script.py", "--JOB_RUN_ID", "jr_example"]
        self.assertEqual("jr_example", get_cli_argument(argv, "JOB_RUN_ID"))

    def test_reads_equals_style_argument(self):
        argv = ["script.py", "--JOB_RUN_ID=jr_example"]
        self.assertEqual("jr_example", get_cli_argument(argv, "JOB_RUN_ID"))

    def test_rejects_missing_argument(self):
        with self.assertRaises(ValueError):
            get_cli_argument(["script.py"], "JOB_RUN_ID")

    def test_accepts_one_candidate_csv(self):
        uri = CANDIDATE_PREFIX + "part-00000-example.csv"
        self.assertEqual(uri, validate_candidate_input(uri))

    def test_rejects_candidate_prefix(self):
        with self.assertRaises(ValueError):
            validate_candidate_input(CANDIDATE_PREFIX)

    def test_rejects_wildcard_input(self):
        with self.assertRaises(ValueError):
            validate_candidate_input(CANDIDATE_PREFIX + "part-*.csv")

    def test_rejects_output_outside_smoke_prefix(self):
        with self.assertRaises(ValueError):
            validate_output_prefix("s3://swh2p4/glue-results/full/")

    def test_normalizes_output_prefix(self):
        self.assertEqual(OUTPUT_PREFIX, validate_output_prefix(OUTPUT_PREFIX.rstrip("/")))


if __name__ == "__main__":
    unittest.main()
