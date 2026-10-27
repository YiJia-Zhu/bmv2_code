import unittest

from data.tools.swh_glue_parent_probe import (
    OUTPUT_ROOT,
    get_cli_argument,
    run_output_uri,
    validate_directory_id,
    validate_output_root,
)


class GlueParentProbeTest(unittest.TestCase):
    def test_validates_directory_id(self):
        value = "e0b550437f963ed02b8ed2dd546b0a4fcb02696b"
        self.assertEqual(value, validate_directory_id(value))
        with self.assertRaises(ValueError):
            validate_directory_id(value.upper())
        with self.assertRaises(ValueError):
            validate_directory_id(value[:-1])

    def test_output_is_confined_to_probe_root(self):
        self.assertEqual(OUTPUT_ROOT, validate_output_root(OUTPUT_ROOT.rstrip("/")))
        with self.assertRaises(ValueError):
            validate_output_root("s3://other-bucket/results/")

    def test_attempt_output_contains_target_and_run(self):
        target = "e0b550437f963ed02b8ed2dd546b0a4fcb02696b"
        self.assertEqual(
            OUTPUT_ROOT + target + "/attempts/jr_example/",
            run_output_uri(OUTPUT_ROOT, target, "jr_example"),
        )

    def test_reads_job_run_id_without_registering_it(self):
        self.assertEqual(
            "jr_example",
            get_cli_argument(["script.py", "--JOB_RUN_ID=jr_example"], "JOB_RUN_ID"),
        )


if __name__ == "__main__":
    unittest.main()
