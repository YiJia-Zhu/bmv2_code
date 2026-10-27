import unittest

from data.tools import run_swh_glue_parent_probe as runner


TARGET = "e0b550437f963ed02b8ed2dd546b0a4fcb02696b"


class GlueParentProbeRunnerTest(unittest.TestCase):
    def test_probe_id_is_stable_and_target_specific(self):
        first = runner.stable_probe_id(TARGET)
        self.assertEqual(first, runner.stable_probe_id(TARGET))
        self.assertNotEqual(
            first,
            runner.stable_probe_id("a0b550437f963ed02b8ed2dd546b0a4fcb02696b"),
        )

    def test_manifest_key_is_deterministic(self):
        self.assertEqual(
            "glue-results/x/manifests/parent-probe-{}.json".format(TARGET),
            runner.manifest_key("glue-results/x/", TARGET),
        )

    def test_run_matching_requires_both_stable_arguments(self):
        probe_id = runner.stable_probe_id(TARGET)
        run = {
            "Arguments": {
                "--PROBE_ID": probe_id,
                "--TARGET_DIRECTORY_ID": TARGET,
            }
        }
        self.assertTrue(runner.run_matches_probe(run, probe_id, TARGET))
        self.assertFalse(runner.run_matches_probe(run, "other", TARGET))

    def test_glue_states_map_to_manifest_states(self):
        self.assertEqual("running", runner.manifest_status_for_glue_state("RUNNING"))
        self.assertEqual(
            "job_succeeded", runner.manifest_status_for_glue_state("SUCCEEDED")
        )
        self.assertEqual("timeout", runner.manifest_status_for_glue_state("TIMEOUT"))
        self.assertEqual("stopped", runner.manifest_status_for_glue_state("STOPPED"))
        self.assertEqual("expired", runner.manifest_status_for_glue_state("EXPIRED"))
        self.assertEqual("failed", runner.manifest_status_for_glue_state("ERROR"))
        with self.assertRaises(ValueError):
            runner.manifest_status_for_glue_state("UNKNOWN")


if __name__ == "__main__":
    unittest.main()
