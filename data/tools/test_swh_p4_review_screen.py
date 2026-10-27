"""Focused tests for second-pass P4 version evidence."""

import hashlib
import importlib.util
import sys
import unittest
from pathlib import Path


sys.path.insert(0, str(Path(__file__).parent))
SPEC = importlib.util.spec_from_file_location(
    "swh_p4_review_screen", Path(__file__).with_name("swh_p4_review_screen.py"))
REVIEW = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(REVIEW)


def classify(source, original_version="unknown"):
    data = source.encode("utf-8")
    content_id = hashlib.sha1(b"blob " + str(len(data)).encode("ascii") + b"\0" + data).hexdigest()
    return REVIEW.classify_review({"content_id": content_id, "version_status": original_version,
                                   "fetch_status": "fetched"}, data)


class ReviewScreenTests(unittest.TestCase):
    def test_p4_16_include_fragment(self):
        row = classify('typedef bit<48> mac_t;\ncontrol Ingress(inout mac_t mac) { apply {} }')
        self.assertEqual(row["review_disposition"], "p4_16_candidate")
        self.assertIn("width_type_declaration", row["review_evidence_p4_16"])

    def test_p4_14_legacy_action(self):
        row = classify("action forward(port) { modify_field(meta.port, port); }")
        self.assertEqual(row["review_disposition"], "p4_14_likely")

    def test_conflict_stays_unresolved(self):
        row = classify("bit<32> x; action a(p) { modify_field(x, p); }")
        self.assertEqual(row["review_version_status"], "mixed_or_conflicted")
        self.assertEqual(row["review_disposition"], "unresolved")

    def test_comments_and_strings_are_not_evidence(self):
        row = classify('// control C() {}\nconst string s = "bit<32> x";')
        self.assertEqual(row["review_disposition"], "unresolved")

    def test_diff_and_path_pointer_are_not_source(self):
        self.assertEqual(classify("--- a/main.p4\n+++ b/main.p4\n@@ -1 +1 @@\n")[
            "review_disposition"], "non_source")
        self.assertEqual(classify("/tmp/repo/main.p4\n")["review_disposition"], "non_source")
        self.assertEqual(classify("/tmp/repo/main.p4/" * 1000)["review_disposition"],
                         "unresolved")

    def test_missing_bytes_remain_unresolved(self):
        row = REVIEW.classify_review({"content_id": "a" * 40,
                                      "version_status": "uninspected"}, None)
        self.assertEqual(row["review_disposition"], "unresolved")


if __name__ == "__main__":
    unittest.main()
