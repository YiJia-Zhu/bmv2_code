"""Check classification boundaries and preservation of review hit provenance."""

import csv
import gzip
from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).parent))
import swh_p4_review_detail as DETAIL


def annotation(data, reason="insufficient_version_evidence", **extra):
    row = {"content_id": DETAIL.git_blob_sha1(data), "review_disposition": "unresolved",
           "review_reasons": [reason], "review_encoding": "utf-8", "fetch_status": "fetched"}
    row.update(extra)
    return row


class ReviewDetailTests(unittest.TestCase):
    def test_fragments_unknown_text_and_binary_keep_distinct_meanings(self):
        samples = (
            (b" \r\n", "insufficient_version_evidence", "whitespace_only", True),
            (b"/* description */\n", "insufficient_version_evidence", "comments_only", True),
            (b'#include "custom.p4"\n#define SIZE 8\n', "insufficient_version_evidence", "preprocessor_only", False),
            (b'const string s = "/* not a comment */";\n', "insufficient_version_evidence", "other_readable_text", False),
            (b"\x89PNG\r\n\x1a\n\x00", "unsupported_encoding_with_nul", "recognized_non_source", True),
            (b"\x00\xffunknown", "unsupported_encoding_with_nul", "unrecognized_encoding_or_binary", False),
            (b"bit<32> x; modify_field(x, 1);", "conflicting_syntax_evidence", "conflicting_syntax", False),
        )
        for data, reason, category, no_evidence in samples:
            with self.subTest(category=category):
                result = DETAIL.classify_detail(annotation(data, reason), data)
                self.assertEqual(result["review_detail_category"], category)
                self.assertEqual(result["review_detail_has_no_language_evidence"], no_evidence)

    def test_missing_or_corrupt_bytes_cannot_become_negative_evidence(self):
        row = annotation(b"source")
        with self.assertRaisesRegex(RuntimeError, "missing"):
            DETAIL.classify_detail(row, None)
        with self.assertRaisesRegex(RuntimeError, "hash mismatch"):
            DETAIL.classify_detail(row, b"changed")
        row["fetch_status"] = "too_large"
        result = DETAIL.classify_detail(row, None)
        self.assertEqual(result["review_detail_category"], "over_read_limit")
        self.assertFalse(result["review_detail_has_no_language_evidence"])

    def test_shared_content_retains_all_hits_and_original_csv_values(self):
        data = b"// just a comment\n"
        label = DETAIL.classify_detail(annotation(data), data)
        content_id = label["content_id"]
        fields = ["content_id", "directory_id", "name", "source_csv", "source_row",
                  "source_csv_index", "discovery_disposition", "extra_provenance"]
        rows = [dict(zip(fields, [content_id, directory, 'name,"quoted".p4', "csv0", str(index),
                                 "0", "needs_review", "line1\nline2"]))
                for index, directory in enumerate(("dir-a", "dir-b"), 1)]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source.csv"
            with source.open("w", newline="", encoding="utf-8") as stream:
                writer = csv.DictWriter(stream, fieldnames=fields)
                writer.writeheader()
                writer.writerows(rows)
            result = DETAIL.transform_csv(source, "source_shard=0/part.csv", root / "output",
                                          {content_id: label}, 0)
            self.assertEqual(result["rows"], 2)
            self.assertEqual(result["no_language_evidence_rows"], 2)
            self.assertEqual(result["content_ids"], [content_id])
            for entry in result["files"]:
                with gzip.open(entry["path"], "rt", encoding="utf-8", newline="") as stream:
                    saved = list(csv.DictReader(stream))
                if entry["relative_key"].startswith("remaining_review_csv/"):
                    self.assertEqual(saved, [])
                else:
                    self.assertEqual([{key: row[key] for key in fields} for row in saved], rows)
            with self.assertRaises(KeyError):
                DETAIL.transform_csv(source, "part.csv", root / "missing", {}, 0)


if __name__ == "__main__":
    unittest.main()
