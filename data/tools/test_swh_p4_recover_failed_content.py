"""Offline checks for serial API recovery after the full S3 screen."""

import hashlib
import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


TOOLS = Path(__file__).parent
sys.path.insert(0, str(TOOLS))
SPEC = importlib.util.spec_from_file_location(
    "swh_p4_recover_failed_content", TOOLS / "swh_p4_recover_failed_content.py"
)
RECOVER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(RECOVER)
from swh_p4_content_screen import git_blob_sha1


class RecoveryTests(unittest.TestCase):
    def test_api_recovery_preserves_identity_and_uploads_raw_archive(self):
        data = b"#include <core.p4>\n"
        content_id = git_blob_sha1(data)
        sha1 = hashlib.sha1(data).hexdigest()

        class FakeS3:
            def __init__(self):
                self.keys = []

            def put_object(self, **kwargs):
                self.keys.append(kwargs["Key"])

            def upload_file(self, local, bucket, key):
                self.keys.append(key)

        s3 = FakeS3()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            row = {"content_id": content_id, "sha1": sha1,
                   "rule_version": "p4-content-screen-v1", "fetch_status": "s3_error",
                   "s3_error_code": "NoSuchKey", "version_status": "uninspected",
                   "quality_status": "unknown", "quality_flags": []}
            (root / "batch-00000.jsonl").write_text(json.dumps(row) + "\n")
            with mock.patch.object(RECOVER, "fetch_content", return_value=(
                data, {"fetch_status": "fetched", "bytes_read": len(data)}
            )):
                counts = RECOVER.recover(root, "s3://swh2p4/out/content_annotations",
                                         "https://example.test/api/1", None, 1024, 0, s3=s3)
            updated = json.loads((root / "batch-00000.jsonl").read_text())
            self.assertEqual(updated["content_id"], content_id)
            self.assertEqual(updated["sha1"], sha1)
            self.assertEqual(updated["fetch_status"], "fetched")
            self.assertEqual(updated["api_fallback_status"], "recovered")
            self.assertTrue(updated["sha1_git_verified"])
            self.assertEqual(updated["version_status"], "p4_16_likely")
            self.assertEqual(counts["recovered"], 1)
            self.assertEqual(len(s3.keys), 2)
            self.assertTrue((root / "raw_batches/batch-00000.tar.gz").exists())
            with mock.patch.object(RECOVER, "fetch_content") as fetch_again:
                second = RECOVER.recover(root, "s3://swh2p4/out/content_annotations",
                                         "https://example.test/api/1", None, 1024, 0, s3=s3)
            fetch_again.assert_not_called()
            self.assertEqual(second["recovered"], 0)
            self.assertEqual(len(s3.keys), 4)


if __name__ == "__main__":
    unittest.main()
