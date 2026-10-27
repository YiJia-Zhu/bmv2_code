"""Focused offline checks for SWH P4 content annotation."""

import hashlib
import gzip
import importlib.util
import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import pyarrow as pa
import pyarrow.parquet as pq


SPEC = importlib.util.spec_from_file_location(
    "swh_p4_content_screen", Path(__file__).with_name("swh_p4_content_screen.py")
)
SCREEN = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(SCREEN)


def inspect(source):
    data = source.encode("utf-8")
    content_id = hashlib.sha1(b"blob " + str(len(data)).encode("ascii") + b"\0" + data).hexdigest()
    return SCREEN.inspect_content(content_id, data)


class ContentScreenTests(unittest.TestCase):
    def test_version_evidence_ignores_comments_and_strings(self):
        row = inspect(
            '// #include <core.p4>\n'
            'const string fake = "header_type H { fields { } }";\n'
            '#include <v1model.p4>\n'
            'parser P(packet_in packet) { }\n'
        )
        self.assertEqual(row["version_status"], "p4_16_likely")
        self.assertIn("core_include", row["positive_evidence"])
        self.assertIn("packet_in_parser", row["positive_evidence"])
        self.assertEqual(row["negative_evidence"], [])

    def test_legacy_and_unknown(self):
        self.assertEqual(inspect("header_type ethernet_t { fields { x : 8; } }")["version_status"], "p4_14_likely")
        self.assertEqual(inspect("#include \"local.p4\"\n")["version_status"], "unknown")

    def test_hash_mismatch_cannot_be_classified(self):
        row = SCREEN.inspect_content("0" * 40, b"#include <core.p4>\n")
        self.assertEqual(row["fetch_status"], "hash_mismatch")
        self.assertEqual(row["version_status"], "uninspected")

    def test_quality_flag_does_not_change_version(self):
        row = inspect("#include <core.p4>\n" + ("a" * 1100))
        self.assertEqual(row["version_status"], "p4_16_likely")
        self.assertEqual(row["quality_status"], "excluded")
        self.assertIn("very_long_line", row["quality_flags"])

    def test_lfs_pointer_is_not_source(self):
        row = inspect("version https://git-lfs.github.com/spec/v1\n" + "oid sha256:" + "a" * 64 + "\nsize 42\n")
        self.assertEqual(row["version_status"], "non_source")

    def test_batch_resume_rejects_different_ids_and_rules(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "batch-00000.jsonl"
            path.write_text('{"content_id":"a","rule_version":"p4-content-screen-v1"}\n', encoding="utf-8")
            self.assertEqual(len(SCREEN.validate_batch(path, ["a"])), 1)
            with self.assertRaises(RuntimeError):
                SCREEN.validate_batch(path, ["b"])
            path.write_text('{"content_id":"a","rule_version":"older"}\n', encoding="utf-8")
            with self.assertRaises(RuntimeError):
                SCREEN.validate_batch(path, ["a"])

    def test_raw_cache_checks_git_blob_hash(self):
        data = b"#include <core.p4>\n"
        content_id = SCREEN.git_blob_sha1(data)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.assertEqual(SCREEN.cache_content(root, content_id, data), data)
            self.assertEqual(SCREEN.cache_content(root, content_id, None), data)
            (root / content_id[:2] / content_id).write_bytes(b"tampered")
            with self.assertRaises(RuntimeError):
                SCREEN.cache_content(root, content_id, None)

    def test_parquet_index_and_batch_output(self):
        data = b"#include <core.p4>\n"
        content_id = SCREEN.git_blob_sha1(data)
        parquet_bytes = io.BytesIO()
        pq.write_table(pa.table({"content_id": [content_id]}), parquet_bytes)

        class FakeS3:
            def get_paginator(self, name):
                self.outer.assertEqual(name, "list_objects_v2")
                return self

            def paginate(self, **kwargs):
                self.outer.assertEqual(kwargs["Bucket"], "swh2p4")
                return [{"Contents": [{"Key": "dedup/unique_content_ids/part-0.parquet"}]}]

            def get_object(self, **kwargs):
                self.outer.assertEqual(kwargs["Key"], "dedup/unique_content_ids/part-0.parquet")
                return {"Body": io.BytesIO(parquet_bytes.getvalue())}

            def put_object(self, **kwargs):
                self.uploaded = kwargs

        s3 = FakeS3()
        s3.outer = self
        self.assertEqual(SCREEN.load_content_ids(s3, "s3://swh2p4/dedup/unique_content_ids"), [content_id])
        row = SCREEN.inspect_content(content_id, data)
        row["fetch_status"] = "fetched"
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            cache = root / "raw_cache"
            SCREEN.cache_content(cache, content_id, data)
            annotation = SCREEN.write_batch(s3, [row], root,
                                            "s3://swh2p4/output/content_annotations", 0)
            self.assertEqual(json.loads(annotation.read_text(encoding="utf-8"))["content_id"], content_id)
            self.assertEqual(s3.uploaded["Key"], "output/content_annotations/batch-00000.jsonl")
            archive = SCREEN.upload_raw_batch(s3, [row], root, cache, None, 0)
            self.assertTrue(archive.exists())

    def test_raw_fetch_limit(self):
        content_id = "a" * 40
        with mock.patch.object(SCREEN.urllib.request, "urlopen", return_value=io.BytesIO(b"abcd")):
            data, info = SCREEN.fetch_content(content_id, "https://example.test/api/1", 1, 3, 0, None)
        self.assertIsNone(data)
        self.assertEqual(info["fetch_status"], "too_large")

    def test_s3_gzip_fetch_verifies_both_hashes(self):
        data = b"#include <core.p4>\n"
        content_id = SCREEN.git_blob_sha1(data)
        sha1 = hashlib.sha1(data).hexdigest()

        class FakeS3:
            def get_object(self, **kwargs):
                self.key = kwargs["Key"]
                return {"Body": io.BytesIO(gzip.compress(data)), "ContentLength": 40}

        s3 = FakeS3()
        result, info = SCREEN.fetch_content_s3(s3, content_id, sha1, 100)
        self.assertEqual(result, data)
        self.assertEqual(info["fetch_status"], "fetched")
        self.assertEqual(s3.key, "content/" + sha1)
        result, info = SCREEN.fetch_content_s3(s3, content_id, "0" * 40, 100)
        self.assertIsNone(result)
        self.assertEqual(info["fetch_status"], "hash_mismatch")

    def test_public_content_client_does_not_use_login_credentials(self):
        from botocore import UNSIGNED

        client = SCREEN.public_s3_client("us-east-1")
        self.assertEqual(client.meta.config.signature_version, UNSIGNED)

    def test_s3_hash_map_rejects_ambiguous_values(self):
        content_id = "a" * 40
        parquet_bytes = io.BytesIO()
        pq.write_table(pa.table({"content_id": [content_id], "sha1": ["b" * 40]}),
                       parquet_bytes)

        class FakeS3:
            def get_paginator(self, name):
                return self

            def paginate(self, **kwargs):
                return [{"Contents": [{"Key": "mapped/matches/a.parquet"}]}]

            def get_object(self, **kwargs):
                return {"Body": io.BytesIO(parquet_bytes.getvalue())}

        self.assertEqual(SCREEN.load_sha1_map(FakeS3(), "s3://swh2p4/mapped/matches"),
                         {content_id: "b" * 40})

        class ConflictingS3(FakeS3):
            def paginate(self, **kwargs):
                return [{"Contents": [{"Key": "mapped/matches/a.parquet"},
                                      {"Key": "mapped/matches/b.parquet"}]}]

            def get_object(self, **kwargs):
                if kwargs["Key"].endswith("a.parquet"):
                    return {"Body": io.BytesIO(parquet_bytes.getvalue())}
                other = io.BytesIO()
                pq.write_table(pa.table({"content_id": [content_id], "sha1": ["c" * 40]}),
                               other)
                return {"Body": io.BytesIO(other.getvalue())}

        with self.assertRaisesRegex(RuntimeError, "ambiguous raw SHA-1"):
            SCREEN.load_sha1_map(ConflictingS3(), "s3://swh2p4/mapped/matches",
                                 workers=2)

    def test_content_index_parts_are_cached_for_resume(self):
        content_id = "a" * 40
        parquet_bytes = io.BytesIO()
        pq.write_table(pa.table({"content_id": [content_id]}), parquet_bytes)
        payload = parquet_bytes.getvalue()

        class FakeS3:
            reads = 0

            def get_paginator(self, name):
                return self

            def paginate(self, **kwargs):
                return [{"Contents": [{"Key": "ids/part-0.parquet", "Size": len(payload)}]}]

            def get_object(self, **kwargs):
                self.reads += 1
                return {"Body": io.BytesIO(payload)}

        s3 = FakeS3()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.assertEqual(SCREEN.load_content_ids_cached(s3, "s3://swh2p4/ids", root),
                             [content_id])
            self.assertEqual(SCREEN.load_content_ids_cached(s3, "s3://swh2p4/ids", root),
                             [content_id])
        self.assertEqual(s3.reads, 1)


if __name__ == "__main__":
    unittest.main()
