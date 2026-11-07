import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from data.pipelines.p4_text_seeds.prepare import prepare


class PrepareTests(unittest.TestCase):
    def test_deduplicates_text_but_preserves_urls(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            crawl = root / "crawl"
            crawl.mkdir()
            (crawl / "raw.html").write_bytes(b"<p>P4</p>")
            (crawl / "text.txt").write_text("P4 language", encoding="utf-8")
            raw_hash = hashlib.sha256(b"<p>P4</p>").hexdigest()
            text_hash = hashlib.sha256(b"P4 language").hexdigest()
            rows = []
            for suffix in ("one", "two"):
                rows.append({
                    "url": "https://p4.org/{}".format(suffix),
                    "source": "p4org", "raw_path": "raw.html",
                    "raw_sha256": raw_hash, "text_path": "text.txt",
                    "text_sha256": text_hash,
                })
            (crawl / "documents.jsonl").write_text(
                "".join(json.dumps(row) + "\n" for row in rows),
                encoding="utf-8",
            )
            counts = prepare(crawl, root / "normalized")
            self.assertEqual(counts["fetched"], 2)
            self.assertEqual(counts["unique_texts"], 1)
            self.assertEqual(counts["review_items"], 2)
            self.assertEqual(len((root / "normalized" / "occurrences.jsonl")
                                 .read_text().splitlines()), 2)

    def test_rejects_modified_raw(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "raw.html").write_bytes(b"modified")
            (root / "documents.jsonl").write_text(json.dumps({
                "url": "https://p4.org/", "raw_path": "raw.html",
                "raw_sha256": "wrong", "text_path": None,
            }) + "\n")
            with self.assertRaisesRegex(ValueError, "raw hash mismatch"):
                prepare(root, root / "out")

    def test_forum_json_replaces_html_in_review_only(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for name in ("html", "json"):
                (root / (name + ".raw")).write_bytes(name.encode())
                (root / (name + ".txt")).write_text(name, encoding="utf-8")
            rows = []
            for name in ("html", "json"):
                rows.append({
                    "url": "https://forum.p4.org/t/question/1" +
                           (".json" if name == "json" else ""),
                    "source": "p4forum", "raw_path": name + ".raw",
                    "raw_sha256": hashlib.sha256(name.encode()).hexdigest(),
                    "text_path": name + ".txt",
                    "text_sha256": hashlib.sha256(name.encode()).hexdigest(),
                })
            (root / "documents.jsonl").write_text(
                "".join(json.dumps(row) + "\n" for row in rows)
            )
            counts = prepare(root, root / "out")
            self.assertEqual(counts["fetched"], 2)
            self.assertEqual(counts["html_topics_with_json"], 1)
            self.assertEqual(counts["review_items"], 1)
            self.assertEqual(len((root / "out" / "occurrences.jsonl")
                                 .read_text().splitlines()), 2)


if __name__ == "__main__":
    unittest.main()
