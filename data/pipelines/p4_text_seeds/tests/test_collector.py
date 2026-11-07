import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from data.pipelines.p4_text_seeds.collector import (
    Collector, PageParser, extract_document, in_scope, normalize_url,
)


class CollectorTests(unittest.TestCase):
    def test_normalize_and_scope(self):
        self.assertEqual(
            normalize_url("https://P4.org//docs?a=1&utm_source=x#part"),
            "https://p4.org/docs?a=1",
        )
        self.assertFalse(in_scope("https://example.com/", "p4org"))
        self.assertTrue(in_scope(
            "https://github.com/p4lang/p4c/wiki/Getting-Started", "p4lang_wiki"
        ))
        self.assertFalse(in_scope(
            "https://github.com/p4lang/p4c/blob/main/README.md", "p4lang_wiki"
        ))
        self.assertIsNone(normalize_url("https://p4.org:bad/path"))

    def test_extract_html_and_links(self):
        raw = (b"<html><head><title>Spec</title></head><body>"
               b"<nav>Ignore me</nav><main><h1>P4_16</h1>"
               b"<p>Packet processing.</p><a href='/next'>Next</a>"
               b"</main><script>bad()</script></body></html>")
        title, text, links = extract_document(raw, "text/html", "https://p4.org/")
        self.assertEqual(title, "Spec")
        self.assertIn("P4_16", text)
        self.assertIn("Packet processing.", text)
        self.assertNotIn("Ignore me", text)
        self.assertNotIn("bad()", text)
        self.assertEqual(links[0][0], "/next")
        self.assertEqual(links[0][1], "Next")

    def test_pdf_content_type_takes_precedence_over_html_url(self):
        with patch("data.pipelines.p4_text_seeds.collector.subprocess.run") as run:
            run.return_value.returncode = 0
            run.return_value.stdout = b"P4 specification"
            _, text, links = extract_document(
                b"pdf bytes", "application/pdf", "https://p4.org/draft.html"
            )
        self.assertEqual(text, "P4 specification")
        self.assertEqual(links, [])

    def test_discourse_topic_json_preserves_posts(self):
        raw = json.dumps({
            "title": "BMv2 question",
            "post_stream": {"posts": [
                {"id": 10, "username": "alice", "created_at": "2026-01-01",
                 "cooked": "<p>How does BMv2 work?</p>"},
                {"id": 11, "username": "bob", "created_at": "2026-01-02",
                 "cooked": "<p>Use <code>simple_switch</code>.</p>"},
            ]},
        }).encode("utf-8")
        title, text, links = extract_document(
            raw, "application/json", "https://forum.p4.org/t/bmv2/10.json"
        )
        self.assertEqual(title, "BMv2 question")
        self.assertIn("author=alice created_at=2026-01-01", text)
        self.assertIn("author=bob created_at=2026-01-02", text)
        self.assertIn("simple_switch", text)
        self.assertEqual(links, [])

    def test_queue_records_out_of_scope_links(self):
        with tempfile.TemporaryDirectory() as directory:
            collector = Collector(Path(directory))
            root, disposition = collector.enqueue("https://p4.org/", "p4org")
            self.assertEqual(disposition, "queued")
            _, disposition = collector.enqueue(
                "https://example.com/x", "p4org", root, 1
            )
            self.assertEqual(disposition, "pending_review")
            self.assertEqual(collector.status(), {"queued": 1})
            self.assertEqual(collector.db.execute(
                "SELECT disposition FROM links"
            ).fetchone()[0], "pending_review")

    def test_depth_limit_preserves_discovered_links_and_exports(self):
        class Response:
            status = 200
            headers = {"Content-Type": "text/html"}

            def geturl(self):
                return "https://p4.org/"

        raw = (b"<title>Home</title><a href='/specs'>Specs</a>"
               b"<a href='https://example.com/paper'>Paper</a>")
        with tempfile.TemporaryDirectory() as directory:
            collector = Collector(Path(directory), delay=0)
            collector.enqueue("https://p4.org/", "p4org")
            with patch.object(collector, "allowed_by_robots", return_value=True), \
                    patch("data.pipelines.p4_text_seeds.collector.open_limited",
                          return_value=(Response(), raw)):
                self.assertEqual(collector.run(2, max_depth=0)["done"], 1)
            self.assertEqual(collector.status(), {"done": 1, "queued": 1})
            collector.export()
            queue = [json.loads(line) for line in
                     (Path(directory) / "queue.jsonl").read_text().splitlines()]
            documents = [json.loads(line) for line in
                         (Path(directory) / "documents.jsonl").read_text().splitlines()]
            links = [json.loads(line) for line in
                     (Path(directory) / "discovered_links.jsonl").read_text().splitlines()]
            self.assertEqual(len(queue), 2)
            self.assertEqual(len(documents), 1)
            self.assertEqual(len(links), 2)
            self.assertEqual({row["disposition"] for row in links},
                             {"queued", "pending_review"})
            self.assertIsNone(collector.step(1, source="eth_adv_net"))
            collector.db.execute(
                "UPDATE urls SET extractor_version=NULL WHERE url='https://p4.org/'"
            )
            self.assertEqual(collector.reprocess(1)["reprocessed"], 1)
            self.assertEqual(collector.db.execute(
                "SELECT anchor FROM links WHERE child_url='https://p4.org/specs'"
            ).fetchone()[0], "Specs")

    def test_extraction_failure_keeps_raw_bytes(self):
        class Response:
            status = 200
            headers = {"Content-Type": "application/pdf"}

            def geturl(self):
                return "https://p4.org/broken.pdf"

        with tempfile.TemporaryDirectory() as directory:
            collector = Collector(Path(directory), delay=0)
            collector.enqueue("https://p4.org/broken.pdf", "p4org")
            with patch.object(collector, "allowed_by_robots", return_value=True), \
                    patch("data.pipelines.p4_text_seeds.collector.open_limited",
                          return_value=(Response(), b"invalid pdf")), \
                    patch("data.pipelines.p4_text_seeds.collector.extract_document",
                          side_effect=ValueError("bad PDF")):
                self.assertEqual(collector.run(1, max_depth=0)["done"], 1)
            row = collector.db.execute(
                "SELECT raw_path,error FROM urls WHERE status='done'"
            ).fetchone()
            self.assertEqual((Path(directory) / row[0]).read_bytes(), b"invalid pdf")
            self.assertIn("bad PDF", row[1])


if __name__ == "__main__":
    unittest.main()
