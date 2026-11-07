#!/usr/bin/env python3
"""Collect P4 documentation seeds with a resumable, bounded URL queue."""

import argparse
import hashlib
import html
from html.parser import HTMLParser
import json
import os
from pathlib import Path
import re
import sqlite3
import subprocess
import sys
import tempfile
import time
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qsl, urlencode, urljoin, urlsplit, urlunsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener
from urllib.robotparser import RobotFileParser
import zipfile
from xml.etree import ElementTree


USER_AGENT = "P4TextSeedCollector/0.1 (research corpus; contact via p4lang)"
EXTRACTOR_VERSION = "0.5"
TRACKING_KEYS = {"fbclid", "gclid", "mc_cid", "mc_eid"}
SKIP_SUFFIXES = {
    ".css", ".js", ".jpg", ".jpeg", ".png", ".gif", ".svg", ".ico",
    ".mp3", ".mp4", ".webm", ".woff", ".woff2", ".ttf", ".zip", ".tar",
    ".gz", ".exe",
}
SOURCE_HOSTS = {
    "p4org": {"p4.org", "www.p4.org"},
    "eth_adv_net": {"adv-net.ethz.ch"},
    "p4forum": {"forum.p4.org"},
    "rfc": {"www.rfc-editor.org", "rfc-editor.org"},
    "p4lang_wiki": {"github.com"},
}
START_URLS = {
    "p4org": ["https://p4.org/"],
    "eth_adv_net": [
        "https://adv-net.ethz.ch/{}/".format(year) for year in range(2018, 2026)
    ],
    "p4forum": ["https://forum.p4.org/"],
    "rfc": [
        "https://www.rfc-editor.org/rfc/rfc{}.html".format(number)
        for number in (768, 791, 7149, 7348, 7426, 8200, 8321, 8926, 9293, 9817)
    ],
}
SCHEMA = """
CREATE TABLE IF NOT EXISTS urls (
    url TEXT PRIMARY KEY,
    source TEXT NOT NULL,
    parent_url TEXT,
    depth INTEGER NOT NULL,
    status TEXT NOT NULL DEFAULT 'queued',
    attempts INTEGER NOT NULL DEFAULT 0,
    final_url TEXT,
    http_status INTEGER,
    content_type TEXT,
    raw_sha256 TEXT,
    raw_path TEXT,
    text_sha256 TEXT,
    text_path TEXT,
    title TEXT,
    fetched_at TEXT,
    extractor_version TEXT,
    error TEXT
);
CREATE INDEX IF NOT EXISTS urls_queue ON urls(status);
CREATE TABLE IF NOT EXISTS links (
    parent_url TEXT NOT NULL,
    child_url TEXT NOT NULL,
    anchor TEXT NOT NULL,
    disposition TEXT NOT NULL,
    PRIMARY KEY (parent_url, child_url)
);
"""


def normalize_url(url, base=None):
    """Return a stable HTTP URL, or None for non-document link schemes."""
    try:
        if base:
            url = urljoin(base, html.unescape(url))
        parts = urlsplit(url.strip())
        if parts.scheme.lower() not in ("http", "https") or not parts.hostname:
            return None
        host = parts.hostname.lower()
        port = parts.port
    except (TypeError, ValueError):
        return None
    if port and not (parts.scheme == "http" and port == 80) and not (
        parts.scheme == "https" and port == 443
    ):
        host = "{}:{}".format(host, port)
    path = re.sub(r"/{2,}", "/", parts.path or "/")
    query = [
        (key, value) for key, value in parse_qsl(parts.query, keep_blank_values=True)
        if key.lower() not in TRACKING_KEYS and not key.lower().startswith("utm_")
    ]
    return urlunsplit(
        (parts.scheme.lower(), host, path, urlencode(sorted(query)), "")
    )


def in_scope(url, source):
    parts = urlsplit(url)
    if parts.hostname not in SOURCE_HOSTS[source]:
        return False
    if source == "p4lang_wiki":
        return bool(re.match(r"^/p4lang/[^/]+/wiki(?:/|$)", parts.path))
    if source == "rfc":
        return bool(re.match(r"^/rfc/rfc[0-9]+\.html$", parts.path))
    if source == "eth_adv_net":
        return bool(re.match(r"^/(201[89]|202[0-5])(?:/|$)", parts.path))
    return True


def candidate_link(url):
    path = urlsplit(url).path.lower()
    return not any(path.endswith(suffix) for suffix in SKIP_SUFFIXES)


class PageParser(HTMLParser):
    """Extract links and readable blocks without navigation and scripts."""

    BLOCKS = {
        "article", "main", "section", "p", "div", "li", "h1", "h2", "h3",
        "h4", "h5", "h6", "pre", "blockquote", "tr", "br",
    }
    OMIT = {"script", "style", "svg", "noscript", "nav", "footer", "header"}

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.links = []
        self.link_stack = []
        self.chunks = []
        self.title_chunks = []
        self.omit_depth = 0
        self.in_title = False

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if tag == "a" and attrs.get("href"):
            self.links.append([attrs["href"], [attrs.get("title", "")]])
            self.link_stack.append(len(self.links) - 1)
        if tag == "img" and self.link_stack and attrs.get("alt"):
            self.links[self.link_stack[-1]][1].append(attrs["alt"])
        if tag in self.OMIT:
            self.omit_depth += 1
        if tag == "title":
            self.in_title = True
        if not self.omit_depth and tag in self.BLOCKS:
            self.chunks.append("\n")

    def handle_endtag(self, tag):
        if tag == "a" and self.link_stack:
            self.link_stack.pop()
        if tag in self.OMIT and self.omit_depth:
            self.omit_depth -= 1
        if tag == "title":
            self.in_title = False
        if not self.omit_depth and tag in self.BLOCKS:
            self.chunks.append("\n")

    def handle_data(self, data):
        if self.link_stack:
            self.links[self.link_stack[-1]][1].append(data)
        if self.in_title:
            self.title_chunks.append(data)
        if not self.omit_depth and not self.in_title:
            self.chunks.append(data)

    def result(self):
        title = " ".join(" ".join(self.title_chunks).split())
        text = "".join(self.chunks)
        lines = [" ".join(line.split()) for line in text.splitlines()]
        text = "\n".join(line for line in lines if line)
        links = [
            (href, " ".join(" ".join(label).split()))
            for href, label in self.links
        ]
        return title, text, links


def decode_html(raw, content_type):
    match = re.search(r"charset=([\w.-]+)", content_type, re.I)
    encodings = [match.group(1)] if match else []
    encodings.extend(["utf-8", "windows-1252"])
    for encoding in encodings:
        try:
            return raw.decode(encoding)
        except (LookupError, UnicodeDecodeError):
            pass
    return raw.decode("utf-8", errors="replace")


def extract_document(raw, content_type, url):
    """Return title, text, and discovered links; keep raw bytes separately."""
    suffix = urlsplit(url).path.lower()
    if ("json" in content_type or suffix.endswith(".json")) and urlsplit(
            url).hostname == "forum.p4.org":
        topic = json.loads(raw.decode("utf-8"))
        if (not isinstance(topic, dict) or
                not isinstance(topic.get("post_stream"), dict) or
                not isinstance(topic["post_stream"].get("posts"), list)):
            raise ValueError("not a Discourse topic JSON response")
        blocks = []
        links = []
        expected_posts = len(topic["post_stream"].get("stream", []))
        blocks.append("[topic_id={} expected_posts={} fetched_posts={}]".format(
            topic.get("id"), expected_posts, len(topic["post_stream"]["posts"])
        ))
        for post in topic["post_stream"]["posts"]:
            parser = PageParser()
            parser.feed(post.get("cooked", ""))
            _, content, post_links = parser.result()
            blocks.append("[post_id={} author={} created_at={}]\n{}".format(
                post.get("id"), post.get("username"), post.get("created_at"),
                content,
            ))
            links.extend(post_links)
        return topic.get("title", ""), "\n\n".join(blocks), links
    if "pdf" in content_type or suffix.endswith(".pdf"):
        with tempfile.NamedTemporaryFile(suffix=".pdf") as source:
            source.write(raw)
            source.flush()
            result = subprocess.run(
                ["pdftotext", "-layout", source.name, "-"],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=60,
                check=False,
            )
        if result.returncode:
            raise ValueError("pdftotext: {}".format(
                result.stderr.decode("utf-8", errors="replace")[:300]
            ))
        return "", result.stdout.decode("utf-8", errors="replace"), []
    if "presentationml" in content_type or suffix.endswith(".pptx"):
        with tempfile.NamedTemporaryFile(suffix=".pptx") as source:
            source.write(raw)
            source.flush()
            with zipfile.ZipFile(source.name) as archive:
                names = sorted(
                    (name for name in archive.namelist()
                     if re.match(r"ppt/slides/slide[0-9]+\.xml$", name)),
                    key=lambda name: int(re.search(r"slide([0-9]+)", name).group(1)),
                )
                slides = []
                for name in names:
                    root = ElementTree.fromstring(archive.read(name))
                    slides.append(" ".join(
                        node.text or "" for node in root.iter()
                        if node.tag.endswith("}t")
                    ))
        return "", "\n\n".join(slides), []
    if "powerpoint" in content_type or suffix.endswith(".ppt"):
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "slides.ppt"
            source.write_bytes(raw)
            profile = Path(directory) / "lo_profile"
            result = subprocess.run(
                ["libreoffice", "-env:UserInstallation=file://{}".format(profile),
                 "--headless", "--convert-to", "pdf", "--outdir", directory,
                 str(source)],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=120,
                check=False,
            )
            pdf = Path(directory) / "slides.pdf"
            if result.returncode or not pdf.exists():
                raise ValueError("LibreOffice PPT conversion failed: {}".format(
                    result.stderr.decode("utf-8", errors="replace")[:300]
                ))
            text = subprocess.run(
                ["pdftotext", "-layout", str(pdf), "-"],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=60,
                check=False,
            )
            if text.returncode:
                raise ValueError("pdftotext after PPT conversion failed")
        return "", text.stdout.decode("utf-8", errors="replace"), []
    if "html" in content_type or suffix.endswith((".html", ".htm")):
        parser = PageParser()
        parser.feed(decode_html(raw, content_type))
        return parser.result()
    if suffix.endswith((".txt", ".md")) or content_type.startswith("text/plain"):
        return "", raw.decode("utf-8", errors="replace"), []
    return "", "", []


class ScopedRedirect(HTTPRedirectHandler):
    def __init__(self, source):
        self.source = source

    def redirect_request(self, request, fp, code, msg, headers, newurl):
        normalized = normalize_url(newurl)
        if not normalized or not in_scope(normalized, self.source):
            raise ValueError("redirect out of source scope: {}".format(newurl))
        return super().redirect_request(request, fp, code, msg, headers, newurl)


def open_limited(opener, url, max_bytes, timeout):
    request = Request(url, headers={"User-Agent": USER_AGENT})
    with opener.open(request, timeout=timeout) as response:
        length = response.headers.get("Content-Length")
        if length and int(length) > max_bytes:
            raise ValueError("content too large: {}".format(length))
        raw = response.read(max_bytes + 1)
        if len(raw) > max_bytes:
            raise ValueError("content exceeds {} bytes".format(max_bytes))
        return response, raw


def write_once(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        if hashlib.sha256(path.read_bytes()).hexdigest() != hashlib.sha256(data).hexdigest():
            raise ValueError("existing content hash mismatch: {}".format(path))
        return
    with tempfile.NamedTemporaryFile(dir=str(path.parent), delete=False) as stream:
        temporary = Path(stream.name)
        try:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        except BaseException:
            temporary.unlink(missing_ok=True)
            raise
    try:
        os.replace(str(temporary), str(path))
    finally:
        if temporary.exists():
            temporary.unlink()


class Collector:
    def __init__(self, output, delay=1.0, timeout=20, max_bytes=25_000_000):
        self.output = Path(output)
        self.output.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(str(self.output / "queue.sqlite3"))
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.executescript(SCHEMA)
        columns = {row[1] for row in self.db.execute("PRAGMA table_info(urls)")}
        if "extractor_version" not in columns:
            self.db.execute("ALTER TABLE urls ADD COLUMN extractor_version TEXT")
        self.delay = delay
        self.timeout = timeout
        self.max_bytes = max_bytes
        self.last_request = {}
        self.robots = {}

    def enqueue(self, url, source, parent=None, depth=0):
        normalized = normalize_url(url, parent)
        if not normalized:
            return None, "invalid"
        disposition = "queued" if in_scope(normalized, source) and candidate_link(
            normalized
        ) else "pending_review"
        if parent:
            self.db.execute(
                "INSERT OR IGNORE INTO links VALUES (?, ?, ?, ?)",
                (parent, normalized, "", disposition),
            )
        if disposition == "queued":
            self.db.execute(
                "INSERT OR IGNORE INTO urls(url, source, parent_url, depth)"
                " VALUES (?, ?, ?, ?)", (normalized, source, parent, depth),
            )
        self.db.commit()
        return normalized, disposition

    def seed(self, sources):
        for source in sources:
            for url in START_URLS.get(source, []):
                self.enqueue(url, source)

    def _wait(self, url):
        host = urlsplit(url).hostname
        remaining = self.delay - (time.monotonic() - self.last_request.get(host, 0))
        if remaining > 0:
            time.sleep(remaining)
        self.last_request[host] = time.monotonic()

    def allowed_by_robots(self, url):
        parts = urlsplit(url)
        origin = "{}://{}".format(parts.scheme, parts.netloc)
        if origin not in self.robots:
            robots_url = origin + "/robots.txt"
            self._wait(robots_url)
            try:
                opener = build_opener(ScopedRedirectForOrigin(origin))
                _, raw = open_limited(opener, robots_url, 1_000_000, self.timeout)
                parser = RobotFileParser()
                parser.parse(raw.decode("utf-8", errors="replace").splitlines())
                self.robots[origin] = parser
            except HTTPError as error:
                if error.code == 404:
                    self.robots[origin] = None
                else:
                    raise ValueError("robots HTTP {}".format(error.code))
            except (URLError, ValueError) as error:
                raise ValueError("robots unavailable: {}".format(error))
        rules = self.robots[origin]
        return rules is None or rules.can_fetch(USER_AGENT, url)

    def _save(self, raw, text, content_type):
        raw_hash = hashlib.sha256(raw).hexdigest()
        suffix = ".html" if "html" in content_type else ".bin"
        raw_path = self.output / "raw" / raw_hash[:2] / (raw_hash + suffix)
        write_once(raw_path, raw)
        text_hash = hashlib.sha256(text.encode("utf-8")).hexdigest()
        text_path = self.output / "text" / text_hash[:2] / (text_hash + ".txt")
        if text:
            write_once(text_path, text.encode("utf-8"))
        return raw_hash, str(raw_path.relative_to(self.output)), (
            text_hash if text else None
        ), str(text_path.relative_to(self.output)) if text else None

    def _record_links(self, parent, source, depth, links):
        for href, anchor in links:
            child, disposition = self.enqueue(href, source, parent, depth + 1)
            if child:
                self.db.execute(
                    "UPDATE links SET anchor=?, disposition=?"
                    " WHERE parent_url=? AND child_url=?",
                    (anchor, disposition, parent, child),
                )

    def _record_topic_json(self, parent, source, depth):
        if source == "p4forum" and re.match(
                r"^/t/[^/]+/[0-9]+$", urlsplit(parent).path):
            self._record_links(parent, source, depth,
                               [(parent + ".json", "topic JSON")])

    def step(self, max_depth, source=None, url_substring=None):
        clauses = ["status='queued'", "depth<=?"]
        parameters = [max_depth]
        if source:
            clauses.append("source=?")
            parameters.append(source)
        if url_substring:
            clauses.append("instr(lower(url), lower(?))>0")
            parameters.append(url_substring)
        row = self.db.execute(
            "SELECT url, source, depth FROM urls WHERE {}"
            " ORDER BY rowid LIMIT 1".format(" AND ".join(clauses)), parameters
        ).fetchone()
        if row is None:
            return None
        url, source, depth = row
        self.db.execute(
            "UPDATE urls SET status='fetching', attempts=attempts+1 WHERE url=?",
            (url,),
        )
        self.db.commit()
        try:
            if not self.allowed_by_robots(url):
                self.db.execute(
                    "UPDATE urls SET status='skipped', error='robots disallow' WHERE url=?",
                    (url,),
                )
                self.db.commit()
                return url, "skipped"
            self._wait(url)
            opener = build_opener(ScopedRedirect(source))
            response, raw = open_limited(
                opener, url, self.max_bytes, self.timeout
            )
            final_url = normalize_url(response.geturl())
            content_type = response.headers.get("Content-Type", "").lower()
            extraction_error = None
            try:
                title, text, links = extract_document(raw, content_type, final_url)
            except (OSError, ValueError, subprocess.TimeoutExpired,
                    zipfile.BadZipFile, ElementTree.ParseError) as error:
                title, text, links = "", "", []
                extraction_error = "extraction failed: {}".format(str(error)[:450])
            raw_hash, raw_path, text_hash, text_path = self._save(
                raw, text, content_type
            )
            self.db.execute(
                "UPDATE urls SET status='done', final_url=?, http_status=?,"
                " content_type=?, raw_sha256=?, raw_path=?, text_sha256=?,"
                " text_path=?, title=?, fetched_at=datetime('now'),"
                " extractor_version=?, error=?"
                " WHERE url=?",
                (final_url, response.status, content_type, raw_hash, raw_path,
                 text_hash, text_path, title, EXTRACTOR_VERSION,
                 extraction_error, url),
            )
            self._record_links(final_url, source, depth, links)
            self._record_topic_json(final_url, source, depth)
            self.db.commit()
            return url, "done"
        except (HTTPError, URLError, OSError, ValueError, subprocess.TimeoutExpired,
                zipfile.BadZipFile, ElementTree.ParseError) as error:
            self.db.execute(
                "UPDATE urls SET status='error', error=? WHERE url=?",
                (str(error)[:500], url),
            )
            self.db.commit()
            return url, "error"

    def run(self, max_pages, max_depth, source=None, url_substring=None):
        self.db.execute(
            "UPDATE urls SET status='queued' WHERE status='fetching'"
        )
        self.db.commit()
        counts = {"done": 0, "error": 0, "skipped": 0}
        for _ in range(max_pages):
            result = self.step(max_depth, source, url_substring)
            if result is None:
                break
            url, status = result
            counts[status] += 1
            print("{} {}".format(status, url), flush=True)
        return counts

    def reprocess(self, max_pages):
        rows = self.db.execute(
            "SELECT url,source,depth,final_url,content_type,raw_path FROM urls"
            " WHERE status='done' AND (extractor_version IS NULL"
            " OR extractor_version!=?) ORDER BY rowid LIMIT ?",
            (EXTRACTOR_VERSION, max_pages),
        ).fetchall()
        counts = {"reprocessed": 0, "failed": 0}
        for url, source, depth, final_url, content_type, raw_path in rows:
            raw = (self.output / raw_path).read_bytes()
            try:
                title, content, links = extract_document(
                    raw, content_type, final_url
                )
                _, _, text_hash, text_path = self._save(
                    raw, content, content_type
                )
                self.db.execute(
                    "UPDATE urls SET title=?,text_sha256=?,text_path=?,"
                    " extractor_version=?,error=NULL WHERE url=?",
                    (title, text_hash, text_path, EXTRACTOR_VERSION, url),
                )
                self._record_links(final_url, source, depth, links)
                self._record_topic_json(final_url, source, depth)
                counts["reprocessed"] += 1
            except (OSError, ValueError, subprocess.TimeoutExpired,
                    zipfile.BadZipFile, ElementTree.ParseError) as error:
                self.db.execute(
                    "UPDATE urls SET extractor_version=?,error=? WHERE url=?",
                    (EXTRACTOR_VERSION, "extraction failed: {}".format(
                        str(error)[:450]), url),
                )
                counts["failed"] += 1
            self.db.commit()
        return counts

    def export(self):
        columns = (
            "url", "source", "parent_url", "depth", "status", "attempts",
            "final_url", "http_status", "content_type", "raw_sha256",
            "raw_path", "text_sha256", "text_path", "title", "fetched_at",
            "extractor_version", "error",
        )
        rows = self.db.execute(
            "SELECT {} FROM urls ORDER BY rowid".format(",".join(columns))
        ).fetchall()
        queue_path = self.output / "queue.jsonl"
        document_path = self.output / "documents.jsonl"
        with queue_path.open("w", encoding="utf-8") as queue, \
                document_path.open("w", encoding="utf-8") as documents:
            for row in rows:
                record = dict(zip(columns, row))
                encoded = json.dumps(record, ensure_ascii=False) + "\n"
                queue.write(encoded)
                if record["status"] == "done":
                    documents.write(encoded)
        links_path = self.output / "discovered_links.jsonl"
        with links_path.open("w", encoding="utf-8") as stream:
            for row in self.db.execute(
                "SELECT parent_url,child_url,anchor,disposition FROM links"
                " ORDER BY parent_url,child_url"
            ):
                record = dict(zip(
                    ("parent_url", "child_url", "anchor", "disposition"), row
                ))
                stream.write(json.dumps(record, ensure_ascii=False) + "\n")
        return document_path

    def status(self):
        return dict(self.db.execute(
            "SELECT status, COUNT(*) FROM urls GROUP BY status"
        ).fetchall())


class ScopedRedirectForOrigin(HTTPRedirectHandler):
    def __init__(self, origin):
        self.origin = origin

    def redirect_request(self, request, fp, code, msg, headers, newurl):
        parts = urlsplit(newurl)
        origin = "{}://{}".format(parts.scheme, parts.netloc)
        if origin != self.origin:
            raise ValueError("robots redirect out of origin")
        return super().redirect_request(request, fp, code, msg, headers, newurl)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=(
        "seed", "run", "reprocess", "status", "export"))
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--sources", nargs="+", choices=sorted(START_URLS),
                        default=["p4org", "eth_adv_net"])
    parser.add_argument("--max-pages", type=int, default=20)
    parser.add_argument("--max-depth", type=int, default=2)
    parser.add_argument("--run-source", choices=sorted(SOURCE_HOSTS))
    parser.add_argument("--url-substring")
    parser.add_argument("--delay", type=float, default=1.0)
    parser.add_argument("--timeout", type=int, default=20)
    parser.add_argument("--max-bytes", type=int, default=25_000_000)
    args = parser.parse_args()
    if (args.max_pages < 0 or args.max_depth < 0 or args.delay < 0 or
            args.timeout <= 0 or args.max_bytes <= 0):
        parser.error("invalid limits")
    collector = Collector(args.output, args.delay, args.timeout, args.max_bytes)
    if args.command == "seed":
        collector.seed(args.sources)
    elif args.command == "run":
        collector.run(args.max_pages, args.max_depth, args.run_source,
                      args.url_substring)
    elif args.command == "reprocess":
        print(json.dumps(collector.reprocess(args.max_pages), sort_keys=True))
    elif args.command == "export":
        print(collector.export())
    print(json.dumps(collector.status(), sort_keys=True))


if __name__ == "__main__":
    main()
