#!/usr/bin/env python3
"""Shared local state and URL rules for the full-history SWH collector."""

import csv
import contextlib
import datetime
import hashlib
import json
import os
import sqlite3
import threading
import time
from pathlib import Path
from urllib.parse import unquote, urlsplit


def utc_now():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def stable_id(value):
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def atomic_json(path, data):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    with temporary.open("w", encoding="utf-8") as output:
        json.dump(data, output, ensure_ascii=True, indent=2, sort_keys=True)
        output.write("\n")
    os.replace(str(temporary), str(path))


def git_source(url):
    """Only combine URL spellings with known identical Git clone semantics.

    Repository moves/renames require remote evidence and are not guessed here.
    Package-page upstream links are intentionally not treated as Git identities.
    """
    parsed = urlsplit(url)
    host = (parsed.hostname or "").lower()
    path = unquote(parsed.path).strip("/")
    if parsed.username or parsed.password or parsed.port:
        return None
    if parsed.scheme not in ("http", "https", "git") or not path:
        return None
    if host == "github.com":
        parts = path.split("/")
        if len(parts) != 2:
            return None
        if path.endswith(".git"):
            path = path[:-4]
        path = path.lower()
        kind = "github"
    elif host == "bitbucket.org":
        if len(path.split("/")) != 2:
            return None
        path = path[:-4] if path.endswith(".git") else path
        kind = "bitbucket"
    elif "gitlab" in host or host == "salsa.debian.org":
        if "/-/" in path or len(path.split("/")) < 2:
            return None
        path = path[:-4] if path.endswith(".git") else path
        kind = "gitlab"
    elif host.startswith("git.") or path.endswith(".git"):
        path = path[:-4] if path.endswith(".git") else path
        kind = "git"
    else:
        return None
    canonical = "https://{}/{}".format(host, path)
    clone_url = canonical + ".git" if kind in ("github", "bitbucket", "gitlab") else url
    return {"repo_id": stable_id(canonical), "canonical_url": canonical,
            "clone_url": clone_url, "host": host, "forge": kind,
            "repo_path": path}


class State:
    def __init__(self, root):
        self.root = Path(root)
        (self.root / "manifests").mkdir(parents=True, exist_ok=True)
        self.lock = threading.RLock()
        self.batch_depth = 0
        self.db = sqlite3.connect(str(self.root / "manifests/state.sqlite3"),
                                  check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self.db.executescript("""
            PRAGMA journal_mode=WAL;
            PRAGMA synchronous=FULL;
            CREATE TABLE IF NOT EXISTS origins (
              origin_id TEXT PRIMARY KEY, origin_url TEXT, repo_id TEXT,
              input_json TEXT NOT NULL, kind TEXT NOT NULL, status TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS repos (
              repo_id TEXT PRIMARY KEY, canonical_url TEXT, clone_url TEXT,
              host TEXT, forge TEXT, repo_path TEXT, status TEXT DEFAULT 'pending',
              attempts INTEGER DEFAULT 0, next_at REAL DEFAULT 0,
              started_at TEXT, finished_at TEXT, elapsed_s REAL, bytes INTEGER,
              commits INTEGER, error TEXT, resolved_url TEXT,
              lfs_status TEXT DEFAULT 'pending', submodule_status TEXT DEFAULT 'pending');
            CREATE TABLE IF NOT EXISTS api_tasks (
              task_id TEXT PRIMARY KEY, repo_id TEXT, host TEXT, forge TEXT,
              url TEXT, kind TEXT, status TEXT DEFAULT 'pending',
              attempts INTEGER DEFAULT 0, next_at REAL DEFAULT 0,
              error TEXT, records INTEGER DEFAULT 0, finished_at TEXT);
            CREATE TABLE IF NOT EXISTS packages (
              origin_id TEXT PRIMARY KEY, status TEXT DEFAULT 'pending',
              attempts INTEGER DEFAULT 0, next_at REAL DEFAULT 0, error TEXT);
            CREATE TABLE IF NOT EXISTS assets (
              asset_id TEXT PRIMARY KEY, origin_id TEXT, url TEXT, filename TEXT,
              status TEXT DEFAULT 'pending', attempts INTEGER DEFAULT 0,
              next_at REAL DEFAULT 0, bytes INTEGER DEFAULT 0, sha256 TEXT, error TEXT);
            CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value TEXT);
            CREATE INDEX IF NOT EXISTS repo_ready ON repos(status,next_at);
            CREATE INDEX IF NOT EXISTS api_ready ON api_tasks(status,next_at);
        """)
        self.db.commit()

    def rows(self, query, params=()):
        with self.lock:
            return [dict(row) for row in self.db.execute(query, params).fetchall()]

    def execute(self, query, params=()):
        with self.lock:
            result = self.db.execute(query, params)
            if not self.batch_depth:
                self.db.commit()
            return result.rowcount

    @contextlib.contextmanager
    def batch(self):
        with self.lock:
            self.batch_depth += 1
            try:
                yield
                if self.batch_depth == 1:
                    self.db.commit()
            except Exception:
                self.db.rollback()
                raise
            finally:
                self.batch_depth -= 1

    def event(self, event, **fields):
        value = dict(fields, at=utc_now(), event=event)
        with self.lock:
            with (self.root / "events.jsonl").open("a", encoding="utf-8") as output:
                output.write(json.dumps(value, ensure_ascii=True) + "\n")

    def enqueue_api(self, repo, url, kind):
        self.execute("""INSERT OR IGNORE INTO api_tasks
            (task_id,repo_id,host,forge,url,kind) VALUES (?,?,?,?,?,?)""",
                     (stable_id(url), repo["repo_id"], repo["host"],
                      repo["forge"], url, kind))

    def import_input(self, path, shard_index, shard_count):
        digest = hashlib.sha256(Path(path).read_bytes()).hexdigest()
        identity = json.dumps([digest, shard_index, shard_count])
        previous = self.rows("SELECT value FROM metadata WHERE key='input_identity'")
        if previous and previous[0]["value"] != identity:
            raise ValueError("Input/shard differs from this output root. Use another root.")
        self.execute("INSERT OR IGNORE INTO metadata VALUES ('input_identity', ?)", (identity,))
        with Path(path).open(newline="", encoding="utf-8-sig") as source:
            rows = list(csv.DictReader(source))
        with self.lock:
            for row in rows:
                repo = git_source(row["origin_url"])
                identity_key = repo["repo_id"] if repo else row["origin_id"]
                if int(identity_key, 16) % shard_count != shard_index:
                    continue
                if repo:
                    self.db.execute("""INSERT OR IGNORE INTO repos
                        (repo_id,canonical_url,clone_url,host,forge,repo_path)
                        VALUES (:repo_id,:canonical_url,:clone_url,:host,:forge,:repo_path)""", repo)
                self.db.execute("INSERT OR IGNORE INTO origins VALUES (?,?,?,?,?,?)",
                                (row["origin_id"], row["origin_url"],
                                 repo["repo_id"] if repo else None, json.dumps(row),
                                 "git" if repo else "package_or_unresolved", "registered"))
                if not repo:
                    self.db.execute("INSERT OR IGNORE INTO packages(origin_id) VALUES (?)",
                                    (row["origin_id"],))
            self.db.commit()
        atomic_json(self.root / "manifests/input.json", {
            "source": str(Path(path).resolve()), "sha256": digest,
            "raw_input_rows": len(rows), "shard_index": shard_index,
            "shard_count": shard_count, "imported_at": utc_now(),
            "canonical_rule": "conservative known forge clone URL aliases; moves kept separate until verified",
        })
        with (self.root / "manifests/origin_repo_map.jsonl").open("w") as output:
            for row in self.rows("SELECT * FROM origins ORDER BY origin_id"):
                output.write(json.dumps(row) + "\n")

    def recover(self, api_only=False):
        # The process lock must be held before reclaiming interrupted work.
        tables = ("api_tasks",) if api_only else ("repos", "api_tasks", "packages", "assets")
        for table in tables:
            self.execute("UPDATE {} SET status='retry_wait',attempts=MAX(0,attempts-1),next_at=0 WHERE status='running'".format(table))
        if api_only:
            return
        # An interruption after mirror verification can leave LFS/index auxiliaries
        # unfinished. Reuse the existing object database instead of dropping it.
        self.execute("""UPDATE repos SET status='retry_wait',next_at=0
            WHERE status='complete' AND attempts<5 AND
              (lfs_status IN ('pending','error') OR submodule_status='pending')""")


def duration(seconds):
    if seconds is None:
        return "unknown"
    seconds = int(max(0, seconds))
    return "{}h{:02d}m{:02d}s".format(seconds // 3600, seconds // 60 % 60, seconds % 60)
