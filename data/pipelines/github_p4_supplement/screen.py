#!/usr/bin/env python3
"""Resume GitHub P4 file discovery and static screening from search evidence."""

import argparse
import base64
import collections
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
import csv
import datetime as dt
import fcntl
import json
import os
from pathlib import Path
import re
import shutil
import sqlite3
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request


ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "data/tools"))
from swh_p4_content_screen import git_blob_sha1, inspect_content
from swh_p4_candidate_quality import inspect_quality
from swh_p4_review_screen import classify_review


SEARCH_DB = ROOT / "data/raw/github_p4_supplement/search.sqlite3"
DEFAULT_OUTPUT = ROOT / "data/raw/github_p4_supplement/screen"
TOKEN_NAMES = ("GITHUB_API_TOKEN", "GITHUB2_API_TOKEN")
SHA1 = re.compile(r"^[0-9a-f]{40}$")
MAX_JSON_BYTES = 20 * 1024 * 1024
MAX_BLOB_BYTES = 2 * 1024 * 1024
MAX_TREE_REQUESTS_PER_REPO = 128


def now():
    return dt.datetime.now(dt.timezone.utc).isoformat()


def read_tokens(env_file):
    values = {}
    if env_file.is_file():
        for line in env_file.read_text(encoding="utf-8").splitlines():
            key, separator, value = line.strip().partition("=")
            if separator and key in TOKEN_NAMES:
                values[key] = value.strip().strip("\"'")
    tokens = [(key, os.environ.get(key) or values.get(key)) for key in TOKEN_NAMES]
    return [(key, value) for key, value in tokens if value]


def connect(path):
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(str(path), timeout=60)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA journal_mode=WAL")
    connection.executescript("""
        CREATE TABLE IF NOT EXISTS repositories (
            github_id INTEGER PRIMARY KEY, full_name TEXT NOT NULL,
            swh_name_match INTEGER NOT NULL, code_hit_count INTEGER NOT NULL DEFAULT 0,
            tree_status TEXT NOT NULL DEFAULT 'pending', tree_sha TEXT,
            head_sha TEXT, head_p4_count INTEGER, error TEXT, attempts INTEGER NOT NULL DEFAULT 0,
            updated_at TEXT);
        CREATE INDEX IF NOT EXISTS repositories_pending ON repositories(tree_status,code_hit_count);
        CREATE TABLE IF NOT EXISTS files (
            github_id INTEGER NOT NULL, source TEXT NOT NULL, path TEXT NOT NULL,
            oid TEXT NOT NULL, mode TEXT,
            PRIMARY KEY (github_id,source,path,oid));
        CREATE INDEX IF NOT EXISTS files_oid ON files(oid);
        CREATE TABLE IF NOT EXISTS blobs (
            oid TEXT PRIMARY KEY, status TEXT NOT NULL DEFAULT 'pending',
            raw_bytes INTEGER, screen_json TEXT, review_json TEXT, quality_json TEXT,
            version_status TEXT, review_disposition TEXT, quality_status TEXT,
            error TEXT, attempts INTEGER NOT NULL DEFAULT 0, updated_at TEXT);
        CREATE INDEX IF NOT EXISTS blobs_pending ON blobs(status);
        CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
    """)
    existing = {row[1] for row in connection.execute("PRAGMA table_info(blobs)")}
    for field in ("version_status", "review_disposition", "quality_status"):
        if field not in existing:
            connection.execute("ALTER TABLE blobs ADD COLUMN {} TEXT".format(field))
    for row in connection.execute("""SELECT oid,screen_json,review_json,quality_json FROM blobs
            WHERE status='complete' AND version_status IS NULL""").fetchall():
        screen = json.loads(row[1])
        review = json.loads(row[2]) if row[2] else None
        quality = json.loads(row[3]) if row[3] else None
        connection.execute("""UPDATE blobs SET version_status=?,review_disposition=?,
            quality_status=? WHERE oid=?""",
            (review["review_version_status"] if review and
             review["review_version_status"] != "unknown" else screen["version_status"],
             review["review_disposition"] if review else None,
             quality["quality_status"] if quality else None, row[0]))
    connection.commit()
    return connection


def initialize(connection, search_db):
    source = sqlite3.connect("file:{}?mode=ro".format(search_db), uri=True)
    source.row_factory = sqlite3.Row
    try:
        jobs = source.execute("SELECT status,COUNT(*) n FROM jobs GROUP BY status").fetchall()
        if any(row["status"] in ("pending", "failed") for row in jobs):
            raise RuntimeError("search still has pending/failed jobs")
        with connection:
            for row in source.execute("SELECT github_id,full_name,swh_name_match FROM repositories"):
                connection.execute("""INSERT OR IGNORE INTO repositories
                    (github_id,full_name,swh_name_match) VALUES(?,?,?)""",
                    (row["github_id"], row["full_name"], row["swh_name_match"]))
            for row in source.execute("""SELECT github_id,path,blob_sha FROM hits
                    WHERE seed='extension_p4' AND path!='' AND blob_sha!=''"""):
                oid = row["blob_sha"].lower()
                if not SHA1.fullmatch(oid) or not row["path"].lower().endswith(".p4"):
                    continue
                connection.execute("INSERT OR IGNORE INTO files VALUES(?,?,?,?,NULL)",
                                   (row["github_id"], "search", row["path"], oid))
                connection.execute("INSERT OR IGNORE INTO blobs(oid) VALUES(?)", (oid,))
            connection.execute("""UPDATE repositories SET code_hit_count=
                (SELECT COUNT(*) FROM files WHERE files.github_id=repositories.github_id
                 AND source='search')""")
            connection.execute("INSERT OR IGNORE INTO meta VALUES('created_at',?)", (now(),))
            connection.execute("INSERT OR REPLACE INTO meta VALUES('search_db',?)",
                               (str(search_db.resolve()),))
    finally:
        source.close()


class ApiError(Exception):
    def __init__(self, status, message):
        super().__init__(message)
        self.status = status


class RatePool:
    def __init__(self, tokens, proxy, interval):
        if not tokens:
            raise RuntimeError("GITHUB_API_TOKEN or GITHUB2_API_TOKEN is required")
        self.states = [{"name": name, "token": value, "next_at": 0.0,
                        "remaining": None, "reset": 0.0} for name, value in tokens]
        self.interval = interval
        self.lock = threading.Lock()
        self.global_pause = 0.0
        self.opener = urllib.request.build_opener(urllib.request.ProxyHandler(
            {"http": proxy, "https": proxy} if proxy else {}))
        self.requests = collections.Counter()

    def acquire(self):
        while True:
            with self.lock:
                state = min(self.states, key=lambda item: item["next_at"])
                wait = max(state["next_at"], self.global_pause) - time.time()
                if wait <= 0:
                    state["next_at"] = time.time() + self.interval
                    self.requests[state["name"]] += 1
                    return state
            time.sleep(min(max(wait, 0.05), 10))

    def observe(self, state, headers, status, body=""):
        remaining = headers.get("X-RateLimit-Remaining")
        reset = headers.get("X-RateLimit-Reset")
        retry = headers.get("Retry-After")
        limited = status in (403, 429) and (
            remaining == "0" or retry or "rate limit" in body.lower() or
            "secondary" in body.lower())
        with self.lock:
            if reset:
                reset_at = float(reset)
                if reset_at != state["reset"]:
                    state["reset"] = reset_at
                    state["remaining"] = None
            if remaining is not None:
                count = int(remaining)
                state["remaining"] = (count if state["remaining"] is None
                                      else min(count, state["remaining"]))
                if state["remaining"] == 0:
                    state["next_at"] = max(state["next_at"], state["reset"] + 2)
            if limited:
                delay = float(retry) if retry and retry.isdigit() else 65.0
                state["next_at"] = max(state["next_at"], time.time() + delay)
                if remaining != "0":
                    self.global_pause = max(self.global_pause, time.time() + delay)
        return limited

    def get_json(self, path):
        url = "https://api.github.com" + path
        for attempt in range(4):
            state = self.acquire()
            request = urllib.request.Request(url, headers={
                "Authorization": "Bearer " + state["token"],
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
                "User-Agent": "swh-p4-github-screen/1.0"})
            try:
                with self.opener.open(request, timeout=60) as response:
                    body = response.read(MAX_JSON_BYTES + 1)
                    self.observe(state, response.headers, response.status)
                    if len(body) > MAX_JSON_BYTES:
                        raise ApiError("too_large", "GitHub JSON response exceeds 20 MiB")
                    try:
                        return json.loads(body)
                    except json.JSONDecodeError:
                        if attempt == 3:
                            raise ApiError("invalid_json", "incomplete GitHub JSON response")
                        time.sleep(min(2 ** attempt, 8))
                        continue
            except urllib.error.HTTPError as error:
                body = error.read(1024).decode("utf-8", "replace")
                limited = self.observe(state, error.headers, error.code, body)
                if limited and attempt < 3:
                    continue
                if error.code in (500, 502, 503, 504) and attempt < 3:
                    time.sleep(min(2 ** attempt, 8))
                    continue
                raise ApiError(error.code, "HTTP {}: {}".format(error.code, body[:160]))
            except (urllib.error.URLError, TimeoutError, OSError) as error:
                if attempt == 3:
                    raise ApiError("network", type(error).__name__)
                time.sleep(min(2 ** attempt, 8))
        raise ApiError("retry_limit", "request retry limit reached")


def repo_path(name):
    return "/repos/" + "/".join(urllib.parse.quote(part, safe="") for part in name.split("/"))


def tree_files(pool, name, tree_sha, recursive=True):
    suffix = "?recursive=1" if recursive else ""
    payload = pool.get_json(repo_path(name) + "/git/trees/" +
                            urllib.parse.quote(tree_sha, safe="") + suffix)
    if payload.get("truncated"):
        if not recursive:
            raise ApiError("truncated", "nonrecursive tree is truncated")
        root_sha = payload.get("sha")
        if not root_sha:
            raise ApiError("invalid_tree", "truncated tree has no SHA")
        pending = [(root_sha, "")]
        files = []
        requests = 1
        while pending:
            if requests >= MAX_TREE_REQUESTS_PER_REPO:
                raise ApiError("tree_limit", "subtree request limit reached")
            sha, prefix = pending.pop()
            child = pool.get_json(repo_path(name) + "/git/trees/" + sha)
            requests += 1
            if child.get("truncated"):
                raise ApiError("truncated", "nonrecursive subtree is truncated")
            for entry in child.get("tree", []):
                path = prefix + entry.get("path", "")
                if entry.get("type") == "tree":
                    pending.append((entry["sha"], path + "/"))
                elif entry.get("type") == "blob" and path.lower().endswith(".p4"):
                    files.append((path, entry["sha"], entry.get("mode")))
        return root_sha, files
    files = [(entry["path"], entry["sha"], entry.get("mode"))
             for entry in payload.get("tree", [])
             if entry.get("type") == "blob" and entry.get("path", "").lower().endswith(".p4")]
    return payload["sha"], files


def fetch_tree(pool, repo):
    metadata = pool.get_json("/repositories/{}".format(repo["github_id"]))
    if metadata.get("id") != repo["github_id"]:
        raise ApiError("id_mismatch", "repository ID changed")
    name = metadata["full_name"]
    branch = metadata.get("default_branch")
    if not branch:
        return {"name": name, "status": "empty", "tree_sha": None,
                "head_sha": None, "files": []}
    tree_sha, files = tree_files(pool, name, branch)
    head_sha = None
    if files:
        commit = pool.get_json(repo_path(name) + "/commits/" +
                               urllib.parse.quote(branch, safe=""))
        head_sha = commit["sha"]
        if commit.get("commit", {}).get("tree", {}).get("sha") != tree_sha:
            tree_sha, files = tree_files(pool, name, commit["commit"]["tree"]["sha"])
    return {"name": name, "status": "complete", "tree_sha": tree_sha,
            "head_sha": head_sha, "files": files}


def blob_cache_path(root, oid):
    return root / "raw_blobs" / oid[:2] / oid


def fetch_blob(pool, oid, names, root):
    path = blob_cache_path(root, oid)
    if path.is_file():
        data = path.read_bytes()
        if git_blob_sha1(data) != oid:
            raise ApiError("cache_corrupt", "cached blob hash mismatch")
    else:
        data = None
        last_error = None
        for name in names:
            try:
                payload = pool.get_json(repo_path(name) + "/git/blobs/" + oid)
            except ApiError as error:
                last_error = error
                if error.status in (404, 410):
                    continue
                raise
            if payload.get("encoding") != "base64":
                raise ApiError("encoding", "unsupported blob encoding")
            if int(payload.get("size", 0)) > MAX_BLOB_BYTES:
                raise ApiError("too_large", "blob exceeds 2 MiB")
            data = base64.b64decode(payload["content"])
            if len(data) > MAX_BLOB_BYTES:
                raise ApiError("too_large", "decoded blob exceeds 2 MiB")
            if payload.get("sha") != oid or git_blob_sha1(data) != oid:
                raise ApiError("hash_mismatch", "Git blob SHA mismatch")
            path.parent.mkdir(parents=True, exist_ok=True)
            temporary = path.with_suffix(".tmp")
            temporary.write_bytes(data)
            os.replace(str(temporary), str(path))
            break
        if data is None:
            raise last_error or ApiError(404, "blob unavailable in recorded repositories")
    screen = inspect_content(oid, data)
    review = (classify_review(screen, data)
              if screen["version_status"] in ("unknown", "mixed_or_conflicted", "uninspected")
              else None)
    candidate = (screen["version_status"] == "p4_16_likely" or
                 review is not None and review["review_disposition"] == "p4_16_candidate")
    quality = inspect_quality(oid, data) if candidate else None
    return {"raw_bytes": len(data), "screen": screen, "review": review,
            "quality": quality}


def save_tree(connection, repo, result, error):
    with connection:
        if error:
            attempts = repo["attempts"] + 1
            status = "failed" if attempts >= 5 or error.status in (
                401, 403, 404, 410, 451, "truncated", "tree_limit",
                "invalid_tree", "id_mismatch") else "pending"
            connection.execute("""UPDATE repositories SET tree_status=?,attempts=?,
                error=?,updated_at=? WHERE github_id=?""",
                (status, attempts, str(error), now(), repo["github_id"]))
            return
        connection.execute("DELETE FROM files WHERE github_id=? AND source='head'",
                           (repo["github_id"],))
        for path, oid, mode in result["files"]:
            if not SHA1.fullmatch(oid):
                continue
            connection.execute("INSERT OR IGNORE INTO files VALUES(?,?,?,?,?)",
                               (repo["github_id"], "head", path, oid, mode))
            connection.execute("INSERT OR IGNORE INTO blobs(oid) VALUES(?)", (oid,))
        connection.execute("""UPDATE repositories SET full_name=?,tree_status=?,
            tree_sha=?,head_sha=?,head_p4_count=?,error=NULL,updated_at=?
            WHERE github_id=?""", (result["name"], result["status"], result["tree_sha"],
                result["head_sha"], len(result["files"]), now(), repo["github_id"]))


def save_blob(connection, oid, result, error):
    with connection:
        if error:
            row = connection.execute("SELECT attempts FROM blobs WHERE oid=?", (oid,)).fetchone()
            attempts = row["attempts"] + 1
            status = "failed" if attempts >= 5 or error.status in (
                401, 403, 404, 410, 451, "too_large", "encoding", "hash_mismatch",
                "cache_corrupt") else "pending"
            connection.execute("""UPDATE blobs SET status=?,attempts=?,error=?,
                updated_at=? WHERE oid=?""", (status, attempts, str(error), now(), oid))
            return
        connection.execute("""UPDATE blobs SET status='complete',raw_bytes=?,
            screen_json=?,review_json=?,quality_json=?,version_status=?,
            review_disposition=?,quality_status=?,error=NULL,updated_at=? WHERE oid=?""",
            (result["raw_bytes"], json.dumps(result["screen"], sort_keys=True),
             json.dumps(result["review"], sort_keys=True) if result["review"] else None,
             json.dumps(result["quality"], sort_keys=True) if result["quality"] else None,
             result["review"]["review_version_status"] if result["review"] and
             result["review"]["review_version_status"] != "unknown"
             else result["screen"]["version_status"],
             result["review"]["review_disposition"] if result["review"] else None,
             result["quality"]["quality_status"] if result["quality"] else None,
             now(), oid))


def status(connection):
    repos = {row[0]: row[1] for row in connection.execute(
        "SELECT tree_status,COUNT(*) FROM repositories GROUP BY tree_status")}
    blobs = {row[0]: row[1] for row in connection.execute(
        "SELECT status,COUNT(*) FROM blobs GROUP BY status")}
    versions = {row[0]: row[1] for row in connection.execute("""
        SELECT version_status,COUNT(*) FROM blobs WHERE status='complete'
        GROUP BY version_status""")}
    return {"at": now(), "repositories": repos, "blobs": blobs,
            "versions": versions,
            "repo_total": sum(repos.values()), "blob_total": sum(blobs.values()),
            "head_p4_repositories": connection.execute(
                "SELECT COUNT(*) FROM repositories WHERE head_p4_count>0").fetchone()[0],
            "head_p4_files": connection.execute(
                "SELECT COALESCE(SUM(head_p4_count),0) FROM repositories").fetchone()[0]}


def save_status(root, value):
    path = root / "status.json"
    temporary = root / "status.json.tmp"
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(str(temporary), str(path))


def run_phase(connection, root, pool, phase, workers, limit):
    if phase == "trees":
        rows = connection.execute("""SELECT * FROM repositories
            WHERE tree_status='pending' ORDER BY code_hit_count>0 DESC,
            code_hit_count,github_id LIMIT ?""", (limit or -1,)).fetchall()
    else:
        rows = connection.execute("""SELECT oid FROM blobs WHERE status='pending'
            ORDER BY oid LIMIT ?""", (limit or -1,)).fetchall()
    target = len(rows)
    done = 0
    submitted = 0
    started = time.time()
    last_log = started
    paused = False
    print(json.dumps({"at": now(), "event": "phase_start", "phase": phase,
                      "target_this_run": target}), flush=True)
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {}
        while submitted < target or futures:
            while not paused and submitted < target and len(futures) < workers * 4:
                if shutil.disk_usage(str(root)).free < 100 * 1024 ** 3:
                    paused = True
                    print(json.dumps({"at": now(), "event": "disk_pause",
                                      "phase": phase, "min_free_gib": 100}), flush=True)
                    break
                row = rows[submitted]
                if phase == "trees":
                    future = executor.submit(fetch_tree, pool, row)
                else:
                    names = [item[0] for item in connection.execute("""
                        SELECT DISTINCT r.full_name FROM files f JOIN repositories r
                        ON r.github_id=f.github_id WHERE f.oid=? ORDER BY
                        CASE f.source WHEN 'head' THEN 0 ELSE 1 END LIMIT 5""",
                        (row["oid"],))]
                    future = executor.submit(fetch_blob, pool, row["oid"], names, root)
                futures[future] = row
                submitted += 1
            if not futures:
                break
            completed, _ = wait(futures, timeout=30, return_when=FIRST_COMPLETED)
            for future in completed:
                row = futures.pop(future)
                try:
                    result, error = future.result(), None
                except ApiError as exc:
                    result, error = None, exc
                except Exception as exc:
                    result, error = None, ApiError("internal", type(exc).__name__ + ": " + str(exc)[:160])
                if phase == "trees":
                    save_tree(connection, row, result, error)
                else:
                    save_blob(connection, row["oid"], result, error)
                done += 1
            if done % 100 == 0 or done == target or time.time() - last_log >= 60:
                elapsed = time.time() - started
                rate = done / elapsed if elapsed else 0
                info = status(connection)
                info.update({"event": "progress", "phase": phase,
                             "done_this_run": done, "target_this_run": target,
                             "in_flight": len(futures),
                             "elapsed_s": round(elapsed, 1),
                             "rate_per_s": round(rate, 3),
                             "eta_s": round((target - done) / rate) if rate else None,
                             "requests_by_token": dict(pool.requests)})
                save_status(root, info)
                print(json.dumps(info, sort_keys=True), flush=True)
                last_log = time.time()


def export(connection, root):
    with (root / "file_evidence.csv").open("w", newline="", encoding="utf-8") as stream:
        writer = csv.writer(stream)
        writer.writerow(("github_id", "full_name", "source", "path", "blob_oid",
                         "mode", "fetch_status", "version_status", "review_status",
                         "quality_status"))
        for row in connection.execute("""SELECT r.github_id,r.full_name,f.source,f.path,
                f.oid,f.mode,b.status,b.version_status,b.review_disposition,b.quality_status
                FROM files f JOIN repositories r ON r.github_id=f.github_id
                LEFT JOIN blobs b ON b.oid=f.oid ORDER BY r.github_id,f.source,f.path"""):
            writer.writerow(tuple(row))
    with (root / "repository_screen.csv").open("w", newline="", encoding="utf-8") as stream:
        writer = csv.writer(stream)
        writer.writerow(("github_id", "full_name", "swh_name_match", "tree_status",
                         "tree_sha", "head_sha", "head_p4_count", "code_hit_count",
                         "p4_16_likely_blobs", "unfetched_blobs"))
        for row in connection.execute("""SELECT r.github_id,r.full_name,r.swh_name_match,
                r.tree_status,r.tree_sha,r.head_sha,r.head_p4_count,r.code_hit_count,
                COUNT(DISTINCT CASE WHEN b.version_status='p4_16_likely'
                    THEN b.oid END),
                COUNT(DISTINCT CASE WHEN b.status!='complete' THEN b.oid END)
                FROM repositories r LEFT JOIN files f ON f.github_id=r.github_id
                LEFT JOIN blobs b ON b.oid=f.oid
                GROUP BY r.github_id ORDER BY r.github_id"""):
            writer.writerow(tuple(row))


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("init", "run", "status", "export", "retry-failed"))
    parser.add_argument("--search-db", type=Path, default=SEARCH_DB)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--env-file", type=Path, default=ROOT / ".env")
    parser.add_argument("--proxy", default=os.environ.get("SWH_HISTORY_PROXY", ""))
    parser.add_argument("--phase", choices=("all", "trees", "blobs"), default="all")
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--max-repos", type=int, default=0)
    parser.add_argument("--max-blobs", type=int, default=0)
    parser.add_argument("--token-interval", type=float, default=0.8)
    args = parser.parse_args(argv)
    if args.workers < 1 or args.max_repos < 0 or args.max_blobs < 0 or args.token_interval < 0.72:
        parser.error("workers must be positive; limits nonnegative; token interval >= 0.72 s")
    args.output.mkdir(parents=True, exist_ok=True)
    lock = (args.output / "screen.lock").open("a")
    if args.command in ("init", "run", "retry-failed"):
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            parser.error("another screen process is running")
    connection = connect(args.output / "screen.sqlite3")
    try:
        if args.command == "init":
            initialize(connection, args.search_db)
        elif args.command == "retry-failed":
            with connection:
                connection.execute("""UPDATE repositories SET tree_status='pending',
                    attempts=0 WHERE tree_status='failed'""")
                connection.execute("""UPDATE blobs SET status='pending',attempts=0
                    WHERE status='failed'""")
        elif args.command == "run":
            pool = RatePool(read_tokens(args.env_file), args.proxy, args.token_interval)
            if args.phase in ("all", "blobs"):
                run_phase(connection, args.output, pool, "blobs", args.workers, args.max_blobs)
            if args.phase in ("all", "trees"):
                run_phase(connection, args.output, pool, "trees", args.workers, args.max_repos)
            if args.phase == "all":
                run_phase(connection, args.output, pool, "blobs", args.workers, args.max_blobs)
        elif args.command == "export":
            export(connection, args.output)
        result = status(connection)
        save_status(args.output, result)
        print(json.dumps(result, sort_keys=True))
    finally:
        connection.close()
        lock.close()


if __name__ == "__main__":
    main()
