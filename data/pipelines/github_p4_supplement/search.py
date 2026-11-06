#!/usr/bin/env python3
"""Resume GitHub P4 repository/code search with auditable query partitions."""

import argparse
import csv
import datetime as dt
import fcntl
import gzip
import hashlib
import http.client
import json
import os
import sqlite3
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
DEFAULT_OUTPUT = ROOT / "data/raw/github_p4_supplement"
SWH_INPUT = ROOT / "data/raw/swh_p4/download_lists/swh_20260604_origin_download_list_v1.csv"
SEEDS = (
    ("language_p4", "repository", "language:P4", "created", "2007-01-01"),
    ("topic_p4", "repository", "topic:p4", "created", "2007-01-01"),
    ("topic_p4lang", "repository", "topic:p4lang", "created", "2007-01-01"),
    ("name_p4", "repository", "p4 in:name", "created", "2007-01-01"),
    ("readme_p4_programming", "repository", '"P4 programming" in:readme', "created", "2007-01-01"),
    ("extension_p4", "code", "extension:p4", "size", 0),
)
MAX_SEARCH_RESULTS = 1000
CODE_MAX_SIZE = 1000000000


def utc_now():
    return dt.datetime.now(dt.timezone.utc).isoformat()


def token_from_env(path):
    values = {}
    if path.is_file():
        for line in path.read_text().splitlines():
            key, separator, value = line.strip().partition("=")
            if separator and key in ("GITHUB_TOKEN", "GH_TOKEN", "GITHUB_API_TOKEN"):
                values[key] = value.strip().strip("\"'")
    return next((os.environ.get(key) or values.get(key) for key in
                 ("GITHUB_TOKEN", "GH_TOKEN", "GITHUB_API_TOKEN")
                 if os.environ.get(key) or values.get(key)), None)


def github_name(url):
    parsed = urllib.parse.urlsplit(url)
    if (parsed.hostname or "").lower() != "github.com":
        return None
    path = urllib.parse.unquote(parsed.path).strip("/")
    if path.lower().endswith(".git"):
        path = path[:-4]
    parts = path.split("/")
    if len(parts) != 2 or not all(parts):
        return None
    return path.lower()


def swh_names(path):
    if not path.is_file():
        raise FileNotFoundError("SWH origin list missing: {}".format(path))
    with path.open(newline="", encoding="utf-8-sig") as stream:
        return {name for row in csv.DictReader(stream)
                for name in [github_name(row["origin_url"])] if name}


def job_id(seed, low, high):
    return hashlib.sha256("{}|{}|{}".format(seed, low, high).encode()).hexdigest()


def query_for(job):
    if job["qualifier"] == "created":
        constraint = "created:{}..{}".format(job["low"], job["high"])
    else:
        constraint = "size:{}..{}".format(job["low"], job["high"])
    return job["base_query"] + " " + constraint


def split_job(job):
    if job["low"] == job["high"]:
        return None
    if job["qualifier"] == "created":
        low = dt.date.fromisoformat(job["low"])
        high = dt.date.fromisoformat(job["high"])
        midpoint = low + (high - low) // 2
        return [(low.isoformat(), midpoint.isoformat()),
                ((midpoint + dt.timedelta(days=1)).isoformat(), high.isoformat())]
    low, high = int(job["low"]), int(job["high"])
    middle = (low + high) // 2
    return [(low, middle), (middle + 1, high)]


def connect(root):
    root.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(str(root / "search.sqlite3"), timeout=30)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA journal_mode=WAL")
    connection.executescript("""
        CREATE TABLE IF NOT EXISTS jobs (
            job_id TEXT PRIMARY KEY, seed TEXT NOT NULL, kind TEXT NOT NULL,
            base_query TEXT NOT NULL, qualifier TEXT NOT NULL, low TEXT NOT NULL,
            high TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'pending',
            total_count INTEGER, next_page INTEGER NOT NULL DEFAULT 1,
            fetched_count INTEGER NOT NULL DEFAULT 0, incomplete INTEGER NOT NULL DEFAULT 0,
            attempts INTEGER NOT NULL DEFAULT 0, error TEXT, updated_at TEXT);
        CREATE INDEX IF NOT EXISTS jobs_ready ON jobs(status,kind,seed);
        CREATE TABLE IF NOT EXISTS repositories (
            github_id INTEGER PRIMARY KEY, full_name TEXT NOT NULL, html_url TEXT NOT NULL,
            default_branch TEXT, archived INTEGER, fork INTEGER, swh_name_match INTEGER NOT NULL,
            first_seen_at TEXT NOT NULL, last_seen_at TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS hits (
            seed TEXT NOT NULL, github_id INTEGER NOT NULL, path TEXT NOT NULL,
            blob_sha TEXT NOT NULL, job_id TEXT NOT NULL, page INTEGER NOT NULL,
            PRIMARY KEY(seed,github_id,path,blob_sha));
        CREATE INDEX IF NOT EXISTS hits_repo ON hits(github_id);
        CREATE TABLE IF NOT EXISTS events (
            at TEXT NOT NULL, event TEXT NOT NULL, details TEXT NOT NULL);
    """)
    return connection


def add_job(connection, seed, kind, base_query, qualifier, low, high):
    identifier = job_id(seed, low, high)
    connection.execute("""INSERT OR IGNORE INTO jobs
        (job_id,seed,kind,base_query,qualifier,low,high)
        VALUES(?,?,?,?,?,?,?)""",
        (identifier, seed, kind, base_query, qualifier, str(low), str(high)))
    return identifier


def initialize(connection, today):
    for seed, kind, base_query, qualifier, low in SEEDS:
        high = today.isoformat() if qualifier == "created" else CODE_MAX_SIZE
        add_job(connection, seed, kind, base_query, qualifier, low, high)
    connection.commit()


def page_path(root, job, page):
    return root / "pages" / job["seed"] / job["job_id"] / ("{:04d}.json.gz".format(page))


def atomic_page(path, data, metadata):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    with gzip.open(str(temporary), "wt", encoding="utf-8") as stream:
        json.dump({"metadata": metadata, "response": data}, stream, ensure_ascii=False)
    os.replace(str(temporary), str(path))


class SearchClient:
    def __init__(self, token, proxy=""):
        self.token = token
        self.opener = urllib.request.build_opener(urllib.request.ProxyHandler(
            {"http": proxy, "https": proxy} if proxy else {}))
        self.next_at = {"repository": 0.0, "code": 0.0}

    def request(self, kind, query, page):
        endpoint = "repositories" if kind == "repository" else "code"
        url = "https://api.github.com/search/{}?{}".format(endpoint,
            urllib.parse.urlencode({"q": query, "per_page": 100, "page": page}))
        delay = self.next_at[kind] - time.time()
        if delay > 0:
            time.sleep(delay)
        headers = {"User-Agent": "swh-p4-github-supplement/1.0",
                   "Accept": "application/vnd.github+json",
                   "Accept-Encoding": "gzip",
                   "X-GitHub-Api-Version": "2022-11-28"}
        if self.token:
            headers["Authorization"] = "Bearer " + self.token
        request = urllib.request.Request(url, headers=headers)
        try:
            with self.opener.open(request, timeout=60) as response:
                raw = response.read()
                if response.headers.get("Content-Encoding", "").lower() == "gzip":
                    raw = gzip.decompress(raw)
                payload = json.loads(raw)
                remaining = int(response.headers.get("X-RateLimit-Remaining", "1"))
                reset = float(response.headers.get("X-RateLimit-Reset", "0"))
                limit = int(response.headers.get("X-RateLimit-Limit", "10"))
                self.next_at[kind] = max(time.time() + 60.0 / max(1, limit),
                    reset + 2 if remaining == 0 else 0)
                return payload, {"url": url, "fetched_at": utc_now(),
                    "http_status": response.status, "rate_limit": limit,
                    "rate_remaining": remaining, "rate_reset": reset}
        except urllib.error.HTTPError as error:
            body = error.read(512).decode("utf-8", "replace")
            retry_after = error.headers.get("Retry-After")
            remaining = error.headers.get("X-RateLimit-Remaining")
            if error.code in (403, 429) and (retry_after or remaining == "0" or
                    "rate limit" in body.lower()):
                reset = float(error.headers.get("X-RateLimit-Reset", "0"))
                wait = max(float(retry_after or 0), reset - time.time() + 2, 60)
                self.next_at[kind] = time.time() + wait
                raise RuntimeError("rate_limited {} wait_s={:.0f}".format(kind, wait))
            raise RuntimeError("GitHub search HTTP {}: {}".format(error.code, body[:200]))


def event(connection, name, **fields):
    connection.execute("INSERT INTO events VALUES(?,?,?)",
                       (utc_now(), name, json.dumps(fields, sort_keys=True)))


def record_page(connection, job, page, payload, existing_names):
    now = utc_now()
    for item in payload.get("items", []):
        repository = item if job["kind"] == "repository" else item.get("repository")
        if not isinstance(repository, dict) or not repository.get("id"):
            continue
        name = repository.get("full_name") or ""
        if not name:
            continue
        repository_id = int(repository["id"])
        connection.execute("""INSERT INTO repositories VALUES(?,?,?,?,?,?,?,?,?)
            ON CONFLICT(github_id) DO UPDATE SET full_name=excluded.full_name,
            html_url=excluded.html_url, default_branch=COALESCE(excluded.default_branch,default_branch),
            archived=COALESCE(excluded.archived,archived), fork=COALESCE(excluded.fork,fork),
            swh_name_match=MAX(swh_name_match,excluded.swh_name_match),
            last_seen_at=excluded.last_seen_at""",
            (repository_id, name, repository.get("html_url") or
             "https://github.com/" + name, repository.get("default_branch"),
             repository.get("archived"), repository.get("fork"),
             int(name.lower() in existing_names), now, now))
        path = item.get("path", "") if job["kind"] == "code" else ""
        sha = item.get("sha", "") if job["kind"] == "code" else ""
        connection.execute("INSERT OR IGNORE INTO hits VALUES(?,?,?,?,?,?)",
                           (job["seed"], repository_id, path, sha,
                            job["job_id"], page))


def divide_job(connection, job, page, payload, existing_names, reason):
    total = int(payload.get("total_count", 0))
    parts = split_job(job)
    with connection:
        record_page(connection, job, page, payload, existing_names)
        fetched = job["fetched_count"] + len(payload.get("items", []))
        if parts is None:
            connection.execute("""UPDATE jobs SET status='unresolved',total_count=?,
                next_page=?,fetched_count=?,error=?,updated_at=? WHERE job_id=?""",
                (total, page + 1, fetched, reason, utc_now(), job["job_id"]))
            event(connection, "unresolved", job_id=job["job_id"], total=total,
                  query=query_for(job), reason=reason)
            return "unresolved"
        for low, high in parts:
            add_job(connection, job["seed"], job["kind"], job["base_query"],
                    job["qualifier"], low, high)
        connection.execute("""UPDATE jobs SET status='split',total_count=?,
            next_page=?,fetched_count=?,error=?,updated_at=? WHERE job_id=?""",
            (total, page + 1, fetched, reason, utc_now(), job["job_id"]))
        event(connection, "split", job_id=job["job_id"], total=total,
              parts=parts, reason=reason)
    return "split"


def process_job(connection, root, client, job, existing_names):
    page = job["next_page"]
    path = page_path(root, job, page)
    if path.is_file():
        with gzip.open(str(path), "rt", encoding="utf-8") as stream:
            saved = json.load(stream)
        payload = saved["response"]
        if payload.get("incomplete_results"):
            path.unlink()
            payload, metadata = client.request(job["kind"], query_for(job), page)
            if not payload.get("incomplete_results"):
                atomic_page(path, payload, metadata)
    else:
        payload, metadata = client.request(job["kind"], query_for(job), page)
        if not payload.get("incomplete_results"):
            atomic_page(path, payload, metadata)
    total = int(payload.get("total_count", 0))
    incomplete = bool(payload.get("incomplete_results"))
    if incomplete:
        raise RuntimeError("GitHub reports incomplete_results for {}".format(query_for(job)))
    if total > MAX_SEARCH_RESULTS:
        return divide_job(connection, job, page, payload, existing_names,
                          "more than 1000 search results")
    items = payload.get("items", [])
    fetched = job["fetched_count"] + len(items)
    expected_total = job["total_count"] if job["total_count"] is not None else total
    if not items and fetched < expected_total:
        return divide_job(connection, job, page, payload, existing_names,
                          "empty page before expected total")
    with connection:
        record_page(connection, job, page, payload, existing_names)
        last_page = fetched >= expected_total
        connection.execute("""UPDATE jobs SET status=?,total_count=COALESCE(total_count,?),
            next_page=?,fetched_count=?,error=NULL,updated_at=? WHERE job_id=?""",
            ("complete" if last_page else "pending", total,
             page + 1, fetched, utc_now(), job["job_id"]))
        if last_page:
            event(connection, "complete", job_id=job["job_id"], total=total, fetched=fetched)
    return "complete" if last_page else "page"


def requeue_recoverable_jobs(connection):
    with connection:
        cursor = connection.execute("""UPDATE jobs SET status='pending',attempts=0,
            error=NULL,updated_at=? WHERE status='failed' AND
            (error LIKE 'query grew beyond 1000 results:%' OR
             error LIKE 'empty page before expected total for %')""", (utc_now(),))
        if cursor.rowcount:
            event(connection, "requeue_recoverable", jobs=cursor.rowcount)


def run(connection, root, kinds, max_requests, proxy, env_file, swh_input):
    token = token_from_env(env_file)
    if not token:
        raise RuntimeError("GitHub token missing in env or {}".format(env_file))
    existing_names = swh_names(swh_input)
    requeue_recoverable_jobs(connection)
    client = SearchClient(token, proxy)
    requests = 0
    started = time.time()
    while max_requests == 0 or requests < max_requests:
        placeholders = ",".join("?" for _ in kinds)
        job = connection.execute("SELECT * FROM jobs WHERE status='pending' AND kind IN ({}) "
            "ORDER BY CASE WHEN kind='repository' THEN 0 ELSE 1 END,"
            "CASE WHEN next_page>1 THEN 0 ELSE 1 END,seed,low,job_id LIMIT 1".format(placeholders),
            kinds).fetchone()
        if job is None:
            print(json.dumps({"event": "queue_finished", "at": utc_now(),
                              "requests_this_run": requests}), flush=True)
            return
        was_cached = page_path(root, job, job["next_page"]).is_file()
        try:
            outcome = process_job(connection, root, client, job, existing_names)
            if not was_cached:
                requests += 1
            if requests % 10 == 0 or outcome in ("split", "unresolved"):
                summary = status(connection)
                print(json.dumps({"at": utc_now(), "event": outcome,
                    "seed": job["seed"], "query": query_for(job),
                    "page": job["next_page"], "requests_this_run": requests,
                    "elapsed_s": round(time.time() - started, 1), **summary}), flush=True)
        except (OSError, ValueError, RuntimeError, urllib.error.URLError,
                http.client.HTTPException) as error:
            attempts = job["attempts"] + 1
            delay = min(600, 30 * 2 ** min(attempts, 4))
            connection.execute("UPDATE jobs SET attempts=?,error=?,updated_at=? WHERE job_id=?",
                               (attempts, str(error)[:500], utc_now(), job["job_id"]))
            event(connection, "error", job_id=job["job_id"], error=str(error)[:500])
            connection.commit()
            print(json.dumps({"at": utc_now(), "event": "error", "query": query_for(job),
                              "error": str(error)[:500], "retry_s": delay}), flush=True)
            if attempts >= 5 and not str(error).startswith("rate_limited"):
                connection.execute("UPDATE jobs SET status='failed' WHERE job_id=?", (job["job_id"],))
                connection.commit()
            else:
                time.sleep(delay)


def status(connection):
    jobs = {"{}_{}".format(row[0], row[1]): row[2] for row in connection.execute(
        "SELECT kind,status,COUNT(*) FROM jobs GROUP BY kind,status")}
    repositories = connection.execute("SELECT COUNT(*),COALESCE(SUM(swh_name_match),0) FROM repositories").fetchone()
    code_repos = connection.execute("SELECT COUNT(DISTINCT github_id) FROM hits WHERE seed='extension_p4'").fetchone()[0]
    return {"jobs": jobs, "repos_seen": repositories[0],
            "swh_name_matches": repositories[1], "new_name_candidates": repositories[0] - repositories[1],
            "code_evidence_repos": code_repos,
            "saved_search_hits": connection.execute("SELECT COUNT(*) FROM hits").fetchone()[0]}


def export(connection, root):
    path = root / "candidate_repositories.csv"
    temporary = path.with_suffix(".csv.tmp")
    with temporary.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.writer(stream)
        writer.writerow(("github_id", "full_name", "html_url", "swh_name_match",
                         "has_code_search_p4_hit", "sources", "code_hit_count", "status"))
        for row in connection.execute("""SELECT r.github_id,r.full_name,r.html_url,r.swh_name_match,
            GROUP_CONCAT(DISTINCT h.seed) sources,
            SUM(CASE WHEN h.seed='extension_p4' THEN 1 ELSE 0 END) code_hits
            FROM repositories r JOIN hits h ON h.github_id=r.github_id
            GROUP BY r.github_id ORDER BY r.full_name COLLATE NOCASE"""):
            writer.writerow((row[0], row[1], row[2], row[3], int(row[5] > 0),
                             row[4], row[5], "file_search_evidence" if row[5] else "repository_search_candidate"))
    os.replace(str(temporary), str(path))
    return path


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("init", "run", "status", "export"))
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--env-file", type=Path, default=ROOT / ".env")
    parser.add_argument("--swh-input", type=Path, default=SWH_INPUT)
    parser.add_argument("--proxy", default="")
    parser.add_argument("--kind", choices=("all", "repository", "code"), default="all")
    parser.add_argument("--max-requests", type=int, default=0,
                        help="network request budget for this invocation; 0 runs until queue ends")
    args = parser.parse_args(argv)
    if args.max_requests < 0:
        parser.error("--max-requests must be nonnegative")
    connection = connect(args.output)
    lock_file = None
    try:
        if args.command == "run":
            lock_file = (args.output / "search.lock").open("a")
            try:
                fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                parser.error("another search process is already running")
        if args.command == "init":
            initialize(connection, dt.date.today())
        elif args.command == "run":
            kinds = ("repository", "code") if args.kind == "all" else (args.kind,)
            run(connection, args.output, kinds, args.max_requests, args.proxy,
                args.env_file, args.swh_input)
        elif args.command == "export":
            print(export(connection, args.output))
        print(json.dumps(status(connection), sort_keys=True))
    finally:
        connection.close()
        if lock_file is not None:
            lock_file.close()


if __name__ == "__main__":
    main()
