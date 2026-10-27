#!/usr/bin/env python3
"""Build a repository-level P4 corpus from a Software Heritage graph export.

The collector follows the repository extraction policy described by StarCoder2
and The Stack v2, adapted for P4:

* keep GitHub origins and, by default, the newest main/master/HEAD revision;
  ``--selection-policy latest-any`` and ``all`` broaden branch recall;
* recursively reconstruct the complete directory tree (depth 64 by default);
* retain a repository when a tracked ``.p4`` is classified as P4_16;
* deduplicate payloads by ``directory_id`` while preserving provenance aliases;
* fetch P4 files for version inspection and optionally materialize every file.

The graph export is intentionally an input to this script. A REST API walk is
not a substitute for a global graph scan. The input files are JSONL/CSV/TSV
(optionally gzip-compressed) dictionaries containing the fields listed in
``--help``. The directory-entry table is indexed in SQLite so the traversal
does not require the whole table in memory.
"""

from __future__ import print_function

import argparse
import csv
import datetime as dt
import gzip
import hashlib
import json
import os
import re
import sqlite3
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import deque
from pathlib import Path


DEFAULT_OUTPUT_ROOT = Path("data/raw/swh_p4")
DEFAULT_API_URL = "https://archive.softwareheritage.org/api/1"
DEFAULT_MAX_DEPTH = 64
DEFAULT_MAX_FILE_BYTES = 10 * 1024 * 1024
DEFAULT_REQUEST_DELAY = 0.5
GITHUB_HOST_RE = re.compile(r"(^|\.)github\.com$", re.IGNORECASE)
MAIN_BRANCHES = {
    "HEAD",
    "main",
    "master",
    "refs/heads/main",
    "refs/heads/master",
}


def utc_now():
    return dt.datetime.now(dt.timezone.utc).isoformat()


def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    with temporary.open("w", encoding="utf-8") as handle:
        json.dump(value, handle, ensure_ascii=False, indent=2, sort_keys=True)
        handle.write("\n")
    os.replace(str(temporary), str(path))


def append_jsonl(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(value, ensure_ascii=False, sort_keys=True))
        handle.write("\n")


def iter_rows(path):
    """Yield dictionaries from JSONL, JSON, CSV, or TSV exports."""
    path = Path(path)
    opener = gzip.open if path.name.endswith(".gz") else open
    lower = path.name.lower()
    if lower.endswith((".jsonl", ".jsonl.gz")):
        with opener(str(path), "rt", encoding="utf-8") as handle:
            for line_number, line in enumerate(handle, 1):
                line = line.strip()
                if not line:
                    continue
                try:
                    row = json.loads(line)
                except ValueError as exc:
                    raise ValueError("invalid JSON at {}:{}".format(path, line_number)) from exc
                if not isinstance(row, dict):
                    raise ValueError("expected an object at {}:{}".format(path, line_number))
                yield row
        return
    if lower.endswith(".json"):
        with opener(str(path), "rt", encoding="utf-8") as handle:
            payload = json.load(handle)
        rows = payload if isinstance(payload, list) else payload.get("rows", [])
        if not isinstance(rows, list):
            raise ValueError("JSON input must be a list or an object with rows")
        for row in rows:
            if not isinstance(row, dict):
                raise ValueError("JSON input contains a non-object row")
            yield row
        return
    delimiter = "\t" if lower.endswith((".tsv", ".tsv.gz")) else ","
    with opener(str(path), "rt", encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle, delimiter=delimiter):
            yield dict(row)


def first(row, *names):
    for name in names:
        value = row.get(name)
        if value is not None and str(value) != "":
            return value
    return None


def text(value):
    return str(value).strip() if value is not None else ""


def int_or_none(value):
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def parse_date(value):
    value = text(value)
    if not value:
        return dt.datetime.min.replace(tzinfo=dt.timezone.utc)
    normalized = value.replace("Z", "+00:00")
    try:
        parsed = dt.datetime.fromisoformat(normalized)
    except ValueError:
        return dt.datetime.min.replace(tzinfo=dt.timezone.utc)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return parsed.astimezone(dt.timezone.utc)


def is_github_origin(origin):
    parsed = urllib.parse.urlparse(origin)
    return bool(GITHUB_HOST_RE.search(parsed.netloc or ""))


def normalize_candidate(row):
    origin = text(first(row, "origin_url", "origin", "url"))
    revision = text(first(row, "revision_id", "revision", "commit_id"))
    directory = text(first(row, "directory_id", "directory", "root_directory"))
    branch = text(first(row, "branch_name", "branch", "ref"))
    if not origin or not revision or not directory:
        return None
    return {
        "origin_url": origin.rstrip("/"),
        "revision_id": revision,
        "directory_id": directory,
        "branch_name": branch,
        "visit_date": text(first(row, "visit_date", "date", "observed_at", "timestamp")),
        "snapshot_id": text(first(row, "snapshot_id", "snapshot")),
        "github_default_branch": text(first(row, "github_default_branch", "default_branch")),
        "source_row": row,
    }


def candidate_rank(candidate, selection_policy="latest-main"):
    branch = candidate["branch_name"]
    default_branch = candidate["github_default_branch"]
    if selection_policy == "latest-main":
        main_rank = 0 if (
            branch in MAIN_BRANCHES
            or branch == default_branch
            or default_branch in MAIN_BRANCHES
        ) else 1
    else:
        main_rank = 0
    return (main_rank, -parse_date(candidate["visit_date"]).timestamp(), candidate["revision_id"])


def select_candidates(path, max_candidates=None, selection_policy="latest-main"):
    selected = {}
    all_candidates = []
    invalid = 0
    non_github = 0
    for row in iter_rows(path):
        candidate = normalize_candidate(row)
        if candidate is None:
            invalid += 1
            continue
        if not is_github_origin(candidate["origin_url"]):
            non_github += 1
            continue
        if selection_policy == "all":
            all_candidates.append(candidate)
            continue
        origin = candidate["origin_url"].lower()
        previous = selected.get(origin)
        if previous is None or candidate_rank(candidate, selection_policy) < candidate_rank(previous, selection_policy):
            selected[origin] = candidate
    if selection_policy == "all":
        unique = {}
        for candidate in all_candidates:
            key = (candidate["origin_url"].lower(), candidate["revision_id"], candidate["directory_id"])
            unique[key] = candidate
        values = list(unique.values())
    else:
        values = list(selected.values())
    values.sort(key=lambda item: (item["origin_url"].lower(), item["revision_id"]))
    if max_candidates is not None:
        values = values[:max_candidates]
    return values, {"invalid_rows": invalid, "non_github_rows": non_github}


def normalize_entry(row):
    directory_id = text(first(row, "directory_id", "dir_id", "directory"))
    name = text(first(row, "name", "path_component"))
    kind = text(first(row, "type", "entry_type")).lower()
    target = text(first(row, "target", "target_id", "sha1_git"))
    if not directory_id or not name:
        return None
    if kind in ("directory", "dir", "folder"):
        kind = "dir"
    elif kind in ("file", "blob", "content"):
        kind = "file"
    elif kind in ("rev", "revision", "release", "snapshot"):
        kind = "special"
    return {
        "directory_id": directory_id,
        "name": name,
        "type": kind or "file",
        "target": target,
        "length": int_or_none(first(row, "length", "size", "content_length")),
        "perms": text(first(row, "perms", "mode", "permission")),
        "checksums": row.get("checksums"),
    }


def build_directory_index(entries_path, db_path, rebuild=False):
    db_path.parent.mkdir(parents=True, exist_ok=True)
    if db_path.exists() and not rebuild:
        connection = sqlite3.connect(str(db_path))
        connection.execute("SELECT 1 FROM directory_entries LIMIT 1")
        return connection, {"reused": True, "rows": connection.execute("SELECT COUNT(*) FROM directory_entries").fetchone()[0]}
    if db_path.exists():
        db_path.unlink()
    connection = sqlite3.connect(str(db_path))
    connection.execute("PRAGMA journal_mode=DELETE")
    connection.execute("PRAGMA synchronous=NORMAL")
    connection.execute(
        "CREATE TABLE directory_entries ("
        "directory_id TEXT NOT NULL, name TEXT NOT NULL, type TEXT NOT NULL, "
        "target TEXT, length INTEGER, perms TEXT, checksums TEXT, "
        "PRIMARY KEY(directory_id, name))"
    )
    connection.execute("CREATE INDEX directory_entries_by_directory ON directory_entries(directory_id)")
    batch = []
    count = 0
    for row in iter_rows(entries_path):
        entry = normalize_entry(row)
        if entry is None:
            continue
        checksums = entry["checksums"]
        if not isinstance(checksums, str):
            checksums = json.dumps(checksums, sort_keys=True) if checksums else ""
        batch.append(
            (
                entry["directory_id"],
                entry["name"],
                entry["type"],
                entry["target"],
                entry["length"],
                entry["perms"],
                checksums,
            )
        )
        if len(batch) >= 5000:
            connection.executemany(
                "INSERT OR REPLACE INTO directory_entries "
                "(directory_id,name,type,target,length,perms,checksums) VALUES (?,?,?,?,?,?,?)",
                batch,
            )
            connection.commit()
            count += len(batch)
            batch = []
    if batch:
        connection.executemany(
            "INSERT OR REPLACE INTO directory_entries "
            "(directory_id,name,type,target,length,perms,checksums) VALUES (?,?,?,?,?,?,?)",
            batch,
        )
        connection.commit()
        count += len(batch)
    return connection, {"reused": False, "rows": count}


def safe_relative_path(path):
    path = path.replace("\\", "/")
    parts = [part for part in path.split("/") if part not in ("", ".")]
    if not parts or any(part == ".." for part in parts):
        return None
    return "/".join(parts)


class ContentFetcher(object):
    def __init__(self, args, output_root):
        self.api_url = args.api_url.rstrip("/")
        self.token = args.token
        self.delay = args.request_delay
        self.retries = args.retries
        self.max_bytes = args.max_file_bytes
        self.content_root = args.content_root
        self.proxy = args.proxy
        self.cache_root = output_root / "snapshots" / "content_cache"
        self.cache_root.mkdir(parents=True, exist_ok=True)
        self.user_agent = "bmv2-swh-p4-collector/1.0"
        if self.proxy:
            self.opener = urllib.request.build_opener(
                urllib.request.ProxyHandler({"http": self.proxy, "https": self.proxy})
            )
        else:
            self.opener = urllib.request.build_opener()

    def _local_path(self, content_id):
        if not self.content_root:
            return None
        root = Path(self.content_root)
        for suffix in ("", ".gz", ".bin", ".txt"):
            candidate = root / (content_id + suffix)
            if candidate.is_file():
                return candidate
        return None

    def _cache_path(self, content_id):
        return self.cache_root / content_id

    def fetch(self, content_id):
        content_id = text(content_id)
        if not content_id:
            return None, {"status": "missing_content_id"}
        cache_path = self._cache_path(content_id)
        local_path = self._local_path(content_id)
        source_path = local_path or (cache_path if cache_path.is_file() else None)
        if source_path:
            try:
                size = source_path.stat().st_size
                if size > self.max_bytes:
                    return None, {"status": "skipped_too_large", "bytes": size, "source": str(source_path)}
                if source_path.name.endswith(".gz"):
                    with gzip.open(str(source_path), "rb") as handle:
                        data = handle.read(self.max_bytes + 1)
                    if len(data) > self.max_bytes:
                        return None, {"status": "skipped_too_large", "bytes": len(data), "source": str(source_path)}
                    return data, {"status": "cached", "bytes": len(data), "source": str(source_path)}
                return source_path.read_bytes(), {"status": "cached", "bytes": size, "source": str(source_path)}
            except OSError as exc:
                return None, {"status": "read_failed", "error": str(exc)}

        url = "{}/content/sha1_git:{}/raw/".format(self.api_url, urllib.parse.quote(content_id, safe=""))
        last_error = None
        for attempt in range(self.retries + 1):
            if self.delay:
                time.sleep(self.delay)
            request = urllib.request.Request(url, headers={"Accept": "*/*", "User-Agent": self.user_agent})
            if self.token:
                request.add_header("Authorization", "Bearer " + self.token)
            try:
                with self.opener.open(request, timeout=60) as response:
                    data = response.read(self.max_bytes + 1)
                    if len(data) > self.max_bytes:
                        return None, {"status": "skipped_too_large", "bytes": len(data), "url": url}
                temporary = cache_path.with_name(cache_path.name + ".tmp")
                temporary.write_bytes(data)
                os.replace(str(temporary), str(cache_path))
                return data, {"status": "fetched", "bytes": len(data), "url": url}
            except urllib.error.HTTPError as exc:
                retry_after = exc.headers.get("Retry-After")
                last_error = "HTTP {}".format(exc.code)
                if exc.code not in (408, 425, 429, 500, 502, 503, 504):
                    break
                wait = float(retry_after) if retry_after and retry_after.isdigit() else min(60.0, 2 ** attempt)
            except (urllib.error.URLError, OSError) as exc:
                last_error = str(exc)
                wait = min(60.0, 2 ** attempt)
            if attempt < self.retries:
                time.sleep(wait)
        return None, {"status": "fetch_failed", "error": last_error or "unknown error", "url": url}


def content_id_from_entry(row):
    if row["target"]:
        return row["target"]
    checksums = row.get("checksums")
    if isinstance(checksums, dict):
        return text(checksums.get("sha1_git") or checksums.get("sha1"))
    if isinstance(checksums, str):
        try:
            parsed = json.loads(checksums)
            if isinstance(parsed, dict):
                return text(parsed.get("sha1_git") or parsed.get("sha1"))
        except ValueError:
            pass
    return ""


def walk_tree(connection, root_directory, max_depth):
    queue = deque([(root_directory, "", 0)])
    visited = set()
    entries = []
    while queue:
        directory_id, prefix, depth = queue.popleft()
        if directory_id in visited:
            continue
        visited.add(directory_id)
        if depth > max_depth:
            continue
        rows = connection.execute(
            "SELECT name,type,target,length,perms,checksums FROM directory_entries "
            "WHERE directory_id=? ORDER BY name",
            (directory_id,),
        )
        for name, kind, target, length, perms, checksums in rows:
            path = "{}/{}".format(prefix, name) if prefix else name
            path = safe_relative_path(path)
            if path is None:
                continue
            entry = {
                "path": path,
                "type": kind,
                "target": target or "",
                "length": length,
                "perms": perms or "",
                "directory_id": directory_id,
            }
            if checksums:
                try:
                    entry["checksums"] = json.loads(checksums)
                except ValueError:
                    entry["checksums"] = checksums
            entries.append(entry)
            if kind == "dir" and target and depth < max_depth:
                queue.append((target, path, depth + 1))
    return entries, len(visited)


def slugify_origin(origin):
    value = urllib.parse.urlparse(origin).path.strip("/") or origin
    value = value.replace("/", "__")
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", value)


def materialize_file(fetcher, output_path, content_id):
    data, status = fetcher.fetch(content_id)
    if data is None:
        return status
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = output_path.with_name(output_path.name + ".tmp")
    temporary.write_bytes(data)
    os.replace(str(temporary), str(output_path))
    status["sha256"] = hashlib.sha256(data).hexdigest()
    status["payload_path"] = str(output_path)
    return status


def fetch_into_record(fetcher, record):
    data, status = fetcher.fetch(record["content_id"])
    record["payload_status"] = status.get("status")
    record["payload_bytes"] = status.get("bytes")
    record["payload_source"] = status.get("source") or status.get("url")
    if data is None:
        record["payload_error"] = status.get("error")
        return None
    record["sha256"] = hashlib.sha256(data).hexdigest()
    return data


def snapshot_payload_path(output_root, origin, revision_id, path):
    return (
        output_root
        / "snapshots"
        / "swh_latest"
        / slugify_origin(origin)
        / revision_id
        / path
    )


def process_candidate(candidate, connection, fetcher, args, output_root, seen_directories):
    origin = candidate["origin_url"]
    directory_id = candidate["directory_id"]
    snapshot_id = "swh_latest__{}__{}".format(slugify_origin(origin), candidate["revision_id"])
    if directory_id in seen_directories:
        alias = dict(candidate)
        alias.update(
            {
                "directory_id": directory_id,
                "duplicate_of": seen_directories[directory_id],
                "alias_kind": "directory_id_duplicate",
            }
        )
        return {"kind": "alias", "alias": alias}

    entries, directory_count = walk_tree(connection, directory_id, args.max_depth)
    tree_path = output_root / "manifests" / "trees" / (snapshot_id + ".jsonl")
    p4_files = []
    tree_path.parent.mkdir(parents=True, exist_ok=True)
    if tree_path.exists():
        tree_path.unlink()
    for entry in entries:
        append_jsonl(tree_path, entry)
        if entry["type"] == "file" and entry["path"].lower().endswith(".p4"):
            p4_files.append(entry)

    file_records = []
    records_by_path = {}
    contains_p4 = bool(p4_files)
    contains_p4_16 = False
    p4_16_conflicted_count = 0
    for entry in entries:
        if entry["type"] != "file":
            continue
        content_id = content_id_from_entry(entry)
        record = dict(entry)
        record.update({"snapshot_id": snapshot_id, "content_id": content_id, "payload_status": "not_requested"})
        file_records.append(record)
        records_by_path[entry["path"]] = record

    # Phase 1 fetches only P4 candidates. This determines repository admission
    # before any non-P4 payload is requested or materialized.
    from smoke_swh_api import classify_p4_source

    for entry in p4_files:
        record = records_by_path[entry["path"]]
        data = fetch_into_record(fetcher, record)
        if data is None:
            record["p4_version_status"] = "unknown"
            continue
        classification = classify_p4_source(data.decode("utf-8", errors="replace"))
        record.update(classification)
        if classification["p4_version_status"] == "p4_16":
            contains_p4_16 = True
        elif classification["p4_version_status"] == "p4_16_conflicted":
            p4_16_conflicted_count += 1

    accepted = contains_p4_16 if args.repository_gate == "p4_16" else contains_p4

    # Phase 2 only runs for accepted repositories. Rejected P4_14/unknown
    # candidates retain metadata and classification evidence without causing a
    # full repository payload download.
    if accepted:
        for record in file_records:
            is_p4 = record["path"].lower().endswith(".p4")
            should_fetch = is_p4 or args.content_mode == "all"
            if should_fetch and record["payload_status"] == "not_requested":
                fetch_into_record(fetcher, record)
            if args.materialize and should_fetch and record.get("payload_status") not in (
                "missing_content_id",
                "read_failed",
                "fetch_failed",
                "skipped_too_large",
            ):
                materialized = materialize_file(
                    fetcher,
                    snapshot_payload_path(
                        output_root,
                        origin,
                        candidate["revision_id"],
                        record["path"],
                    ),
                    record["content_id"],
                )
                record["payload_status"] = materialized.get("status")
                record["payload_path"] = materialized.get("payload_path")

    seen_directories[directory_id] = snapshot_id
    repository = dict(candidate)
    repository.update(
        {
            "snapshot_id": snapshot_id,
            "status": "accepted" if accepted else "rejected",
            "accepted": accepted,
            "contains_p4": contains_p4,
            "contains_p4_16": contains_p4_16,
            "p4_16_conflicted_count": p4_16_conflicted_count,
            "p4_file_count": len(p4_files),
            "file_count": sum(1 for item in entries if item["type"] == "file"),
            "directory_count": directory_count,
            "tree_manifest_path": str(tree_path.relative_to(output_root)),
            "payload_policy": "P4-first gate; accepted files requested by content-mode={}".format(args.content_mode),
            "repository_gate": args.repository_gate,
            "repository_retention": (
                "keep_complete_repository_if_contains_{}".format(args.repository_gate)
                if accepted
                else "reject_from_repository_payload"
            ),
        }
    )
    return {
        "kind": "repository",
        "accepted": accepted,
        "repository": repository,
        "files": file_records,
    }


def build_parser():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidates", type=Path, help="prejoined SWH origin/revision candidates JSONL/CSV/TSV")
    parser.add_argument("--directory-entries", type=Path, help="SWH directory_entry export")
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--content-root", type=Path, help="optional local content mirror keyed by sha1_git")
    parser.add_argument("--api-url", default=DEFAULT_API_URL)
    parser.add_argument("--token", default=os.environ.get("SWH_API_TOKEN"))
    parser.add_argument("--proxy", default=os.environ.get("SWH_HTTP_PROXY"), help="HTTP(S) proxy URL")
    parser.add_argument("--max-candidates", type=int)
    parser.add_argument(
        "--selection-policy",
        choices=("latest-main", "latest-any", "all"),
        default="latest-main",
        help="latest-main follows the paper; latest-any/all require candidates for all branches",
    )
    parser.add_argument("--max-depth", type=int, default=DEFAULT_MAX_DEPTH)
    parser.add_argument("--max-file-bytes", type=int, default=DEFAULT_MAX_FILE_BYTES)
    parser.add_argument("--request-delay", type=float, default=DEFAULT_REQUEST_DELAY)
    parser.add_argument("--retries", type=int, default=5)
    parser.add_argument("--content-mode", choices=("p4", "all"), default="p4", help="fetch only P4 files or every file")
    parser.add_argument(
        "--repository-gate",
        choices=("p4_16", "p4"),
        default="p4_16",
        help="default keeps only repositories with at least one classified P4_16 file",
    )
    parser.add_argument("--materialize", action="store_true", help="write fetched files below snapshots/swh_latest")
    parser.add_argument("--rebuild-index", action="store_true")
    parser.add_argument("--dry-run", action="store_true", help="select candidates and index entries, but do not walk or fetch")
    return parser


def main(argv=None):
    args = build_parser().parse_args(argv)
    if not args.candidates or not args.directory_entries:
        raise SystemExit("--candidates and --directory-entries are required")
    if args.max_depth < 0 or args.max_file_bytes < 1 or args.request_delay < 0 or args.retries < 0:
        raise SystemExit("depth, file size, delay, and retries must be non-negative/positive")

    output_root = args.output_root
    output_root.mkdir(parents=True, exist_ok=True)
    candidates, selection_stats = select_candidates(
        args.candidates, args.max_candidates, args.selection_policy
    )
    discovery_root = output_root / "discovery"
    candidate_manifest = discovery_root / "swh_candidate_revisions.jsonl"
    candidate_manifest.parent.mkdir(parents=True, exist_ok=True)
    candidate_manifest.write_text("", encoding="utf-8")
    for candidate in candidates:
        append_jsonl(candidate_manifest, candidate)

    connection, index_stats = build_directory_index(
        args.directory_entries,
        discovery_root / "directory_entries.sqlite3",
        rebuild=args.rebuild_index,
    )
    summary = {
        "schema_version": 1,
        "started_at": utc_now(),
        "policy": {
            "source": "Software Heritage graph export",
            "selection": args.selection_policy,
            "directory_depth": args.max_depth,
            "repository_gate": args.repository_gate,
            "low_p4_ratio_kept": True,
            "deduplication_key": "directory_id",
        },
        "candidate_count": len(candidates),
        "selection": selection_stats,
        "directory_index": index_stats,
        "content_mode": args.content_mode,
        "materialize": args.materialize,
    }
    if args.dry_run:
        summary["status"] = "dry_run"
        summary["completed_at"] = utc_now()
        write_json(discovery_root / "discovery_summary.json", summary)
        connection.close()
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        return 0

    repositories_path = output_root / "manifests" / "repositories.jsonl"
    rejected_path = output_root / "manifests" / "rejected_repositories.jsonl"
    inspections_path = output_root / "manifests" / "repository_inspections.jsonl"
    files_path = output_root / "manifests" / "files.jsonl"
    aliases_path = output_root / "manifests" / "duplicate_aliases.jsonl"
    errors_path = output_root / "manifests" / "errors.jsonl"
    for path in (
        repositories_path,
        rejected_path,
        inspections_path,
        files_path,
        aliases_path,
        errors_path,
    ):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.touch(exist_ok=True)
    fetcher = ContentFetcher(args, output_root)
    seen_directories = {}
    completed_candidates = set()
    existing_repositories = []
    if repositories_path.exists():
        for line in repositories_path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            try:
                repository = json.loads(line)
            except ValueError:
                continue
            existing_repositories.append(repository)
            key = (repository.get("origin_url"), repository.get("revision_id"), repository.get("directory_id"))
            completed_candidates.add(key)
            if repository.get("directory_id") and repository.get("snapshot_id"):
                seen_directories[repository["directory_id"]] = repository["snapshot_id"]
    existing_rejected = []
    if rejected_path.exists():
        for line in rejected_path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            try:
                repository = json.loads(line)
            except ValueError:
                continue
            existing_rejected.append(repository)
            key = (repository.get("origin_url"), repository.get("revision_id"), repository.get("directory_id"))
            completed_candidates.add(key)
            if repository.get("directory_id") and repository.get("snapshot_id"):
                seen_directories[repository["directory_id"]] = repository["snapshot_id"]
    if inspections_path.exists():
        for line in inspections_path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            try:
                inspection = json.loads(line)
            except ValueError:
                continue
            completed_candidates.add(
                (inspection.get("origin_url"), inspection.get("revision_id"), inspection.get("directory_id"))
            )
            if inspection.get("directory_id") and inspection.get("snapshot_id"):
                seen_directories[inspection["directory_id"]] = inspection["snapshot_id"]
    existing_aliases = 0
    if aliases_path.exists():
        for line in aliases_path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            try:
                alias = json.loads(line)
            except ValueError:
                continue
            existing_aliases += 1
            completed_candidates.add(
                (alias.get("origin_url"), alias.get("revision_id"), alias.get("directory_id"))
            )
    counts = {
        "repositories": len(existing_repositories),
        "rejected_repositories": len(existing_rejected),
        "aliases": existing_aliases,
        "errors": 0,
        "p4_repositories": sum(
            int(bool(item.get("contains_p4")))
            for item in existing_repositories + existing_rejected
        ),
        "p4_16_repositories": sum(
            int(bool(item.get("contains_p4_16")))
            for item in existing_repositories + existing_rejected
        ),
        "resumed_skips": 0,
    }
    for index, candidate in enumerate(candidates, 1):
        candidate_key = (candidate.get("origin_url"), candidate.get("revision_id"), candidate.get("directory_id"))
        if candidate_key in completed_candidates:
            counts["resumed_skips"] += 1
            continue
        try:
            result = process_candidate(candidate, connection, fetcher, args, output_root, seen_directories)
            if result["kind"] == "alias":
                append_jsonl(aliases_path, result["alias"])
                completed_candidates.add(candidate_key)
                counts["aliases"] += 1
                continue
            repository = result["repository"]
            for record in result["files"]:
                append_jsonl(files_path, record)
            if result["accepted"]:
                append_jsonl(repositories_path, repository)
                counts["repositories"] += 1
            else:
                append_jsonl(rejected_path, repository)
                counts["rejected_repositories"] += 1
            # This row is the completion marker and is written after all file
            # and admission records so interrupted candidates are retried.
            append_jsonl(inspections_path, repository)
            completed_candidates.add(candidate_key)
            counts["p4_repositories"] += int(repository["contains_p4"])
            counts["p4_16_repositories"] += int(repository["contains_p4_16"])
        except Exception as exc:
            append_jsonl(
                errors_path,
                {"candidate": candidate, "error": str(exc), "error_type": type(exc).__name__, "recorded_at": utc_now()},
            )
            counts["errors"] += 1
        if index % 100 == 0 or index == len(candidates):
            print("processed {}/{} candidates".format(index, len(candidates)), flush=True)
    connection.close()
    summary.update(counts)
    summary["status"] = "completed"
    summary["completed_at"] = utc_now()
    write_json(discovery_root / "discovery_summary.json", summary)
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0 if counts["errors"] == 0 else 2


if __name__ == "__main__":
    sys.exit(main())
