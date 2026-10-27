#!/usr/bin/env python3
"""Discover SWH P4 candidates from remote ORC or Athena query results.

The script deliberately separates expensive global discovery from repository
collection.  Both input modes produce the same local manifests:

* ``local-s3`` scans public SWH ``directory_entry`` ORC objects with HTTP
  Range requests and keeps file entries whose basename ends in ``.p4``;
* ``athena-import`` normalizes downloaded Athena CSV/JSONL results and, when
  the rows contain origin/revision/root-directory fields, emits repository
  candidates accepted by ``collect_swh_p4.py``.

A P4 directory hit is not by itself a repository.  Mapping it to the latest
main-branch revision/origin is a separate graph join.  The importer therefore
never invents repository fields from a directory hit.
"""

from __future__ import print_function

import argparse
import csv
import gzip
import hashlib
import io
import json
import os
import sys
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from pathlib import Path


DEFAULT_BUCKET_URL = "https://softwareheritage.s3.amazonaws.com"
DEFAULT_ORC_PREFIX = "graph/2026-06-04/orc/directory_entry/"
DEFAULT_OUTPUT_ROOT = Path("data/raw/swh_p4/discovery")
S3_XML_NAMESPACE = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}


def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    with temporary.open("w", encoding="utf-8") as handle:
        json.dump(value, handle, ensure_ascii=False, indent=2, sort_keys=True)
        handle.write("\n")
    os.replace(str(temporary), str(path))


def write_jsonl(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    with temporary.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True))
            handle.write("\n")
    os.replace(str(temporary), str(path))


def iter_rows(path):
    path = Path(path)
    opener = gzip.open if path.name.endswith(".gz") else open
    lower = path.name.lower()
    if lower.endswith((".jsonl", ".jsonl.gz")):
        with opener(str(path), "rt", encoding="utf-8") as handle:
            for line_number, line in enumerate(handle, 1):
                line = line.strip()
                if not line:
                    continue
                row = json.loads(line)
                if not isinstance(row, dict):
                    raise ValueError("expected an object at {}:{}".format(path, line_number))
                yield row
        return
    if lower.endswith((".json", ".json.gz")):
        with opener(str(path), "rt", encoding="utf-8") as handle:
            value = json.load(handle)
        rows = value if isinstance(value, list) else value.get("rows", [])
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
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace").strip()
    return str(value).strip() if value is not None else ""


def normalize_kind(value):
    value = text(value).lower()
    if value in ("file", "blob", "content"):
        return "file"
    if value in ("dir", "directory", "folder"):
        return "dir"
    return value


def normalize_hit(row, source_kind):
    name = text(first(row, "name", "path", "file_path"))
    kind = normalize_kind(first(row, "type", "entry_type") or "file")
    directory_id = text(first(row, "directory_id", "containing_directory_id", "parent_directory_id"))
    content_id = text(first(row, "content_id", "target", "target_id", "sha1_git"))
    if not directory_id or not name or kind != "file" or not name.lower().endswith(".p4"):
        return None
    return {
        "directory_id": directory_id,
        "name": name,
        "type": "file",
        "content_id": content_id,
        "source_kind": source_kind,
    }


def normalize_repository_candidate(row, allow_directory_as_root=False):
    origin_url = text(first(row, "origin_url", "origin", "url")).rstrip("/")
    revision_id = text(first(row, "revision_id", "revision", "commit_id"))
    root_directory_id = text(
        first(row, "root_directory_id", "revision_directory_id", "root_directory")
    )
    if not root_directory_id and allow_directory_as_root:
        root_directory_id = text(first(row, "directory_id"))
    if not origin_url or not revision_id or not root_directory_id:
        return None
    if not is_github_origin(origin_url):
        return None
    return {
        "origin_url": origin_url,
        "revision_id": revision_id,
        "directory_id": root_directory_id,
        "branch_name": text(first(row, "branch_name", "branch", "ref")),
        "visit_date": text(first(row, "visit_date", "observed_at", "timestamp")),
        "snapshot_id": text(first(row, "snapshot_id", "snapshot")),
        "github_default_branch": text(first(row, "github_default_branch", "default_branch")),
        "discovery_source": "athena_import",
    }


def is_github_origin(origin_url):
    parsed = urllib.parse.urlparse(origin_url)
    return (parsed.hostname or "").lower() in ("github.com", "www.github.com")


def hit_key(row):
    return (row["directory_id"], row["name"], row["content_id"])


def candidate_key(row):
    return (row["origin_url"].lower(), row["revision_id"], row["directory_id"])


def build_opener(proxy):
    if proxy:
        return urllib.request.build_opener(
            urllib.request.ProxyHandler({"http": proxy, "https": proxy})
        )
    return urllib.request.build_opener(urllib.request.ProxyHandler({}))


class HttpRangeReader(object):
    """Seekable HTTP reader that refuses non-Range object responses."""

    def __init__(self, url, opener, timeout=60, max_range_bytes=64 * 1024 * 1024):
        self.url = url
        self.opener = opener
        self.timeout = timeout
        self.max_range_bytes = max_range_bytes
        request = urllib.request.Request(url, method="HEAD")
        with self.opener.open(request, timeout=timeout) as response:
            self.size = int(response.headers["Content-Length"])
        self.position = 0
        self.bytes_fetched = 0
        self.request_count = 0

    def readable(self):
        return True

    def seekable(self):
        return True

    def tell(self):
        return self.position

    def seek(self, offset, whence=io.SEEK_SET):
        if whence == io.SEEK_SET:
            position = offset
        elif whence == io.SEEK_CUR:
            position = self.position + offset
        elif whence == io.SEEK_END:
            position = self.size + offset
        else:
            raise ValueError("invalid whence: {}".format(whence))
        if position < 0:
            raise ValueError("negative seek position")
        self.position = position
        return self.position

    def read(self, count=-1):
        if self.position >= self.size:
            return b""
        if count is None or count < 0:
            count = self.size - self.position
        count = min(count, self.max_range_bytes)
        end = min(self.size - 1, self.position + count - 1)
        request = urllib.request.Request(
            self.url,
            headers={"Range": "bytes={}-{}".format(self.position, end)},
        )
        with self.opener.open(request, timeout=self.timeout) as response:
            status = getattr(response, "status", response.getcode())
            if status != 206:
                raise IOError("server ignored Range request for {} (HTTP {})".format(self.url, status))
            data = response.read()
        self.position += len(data)
        self.bytes_fetched += len(data)
        self.request_count += 1
        return data


def list_s3_objects(bucket_url, prefix, opener, timeout):
    continuation_token = None
    while True:
        parameters = {"list-type": "2", "prefix": prefix, "max-keys": "1000"}
        if continuation_token:
            parameters["continuation-token"] = continuation_token
        url = bucket_url.rstrip("/") + "/?" + urllib.parse.urlencode(parameters)
        with opener.open(url, timeout=timeout) as response:
            root = ET.fromstring(response.read())
        for item in root.findall("s3:Contents", S3_XML_NAMESPACE):
            key = item.findtext("s3:Key", namespaces=S3_XML_NAMESPACE)
            if not key or not key.endswith(".orc"):
                continue
            yield {
                "key": key,
                "size": int(item.findtext("s3:Size", "0", S3_XML_NAMESPACE)),
                "etag": text(item.findtext("s3:ETag", "", S3_XML_NAMESPACE)).strip('"'),
            }
        if root.findtext("s3:IsTruncated", "false", S3_XML_NAMESPACE) != "true":
            break
        continuation_token = root.findtext(
            "s3:NextContinuationToken", namespaces=S3_XML_NAMESPACE
        )


def iter_p4_hits(rows, source_kind, max_rows=None):
    row_count = 0
    hits = []
    for row in rows:
        row_count += 1
        if max_rows is not None and row_count > max_rows:
            return hits, row_count - 1, False
        if isinstance(row, dict):
            candidate = row
        else:
            directory_id, name, kind, target = row
            candidate = {
                "directory_id": directory_id,
                "name": name,
                "type": kind,
                "target": target,
            }
        hit = normalize_hit(candidate, source_kind)
        if hit is not None:
            hits.append(hit)
    return hits, row_count, True


def load_state(path):
    if not path.is_file():
        return {"schema_version": 1, "objects": {}}
    with path.open("r", encoding="utf-8") as handle:
        value = json.load(handle)
    if not isinstance(value.get("objects"), dict):
        raise ValueError("invalid scan state: {}".format(path))
    return value


def merge_hit_shards(shard_root, output_path):
    unique = {}
    if shard_root.is_dir():
        for shard in sorted(shard_root.glob("*.jsonl")):
            for row in iter_rows(shard):
                unique[hit_key(row)] = row
    rows = [unique[key] for key in sorted(unique)]
    write_jsonl(output_path, rows)
    return len(rows)


def scan_s3(args):
    if args.max_objects is None and not args.confirm_full_scan:
        raise SystemExit(
            "refusing an unbounded 27 TiB directory_entry scan; use --max-objects for a "
            "smoke run or --confirm-full-scan after reviewing cost and runtime"
        )
    try:
        import pyorc
    except ImportError:
        raise SystemExit(
            "local-s3 requires pyorc; install data/tools/requirements-swh-s3.txt"
        )

    opener = build_opener(args.proxy)
    output_root = args.output_root
    shard_root = output_root / "p4_directory_hit_shards"
    state_path = output_root / "local_s3_scan_state.json"
    state = load_state(state_path)
    objects = list(
        list_s3_objects(args.bucket_url, args.prefix, opener, args.timeout)
    )
    if args.max_objects is not None:
        objects = objects[: args.max_objects]

    processed = 0
    skipped = 0
    partial = 0
    for item in objects:
        previous = state["objects"].get(item["key"])
        if (
            previous
            and previous.get("status") == "complete"
            and previous.get("etag") == item["etag"]
            and previous.get("size") == item["size"]
        ):
            skipped += 1
            continue
        object_url = args.bucket_url.rstrip("/") + "/" + urllib.parse.quote(
            item["key"], safe="/"
        )
        source = HttpRangeReader(
            object_url,
            opener,
            timeout=args.timeout,
            max_range_bytes=args.max_range_bytes,
        )
        reader = pyorc.Reader(
            source,
            column_names=["directory_id", "name", "type", "target"],
        )
        hits, row_count, complete = iter_p4_hits(
            reader,
            "swh_s3_orc",
            max_rows=args.max_rows_per_object,
        )
        for hit in hits:
            hit["s3_object_key"] = item["key"]
        shard_name = hashlib.sha256(item["key"].encode("utf-8")).hexdigest() + ".jsonl"
        write_jsonl(shard_root / shard_name, hits)
        state["objects"][item["key"]] = {
            "status": "complete" if complete else "partial",
            "etag": item["etag"],
            "size": item["size"],
            "rows_scanned": row_count,
            "hits": len(hits),
            "http_requests": source.request_count,
            "bytes_fetched": source.bytes_fetched,
            "shard": shard_name,
        }
        write_json(state_path, state)
        processed += 1
        partial += int(not complete)
        print(
            "scanned {} rows={} hits={} bytes_fetched={} status={}".format(
                item["key"], row_count, len(hits), source.bytes_fetched,
                "complete" if complete else "partial",
            ),
            flush=True,
        )

    hit_count = merge_hit_shards(shard_root, output_root / "p4_directory_hits.jsonl")
    summary = {
        "mode": "local-s3",
        "objects_selected": len(objects),
        "objects_processed": processed,
        "objects_resumed": skipped,
        "partial_objects": partial,
        "unique_p4_hits": hit_count,
        "repository_mapping_complete": False,
        "next_step": "map hit directories to latest main revisions/origins before collection",
    }
    write_json(output_root / "local_s3_scan_summary.json", summary)
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


def import_athena(args):
    hits = {}
    candidates = {}
    total_rows = 0
    rows_without_hit = 0
    rows_without_repository = 0
    github_repository_candidates = 0
    p4_hit_rows_with_repository = 0
    p4_hit_rows_without_repository = 0
    for row in iter_rows(args.input):
        total_rows += 1
        hit = normalize_hit(row, "athena_export")
        if hit is None:
            rows_without_hit += 1
        else:
            hits[hit_key(hit)] = hit

        allow_directory_as_root = hit is None
        candidate = normalize_repository_candidate(
            row,
            allow_directory_as_root=allow_directory_as_root,
        )
        if candidate is None:
            rows_without_repository += 1
            if hit is not None:
                p4_hit_rows_without_repository += 1
        else:
            candidates[candidate_key(candidate)] = candidate
            github_repository_candidates += 1
            if hit is not None:
                p4_hit_rows_with_repository += 1

    hit_rows = [hits[key] for key in sorted(hits)]
    candidate_rows = [candidates[key] for key in sorted(candidates)]
    args.output_root.mkdir(parents=True, exist_ok=True)
    write_jsonl(args.output_root / "p4_directory_hits.jsonl", hit_rows)
    write_jsonl(args.output_root / "swh_candidate_revisions.jsonl", candidate_rows)
    if hit_rows:
        mapping_complete = p4_hit_rows_without_repository == 0
    else:
        mapping_complete = bool(candidate_rows)
    directory_entry_closure_present = bool(
        args.directory_entries and args.directory_entries.is_file()
    )
    summary = {
        "mode": "athena-import",
        "input": str(args.input),
        "input_rows": total_rows,
        "unique_p4_hits": len(hit_rows),
        "unique_repository_candidates": len(candidate_rows),
        "rows_without_p4_hit_fields": rows_without_hit,
        "rows_without_repository_mapping_fields": rows_without_repository,
        "github_repository_rows": github_repository_candidates,
        "p4_hit_rows_with_repository_mapping": p4_hit_rows_with_repository,
        "p4_hit_rows_without_repository_mapping": p4_hit_rows_without_repository,
        "repository_mapping_complete": mapping_complete,
        "candidate_manifest_ready": mapping_complete,
        "directory_entry_closure_present": directory_entry_closure_present,
        "collector_ready": mapping_complete and directory_entry_closure_present,
        "required_repository_fields": [
            "origin_url",
            "revision_id",
            "root_directory_id",
        ],
    }
    if not mapping_complete:
        summary["next_step"] = (
            "join P4 hit directories to latest main revision/origin; a hit-only Athena "
            "export is not yet a repository list"
        )
    elif not directory_entry_closure_present:
        summary["next_step"] = (
            "export the complete directory_entry closure for the candidate root directories "
            "or use a separate GitHub/SWH snapshot materializer"
        )
    else:
        summary["directory_entries"] = str(args.directory_entries)
    write_json(args.output_root / "athena_import_summary.json", summary)
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    if args.require_mapped_repositories and not mapping_complete:
        return 2
    return 0


def build_parser():
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    local = subparsers.add_parser("local-s3", help="scan public SWH ORC with HTTP Range reads")
    local.add_argument("--bucket-url", default=DEFAULT_BUCKET_URL)
    local.add_argument("--prefix", default=DEFAULT_ORC_PREFIX)
    local.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    local.add_argument("--proxy", help="HTTP proxy URL, for example http://127.0.0.1:7892")
    local.add_argument("--timeout", type=int, default=60)
    local.add_argument("--max-range-bytes", type=int, default=64 * 1024 * 1024)
    local.add_argument("--max-objects", type=int, help="limit ORC objects for a smoke run")
    local.add_argument("--max-rows-per-object", type=int, help="limit rows; object remains partial")
    local.add_argument(
        "--confirm-full-scan",
        action="store_true",
        help="acknowledge that directory_entry is about 27 TiB before an unbounded scan",
    )

    athena = subparsers.add_parser("athena-import", help="normalize downloaded Athena results")
    athena.add_argument("--input", type=Path, required=True)
    athena.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    athena.add_argument(
        "--directory-entries",
        type=Path,
        help="candidate root directory_entry closure used by collect_swh_p4.py",
    )
    athena.add_argument(
        "--require-mapped-repositories",
        action="store_true",
        help="fail unless origin/revision/root-directory fields are present",
    )
    return parser


def main(argv=None):
    args = build_parser().parse_args(argv)
    if args.command == "local-s3":
        return scan_s3(args)
    if args.command == "athena-import":
        return import_athena(args)
    raise AssertionError("unknown command")


if __name__ == "__main__":
    sys.exit(main())
