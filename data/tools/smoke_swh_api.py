#!/usr/bin/env python3
"""End-to-end smoke for the SWH-first P4 repository discovery flow.

This is a small REST-API smoke, not the full SWH graph-dataset job. It
demonstrates the same decisions on a bounded sample:

1. discover candidate GitHub origins through Software Heritage origin search;
2. resolve origin -> latest visit -> snapshot -> revision -> root directory;
3. recursively inspect directory entries and find filenames ending in .p4;
4. fetch only those P4 file contents and classify P4_16/P4_14/unknown;
5. emit a materialization plan for repositories that contain at least one
   P4_16 file. The script does not download full repositories or call Vault.

For the real all-GitHub collection, use the SWH graph/export tables to perform
the global filename scan. The REST API is intentionally bounded by limits and
is suitable for smoke tests and spot checks only.
"""

from __future__ import annotations

import argparse
import collections
import datetime as dt
import json
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path


SWH_API = "https://archive.softwareheritage.org/api/1"
DEFAULT_SEARCH_PATTERN = "p4lang"
GITHUB_HOST_RE = re.compile(r"(^|\.)github\.com$", re.IGNORECASE)

P4_16_POSITIVE_PATTERNS = (
    re.compile(r"#\s*include\s*[<\"]core\.p4[>\"]"),
    re.compile(r"#\s*include\s*[<\"]v1model\.p4[>\"]"),
    re.compile(r"#\s*include\s*[<\"]psa\.p4[>\"]"),
    re.compile(r"#\s*include\s*[<\"]pna\.p4[>\"]"),
    re.compile(r"\bparser\b[^{;]*\bpacket_in\b"),
    re.compile(r"\bstruct\s+\w+\s*\{"),
    re.compile(r"\bpackage\s+\w+\s*\("),
    re.compile(r"\bV1Switch\s*\("),
    re.compile(r"\bPSA_Switch\s*\("),
)
P4_14_NEGATIVE_PATTERNS = (
    re.compile(r"\bheader_type\s+\w+\s*\{"),
    re.compile(r"\bfields\s*\{"),
    re.compile(r"\bparser\s+start\s*\{[^}]*\breturn\b", re.DOTALL),
    re.compile(r"\bcontrol\s+ingress\s*\{\s*apply\s*\(", re.DOTALL),
)


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--api-url", default=SWH_API)
    parser.add_argument(
        "--search-pattern",
        action="append",
        default=[],
        help=(
            "origin search pattern; default is {!r}. Can be supplied multiple "
            "times. REST search is only a bounded smoke, not global filename "
            "discovery.".format(DEFAULT_SEARCH_PATTERN)
        ),
    )
    parser.add_argument(
        "--origin",
        action="append",
        default=[],
        help="explicit origin URL to inspect; bypasses origin search when supplied",
    )
    parser.add_argument("--candidate-limit", type=int, default=20)
    parser.add_argument("--max-origins", type=int, default=5)
    parser.add_argument("--max-depth", type=int, default=8)
    parser.add_argument("--max-directories", type=int, default=120)
    parser.add_argument("--max-directory-entries", type=int, default=20000)
    parser.add_argument("--max-p4-files", type=int, default=20)
    parser.add_argument("--max-content-bytes", type=int, default=512 * 1024)
    parser.add_argument("--timeout", type=float, default=20.0)
    parser.add_argument(
        "--request-delay",
        type=float,
        default=0.0,
        help="sleep this many seconds before each SWH request; useful for anonymous API limits",
    )
    parser.add_argument(
        "--include-non-github",
        action="store_true",
        help="do not filter origin search results to github.com URLs",
    )
    parser.add_argument(
        "--stop-after-positive",
        action="store_true",
        help="stop after the first origin containing a P4_16 file",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="optional JSON output path; stdout is always written",
    )
    parser.add_argument(
        "--token",
        default=os.environ.get("SWH_API_TOKEN"),
        help="optional token; defaults to SWH_API_TOKEN",
    )
    args = parser.parse_args()
    for name in (
        "candidate_limit",
        "max_origins",
        "max_depth",
        "max_directories",
        "max_directory_entries",
        "max_p4_files",
        "max_content_bytes",
    ):
        if getattr(args, name) < 1:
            parser.error("--{} must be positive".format(name.replace("_", "-")))
    if args.timeout <= 0:
        parser.error("--timeout must be positive")
    if args.request_delay < 0:
        parser.error("--request-delay must be non-negative")
    if not args.search_pattern:
        args.search_pattern = [DEFAULT_SEARCH_PATTERN]
    return args


def api_join(api_url, suffix):
    return api_url.rstrip("/") + "/" + suffix.lstrip("/")


def request(url, timeout, token=None, accept="application/json"):
    delay = float(os.environ.get("SWH_REQUEST_DELAY", "0") or "0")
    if delay > 0:
        time.sleep(delay)
    headers = {
        "Accept": accept,
        "User-Agent": "bmv2-p4-swh-flow-smoke/0.2",
    }
    if token:
        headers["Authorization"] = "Bearer {}".format(token)
    request_obj = urllib.request.Request(url, headers=headers)
    try:
        response = urllib.request.urlopen(request_obj, timeout=timeout)
        return response
    except urllib.error.HTTPError as exc:
        snippet = exc.read(500).decode("utf-8", errors="replace").replace("\n", " ")
        raise RuntimeError("GET {} -> HTTP {}: {}".format(url, exc.code, snippet))
    except urllib.error.URLError as exc:
        raise RuntimeError("GET {} failed: {}".format(url, exc.reason))


def request_json(url, timeout, token=None):
    with request(url, timeout, token=token) as response:
        body = response.read()
    return json.loads(body.decode("utf-8"))


def request_raw_limited(url, timeout, limit, token=None):
    with request(url, timeout, token=token, accept="*/*") as response:
        data = response.read(limit + 1)
    truncated = len(data) > limit
    return data[:limit], truncated


def is_github_url(url):
    parsed = urllib.parse.urlparse(url)
    return bool(GITHUB_HOST_RE.search(parsed.netloc or ""))


def origin_search(api_url, pattern, limit, timeout, token):
    quoted_pattern = urllib.parse.quote(pattern.strip("/"))
    query = urllib.parse.urlencode({"limit": limit, "with_visit": "true"})
    url = api_join(api_url, "origin/search/{}/?{}".format(quoted_pattern, query))
    payload = request_json(url, timeout, token)
    if isinstance(payload, dict) and "results" in payload:
        return payload.get("results") or []
    if isinstance(payload, list):
        return payload
    raise RuntimeError("unexpected origin search payload: {}".format(type(payload).__name__))


def candidate_origins(args):
    seen = set()
    rows = []
    if args.origin:
        for origin in args.origin:
            normalized = origin.rstrip("/")
            if normalized not in seen:
                seen.add(normalized)
                rows.append({"url": normalized, "discovery_source": "explicit_origin"})
        return rows[: args.max_origins]

    for pattern in args.search_pattern:
        for row in origin_search(
            args.api_url, pattern, args.candidate_limit, args.timeout, args.token
        ):
            url = (row.get("url") or "").rstrip("/")
            if not url or url in seen:
                continue
            if not args.include_non_github and not is_github_url(url):
                continue
            seen.add(url)
            rows.append(
                {
                    "url": url,
                    "discovery_source": "origin_search",
                    "search_pattern": pattern,
                }
            )
            if len(rows) >= args.max_origins:
                return rows
    return rows


def origin_variants(origin):
    value = origin.rstrip("/")
    variants = [value]
    if not value.endswith(".git"):
        variants.append(value + ".git")
    return variants


def lookup_origin(api_url, origin, timeout, token):
    errors = []
    for candidate in origin_variants(origin):
        url = api_join(api_url, "origin/{}/get/".format(candidate))
        try:
            return candidate, request_json(url, timeout, token)
        except RuntimeError as exc:
            errors.append(str(exc))
    raise RuntimeError("origin lookup failed: " + " | ".join(errors))


def latest_visit(api_url, origin_url, timeout, token):
    url = api_join(
        api_url,
        "origin/{}/visit/latest/?require_snapshot=true".format(origin_url),
    )
    return request_json(url, timeout, token)


def resolve_branch_target(branches, name):
    visited = set()
    current_name = name
    while current_name and current_name not in visited:
        visited.add(current_name)
        branch = branches.get(current_name)
        if not branch:
            return None
        target_type = branch.get("target_type")
        target = branch.get("target")
        if target_type == "revision" and target:
            return current_name, target
        if target_type == "alias" and target:
            current_name = target
            continue
        return None
    return None


def resolve_revision_branch(snapshot):
    branches = snapshot.get("branches") or {}
    preferred = ("HEAD", "refs/heads/main", "refs/heads/master")
    for name in preferred:
        resolved = resolve_branch_target(branches, name)
        if resolved is None:
            continue
        resolved_name, revision_id = resolved
        if resolved_name == name:
            return name, revision_id
        return "{} -> {}".format(name, resolved_name), revision_id
    for name in sorted(branches):
        resolved = resolve_branch_target(branches, name)
        if resolved is not None:
            return resolved
    raise RuntimeError("snapshot has no revision branch")


def get_revision_and_root(api_url, revision_id, timeout, token):
    revision = request_json(api_join(api_url, "revision/{}/".format(revision_id)), timeout, token)
    directory_id = revision.get("directory")
    if not directory_id:
        raise RuntimeError("revision {} has no root directory".format(revision_id))
    return revision, directory_id


def directory_entries(api_url, directory_id, timeout, token):
    payload = request_json(api_join(api_url, "directory/{}/".format(directory_id)), timeout, token)
    if not isinstance(payload, list):
        raise RuntimeError("directory {} payload is not a list".format(directory_id))
    return payload


def entry_name(entry):
    return str(entry.get("name") or "")


def entry_target(entry):
    return entry.get("target") or (entry.get("checksums") or {}).get("sha1_git")


def walk_for_p4(api_url, root_directory, args):
    queue = collections.deque([(root_directory, "", 0)])
    visited_dirs = set()
    p4_files = []
    directory_count = 0
    entry_count = 0
    truncated_reasons = []

    while queue:
        directory_id, prefix, depth = queue.popleft()
        if directory_id in visited_dirs:
            continue
        visited_dirs.add(directory_id)
        directory_count += 1
        if directory_count > args.max_directories:
            truncated_reasons.append("max_directories")
            break
        if depth > args.max_depth:
            truncated_reasons.append("max_depth")
            continue

        entries = directory_entries(api_url, directory_id, args.timeout, args.token)
        entry_count += len(entries)
        if entry_count > args.max_directory_entries:
            truncated_reasons.append("max_directory_entries")
            break

        for entry in entries:
            name = entry_name(entry)
            if not name:
                continue
            path = "{}/{}".format(prefix, name) if prefix else name
            kind = entry.get("type")
            target = entry_target(entry)
            if kind == "dir" and target:
                queue.append((target, path, depth + 1))
            elif kind == "file" and name.lower().endswith(".p4"):
                p4_files.append(
                    {
                        "path": path,
                        "content_sha1_git": target,
                        "length": entry.get("length"),
                        "target_url": entry.get("target_url"),
                    }
                )
                if len(p4_files) >= args.max_p4_files:
                    truncated_reasons.append("max_p4_files")
                    return {
                        "p4_files": p4_files,
                        "directories_scanned": directory_count,
                        "entries_scanned": entry_count,
                        "truncated": True,
                        "truncated_reasons": sorted(set(truncated_reasons)),
                    }

    return {
        "p4_files": p4_files,
        "directories_scanned": directory_count,
        "entries_scanned": entry_count,
        "truncated": bool(truncated_reasons),
        "truncated_reasons": sorted(set(truncated_reasons)),
    }


def raw_content_url(api_url, p4_file):
    if p4_file.get("content_sha1_git"):
        return api_join(
            api_url,
            "content/sha1_git:{}/raw/".format(p4_file["content_sha1_git"]),
        )
    target_url = p4_file.get("target_url")
    if target_url:
        return target_url.rstrip("/") + "/raw/"
    return None


def strip_comments(source):
    source = re.sub(r"/\*.*?\*/", "", source, flags=re.DOTALL)
    source = re.sub(r"//.*", "", source)
    return source


def classify_p4_source(source):
    text = strip_comments(source)
    positives = [pattern.pattern for pattern in P4_16_POSITIVE_PATTERNS if pattern.search(text)]
    negatives = [pattern.pattern for pattern in P4_14_NEGATIVE_PATTERNS if pattern.search(text)]

    if positives and not negatives:
        status = "p4_16"
    elif negatives and not positives:
        status = "p4_14"
    elif positives and negatives:
        status = "p4_16_conflicted"
    else:
        status = "unknown"
    return {
        "p4_version_status": status,
        "positive_signal_count": len(positives),
        "negative_signal_count": len(negatives),
        "positive_signals": positives[:5],
        "negative_signals": negatives[:5],
    }


def inspect_p4_file(api_url, p4_file, args):
    url = raw_content_url(api_url, p4_file)
    result = dict(p4_file)
    if not url:
        result.update(
            {
                "content_fetch_status": "missing_content_id",
                "p4_version_status": "unknown",
            }
        )
        return result
    try:
        data, truncated = request_raw_limited(
            url, args.timeout, args.max_content_bytes, token=args.token
        )
        source = data.decode("utf-8", errors="replace")
        result.update(
            {
                "content_fetch_status": "fetched",
                "bytes_fetched": len(data),
                "content_truncated": truncated,
            }
        )
        result.update(classify_p4_source(source))
    except RuntimeError as exc:
        result.update(
            {
                "content_fetch_status": "failed",
                "content_fetch_error": str(exc),
                "p4_version_status": "unknown",
            }
        )
    return result


def process_origin(args, candidate):
    api_url = args.api_url.rstrip("/")
    origin_url = candidate["url"]
    result = {
        "origin": origin_url,
        "candidate": candidate,
        "status": "started",
        "contains_p4": False,
        "contains_p4_16": False,
    }
    canonical_origin, origin_payload = lookup_origin(
        api_url, origin_url, args.timeout, args.token
    )
    result["origin"] = origin_payload.get("url") or canonical_origin
    result["origin_visit_types"] = origin_payload.get("visit_types")

    visit = latest_visit(api_url, canonical_origin, args.timeout, args.token)
    snapshot_id = visit.get("snapshot")
    if not snapshot_id:
        raise RuntimeError("latest visit has no snapshot")
    snapshot = request_json(api_join(api_url, "snapshot/{}/".format(snapshot_id)), args.timeout, args.token)
    branch_name, revision_id = resolve_revision_branch(snapshot)
    revision, root_directory = get_revision_and_root(
        api_url, revision_id, args.timeout, args.token
    )

    walk = walk_for_p4(api_url, root_directory, args)
    inspected_files = [
        inspect_p4_file(api_url, p4_file, args) for p4_file in walk["p4_files"]
    ]
    p4_16_files = [
        row
        for row in inspected_files
        if row.get("p4_version_status") in ("p4_16", "p4_16_conflicted")
    ]

    result.update(
        {
            "status": "inspected",
            "latest_visit": {
                "date": visit.get("date"),
                "status": visit.get("status"),
                "type": visit.get("type"),
                "snapshot": snapshot_id,
            },
            "selected_branch": branch_name,
            "revision": {
                "id": revision_id,
                "swhid": "swh:1:rev:{}".format(revision_id),
                "date": revision.get("date"),
                "type": revision.get("type"),
                "directory": root_directory,
                "directory_swhid": "swh:1:dir:{}".format(root_directory),
            },
            "directory_walk": {
                "directories_scanned": walk["directories_scanned"],
                "entries_scanned": walk["entries_scanned"],
                "truncated": walk["truncated"],
                "truncated_reasons": walk["truncated_reasons"],
            },
            "contains_p4": bool(walk["p4_files"]),
            "contains_p4_16": bool(p4_16_files),
            "p4_file_count_seen": len(walk["p4_files"]),
            "p4_16_file_count_seen": len(p4_16_files),
            "p4_files": inspected_files,
        }
    )
    if p4_16_files:
        result["materialization_plan"] = {
            "action": "save_complete_repository_tree",
            "reason": "at_least_one_p4_16_file",
            "origin": result["origin"],
            "revision_swhid": result["revision"]["swhid"],
            "root_directory_swhid": result["revision"]["directory_swhid"],
            "tree_manifest_required": True,
            "code_payload_required": True,
            "p4_16_evidence_paths": [row["path"] for row in p4_16_files],
        }
    else:
        result["materialization_plan"] = {
            "action": "do_not_save_complete_repository_yet",
            "reason": "no_p4_16_file_found_in_bounded_rest_walk",
        }
    return result


def graph_dataset_plan():
    return {
        "why_needed": (
            "REST origin search is not global filename discovery. The real "
            "all-GitHub collection should use SWH graph/export tables."
        ),
        "batch_flow": [
            "filter origin URLs to GitHub origins",
            "join origin_visit_status to snapshots",
            "select default/main/master snapshot branches",
            "join snapshot_branch targets to revisions",
            "join revision.directory to directory_entry recursively",
            "filter full paths whose basename ends with .p4",
            "fetch only those content objects and classify P4_16",
            "materialize full repository tree for revisions with >=1 P4_16 file",
        ],
        "core_objects": [
            "origin",
            "origin_visit_status",
            "snapshot",
            "snapshot_branch",
            "revision",
            "directory",
            "directory_entry",
            "content",
        ],
    }


def main():
    args = parse_args()
    if args.request_delay:
        os.environ["SWH_REQUEST_DELAY"] = str(args.request_delay)
    started_at = dt.datetime.now(dt.timezone.utc).isoformat()
    candidates = candidate_origins(args)
    origin_results = []
    errors = []

    for candidate in candidates:
        try:
            result = process_origin(args, candidate)
            origin_results.append(result)
            if args.stop_after_positive and result.get("contains_p4_16"):
                break
        except RuntimeError as exc:
            errors.append({"origin": candidate.get("url"), "error": str(exc)})

    positives = [row for row in origin_results if row.get("contains_p4_16")]
    output = {
        "smoke_kind": "swh_rest_bounded_end_to_end",
        "started_at": started_at,
        "completed_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "api_url": args.api_url.rstrip("/"),
        "limits": {
            "candidate_limit": args.candidate_limit,
            "max_origins": args.max_origins,
            "max_depth": args.max_depth,
            "max_directories": args.max_directories,
            "max_directory_entries": args.max_directory_entries,
            "max_p4_files": args.max_p4_files,
            "max_content_bytes": args.max_content_bytes,
            "request_delay": args.request_delay,
        },
        "rest_smoke_limitations": [
            "origin/search finds origins by URL substring, not all filenames",
            "directory recursion is bounded by this smoke's limits",
            "full repository materialization is emitted as a plan only",
        ],
        "graph_dataset_full_flow": graph_dataset_plan(),
        "candidate_count": len(candidates),
        "origins_inspected": len(origin_results),
        "origins_with_p4": sum(1 for row in origin_results if row.get("contains_p4")),
        "origins_with_p4_16": len(positives),
        "materialization_plans": [
            row["materialization_plan"]
            for row in positives
            if row.get("materialization_plan")
        ],
        "origin_results": origin_results,
        "errors": errors,
    }

    rendered = json.dumps(output, ensure_ascii=False, indent=2, sort_keys=True)
    print(rendered)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered + "\n", encoding="utf-8")
    return 0 if origin_results or errors else 1


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except RuntimeError as exc:
        print("error: {}".format(exc), file=sys.stderr)
        raise SystemExit(1)
