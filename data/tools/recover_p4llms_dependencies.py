#!/usr/bin/env python3
"""Recover missing p4llms include files from their original GitHub projects.

The p4llms export retains P4 sources but omits many non-``.p4`` headers and
some project files.  This tool reads compiler errors, derives the original
GitHub repository/path, and stores successful downloads in a separate
repository-shaped overlay.  It never edits the HF snapshot or normalized
records.
"""

import argparse
import datetime as dt
import hashlib
import json
import os
import posixpath
import re
import sys
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path, PurePosixPath


DATA_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SOURCE = DATA_ROOT / "normalized" / "hf_p4llms" / "hf_p4llms_all_records.jsonl"
DEFAULT_RESULTS = (
    DATA_ROOT / "normalized" / "hf_p4llms" / "hf_p4llms_bmv2_compile_results.jsonl"
)
DEFAULT_OUTPUT = DATA_ROOT / "raw" / "hf" / "p4dataset_supplemental"
ALLP4_PREFIX = "allp4files/"


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, default=DEFAULT_SOURCE)
    parser.add_argument("--results", type=Path, default=DEFAULT_RESULTS)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--timeout", type=float, default=20.0)
    parser.add_argument("--workers", type=int, default=12)
    parser.add_argument("--limit", type=int, default=0, help="0 means all missing includes")
    parser.add_argument(
        "--proxy",
        default=None,
        help="optional HTTP(S) proxy, for example http://127.0.0.1:7890",
    )
    return parser.parse_args()


def read_jsonl(path):
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                yield json.loads(line)


def read_stderr(result, results_root):
    relative = result.get("stderr_file")
    if relative:
        path = results_root / relative
        if path.is_file():
            return path.read_text(encoding="utf-8", errors="replace")
    return result.get("stderr_excerpt") or ""


def missing_includes(stderr):
    """Return ``(including_file, include_name)`` pairs from GCC-style errors."""
    pattern = re.compile(
        r"(?m)^(?P<source>.+?):\d+(?::\d+)?: fatal error:\s*"
        r"(?P<include>[^:\n]+): No such file or directory"
    )
    return [(match.group("source"), match.group("include").strip()) for match in pattern.finditer(stderr)]


def normalized_relative(path):
    normalized = posixpath.normpath(str(path).replace("\\", "/"))
    if normalized in ("", ".") or normalized == ".." or normalized.startswith("../"):
        return None
    return PurePosixPath(normalized.lstrip("/"))


def repository_mapping(record):
    """Return ``(owner, repository, path prefix count)`` for a normalized row."""
    source_repo = record.get("source_repo") or ""
    source_path = PurePosixPath(record.get("source_path") or "")
    if source_repo.startswith(ALLP4_PREFIX):
        owner = source_repo[len(ALLP4_PREFIX) :]
        if not source_path.parts:
            return None
        return owner, source_path.parts[0], 1
    parts = source_repo.split("/", 1)
    if len(parts) != 2 or not all(parts):
        return None
    return parts[0], parts[1], 0


def including_source_path(record, compiler_path):
    """Map a temporary compiler path back to the HF repository namespace."""
    compiler_text = compiler_path.replace("\\", "/")
    marker = "/{}/".format(record["source_repo_id"])
    if marker in compiler_text:
        return normalized_relative(compiler_text.split(marker, 1)[1])
    return normalized_relative(record.get("source_path") or "")


def dependency_request(record, compiler_path, include_name):
    mapping = repository_mapping(record)
    including_path = including_source_path(record, compiler_path)
    if mapping is None or including_path is None:
        return None
    owner, repository, strip_parts = mapping
    include_text = include_name.strip('"<>').replace("\\", "/")
    if not include_text or include_text.startswith("/"):
        return None
    project_root_parts = including_path.parts[:strip_parts]
    search_dirs = []
    current = including_path.parent
    while len(current.parts) >= strip_parts:
        search_dirs.append(current)
        if len(current.parts) == strip_parts:
            break
        current = current.parent
    candidates = []
    seen = set()
    for directory in search_dirs:
        target = normalized_relative(directory / PurePosixPath(include_text))
        if target is None or len(target.parts) <= strip_parts:
            continue
        if project_root_parts and target.parts[:strip_parts] != project_root_parts:
            continue
        github_path = PurePosixPath(*target.parts[strip_parts:])
        quoted_path = "/".join(urllib.parse.quote(part, safe="") for part in github_path.parts)
        url = "https://raw.githubusercontent.com/{}/{}/HEAD/{}".format(
            urllib.parse.quote(owner, safe=""),
            urllib.parse.quote(repository, safe=""),
            quoted_path,
        )
        if url in seen:
            continue
        seen.add(url)
        candidates.append(
            {"github_path": str(github_path), "overlay_path": str(target), "url": url}
        )
    if not candidates:
        return None
    return {
        "owner": owner,
        "repository": repository,
        "source_repo_id": record["source_repo_id"],
        "including_path": str(including_path),
        "include": include_text,
        "candidates": candidates,
    }


def build_opener(proxy):
    if not proxy:
        return urllib.request.build_opener()
    return urllib.request.build_opener(
        urllib.request.ProxyHandler({"http": proxy, "https": proxy})
    )


def fetch_dependency(request, output_root, proxy, timeout):
    row = dict(request)
    row.pop("candidates", None)
    row["retrieved_at"] = dt.datetime.now(dt.timezone.utc).isoformat()
    for candidate in request["candidates"]:
        target = output_root / request["source_repo_id"] / candidate["overlay_path"]
        if target.is_file():
            content = target.read_bytes()
            row.update(
                {
                    "status": "existing",
                    "github_path": candidate["github_path"],
                    "overlay_path": candidate["overlay_path"],
                    "url": candidate["url"],
                    "local_path": str(target.relative_to(output_root)),
                    "size_bytes": len(content),
                    "sha256": hashlib.sha256(content).hexdigest(),
                }
            )
            return row

    attempted = []
    opener = build_opener(proxy)
    for candidate in request["candidates"]:
        target = output_root / request["source_repo_id"] / candidate["overlay_path"]
        try:
            http_request = urllib.request.Request(
                candidate["url"], headers={"User-Agent": "bmv2-dataset-recovery/1.0"}
            )
            with opener.open(http_request, timeout=timeout) as response:
                content = response.read()
                final_url = response.geturl()
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(content)
            row.update(
                {
                    "status": "downloaded",
                    "github_path": candidate["github_path"],
                    "overlay_path": candidate["overlay_path"],
                    "url": candidate["url"],
                    "final_url": final_url,
                    "local_path": str(target.relative_to(output_root)),
                    "size_bytes": len(content),
                    "sha256": hashlib.sha256(content).hexdigest(),
                    "attempted_urls": attempted + [candidate["url"]],
                }
            )
            return row
        except (OSError, urllib.error.URLError, urllib.error.HTTPError) as exc:
            attempted.append(candidate["url"])
            row["error"] = str(exc)
    row.update({"status": "failed", "attempted_urls": attempted})
    return row


def main():
    args = parse_args()
    if args.timeout <= 0 or args.limit < 0 or args.workers < 1:
        raise SystemExit("timeout/workers must be positive and limit must not be negative")
    if not args.source.is_file() or not args.results.is_file():
        raise SystemExit("source and results JSONL files must exist")
    source_records = {row["record_id"]: row for row in read_jsonl(args.source)}
    requests = {}
    unresolvable = []
    for result in read_jsonl(args.results):
        if result.get("status") == "compiled":
            continue
        record = source_records.get(result.get("record_id"))
        if not record:
            continue
        stderr = read_stderr(result, args.results.parent)
        for compiler_path, include_name in missing_includes(stderr):
            request = dependency_request(record, compiler_path, include_name)
            if request is None:
                unresolvable.append(
                    {
                        "record_id": record["record_id"],
                        "include": include_name,
                        "reason": "cannot_map_repository_or_path",
                    }
                )
                continue
            request["record_ids"] = [record["record_id"]]
            key = (
                request["source_repo_id"],
                request["including_path"],
                request["include"],
            )
            if key in requests:
                requests[key]["record_ids"].append(record["record_id"])
            else:
                requests[key] = request
    work = list(requests.values())
    if args.limit:
        work = work[: args.limit]

    output_root = args.output_root.resolve()
    output_root.mkdir(parents=True, exist_ok=True)
    manifest_rows = []
    counts = {"requested": len(work), "downloaded": 0, "existing": 0, "failed": 0}
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = [
            executor.submit(
                fetch_dependency, request, output_root, args.proxy, args.timeout
            )
            for request in work
        ]
        for index, future in enumerate(as_completed(futures), 1):
            row = future.result()
            counts[row["status"]] += 1
            manifest_rows.append(row)
            if index % 25 == 0 or index == len(work):
                print(
                    "[{}/{}] downloaded={} existing={} failed={}".format(
                        index, len(work), counts["downloaded"], counts["existing"], counts["failed"]
                    ),
                    flush=True,
                )

    manifest_rows.sort(
        key=lambda row: (
            row.get("source_repo_id", ""),
            row.get("including_path", ""),
            row.get("include", ""),
        )
    )
    manifest_path = output_root / "download_manifest.jsonl"
    previous_rows = []
    if manifest_path.is_file():
        previous_rows = list(read_jsonl(manifest_path))
    manifest_by_key = {}
    for row in previous_rows + manifest_rows + unresolvable:
        key = (
            row.get("source_repo_id", ""),
            row.get("including_path", ""),
            row.get("include", row.get("overlay_path", "")),
        )
        manifest_by_key[key] = row
    merged_manifest = sorted(
        manifest_by_key.values(),
        key=lambda row: (
            row.get("source_repo_id", ""),
            row.get("including_path", ""),
            row.get("include", row.get("overlay_path", "")),
        ),
    )
    with manifest_path.open("w", encoding="utf-8") as handle:
        for row in merged_manifest:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")
    summary = {
        "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "source": str(args.source.resolve()),
        "results": str(args.results.resolve()),
        "output_root": str(output_root),
        "proxy": args.proxy,
        "unique_dependency_count": len(requests),
        "selected_dependency_count": len(work),
        "manifest_entry_count": len(merged_manifest),
        "unresolvable_count": len(unresolvable),
        "by_status": counts,
    }
    (output_root / "download_summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0 if counts["failed"] == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
