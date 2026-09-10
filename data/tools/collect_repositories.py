#!/usr/bin/env python3
"""Download reproducible raw snapshots of the P4 repositories in the catalog.

The checked-out repository directories under data/raw/repositories are the
original source snapshots.  Each successful snapshot has collection.json with
the URL, branch, commit, proxy mode and timestamp.  Re-running this script is
safe: completed snapshots are skipped and failed repositories are recorded.
"""

import argparse
import datetime as dt
import json
import os
import re
import shutil
import subprocess
import sys
import tarfile
import time
import urllib.parse
from pathlib import Path


DATA_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CATALOG = DATA_ROOT / "config" / "repositories.json"
DEFAULT_RAW_ROOT = DATA_ROOT / "raw" / "repositories"
DEFAULT_PROXY = "http://127.0.0.1:7890"


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog", type=Path, default=DEFAULT_CATALOG)
    parser.add_argument("--raw-root", type=Path, default=DEFAULT_RAW_ROOT)
    parser.add_argument(
        "--repo",
        action="append",
        dest="repo_ids",
        help="collect only this catalog id; may be repeated",
    )
    parser.add_argument(
        "--proxy",
        default=os.environ.get("GITHUB_HTTP_PROXY", DEFAULT_PROXY),
        help="HTTP proxy for git/curl (default: %(default)s)",
    )
    parser.add_argument(
        "--direct",
        action="store_true",
        help="disable proxy use",
    )
    parser.add_argument("--timeout", type=float, default=900.0)
    parser.add_argument("--retries", type=int, default=2)
    parser.add_argument(
        "--method",
        choices=("auto", "git", "codeload"),
        default="auto",
        help="download method; auto tries git then codeload",
    )
    parser.add_argument(
        "--no-codeload-fallback",
        action="store_true",
        help="do not fall back to a GitHub codeload tarball when git clone fails",
    )
    parser.add_argument("--force", action="store_true", help="refresh existing snapshots")
    args = parser.parse_args()
    if args.timeout <= 0 or args.retries < 1:
        parser.error("timeout must be positive and retries must be at least one")
    return args


def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def read_json(path):
    return json.loads(path.read_text(encoding="utf-8"))


def run_command(command, cwd=None, proxy=None, timeout=900.0):
    env = os.environ.copy()
    if proxy:
        env["http_proxy"] = proxy
        env["https_proxy"] = proxy
        env["HTTP_PROXY"] = proxy
        env["HTTPS_PROXY"] = proxy
    else:
        for key in ("http_proxy", "https_proxy", "HTTP_PROXY", "HTTPS_PROXY"):
            env.pop(key, None)
    return subprocess.run(
        command,
        cwd=str(cwd) if cwd else None,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        timeout=timeout,
        check=False,
    )


def discover_branch(url, proxy, timeout):
    result = run_command(
        ["git", "ls-remote", "--symref", url, "HEAD"],
        proxy=proxy,
        timeout=min(timeout, 60.0),
    )
    if result.returncode != 0:
        raise RuntimeError("git ls-remote failed: {}".format(result.stderr.strip()[-1000:]))
    match = re.search(r"ref: refs/heads/([^\s]+)\s+HEAD", result.stdout)
    return match.group(1) if match else "main"


def git_value(repo_dir, *arguments):
    result = subprocess.run(
        ["git", "-C", str(repo_dir)] + list(arguments),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip()[-1000:])
    return result.stdout.strip()


def safe_extract_tarball(archive_path, destination):
    with tarfile.open(str(archive_path), "r:gz") as archive:
        members = archive.getmembers()
        destination_resolved = destination.resolve()
        for member in members:
            target = (destination / member.name).resolve()
            if target != destination_resolved and destination_resolved not in target.parents:
                raise RuntimeError("tarball contains an unsafe path: {}".format(member.name))
        archive.extractall(str(destination))
    top_levels = sorted(
        item for item in destination.iterdir() if item.name not in {"."} and item.is_dir()
    )
    if len(top_levels) != 1:
        raise RuntimeError("unexpected codeload archive layout")
    source_root = top_levels[0]
    for item in source_root.iterdir():
        item.rename(destination / item.name)
    source_root.rmdir()


def codeload_snapshot(spec, branch, destination, proxy, timeout, retries):
    repository = spec["repository"]
    archive_url = "https://codeload.github.com/{}/tar.gz/refs/heads/{}".format(
        repository, urllib.parse.quote(branch, safe="")
    )
    archive_root = destination.parent.parent / "archives"
    archive_root.mkdir(parents=True, exist_ok=True)
    archive_path = archive_root / (spec["id"] + "_" + re.sub(r"[^A-Za-z0-9_.-]", "_", branch) + ".tar.gz")
    command = [
        "curl",
        "--fail",
        "--location",
        "--silent",
        "--show-error",
        "--retry",
        "2",
        "--connect-timeout",
        "20",
        "--max-time",
        str(timeout),
        "--output",
        str(archive_path),
        archive_url,
    ]
    last_error = ""
    for attempt in range(1, retries + 1):
        try:
            result = run_command(command, proxy=proxy, timeout=timeout + 30.0)
        except subprocess.TimeoutExpired:
            result = None
            last_error = "codeload request timed out"
        else:
            last_error = (result.stderr or result.stdout).strip()[-2000:]
        if result is not None and result.returncode == 0 and archive_path.is_file():
            break
        if attempt < retries:
            time.sleep(min(10.0 * attempt, 30.0))
    else:
        raise RuntimeError(last_error or "codeload download failed")

    destination.mkdir(parents=True, exist_ok=True)
    safe_extract_tarball(archive_path, destination)
    return archive_url, archive_path


def collect_one(spec, args):
    repo_id = spec["id"]
    destination = args.raw_root / repo_id
    marker = destination / "collection.json"
    proxy = None if args.direct else (args.proxy.strip() or None)
    started = dt.datetime.now(dt.timezone.utc).isoformat()
    result = {
        "id": repo_id,
        "repository": spec["repository"],
        "url": spec["url"],
        "role": spec.get("role", ""),
        "families": spec.get("families", []),
        "status": "failed",
        "started_at": started,
        "proxy": proxy,
    }

    if marker.exists() and (destination / ".git").is_dir() and not args.force:
        try:
            metadata = read_json(marker)
            result.update(metadata)
            result["status"] = "skipped_existing"
            return result
        except (OSError, ValueError, TypeError):
            pass

    try:
        branch = discover_branch(spec["url"], proxy, args.timeout)
    except Exception as exc:
        result["error"] = str(exc)
        return result

    partial = destination.with_name(destination.name + ".partial")
    if partial.exists():
        shutil.rmtree(partial)
    destination.parent.mkdir(parents=True, exist_ok=True)

    command = [
        "git",
        "clone",
        "--depth",
        "1",
        "--no-tags",
        "--single-branch",
        "--branch",
        branch,
        spec["url"],
        str(partial),
    ]
    sparse_paths = spec.get("sparse_paths", [])
    if sparse_paths:
        command[2:2] = ["--filter=blob:none", "--sparse"]
    last_error = ""
    git_succeeded = False
    if args.method != "codeload":
        for attempt in range(1, args.retries + 1):
            try:
                clone_result = run_command(command, proxy=proxy, timeout=args.timeout)
            except subprocess.TimeoutExpired:
                clone_result = None
                last_error = "git clone timed out after {} seconds".format(args.timeout)
            else:
                last_error = (clone_result.stderr or clone_result.stdout).strip()[-2000:]
            if clone_result is not None and clone_result.returncode == 0:
                git_succeeded = True
                break
            if attempt < args.retries:
                time.sleep(min(10.0 * attempt, 30.0))
    if not git_succeeded:
        if partial.exists():
            shutil.rmtree(partial)
        if args.method == "git" or args.no_codeload_fallback:
            result["error"] = last_error or "git clone failed"
            return result
        try:
            archive_url, archive_path = codeload_snapshot(
                spec, branch, partial, proxy, args.timeout, args.retries
            )
            metadata = {
                **spec,
                "status": "downloaded_codeload",
                "branch": branch,
                "commit": "unknown_codeload_snapshot",
                "remote": spec["url"],
                "downloaded_at": dt.datetime.now(dt.timezone.utc).isoformat(),
                "proxy": proxy,
                "method": "github codeload tar.gz",
                "archive_url": archive_url,
                "archive_path": str(archive_path),
            }
            (partial / "DOWNLOAD_SOURCE.json").write_text(
                json.dumps(metadata, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
            )
            (partial / "collection.json").write_text(
                json.dumps(metadata, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
            )
            if destination.exists():
                shutil.rmtree(destination)
            partial.rename(destination)
            result.update(metadata)
            return result
        except Exception as exc:
            result["error"] = "git clone failed; codeload fallback failed: {}".format(exc)
            if partial.exists():
                shutil.rmtree(partial)
            return result

    if sparse_paths:
        sparse_result = run_command(
            [
                "git",
                "-C",
                str(partial),
                "sparse-checkout",
                "set",
                "--no-cone",
            ]
            + sparse_paths,
            proxy=proxy,
            timeout=min(args.timeout, 300.0),
        )
        if sparse_result.returncode != 0:
            result["error"] = "git sparse-checkout failed: {}".format(
                sparse_result.stderr.strip()[-2000:]
            )
            if partial.exists():
                shutil.rmtree(partial)
            return result

    try:
        commit = git_value(partial, "rev-parse", "HEAD")
        remote = git_value(partial, "config", "--get", "remote.origin.url")
        (partial / "DOWNLOAD_SOURCE.json").write_text(
            json.dumps(
                {
                    "repository": spec["repository"],
                    "url": spec["url"],
                    "branch": branch,
                    "commit": commit,
                    "remote": remote,
                    "downloaded_at": dt.datetime.now(dt.timezone.utc).isoformat(),
                    "proxy": proxy,
                    "method": "git clone --depth 1 --no-tags{}".format(
                        " --filter=blob:none --sparse" if sparse_paths else ""
                    ),
                    "sparse_paths": sparse_paths,
                },
                ensure_ascii=False,
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )
        metadata = {
            **spec,
            "status": "downloaded",
            "branch": branch,
            "commit": commit,
            "remote": remote,
            "downloaded_at": dt.datetime.now(dt.timezone.utc).isoformat(),
            "proxy": proxy,
            "method": "git clone --depth 1 --no-tags{}".format(
                " --filter=blob:none --sparse" if sparse_paths else ""
            ),
            "sparse_paths": sparse_paths,
        }
        (partial / "collection.json").write_text(
            json.dumps(metadata, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
        )
        if destination.exists():
            shutil.rmtree(destination)
        partial.rename(destination)
        result.update(metadata)
        return result
    except Exception as exc:
        result["error"] = "snapshot metadata failed: {}".format(exc)
        if partial.exists():
            shutil.rmtree(partial)
        return result


def main():
    args = parse_args()
    try:
        catalog = read_json(args.catalog.resolve())
    except (OSError, ValueError) as exc:
        print("catalog error: {}".format(exc), file=sys.stderr)
        return 2
    selected = set(args.repo_ids or [])
    unknown = selected - {item.get("id") for item in catalog}
    if unknown:
        print("unknown repository ids: {}".format(", ".join(sorted(unknown))), file=sys.stderr)
        return 2

    args.raw_root.mkdir(parents=True, exist_ok=True)
    manifest_path = args.raw_root.parent / "collection_manifest.jsonl"
    results = []
    with manifest_path.open("a", encoding="utf-8") as handle:
        for spec in catalog:
            if selected and spec["id"] not in selected:
                continue
            print("[collect] {}".format(spec["id"]), flush=True)
            result = collect_one(spec, args)
            results.append(result)
            handle.write(json.dumps(result, ensure_ascii=False) + "\n")
            handle.flush()
            if result["status"] in ("downloaded", "downloaded_codeload", "skipped_existing"):
                print(
                    "  {} {} {}".format(
                        result["status"], result.get("branch", "?"), result.get("commit", "")[:12]
                    ),
                    flush=True,
                )
            else:
                print("  FAILED: {}".format(result.get("error", "unknown error")), flush=True)
    failed = [item for item in results if item["status"] == "failed"]
    print(
        "completed: {} repositories, {} failed; manifest: {}".format(
            len(results), len(failed), manifest_path
        )
    )
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
