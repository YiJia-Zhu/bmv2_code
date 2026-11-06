#!/usr/bin/env python3
"""Preserve an old unsharded SWH collection in its four current shards."""

import argparse
import hashlib
import json
import os
import shutil
import sqlite3
import subprocess
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path


DATA_DIRS = ("repos", "trackers", "packages")
TABLE_KEYS = {
    "origins": "origin_id", "repos": "repo_id", "api_tasks": "task_id",
    "packages": "origin_id", "assets": "asset_id",
}
SUCCESS_PACKAGES = {"go_empty_version_index", "go_version_index_saved",
                    "version_index_saved", "provider_unavailable"}


def utc_stamp():
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def digest(path):
    result = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(4 * 1024 * 1024), b""):
            result.update(block)
    return result.hexdigest()


def same_file(left, right):
    if left.stat().st_size != right.stat().st_size:
        return False
    return digest(left) == digest(right)


def shard_for(identifier):
    return int(identifier, 16) % 4


def source_rows(db, table):
    return [dict(row) for row in db.execute("SELECT * FROM " + table)]


def row_shard(table, row):
    if table == "repos":
        return shard_for(row["repo_id"])
    if table == "api_tasks":
        return shard_for(row["repo_id"])
    if table == "origins":
        return shard_for(row["repo_id"] or row["origin_id"])
    return shard_for(row["origin_id"])


def copy_new(source, destination):
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(destination.name + ".restore_tmp_" + str(os.getpid()))
    try:
        shutil.copy2(source, temporary)
        if not same_file(source, temporary):
            raise RuntimeError("copy checksum mismatch: " + str(source))
        try:
            os.link(temporary, destination)
        except FileExistsError:
            if not same_file(source, destination):
                raise RuntimeError("destination appeared with different data: " + str(destination))
    finally:
        temporary.unlink(missing_ok=True)


def audit_files(source, shards, apply, report):
    for section in DATA_DIRS:
        for path in (source / section).rglob("*"):
            if not path.is_file():
                continue
            relative = path.relative_to(source)
            identifier = relative.parts[1]
            target = shards[shard_for(identifier)]
            destination = target / relative
            report["files_checked"] += 1
            if not destination.exists():
                report["files_missing_by_section"][section] += 1
                report["bytes_missing_by_section"][section] += path.stat().st_size
                if apply:
                    copy_new(path, destination)
                continue
            if not same_file(path, destination):
                report["files_different_by_section"][section] += 1
                alternate = target / "legacy_full_history" / "conflicts" / relative
                if apply and not alternate.exists():
                    copy_new(path, alternate)
                if alternate.exists() and same_file(path, alternate):
                    report["conflicts_preserved"] += 1
                else:
                    report["conflicts_unpreserved"].append(str(relative))
                report["conflicts"].append(str(relative))
            if report["files_checked"] % 5000 == 0:
                print("files checked:", report["files_checked"], flush=True)


def archive_old_metadata(source, root):
    destination = root / "legacy_full_history"
    for path in source.rglob("*"):
        if not path.is_file() or path.relative_to(source).parts[0] in DATA_DIRS:
            continue
        target = destination / path.relative_to(source)
        if not target.exists():
            copy_new(path, target)
        elif not same_file(path, target):
            raise RuntimeError("archived metadata differs: " + str(target))


def audit_old_metadata(source, root, report):
    destination = root / "legacy_full_history"
    for path in source.rglob("*"):
        if not path.is_file() or path.relative_to(source).parts[0] in DATA_DIRS:
            continue
        report["metadata_checked"] += 1
        target = destination / path.relative_to(source)
        if not target.exists() or not same_file(path, target):
            report["metadata_unpreserved"].append(str(path.relative_to(source)))


def valid_repo(root, row):
    path = root / "repos" / row["repo_id"]
    marker = path / "mirror_complete.json"
    if not all((path / name).is_file() for name in
               ("commits.jsonl", "refs.tsv", "mirror.git/HEAD")) or not marker.is_file():
        return False
    try:
        info = json.loads(marker.read_text())
        if info["repo_id"] != row["repo_id"] or info["index_rows"] != row["commits"]:
            return False
        with (path / "commits.jsonl").open("rb") as stream:
            if sum(1 for _ in stream) != row["commits"]:
                return False
        result = subprocess.run(
            ["git", "--git-dir", str(path / "mirror.git"), "fsck",
             "--connectivity-only", "--no-reflogs"],
            stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, timeout=300,
        )
        return result.returncode == 0
    except (OSError, ValueError, KeyError, subprocess.TimeoutExpired):
        return False


def valid_api(root, row):
    path = root / "trackers" / row["repo_id"] / "pages" / (row["task_id"] + ".json")
    try:
        data = json.loads(path.read_text())
        return data["request_url"] == row["url"] and "response" in data
    except (OSError, ValueError, KeyError):
        return False


def valid_asset(root, row):
    path = root / "packages" / row["origin_id"] / "archives" / (row["asset_id"][:16] + "-" + row["filename"])
    marker = path.with_name(path.name + ".complete.json")
    try:
        info = json.loads(marker.read_text())
        return (path.stat().st_size == row["bytes"] == info["bytes"] and
                digest(path) == row["sha256"] == info["sha256"])
    except (OSError, ValueError, KeyError):
        return False


def replace_row(db, table, row):
    columns = list(row)
    db.execute("INSERT OR REPLACE INTO {} ({}) VALUES ({})".format(
        table, ",".join(columns), ",".join("?" for _ in columns)),
        tuple(row[column] for column in columns))


def merge_databases(source, shards, apply, report):
    old = sqlite3.connect("file:{}?mode=ro".format(source / "manifests/state.sqlite3"), uri=True)
    old.row_factory = sqlite3.Row
    current = []
    for shard in shards:
        db = sqlite3.connect(str(shard / "manifests/state.sqlite3"))
        db.row_factory = sqlite3.Row
        current.append(db)
    try:
        for table, key in TABLE_KEYS.items():
            existing = [{row[key]: dict(row) for row in source_rows(db, table)} for db in current]
            for row in source_rows(old, table):
                index = row_shard(table, row)
                target = existing[index].get(row[key])
                old_complete = row.get("status") == "complete"
                root = shards[index]
                valid = (valid_repo(root, row) if table == "repos" and old_complete else
                         valid_api(root, row) if table == "api_tasks" and old_complete else
                         valid_asset(root, row) if table == "assets" and old_complete else
                         True)
                if target is None:
                    report["db_missing"][table] += 1
                    if old_complete and not valid:
                        report["db_unresolved"].append({"table": table, "id": row[key],
                                                        "shard": index, "target_status": None})
                    if apply:
                        imported = dict(row)
                        if old_complete and not valid:
                            imported["status"] = "retry_wait"
                            imported["next_at"] = 0
                        replace_row(current[index], table, imported)
                    continue
                if not old_complete and not (
                        table == "packages" and row["status"] in SUCCESS_PACKAGES):
                    continue
                if target.get("status") == row.get("status"):
                    if old_complete and table in ("repos", "api_tasks", "assets"):
                        target_valid = (valid_repo(root, target) if table == "repos" else
                                        valid_api(root, target) if table == "api_tasks" else
                                        valid_asset(root, target))
                        if not target_valid:
                            report["db_unresolved"].append({"table": table, "id": row[key],
                                                            "shard": index, "target_status": target["status"]})
                    continue
                if valid and (table != "packages" or target["status"] not in SUCCESS_PACKAGES):
                    report["db_promoted"][table] += 1
                    if apply:
                        replace_row(current[index], table, row)
                elif not valid:
                    report["db_unresolved"].append({"table": table, "id": row[key],
                                                    "shard": index, "target_status": target["status"]})
        if apply:
            for db in current:
                db.commit()
    finally:
        old.close()
        for db in current:
            db.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--shards", type=Path, required=True)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    shards = [args.shards / ("shard_" + str(index)) for index in range(4)]
    if any(not (shard / "manifests/state.sqlite3").is_file() for shard in shards):
        parser.error("all four shard databases must exist")
    report = {"files_checked": 0, "files_missing_by_section": Counter(),
              "bytes_missing_by_section": Counter(), "files_different_by_section": Counter(),
              "conflicts": [], "conflicts_preserved": 0, "conflicts_unpreserved": [],
              "metadata_checked": 0, "metadata_unpreserved": [], "db_missing": Counter(),
              "db_promoted": Counter(), "db_unresolved": []}
    stamp = utc_stamp()
    if args.apply:
        for shard in shards:
            source_db = sqlite3.connect(str(shard / "manifests/state.sqlite3"))
            backup = sqlite3.connect(str(shard / "manifests" / ("state.before_full_restore_" + stamp + ".sqlite3")))
            source_db.backup(backup)
            backup.close()
            source_db.close()
        archive_old_metadata(args.source, args.shards)
    audit_files(args.source, shards, args.apply, report)
    audit_old_metadata(args.source, args.shards, report)
    merge_databases(args.source, shards, args.apply, report)
    output = args.shards / ("restore_full_history_" + stamp + ("_applied" if args.apply else "_plan") + ".json")
    if args.apply:
        output.write_text(json.dumps(report, indent=2, ensure_ascii=True) + "\n")
    print(json.dumps({key: value if key not in ("conflicts", "db_unresolved", "conflicts_unpreserved", "metadata_unpreserved") else len(value)
                      for key, value in report.items()}, indent=2))
    if args.apply:
        print("report:", output)
    return 0 if not (report["db_unresolved"] or report["conflicts_unpreserved"]
                     or report["metadata_unpreserved"]) else 2


if __name__ == "__main__":
    sys.exit(main())
