#!/usr/bin/env python3
"""Partition a stopped full-history collector checkpoint among four machines."""

import argparse
import csv
import fcntl
import hashlib
import json
import shutil
import sqlite3
from pathlib import Path

from swh_history_common import atomic_json, utc_now


COLLECTOR_FILES = (
    "download_swh_history.py", "export_git_commit_index.py",
    "swh_history_api.py", "swh_history_common.py",
    "swh_history_packages.py", "swh_history_status.py",
)
TABLES = ("origins", "repos", "api_tasks", "packages", "assets")
RETRY_TABLES = ("repos", "api_tasks", "packages", "assets")


def owner(identifier, count):
    return int(identifier, 16) % count


def copy_directory(source, destination):
    if source.is_dir():
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copytree(source, destination, symlinks=True)


def copy_git_helper(source_root, destination):
    source = source_root / "toolchain/git-core"
    binary = (source / "git-remote-http").resolve()
    if not binary.is_file():
        return False
    core = destination / "toolchain/git-core"
    core.mkdir(parents=True)
    shutil.copy2(binary, core / "git-remote-http")
    for item in source.iterdir():
        if item.name == "git-remote-http":
            continue
        if item.name in ("git-remote-https", "git-remote-ftp", "git-remote-ftps"):
            (core / item.name).symlink_to("git-remote-http")
        elif item.is_symlink():
            (core / item.name).symlink_to(item.readlink())
    return True


def write_csv(path, rows, columns):
    with path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def export_queue(db, root):
    queue = root / "queues"
    queue.mkdir()
    specs = (
        ("git", "repos", "repo_id,canonical_url,host,status,attempts,error",
         "status!='complete'"),
        ("api", "api_tasks", "task_id,repo_id,url,kind,status,attempts,error",
         "status!='complete'"),
        ("packages", "packages", "origin_id,status,attempts,error",
         "status IN ('pending','retry_wait','failed')"),
        ("assets", "assets", "asset_id,origin_id,url,status,attempts,error",
         "status!='complete'"),
    )
    counts = {}
    for name, table, fields, predicate in specs:
        columns = fields.split(",")
        rows = [dict(zip(columns, row)) for row in db.execute(
            "SELECT {} FROM {} WHERE {} ORDER BY {}".format(
                fields, table, predicate, columns[0]))]
        write_csv(queue / (name + ".csv"), rows, columns)
        counts[name] = len(rows)
    return counts


def partition_database(source, destination, index, count, digest):
    target = sqlite3.connect(str(destination))
    try:
        source.backup(target)
        with target:
            target.execute("CREATE TEMP TABLE keep_repos (id TEXT PRIMARY KEY)")
            target.execute("CREATE TEMP TABLE keep_origins (id TEXT PRIMARY KEY)")
            target.executemany("INSERT INTO keep_repos VALUES (?)", (
                (row[0],) for row in target.execute("SELECT repo_id FROM repos")
                if owner(row[0], count) == index))
            target.executemany("INSERT INTO keep_origins VALUES (?)", (
                (row[0],) for row in target.execute("SELECT origin_id,repo_id FROM origins")
                if owner(row[1] or row[0], count) == index))
            target.execute("DELETE FROM api_tasks WHERE repo_id NOT IN (SELECT id FROM keep_repos)")
            target.execute("DELETE FROM assets WHERE origin_id NOT IN (SELECT id FROM keep_origins)")
            target.execute("DELETE FROM packages WHERE origin_id NOT IN (SELECT id FROM keep_origins)")
            target.execute("DELETE FROM repos WHERE repo_id NOT IN (SELECT id FROM keep_repos)")
            target.execute("DELETE FROM origins WHERE origin_id NOT IN (SELECT id FROM keep_origins)")
            target.execute("UPDATE metadata SET value=? WHERE key='input_identity'",
                           (json.dumps([digest, index, count]),))
            target.execute("DELETE FROM metadata WHERE key='first_started_epoch'")
            for table in RETRY_TABLES:
                target.execute("UPDATE {} SET status='retry_wait',attempts=0,next_at=0 "
                               "WHERE status IN ('failed','retry_wait','running')".format(table))
            target.execute("UPDATE repos SET status='retry_wait',attempts=0,next_at=0 "
                           "WHERE status='inaccessible'")
        target.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        target.execute("PRAGMA journal_mode=DELETE")
        target.row_factory = sqlite3.Row
        return {table: [dict(row) for row in target.execute("SELECT * FROM " + table)]
                for table in TABLES}, export_queue(target, destination.parent)
    finally:
        target.close()


def write_runner(root, index, count):
    script = root / "run.sh"
    script.write_text("""#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd -- "$(dirname -- "${{BASH_SOURCE[0]}}")" && pwd)"
for REQUIRED in input.csv manifests/state.sqlite3; do
  if [[ ! -f "$ROOT/$REQUIRED" ]]; then
    printf 'Missing %s; copy this shard checkpoint from the source machine.\\n' "$ROOT/$REQUIRED" >&2
    exit 2
  fi
done
python3 -u "$ROOT/collector/download_swh_history.py" \\
  --input "$ROOT/input.csv" --output "$ROOT" \\
  --shard-count {count} --shard-index {index} \\
  --proxy "${{SWH_HISTORY_PROXY:-}}" \\
  --env-file "${{SWH_HISTORY_ENV_FILE:-$ROOT/.env}}" \\
  "$@" >> "$ROOT/run.log" 2>&1
""".format(index=index, count=count), encoding="utf-8")
    script.chmod(0o755)


def prepare(source_root, output_root, count=4):
    source_root = Path(source_root).resolve()
    output_root = Path(output_root).resolve()
    if count != 4:
        raise ValueError("this handoff requires exactly four shards")
    if output_root == source_root or source_root in output_root.parents:
        raise ValueError("output must be outside the original checkpoint")
    status = json.loads((source_root / "status.json").read_text())
    if not status.get("final"):
        raise ValueError("stop the collector and wait for final=true before splitting")
    config = json.loads((source_root / "run_config.json").read_text())
    if (config["shard_index"], config["shard_count"]) != (0, 1):
        raise ValueError("source must be the original unsharded checkpoint")
    input_path = Path(config["input"])
    digest = hashlib.sha256(input_path.read_bytes()).hexdigest()
    previous = json.loads((source_root / "manifests/input.json").read_text())
    if previous["sha256"] != digest:
        raise ValueError("input CSV differs from the stopped checkpoint")
    output_root.mkdir(parents=True, exist_ok=True)
    for index in range(count):
        if (output_root / ("shard_{}".format(index))).exists() or (output_root / (".shard_{}.building".format(index))).exists():
            raise FileExistsError("shard output already exists; leave it untouched: {}".format(index))

    with (source_root / "collector.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        source = sqlite3.connect("file:{}?mode=ro".format(source_root / "manifests/state.sqlite3"), uri=True)
        try:
            manifest = {"created_at": utc_now(), "source": str(source_root),
                        "source_status_at": status["updated_at"], "input_sha256": digest,
                        "shard_count": count, "shards": []}
            tools_dir = Path(__file__).resolve().parent
            for index in range(count):
                stage = output_root / (".shard_{}.building".format(index))
                final = output_root / ("shard_{}".format(index))
                stage.mkdir()
                (stage / "manifests").mkdir()
                records, pending = partition_database(
                    source, stage / "manifests/state.sqlite3", index, count, digest)
                shutil.copy2(input_path, stage / "input.csv")
                (stage / "collector").mkdir()
                for filename in COLLECTOR_FILES:
                    shutil.copy2(tools_dir / filename, stage / "collector" / filename)
                bundled_helper = copy_git_helper(source_root, stage)
                for repo in records["repos"]:
                    repo_id = repo["repo_id"]
                    copy_directory(source_root / "repos" / repo_id,
                                   stage / "repos" / repo_id)
                    copy_directory(source_root / "trackers" / repo_id,
                                   stage / "trackers" / repo_id)
                for package in records["packages"]:
                    origin_id = package["origin_id"]
                    copy_directory(source_root / "packages" / origin_id,
                                   stage / "packages" / origin_id)
                    evidence = stage / "packages" / origin_id / "upstream_evidence.json"
                    if evidence.exists():
                        item = json.loads(evidence.read_text())
                        upstream_id = item.get("upstream_repository", {}).get("repo_id")
                        if upstream_id:
                            item["upstream_owner_shard"] = owner(upstream_id, count)
                            atomic_json(evidence, item)
                with (stage / "manifests/origin_repo_map.jsonl").open("w", encoding="utf-8") as stream:
                    for row in sorted(records["origins"], key=lambda item: item["origin_id"]):
                        stream.write(json.dumps(row, ensure_ascii=True) + "\n")
                atomic_json(stage / "manifests/input.json", dict(previous,
                            shard_index=index, shard_count=count, imported_at=utc_now()))
                write_runner(stage, index, count)
                counts = {table: len(records[table]) for table in TABLES}
                counts["git_complete"] = sum(row["status"] == "complete" for row in records["repos"])
                counts["api_complete"] = sum(row["status"] == "complete" for row in records["api_tasks"])
                counts["assets_complete"] = sum(row["status"] == "complete" for row in records["assets"])
                counts["pending"] = pending
                counts["bundled_git_helper"] = bundled_helper
                atomic_json(stage / "handoff.json", dict(counts, shard_index=index,
                            shard_count=count, source_status_at=status["updated_at"]))
                stage.rename(final)
                manifest["shards"].append(dict(directory=final.name, **counts))
                atomic_json(output_root / "split_manifest.json", manifest)
                print("shard {} ready: {} repos, {} pending Git, {} pending API, {} pending assets".format(
                    index, counts["repos"], pending["git"], pending["api"], pending["assets"]), flush=True)
            return manifest
        finally:
            source.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, default=Path("data/raw/repositories/swh_full_history"))
    parser.add_argument("--output", type=Path, default=Path("data/raw/repositories/swh_full_history_shards"))
    args = parser.parse_args()
    prepare(args.source, args.output)


if __name__ == "__main__":
    main()
