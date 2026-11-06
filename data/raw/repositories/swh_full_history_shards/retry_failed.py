#!/usr/bin/env python3
"""Back up shard checkpoints and requeue selected terminal tasks."""

import argparse
import datetime
import fcntl
import sqlite3
from pathlib import Path


TABLES = ("repos", "api_tasks", "assets")


def counts(connection, gitlab_401=False):
    if gitlab_401:
        return {"gitlab_401": connection.execute(
            "SELECT COUNT(*) FROM api_tasks WHERE host='gitlab.com' "
            "AND status='unavailable_http_401'").fetchone()[0]}
    return {table: connection.execute(
        "SELECT COUNT(*) FROM {} WHERE status='failed'".format(table)
    ).fetchone()[0] for table in TABLES}


def requeue(path, apply, stamp, gitlab_401=False):
    lock = path.parent.parent / "collector.lock"
    with lock.open("a") as lock_file:
        try:
            fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise RuntimeError("collector is active: {}".format(lock))
        return requeue_unlocked(path, apply, stamp, gitlab_401)


def requeue_unlocked(path, apply, stamp, gitlab_401):
    connection = sqlite3.connect(str(path), timeout=1)
    try:
        before = counts(connection, gitlab_401)
        if not apply or not any(before.values()):
            return before, None

        label = "gitlab_401" if gitlab_401 else "failed_retry"
        backup_path = path.with_name("state.before_{}_{}.sqlite3".format(label, stamp))
        if backup_path.exists():
            raise RuntimeError("backup already exists: {}".format(backup_path))
        backup = sqlite3.connect(str(backup_path))
        try:
            connection.backup(backup)
            if backup.execute("PRAGMA quick_check").fetchone()[0] != "ok":
                raise RuntimeError("SQLite backup failed validation: {}".format(backup_path))
        finally:
            backup.close()

        connection.execute("BEGIN IMMEDIATE")
        if counts(connection, gitlab_401) != before:
            connection.rollback()
            raise RuntimeError("Checkpoint changed during backup: {}".format(path))
        if gitlab_401:
            connection.execute("""UPDATE api_tasks SET status='pending', attempts=0,
                next_at=0, finished_at=NULL WHERE host='gitlab.com'
                AND status='unavailable_http_401'""")
        else:
            connection.execute("""UPDATE repos SET status='retry_wait', attempts=0,
                next_at=0, started_at=NULL, finished_at=NULL, elapsed_s=NULL
                WHERE status='failed'""")
            connection.execute("""UPDATE api_tasks SET status='retry_wait', attempts=0,
                next_at=0, finished_at=NULL WHERE status='failed'""")
            connection.execute("""UPDATE assets SET status='retry_wait', attempts=0,
                next_at=0 WHERE status='failed'""")
        connection.commit()
        return before, backup_path
    finally:
        connection.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="back up and update checkpoints")
    parser.add_argument("--gitlab-401", action="store_true",
                        help="requeue only old gitlab.com HTTP 401 API tasks")
    args = parser.parse_args()
    root = Path(__file__).resolve().parent
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    paths = [root / "shard_{}".format(index) / "manifests/state.sqlite3"
             for index in range(4)]
    for path in paths:
        if not path.is_file():
            parser.error("missing checkpoint: {}".format(path))
    lock_path = root / "run_all.lock"
    with lock_path.open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            parser.error("run_all.sh is active; stop it before requeueing")
        for index, path in enumerate(paths):
            before, backup = requeue(path, args.apply, stamp, args.gitlab_401)
            if args.gitlab_401:
                print("shard_{} gitlab.com HTTP 401={} backup={}".format(
                    index, before["gitlab_401"], backup if backup else "none"))
            else:
                print("shard_{} failed: Git={} API={} assets={} backup={}".format(
                    index, before["repos"], before["api_tasks"], before["assets"],
                    backup if backup else "none"))


if __name__ == "__main__":
    main()
