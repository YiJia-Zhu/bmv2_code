#!/usr/bin/env python3
"""Export reachable Git commit metadata without checking out each revision."""

import argparse
import json
import os
import subprocess
from pathlib import Path


def parse_commit(oid, payload):
    header, separator, message = payload.partition(b"\n\n")
    if not separator:
        raise ValueError("commit {} has no header/message separator".format(oid))
    fields = {}
    parents = []
    for line in header.splitlines():
        if line.startswith(b" "):
            continue
        key, _, value = line.partition(b" ")
        if key == b"parent":
            parents.append(value.decode("ascii"))
        elif key in (b"tree", b"author", b"committer", b"encoding"):
            fields[key.decode("ascii")] = value.decode("utf-8", "surrogateescape")
    for required in ("tree", "author", "committer"):
        if required not in fields:
            raise ValueError("commit {} is missing {}".format(oid, required))
    return {
        "commit_oid": oid,
        "tree_oid": fields["tree"],
        "parent_oids": parents,
        "author": fields["author"],
        "committer": fields["committer"],
        "encoding": fields.get("encoding"),
        "message": message.decode("utf-8", "surrogateescape"),
    }


def export(repository, output):
    rev_command = ["git", "-C", str(repository), "rev-list", "--all"]
    cat_command = ["git", "-C", str(repository), "cat-file", "--batch"]
    temporary = output.with_name(output.name + ".tmp")
    count = 0
    try:
        with subprocess.Popen(rev_command, stdout=subprocess.PIPE) as revisions, \
                subprocess.Popen(cat_command, stdin=subprocess.PIPE,
                                 stdout=subprocess.PIPE) as objects, \
                temporary.open("w", encoding="utf-8") as destination:
            for raw_oid in revisions.stdout:
                oid = raw_oid.strip().decode("ascii")
                objects.stdin.write(raw_oid)
                objects.stdin.flush()
                response = objects.stdout.readline().split()
                if len(response) != 3 or response[0] != raw_oid.strip() or response[1] != b"commit":
                    raise ValueError("unexpected git cat-file response for {}: {}".format(oid, response))
                size = int(response[2])
                payload = objects.stdout.read(size)
                if len(payload) != size or objects.stdout.read(1) != b"\n":
                    raise ValueError("truncated git object {}".format(oid))
                destination.write(json.dumps(parse_commit(oid, payload), ensure_ascii=True) + "\n")
                count += 1
            objects.stdin.close()
            if revisions.wait() != 0 or objects.wait() != 0:
                raise RuntimeError("git rev-list or cat-file failed")
        os.replace(str(temporary), str(output))
    except Exception:
        if temporary.exists():
            temporary.unlink()
        raise
    return count


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("repository", type=Path, help="bare or regular Git repository")
    parser.add_argument("output", type=Path, help="JSONL commit index")
    args = parser.parse_args()
    count = export(args.repository, args.output)
    print("Exported {} reachable commits to {}".format(count, args.output))


if __name__ == "__main__":
    main()
