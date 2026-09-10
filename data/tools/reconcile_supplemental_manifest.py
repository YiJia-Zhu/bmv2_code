#!/usr/bin/env python3
"""Add orphaned supplemental files to the recovery manifest with caveats."""

import argparse
import datetime as dt
import hashlib
import json
import urllib.parse
from pathlib import Path


def read_jsonl(path):
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                yield json.loads(line)


def github_url(repo_key, relative):
    if repo_key.startswith("hf_p4llms_allp4files__"):
        owner = repo_key.split("__", 1)[1]
        parts = Path(relative).parts
        if len(parts) < 2:
            return None
        repository = parts[0]
        github_path = Path(*parts[1:])
    elif repo_key.startswith("hf_p4llms_") and "__" in repo_key[len("hf_p4llms_") :]:
        owner, repository = repo_key[len("hf_p4llms_") :].split("__", 1)
        github_path = Path(relative)
    else:
        return None
    encoded = "/".join(urllib.parse.quote(part, safe="") for part in github_path.parts)
    return "https://raw.githubusercontent.com/{}/{}/HEAD/{}".format(
        urllib.parse.quote(owner, safe=""), urllib.parse.quote(repository, safe=""), encoded
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    args = parser.parse_args()
    root = args.root.resolve()
    manifest_path = root / "download_manifest.jsonl"
    rows = list(read_jsonl(manifest_path)) if manifest_path.is_file() else []
    keys = {
        (row.get("source_repo_id", ""), row.get("local_path", "")) for row in rows
    }
    added = 0
    for path in sorted(root.rglob("*")):
        if not path.is_file() or path.name in {
            "README.md",
            "download_manifest.jsonl",
            "download_summary.json",
        }:
            continue
        relative = path.relative_to(root)
        if not relative.parts:
            continue
        repo_key = relative.parts[0]
        local_path = str(relative)
        key = (repo_key, local_path)
        if key in keys:
            continue
        url = github_url(repo_key, Path(*relative.parts[1:]))
        content = path.read_bytes()
        rows.append(
            {
                "source_repo_id": repo_key,
                "local_path": local_path,
                "url": url,
                "status": "recovered_existing_untracked",
                "retrieved_at": None,
                "size_bytes": len(content),
                "sha256": hashlib.sha256(content).hexdigest(),
                "provenance_note": "File existed from an earlier recovery pass before manifest merge support; retrieval time is unavailable.",
            }
        )
        keys.add(key)
        added += 1
    rows.sort(key=lambda row: (row.get("source_repo_id", ""), row.get("local_path", "")))
    with manifest_path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")
    summary_path = root / "download_summary.json"
    summary = {}
    if summary_path.is_file():
        summary = json.loads(summary_path.read_text(encoding="utf-8"))
    summary.update(
        {
            "manifest_reconciled_at": dt.datetime.now(dt.timezone.utc).isoformat(),
            "manifest_entry_count": len(rows),
            "manifest_reconciled_orphans_added": added,
        }
    )
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"manifest_entry_count": len(rows), "orphans_added": added}, ensure_ascii=False))


if __name__ == "__main__":
    main()
