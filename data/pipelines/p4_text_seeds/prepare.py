#!/usr/bin/env python3
"""Verify crawled seed files and build an unlabeled document review queue."""

import argparse
from collections import Counter
import hashlib
import json
import os
from pathlib import Path
import tempfile
from urllib.parse import urlsplit


def sha256_file(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def checked_path(root, relative):
    path = (root / relative).resolve()
    if root.resolve() not in path.parents:
        raise ValueError("path outside crawl root: {}".format(relative))
    return path


def output_jsonl(path, records):
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", dir=str(path.parent),
            delete=False) as stream:
        temporary = Path(stream.name)
        try:
            for record in records:
                stream.write(json.dumps(record, ensure_ascii=False,
                                        sort_keys=True) + "\n")
            stream.flush()
            os.fsync(stream.fileno())
        except BaseException:
            temporary.unlink(missing_ok=True)
            raise
    os.replace(str(temporary), str(path))


def prepare(crawl_root, output_root):
    crawl_root = Path(crawl_root)
    output_root = Path(output_root)
    documents = {}
    occurrences = []
    review = []
    counts = Counter()
    with (crawl_root / "documents.jsonl").open(encoding="utf-8") as stream:
        rows = [json.loads(line) for line in stream]
    structured_topics = {
        row["url"][:-5] for row in rows
        if row.get("source") == "p4forum" and row["url"].endswith(".json")
        and row.get("text_path")
    }
    for row in rows:
        counts["fetched"] += 1
        raw_path = checked_path(crawl_root, row["raw_path"])
        if sha256_file(raw_path) != row["raw_sha256"]:
            raise ValueError("raw hash mismatch: {}".format(row["url"]))
        text_path = row.get("text_path")
        text_hash = row.get("text_sha256")
        if text_path:
            path = checked_path(crawl_root, text_path)
            if sha256_file(path) != text_hash:
                raise ValueError("text hash mismatch: {}".format(row["url"]))
            content = path.read_text(encoding="utf-8")
        else:
            content = ""
        doc_id = "p4doc_{}".format(text_hash) if content else None
        if doc_id and doc_id not in documents:
            documents[doc_id] = {
                "doc_id": doc_id,
                "text_sha256": text_hash,
                "text_path": text_path,
                "text_chars": len(content),
                "text_words": len(content.split()),
                "extractor_version": row.get("extractor_version"),
            }
            counts["unique_texts"] += 1
        if not content:
            counts["empty_text"] += 1
        if row.get("error"):
            counts["extraction_errors"] += 1
        occurrence_id = "p4occ_{}".format(hashlib.sha256(
            row["url"].encode("utf-8")).hexdigest())
        occurrences.append({
            "occurrence_id": occurrence_id,
            "doc_id": doc_id,
            "url": row["url"],
            "final_url": row.get("final_url"),
            "source": row["source"],
            "host": urlsplit(row["url"]).hostname,
            "parent_url": row.get("parent_url"),
            "title": row.get("title"),
            "content_type": row.get("content_type"),
            "raw_sha256": row["raw_sha256"],
            "raw_path": row["raw_path"],
            "fetched_at": row.get("fetched_at"),
            "extractor_version": row.get("extractor_version"),
            "extraction_error": row.get("error"),
        })
        if doc_id and row["url"] not in structured_topics:
            review.append({
                "occurrence_id": occurrence_id,
                "doc_id": doc_id,
                "url": row["url"],
                "title": row.get("title"),
                "source": row["source"],
                "text_path": text_path,
                "label": "pending",
                "p4_version": None,
                "architecture": None,
                "license_status": "unreviewed",
                "notes": None,
            })
            counts["review_items"] += 1
        elif row["url"] in structured_topics:
            counts["html_topics_with_json"] += 1
    output_jsonl(output_root / "documents.jsonl", documents.values())
    output_jsonl(output_root / "occurrences.jsonl", occurrences)
    output_jsonl(output_root / "review_queue.jsonl", review)
    return dict(counts)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--crawl-root", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(prepare(args.crawl_root, args.output_root), sort_keys=True))


if __name__ == "__main__":
    main()
