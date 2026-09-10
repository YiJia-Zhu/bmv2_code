#!/usr/bin/env python3
"""Build a consolidated inventory from successful and incomplete downloads."""

import datetime as dt
import json
from pathlib import Path


DATA_ROOT = Path(__file__).resolve().parents[1]
RAW_ROOT = DATA_ROOT / "raw"
REPO_ROOT = RAW_ROOT / "repositories"


def main():
    records = []
    if REPO_ROOT.is_dir():
        for path in sorted(REPO_ROOT.iterdir()):
            marker = path / "collection.json"
            if marker.is_file():
                record = json.loads(marker.read_text(encoding="utf-8"))
                record["inventory_status"] = "complete"
            elif path.is_dir():
                record = {
                    "id": path.name,
                    "inventory_status": "incomplete_directory",
                    "path": str(path.relative_to(RAW_ROOT)),
                }
            else:
                continue
            records.append(record)
    for path in sorted((RAW_ROOT / "archives").glob("*.partial*")) if (RAW_ROOT / "archives").is_dir() else []:
        records.append(
            {
                "id": path.name,
                "inventory_status": "incomplete_archive",
                "path": str(path.relative_to(RAW_ROOT)),
                "size_bytes": path.stat().st_size,
            }
        )
    output = RAW_ROOT / "inventory.jsonl"
    with output.open("w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record, ensure_ascii=False) + "\n")
    summary = {
        "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "complete": sum(item.get("inventory_status") == "complete" for item in records),
        "incomplete": sum(item.get("inventory_status", "").startswith("incomplete") for item in records),
        "records": len(records),
        "output": str(output),
    }
    (RAW_ROOT / "inventory_summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
