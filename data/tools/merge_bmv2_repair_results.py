#!/usr/bin/env python3
"""Overlay repaired BMv2 results onto an immutable baseline result JSONL."""

import argparse
import datetime as dt
import json
from pathlib import Path


def read_jsonl(path):
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                yield json.loads(line)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", type=Path, required=True)
    parser.add_argument("--repair", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    baseline = list(read_jsonl(args.baseline))
    repairs = {row["record_id"]: row for row in read_jsonl(args.repair)}
    if len(repairs) != sum(1 for _ in read_jsonl(args.repair)):
        raise SystemExit("repair result IDs are duplicated")
    replaced = 0
    output = []
    for row in baseline:
        repair = repairs.get(row.get("record_id"))
        if repair is None:
            output.append(row)
            continue
        merged = dict(row)
        merged.update(repair)
        merged["baseline_status"] = row.get("status")
        merged["repair_source"] = str(args.repair)
        output.append(merged)
        replaced += 1
    missing = sorted(set(repairs) - {row.get("record_id") for row in baseline})
    if missing:
        raise SystemExit("repair IDs absent from baseline: {}".format(missing[:3]))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", encoding="utf-8") as handle:
        for row in output:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")
    summary = {
        "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "baseline": str(args.baseline),
        "repair": str(args.repair),
        "output": str(args.output),
        "baseline_count": len(baseline),
        "repair_count": len(repairs),
        "replaced_count": replaced,
        "unchanged_count": len(baseline) - replaced,
    }
    summary_path = args.output.with_name(args.output.stem + "_summary.json")
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
