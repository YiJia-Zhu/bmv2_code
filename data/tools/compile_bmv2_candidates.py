#!/usr/bin/env python3
"""Compile normalized P4_16 candidates with p4c-bm2-ss.

This is a provenance-preserving pre-filter, not the final functional
benchmark. It uses each candidate's original repository working tree so that
relative includes continue to resolve. Compiler stderr is saved separately.
"""

import argparse
import datetime as dt
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from collections import defaultdict


DATA_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_RAW_ROOT = DATA_ROOT / "raw" / "repositories"
DEFAULT_INPUT = DATA_ROOT / "normalized" / "p4_code.jsonl"
DEFAULT_OUTPUT = DATA_ROOT / "normalized" / "bmv2_compile_results.jsonl"


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument(
        "--context-input",
        type=Path,
        default=None,
        help="optional JSONL containing inline repository context (used by p4llms exports)",
    )
    parser.add_argument("--raw-root", type=Path, default=DEFAULT_RAW_ROOT)
    parser.add_argument(
        "--supplemental-root",
        type=Path,
        default=None,
        help="optional repository-shaped dependency overlay; raw inputs are not modified",
    )
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--compiler", default="p4c-bm2-ss")
    parser.add_argument("--timeout", type=float, default=60.0)
    parser.add_argument("--limit", type=int, default=0, help="0 means all candidates")
    parser.add_argument("--offset", type=int, default=0, help="skip this many selected candidates before applying --limit")
    parser.add_argument(
        "--architectures",
        default="v1model",
        help="comma-separated architecture hints to include; default: v1model",
    )
    parser.add_argument(
        "--include-unknown",
        action="store_true",
        help="also compile P4_16 files whose architecture is unknown",
    )
    parser.add_argument(
        "--include-path",
        action="append",
        default=[],
        help="additional preprocessor include path; may be repeated",
    )
    parser.add_argument(
        "--auto-include",
        action="store_true",
        help="resolve repository-local #include files and add their directories",
    )
    parser.add_argument(
        "--error-root",
        type=Path,
        default=None,
        help="directory for stderr logs (default: output directory/bmv2_compile_errors)",
    )
    return parser.parse_args()


def safe_name(value):
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", str(value)).strip("._") or "candidate"


def include_names(text):
    """Return quoted and angle-bracket include names in source order."""
    return re.findall(r"^\s*#\s*include\s*[<\"]([^>\"]+)[>\"]", text, re.MULTILINE)


def build_repo_include_index(repo_dir):
    """Index repository files by basename for generated/local includes.

    The index deliberately excludes VCS and common build output directories.
    It is only used to add include directories; source files remain untouched.
    """
    index = defaultdict(list)
    for path in repo_dir.rglob("*"):
        if not path.is_file() or skip_include_path(path):
            continue
        index[path.name].append(path)
    return index


def skip_include_path(path):
    return any(part in {".git", "build", "out", "dist", "bazel-bin", "bazel-out"} for part in path.parts)


def auto_include_dirs(repo_dir, source, text, index):
    """Find likely include directories without changing the repository."""
    dirs = []
    seen = set()

    def add(path):
        path = Path(path).resolve()
        if path.is_dir() and str(path) not in seen:
            seen.add(str(path))
            dirs.append(path)

    add(source.parent)
    add(repo_dir)
    for include in include_names(text):
        include_path = Path(include)
        # Preserve normal relative include semantics first.
        for candidate in (source.parent / include_path, repo_dir / include_path):
            if candidate.is_file():
                add(candidate.parent)
        # Resolve a repository-relative suffix only when it is unique.  The
        # previous basename-only fallback could select an unrelated project
        # file (for example another ``headers.p4``) from the same aggregated
        # owner export and silently compile against the wrong source.
        basename = include_path.name
        matches = list(index.get(basename, []))
        include_parts = tuple(part for part in include_path.parts if part not in (".", ".."))
        suffix = "/".join(include_parts)
        suffix_matches = []
        for file_path in matches:
            try:
                relative = file_path.resolve().relative_to(repo_dir.resolve())
            except ValueError:
                continue
            relative_text = str(relative).replace(os.sep, "/")
            if suffix and (relative_text == suffix or relative_text.endswith("/" + suffix)):
                suffix_matches.append(file_path)
        if len(suffix_matches) == 1:
            file_path = suffix_matches[0]
            candidate = file_path.parent
            for _ in range(max(0, len(include_parts) - 1)):
                candidate = candidate.parent
            add(candidate)
        elif len(include_parts) == 1 and len(suffix_matches) == 0 and len(matches) == 1:
            # A plain basename such as ``deparser.p4`` has no useful suffix;
            # permit it only when the repository contains exactly one file
            # with that name.
            add(matches[0].parent)
    return dirs


def has_whitespace(path):
    """Return whether a compiler-facing path contains shell-significant whitespace."""
    return any(character.isspace() for character in str(path))


def compiler_alias(path, temporary_path, prefix, cache):
    """Create a whitespace-free alias for p4c's internal C preprocessor.

    Some p4c versions invoke ``cpp`` with an unquoted input path.  A source
    path such as ``p4 code/main.p4`` then becomes two arguments even though
    Python passed it correctly.  Symlinks keep the source immutable while
    presenting p4c a path that contains no spaces.
    """
    path = Path(path)
    if not has_whitespace(path):
        return path
    key = str(path.resolve())
    if key in cache:
        return cache[key]
    alias = temporary_path / (prefix + str(len(cache)))
    try:
        alias.symlink_to(path, target_is_directory=path.is_dir())
    except OSError:
        if path.is_dir():
            shutil.copytree(path, alias)
        else:
            shutil.copy2(path, alias)
    cache[key] = alias
    return alias


def resolve_source(raw_root, record):
    """Resolve both repository snapshots and flat HF source snapshots.

    GitHub records retain ``source_repo_id/source_path``.  The HF export has
    no repository working tree, so it records the actual cleaned source under
    ``source_file`` (normally ``individual_files/<index>...p4``).
    """
    source_file = record.get("source_file")
    if source_file:
        source = raw_root / source_file
        return raw_root, source
    # Some normalized exports (notably p4llms/p4dataset) contain the source
    # inline in JSONL and have no repository working tree.  The caller will
    # materialize ``content`` in its per-record temporary directory.
    if record.get("source_dataset") == "p4llms/p4dataset" or record.get("source_variant"):
        return raw_root, None
    repo_dir = raw_root / record["source_repo_id"]
    return repo_dir, repo_dir / record["source_path"]


def main():
    args = parse_args()
    if args.timeout <= 0 or args.limit < 0 or args.offset < 0:
        print("timeout must be positive; offset and limit must not be negative", file=sys.stderr)
        return 2
    if not args.input.is_file():
        print("input not found: {}".format(args.input), file=sys.stderr)
        return 2
    if args.limit and args.limit < 1:
        print("limit must be zero or positive", file=sys.stderr)
        return 2
    selected_architectures = {item.strip() for item in args.architectures.split(",") if item.strip()}
    raw_root = args.raw_root.resolve()
    candidates = []
    selected_count = 0
    with args.input.open(encoding="utf-8") as input_handle:
        for line in input_handle:
            if not line.strip():
                continue
            record = json.loads(line)
            if record.get("record_kind") != "p4_source":
                continue
            if record.get("language") != "p4_16":
                continue
            architecture = record.get("architecture", "unknown")
            if architecture not in selected_architectures and not (
                args.include_unknown and architecture == "unknown"
            ):
                continue
            if selected_count < args.offset:
                selected_count += 1
                continue
            if args.limit and len(candidates) >= args.limit:
                break
            candidates.append(record)
            selected_count += 1
    if not candidates:
        print("no candidates selected", file=sys.stderr)
        return 2

    # The p4llms export stores files inline in JSONL rather than as a checked
    # out repository.  Reconstruct a lightweight per-repository tree from the
    # candidate records so relative includes such as ``include/headers.p4``
    # resolve exactly as they would in the source snapshot.  Other datasets
    # continue using their existing raw repository/source-file paths.
    inline_context_temp = None
    inline_sources = {}
    inline_repo_dirs = {}
    candidate_repo_ids = {
        record.get("source_repo_id")
        for record in candidates
        if record.get("source_repo_id")
    }
    inline_records = []
    context_input = args.context_input or args.input
    if any(record.get("source_dataset") == "p4llms/p4dataset" or record.get("source_variant") for record in candidates):
        with context_input.open(encoding="utf-8") as context_handle:
            for line in context_handle:
                if not line.strip():
                    continue
                record = json.loads(line)
                if record.get("source_repo_id") in candidate_repo_ids and (
                    record.get("source_dataset") == "p4llms/p4dataset" or record.get("source_variant")
                ):
                    inline_records.append(record)
    if inline_records:
        inline_context_temp = tempfile.TemporaryDirectory(prefix="bmv2_inline_context_")
        context_root = Path(inline_context_temp.name)
        for record in inline_records:
            repo_key = record.get("source_repo_id") or record.get("source_repo") or "unknown"
            repo_dir = inline_repo_dirs.get(repo_key)
            if repo_dir is None:
                repo_dir = context_root / safe_name(repo_key)
                repo_dir.mkdir(parents=True, exist_ok=True)
                inline_repo_dirs[repo_key] = repo_dir
            relative = Path(str(record.get("source_path") or "source.p4").lstrip("/"))
            if not relative.parts or any(part in {"", ".", ".."} for part in relative.parts):
                relative = Path(safe_name(relative.name or "source.p4"))
            materialized = repo_dir / relative
            materialized.parent.mkdir(parents=True, exist_ok=True)
            materialized.write_text(record.get("content") or "", encoding="utf-8")
            inline_sources[record["record_id"]] = (repo_dir, materialized)
        if args.supplemental_root:
            supplemental_root = args.supplemental_root.resolve()
            for repo_key, repo_dir in inline_repo_dirs.items():
                supplemental_repo = supplemental_root / repo_key
                if not supplemental_repo.is_dir():
                    continue
                for supplemental_file in supplemental_repo.rglob("*"):
                    if not supplemental_file.is_file():
                        continue
                    relative = supplemental_file.relative_to(supplemental_repo)
                    target = repo_dir / relative
                    if target.exists():
                        continue
                    target.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(supplemental_file, target)

    compiler_path = args.compiler
    if os.path.sep not in compiler_path and not Path(compiler_path).is_file():
        from shutil import which

        compiler_path = which(compiler_path) or compiler_path
    output_path = args.output.resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    error_root = (args.error_root or (output_path.parent / "bmv2_compile_errors")).resolve()
    error_root.mkdir(parents=True, exist_ok=True)
    repo_include_indexes = {}
    summary = {
        "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "compiler": compiler_path,
        "compiler_args": ["--p4v", "16", "--p4runtime-files", "<p4info>", "-o", "<json>"],
        "candidate_count": len(candidates),
        "candidate_offset": args.offset,
        "compiled": 0,
        "failed": 0,
        "missing_source": 0,
        "timeout": 0,
        "auto_include": bool(args.auto_include),
        "include_paths": [str(Path(item).resolve()) for item in args.include_path],
        "supplemental_root": str(args.supplemental_root.resolve()) if args.supplemental_root else None,
    }
    try:
        with output_path.open("w", encoding="utf-8") as output:
            for index, record in enumerate(candidates, 1):
                repo_dir, source = resolve_source(raw_root, record)
                if record["record_id"] in inline_sources:
                    repo_dir, source = inline_sources[record["record_id"]]
                result = {
                    "record_id": record["record_id"],
                    "source_repo_id": record["source_repo_id"],
                    "source_repo": record.get("source_repo"),
                    "source_commit": record.get("source_commit"),
                    "source_path": record["source_path"],
                    "architecture": record.get("architecture"),
                    "language": record.get("language"),
                    "status": "missing_source",
                }
                inline_source = record["record_id"] in inline_sources
                temporary_source = source is None and record.get("content") is not None
                inline_source = inline_source or temporary_source
                if source is None and not temporary_source:
                    summary["missing_source"] += 1
                    output.write(json.dumps(result, ensure_ascii=False) + "\n")
                    continue
                if source is not None and not source.is_file():
                    summary["missing_source"] += 1
                    output.write(json.dumps(result, ensure_ascii=False) + "\n")
                    continue
                with tempfile.TemporaryDirectory(prefix="bmv2_compile_") as temporary:
                    temporary_path = Path(temporary)
                    if temporary_source:
                        source = temporary_path / (safe_name(record["record_id"]) + ".p4")
                        source.write_text(record.get("content") or "", encoding="utf-8")
                    json_output = temporary_path / "program.json"
                    p4info_output = temporary_path / "program.p4info.txt"
                    include_dirs = [Path(item).resolve() for item in args.include_path]
                    if args.auto_include:
                        if record.get("source_file"):
                            include_dirs = [source.parent.resolve(), repo_dir.resolve()] + include_dirs
                        elif inline_source:
                            if record["record_id"] not in inline_sources:
                                include_dirs = [repo_dir.resolve()] + include_dirs
                            else:
                                if record["source_repo_id"] not in repo_include_indexes:
                                    repo_include_indexes[record["source_repo_id"]] = build_repo_include_index(repo_dir)
                                include_dirs = auto_include_dirs(
                                    repo_dir,
                                    source,
                                    source.read_text(encoding="utf-8", errors="replace"),
                                    repo_include_indexes[record["source_repo_id"]],
                                ) + include_dirs
                        else:
                            if record["source_repo_id"] not in repo_include_indexes:
                                repo_include_indexes[record["source_repo_id"]] = build_repo_include_index(repo_dir)
                            include_dirs = auto_include_dirs(
                                repo_dir,
                                source,
                                source.read_text(encoding="utf-8", errors="replace"),
                                repo_include_indexes[record["source_repo_id"]],
                            ) + include_dirs
                    alias_cache = {}
                    compile_source = compiler_alias(
                        source, temporary_path, "source_", alias_cache
                    )
                    compiler_include_dirs = [
                        compiler_alias(path, temporary_path, "include_", alias_cache)
                        for path in include_dirs
                    ]
                    command = [compiler_path, "--p4v", "16"]
                    for include_dir in compiler_include_dirs:
                        command.extend(["-I", str(include_dir)])
                    command.extend([
                        "--p4runtime-files",
                        str(p4info_output),
                        "-o",
                        str(json_output),
                        str(compile_source),
                    ])
                    result["compile_command"] = command
                    result["include_paths"] = [str(path) for path in include_dirs]
                    if compile_source != source or compiler_include_dirs != include_dirs:
                        result["used_whitespace_safe_paths"] = True
                    try:
                        completed = subprocess.run(
                            command,
                            cwd=str(repo_dir),
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE,
                            text=True,
                            timeout=args.timeout,
                            check=False,
                        )
                    except subprocess.TimeoutExpired as exc:
                        summary["timeout"] += 1
                        summary["failed"] += 1
                        stderr = exc.stderr or ""
                        if isinstance(stderr, bytes):
                            stderr = stderr.decode("utf-8", errors="replace")
                        stderr = stderr.strip()
                        result.update({"status": "timeout", "returncode": None})
                    else:
                        stderr = completed.stderr.strip()
                        result["stdout"] = completed.stdout.strip() if completed.stdout.strip() else None
                        if completed.returncode == 0:
                            summary["compiled"] += 1
                            result.update({"status": "compiled", "returncode": 0})
                        else:
                            summary["failed"] += 1
                            result.update({"status": "compile_failed", "returncode": completed.returncode})
                    if stderr:
                        error_file = error_root / (safe_name(record["record_id"]) + ".stderr.txt")
                        error_file.write_text(stderr + "\n", encoding="utf-8")
                        try:
                            result["stderr_file"] = str(error_file.relative_to(output_path.parent))
                        except ValueError:
                            result["stderr_file"] = str(error_file)
                output.write(json.dumps(result, ensure_ascii=False) + "\n")
                if index % 50 == 0 or index == len(candidates):
                    print("[{}/{}] compiled={} failed={}".format(index, len(candidates), summary["compiled"], summary["failed"]), flush=True)
    finally:
        if inline_context_temp is not None:
            inline_context_temp.cleanup()

    summary["compile_rate_over_candidates"] = (
        summary["compiled"] / summary["candidate_count"] if summary["candidate_count"] else 0.0
    )
    if output_path.name == "bmv2_compile_results.jsonl":
        summary_path = output_path.with_name("bmv2_compile_summary.json")
    else:
        summary_path = output_path.with_name(output_path.stem + "_summary.json")
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
