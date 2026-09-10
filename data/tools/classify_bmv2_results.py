#!/usr/bin/env python3
"""Split BMv2 compile results into auditable, disjoint dataset subsets.

The source snapshots and the original compiler logs are never changed.  A
``positive_compile_clean`` record is included only when the recorded
``p4c-bm2-ss`` process returned zero.  Failed records are retained and
classified so that an intended compiler-error test is not confused with a
missing include or a real source defect.
"""

import argparse
import datetime as dt
import json
import re
from collections import Counter
from pathlib import Path


DATA_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_RESULTS = DATA_ROOT / "normalized" / "bmv2_compile_results.jsonl"
DEFAULT_SOURCE = DATA_ROOT / "normalized" / "p4_code.jsonl"
DEFAULT_OUTPUT = DATA_ROOT / "normalized"


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--results", type=Path, default=DEFAULT_RESULTS)
    parser.add_argument("--source", type=Path, default=DEFAULT_SOURCE)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument(
        "--prefix",
        default="",
        help="prefix every generated artifact (for example: hf_)",
    )
    return parser.parse_args()


def read_jsonl(path):
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                yield json.loads(line)


def read_stderr(result, results_root):
    relative = result.get("stderr_file")
    if not relative:
        return ""
    path = results_root / relative
    if not path.is_file():
        # Result files may be moved from a versioned staging directory to the
        # canonical ``bmv2_compile_errors`` directory after a run.  Preserve
        # compatibility with those recorded relative paths.
        path = results_root / "bmv2_compile_errors" / Path(relative).name
    return path.read_text(encoding="utf-8", errors="replace") if path.is_file() else ""


def is_error_seed(record):
    path = record.get("source_path", "").lower()
    role = record.get("source_role")
    if role == "compiler_error_seed":
        return True
    if any(token in path for token in ("p4_16_errors/", "p4_16_bmv_errors/")):
        return True
    return bool(re.search(r"(^|/)(err|error)(s)?([/_-]|$)", path))


def is_intermediate(record):
    path = record.get("source_path", "")
    lowered = path.lower()
    return (
        "/tmp/" in lowered
        or "/out/" in lowered
        or bool(re.search(r"(?:front|mid|back)end[_-]\d+", lowered))
        or "frontendl" in lowered
        or "midendl" in lowered
    )


def is_target_specific(record, stderr):
    path = record.get("source_path", "").lower()
    content = record.get("content", "").lower()
    path_tokens = re.compile(r"(^|/)(tofino|dpdk|ebpf|xilinx)(/|_|-|$)")
    source_tokens = (
        "#include <psa.p4>",
        "#include \"psa.p4\"",
        "package psa_switch",
        "#include <pna.p4>",
        "#include \"pna.p4\"",
        "#include <tna.p4>",
        "#include \"tna.p4\"",
        "package pna",
        "package tna",
    )
    if bool(path_tokens.search(path)) or any(token in content for token in source_tokens):
        return True
    # Directory names such as psa-examples and DASH/SONIC project trees are
    # not sufficient evidence by themselves: several of their files are
    # valid v1model programs.  Treat them as target-specific only when the
    # source does not declare v1model and is not a complete main program.
    has_v1model = "v1model.p4" in content or "v1switch" in content
    project_path = re.search(r"(^|/)(psa|pna|tna|dash|sonic|ipdk)(/|_|-|$)", path)
    return bool(project_path and not has_v1model and not re.search(r"\bmain\s*;", content))


def is_missing_dependency(stderr):
    lowered = stderr.lower()
    return "no such file or directory" in lowered and (
        "fatal error" in lowered or "include" in lowered
    )


def is_compiler_limitation(stderr):
    lowered = stderr.lower()
    tokens = (
        "target model error",
        "target-error",
        "unknown extern",
        "unknown extern method",
        "not supported",
        "unsupported",
        "wrong model",
        "program is not supported",
        "could not generate p4info",
    )
    return any(token in lowered for token in tokens)


def is_fragment(record, stderr):
    path = record.get("source_path", "").lower()
    lowered = stderr.lower()
    if any(token in path for token in ("/include/", "/fixed/", "/instantiations/")):
        return True
    if re.search(r"(^|/)(dash|sonic|ipdk)(/|_|-|$)", path) and not re.search(
        r"\bmain\s*;", record.get("content", "")
    ):
        return True
    if "declaration not found" in lowered and not re.search(r"\bmain\s*;", record.get("content", "")):
        return True
    if "unexpected identifier \"metadata_t\"" in lowered:
        return True
    return False


def classify(record, result, stderr):
    """Return (class, reason) in priority order."""
    if result.get("status") == "compiled":
        return "positive_compile_clean", "p4c-bm2-ss returned zero"
    if is_error_seed(record):
        return "intended_negative", "path or source role identifies an error regression"
    if is_intermediate(record):
        return "intermediate_artifact", "compiler intermediate/output artifact"
    if is_missing_dependency(stderr):
        return "missing_dependency", "included file is absent from the raw snapshot or include path"
    if is_target_specific(record, stderr):
        return "target_specific_or_arch_mismatch", "PSA/PNA/TNA/DASH or another non-v1model target"
    if is_compiler_limitation(stderr):
        return "compiler_or_target_limitation", "BMv2 backend or installed p4c rejects this feature"
    if is_fragment(record, stderr):
        return "incomplete_project_sample", "file is a project fragment or requires a wrapper"
    return "source_syntax_or_type_error", "standalone source failed syntax/type checking"


def main():
    args = parse_args()
    if not args.results.is_file() or not args.source.is_file():
        raise SystemExit("results and source JSONL files must exist")
    source_records = {record["record_id"]: record for record in read_jsonl(args.source)}
    results = list(read_jsonl(args.results))
    args.output_root.mkdir(parents=True, exist_ok=True)
    output_names = {
        "positive_compile_clean": args.prefix + "positive_compile_clean.jsonl",
        "intended_negative": args.prefix + "negative_compile_examples.jsonl",
        "intermediate_artifact": args.prefix + "intermediate_artifacts.jsonl",
        "missing_dependency": args.prefix + "missing_dependency_samples.jsonl",
        "target_specific_or_arch_mismatch": args.prefix + "unsupported_or_target_specific.jsonl",
        "compiler_or_target_limitation": args.prefix + "compiler_or_target_failures.jsonl",
        "incomplete_project_sample": args.prefix + "incomplete_project_samples.jsonl",
        "source_syntax_or_type_error": args.prefix + "source_compile_failures.jsonl",
    }
    handles = {}
    examples = {}
    try:
        for name in output_names.values():
            handles[name] = (args.output_root / name).open("w", encoding="utf-8")
        counts = Counter()
        results_root = args.results.parent
        for result in results:
            record = source_records.get(result.get("record_id"), {})
            merged = dict(record)
            merged.update(result)
            stderr = read_stderr(result, results_root)
            category, reason = classify(record, result, stderr)
            merged.update(
                {
                    "dataset_split": category,
                    "compile_status": result.get("status"),
                    "failure_class": None if category == "positive_compile_clean" else category,
                    "classification_reason": reason,
                    "is_intended_negative": category == "intended_negative",
                    "is_compile_clean": category == "positive_compile_clean",
                }
            )
            if stderr and category != "positive_compile_clean":
                merged["stderr_excerpt"] = stderr[:1200]
            output_name = output_names[category]
            handles[output_name].write(json.dumps(merged, ensure_ascii=False) + "\n")
            counts[category] += 1
            examples.setdefault(category, []).append(
                "{}:{}".format(record.get("source_repo_id", ""), record.get("source_path", ""))
            )
    finally:
        for handle in handles.values():
            handle.close()

    total = len(results)
    clean = counts["positive_compile_clean"]
    summary = {
        "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "results": str(args.results),
        "source": str(args.source),
        "prefix": args.prefix,
        "candidate_count": total,
        "positive_compile_clean": clean,
        "positive_compile_clean_rate": clean / total if total else 0.0,
        "failure_count": total - clean,
        "by_split": dict(sorted(counts.items())),
        "definition_of_100_percent": "Every record in {} has compile_status=compiled and returncode=0.".format(
            args.prefix + "positive_compile_clean.jsonl"
        ),
    }
    summary_path = args.output_root / (args.prefix + "bmv2_compile_classification_summary.json")
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    labels = {
        "positive_compile_clean": "编译成功（正样本）",
        "intended_negative": "错误回归样例（预期失败）",
        "intermediate_artifact": "编译中间产物",
        "missing_dependency": "缺失 include/生成依赖",
        "target_specific_or_arch_mismatch": "非 v1model / 目标不匹配",
        "compiler_or_target_limitation": "BMv2 后端或 p4c 能力限制",
        "incomplete_project_sample": "项目片段/缺少 wrapper",
        "source_syntax_or_type_error": "源代码语法/类型失败",
    }
    report_lines = [
        "# BMv2 编译失败分析",
        "",
        "本报告由 `classify_bmv2_results.py` 根据 `p4c-bm2-ss` 返回码、原始 stderr、源码路径和归一化角色生成。原始仓库和日志不修改。",
        "",
        "## 总览",
        "",
        "- 候选：{}".format(total),
        "- 编译成功：{}（原始候选通过率 {:.2%}）".format(clean, clean / total if total else 0.0),
        "- 失败：{}".format(total - clean),
        "- `{}positive_compile_clean.jsonl`：{} 条，定义上为 100% 返回码 0.".format(args.prefix, clean),
        "",
        "## 失败分类",
        "",
        "| 分类 | 数量 | 处理方式 |",
        "|---|---:|---|",
        "| 错误回归样例 | {} | 保留为负例，不混入正集 |".format(counts["intended_negative"]),
        "| 编译中间产物 | {} | 排除，避免重复计算 |".format(counts["intermediate_artifact"]),
        "| 缺失 include/生成依赖 | {} | 补 include；无法重建的单独保留 |".format(counts["missing_dependency"]),
        "| 非 v1model/目标不匹配 | {} | 移到 target-specific 集合 |".format(counts["target_specific_or_arch_mismatch"]),
        "| BMv2/p4c 能力限制 | {} | 记录为后端限制，不改源码 |".format(counts["compiler_or_target_limitation"]),
        "| 项目片段 | {} | 需要 wrapper 或完整构建上下文 |".format(counts["incomplete_project_sample"]),
        "| 语法/类型失败 | {} | 作为待修复源样例保留 |".format(counts["source_syntax_or_type_error"]),
        "",
        "## 代表性样例",
        "",
    ]
    for category in output_names:
        if category == "positive_compile_clean" or not examples.get(category):
            continue
        report_lines.append("### {}（{}）".format(labels.get(category, category), counts[category]))
        for example in examples[category][:3]:
            report_lines.append("- `{}`".format(example))
        report_lines.append("")
    report_lines.extend(
        [
            "## 结论",
            "",
            "不能要求原始仓库中的所有 `.p4` 文件都 100% 编译：其中包含故意报错的 p4c 回归测试、目标专用代码、只在构建系统中使用的 include 片段和编译器中间文件。后续 BMv2/LLM 通过率应以 `positive_compile_clean.jsonl` 为分母；负例和不完整项目继续保留用于数据审计和错误修复任务。",
            "",
        ]
    )
    (args.output_root / (args.prefix + "bmv2_compile_failure_report.md")).write_text(
        "\n".join(report_lines), encoding="utf-8"
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
