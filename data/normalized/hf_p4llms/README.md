# p4llms/p4dataset 归一化

## 来源

原始数据位于 `data/raw/hf/p4dataset/`，对应 Hugging Face 数据集
[`p4llms/p4dataset`](https://huggingface.co/datasets/p4llms/p4dataset)。
该数据集的 `code.jsonl` 是 P4 代码集合，`code+text.jsonl` 是最终的代码加
P4 知识/上下文集合。后者包含 10,979 条代码记录和 1,269 条使用
`__dummy_name/__dummy_path` 标识的上下文记录。

## 统一输出

- `hf_p4llms_p4_code.jsonl`：最终 `code+text` 版本中的 P4 源码记录。
- `hf_p4llms_all_records.jsonl`：最终版本的全部记录，包括上下文。
- `hf_p4llms_accepted_records.jsonl`：去除空记录后的全部记录。
- `hf_p4llms_filtered_records.jsonl`：空记录或超限记录及过滤原因。
- `hf_p4llms_code.jsonl`：仅归一化原始 `code.jsonl` 版本。
- `hf_p4llms_code_text.jsonl`：归一化原始 `code+text.jsonl` 版本。
- `hf_p4llms_stats.json`：数量、版本、架构和过滤统计。

每条记录兼容 GitHub 归一化字段，并增加 `source_variant`、`native_id` 和
`source_dataset`。原始 `content` 同时保存在 `content`、`cleaned_content` 和
`raw_content` 中；原始 HF JSONL 未被修改。所有输出均使用 `hf_p4llms_` 前缀，
与 GitHub 和 SFU-CONNECT 数据隔离。

## 当前统计

- `code.jsonl`：10,979 条，过滤 1 条空记录，保留 10,978 条。
- `code+text.jsonl`：12,248 条，过滤 11 条空记录，保留 12,237 条。
- P4 源码正向集合：10,978 条。

该步骤只完成统一归一化和质量过滤；BMv2 编译预筛选应使用统一后的
`hf_p4llms_p4_code.jsonl` 单独运行，并将结果继续写在本目录。

## BMv2 编译结果

已使用本地 `p4c-bm2-ss` 对全部 P4_16 + v1model 候选执行检查：

- 候选：3,968 条；每条候选恰好出现一次。
- 初始编译成功：2,353 条（59.30%）。
- 失败或超时：1,615 条，其中超时 6 条。
- 严格正集：`hf_p4llms_positive_compile_clean.jsonl`，2,353 条，内部通过率定义为 100%。
- 失败分类：后端/目标能力限制 625、缺失依赖 294、源代码语法/类型失败 572、项目片段 35、目标不匹配 15、中间产物 37、预期负例 37。

结果、stderr 和分类文件分别为 `hf_p4llms_bmv2_compile_results.jsonl`、
`hf_p4llms_bmv2_compile_errors/`、`hf_p4llms_*failures.jsonl` 和
`hf_p4llms_bmv2_compile_failure_report.md`。p4llms 数据没有仓库工作树，
编译脚本会按 `source_repo_id/source_path` 临时重建每个仓库的源码/include
上下文；原始 HF 文件仍保持不变。

## Include 修复和最终 BMv2 切分

编译脚本会为含空格的原始路径建立临时无空格别名，避免旧版 p4c 调用
`cpp` 时拆分路径；`--auto-include` 只接受源码目录、仓库相对路径或唯一的
文件匹配，不会把其他项目的同名 `headers.p4` 混入。缺失的非 `.p4` 文件从
对应 GitHub 仓库恢复到独立的 `data/raw/hf/p4dataset_supplemental/` overlay，
下载 URL、失败请求和 SHA-256 保存在 `download_manifest.jsonl`，没有修改 HF
原始文件或归一化源码。

经过 5 轮依赖恢复和重新编译，294 个原始缺失依赖候选中新增 86 个通过。
将其覆盖回全量结果后，3,968 个候选的最终切分为：

- `hf_p4llms_bmv2_eligible.jsonl`：2,439 条，全部 `compile_status=compiled`、
  `returncode=0`，因此该正集通过率为 100%。
- `hf_p4llms_bmv2_excluded.jsonl`：828 条，包含预期负例、中间产物、非
  v1model/目标专用代码、BMv2 后端能力限制和不完整项目片段。
- `hf_p4llms_bmv2_repair_needed.jsonl`：701 条，保留缺失依赖（68）和源代码
  语法/类型失败（633），不伪造为正样本。
- `hf_p4llms_bmv2_repaired_results.jsonl`：覆盖后的 3,968 条完整审计结果；
  `hf_p4llms_repaired_*` 文件为对应分类。

重跑依赖恢复与切分：

```bash
python3 data/tools/recover_p4llms_dependencies.py --workers 48
python3 data/tools/compile_bmv2_candidates.py \
  --input data/normalized/hf_p4llms/hf_p4llms_missing_dependency_samples.jsonl \
  --context-input data/normalized/hf_p4llms/hf_p4llms_all_records.jsonl \
  --raw-root data/raw/hf/p4dataset \
  --supplemental-root data/raw/hf/p4dataset_supplemental \
  --auto-include --include-path /usr/local/share/p4c/p4include
```

重跑命令：

```bash
python3 data/tools/compile_bmv2_candidates.py \
  --input data/normalized/hf_p4llms/hf_p4llms_p4_code.jsonl \
  --context-input data/normalized/hf_p4llms/hf_p4llms_all_records.jsonl \
  --raw-root data/raw/hf/p4dataset \
  --output data/normalized/hf_p4llms/hf_p4llms_bmv2_compile_results.jsonl \
  --error-root data/normalized/hf_p4llms/hf_p4llms_bmv2_compile_errors \
  --auto-include --include-path /usr/local/share/p4c/p4include
```
