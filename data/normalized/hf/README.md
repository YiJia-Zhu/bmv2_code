# HF P4 数据集归一化与 BMv2 检查

## 来源关系

`data/raw/hf/SFU_p4_dataset` 对应 Hugging Face 数据集
`SFU-CONNECT/p4_dataset`，是独立上传的 405 条 P4 数据。它与本地
`data/raw/hf/p4dataset/`（对应 `p4llms/p4dataset`）不是同一个 HF 数据集。
SFU 数据使用了类似 Stack v2 的 `raw-repositories/<owner>__<repo>/<path>`
路径表示，但本地没有它的构建脚本，不能仅凭路径断定它是 p4llms 数据集的下游。

`ref_paper/starcoder2.pdf` 介绍的是 StarCoder2 和 The Stack v2：Stack v2 是
StarCoder2 训练数据体系的重要代码来源，由 Software Heritage 等来源构建。
因此 SFU 数据最多只能说使用了相似的仓库路径约定；它不是 StarCoder2 官方
训练集，也不等同于 Stack v2 全量数据。

## 生成与编译

```bash
python3 data/tools/normalize_hf_dataset.py
python3 data/tools/compile_bmv2_candidates.py \
  --input data/normalized/hf/hf_p4_code.jsonl \
  --raw-root data/raw/hf/SFU_p4_dataset \
  --output data/normalized/hf/hf_bmv2_compile_results.jsonl \
  --error-root data/normalized/hf/hf_bmv2_compile_errors \
  --auto-include --include-path /usr/local/share/p4c/p4include
python3 data/tools/classify_bmv2_results.py \
  --results data/normalized/hf/hf_bmv2_compile_results.jsonl \
  --source data/normalized/hf/hf_p4_code.jsonl \
  --output-root data/normalized/hf --prefix hf_
```

归一化后的 `content` 使用 HF 的 `cleaned_p4`，同时保留 `raw_content`；
`source_file` 指向原始下载目录中的清洗文件。所有 HF 产物使用 `hf_` 前缀，
与 `data/normalized/` 下的 GitHub 产物隔离。HF 数据集自带的 `compiles` 字段
只作为元数据；BMv2 通过率以本机 `p4c-bm2-ss` 返回码为准。

当前检查选择 `language=p4_16` 且 `architecture=v1model` 的记录；需要完整
仓库上下文的 include 片段仍会被保留并分类，不会修改原始源码来提高通过率。

## 本次结果

- 原始 HF 记录：405；其中数据集自带 `compiles=True`：405。
- BMv2 候选：396（排除 8 条版本未知、1 条架构未知记录）。
- 本机 `p4c-bm2-ss` 编译成功：381/396（96.21%）。
- 失败：15（后端/能力限制 8、项目片段 3、语法/类型 4）。
- `hf_positive_compile_clean.jsonl` 的 381 条记录全部返回码为 0，因此该
  严格正集内部通过率为 100%；96.21% 才是原始候选集通过率。

完整 stderr 和失败分组见 `hf_bmv2_compile_errors/`、
`hf_bmv2_compile_failure_report.md` 及各个 `hf_*_failures.jsonl` 文件。
