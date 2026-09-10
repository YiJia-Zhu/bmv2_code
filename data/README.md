# P4/BMv2 数据收集目录

## GHArchive-first / BigQuery-optional 项目级原始采集器

`tools/collect_gharchive_p4.py` 实现了隔离的、分阶段的项目级采集流程。它借鉴
The Stack v2 的仓库/commit 选择、完整 tree 标识和 repository-context 思路，但
不会把含 `.p4` 的项目缩小成 `.p4` 文件：完整 tree 进入 `tree_manifest`，P4
include 闭包、C/C++、Python、JSON/P4Info、控制面、测试和构建文件进入
`code_payload`。图片、视频、压缩包、数据集、缓存和明显生成输出可进入可选层，
但仍保留在 tree manifest 中。默认候选发现来自 GHArchive；BigQuery 发现逻辑
继续保留为可选的补充来源。GHArchive 事件不能单独证明仓库含有 P4，最终的
`.p4` 判断来自目标 commit 的完整 Git tree 和内容校验。

默认只显示帮助或生成本地查询模板，不会自动联网。网络操作必须显式调用以下子命令：

```bash
# 可选：只生成 BigQuery schema/query 模板，不联网
python3 data/tools/collect_gharchive_p4.py discover-bigquery \
  --output-root data/raw/gharchive_p4

# 下载指定时间段 GHArchive 小时文件（网络操作）
python3 data/tools/collect_gharchive_p4.py download-gharchive \
  --start-hour 2026-01-01-0 --end-hour 2026-01-01-1

# 从已下载的 GHArchive 文件提取仓库事件、候选和去重 PushEvent（离线）
python3 data/tools/collect_gharchive_p4.py index-gharchive \
  --event-root data/raw/gharchive_p4/events/gharchive

# 从已建立的 GHArchive 仓库事件索引生成候选（离线）
python3 data/tools/collect_gharchive_p4.py discover-gharchive \
  --repository-events data/raw/gharchive_p4/manifests/gharchive_repository_events.jsonl \
  --output-root data/raw/gharchive_p4

# 可选：生成候选时解析默认分支和最新 commit（网络操作）
python3 data/tools/collect_gharchive_p4.py discover-gharchive \
  --repository-events data/raw/gharchive_p4/manifests/gharchive_repository_events.jsonl \
  --resolve-commits \
  --output-root data/raw/gharchive_p4

# 枚举 p4lang 当前公开仓库，并查询每个默认分支最新 commit（网络操作）
python3 data/tools/collect_gharchive_p4.py discover-p4lang \
  --output-root data/raw/gharchive_p4

# 合并 GHArchive、BigQuery 和 p4lang 发现结果（离线；BigQuery 可省略）
python3 data/tools/collect_gharchive_p4.py merge-discovery \
  --input data/raw/gharchive_p4/discovery/gharchive_repositories.jsonl \
  --input data/raw/gharchive_p4/discovery/bigquery_p4_repositories.jsonl \
  --input data/raw/gharchive_p4/discovery/p4lang_repositories.jsonl

# 下载默认分支最新项目快照（网络操作）
python3 data/tools/collect_gharchive_p4.py collect-snapshots \
  --repositories data/raw/gharchive_p4/manifests/repositories.jsonl \
  --source-kind gharchive_events \
  --limit 4

# 可选：按 GHArchive PushEvent 采集历史 commit；默认每仓库最近 20 个
# （网络操作，主线最新快照不会被覆盖）
python3 data/tools/collect_gharchive_p4.py collect-history \
  --push-events data/raw/gharchive_p4/manifests/push_events.jsonl \
  --repositories data/raw/gharchive_p4/manifests/repositories.jsonl \
  --mode window --max-per-repository 20

# 根据已采集快照离线生成按项目组织的轻量 repository-context 视图
python3 data/tools/collect_gharchive_p4.py build-context \
  --snapshots data/raw/gharchive_p4/manifests/snapshots.jsonl
```

采集器使用标准库 HTTP 客户端，不把 token 写入日志；GitHub token 从
`GITHUB_TOKEN` 或 `--token` 读取，BigQuery token 从 `BIGQUERY_ACCESS_TOKEN`
或 gcloud application-default credentials 读取。运行全量查询前必须设置
`--execute`、billing project 和 `--max-bytes-billed`，并检查生成的 dry-run 报告。
GHArchive 候选记录的 `contains_p4` 保持为 `null`、`p4_verification_status` 为
`unverified`；快照完成后，`snapshots.jsonl` 会写入
`snapshot_present_readable`、`snapshot_present_unreadable` 或
`snapshot_absent`。因此发现来源和 P4 内容验证不会混在同一个字段里。
完整 tree 记录在 `manifests/trees/`，项目快照在 `snapshots/`，文件级索引和筛选
标记在 `manifests/files.jsonl` 与 `file_filter_annotations.jsonl`。

离线测试：

```bash
python3 data/tools/test_collect_gharchive_p4.py
```

本目录保留两层数据：

- `raw/repositories/`：按仓库保存的原始浅克隆快照，不在归一化过程中修改。
- `normalized/`：由 `tools/normalize_dataset.py` 生成的统一 JSONL 数据。

当前首轮收集结果：21 个仓库成功完成原始快照，`p4lang/p4c`、BMv2、tutorials、
P4Runtime、PI、open-p4studio、p4-guide、p4app 及相关应用仓库均已纳入；
`nsg-ethz/p4-learning` 已由用户提供完整快照并登记为 `downloaded_manual`。此前
失败尝试留下的两个部分压缩包仍保存在 `raw/archives/*.partial*`，仅作下载审计，
不会进入归一化统计。下载清单是追加式审计日志 `raw/collection_manifest.jsonl`；当前快照汇总见
`raw/inventory.jsonl` 和 `raw/inventory_summary.json`。

仓库清单在 `config/repositories.json`。每个成功下载的仓库目录包含：

- `collection.json`：仓库、分支、commit、许可证角色、代理和下载时间；
- `DOWNLOAD_SOURCE.json`：原始下载方法；
- `.git/` 和工作树：用于追溯原始快照。

## 收集原始仓库

默认使用本机 Clash HTTP 代理 `127.0.0.1:7890`，可改为直连：

```bash
python3 data/tools/collect_repositories.py
python3 data/tools/collect_repositories.py --direct
```

只收集指定仓库：

```bash
python3 data/tools/collect_repositories.py \
  --repo p4lang__p4c \
  --repo p4lang__behavioral-model
```

## 归一化

```bash
python3 data/tools/normalize_dataset.py
```

主要输出：

- `normalized/p4_code.jsonl`：去除空文件、明显非 P4、自动生成和超大文件后的唯一 P4 记录；
- `normalized/accepted_records.jsonl`：P4、STF、控制面、文档和支持文件；
- `normalized/all_records.jsonl`：接受记录及重复记录；
- `normalized/filtered_records.jsonl`：每个被过滤文件及原因；
- `normalized/stats.json`：数量、P4 版本、架构和仓库统计。

可选的 BMv2 编译预筛选：

```bash
python3 data/tools/compile_bmv2_candidates.py --limit 50
python3 data/tools/compile_bmv2_candidates.py --auto-include --include-path /usr/include
```

结果写入 `normalized/bmv2_compile_results.jsonl`、
`normalized/bmv2_compile_summary.json` 和 `normalized/bmv2_compile_errors/`；
这一步只判断 `p4c-bm2-ss` 编译，不代替隐藏流量测试。

编译结果分类和 100% clean 子集：

```bash
python3 data/tools/classify_bmv2_results.py
```

`normalized/positive_compile_clean.jsonl` 只包含返回码为 0 的样例，因此该
子集的编译通过率定义为 100%。`negative_compile_examples.jsonl` 保留有意的
错误回归样例；`unsupported_or_target_specific.jsonl`、
`incomplete_project_samples.jsonl` 和 `source_compile_failures.jsonl` 分别保留
目标不匹配、项目片段/缺依赖和真实语法/类型失败。原始源码和每次编译的 stderr
始终保留，不通过修改源码来提高通过率。分类汇总见
`normalized/bmv2_compile_classification_summary.json`。

## Hugging Face P4 数据集

`raw/hf/SFU_p4_dataset/` 对应 HF 数据集 `SFU-CONNECT/p4_dataset`，是独立的
SFU 数据集快照。它使用类似 Stack v2 的 `raw-repositories/...` 路径约定，
但不是 StarCoder2/Stack v2 全量训练数据，也不是 `p4llms/p4dataset` 的同一
个发布物。归一化和 BMv2 检查不会写入 GitHub 输出：

```bash
python3 data/tools/normalize_hf_dataset.py
python3 data/tools/compile_bmv2_candidates.py \
  --input normalized/hf/hf_p4_code.jsonl \
  --raw-root raw/hf/SFU_p4_dataset \
  --output normalized/hf/hf_bmv2_compile_results.jsonl \
  --error-root normalized/hf/hf_bmv2_compile_errors \
  --auto-include --include-path /usr/local/share/p4c/p4include
python3 data/tools/classify_bmv2_results.py \
  --results normalized/hf/hf_bmv2_compile_results.jsonl \
  --source normalized/hf/hf_p4_code.jsonl \
  --output-root normalized/hf --prefix hf_
```

HF 的 `content` 使用清洗后的 P4，`raw_content` 保留原始文本；`hf_` 前缀
保证产物不会与 GitHub 数据混用。当前筛选条件为 P4_16 + v1model。

`raw/hf/p4dataset/`（HF: `p4llms/p4dataset`）也已单独归一化到
`normalized/hf_p4llms/`，全部产物使用 `hf_p4llms_` 前缀。其最终
`code+text.jsonl` 版本包含 P4 源码和 P4 知识上下文；P4-only 文件为
`normalized/hf_p4llms/hf_p4llms_p4_code.jsonl`。初始 BMv2 编译预筛选为
3,968 个 P4_16/v1model 候选中 2,353 个成功（59.30%）。随后恢复了独立
GitHub supplemental include 上下文并重试，最终 `hf_p4llms_bmv2_eligible.jsonl`
包含 2,439 条且内部通过率为 100%；828 条不适合 BMv2 的样例进入
`hf_p4llms_bmv2_excluded.jsonl`，701 条源依赖/源码问题进入
`hf_p4llms_bmv2_repair_needed.jsonl`。完整结果、stderr、下载 manifest 和
失败分类见 `normalized/hf_p4llms/`，不与 SFU 候选合并统计。

重新生成原始快照 inventory：

```bash
python3 data/tools/build_inventory.py
```

每条记录包含原仓库、commit、相对路径、许可证提示、SHA-256、P4 版本/架构提示和文件内容。归一化规则参考 `ref_paper/Generating_P4_Dataplanes_Using_LLMs.pdf` 与 `ref_paper/starcoder2.pdf`；P4-14 记录会保留并明确标记，不会混入 P4_16 统计。

本轮归一化得到 37,068 个唯一内容记录，其中 2,751 个 P4_16 候选。BMv2
预筛选基线为 964 个 v1model 提示候选；补上仓库内 include 和 `/usr/include`
后，705 个返回码为 0（73.13%）。这个比例是源样例预筛选统计，不是模型生成
代码的最终 benchmark 结果；后续模型评测应使用 `positive_compile_clean.jsonl`。
