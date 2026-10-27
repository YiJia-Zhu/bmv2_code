# P4/BMv2 数据收集目录

## P4 仓库级原始采集：A/B 双路线

当前目标是尽可能全面地收集曾经有一个 revision 至少含有一个 P4_16 `.p4` 文件的
GitHub 仓库。文件名 `.p4` 只负责发现候选，P4_16 内容判定决定仓库是否准入；
命中后仍保存整个仓库，而不是只保存 P4 文件。发现阶段保留所有候选 revision，
物化阶段默认选择每个 origin 最新的 P4_16 revision。主方案见
`../SWH_P4_仓库级原始数据收集方案.md`：用 Software Heritage graph/export
先从全局文件路径中找到精确 `.p4` 候选，再映射回
 `content -> directory -> revision -> snapshot -> origin`（历史 commit 还需
`revision_parent` 可达关系）；计划在 AWS 远程查询，本机下载结果分片，体量需统计。
随后下载候选 `.p4`
内容做 P4_16 判定。只有一个 revision 中至少有一个 `.p4` 文件被判定为 P4_16，
才保存该 revision 的完整仓库树；`.p4` 文件占比低不参与过滤。

当前分成两条路线：

- A 方案（最终高召回主线）：SWH Graph 全量扫描。由于本机读取 S3 的实测吞吐远低于
  50--100 MB/s，生产扫描改为 AWS Athena + Glue/EMR Serverless；AWS 结果桶已经准备，
  128 个 `directory_entry` ORC 的 `.p4` 文件名扫描已于 2026-10-16 完成，下一阶段是
  目录、revision、snapshot 和 origin 图映射。`tools/swh_glue_graph_mapper.py` 先输出
  `.p4` 文件与仓库 URL 的关系及去重 URL 清单；P4_16 判定留到随后内容读取阶段。
  该映射已通过本地 Spark 小图测试，尚未在完整 Graph 上运行；
- B 方案（独立补充）：BigQuery `github_repos.files` 发现 `.p4` 路径，GHArchive 和
  已知组织补充仓库名，随后检查 GitHub 默认分支最新 commit。先读取 `.p4` 严格确认
  P4_16，通过后才下载整个项目。

B 方案不能覆盖所有只存在于 SWH 的已删除仓库，但其结果可在 A 完成后按 tree hash
去重并保留 provenance。B 的独立目录是 `pipelines/bigquery_gharchive_p4/`；后续先读
其中的 `pipeline.json` 获取执行状态，不需要解析 A 方案文档。

官方 graph dataset 截止信息：截至 2026-10-09，官方列出的最新完整快照为
`2026-06-04`，上一版为 `2026-03-02`。详见
<https://docs.softwareheritage.org/devel/swh-export/graph/dataset.html>。
`2023-09-06` 是 StarCoder2 论文使用的旧快照，不是当前 graph 的截止时间。

完整 Graph 不需要全部下载到本机。SWH 将 ORC 表放在 S3，例如
`s3://softwareheritage/graph/2026-06-04/orc`；2026-06-04 完整 ORC 约 33 TiB。
推荐在 S3 上用 Athena/Spark 远程筛选候选 revision，再把候选清单、目录索引
和最终命中仓库保存到本地。用户的 `s3://swh2p4/athena-results/` 是查询结果桶，
不是 SWH 源码桶；Graph 元数据映射完成后，本机下载候选表。现有程序支持 SWH
内容 API/本地缓存；A 方案计划用 GitHub exact commit 加速完整项目下载，失败时
回退到 SWH 内容。已验证公共桶 `s3://softwareheritage/content/<sha1>` 可匿名
读取 gzip 压缩的正文；Graph content ORC 的 `sha1_git`/`sha1` 列可用于批量
映射；单分片映射与 S3 正文筛查已试跑，全量尚未运行。SWH 没有现成的“只含 2017 年以后 revision”的完整
Graph；`revision.date` 过滤可以减少候选和内容下载，但不能保证按比例减少目录
扫描。P4-16 稳定规范标记对应 2017-05-23；高召回模式默认不做年份硬过滤，
仍必须读取 `.p4` 内容确认版本。
官方约 2 TiB 的 `history and hosting` 子图可以先帮助筛选 origin、snapshot 和
revision，但没有大部分嵌套目录，不能单独完成 `.p4` 文件名扫描。

来源职责按路线区分：

- SWH：主发现、主溯源、完整 tree/revision/origin 关系；
- GitHub API：只做 GitHub origin 的元数据补充、当前可访问性检查和可选下载加速；
- GHArchive：在 A 中是辅助元数据；在 B 中还提供当前/历史仓库名候选，但不直接证明含 P4；
- BigQuery：是 B 的主文件名索引和 A 的交叉召回来源，不替代 SWH。

建议新的 raw 根目录为 `data/raw/swh_p4/`。GHArchive/BigQuery/GitHub
产物如果继续保留，应写入独立的辅助 manifest，不能让事件流或搜索结果替代
SWH 的源码图和 P4_16 本地判定。

### SWH 图导出采集器

`tools/collect_swh_p4.py` 是仓库级主采集器，按 StarCoder2/The Stack v2 的
口径工作，但把仓库准入条件改为“代表 revision 的完整 tree 中至少有一个 `.p4`
路径被判定为 P4_16”。因此 P4 文件只占仓库 1% 时，README、构建文件、控制面代码
和其他 tracked 路径仍然会写入完整 `tree_manifest`；不会按 P4 占比过滤。

采集器需要由 SWH graph/export 或 BigQuery 预先生成的两个表。它负责目录递归和
内容判定，不负责自动下载 33 TiB 的官方 Graph：

- `--candidates`：每行至少包含 `origin_url`、`revision_id`、`directory_id`、
  `branch_name`，可选 `github_default_branch`、`visit_date`、`snapshot_id`；
- `--directory-entries`：每行至少包含 `directory_id`、`name`、`type`、`target`，
  可选 `length`、`perms`、`checksums`。

两者可以是 JSONL、CSV、TSV，也可以 gzip 压缩。候选表应先完成
`origin -> snapshot -> branch/历史关系 -> revision -> root directory` 的
图关系连接；现有脚本按配置选择候选，并把 directory_entry
索引到 SQLite 后递归展开到 64 层。REST API 不适合作为全局候选发现入口。

为最大召回，发现阶段应保留所有可映射的候选 revision；物化阶段推荐选择每个
origin 最新的 P4_16 revision。`--selection-policy latest-main` 仍保留，作为与
StarCoder2 论文一致的当前默认分支视图；`latest-any` 选择最新任意分支，`all`
保留/物化全部候选 revision。`latest-main` 可能漏掉历史版本或仅 feature 分支含
`.p4` 的仓库，不能作为高召回主结果。

推荐的 `latest-p4-16` 仍是待实现设计，当前 CLI 没有这个选项。B 现有代码也仍
只检查当前 GitHub 默认分支，不代表已经实现历史全覆盖。三篇论文方法与常见
case 见 [主方案](../SWH_P4_仓库级原始数据收集方案.md) 和
[逐步示例](../SWH_P4_仓库发现逻辑示例.md)。

候选表的 SQL 起点见 `tools/swh_graph_candidates.sql`。它是按论文流程写的模板，
需要把 `GRAPH_DATASET` 替换为你实际的 SWH graph export 前缀，并按该版本的列名
调整。这个文件使用 BigQuery 风格语法且过滤最新主线/日期；它不能直接在 Athena
执行，也不是当前高召回反向映射的已完成实现。

完整执行顺序是：

```text
S3/Graph 远程查询
  -> .p4 文件名命中
  -> 目录祖先 + revision 历史 + GitHub origin 映射，保留 candidates
  -> 按 content_id 去重获取 .p4，按需取 include，判断 P4_16
  -> 每个 origin 选择最新的已确认 P4_16 revision
  -> 展开完整 tree，保存整个所选项目
```

实现状态说明：当前 `collect_swh_p4.py` 已完成候选 revision 选择、目录递归、
`.p4` 内容读取、正则 P4_16 gate、tree manifest 和候选 checkpoint。默认
`--repository-gate p4_16`：先只读取候选 `.p4`，只有至少一个文件判定为 P4_16，
才按 `--content-mode` 下载并物化其他文件；P4_14/未知版本进入
`rejected_repositories.jsonl`，不会触发完整仓库下载。

### 双模式 S3/Athena 发现入口

目标设计是按 128 个 `directory_entry` ORC 对象分片；“分片”只是让每个 S3 对象成为
一个独立查询任务，不把它下载到本机。任务 manifest 至少记录 `source_key`、ETag、
对象大小、状态、Athena query ID、结果 key、实际扫描字节数和命中行数。恢复时只运行
`pending`/可重试 `failed`，核验结果后跳过 `complete`。对保存了 query ID 的
running/已成功查询先查状态或下载结果，不能直接重提。换账号须保留本地结果/清单
并处理旧桶权限；不能续接另一账号中间的一条查询。

示例（演示值，非真实结果）：

```json
{"source_key":"graph/2026-06-04/orc/directory_entry/directory_entry-001.orc","status":"complete","query_execution_id":"abc123","bytes_scanned":198765432100,"result_key":"athena-results/p4-hits/part-0001.csv"}
```

Athena 分片调度由 `tools/run_swh_athena_p4.py` 实现。2026-10-16 已验证 `$path`
单对象过滤并完成全部 128 个对象：总扫描 10,509,202,350,395 bytes，稳定 CSV
结果 3,366,228,106 bytes。`swh-p4-pilot` 工作组强制 SSE-S3，并设置每查询
400,000,000,000 bytes 扫描上限。运行或恢复命令：

```bash
python3 data/tools/run_swh_athena_p4.py --concurrency 8 --resume
```

`tools/discover_swh_p4.py` 把两种发现方式规范化为同一组 manifest：

- `local-s3`：本机通过 HTTP Range 读取 SWH `directory_entry` ORC，筛选
  `type=file` 且文件名以 `.p4` 结尾；按 ORC 对象保存分片和 checkpoint；
- `athena-import`：导入从 Athena 下载的 CSV/TSV/JSONL，输出相同的
  `p4_directory_hits.jsonl`；如果输入同时有仓库映射字段，还输出
  `swh_candidate_revisions.jsonl`。

安装本地 ORC 依赖：

```bash
python3 -m pip install -r data/tools/requirements-swh-s3.txt
```

本地只扫描一个 ORC 对象的前 100,000 行做冒烟测试：

```bash
python3 data/tools/discover_swh_p4.py local-s3 \
  --proxy http://127.0.0.1:7892 \
  --max-objects 1 \
  --max-rows-per-object 100000
```

未指定 `--max-objects` 时，必须显式加入 `--confirm-full-scan`，避免误启动约
27 TiB 的本机目录表扫描。`local_s3_scan_state.json` 只将完整读完且 ETag/大小
未变化的对象视为可跳过；限制行数的对象标记为 `partial`，正式运行仍会重扫。

Athena 的 `.p4` 命中 SQL 模板是 `tools/swh_athena_p4_hits.sql`。下载结果后：

```bash
python3 data/tools/discover_swh_p4.py athena-import \
  --input /path/to/athena-result.csv \
  --require-mapped-repositories
```

这一步成功只表示 `candidate_manifest_ready=true`。若 Athena/Spark 也导出了候选根
目录的完整目录闭包，可同时传入：

```bash
--directory-entries /path/to/candidate_directory_entries.jsonl
```

此时摘要才会标记 `collector_ready=true`。

Athena 只有以下四个字段时，只完成了文件命中，还不是仓库列表：

```text
directory_id,name,type,content_id
```

要让导入结果可交给仓库采集器，每个仓库还必须有：

```text
origin_url,revision_id,root_directory_id
```

可选字段是 `branch_name`、`visit_date`、`snapshot_id`、
`github_default_branch`。脚本不会错误地把 `.p4` 的直接父目录当成 revision 根目录；
命中行同时存在时必须明确提供 `root_directory_id`。

最后，SWH 方式物化完整仓库仍需提供这些候选根目录的完整 `directory_entry` 闭包：

```bash
python3 data/tools/collect_swh_p4.py \
  --candidates data/raw/swh_p4/discovery/swh_candidate_revisions.jsonl \
  --directory-entries /path/to/candidate_directory_entries.jsonl \
  --repository-gate p4_16 \
  --content-mode all --materialize
```

因此当前边界是：两个输入模式、`.p4` 命中规范化、Spark 图映射、选中 revision
的完整 tree 导出、正则 gate 和文件物化代码已经存在。Spark 作业必须先通过实际
Graph release 的 schema smoke test，并将 `swh_glue_tree_export.py` 输出的
`tree_entries_json/` 目录索引交给本地 collector；本机对每个目录逐个查询远程 ORC
不能替代 Spark 的批量映射。

只做索引和候选检查：

```bash
python3 data/tools/collect_swh_p4.py \
  --candidates /path/to/swh_candidates.jsonl \
  --directory-entries /path/to/directory_entries.jsonl \
  --dry-run
```

使用本地 SWH content 镜像检查 `.p4`，并保存完整树清单：

```bash
python3 data/tools/collect_swh_p4.py \
  --candidates /path/to/swh_candidates.jsonl \
  --directory-entries /path/to/directory_entries.jsonl \
  --content-root /path/to/content-mirror \
  --content-mode all --materialize \
  --request-delay 0.5
```

`content-mode=all --materialize` 才会把可读取的所有文件写入
`snapshots/swh_latest/`；默认 `content-mode=p4` 只获取 `.p4` 内容用于判定，
但无论哪种模式都会保存完整目录树和每个文件的 provenance。只有判定为 P4_16 的
候选才进入完整仓库物化。已完成的候选会从
现有 manifest 中跳过。候选级 checkpoint 不等于可靠的文件级恢复：内容失败/未知
可能已被记录为候选完成，完整性核验与这些失败的重试仍需补齐。

当前 `--max-file-bytes` 默认 10 MiB，超限只留 metadata；目录默认最多 64 层。这
不能保证完整项目。StarCoder2 限制的是压缩文件大小，也不是同一种计量。目标完整
模式必须记录全部 tracked 文件及权限/链接，缺失或截断标记 partial；轻量模式才
允许主动跳过。全量运行时间主要由 graph export
读取、目录索引大小、候选仓库数量和 content 下载量决定：小样本（20--50 个仓库）
通常是几分钟到数小时；如果通过 REST 逐文件下载全量 GitHub，可能需要数周且不适合
该目标。内容 S3 是否可访问和 Graph 映射成本仍待核验。

P4_16 语言身份与 BMv2/v1model 编译成功分开记录。采用其他架构、缺少依赖或有
源代码缺陷的 P4_16 可保留 raw；不能仅因 BMv2 失败就排除仓库。证据不足的
unknown/conflicted 进入复核。P4C 复核尚未加入 A 采集器；严格 BMv2 正数据集仍
遵守 `positive_compile_clean.jsonl` 的编译成功 gate。

### B 方案采集器

`pipelines/bigquery_gharchive_p4/collector.py` 是 B 方案规范入口；
`tools/collect_gharchive_p4.py` 仅保留为旧命令兼容入口。采集器支持 BigQuery 全局 `.p4` 查询、
不超过 10,000 个 GHA 仓库名的受限 BigQuery 交叉检查、多来源候选合并、GitHub
默认分支最新 commit 解析、完整 tree manifest、P4_16 gate、整仓库/分层 payload
下载、断点复用和 repository-context 派生视图。

BigQuery 默认从忽略提交的 `data/token.json` 读取 authorized-user OAuth 凭据；也可用
`--access-token`、`BIGQUERY_ACCESS_TOKEN` 或 gcloud application-default credentials。
凭据不能写入 manifest、日志或 Git。实际 BigQuery/GHArchive/GitHub 网络命令、费用
保护和覆盖边界见 `pipelines/bigquery_gharchive_p4/pipeline.json` 与该目录 README。

采集器的关键门槛是：读取 Git tree 后先只请求所有 `.p4` blob，只有至少一个文件的
`p4_version_status=p4_16` 才请求完整仓库 payload。P4_14、unknown、conflicted、无 P4
或不可读 P4 会写入审计 manifest，状态为 `rejected`，不会下载 commit tarball。

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
