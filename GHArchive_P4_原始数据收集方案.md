# GHArchive-first / BigQuery-optional P4 项目级原始数据收集方案

**状态：方案草案 v0.4（GHArchive 默认发现，BigQuery 可选）**  
**目标：** 在不修改源码、不混入现有归一化数据的前提下，建立可恢复、可审计、可复现、代码上下文尽量完整的 P4 GitHub 原始项目库。

## 1. 范围和原则

本阶段以 raw 数据收集为主，并允许做**非破坏性的基础筛选**。筛选结果用于控制候选规模和生成下游索引；完整目录树清单和已保存的源码内容不因筛选而修改。

这里的“完整”分成两层：

1. **树级完整：** 记录目标 commit 的全部 tracked 路径、目录、文件模式、大小、blob/tree 标识和子模块/LFS 状态；
2. **代码级完整：** 对 P4 程序实际可能依赖的源码、头文件、include、控制面、测试、构建配置和关键文档保存原始内容。

大型二进制、数据集、缓存和明显生成产物可以不进入默认内容 payload，但不能从树清单中消失；大而可能重要的源代码文件进入独立的 `large_code` 隔离队列，而不是直接丢弃。只有在代码文件内容已经取得并校验后，快照才可标记为 `code_complete=true`。

本阶段不做以下工作：

- 不把单个 `.p4` 文件直接当成最终样本；
- 不改写、清洗、修复、拼接或“制造”可编译代码；
- 不进行 BMv2 编译筛选、任务生成、翻译、近重复裁剪或模型训练；
- 不把 `SFU-CONNECT/p4_dataset` 或 `p4llms/p4dataset` 合并到本项目的 GitHub 原始数据；
- 未得到确认前，不运行大规模 BigQuery、GHArchive 下载或 GitHub 克隆。

原始数据必须能够回答：某个仓库在什么来源、什么时间、什么分支、什么 commit 被发现；下载的内容是否正好对应这个 commit；仓库中有哪些 `.p4` 文件；失败或缺失发生在哪里。

## 2. 核心决策：项目级主样本，文件级索引

### 2.1 主收集单位

主样本定义为：

> 一个公开 GitHub repository 在一个确定 commit（默认分支的指定版本）下的完整项目树。

**硬性要求：只要仓库被判定为含有 `.p4` 文件，就不能只下载 `.p4` 文件。** 必须先枚举该 commit 的完整 Git tree，并保存代码完整的 P4 项目上下文。默认内容下载范围包括但不限于：

- 所有 `.p4`、`.p4i`、头文件和 include/support 文件；
- 控制面代码（Python/C/C++/Go 等）、PTF/Scapy 测试、CLI 或 P4Runtime 脚本；
- `Makefile`、构建脚本、Dockerfile、配置文件、架构/target 文件；
- README、文档、许可证文件和示例输入输出；
- 子模块指针、符号链接和 Git LFS pointer（raw 阶段原样保留并标记状态）。

也就是说，BigQuery 中发现的 `.p4` 路径只负责**发现仓库**；它不能把内容下载范围缩小成 `.p4` 文件白名单。每个候选仓库最终应形成一个“仓库名 + 固定 commit + 完整树清单 + 代码完整内容层”的快照，并通过 `files.jsonl` 索引树中的全部文件。这样可以保留 P4 程序引用的 include、C/C++ 或其他控制面代码、Python/Scapy 测试、JSON/P4Info 配置、构建文件、README 和目录上下文。raw 阶段不因某个文件不能编译而删除整个项目。

实现上，项目树枚举不得使用只返回 `.p4` 的代码搜索结果。应先获取完整 tree 元数据，再按下文的内容策略抓取 blob；如果使用 sparse/partial fetch 作为节省带宽的实现细节，必须确保完整 tree manifest 已保存，且所有代码级必保文件和依赖闭包都被抓取。若使用 codeload tarball，也必须使用 commit 级归档，并对树清单和内容层分别校验。默认不要求把所有视频、图片、压缩包、数据集和缓存内容塞进代码 payload，但必须在 tree manifest 中记录它们。

这里的“完整”指目标 commit 的完整文件树，不等于下载该仓库的全部 Git 历史；历史 push/commit 是否进一步下载由第 4 节的 `history_mode` 单独控制。

对于 `p4lang` 组织，按“组织下的每个公开仓库的最新默认分支版本”收集；即使某个仓库当前没有 `.p4`，raw 阶段也保留它并记录 `contains_p4=false`，避免违背“p4lang 下所有项目”的要求。后续归一化时再按需要筛选。

### 2.2 文件级索引

项目快照之外，生成一个可重建的 `files.jsonl`：每行对应一个文件，包含相对路径、大小、Git blob SHA-1/SHA-256、语言/扩展名提示和所属项目快照 ID。文件内容仍以项目归档为准，不在此阶段拆成相互独立的训练样本。

这种“双层表示”兼顾两种用途：

| 层级 | 一行代表什么 | 用途 |
|---|---|---|
| repository snapshot | 一个仓库的一次完整 commit 状态 | 保留项目上下文、控制面、依赖和可复现性 |
| file index | 快照中的一个文件 | 快速查找 `.p4`、统计、后续归一化和去重 |

### 2.2.1 代码完整性优先的内容分层

为了控制超大仓库的存储量，同时不丢失 P4 项目的实际代码上下文，每个项目快照分成三个可独立校验的层：

| 层 | 必须保存的内容 | 说明 |
|---|---|---|
| `tree_manifest` | 目标 commit 的全部 tracked 路径、目录、模式、大小、blob/tree hash、子模块/LFS 状态 | 100% 项目树索引，任何文件都不能在此层消失 |
| `code_payload` | 全部 `.p4`/`.p4i`、P4 include 闭包、C/C++/Rust/Go/Java 等源文件、Python/脚本、JSON/YAML/TOML 配置和测试、P4Info/protobuf、构建文件、README/LICENSE 等 | P4 项目代码上下文；这是默认必须完整取得的内容层 |
| `optional_payload` | 图片、视频、压缩包、模型权重、数据集、缓存、日志和明显生成输出 | 默认可只保留元数据或单独隔离，不影响 `code_complete` |

对普通大小项目，优先直接保存包含全部文件的 commit 归档，使 `full_content=true`。只有超过运营大小阈值的项目才启用分层下载：仍保存完整 `tree_manifest`，并尽力完整取得 `code_payload`；无法取得的超大代码文件必须进入 `large_code` 队列并在 manifest 中标记，不能静默丢弃。

因此，项目状态需要区分：

- `full_content=true`：全部可访问的 tracked blob 都已取得（子模块外部内容和 LFS pointer 另有状态）；
- `code_complete=true`：代码级必保文件及其依赖闭包已取得，非代码大文件可以只保留元数据；
- `tree_only=true`：只取得完整树清单，代码内容因失败或资源限制尚未完整取得。

默认不把 C、C++、Python、JSON、YAML、P4Info、测试夹具或构建文件视为“可省略的其它文件”。这些文件即使没有 `.p4` 后缀，也属于 `code_payload`。

### 2.3 项目级筛选与文件级过滤不是一回事

本项目的最终样本单位仍然是**项目快照**，不是孤立文件。筛选流程分为四层：

| 层次 | 判断单位 | 判断内容 | 结果 |
|---|---|---|---|
| 发现 | repository | GHArchive 事件索引得到候选；可选地叠加 BigQuery 精确 `.p4` 结果或 `p4lang` 组织清单 | 得到候选项目 |
| 选取 | repository + commit | 默认分支 commit 可解析、完整 tree 可下载、版本和来源可审计 | 得到 raw 项目快照 |
| 项目级准入 | repository snapshot | 普通候选的目标 commit 至少有一个非空、可读取的 `.p4` 文件；快照没有严重损坏 | 进入 P4 项目主视图 |
| 项目内部过滤 | file | 对生成文件、超长文件、二进制/缓存/编码数据做标记或派生视图排除 | 保留项目 ID，重组轻量项目上下文 |

因此，一个项目可以在 `derived/p4_context/` 中只保留部分文件，但这些文件仍然以同一个 `snapshot_id` 组织；不会把它们当成相互独立的最终样本。项目级准入失败的仓库也不删除 raw 归档，只进入相应的 `excluded`、`quarantine` 或 `p4_disappeared` 视图。

“至少一个可读取 `.p4` 文件”只表示非空、文本可读且不是 Git LFS pointer；raw 阶段不以编译成功或 P4 语义正确作为准入条件。这样可以保留有真实源码缺陷的项目，供后续单独分类。

### 2.4 与 StarCoder2 / The Stack v2 的对应关系

论文中与本项目直接相关的做法是：以 Software Heritage 图数据为主要来源，选 GitHub 仓库最新抓取版本，保留主分支，取主分支最新 commit，以完整目录内容 hash（`directory_id`）去重，并递归重建仓库树。之后 basic filters 主要作用于**文件**：长行、自动生成、低字母比例、编码数据和语言特定规则。经过这些文件级处理和语言子采样后，论文再构造 repository-context 数据集，把同一仓库中**保留下来的文件**放在一个上下文中并随机排列。

所以，The Stack v2 的“项目级”体现在仓库/commit 选择、目录树 hash 去重和最终 repository-context 组织；它并不意味着仓库内每一个原始文件都无条件进入训练上下文。论文中“full repository”应理解为经过前置过滤后保留下来的该仓库文件集合，而不是未过滤的 Git 原始树。

本方案在 raw 阶段沿用这些原则，但做两点适配：

1. GHArchive 用于事件、默认分支和 push/commit 溯源；代码内容从 GitHub 的 commit 下载，因为 GHArchive 本身主要保存事件 JSON，不是完整源码对象库。
2. raw 阶段保留仓库原貌和所有来源；论文中的许可证过滤、语言过滤、自动生成文件过滤、PII 处理和近重复删除只产生标记或下游视图，不回写/覆盖 raw 快照。
3. 我们将项目级准入和文件级基础过滤明确分开：只有“完整项目快照 + 至少一个可读取 `.p4`”才进入 P4 项目主视图；项目内部的低质量文件只从派生上下文视图排除。

## 3. 数据来源和职责

### 3.1 BigQuery GitHub 公共数据集：可选补充候选索引

保留 `bigquery-public-data.github_repos` 作为可选的补充发现源。实际运行前先通过 `INFORMATION_SCHEMA.COLUMNS` 检查当前表结构，不硬编码未经验证的字段。

候选查询的逻辑如下（字段名以运行时 schema 为准）：

```sql
-- 运行前先做 dry run，并将实际字段名与 schema 记录到 manifest。
SELECT
  repo_name,
  COUNTIF(REGEXP_CONTAINS(LOWER(path), r'(^|/)[^/]+\.p4$')) AS p4_file_count,
  ARRAY_AGG(path IGNORE NULLS LIMIT 100) AS example_paths
FROM `bigquery-public-data.github_repos.files`
WHERE REGEXP_CONTAINS(LOWER(path), r'(^|/)[^/]+\.p4$')
GROUP BY repo_name;
```

说明：

- `.p4` 使用大小写不敏感的精确扩展名匹配；不自动把 `.p4i`、`.p4include` 或任意包含字符串 `p4` 的文件算作 P4 源文件。
- 结果只作为 repository 名称和候选路径索引，不把 BigQuery 中的文件记录当作当前源码快照；其 `p4_verification_status` 标记为 `external_index`。
- 需要保存查询文本、执行时间、schema、job ID、计费项目、dry-run bytes processed 和导出文件 SHA-256。
- 如果 `files` 表没有可靠的分支/commit 语义，则不从它推断最新版本；最新版本由 GHArchive/GitHub commit 阶段确定。
- 查询应支持按日期/分区（如果当前表提供）或按批次导出；禁止未经 dry run 就对全表反复扫描。

BigQuery 的作用是提供独立的文件索引交叉来源，回答“哪些项目曾被索引到 `.p4` 文件”；它不负责替代 GHArchive 候选发现、GitHub 源码下载和版本校验。关闭 BigQuery 时，GHArchive 主流程仍可独立运行。

## 3.1.1 项目级基础筛选（推荐默认值）

候选仓库可能非常多，但筛选必须以**仓库快照**为判断单位，而不是以某个 `.p4` 文件为判断单位。推荐把筛选分成“项目级准入/隔离”和“项目内部文件标记”两步。项目级准入只决定一个完整 commit 快照进入哪个视图，不会把已下载项目裁成单文件集合。

### 项目级准入规则

| 阶段 | 默认条件 | 处理方式 |
|---|---|---|
| 下载前 | 仓库公开、非 empty、默认分支可解析、仓库名和 URL 合法 | 不满足则记入 `repository_filter_annotations.jsonl`，不下载 |
| 下载前 | BigQuery 发现至少一个精确 `.p4` 路径 | 进入普通候选；路径只作发现依据，不作文件下载白名单 |
| 下载前 | GitHub 仓库元数据的大小明显超过可配置上限，或预计文件数极大 | 不静默丢弃；记录 `oversize_quarantine`，暂不下载或只做小样本试下载 |
| 下载后 | 目标 commit 的完整 `tree_manifest` 校验通过，`code_complete=true`，且至少有一个非空、非 LFS pointer 的 `.p4` | `project_status=eligible`，进入 P4 项目主视图 |
| 下载后 | 目标 commit 实际没有 `.p4`（路径已删除、分支变化或 BigQuery 陈旧） | 保留完整快照和文件索引，标记 `p4_disappeared=true`，不计入 P4 主视图 |
| 下载后 | `.p4` 全部为空、二进制损坏或只是 LFS pointer | 保留完整快照，标记 `no_readable_p4=true`，进入异常/隔离视图 |
| 任一阶段 | 快照不完整、归档损坏、commit 不可达 | 保留错误和可重试信息，`project_status=failed`，不进入主视图 |
| `p4lang` 例外 | 组织当前公开仓库的最新默认分支 | 有默认分支则收集完整快照；没有 `.p4` 也保留；empty 仓库至少保留组织清单元数据 |

默认**不**按 stars、fork 数、最近更新时间、是否 fork、是否 archived、许可证是否已识别来排除仓库。这些字段需要进入 manifest，供后续分层抽样和敏感性分析使用；如果存储预算不足，优先降低历史版本范围，而不是删除低 star 的最新完整项目。

建议初始运行时把总项目大小上限设为**运营保护阈值**，例如压缩归档 2 GiB、解压后 10 GiB、tracked 文件 100,000 个；这些数值需要在阶段 0 的分布估计后调整，不是论文规定的质量阈值。超过上限的仓库仍应保留候选元数据、GHArchive 事件和失败原因；取得额外存储后可单独执行，不得把它们报告成“没有 P4”。

### 项目级去重

同一个仓库的多个来源记录、同一个 commit 的重复发现，以及不同仓库具有完全相同项目树的情况，应在**项目快照层**去重：

- `(repository_id, commit_sha)` 相同：只保存一份内容，合并所有发现来源和事件 ID；
- 不同仓库的完整 tree hash 相同：只保存一份内容快照，其他仓库保留 provenance alias；
- 推荐计算 Git tree SHA 或递归路径/内容 Merkle hash，字段命名为 `tree_hash` / `directory_id`；
- raw 阶段不做 MinHash 近重复删除；近重复只在后续训练视图中处理。

这正是 The Stack v2 论文中最接近“项目级筛选”的部分：他们从主分支最新 commit 构造仓库目录树，并按 `directory_id`（完整目录内容标识）去重。这个去重发生在仓库/commit 层；论文后面的长行、自动生成、编码数据等 basic filters 才是文件层规则。

### 项目级筛选流程

```text
BigQuery `.p4` 仓库候选 + p4lang 组织清单
        │
        ├─ 项目元数据预检（公开、非空、默认分支、预计大小）
        │       └─ 不合格：repository_filter_annotations / quarantine
        │
        ├─ 下载目标 commit 的完整项目树
        │       └─ 失败：errors.jsonl；不产生 eligible 项目
        │
        ├─ 完整性校验 + 项目级指标（文件数、总字节、可读取 `.p4` 数）
        │       ├─ 无可读取 `.p4`：异常/隔离视图
        │       └─ 有可读取 `.p4`：P4 项目主视图
        │
        └─ 项目内部文件标记 → derived/p4_context/（仍按 snapshot_id 组织）
```

这里不能用“`.p4` 文件占项目文件的比例”作为默认硬门槛：真实 P4 项目通常包含较多控制面、测试、构建和文档文件，低比例并不代表项目无价值。比例、star、fork、语言构成等只用于分层统计和后续抽样。

### 最终推荐的项目级筛选决策

如果本阶段必须控制规模，建议按下面顺序减量；前两项是项目级硬决策，后两项是项目级去重/视图决策：

1. **先限制版本数：** 普通候选只收集默认分支最新 commit；GHArchive 的历史 push 先只保存去重后的 lineage 索引，不下载每个历史项目树。历史源码只有在选择 `window` 或 `all` 时另行收集。这是规模收益最大、对项目完整性影响最小的办法。
2. **再做项目级准入：** 完整 `tree_manifest` 校验通过、代码级必保文件已取得（`code_complete=true`），且目标 commit 至少有一个非空、可读取、不是 LFS pointer 的 `.p4` 文件，才标记 `project_status=eligible`。不满足的项目保留元数据/错误和（若已下载）已有 raw 层，但不进入 P4 主视图。
3. **按完整项目树精确去重：** 对 `(repository_id, commit_sha)` 和完整 `tree_hash`/`directory_id` 去重；不同仓库内容相同只保存一份内容并保留多个 provenance alias。
4. **最后生成轻量项目视图：** 在每个 eligible 项目内部应用 basic filters，去掉派生视图中的明显二进制、缓存、超长/自动生成文件，但仍以 `snapshot_id` 为单位输出项目上下文。raw 完整快照不变。

因此，项目级最终状态应至少有：

| `project_status` | 含义 | 是否进入 P4 项目主视图 |
|---|---|---:|
| `eligible` | 完整 `tree_manifest` + `code_complete=true` + 至少一个可读取 `.p4` | 是 |
| `eligible_with_warnings` | 满足上述准入，但含超大/生成/外部依赖未解析等警告 | 是，带警告 |
| `p4_disappeared` | BigQuery 曾发现 `.p4`，目标 commit 已不存在 | 否，保留审计快照 |
| `no_readable_p4` | `.p4` 全为空、损坏或仅有 LFS pointer | 否，进入异常视图 |
| `oversize_quarantine` | 超过运营大小阈值，尚未完整下载 | 否，可重试 |
| `failed` | commit 不可达、归档损坏或下载失败 | 否，可重试 |

这套规则才是“项目级筛选”：项目是否进入主视图由完整性、版本和可读取 P4 的**项目级聚合条件**决定；文件级规则只影响该项目的轻量派生视图，不改变项目是否被发现和 raw 是否完整保留。

## 3.1.2 论文启发的文件级基础筛选

StarCoder2/The Stack v2 的 basic filters 主要用于训练前清理 autogenerated、数据文件和低质量长文本。本项目的 raw 快照必须完整保留，因此下面规则只生成 `file_filter_annotations.jsonl` 和一个可删除重建的 `derived/p4_context/` 视图，不删除归档中的原始文件。

### 3.1.2.1 内容抓取白名单不是 `.p4` 白名单

在分层下载模式下，先对完整 tree 中的每个路径分类，再决定是否抓取 blob。默认进入 `code_payload` 的文件包括：

- `.p4`、`.p4i`、P4 include 递归依赖，以及同目录/邻近目录中的 `.h`、`.hpp`、`.inc` 等支持文件；
- C/C++/Objective-C、Rust、Go、Java、JavaScript/TypeScript、Python、Shell 等源码和脚本；
- P4Runtime/控制面代码、PTF/Scapy 测试、CLI/BF-Runtime/驱动调用代码；
- JSON/YAML/TOML/INI/CSV 中的配置、P4Info、schema、protobuf/IDL 和测试夹具；
- Make/CMake/Bazel/meson、Dockerfile、requirements/package manifest、CI 配置；
- README、LICENSE、设计说明和与 P4 程序相邻的短文档。

`.json` 不能仅按扩展名排除：位于 `p4info`、`config`、`schema`、`test`、`examples`、`src` 或仓库根目录的 JSON 默认视为代码/配置上下文；明显的 dataset、dump、coverage、result、cache 或生成输出才标记为 `optional_payload` 或 `generated_suspect`。同样，`.c`/`.h`/`.py` 文件无论是否被 BigQuery 作为 P4 路径发现，都不能因“不是 P4”而排除。

P4 include 的闭包必须递归解析并加入 `code_payload`：从每个 `.p4`/`.p4i` 的 `#include` 开始，按仓库相对路径、包含文件所在目录和项目构建参数尝试解析；解析不到的外部系统 include 记录为 `external_dependency_unresolved`，不能伪造文件。

### 3.1.2.2 大文件处理

- 对代码、配置和测试文件不采用论文中“所有文件统一 10 MB 内容上限”的机械规则；否则容易丢失重要的 C/Python/JSON/P4Info 文件。
- 默认优先完整抓取所有代码级文件；单个大于 50 MiB 的文本/代码文件进入 `large_code` 队列，继续尝试抓取，只有资源或服务限制导致未取得时才标记缺失。
- 图片、视频、压缩包、模型权重、数据集、缓存、日志和明显生成输出默认不抓取内容，保留 tree 元数据；小型许可证、示例输入和配置归入 `code_payload`。
- 项目级运营阈值（初始建议：压缩 2 GiB、解压 10 GiB、tracked 文件 100,000 个）只用于隔离超大项目，不是代码质量判断。超阈值项目至少保存 tree manifest，并可分批补抓 `code_payload`。
- 只有 `code_payload` 中所有必保路径均已取得并通过 SHA 校验，才设置 `code_complete=true`；任一必保路径缺失都要列入 `missing_code_paths`，不能仍标记为代码完整。

对全部文件计算并记录：路径、扩展名、字节数、行数、平均/最大行长、NUL/二进制检测、前 5 行生成标记、疑似编码数据比例、过滤原因和建议动作。

推荐的保守规则：

1. **空/二进制文件：** 空文件、仅空白文件、含 NUL 或明显二进制内容的文件标记为 `non_text`。仍保留原文件；不把它们放进文本上下文视图。
2. **超长文件：** 超过 100,000 行，或对代码类文件平均行长超过 100、最大行长超过 1,000 的文件标记为 `long_or_generated_suspect`。HTML/JSON/Markdown/XML 等文档/数据类文件沿用论文的放宽规则；论文还对单个压缩内容超过 10 MB 的文件不下载到其内容存储，本项目 raw 归档仍保留该文件或 pointer，并在派生视图中可隔离。
3. **自动生成提示：** 文件前 5 行出现 `auto-generated`、`autogenerated`、`automatically generated`、`generated automatically` 或 `this file is generated` 时标记为 `autogenerated_suspect`。如果后续接入 go-enry `is_generated`，把版本和判定结果写入 manifest。
4. **编码数据提示：** 对非 P4 文本检查论文中的 Base64、长十六进制和 Unicode escape 规则；匹配串超过 1,024 字符或匹配字符超过文件 50% 时标记为 `encoded_data_suspect`。这条规则只作为提示，不能直接用于删除 P4 源码，因为 P4 合法程序可能含有较多十六进制常量。
5. **低字母比例：** 论文的 25% alpha 规则不直接应用于 `.p4`、`.p4i`、头文件、Makefile、脚本和配置文件；这些文件的符号密度可能是合法语法。对明显的非代码文本/数据文件可以记录 `low_alpha_suspect`，但仍保留。
6. **明显构建/缓存目录：** `node_modules/`、`.venv/`、`build/`、`dist/`、`target/`、缓存和日志目录只在 `derived/p4_context/` 中默认排除；如果其中含 `.p4` 或被项目源码引用，必须在索引中保留路径并设置 `referenced_filtered_file=true`。

`derived/p4_context/` 的建议保留集合是：全部 `.p4`/`.p4i`、被 P4 `#include` 或构建脚本引用的支持文件、所有源代码和脚本、配置/P4Info/protobuf、控制面/测试源文件、构建配置、README/许可证和短文档。它应尽量与 `code_payload` 一致，只额外应用论文中的基础质量过滤；这个视图用于后续人工检查或任务种子提取，不是 raw 数据，也不能代替完整项目快照。

## 3.1.3 筛选结果字段和统计

每条仓库和文件筛选记录都必须同时保存 `rule_version`、`status`（`keep`/`annotated`/`quarantine`/`excluded`）、`reasons`、`raw_snapshot_id` 和处理时间。最终报告至少分别统计：

- 发现仓库数、下载仓库数、大小隔离仓库数、失败仓库数；
- 含 `.p4` 的最新快照数、BigQuery 路径已消失数、`p4lang` 无 `.p4` 仓库数；
- 完整 raw 文件总数/字节数，以及派生视图保留文件总数/字节数；
- 各筛选原因数量，尤其是 autogenerated、long、binary、encoded-data 和 build/cache 路径；
- 任何规则升级前后的差异清单。

这样可以达到“基础减负”而不改变原始项目的完整性：完整仓库仍可追溯，后续 normalized 阶段又能直接使用轻量的 P4 上下文视图。

### 3.2 GHArchive：公共事件和版本溯源

GHArchive 按小时保存公开 GitHub event JSON（典型地址形如 `https://data.gharchive.org/YYYY-MM-DD-H.json.gz`）。原始采集应保存下载的压缩事件文件或其 SHA-256，并从中提取：

- `PushEvent`：`repo.id`、`repo.name`、`payload.ref`、`payload.before`、`payload.head`、`payload.size`、`payload.distinct_size`、`created_at`、事件 `id`；
- `CreateEvent`/`DeleteEvent`/`RepositoryEvent`/`ForkEvent`：补充分支、仓库创建/删除、fork 和可见性变化；
- actor、组织、默认分支线索以及 GHArchive 文件 URL。

版本处理必须以 `payload.head` 对应的 commit 为准。`payload.commits` 只是事件中携带的提交摘要，可能被截断，不能用来替代完整 Git history。

GHArchive 的边界：

- 只覆盖公开事件；
- 事件流不是完整 Git 对象数据库，历史 commit 可能无法从 GitHub 再下载；
- 某些仓库没有近期事件，因此不能只靠 GHArchive 得到“当前所有仓库”；
- 因此本方案把 GHArchive 作为默认的候选仓库和 push lineage 来源；BigQuery 只作为可选的补充/交叉来源，当前代码仍使用 GitHub commit/codeload。

GHArchive 候选阶段只回答“哪些仓库在事件窗口内进入候选集合”，不回答“仓库是否含有 P4”。实现上，`index-gharchive` 保存所有事件的仓库级索引和去重后的 `PushEvent` lineage，`discover-gharchive` 聚合为 `gharchive_events` 候选，并将 `contains_p4=null`、`p4_verification_status=unverified` 写入记录。随后 `collect-snapshots` 获取默认分支最新 commit、完整 Git tree 和代码内容；只有这个快照阶段才能产生 `snapshot_present_readable`、`snapshot_present_unreadable` 或 `snapshot_absent` 的 P4 验证结果。

默认命令链为：

```text
GHArchive hourly files
    -> index-gharchive
    -> discover-gharchive
    -> merge-discovery
    -> collect-snapshots
    -> build-context
```

如果需要补充 BigQuery，只需额外运行 `discover-bigquery`，再把
`discovery/bigquery_p4_repositories.jsonl` 作为另一个输入交给
`merge-discovery`；不会改变 GHArchive 主流程。

### 3.3 `p4lang` 组织：完整组织清单 + 最新版本

目标地址为 `https://github.com/p4lang/`。`p4lang` 组织清单作为可选的领域专用补充来源保留；它不替代 GHArchive 默认发现，也不用于代码搜索。

每个仓库：

1. 记录是否 archived、fork、empty、default branch、license、更新时间等元数据；
2. 解析默认分支最新 commit；
3. 下载该 commit 的完整项目树；
4. 扫描并记录所有 `.p4` 路径，保留无 `.p4` 仓库的 `contains_p4=false` 标记。

若组织接口暂时不可用，则可以用 GHArchive 中的组织事件形成临时清单，但必须标记为 `membership_source=gharchive_incomplete`，不能宣称已覆盖全部组织项目。

## 4. 版本策略

为避免把“所有 push 版本”与“所有仓库当前版本”混成一个无法控制的任务，采用分层策略：

### 4.1 默认主线（必须收集）

- GHArchive 候选仓库：收集其可访问的默认分支最新 commit；
- BigQuery 候选仓库（如果启用）：收集其可访问的默认分支最新 commit；
- `p4lang`：收集组织清单中每个公开仓库的默认分支最新 commit；
- 主线 snapshot ID 使用 `source + owner__repo + commit_sha`，同一个 commit 不重复下载。

### 4.2 历史 push 索引（必须收集，内容按预算执行）

从 GHArchive 提取所有观测到的 `(repo_id, ref, head_sha)`，按该三元组精确去重，保存完整 push 索引和事件引用。这样即使暂时不下载每个历史版本，也不会丢掉版本线索。

### 4.3 历史源码快照（可配置）

历史 commit 的源码下载设置为独立开关：

- `history_mode=none`：只保存历史 push 索引，不下载历史源码；
- `history_mode=window`：在指定日期窗口或每仓库最近 N 个唯一 head 中下载；
- `history_mode=all`：尝试下载所有可访问的历史 head，适合有明确存储预算时运行。

无论选择哪种模式，历史快照都放在与主线不同的目录和 manifest 中，不能覆盖最新快照。建议先用 `window` 做小规模估算，再决定是否启用 `all`。

采集器对应的可选命令为 `collect-history`：它读取已建立的
`manifests/push_events.jsonl` 和合并后的候选仓库清单，只对候选仓库选择
`refs/heads/*` 的去重 `head_sha`，并将
历史快照写入 `snapshots/history/`。默认 `--mode window --max-per-repository 20`；
`--mode all` 才会尝试每个仓库索引到的全部历史 head，`--mode none` 只生成空选择。
历史 PushEvent 可能早于仓库当前保留范围，下载失败必须保留在 `errors.jsonl`，不能
把“不可达历史 commit”解释成当前项目缺少 P4。

## 5. 原始目录布局

建议使用新的隔离根目录 `data/raw/gharchive_p4/`，不覆盖当前 `data/raw/repositories/` 中已经审核过的 P4 项目快照：

```text
data/raw/gharchive_p4/
├── README.md
├── discovery/
│   ├── bigquery_schema.json
│   ├── bigquery_query.sql
│   ├── bigquery_p4_repositories.jsonl
│   └── bigquery_run.json
├── events/
│   └── gharchive/YYYY/MM/DD/YYYY-MM-DD-H.json.gz
├── manifests/
│   ├── repositories.jsonl          # 一行一个仓库候选/组织项目
│   ├── snapshots.jsonl             # 一行一个 commit 快照
│   ├── trees/                      # 完整 tree manifest，每行一个 tracked path
│   ├── files.jsonl                 # 一行一个快照文件
│   ├── file_filter_annotations.jsonl
│   ├── repository_filter_annotations.jsonl
│   ├── gharchive_repository_events.jsonl # 所有事件的仓库级索引
│   ├── push_events.jsonl           # 去重后的 push lineage
│   ├── errors.jsonl
│   └── checksums.sha256
├── snapshots/
│   ├── github_candidates/owner__repo/commit_sha/
│   │   ├── repository.tar.gz        # 普通项目优先保存完整 commit 归档
│   │   ├── code_payload/            # 分层模式下的代码级文件
│   │   └── optional_payload/        # 可选的大文件（可为空）
│   ├── p4lang_latest/owner__repo/commit_sha/
│   └── history/owner__repo/commit_sha/
└── logs/
    ├── discovery/
    └── download/
```

每个成功快照旁边保存 `snapshot.json`，至少包含下载 URL、目标 commit、实际 `HEAD`、默认分支、下载方法、时间、字节数、文件数、`.p4` 清单、`tree_manifest` 路径、`content_mode`、`full_content`、`code_complete` 和归档 SHA-256。完整归档或分层内容校验通过后才把对应状态从 `partial` 改为 `downloaded`。

## 6. manifest 记录格式

### 6.1 仓库记录（`repositories.jsonl`）

```json
{
  "repository_id": 123456,
  "repository": "owner/name",
  "html_url": "https://github.com/owner/name",
  "clone_url": "https://github.com/owner/name.git",
  "discovery_sources": ["gharchive_events", "bigquery_github_repos"],
  "discovery_basis": "gharchive_repository_event",
  "bigquery_first_seen": "...",
  "gharchive_last_seen": "...",
  "default_branch": "main",
  "is_fork": false,
  "is_archived": false,
  "contains_p4": null,
  "p4_verification_status": "unverified",
  "p4_paths": ["src/main.p4", "tests/foo.p4"],
  "status": "candidate"
}
```

### 6.2 快照记录（`snapshots.jsonl`）

```json
{
  "snapshot_id": "github_candidates__owner__name__<commit_sha>",
  "repository": "owner/name",
  "source_kind": "github_candidate",
  "ref": "refs/heads/main",
  "commit_sha": "<full_sha>",
  "parent_shas": ["..."],
  "observed_event_ids": ["..."],
  "download_url": "https://github.com/owner/name/archive/<full_sha>.tar.gz",
  "download_method": "git_clone_depth1|codeload_commit_tarball",
  "p4_discovery_status": "unverified|external_index",
  "p4_verification_status": "snapshot_present_readable|snapshot_present_unreadable|snapshot_absent",
  "snapshot_path": "snapshots/github_candidates/owner__name/<sha>/repository.tar.gz",
  "tree_sha": "...",
  "tree_manifest_path": "manifests/trees/...jsonl",
  "archive_sha256": "...",
  "full_content": true,
  "code_complete": true,
  "content_mode": "full_archive|layered_code_payload|tree_only",
  "code_payload_path": "snapshots/.../code_payload",
  "optional_payload_path": "snapshots/.../optional_payload",
  "missing_code_paths": [],
  "large_code_paths": [],
  "optional_omitted_paths": [],
  "file_count": 0,
  "total_bytes": 0,
  "p4_file_count": 0,
  "p4_paths": [],
  "submodules": [],
  "git_lfs_pointers": [],
  "status": "downloaded"
}
```

### 6.3 文件索引记录（`files.jsonl`）

```json
{
  "snapshot_id": "...",
  "path": "src/main.p4",
  "extension": ".p4",
  "size_bytes": 1234,
  "git_blob_sha": "...",
  "sha256": "...",
  "is_symlink": false,
  "language_hint": "P4",
  "content_location": "inside_snapshot_archive"
}
```

原始源码不得在写入归档前做换行、编码、注释或 include 修改。若 Git LFS 只下载到 pointer，保留 pointer 原文并设置 `content_status=lfs_pointer_missing`，不得伪造真实内容。

## 7. 下载和完整性检查

### 7.1 下载顺序

对每个目标 commit 使用以下顺序：

1. 以完整 commit SHA 固定目标，优先 `git clone/fetch` 后校验 `git rev-parse HEAD`；
2. Git clone 失败时使用 GitHub codeload 的 commit tarball；
3. 两者都失败则写入 `errors.jsonl`，保留 HTTP/Git 错误尾部、重试次数和最后一次 URL；
4. 不把分支名下载结果视为已固定版本，除非同时记录并校验实际 commit SHA。

### 7.2 “完整项目”检查

- 归档根目录、目标 commit 和 tree SHA 一致；
- `tree_manifest` 必须包含目标 commit 的全部 tracked 文件，而不是仅包含 `.p4` 文件；
- `full_content=true` 时，所有可访问 tracked blob 均已取得并通过 SHA 校验；
- `code_complete=true` 时，全部 `.p4`/`.p4i`、include 闭包、源代码、配置、测试、构建和关键文档路径均已取得并通过 SHA 校验；
- 记录所有普通文件、目录、符号链接、子模块指针和 LFS pointer；
- 扫描 `.p4` 时忽略大小写，但保留原始路径大小写；
- 检查归档能否完整解压到临时目录，防止路径穿越和截断归档；
- 保存总文件数、总字节数、最大文件、`.p4` 文件大小分布；
- raw 阶段不改写已取得的文件；未抓取的 optional/large 内容必须在 manifest 中列明路径、原因和可重试状态。

## 8. 去重和数据泄漏边界

raw 阶段不做破坏性去重，只做可审计标记：

- 同一 `(repository, commit_sha)` 只下载一次；
- 不同仓库若 `tree_sha` 或归档内容 hash 相同，保留两条 provenance，只增加 `duplicate_of_snapshot_id`；
- fork、镜像、模板仓库不在 raw 阶段删除；
- 近重复、MinHash/LSH、许可证过滤、自动生成文件过滤、PII 清理留到 normalized 阶段。

StarCoder2/The Stack v2 使用 5-gram MinHash、Jaccard 0.7，并优先保留 stars/forks 较高或 commit 较新的仓库；本项目若后续采用，应在新脚本和新输出前缀下运行，不能回写 raw 归档。

## 8.1 许可证、撤回和公开性记录

raw 阶段不依据许可证删除源码，但必须尽可能保存仓库的许可证元数据、`LICENSE` 文件路径、GitHub API 返回的 license 提示、仓库可见性和 archived/fork 状态。许可证未知时记录 `license_status=unknown`，不能推断为可训练或可再发布。

如果作者后来提出 GitHub 删除、opt-out 或访问限制请求，原始快照不能悄悄覆盖；应在独立的撤回清单中记录 repository、commit、请求来源、处理时间和状态，并在后续导出阶段排除。raw 归档的访问权限、再分发和训练用途需另行依据项目的法律/合规审查确定。

## 9. 分阶段执行计划（得到同意后才执行）

### 阶段 0：只读预检

- 检查 GHArchive 下载连通性、磁盘空间、代理和 Git/curl；
- 可选地检查 BigQuery 项目、凭据、区域、表 schema 和最大计费字节设置；
- 估算 GHArchive 候选仓库数量、历史 push 数量和存储量；
- 生成候选摘要和 dry-run 报告，不下载仓库。

### 阶段 1：候选发现

- 按配置的日期窗口下载 GHArchive 小时文件，建立仓库事件和 push lineage 索引；
- 从 GHArchive 事件聚合 `gharchive_events` 候选；此时 `contains_p4` 不作断言；
- 可选地执行 BigQuery 查询、枚举 `p4lang` 当前组织仓库，并作为独立来源合并；
- 按 GitHub repository ID 和规范化 `owner/name` 合并来源，保留来源集合。

### 阶段 2：小规模下载试运行

- 从 GHArchive 候选和可选的其他来源各抽取一小批（建议每类 20--50 个）；
- 验证完整 tree manifest、代码内容层、commit 校验、`.p4` 扫描、断点续跑和错误记录；
- 只在审计报告通过后扩大并发和批量。

### 阶段 3：主线快照收集

- 下载全部候选仓库的默认分支最新 commit；
- 下载 `p4lang` 全部公开仓库的最新默认分支 commit；
- 生成 `repositories.jsonl`、`snapshots.jsonl`、`files.jsonl` 和校验和清单。

### 阶段 4：历史版本（按确认的模式）

- 先完成 GHArchive push 索引；
- 按 `none/window/all` 选择下载历史源码；
- 历史与主线分目录、分 manifest、分统计。

### 阶段 5：收尾审计

- 生成候选覆盖、成功/失败、`.p4` 数量、仓库大小、来源交集和重复 hash 报告；
- 对失败仓库分类（不存在、权限/限流、网络超时、commit 不可达、归档损坏、LFS 缺失等）；
- 生成可重跑命令和工具版本清单；
- 不运行 normalize、compile 或任务生成，除非另行授权。

## 10. 可恢复性、限流和安全

- manifest 采用追加式 JSONL；每个 repo/commit 有独立状态，支持中断后跳过已校验成功项；
- 网络请求使用有限并发、指数退避、`Retry-After`、连接/总超时和 403/429 分类；
- BigQuery 必须先 dry run，设置 `maximum_bytes_billed`，并保存 job ID；
- GitHub token（若使用）只从环境变量或 `.env` 读取，不写入 manifest、日志或命令行输出；
- 不使用递归删除覆盖 raw 根目录；失败的 partial 文件保留或移动到明确的 quarantine 目录；
- 采集日志不得包含 token、代理认证信息或其他秘密。

## 11. 交付物和验收标准

首轮成功后至少应有：

1. GHArchive 原始小时文件（或其不可变缓存）、仓库事件索引、候选仓库和 push lineage JSONL；
2. GHArchive 候选仓库的主线快照、文件索引和 SHA-256 清单；
3. 如果启用，BigQuery schema、查询文本、运行元数据和候选仓库 JSONL；
4. `p4lang` 组织清单及最新版本快照（如果启用）；
5. 失败/跳过/缺少 LFS 或不可达 commit 的审计报告；
6. 一份覆盖率和存储估算报告；
7. 不改变 `data/normalized/`，不覆盖已有 `data/raw/repositories/`。

验收最低条件：

- 每个 `downloaded` 快照都能由 manifest 定位到唯一 URL、commit、tree manifest 和内容 hash；
- 完整归档或分层内容层校验后，`.p4` 路径与 `files.jsonl`/tree manifest 一致；
- 同一 commit 重跑不会产生第二份内容归档；
- 所有失败都有明确错误类别和可重试信息；
- 能从 manifest 重建“仓库 → 来源事件 → commit → 快照 → `.p4` 文件”的链路。

## 12. 启动前需要你确认的参数

在执行任何网络采集前，请确认以下选择；未确认项将保持 dry-run：

1. **普通仓库历史版本：** 只收集默认分支最新版本，还是启用 `window`/`all` 历史 push 快照？
2. **候选范围：** 是否保留 fork、archived 和仅含 LFS pointer 的项目？默认保留并标记；empty 仓库没有可下载的项目树，普通候选默认只保留元数据，`p4lang` empty 仓库也至少保留组织清单记录。
3. **`p4lang` 范围：** 是否按“所有公开仓库”收集，即使没有 `.p4`？本方案默认是。
4. **BigQuery：** 使用哪个 billing project、区域和最大 bytes billed？
5. **存储/并发：** 可用磁盘空间、最大并发下载数、是否使用现有 HTTP 代理。
6. **子模块和 Git LFS：** 默认只保留子模块指针、保留 LFS pointer；如需递归拉取，需单独确认其额外网络和许可风险。

确认后先执行阶段 0 和阶段 2 的小规模试运行；试运行报告通过后再执行全量收集。
