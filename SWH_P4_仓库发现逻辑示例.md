# 从 `.p4` 文件名找到完整仓库：逐步 case

目标是尽可能全面地找到 SWH 中可追溯到 GitHub origin、某个版本包含 P4_16 的
仓库，然后保存确定 revision 的完整项目。发现阶段保留历史/分支候选；物化时
默认每个 origin 选择最新的、已确认含 P4_16 的 revision。

本文所有 `dir-*`、`rev-*`、`blob-*`、仓库 URL、query ID 和命中行数都是**虚构的
教学数据**，不能用于实际下载。真实对象标识来自 Graph 查询；URL 也来自 origin
表，不能凭仓库名猜测它是否含 P4。当前代码与目标设计的差别见末尾。

## Case 1：AWS 扫描的第一条记录是什么？

源数据在 SWH 公开桶：

```text
s3://softwareheritage/graph/2026-06-04/orc/directory_entry/
```

2026-10-10 已核验这里有 128 个 ORC 对象，总共 27.079 TiB；完整 Graph 约
33 TiB，目录表只是其中一部分。ORC 是表数据，不是 128 个仓库源码压缩包。

假设其中几行如下。`name` 在 ORC 中为 binary，查询时解码；这里显示为文本：

| directory_id | name | type | target | 是否命中 |
| --- | --- | --- | --- | --- |
| dir-root | src | dir | dir-src | 否，是子目录 |
| dir-src | main.p4 | file | blob-p4 | 是 |
| dir-src | controller.py | file | blob-python | 否 |
| dir-docs | about-p4.txt | file | blob-text | 否，后缀不是 `.p4` |
| dir-root | fake.p4 | dir | dir-other | 否，类型不是 file |
| dir-src | helper.P4 | file | blob-helper | 是，扩展名不区分大小写 |

筛选逻辑：

```sql
WHERE type = 'file'
  AND ends_with(lower(from_utf8(name)), '.p4')
```

首次扫描输出的是：

```json
{"directory_id":"dir-src","name":"main.p4","type":"file","content_id":"blob-p4"}
```

此时**还不知道 URL、revision、根目录和完整路径**。`dir-src` 是直接父目录，
`main.p4` 是文件名，不是 `src/main.p4` 的完整路径；也还没有读取文件内容。

## Case 2：仓库 URL 从哪里来？

同一 Graph 的相关关系，简化为：

| 关系 | 示例 |
| --- | --- |
| directory_entry：父目录引用子目录 | dir-root 的 `src` 指向 dir-src |
| revision：commit 使用哪个根目录 | rev-old 的根目录是 dir-root |
| revision_parent：commit 历史 | rev-new 的父 commit 是 rev-old |
| snapshot_branch：分支引用 | snap-1 的 refs/heads/main 指向 rev-new |
| snapshot alias（如有） | HEAD 指向 refs/heads/main，需解析 alias |
| origin_visit_status：抓取关系 | origin-1 的一次有效抓取对应 snap-1 |
| origin：来源 URL | origin-1 的 URL 为 https://github.com/example/net-project |

反向查找路径：

```text
blob-p4 ← main.p4 / dir-src
             ↑ 被 src 引用
          dir-root ← revision rev-old
                        ↑ 是其祖先
                     rev-new ← snap-1 / refs/heads/main
                                  ← origin-1 / 一次抓取
                                  ← origin.url
```

因此即使 `rev-old` 不是当前分支 tip，也能通过 commit 历史追溯到该 origin。
**只把 revision 直接连接到 snapshot_branch.target 会漏掉这个旧版本。**
必须同时处理目录祖先关系和 `revision_parent` 历史关系；tag/release 与 alias
也需按真实 schema 解析。这部分预计使用 Spark/等价图批处理，尚未实现全量映射。

映射完成后导出的候选表才类似：

```json
{
  "origin_url": "https://github.com/example/net-project",
  "snapshot_id": "snap-1",
  "branch_name": "refs/heads/main",
  "branch_tip_revision_id": "rev-new",
  "revision_id": "rev-old",
  "root_directory_id": "dir-root",
  "p4_directory_id": "dir-src",
  "p4_path": "src/main.p4",
  "p4_content_id": "blob-p4",
  "revision_relation": "ancestor_of_branch_tip"
}
```

`root_directory_id` 和 `p4_directory_id` 不能混用。一个子目录可以被多个根目录
引用，一个 revision 也可能关联多个 origin；所有对应关系都应保留。

## Case 3：映射时下载了什么？后面源码从哪里来？

| 阶段 | 数据来源 | 本机得到什么 |
| --- | --- | --- |
| 文件名筛选 | SWH Graph S3，AWS 端读取 ORC | `.p4` 命中结果分片 |
| 目录/commit/分支/origin 映射 | SWH Graph 关系表，AWS 端处理 | 候选 manifest、必要路径索引 |
| P4_16 判断 | SWH content_id 对应内容/API或已核验的 GitHub 精确 commit | `.p4` 原始内容，按需获取同版本 include |
| 完整项目物化 | GitHub 指定 commit 归档优先；SWH 内容作为回退 | 完整 tree、文件 payload、校验与缺失报告 |

云端查询结果写到：

```text
s3://swh2p4/athena-results/
```

这个桶是你自己的**结果桶**，不是 SWH 源码桶。导出候选表到这个桶，再下载
结果表到本机，不意味着把 ORC 全部下载。历史候选表/目录索引体量需统计，不能
在查询前保证它们一定很小。

StarCoder2 使用了 SWH content S3 bucket 下载文件内容。已验证公共桶的
`content/<sha1>` 存放 gzip 压缩正文，可匿名读取；Graph content ORC 表提供
`sha1_git` 到 `sha1` 的映射。已实现单 ORC 分片 Glue 映射和 content S3
批次筛查，2026-10-20 完成有上限试跑；128 分片全量尚未运行。

例如从 SWH API 获取一个精确内容对象，接口形态是：

```text
https://archive.softwareheritage.org/api/1/content/sha1_git:<真实content_id>/raw/
```

如果从 GitHub 获取完整仓库，需固定实际 Git commit，并核验与 SWH 所选 revision/
文件哈希的对应。直接下载“今天的 main”可能与命中的旧版本不同。

## Case 4：最新版本没有 P4，还收集吗？

| origin | revision | 状态 | 处理 |
| --- | --- | --- | --- |
| net-project | rev-2023 | `.p4` 全是 P4-14 | 保留审计，不准入 |
| net-project | rev-2024 | 有 P4_16 | 准入候选 |
| net-project | rev-2025 | 有 P4_16，提交时间更晚 | 默认物化此版本 |
| net-project | rev-2026 | `.p4` 已全部删除 | 记录当前状态，不能因此删除旧候选 |

推荐策略叫 `latest-p4-16`：在**内容判定后**选择最新的 P4_16 版本，而不是
先取最新版本再决定是否整个 origin 都应排除。它是待实现的方案名称，当前 CLI
没有这个选项。revision 提交时间与 visit 抓取时间要分别记录，异常时间进入复核。

“保留候选 revision”是保存表里的关系记录，不等于下载全部历史项目。
同一 origin 可从最新候选依次检查；找到了已确认的 P4_16 版本后，默认只物化
一个完整快照，历史候选关系仍保留。若要全部历史 payload，另开历史模式。

只有 feature 分支含 P4_16 时也如此：不能在发现阶段按 `main/master` 提前删除它。
StarCoder2 的 latest-main 策略适合另一个“当前默认分支”视图，但召回更窄。

## Case 5：编译失败就不是 P4_16 吗？

不是。语言版本和编译目标分开记录：

| 内容/编译情况 | raw 收集 | BMv2/v1model 正数据集 |
| --- | --- | --- |
| 明确 P4_16，BMv2 编译通过 | 保存整个所选仓库 | 可以进入 |
| 明确 P4_16，但采用 PSA/PNA/厂商架构 | 保存整个所选仓库 | 需对应目标，不能直接进入 |
| 明确 P4_16，但源代码有缺陷 | 保存 raw 与错误证据 | 不进入 |
| include 缺失，版本证据不足 | 保留候选并补依赖/复核 | 暂不进入 |
| P4_16 include 片段，不能独立作为 main 编译 | 根据语法/引用证据确认版本 | 不能直接作为独立可编译程序 |
| 仅 P4-14，或只有空 `.p4` | 保留审计，不准入 | 不进入 |
| 注释中提到 v1model，但没有实际 P4_16 语法 | 不凭字符串准入，进入复核 | 不进入 |

Generating P4 Dataplanes 的 P4-14 正则可用于初筛；PINC 的依赖解析和编译验证可
用于复核与可用性标记。去掉注释干扰、记录证据；编译成功是强证据，编译失败
则应分类。不能要求所有含 P4_16 仓库都通过 BMv2，否则违背高召回目标。

当前 `smoke_swh_api.py` 的分类器仍为正则，`collect_swh_p4.py` 尚未调用 P4C
复核；当前它会把 unknown/conflicted 候选写入 rejected 记录，后续需支持复核重试。

## Case 6：P4 只占 1%，整个样本是什么？

假设 `rev-2025` 包含以下完整树：

```text
net-project/
├── README.md
├── LICENSE
├── Makefile
├── src/
│   ├── main.p4
│   └── controller.py
├── include/
│   └── headers.p4
└── docs/
    └── design.md
```

只要至少一个 `.p4` 被确认是 P4_16，整个项目都进入 raw，包括控制面代码、
文档、构建文件和其他 tracked 文件。若实际有 100 个文件而只有 1 个 P4_16，
仍保存全部 tracked 文件，比例不参与准入。

完整 tree manifest 每行表示一个路径，例如：

```json
{"path":"src/main.p4","type":"file","content_id":"blob-p4","perms":"100644"}
{"path":"src/controller.py","type":"file","content_id":"blob-python","perms":"100644"}
{"path":"README.md","type":"file","content_id":"blob-readme","perms":"100644"}
```

注意它与**扫描任务 manifest**不同：一个记录项目路径，一个记录 ORC 查询进度。
完整 tree manifest 也不等于全部文件已下载。应另有 `materialization_status` 和
缺失清单；有超限、深度截断或下载失败时标记 `partial`。

“完整项目”指该 commit 的 tracked 快照，不是全部 `.git` 历史。LFS 外部对象、
子模块源码、权限和符号链接单独记录/处理。轻量模式可跳过大 payload，但不能
称为完整下载。当前代码仍默认 10 MiB 文件上限与 64 层遍历，完整性判定待完善。

## Case 7：去重如何省下载而不丢仓库？

```text
origin-A/rev-1 ─┐
               ├─ root_directory_id=dir-shared
origin-B/rev-2 ─┘
```

根目录哈希相同，完整项目内容只存一份，但仓库清单保留两行来源：

```json
{"origin_url":"https://github.com/example/project-a","revision_id":"rev-1","root_directory_id":"dir-shared","payload_ref":"trees/dir-shared"}
{"origin_url":"https://github.com/example/project-b","revision_id":"rev-2","root_directory_id":"dir-shared","payload_ref":"trees/dir-shared"}
```

相同 content_id 的 `.p4` 也只下载/语言初筛一次，但每个路径关系都保留。
编译缓存需包含依赖/目标信息：相同 main.p4 在不同版本的 include 上可能表现不同。
MinHash（Generating P4 Dataplanes 使用 Jaccard 0.85）以后用于训练视图，不能
因两个文件相似就删除一个 raw 仓库。PINC 正文只说明去重，未给出算法/阈值。

## Case 8：分片、manifest 和 resume

把 128 个 ORC 对象各作为一个工作单元，例如：

```text
对象 1 → 查询成功 + 结果已保存 → complete
对象 2 → 查询仍在运行         → running
对象 3 → 查询失败             → failed
对象 4 → 尚未提交             → pending
```

不是把 ORC 拆成新的文件，也不是下载 128 个 ORC 到本机。

设计中的任务 manifest 示例（演示值）：

```json
{
  "graph_release": "2026-06-04",
  "source_bucket": "softwareheritage",
  "source_key": "graph/2026-06-04/orc/directory_entry/directory_entry-001.orc",
  "etag": "example-etag",
  "size_bytes": 232839248651,
  "status": "complete",
  "aws_account_id": "account-A",
  "region": "us-east-1",
  "query_execution_id": "example-query-id",
  "bytes_scanned": 198765432100,
  "result_s3_uri": "s3://swh2p4/athena-results/example-query-id.csv",
  "local_result_path": "p4_hit_shards/object-001.csv",
  "result_sha256": "example-sha256",
  "hit_rows": 12345
}
```

恢复步骤：

1. 对象 1：核验 source key/ETag/大小及本地结果可用，跳过查询。
2. 对象 2：按保存的 query ID 在原账号查状态，不能重新提交。
3. 对象 3：先查失败原因和已计费扫描量，修正后重试。
4. 对象 4：在批准的累计预算内提交。

若查询成功而结果下载失败，仅重试下载，不重新扫描 ORC。换账号之前保留
manifest/本地结果，解决旧桶访问权限；新账号用自己的结果桶处理未完成工作。
旧账号 running 查询单独处理，不能跨账号继续半条 Athena 查询。

2026-10-16 已通过真实扫描验证 `$path` 单对象过滤，并完成全部 128 个 ORC 分片。
执行器使用幂等 token、QueryExecutionId 和本地/S3 双 manifest 恢复；不能依靠
`LIMIT` 控制费用这一约束仍然成立。跨账号只能处理尚未提交的分片，不能接续旧账号
中间的一条 Athena 查询。

## 当前可执行边界

| 功能 | 当前状态 |
| --- | --- |
| AWS/S3 只读核验 | 完成 |
| 本机 local-s3 扫描与对象 checkpoint | 已有，非本次推荐生产路线 |
| Athena 命中 SQL/CSV 导入 | 128 个 ORC 文件名扫描完成；导入器已有 |
| Athena 单对象调度/扫描上限/resume | 已实现并完成全量实跑 |
| 目录祖先、revision 历史、origin 全量映射 | `data/tools/swh_glue_graph_mapper.py` 已实现；需用实际 Graph schema 做 AWS smoke test |
| 本地候选目录递归、正则 gate、文件物化 | 已有，仍受默认深度/大小限制 |
| P4C 复核、latest-p4-16、完整性审计 | 待实现 |
| GitHub exact commit 或 SWH content S3 下载 | A 中待实现；content S3 样本已验证可匿名读取 |

现有 collector 接受已生成的两个输入：`--candidates`（origin/revision/root）和
`--directory-entries`（需要的完整目录闭包）。`--selection-policy all` 可处理全部
候选，但会物化全部通过的候选，不等同于目标策略 latest-p4-16。

总体约束、论文参考和阶段划分见
[SWH_P4_仓库级原始数据收集方案.md](SWH_P4_仓库级原始数据收集方案.md)。
