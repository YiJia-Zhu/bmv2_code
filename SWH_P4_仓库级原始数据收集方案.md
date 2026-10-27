# P4 仓库级原始数据收集计划（SWH 主线）

**目标：** 尽可能全面地收集“曾经有一个确定 revision 包含至少一个 P4_16 `.p4` 文件”的 GitHub 仓库。样本单位是仓库/项目目录在某个确定 revision 下的完整代码上下文，不是单个 `.p4` 文件。文件名 `.p4` 只负责发现候选，P4_16 内容判定决定仓库是否准入；即使 P4 文件只占仓库 1%，也要保存整个仓库。发现阶段保留所有候选 revision，物化阶段默认选择每个 origin 最新的 P4_16 revision；需要历史数据时再开启 `all` 模式。

本计划以 Software Heritage Graph 为发现主索引，目标是最大化历史和已删除仓库召回。BigQuery GitHub 文件索引的覆盖不足以承担本目标，Plan B 已从当前执行路线排除；其隔离代码和历史试验记录保留，不作为本计划的覆盖率依据。

## 文件命中主表与内容缓存（2026-10-19 核对）

**仓库发现的主表始终是 `(directory_id, name, content_id)` 文件命中表。**
`unique_content_ids/` 只用于避免重复下载和单文件判定，不能替代命中主表，
不能从每个 `content_id` 任取一个目录或一个仓库。

| 已完成结果 | 实测数量 | 后续用途 |
| --- | ---: | --- |
| `source_rows/` | 14,060,834 行 | 保存原始 CSV、记录序号及来源 ORC，供审计关联 |
| `unique_hits/` | 14,060,834 条三元组 | 仓库发现主表；本次没有完全重复三元组 |
| `unique_content_ids/` | 143,211 个 | 若需获取源码，按内容缓存下载与判定 |
| `unique_parent_directory_ids/` | 83,299 个 | 筛查前直接父目录索引；不是仓库数 |

三元组没有重复是本次实际结果；相同 `.p4` 字节可以出现在不同目录对象、
文件名和 revision 中，所以不能只留一个 content ID 对应的目录。反过来，
相同目录对象也可能被多个 revision/origin 复用，因此一条三元组也不等于一个仓库。

去重作业已完成，但三元组完全重复数为 **0**，所以它没有缩小仓库映射的
文件命中数；143,211 个内容 ID 也不是仓库数。它只把未来潜在的逐命中
重复内容获取压缩为按内容获取，不能证明 Graph 会更快。文件名筛选本身
是必要的发现入口：它从 SWH 全量目录项中找出候选 `.p4` 边，但文件名
不证明内容为 P4_16。

如果开展单文件判定，正确的交接关系是：

```text
14,060,834 条文件命中
  └─ 提取 143,211 个 content_id → 每个内容只下载/筛查一次
                              ↓
             按 content_id LEFT JOIN 回全部文件命中
                              ↓
             14,060,834 条带注释的三元组
               ├─ 可能含 P4_16 / 未检查 / 未知 → 仓库发现输入
               └─ 确认非源码 → 审计清单
                              ↓
       合格命中的父目录共用 Graph 查询，路径和来源全部保留
                              ↓
       目录关系关联回每条命中 → 所有相关 revision/origin
```

内容注释关联前后命中数必须一致；各分流清单的三元组必须互斥且并集等于
主表。一个内容如果出现在 100 条命中中，判定一次后必须保留 100 条关系。
版本线索与训练质量标记保留在注释中；质量差本身不能从仓库发现输入剔除。

## 当前决策与状态

| 路线 | 发现范围 | 当前状态 | 下一步 |
| --- | --- | --- | --- |
| SWH Graph | SWH 2026-06-04 快照中可追溯的公开 origin，包括历史 revision | 128 个 `.p4` 命中 CSV 已完成去重：14,060,834 条唯一三元组、143,211 个唯一内容 ID、83,299 个唯一父目录；尚无仓库 URL | 小样本验证内容获取与图反查的成本，随后批量映射命中；内容质量仅作训练标记 |
| Plan B | BigQuery/GHArchive/GitHub 候选 | 因文件索引覆盖不足，已从当前执行路线排除 | 不用于估计或声称 SWH 主线的收集覆盖率 |

“所有”在本计划中指所用 SWH 快照中能追溯到 origin、能读取内容且可确认 P4_16 的公开仓库。SWH 未收录、缺少 origin 关系或无法取得内容的仓库不可能由这份快照保证覆盖，必须分别计数。
**当前决策：不以训练质量规则缩小仓库发现范围，也不把全量单文件筛查
设为 Graph 映射的前置条件。** 之前的“质量优先首轮映射”只能产生部分仓库，
而且没有实测证明会降低目录 ORC 扫描费用。是否在映射前排除任何内容，
先以小样本测量获取成功率、误排风险和关联回命中后的唯一父目录减少量；
未知、获取失败和仅凭规则疑似 P4_14 的内容仍保留在发现候选中。

### 接下来的执行顺序与成本闸门

1. **已完成：合并、去重并量化候选。** 128 个 CSV 合计 3,366,228,106 字节。Glue 作业于 2026-10-19 成功生成全部来源行、唯一 `(directory_id, name, content_id)` 命中、唯一直接父目录 ID 和唯一 `content_id`；来源行通过 `source_csv/source_row` 保留。14,060,834 条原始命中均为不同三元组，得到 143,211 个唯一内容 ID 和 83,299 个唯一父目录。结果和日志见 [dedup_20261019_summary.json](data/raw/swh_p4/discovery/dedup_20261019_summary.json)。下一步才是抽样测量内容获取成功率和 P4_16 比例。
2. **小样本验证内容获取是否值得前置。** 从唯一内容 ID 试取少量 `.p4` 字节，记录成功率、限速、耗时和版本/非源码分布；将样本结论关联回对应三元组，统计真正减少了多少唯一父目录。14,060,834 条命中只有 83,299 个唯一父目录，减少命中行数不等于减少 Graph 工作量。若没有显著且可靠的父目录减少，跳过全量前置筛查；后续按内容 ID 缓存判定即可。质量规则仅标记训练资格，不作为仓库发现的硬排除。
3. **验证反向查询与一条真实映射。** 从原属 0 号 CSV 的少量真实命中选样，核验 `directory -> revision -> snapshot -> origin URL` 的证据链。此处的“一条”只是展示一条完整结果，不表示为这一行单独启动 Spark 或扫描 ORC。先查明 SWH compressed graph/服务是否提供可批量使用的反向边访问；若没有，正确性验证也必须放进共享扫描的小批次，不能为单行支付一次约 27.079 TiB 的全表扫描。再用有界样本比较原始 ORC 批量连接和一次性反向索引的构建、存储及查询成本；索引也要读完整目录 ORC 并重排写出，不能预设更省钱。
4. **批量映射，不按行查询。** 默认以全部 `unique_hits/` 三元组为发现输入，用其唯一父目录 ID 作为共同 frontier 与 `type=dir` 父子边连接。每向上查一层，只对新得到的父目录 ID 去重以避免重复查询；保留每个原始目录的所有父路径，最后将祖先关系连接回全部文件命中和 CSV 来源。只有测得安全且显著的父目录减少，才另做经筛查的试验批次，并保留其余命中供完整覆盖。目标是在资源容许时让 128 个 CSV 命中共用同一批图扫描；若实测内存/扇出不允许，再按可承受的批次拆分并计入重复扫描费用，绝不按行拆分。减少 frontier 不保证减少 Graph ORC 的全表读取；现有 `edges.persist()` 也只是尝试缓存，是否能避免后续重读尚无实测。
5. **连接 revision、snapshot、origin 并审计漏口。** 根据祖先目录关联 revision 根目录，再沿可验证的历史/分支/visit 关系取得所有相关 origin；保存文件、路径、revision、snapshot 和 URL 的证据链。8 层目录上限可用于试跑；触及上限或没有映射的候选分别输出，后续继续追溯，不能标记为“全部仓库已找到”。revision 深度、alias、release 和缺失 origin 也需要独立统计。
6. **获取候选仓库，再确认 P4_16。** 单文件规则只决定优先级；获取有 P4_16 可能性的候选 revision 的完整原始快照后，再利用同 revision 的依赖、架构和编译/解析证据复核。不确定项保留待处理。对每个 origin 选择最新的已确认含 P4_16 的 revision，保存完整树、源码、内容哈希、SWH ID、URL 和下载 provenance，并按内容/树去重而保留多个 origin 别名；候选原始快照与最终准入状态分开记录。

每个付费试跑应预先固定 worker 数、硬超时、最大计算费用、输入分片和输出前缀，记录完成的 Spark task、每层 frontier、shuffle、读写字节及 DPU 秒；达到上限就停止并根据结果决定是否扩大。现有的 7.5 小时/$66 外推仅对应旧版程序的一个目录扫描 Spark stage，不是单行、单 CSV 或全量完成时间。128 个 CSV 的完成时间和费用仍须由真实批量试跑推断；此前“3--7 天/$600--$1,500”只能当资源消耗情景，不能当完成承诺。CSV 规范化现可按分片 resume，图映射现仅能从完整阶段恢复；无需为每一行设计 checkpoint。

## P4_16 准入目标

目标流程如下；当前执行范围为 SWH 主线：

```text
保留全部 `(directory_id, name, content_id)` 命中及 CSV 来源
  -> 小样本测量内容获取和图反查的可行性/成本
  -> 批量映射命中目录到 origin/revision，保留全部路径关系
  -> 按唯一 content_id 获取 `.p4` 并做版本判定，关联回全部三元组
  -> 获取有 P4_16 可能性的候选 revision 的完整原始快照
  -> 结合该 revision 的 include/架构和 P4C/人工证据复核版本
  -> 至少一个文件严格判定为 p4_16 才进入最终仓库集
  -> 每个 origin 默认选择最新的 P4_16 revision
  -> 保存选中快照的完整 tree 和 provenance，另建派生训练视图
```

`p4_16_conflicted`、`p4_14` 和 `unknown` 都不自动准入。`.p4` 数量和占仓库比例不参与过滤。

A 的目标覆盖 SWH 中可追溯到 GitHub origin 的历史/分支候选。SWH 没有收录、内容缺失、
没有 `.p4` 后缀或 origin 无法追溯的项目，都属于已知覆盖边界。

## 官方 graph 截止时间

截至 2026-10-09，Software Heritage 官方 graph dataset 页面列出的最新完整
graph 快照是 **2026-06-04**，上一版是 **2026-03-02**。官方页面：

<https://docs.softwareheritage.org/devel/swh-export/graph/dataset.html>

2026-06-04 版本提供 Apache ORC columnar tables（约 33 TiB）和 compressed
graph（约 16 TiB）；2026-03-02 版本分别约 30 TiB 和 16 TiB。这里的日期是
graph export 的数据快照日期，不是“所有 GitHub 仓库都抓取到该日期”：每个 origin
仍有自己的 visit 时间，且新仓库可能尚未被 SWH 抓取。

StarCoder2 论文使用的是旧的 **2023-09-06** graph，不能代表当前 SWH 覆盖范围。
本项目的主线应优先使用官方最新 graph。筛前内容获取先用 content ORC 将
`sha1_git` 映射到普通 `sha1`，再从公共 content S3 获取 gzip 正文；SWH REST
API 留作缺失或错误回退。内容获取不能替代 graph export 的仓库关系映射。

## S3 与“不要下载完整 Graph”

S3 是 Software Heritage 存放大规模 Graph 文件的对象存储，不是逐个仓库的 REST
接口。路径例如：

```text
s3://softwareheritage/graph/2026-06-04/orc
```

其中 `softwareheritage` 是存储桶，`graph/2026-06-04/orc` 是对象路径前缀。完整
2026-06-04 ORC 约 33 TiB，不能也没有必要全部下载到本机。生产方案使用 AWS
Athena/Spark 读取源 S3，只把候选清单、目录索引和命中的仓库内容
保存到本地。`s3://softwareheritage` 是源 Graph；用户的
`s3://swh2p4/athena-results/` 只保存查询结果、manifest 和候选表，不会自动包含源码。

默认先用全部 `(directory_id, name, content_id)` 命中在 AWS 中处理 Graph
关系；之后按唯一 `content_id` 获取候选 `.p4` 并生成单文件注释，再将注释
关联回全部命中和仓库候选，而不是下载完整 ORC 到本机。候选表大小
需要实测：保留历史关系可能产生大量行，不能事先保证很小。源码获取分两步：

1. 仓库准入判定前：按唯一 `content_id` 获取 `.p4` 内容，当前程序支持 SWH REST API、本地缓存和已映射哈希的 content S3；
2. 准入后：完整仓库优先尝试 GitHub exact commit/codeload，失败时使用 SWH 内容重建。

已验证公共桶 `s3://softwareheritage/content/<sha1>` 存有 gzip 压缩的文件正文，
可匿名读取。2026-10-20 用一个已从 SWH API 获取的 `.p4` 文件核验：对象大小
1,448 字节，解压后 6,710 字节；解压内容的普通 SHA-1 与对象键一致，Git blob
SHA-1 与候选 `content_id` 一致。`content/<sha1_git>` 对该样本返回 404。
`graph/2026-06-04/orc/content/` 的 schema 同时有 `sha1_git` 与 `sha1`，可用于
批量映射；抽查一个 637,952 行的 ORC stripe，`data` 列全部为空，因此该 ORC
表不能直接当作源码正文。全量正文下载应先在 AWS 上批量连接候选 `sha1_git` 与
content ORC 表取得普通 `sha1`，再从 `content/<sha1>` 读取、解压和双哈希校验。
已实现单 ORC 分片的 Glue 哈希映射试跑和 S3 正文筛查，尚未运行 128 分片全量。
2026-10-20 的单分片作业找到 1,053 个候选 ID 映射，耗时 430 秒、860 DPU 秒；
从这些映射中抽取的 210 个不同文件全部从 S3 取回并通过双哈希校验。
4 并发的 100 文件批次耗时 22.0 秒，16 并发的另一 100 文件批次耗时 11.7 秒，
均含检查与上传。`content` ORC 共 128 个对象、约 2.889 TiB；按单分片线性
外推约 15.29 小时串行 Glue 时间和约 13.45 美元 Glue 计算费（us-east-1
2026-10-20 Price List API 的 0.44 美元/DPU-小时），不含其他费用或重试。
正文阶段按 4/16 并发样本线性外推约 8.75/4.66 小时，样本量有限且不含首次
读取索引的等待，不能视为全量保证。详情见
`data/raw/swh_p4/discovery/content_s3_pilot_20261020_summary.json`。全量前需按
ORC 分片建立 checkpoint 调度，汇总全部映射并核查未命中、S3 错误及对象缺失。

不能把 Graph 命中表直接当成仓库源码，也不能下载 GitHub 当前默认分支来代替命中的
SWH revision。

SWH 官方提供的是“截至某日的完整归档快照”，没有一个现成的“只含 2017 年以后
revision”的完整 Graph。对 `revision` 做日期过滤可以减少候选数量和内容下载量，
但不能保证按同样比例减少 `directory_entry` 的扫描，因为目录对象会被不同时间的
revision 共享。完整召回仍需要读取候选 revision 所需的目录闭包。

官方还列出过约 2 TiB 的 `history and hosting` 压缩子图，它主要包含 origin、
snapshot、revision 和根目录关系，不包含大部分嵌套目录和文件内容。它适合先筛选
GitHub 最新主线 revision，不能单独判断目录中是否有 `.p4`；最终仍要读取完整
目录闭包。

P4-16 稳定规范标记 `P4-16-v1.0.0` 对应 2017-05-23，但高召回模式默认不使用
revision 年份硬过滤：实验代码可能更早，提交日期也可能异常。若主动启用时间窗口，
必须记录可能漏收；时间条件不能替代内容判定，也不能直接减少目录表扫描范围。

## A 方案：SWH Graph 全量主线

A 方案采用：

```text
Software Heritage graph / export
  -> 扫描 directory_entry ORC 分片，找到 basename 以 .p4 结尾的 file
  -> 保留全部 .p4 命中三元组及来源，提取唯一父目录供批量图查询
  -> 将目录关系关联回所有命中和相关 revision/origin
  -> 按唯一 content_id 缓存单文件判定，再关联回全部三元组/仓库候选
  -> 保留候选 revision manifest，不在这里过早只留 latest-main
  -> 获取待复核候选 revision 的完整原始仓库快照
  -> 利用同 revision 的 include/架构复核 P4_16
  -> 每个 origin 选择最新的 P4_16 revision
  -> 保留所选完整仓库树与文件及审计信息
  -> 生成 repository-context 训练视图
```

当前代码状态：`discover_swh_p4.py` 提供本机 S3 ORC 扫描和 Athena 结果导入两种
发现模式；`swh_glue_graph_mapper.py` 提供 Glue/EMR Spark 的目录反向闭包、revision
历史可达、snapshot/release、origin visit 和 URL 映射，并按 run 输出 Parquet manifest。
`collect_swh_p4.py` 已实现候选 revision 选择、目录递归、`.p4` 内容读取、基于正则的
P4_16 准入、tree manifest 和候选级 checkpoint。默认先只读取 `.p4`，只有
`contains_p4_16=true` 才请求并物化其他仓库文件；未知/P4_14 版本只保留审计记录。
这里的 `contains_p4_16` 仍由正则产生，不能代表本文要求的最终确认；
要支持先获取待复核候选仓库，采集器还需改造，目前不可直接按新流程全量运行。

两种发现模式使用同一中间格式，但都不能省略 Graph 映射：一个
`directory_id/name/content_id` 命中只是 `.p4` 的直接父目录，还必须映射到相关
revision、origin 和根目录，并导出该根目录的完整 directory entry 闭包。Athena
结果如果已经包含 `origin_url/revision_id/root_directory_id`，导入器可直接生成候选；
否则会明确标记 `collector_ready=false`，不会把命中目录冒充仓库根目录。

**仍待实现**：把候选 `.p4` 内容的 P4_16 复核结果回写 Spark manifest、
`latest-p4-16` 的正式物化策略、GitHub 指定 commit 下载、完整性核验。Spark 图映射
作业已经实现，但必须先用实际 Graph release 做 schema smoke test；`latest-p4-16` 和
`window` 是方案名称，当前 CLI 不支持这两个参数；当前仅支持 `latest-main`、
`latest-any`、`all`。不能把下面的设计示例当成已生成的扫描结果。

GHA / GHArchive 不作为主发现入口。它不是文件名索引，也不是源码库，不能回答“所有仓库里哪些含 `.p4`”。GHA 只做辅助：默认分支线索、license 元数据、issue/PR 语料、活跃度、历史 push 时间线。

GitHub API 不作为主发现入口。它只用于 GitHub origin 的元数据补充、当前可访问性检查、默认分支确认，以及在 SWH revision 可对应到 Git commit 且 GitHub 仍可访问时作为下载加速来源。

BigQuery 当前不参与仓库发现、交叉召回或覆盖率估计。

### 分片与 manifest

2026-10-10 只读 listing 核验：该快照的 `directory_entry` 有 128 个 ORC 对象，
共 29,773,222,404,015 bytes（29.773 TB / 27.079 TiB）。这是目录表，不是整个
33 TiB Graph。对象总大小与 Athena 实际扫描字节数不同，列裁剪效果需试查确认。

分片不是把 ORC 下载到
本机，而是把每个 S3 对象作为一个独立 Athena 工作单元。每个工作单元完成后写入
manifest，例如（文件名、query ID、扫描量和命中行数为演示值）：

```json
{
  "source_key": "graph/2026-06-04/orc/directory_entry/directory_entry-001.orc",
  "etag": "...",
  "size_bytes": 232839248651,
  "status": "complete",
  "query_execution_id": "abc123",
  "aws_account_id": "account-A",
  "region": "us-east-1",
  "result_bucket": "swh2p4",
  "result_key": "athena-results/p4-hits/part-0001.csv",
  "local_result_path": "p4_hit_shards/part-0001.csv",
  "result_sha256": "example-sha256",
  "bytes_scanned": 198765432100,
  "hit_rows": 12345
}
```

设计状态为 `pending -> submitted/running -> query_succeeded -> complete`，失败为
`failed`。只有查询成功且结果已经下载/校验，才标记 `complete`。重启时先用保存的
query ID 检查 `submitted/running/query_succeeded`，不要直接重提；`failed` 的部分
扫描可能已计费，不能把失败次数当作零成本。

`complete` 仅在 source key/ETag/大小一致、结果文件可用时跳过。换账号前保存本地
manifest 和结果分片，解决旧结果桶的读取权限；新账号处理剩余对象并写自己的桶。
旧账号未结束的查询需单独处理，不能在新账号自动接续，也不能直接重复提交。

2026-10-16 已验证并完成单对象执行：`WHERE "$path"=...` 将每条查询限制到一个
ORC 对象，128 条查询全部成功。总扫描量为 10,509,202,350,395 bytes，稳定命名的
CSV 结果共 3,366,228,106 bytes，保存在
`s3://swh2p4/athena-results/swh-p4-20260604/p4-directory-hits/`。本地和 S3 manifest
均记录 source ETag/大小、query ID、扫描量、耗时和结果对象；同一 `--resume` 命令
复跑会核验并跳过 128 个 `complete` 分片。

### A 方案暂停点与恢复条件

本机通过公开 S3 HTTP/Range 读取不需要 AWS 账号，但现有链路实测远低于目标吞吐；约 27 TiB 的 `directory_entry` 即使持续 50--100 MB/s，单次理想顺序读取也约需 3.4--6.9 天，图映射还可能需要多轮读取。因此正式方案使用 AWS 远程扫描，不在本机直接扫完整 ORC。

恢复 A 方案前需要准备：

1. AWS 账号已准备；总预算、累计扫描预算和单查询上限分别核验；
2. 与 SWH 数据相近区域的 Athena，以及调用方拥有的查询结果 S3 bucket；（已准备）
3. Athena 先做 ORC schema、列裁剪和单对象 `.p4` 命中扫描；
4. Glue/EMR Serverless Spark 或等价分布式任务，完成命中子目录到根目录、revision、snapshot、origin 的图映射；
5. 只把候选表和候选根目录的 `directory_entry` 闭包下载回本机；
6. 先对极小范围 dry-run/抽样估价，经人工确认后再运行全量任务。

Athena 不是完整流程的唯一组件：它适合第一次 `.p4` 列式扫描，但多层目录祖先映射更适合 Spark。不得在只有 `directory_id/name/content_id` 时把 `.p4` 的直接父目录冒充仓库根目录。

2026-10-16 已建立 `swh-p4-pilot` 工作组：Engine 3、强制 SSE-S3 结果配置、
单查询扫描上限 400,000,000,000 bytes，原始查询结果写入
`s3://swh2p4/athena-results/swh-p4-20260604/_athena-query-results/`。文件名扫描已经
完成；后续预算仍需覆盖目录/历史映射、结果存储和内容获取，不能只计算第一次扫描。

2026-10-16 的 Glue 单目录父级反查探针表明，直接对完整 `directory_entry` 前缀执行
`target = <directory_id>` 不能依赖 ORC 谓词下推完成低成本随机反查。修复公共桶
读取权限后，`G.1X x 2` 的任务在 15 分钟硬上限超时，消耗 1,801 DPU 秒且没有产生
完整输出。因此不得提高资源后盲目重试，也不得把该查询按候选目录重复执行。下一步
应评估 SWH compressed graph，或构建一次可复用的 `target -> parent directory` 反向
索引；任一正式任务必须按独立分片写结果和 manifest，支持从已完成分片恢复。

## 已排除的 Plan B：BigQuery + GHArchive + GitHub

B 已迁移为独立流水线，但因候选覆盖不足，不纳入当前收集路线。本节只保留历史位置，不作为执行指令：

- 人类说明：`data/pipelines/bigquery_gharchive_p4/README.md`；
- 机器可读状态、阶段和安全门槛：`data/pipelines/bigquery_gharchive_p4/pipeline.json`；
- 主程序：`data/pipelines/bigquery_gharchive_p4/collector.py`；
- 独立运行数据：`data/raw/gharchive_p4/`。

若将来单独研究 B，其状态以 `pipeline.json` 为准；当前 SWH 主线不依赖 B 的结果。

## 数据来源分工

| 来源                              | 在本方案中的角色                                                                 | 不承担的角色                                                   |
| --------------------------------- | -------------------------------------------------------------------------------- | -------------------------------------------------------------- |
| Software Heritage                 | 主源码索引和主溯源；提供 content、directory、revision、snapshot、origin 的图关系 | 不直接保证某个`.p4` 是 P4_16；不天然等同当前 GitHub 默认分支 |
| GitHub API / codeload / git fetch | 对 GitHub origin 做元数据补充和可选快照下载加速                                  | 不做全量`.p4` 发现；不使用 code search 做主发现              |
| GHArchive                         | 默认分支/license/issue/PR/活跃度/历史 push 辅助元数据                            | 不做 P4 仓库发现主入口；不证明仓库含 P4                        |
| BigQuery GitHub public dataset    | 当前计划不使用；现有索引覆盖不足                                               | 不承担全量仓库发现或覆盖率估计                                  |

## 三篇论文可借鉴点

StarCoder2 / The Stack v2 使用 Software Heritage graph 作为源码主体，抽取 GitHub 仓库最近抓取版本，保留主分支，取主分支最新 revision，并按完整目录内容标识 `directory_id` 去重；随后递归重建目录树，再做许可证、语言、质量、PII 和近重复处理。

本项目沿用这些原则，但做 P4 特化：

- 发现条件是路径精确匹配 `*.p4`，而不是通用语言分类；
- P4_16 判定发生在下载候选 `.p4` 内容之后，并作为项目级准入条件；
- 项目级准入要求“至少一个 tracked `.p4` 路径被判定为 P4_16”；
- 不按 `.p4` 文件比例、star 数、fork 数、仓库语言占比过滤；
- raw 层保留完整项目树 manifest 和尽可能完整的代码上下文；
- 文件级质量规则决定派生训练视图的资格，不回写原始 CSV，也不用于
  排除仓库发现候选；版本规则只提供线索，最终准入依赖仓库上下文。

| 论文与依据 | 论文做法 | 本项目采用方式 |
| --- | --- | --- |
| [StarCoder2](ref_paper/starcoder2.pdf)，§2.1 | 最近抓取的 GitHub 默认/main/master 分支最新 revision；根 directory_id 去重；递归目录到 64 层；压缩文件小于 10 MB 才下载 | 借鉴目录/内容哈希和 provenance；主发现扩大到历史/分支；完整模式不照搬深度/大小截断 |
| [Generating P4 Dataplanes Using LLMs](ref_paper/Generating_P4_Dataplanes_Using_LLMs.pdf)，§IV-A | GitHub + The Stack v2；正则排除 P4-14；MinHash，Jaccard 阈值 0.85；保留其他架构代码 | 正则用于初筛；MinHash 只用于训练视图；版本与 BMv2 架构分别标记 |
| [PINC](ref_paper/PINC_P4_and_Intent_for_Network_Configuration_A_Prototype_Using_Large_Language_Models.pdf)，§IV-B2、IV-C | 405 个 GitHub P4_16 程序，解析仓库依赖、去重、许可证过滤、编译验证；保留 raw/cleaned；验证使用 BMv2/v1model | 保留依赖、raw 与编译证据；BMv2 通过集单独导出；该 PDF 正文未给出去重算法/阈值，不替作者补写 |

三篇论文都不能证明我们的扫描已经实现或能收集全部仓库。论文的训练样本也不是
统一的“完整项目作为一条样本”：两篇 P4 论文以文件/程序为主，StarCoder2 先从
仓库抽取文件。我们的 raw 层特意保存完整项目，训练视图以后单独构造。

## SWH 图对象口径

需要记录并可追溯这些对象：

| 对象          | 用途                                                 |
| ------------- | ---------------------------------------------------- |
| `content`   | 文件内容对象；候选`.p4` 文件和项目文件 blob 的来源 |
| `directory` | 目录树对象；用于重建完整项目树                       |
| `revision`  | 类似 Git commit；项目快照绑定到一个确定 revision     |
| `release`   | tag/release；可作为后续版本扩展                      |
| `snapshot`  | origin 一次抓取时的分支/引用状态                     |
| `origin`    | 源码来源 URL，如 GitHub/GitLab/包管理器源码包        |

每个最终 raw 快照必须至少绑定：

```json
{
  "source_kind": "swh",
  "origin_url": "https://github.com/owner/repo",
  "swhid_origin": "...",
  "swhid_snapshot": "...",
  "swhid_revision": "...",
  "swhid_directory": "...",
  "revision_sha1_git": "...",
  "selected_ref": "refs/heads/main",
  "selection_policy": "latest_confirmed_p4_16",
  "contains_p4": true,
  "contains_p4_16": true,
  "repository_retention": "keep_complete_repository_if_contains_p4_16"
}
```

## 候选发现

### 路径规则

主发现条件：

```text
lower(path) matches (^|/)[^/]+\.p4$
```

说明：

- `.p4` 是主发现扩展名；
- `.p4i`、`.inc`、`.h` 等作为 include/support 文件收集，不作为主发现入口；
- 不把任意包含字符串 `p4` 的文件名算作 P4；
- 候选路径只用于发现，不能把下载范围缩小成这些 `.p4` 文件。

### SWH 映射流程

```text
path/name endswith .p4
  -> candidate directory_entry
  -> containing directory_id
  -> referencing revision(s) via root directory
  -> revision_parent 历史可达关系（旧 commit 不一定直接是 branch tip）
  -> snapshot branch/alias/release references
  -> origin(s)
```

同一个 `.p4` content 可能出现在多个目录、多个 revision、多个 origin 中。不能只按 content 去重后丢掉 provenance；必须在 origin/revision 层保留来源。

历史 revision 往往不直接出现在 `snapshot_branch`：例如分支指向新 commit，旧 commit
通过 `revision_parent` 链可达。因此只连接 `revision.id=snapshot_branch.target` 会漏
历史命中；需记录“哪个 snapshot/分支能够到达这个 revision”，并解析 HEAD alias、
tag/release 等引用。目录祖先映射与 revision 祖先映射都可能需要多轮计算。

## Revision 选择策略

以下为目标设计，尚未全部实现。默认不在文件名发现阶段丢弃 revision。物化时每个 origin 默认保存一个“最新的、
已确认含 P4_16 的 revision”；如果目标是复现 StarCoder2 的当前项目视图，才使用
`latest-main` 只检查默认/main/master。两者必须在 manifest 中明确记录。

### 默认主线

对每个 origin 的高召回模式：

1. 收集所有包含命中目录的可追溯 revision 和分支；
2. 记录默认分支、`main`/`master`/`HEAD` 等分支标签，但不提前删除 feature 分支候选；
3. 对每个 origin，在 P4_16 判定后选择最新的 P4_16 revision 作为主物化样本；
4. 如果仍不能确定分支关系，进入 `ambiguous_ref` 队列；GitHub API/GHArchive 只做辅助核验；
5. GitHub 当前 commit 记录为 `github_current_commit`，不覆盖命中的 SWH revision。

### 历史模式

历史版本不进入默认主线，单独配置：

| 模式       | 含义                                                     |
| ---------- | -------------------------------------------------------- |
| `latest-p4-16` | 每个 origin 保存最新的、确认含 P4_16 的 revision（推荐） |
| `latest-main` | 只检查默认/main/master 的最新 revision（StarCoder2 兼容视图） |
| `window` | 每个 origin 按时间窗口或最近 N 个含`.p4` revision 采样 |
| `all`    | 尝试物化所有可追溯含`.p4` revision，成本最高           |

历史快照必须进入独立目录和 manifest，不能覆盖主线快照。

“最新”需要固定规则：优先用 revision 提交时间排序，缺失/异常时记录状态并使用
visit 时间及确定性 tie-break；默认分支只作同等候选的优先线索。多个分支不能简单
认为按 commit hash 大小就能代表最新，visit 时间也不等于 commit 时间。

## P4_16 判定与准入

SWH 路径只能证明候选文件名像 `.p4`，不能证明是 P4_16。先按
[第2步_P4单文件筛查方案.md](第2步_P4单文件筛查方案.md) 只读候选文件做初筛；
有 P4_16 可能性的 revision 获取候选仓库原始快照后，再结合该 revision 的
include、架构和编译/解析证据做最终版本判定。

正向特征（用于预筛选）：

- `#include <core.p4>` 或 `#include "core.p4"`；
- `#include <v1model.p4>`、`#include <psa.p4>`、`#include <pna.p4>`；
- `parser ... packet_in`；
- P4_16 风格 `control`、`struct headers`、`package`；
- `V1Switch(...) main`、`PSA_Switch(...) main` 等 P4_16 架构实例。

P4_14 反向特征：

- `header_type`；
- `fields { ... }`；
- 老式 `parser start { return ... }`；
- 老式 `control ingress { apply(...) }`；
- P4_14 风格 action/table 写法。

Generating P4 Dataplanes 论文的 P4-14 正则规则可以用于预筛选；PINC 论文则使用
`p4c --target bmv2 --arch v1model` 验证 BMv2 可编译性。我们的广义 P4_16 gate
应先检查实际源码语法并按需解决同 revision 的 include；去除注释干扰，记录正反
特征及解析证据。P4C P4_16 解析/前端结果用于复核；完整程序编译成功是强证据，
但编译失败需要分类，不能直接等同于“不是 P4_16”。

本项目收集的是含 P4_16 的仓库，不仅是正确可编译程序。PSA/PNA/厂商架构、
P4_16 include 片段、源代码缺陷和依赖缺失都不应仅因 BMv2 失败而丢弃。
已确认语言版本的源码可以进入 raw；证据不足的 `unknown/conflicted` 保留在复核
队列继续处理。严格 BMv2 正数据集仍要求 `p4c-bm2-ss --p4v 16` 成功，与 raw 准入分开。

输出分类：

| 状态                        | 含义                                                                 | 是否保存整仓库          |
| --------------------------- | -------------------------------------------------------------------- | ----------------------- |
| `contains_p4=true`        | 代表 revision 的完整 tree 中至少有一个 `.p4` 路径                   | 不是最终准入条件        |
| `contains_p4_16=true`     | 至少一个可读`.p4` 文件判定为 P4_16                                 | 是，保存完整仓库        |
| `contains_p4=true` 且版本未知 | `.p4` 存在但内容无法可靠判定                                     | 否，进入复核队列        |
| `contains_p4=false`       | 目标 revision 没有 `.p4` 路径                                      | 否，保留候选审计        |
| `p4_version_unknown=true` | 无法可靠判断                                                         | 否，进入人工/编译器复核队列 |

计划增加 `p4test`/P4C 的 P4_16 解析复核；具体命令/依赖需按本机编译器核验，
当前采集器尚未调用它们。raw 收集阶段不要求 BMv2 编译成功。

## 常见 case（为什么不能提前简化）

### Case A：最新默认分支已经删除 `.p4`

```text
repo-X / rev-old   → src/main.p4（P4_16）
repo-X / rev-new   → 没有 .p4
```

如果只检查 `rev-new`，会漏掉 `repo-X`。高召回模式保留 `rev-old`，把它作为
该 origin 的最新 P4_16 revision；`rev-new` 可以作为当前状态元数据单独记录。

### Case B：只有 feature 分支含 `.p4`

```text
main       → README、C++，没有 .p4
feature/p4 → dataplane/main.p4（P4_16）
```

不能只按 `main/master` 过滤。先保留 feature revision，经过 P4_16 gate 后再决定
物化；`latest-main` 只作为 StarCoder2 兼容的窄召回视图。

### Case C：命中的是子目录，不是仓库根目录

```text
dir-root → src → dir-src → main.p4
```

`dir-src` 只能说明 `.p4` 的直接父目录。必须回到 `revision.directory=dir-root`，
递归得到整个项目，不能把 `dir-src` 当成完整仓库。

### Case D：两个仓库共享同一个目录树

```text
origin-A/rev-1 → root_directory_id=D
origin-B/rev-2 → root_directory_id=D
```

文件 payload 只物化一次，但保留两个 provenance alias；不能因 content 或 directory
去重而删除 origin/revision 记录。

### Case E：`.p4` 文件是 P4-14、空文件或无法解析

文件名命中只进入候选表。确定全是 P4-14/空文件的候选不准入；LFS pointer、版本
证据不足或依赖缺失进入复核。若明确是 P4_16，仅因依赖/目标/源码缺陷而编译失败，
仍可保存 raw 仓库，但不能进入 BMv2 编译成功集。

### Case F：GitHub 已删除但 SWH 仍有快照

候选和内容优先使用 SWH 的 revision/content；GitHub codeload 只作为 exact commit 的
下载加速，不能因为 GitHub 404 就丢弃 SWH 中的项目。

## 整仓库保存原则

只要 `contains_p4_16=true`，保存该 revision 的完整项目树：

- 完整 `tree_manifest`：所有 tracked 路径、文件模式、大小、content hash、子模块/LFS 状态；
- `code_payload`：`.p4`、`.p4i`、include 闭包、C/C++/Rust/Go/Java/Python/脚本、P4Runtime/PTF/Scapy、JSON/YAML/TOML/P4Info/protobuf、构建文件、README/LICENSE；
- 完整模式也获取 tracked 图片、压缩包等非源码文件；轻量模式才允许只保存其元数据；
- `files.jsonl`：一行一个文件，绑定 `snapshot_id`。

“完整”指确定 revision 的所有 tracked 普通文件及符号链接/权限，不是 `.git` 全部
历史或开发机 untracked 文件。子模块引用和 LFS pointer 必须记录，外部 payload
是否获取单独标记；SWH 缺失、超限、深度截断、下载失败必须令
`materialization_status=partial`，不得标记完整。当前脚本默认 64 层、10 MiB 上限且
尚未实现这些完整性状态，属于待修正项；StarCoder2 原文是**压缩文件大小**限制，
与当前脚本的读取字节上限不完全相同。

不得做这些事：

- 不只保存 `.p4` 文件；
- 不因 `.p4` 文件少或占比低而排除仓库；
- 不修改源码来提高编译通过率；
- 不把不同来源的 HF/SFU/p4llms 归一化数据混入 SWH raw；
- 不把文件级过滤结果回写 raw。

## 去重策略

去重分为三层，均保留来源记录：

- `(origin_url, revision_id)` 相同：只保存一份内容，合并发现来源；
- `directory_id` 相同：只保存一份 payload，其他 origin/revision 作为 provenance alias；
- `content_id` 相同的文件内容只下载/初筛一次，但保留每个路径与 revision 的对应；
- 依赖解析或编译不能仅按 content_id 复用结论：同一文件在不同 revision 的 include 可能不同；
- 近重复和 MinHash（P4 论文参考阈值 0.85）只用于训练视图，不删除 raw 项目。

优化原则：不用 GHA/BigQuery 再扫描一遍 A；不在本机重复扫描已由 AWS 完成的 ORC；
不为明显重复 blob 重复下载；不为每个候选历史版本先下载完整项目再决定准入。
只在需要 include/版本复核时获取必要依赖，完整目录和 payload 主要为最终所选快照展开。

## 输出目录

建议使用新的隔离根目录：

```text
data/raw/swh_p4/
├── discovery/
│   ├── swh_schema.json
│   ├── swh_p4_paths.jsonl
│   ├── swh_candidate_revisions.jsonl
│   └── discovery_summary.json
├── manifests/
│   ├── origins.jsonl
│   ├── repositories.jsonl
│   ├── snapshots.jsonl
│   ├── files.jsonl
│   ├── trees/
│   ├── p4_version_annotations.jsonl
│   ├── file_filter_annotations.jsonl
│   ├── repository_filter_annotations.jsonl
│   ├── duplicate_aliases.jsonl
│   ├── errors.jsonl
│   └── checksums.sha256
├── snapshots/
│   ├── swh_latest/
│   ├── github_materialized/
│   └── history/
└── derived/
    └── p4_16_repository_context/
```

## 实施阶段

### 阶段 0：只读预检

- 确认 S3/Graph export 访问方式、磁盘预算和本地临时目录；
- 不下载 33 TiB 完整 Graph，先检查表结构、对象大小和远程查询能力；
- 估算 GitHub origin、主线 revision 和日期过滤后的候选数量；
- 抽样检查 SWH origin 到 GitHub repo 的映射质量；
- 不下载整仓库。

### 阶段 1：候选路径和 revision 索引

- 在 S3 远程按 ORC 对象分片扫描 `directory_entry`，筛选 `type=file` 且文件名以 `.p4` 结尾；
- 该文件名扫描已完成；先统计唯一目录/内容 ID，抽样测量单文件内容获取成本和筛查状态；
- 默认以全部三元组批量映射 directory/revision/snapshot/origin，不按 CSV 行独立扫描全图；按唯一 `content_id` 缓存后续单文件判定并关联回全部三元组，质量规则只用于训练视图；
- 保留所有候选 revision，不把 `.p4` 的直接父目录冒充仓库根目录；
- 在 AWS 端导出候选表和必要路径索引，本机只下载结果分片（体量待统计）；
- 暂不下载完整仓库内容。

### 阶段 2：P4_16 小样本试运行

- 抽取 20 到 50 个候选 origin；
- 从 SWH content 或 GitHub exact commit 获取候选 revision 的原始仓库快照，再使用同 revision include；
- 验证单文件筛查、仓库上下文 P4_16 判定、P4C 错误分类及 manifest 字段；
- 检查低 P4 占比仓库是否仍被保存。

### 阶段 3：主线整仓库导出

- 对单文件阳性和待复核候选分批获取原始快照，完成仓库上下文版本确认；每个 origin 选择最新的已确认 P4_16 revision；
- 保存完整 tree manifest；
- 优先用 GitHub exact commit/codeload 下载仍可访问的完整仓库，失败时使用 SWH 内容；
- 生成 `repositories.jsonl`、`snapshots.jsonl`、`files.jsonl`、校验和和错误报告。

### 阶段 4：辅助元数据

- GitHub API：补 stars/forks/archived/fork/default branch/license/current commit；
- GHArchive：补 license/default branch 线索、issue/PR、活跃度、历史 push；

### 阶段 5：派生训练视图

- 对 raw 快照生成 repository-context；
- 复用单文件阶段已算出的 basic filter 结果；对其他仓库文件补做同类检查；
- 做 license/opt-out/PII/近重复处理；
- 不改 raw。

## 最低验收标准

1. 每个保存的快照都能追溯到唯一 origin、revision、directory 和 discovery path；
2. 每个 `contains_p4_16=true` 快照至少有一个 tracked `.p4` 路径且内容判定为 P4_16；
3. `.p4` 占比不参与准入；
4. `tree_manifest` 覆盖完整项目树，不是 `.p4` 白名单；
5. 同一 `directory_id` 不重复保存 payload，但 provenance alias 完整；
6. 失败和跳过都有可重试原因；
7. GHA、GitHub API 的辅助结果与 SWH 主线 manifest 分开保存；不以它们替代 SWH 发现结果。

## 阅读入口

逐条记录如何变化、源码从哪里下载、resume 如何继续，见
[SWH_P4_仓库发现逻辑示例.md](SWH_P4_仓库发现逻辑示例.md)。
