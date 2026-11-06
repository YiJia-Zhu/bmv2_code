# P4 代码收集 TODO

目标：从 SWH Graph 2026-06-04 中找出曾包含候选 P4_16 文件的 origin URL，
把这批来源的可获取仓库历史、提交元数据、refs、issue/合并请求以及
包/归档历史版本**完整下载到本地**。现有单文件 P4_16 候选筛查已经
完成，不把再次筛查设为下载门槛；下载后核对候选文件与具体版本的
对应关系，另选训练视图。原始历史不得因训练筛选而删减。
另用 GitHub 搜索补充 SWH 未发现的仓库；该补集须先核实 `.p4` 文件和
P4_16 证据，再决定哪些新仓库进入完整历史收集。
`directory_id` 只是文件的直属父目录，不是仓库 ID。

## 当前阶段（2026-11-06）

**SWH 阶段暂时收尾，下一步只推进 GitHub 搜索补集。** SWH 已完成文件
初筛、Graph 映射和四分片仓库历史收集的第一轮；本阶段不再启动 SWH
重试、未判定内容回补或历史版本核对。以下 SWH 统计和缺口继续保留供
将来恢复，不能把“暂时收尾”写成 10,069 个 Git 目标全部下载成功。
GitHub 补集要先复用同一套单文件 P4_16 与质量规则，确认新增来源，
再对需要的仓库收集完整历史。当前不启动批量筛查或下载。

## SWH 阶段记录

| 环节 | 已核实结果 | 尚未完成的边界 |
| --- | --- | --- |
| 文件名与单文件初筛 | 128 个 CSV、14,060,834 条 `.p4` 命中；其中 13,465,757 条 P4_16 candidate，涉及 122,392 个唯一 content ID | candidate 是静态单文件判断，不是实际仓库版本内的最终确认 |
| 质量标注 | 13,390,876 条通过、74,881 条标记不适合独立训练；仓库发现仍使用全部 candidate | 质量标记不用于删除仓库来源证据 |
| 待复核内容 | 历史 `needs_review` 共 202,389 条；3,020 条已识别为空/非源码，199,369 条仍待复核 | 后者未进入现有 candidate D0 映射，当前结果不能称为所有 P4_16 仓库 |
| 目录与 revision | candidate 对应 65,321 个 D0；本地图扩展并收敛于 523,435 个目录、870,763 条去重父边；317,804 个 candidate revision 已反解 ID 并匹配 ORC 记录，其中 2 条原始日期为空 | revision 候选尚未经过仓库上下文中的 P4_16 确认 |
| origin 来源证据 | revision 路径覆盖 64,984 个 D0、10,107 个 URL；全量 release/snapshot 入口审计覆盖 756 个 D0、966 条 D0→origin 关系 | 65,321 个 D0 各有至少一个 URL；合计 **10,201 个已发现的不同 SWH origin URL**，不是规范仓库数或已确认 P4_16 仓库数 |
| 文件回连与源码 | 128 个 candidate 分片已有统一来源视图；10,201 行 origin 清单已读回本机；四分片初轮结束，10,069 个 Git 目标中 9,250 个镜像校验完成 | 201 个 Git 失败、618 个被程序标为不可访问；该标签不等于已证明仓库失效，详见下方错误核查和[结果总览](data/raw/swh_p4/RESULTS_SUMMARY_20261022.md) |

### SWH 仓库镜像未拉取的具体原因（2026-11-06 核查）

以下统计读取四个 `shard_0..3/manifests/state.sqlite3` 的当前 `repos`
记录，仅指 **Git 镜像**，不包含 issue API、LFS、submodule 或包归档。
10,069 个 Git 目标中 9,250 个镜像已校验完成，剩余 819 个：

| 程序状态 | 数量 | 最后一次错误的共同特征 | 能得出的结论 |
| --- | ---: | --- | --- |
| `inaccessible` | 618 | `fatal: could not read Username for 'https://...': terminal prompts disabled`，伴随 `unable to read askpass response from '/bin/false'` | Git 遇到需要认证的响应，且脚本禁止交互输入；**不能仅凭这条消息确定仓库已删除或私有** |
| `failed` | 115 | 接收 pack 时 `RPC failed; curl 56`、`unexpected disconnect`、`fatal: early EOF`、`invalid index-pack output` | Git 数据传输中断，镜像未通过完整性校验 |
| `failed` | 38 | `OpenSSL SSL_connect: Connection reset by peer` | 建立 TLS 连接时被重置 |
| `failed` | 48 | TLS 握手/读取时 `unexpected eof while reading` 或 `gnutls_handshake() failed` | TLS 连接异常结束 |

618 个 `inaccessible` 中 561 个只试了 **1 次**；另 57 个此前有重试，
但最后错误同样是用户名提示。201 个 `failed` 均已达到配置的 **5 次**
尝试。代码按错误文本中出现 `could not read username` 等关键词直接标记
`inaccessible`；其他错误重试到上限才标记 `failed`。因此前一类是
**采集器的错误分类**，不是 GitHub/GitLab 对仓库真实状态的判决。

Git 命令经 `http://127.0.0.1:7892` 代理运行；日志能够证明连接/传输
失败，却不能区分代理、中间网络、远端断开等根因。用户名提示也可能
由仓库私有、迁移/删除、URL 失效或代理/服务端认证响应引起。当前错误
样本没有 `No space left on device`，不能把这批失败归因于磁盘空间。
下一步应对 618 个逐项核对原站 URL/重定向和实际 HTTP 状态，再重试
仍公开可读的仓库；201 个按网络错误重试，并保留每次尝试的完整
`repos/<repo_id>/fetch.log`。在核对前，这 819 个都不能计入成功收集，
也不能把 618 个永久排除。

337 个 D0 的公共 `revision` ORC 独立复查为零命中，但本地图的
`directory→release/snapshot→origin` 路径全部补出 URL。随后对全部
523,435 个目录审计这类入口，又发现 41 个新 URL；详见
[`SNAPSHOT_AUDIT_20261027.md`](data/raw/swh_p4/SNAPSHOT_AUDIT_20261027.md)。包/归档
来源不能按 Git revision 日期排序。
旧 Athena 视图 `swh_p4_20260604.p4_file_origin_425ca2322f7030ec`
只包含 revision 路径；统一后的新视图和本机下载清单见
[`UNIFIED_ORIGIN_DOWNLOAD_LIST_20261027.md`](data/raw/swh_p4/UNIFIED_ORIGIN_DOWNLOAD_LIST_20261027.md)。

**SWH 原计划与留存缺口（本阶段暂缓）：**

1. [x] **补全来源路径审计。** 已核查全部 523,435 个目录的
   `rel`/`snp` 邻居；395 条 release 边、12 条 snapshot 边通向
   97 个 origin，其中 41 个 URL 为新发现。756 个 D0 经该路径得到
   966 条 D0→origin 关系，旧 337 个 D0 的 372 条关系逐对复现。
   结果和日志已归档，见本页上方审计报告。
2. [x] **生成版本化统一来源视图和下载清单。** 已将 revision 表与
   966 条 release/snapshot 证据 `UNION ALL`，再按 D0 关联 128 个
   candidate 文件分片；保留原始文件字段、多对多 URL、revision/归档
   路径的不同字段和 `mapping_basis`。新 Athena 文件视图实测返回
   `.p4` 文件名、content ID 与补充来源 URL。10,201 行 origin 清单
   已上传 S3 并下载回本机，SHA-256 核对通过；详细表名和路径见上方报告。
3. [ ] **补齐本地历史收集缺口。** 输入已筛查的 10,201 个
   原始 origin URL；规范化同一仓库的别名，保留全部原始来源证据。
   GitHub/GitLab/Bitbucket 等 Git 来源每个规范仓库下载一次完整 bare
   mirror，导出 refs 与所有可达 commit 元数据；包/归档来源按可获取
   历史版本下载。四分片初轮已处理完 10,069 个 Git 目标，9,250 个
   镜像校验完成、201 个 `failed`、618 个 `inaccessible`；不能把
   `final=true` 解释为全部成功。先按错误原因复查 201 个失败项，
   对 618 个不可访问项核实失效、私有或迁移证据；42 个失败归档
   单独补查。运行位置、恢复规则和分片见
   [`FULL_HISTORY_DOWNLOAD_20261027.md`](data/raw/swh_p4/FULL_HISTORY_DOWNLOAD_20261027.md)。
4. [ ] **补齐协作记录。** 对可访问的 tracker 分页保存全部可见
   open/closed issues、评论、事件、标签、里程碑和 PR/MR/reviews。
   GitHub issues API 中的 PR 单独归类；按 endpoint + page/cursor
   恢复。初轮已有 352,497 个 API 页任务成功；仍有 392 个 `failed`，
   以及 HTTP 401/403/404/410/422 终态。`gitlab.com` 的 920 个旧 401
   已于 2026-11-05 备份后重排为 `pending`，等待手动运行
   `run_all.sh --api-only`；实测 token 可以读取先前 401 的标签及 MR
   评论接口。不要将 404 或私有端点一律重试。
5. [ ] **核对来源证据并补缺口。** 用已有 candidate 的 content ID、
   D0、revision、origin 证据核对本地仓库历史，记录哪些 SWH 旧
   revision 在当前镜像中缺失；可从 SWH 定点补取的单列任务。
   这是版本/provenance 核对，**不重跑 122,392 个唯一内容的 P4_16
   单文件筛查，也不据此丢弃已下载仓库**。
6. [ ] **扩大覆盖并构建训练视图。** 专项复核其余 199,369 条
   `needs_review`，对新增候选再做来源映射；训练视图选择适合的
   P4_16 版本，完整历史和 issue 原件始终保留在本地。

第 3～4 项初轮已结束，后续若恢复 SWH，应针对错误类别补查，不重新下载
9,250 个已校验镜像。LFS 仍有 `tool_missing`/`error` 状态，外部
submodule 历史也未收齐；源码与 SWH 候选 revision 的对应核验尚未执行。
无需重跑已完成的 D0→D16 目录遍历、128 分片 revision 日期查询或
已完成的单文件筛查。
路线 B 的剩余纯 AWS 全表扫描保留为历史
备选，不是当前待启动任务。

## GitHub 搜索补集的 P4_16 筛查（当前下一步）

入口是 [`github_p4_supplement`](data/pipelines/github_p4_supplement/README.md)
的搜索检查点和 `candidate_repositories.csv`。仓库搜索命中只是候选；
`extension:p4` 命中只证明 GitHub 搜索索引返回过 `.p4` 路径，均不等于
已确认 P4_16。此流程只处理 GitHub **补集**，不重跑已完成的 SWH 单文件
筛查，也不改变已下载的 SWH 仓库及其完整历史。

2026-11-06 检查点：搜索队列已结束，按 GitHub ID 去重后有 **45,955**
个候选仓库，其中 **2,101** 个有 `.p4` 文件搜索命中；**1,760** 个
仓库名称与 SWH URL 清单匹配，其中 **897** 个也有文件搜索命中。
另外 3 个 `p4 in:name` 的单日查询因分页前预期条数变化标记
`unresolved`，不能宣称搜索完全覆盖。`candidate_repositories.csv` 尚未
导出；GitHub 补集尚未核实 P4_16，也尚未下载新增仓库的完整历史。

1. [ ] **冻结搜索与身份清单。** 从已结束的搜索检查点导出 CSV，记录搜索
   时间、`complete/failed/unresolved` 分区、GitHub 仓库 ID、规范 URL、
   搜索来源和已知 `.p4` 路径。按 GitHub ID 去重，并与已有 SWH origin/
   本地镜像对账；同一仓库复用已有源码与证据，保留全部搜索命中。
   3 个 `unresolved` 分区列为覆盖缺口；无文件搜索命中的仓库仍保留，
   不能计为阴性。名称相同仅是对账线索，重命名后需核实 GitHub ID。
2. [ ] **先找文件，暂不下载完整历史。** 已有 `extension:p4` 路径的仓库
   可先按搜索命中的 blob SHA 定点取内容，之后核实该路径与当前 HEAD
   的关系；其余仓库按固定的默认分支 HEAD SHA 查询 GitHub Git Trees，
   筛出 `.p4` 文件。递归 tree 若返回 `truncated=true`，逐层遍历
   未覆盖的子树；截断、404/403、重命名和权限错误单列状态。只有完整
   检查过该 HEAD 的树，才能标记 `no_p4_on_default_head`；这个状态
   **不表示其他分支或历史版本没有 `.p4`**。搜索索引里已过期的路径
   也要记录为 `stale_hit`，不能冒充当前 HEAD 文件。
3. [ ] **只取 `.p4` 字节并按 Git blob OID 缓存。** 记录仓库 ID、HEAD
   SHA、路径、blob OID、字节数、抓取时刻和错误；相同 blob 只取和判定
   一次，但保留它在所有仓库/路径/版本的关系。先查本地 SWH 内容缓存
   是否已有相同 SHA-1 Git blob，再对缺失对象调用 GitHub blob 接口。
   超限、截断、LFS 指针或获取失败须明确标记，不能当作非 P4_16。
4. [ ] **复用 SWH 的版本与质量判定。** GitHub 文件字节及 Git blob
   SHA-1 ID 适配到 [`inspect_content`](data/tools/swh_p4_content_screen.py)
   （`p4-content-screen-v1`）：先校验 `sha1_git`，剔出确证的空文件、
   二进制和 LFS 指针；忽略注释/字符串中的伪语法，记录 `core.p4`、
   `v1model.p4`、`parser(packet_in ...)` 等 P4_16 线索，以及
   `header_type`、`field_list` 等 P4_14 线索。结果只分
   `p4_16_likely`、`p4_14_likely`、`mixed_or_conflicted`、`unknown`、
   `non_source`、`uninspected`；**正则阳性不是已确认 P4_16**。
   对模糊项复用 [`classify_review`](data/tools/swh_p4_review_screen.py)
   （`p4-review-screen-v2`）细分语法证据；这一步仍可能只有 likely。
   对所有可读的 P4_16 候选调用
   [`inspect_quality`](data/tools/swh_p4_candidate_quality.py)
   （`p4-candidate-quality-v1`），记录行数、平均/最长行长、字母比例、
   生成声明、内嵌编码等训练质量标记。质量 `excluded` 只影响训练文件
   资格，**不能据此删除仓库**；所有判定结果按 blob ID 缓存，并关联
   回每个 `github_id + HEAD/搜索快照 + path`，不按文件去重丢来源。
5. [ ] **确认 P4_16，保留无法判定项。** 对 `p4_16_likely`、混合和
   `unknown` 的相关文件，按需定点获取同仓库 include/目标头文件；
   用匹配架构的 P4C 解析/编译，记录编译器版本、命令、退出码和 stderr。
   成功的 P4_16 解析/编译，或有可审计语法证据的人工复核，才标记
   `p4_16_confirmed`。单独的 `v1model.p4` include 或 BMv2 编译失败
   均不足以作最终判定：PSA/TNA 等目标、片段、缺失依赖或源码错误
   也会失败。`p4_14_likely`、取回失败和仍需依赖的文件保留复核状态，
   不把它们的仓库记为“确定没有 P4_16”。
6. [ ] **生成逐仓库决策与下载增量清单。** 至少保存 `github_id`、
   规范 URL、与 SWH 是否重合、搜索来源、HEAD SHA、`.p4` 路径及
   blob OID、证据规则版本、判定状态和检查时间。一个仓库只要有一份
   已确认 P4_16 文件，就进入新增完整历史下载清单；只有静态线索的
   仓库进入 `likely/review` 清单。`no_p4_on_default_head`、请求失败和
   未查历史分别统计，不能写成“该仓库从未含 P4_16”。后续历史镜像
   到手后再查其他 refs/旧提交；仅靠当前 HEAD 不能宣称发现全部历史
   P4_16 仓库。
7. [ ] **补查仅历史版本含 P4_16 的仓库。** 先用已有 SWH 证据回连
   `no_p4_on_default_head` 和未确认仓库；对仍无证据的新增仓库，若要
   主张“曾经含 P4_16”的完整覆盖，就须枚举其他 refs 和历史 tree，
   再定点读取历史 `.p4` blob（可评估无 blob 的部分 Git 克隆）。
   这一阶段先抽样测量请求数、历史对象量和耗时，再决定全量资源。
   未完成前统一标记 `history_unchecked`，不得并入已排除仓库数。

**时间字段与运行时间：** GitHub 搜索使用 `created` 日期拆分查询，
不是按创建日期过滤 P4_16。P4_16 出现之前创建的仓库可能在多年后新增
P4_16，因此绝不按 `created_at` 早于某一年直接剔除。`pushed_at` 或
某个文件的 commit 日期也不是语言版本的证明；早期实现、回填提交、
分支及元数据差异使其最多用于安排复核优先级。若以后要做真正的
时间排除，须先核实 P4_16 首次公开版本的权威日期及该仓库**所有
相关版本**的时间覆盖，并单列有覆盖损失的实验结果，不能用于本轮
“尽可能收集所有仓库”的主清单。

执行前先以 100～500 个仓库试跑，记录每仓 API 请求数、`.p4` 命中率、
tree 截断率、下载字节、429/403 和实际吞吐，再按剩余仓库数估计 ETA。
当前 **43,854** 个仓库尚无 `.p4` 文件搜索命中；即使每仓只需一次 tree
请求，也至少约 4.4 万次 API 请求。按单 token 常见的 GitHub REST
主配额 5,000 次/小时计算，理论下界约 9 小时；固定 HEAD、获取 blob、
子树展开和次级限流会增加耗时。搜索阶段缓存的配额头显示代码搜索
10 次/分钟、仓库搜索 30 次/分钟；该阶段已结束，不应把搜索限额
直接当作 Git Trees/Blobs 的速率。先用当前 token 和机器做试跑，按
实际响应头控制速率并按仓库/blob 保存检查点；只有测出主要瓶颈后
再决定是否分机器。四台机器共用 token/出口时不能把配额乘以四。
每轮输出已查/总数、各状态数量、请求速率、开始时间和滚动 ETA。

以下带有 2026-10-27 日期的启动参数、单机路径和容量是历史运行记录；
最新分片检查点位于 `data/raw/repositories/swh_full_history_shards/shard_0..3/`。

### 已完成的 GitHub 方法样本

`https://github.com/ANTLab-polimi/FOP4` 出现在本机 10,201 行清单中，
其首候选 `swh:1:rev:6edf95398f8fd93b0d8f52ebf666a8dc6c0b27ab`
与本次 `master` OID 相同。本机 `git clone --mirror` 获取的服务端可见历史
有 2,473 个可达 commit、2 个分支、2 个 tag、3 个 PR head refs；镜像约
4 MB，独立 bundle 约 3.6 MB。`git fsck --connectivity-only` 和 bundle
验证通过；`data/tools/export_git_commit_index.py` 流式导出 2,473 条
唯一 commit 的 JSONL，约 1.1 MB。当前 `master` 有 19 个 `.p4`，
`v1.0`/`v2.0` 无 `.p4`，说明仓库历史版本与含 P4 的版本不可混为一谈。
GitHub `issues?state=all&per_page=100` 返回 4 条，其中 1 条是真 issue、
3 条是 PR；PR #1 的 2 条讨论评论已单独保存。原始文件、哈希和命令见
[`../repositories/swh_github_example/README.md`](data/raw/repositories/swh_github_example/README.md)。
该样本仅验证保存方法，不代替已有的单文件筛查；未上传 S3，也未
收集完整 PR/review/LFS/submodule 数据。不能以它的 4 MB 镜像或 4 条
issue 外推全部 10,201 个来源的存储量和耗时。

### 全量本地收集与恢复单位

输入是本机 10,201 行 origin 下载清单，覆盖目前 13,465,757 条
已筛查 candidate 文件命中。**一次全量运行遍历全部来源**，而不是只
处理 8 个或先限定总下载量。`candidate` 仍是单文件静态判断，不能
宣称所有映射 URL 已经过仓库版本级确认；但这不阻止先把仓库历史
下载到本地。历史 `needs_review` 的 199,369 条尚未纳入这份清单，
以后补充来源时增量下载。

本地输出根目录为
`data/raw/repositories/swh_full_history/`。`manifests/` 保留原始
origin→规范仓库关系、逐仓库状态与日志；`repos/<repo-id>/` 存一份
bare mirror、refs 快照、commit JSONL 和哈希/连通性报告；
`trackers/<repo-id>/` 存 issues、PR/MR、评论与事件的原始分页 JSON；
`packages/<origin-id>/` 存非 Git 版本归档；`swh_gaps/` 存当前原站
镜像缺少的 SWH revision 与后续补取结果。文件用稳定 ID 命名，
URL 和可读仓库名保存在 manifest。已有 `swh_github_example/`
不移动、不覆盖。自己的 S3 桶继续保留前几阶段的映射输入/结果；
新下载的仓库源码和 issue **以本地完成为准，不默认上传 S3**。

当前 `/mnt/8T` 约有 **951 GB 可用空间**（2026-10-27 检查）。
完整历史、LFS、issue 总体积尚未知，不能保证 10,201 个 URL 所有
对象都装得下，也不能用单个 4 MB 样本外推。运行中持续统计已完成
仓库数、失败/待重试数、下载字节、磁盘可用量、平均速度和 ETA。
不设 5 GiB 等任意总量上限；当可用空间低于 **100 GB 安全余量**
时，停止调度新下载、保留已完成结果和断点，报告还需增加多少本地
空间。这个阈值是防止写满磁盘，不把任务标记为全量完成。网络/API
限流只暂停对应来源队列，其他来源继续运行。长任务用后台会话保存
日志，按仓库和 API 页恢复，不要求一轮进程无中断地跑到底。

1. **来源规范化**：保留 10,201 个原始 origin 的身份和证据，建立
   `origin_id -> canonical_repo_id` 多对一关系；跟随重定向并核验实际
   仓库身份，不能只靠去掉 `.git` 合并。`pkg.go.dev` 等页面中的上游
   Git URL 只是线索，须验证后才共享镜像。按 `origin_id` 保存状态。
2. **完整历史**：每个规范 Git 仓库下载一次完整 bare mirror，保留
   服务端可见 refs 可达的 commit/tree/blob；不做浅克隆，不逐 commit
   下载源码，也不因单文件质量标记跳过仓库。镜像先写临时目录，
   `git fsck`、refs/对象数检查通过后标记本地完成。保留 HEAD、
   refs→OID；流式导出每个可达 commit 的 OID、tree、parents、author、
   committer 和完整 message。10 万个 commit 是一份镜像和约 10 万行
   索引。正式全量只保留镜像一份，不为每仓再存一份重复的 bundle。
   克隆中断则对该仓库重试/续 fetch；无法修复的临时目录保留失败
   状态，不能冒充完成。包/归档来源逐版本保存。
3. **协作记录**：按 `canonical_repo_id + tracker + endpoint + page/cursor`
   分页保存原始 issues、PR/MR、评论、reviews 和事件；GitHub issues
   结果中带 `pull_request` 的条目归 PR，不重复计为 issue。
   记录 `Link`/下一页游标、`ETag`、响应配额头和抓取时间；空页、
   tracker disabled、403/429、网络失败与真正完成分别存状态。
   无认证 GitHub REST 按 IP 限 60 次/小时；若对约 9,940 个原始
   GitHub URL 逐一请求，即使每个只取 1 页也要约 9,940 次；先规范
   去重会减少实际仓库数和请求量。全量 issue 收集须先确认可用的
   正规认证配额，并按响应头退避。7892/7893 共用出口，不能当作
   两份独立 IP 配额。
4. **版本证据复核**：下载后将现有 SWH candidate revision 与镜像
   的 commit OID 对照，在可达历史中核对 `.p4` 路径和 `content_id`。
   现有 `content_id` 是 `sha1_git`，对 SHA-1 Git 对象可与 blob OID
   对比；原始字节 SHA-1 是公共 `content` 表的另一列。缺失旧提交
   单列 SWH 补取，版本关联不明则保留待核对状态。这一步不重复执行
   已完成的 P4_16 特征/质量筛查，也不删本地源码。
5. **本地验收**：每个规范仓库保存镜像连通性、refs 数、可达 commit
   数、索引行数、字节数、开始/完成时间和状态；每个 tracker 保存
   page 数、记录数、下页游标、限流状态；每个原始 origin 保存到
   规范仓库或包版本的关系。镜像、commit 索引和 tracker 各自独立
   标记完成，所有来源处理或明确记录失败/缺口后汇总，不丢多对多
   provenance。issue 原件与训练视图隔离。

### 历史源码与协作数据的收集边界

- **Git 历史**：对规范化后的 GitHub/GitLab/Bitbucket 等 Git 仓库，
  优先保存完整 bare mirror（所有服务端可见的 branches、tags 和其他
  refs，以及它们可达的 commits、trees、blobs），而非为每个 commit
  展开一份目录或只下载最新 commit 的 tarball。保存抓取时刻、源 URL、
  object format、`HEAD`、refs→OID 映射和镜像对象/refs 连通性校验。
  用镜像逐个重建可达 commit 的源码树；另输出 commit OID、tree OID、
  parents、author/committer、原始日期和 message 索引。保留原始 Git
  对象以免结构化索引丢掉签名等提交字段。
- **覆盖核验**：下载后检查 SWH 命中的候选 revision 是否存在于镜像，
  核对该 revision 的 `.p4` 路径、Git blob OID 与已有单文件筛查记录；
  版本级 P4_16 判定仅用于后续训练视图及缺口报告，不作为仓库下载门槛，
  也不批量重跑已完成的单文件筛查。
  SWH 旧提交若已从原站 refs 消失，另建 SWH 定点补取任务并标记其
  来源；不能把当前镜像说成含有已删除、不可达或私有的全部历史。
  Git LFS 实体和 submodule 指向的外部仓库单独记录并补取；未补齐
  时分别标记 `lfs_incomplete` 或 `submodule_incomplete`，不混同
  Git 对象缺失。同一规范仓库只取一份镜像，
  但保留所有原始 origin URL 和各自的 SWH 证据。
- **非 Git 来源**：先确定精确包名、版本、归档和可验证的上游 URL；
  保存提供者能列出的全部历史发布版本及校验值。只有核实是同一上游
  Git 仓库后才复用镜像。单个 SWH release/snapshot 或包归档不能
  证明其他历史版本存在；缺版本、无提交历史和无 issue tracker 分别
  记录，不填造 Git commit 或 issue。
- **Issues 与对应记录**：每个 tracker 全量分页、包括 open/closed，
  原始响应和稳定 ID 单独存储；GitHub issues API 中的 PR 条目须去重，
  issue 的评论、事件和时间线，PR 详情、review/评论另表保存；
  GitLab 的 MR/notes、Bitbucket 的
  pull request/启用时的 issue tracker 同理。保留更新时刻、抓取时刻、
  页码/游标、HTTP/配额状态及 `disabled`、`private`、`not_found`、
  `rate_limited` 等明确结果。issue 内容不自动进入训练数据。
- **恢复与存放**：代码镜像、commit/refs 索引、issue/PR 原始数据和
  版本核对分别有状态。代码按规范仓库和抓取快照恢复；issue 按
  tracker + endpoint + page/cursor 恢复。校验通过后保存在本地，
  保留来源清单、校验报告、字节数、开始/完成时间、失败原因、
  速率和 ETA；新源码与协作数据不默认上传 S3。中断的 Git 传输或
  归档不假设支持字节级断点。
- **并发与容量**：全量队列包含当前 10,201 个原始来源，按站点设
  独立下载与 API 队列。Git 镜像从 4～8 并发试起，稳定后才试 16；
  GitHub 认证 REST 以约 1 请求/秒试跑，GitLab/其他 tracker 独立控速；
  遇 `403/429`、`Retry-After` 或剩余配额下降即退避。7892 HTTP 与
  7893 SOCKS 代理共用公网出口，不增加独立 IP 配额。持续测量历史、LFS
  和 issue 占用；不设 5 GiB 总量上限。可用空间低于 100 GB 时暂停调度，
  保留完成记录并报告缺口，扩容后继续。旧阶段 AWS $100 作业上限不适用
  于本地下载；API 限流或不可访问状态逐来源记录。

## 当前决策（2026-10-27）

- **保留的目标**：把 13,465,757 条候选文件命中关联到有证据的
  revision 或归档对象和 origin URL；对全部已发现的原始来源规范去重，
  将可获取的完整历史、commit 索引和协作记录下载到本地并保存 provenance。
  已完成的 P4_16 单文件筛查不重跑；版本核对和训练视图在下载后进行。
  2026-10-27 已完成 317,804 个 revision node ID→SWHID/hash 反解。详细
  统计见 ROUTE_A_REVISION_RESOLVE_20261027.md。revision 日期查询已经
  完成；内容复核和完整源码下载仍未完成。全量入口审计后为 10,201 个 SWH
  origin URL，不能称为规范仓库数或已确认 P4_16 仓库数。
  2026-10-26 已处理并归档 65,321 个直属父目录的 revision 路径，
  其中 64,984 个映射到 URL、337 个记录为无 revision 入口；
  退出码 0。任务上限为 AWS $100、首次启动起 12 小时；实际耗时
  2 小时 24 分 23 秒，Athena 扫描费估算 $0.248247，另计 S3。
- **路线 A 样本已通过**：图读取器已编译运行，节点类型已解压，16 个 D0
  的 SWHID/node ID 正反映射及一跳父目录集合全部通过，与路线 B 无差异；
  旧 Athena 的 7 对目录→revision 关系也全部一致。成功运行约 105 秒，
  其中读取器加载约 95 秒，峰值 RSS 约 16.1 GiB。详情见
  [`ROUTE_A_SAMPLE_20261023.md`](data/raw/swh_p4/ROUTE_A_SAMPLE_20261023.md)。
  **全量 D0→D1 已完成并通过对账**：北京时间 2026-10-23 00:53:42 启动，
  01:23:02 完成计算，耗时 1,759.72 秒（约 29 分 20 秒）。全部 65,321 个
  D0 验证通过，得到 345,515 个父目录对、137,691 个不同父目录、
  114,293 个新 D1、10,525 个无父边 D0，三个集合与 B 全量一致，未解析为 0。
  用户上限 $25，实际采用的保守预算上限 $18.36；模型预留 $9.716656
  **不是实际账单**。原上传阶段因 AWS 凭证导出失败而退出；2026-10-26
  已用账号 `588301174757` 完成补传，共 16 个对象、47,418,374 字节，
  S3 长度与 SHA-256 全部通过，最后上传的完成标记已读回。结果在
  `s3://swh2p4/new-account-588301174757/route-a/d0-d1-v1/`；运行与验收见
  [`ROUTE_A_FULL_D1_20261023.md`](data/raw/swh_p4/ROUTE_A_FULL_D1_20261023.md)。
  **D2～D8 已完成**：2026-10-26 10:07:04 UTC 在 tmux
  `swh-route-a-d2-d8` 启动，10:07:48 UTC 完成，耗时 **44.219 秒**，
  退出码 0。D0～D8 共 **523,311** 个唯一目录节点，D8 边界 **2,568**
  个节点未展开。所有层保存完整父边、检查点、日志和验证报告；
  47 个对象、15,047,952 字节已归档到旧桶，S3 SHA-256/长度核验通过。
  本阶段图查询无网络请求、无 AWS 计算作业。见
  [`ROUTE_A_D2_D8_20261026.md`](data/raw/swh_p4/ROUTE_A_D2_D8_20261026.md)。
  **随后已扩展至 D16 并收敛**：2026-10-26 10:33:15 UTC 从 D8 边界
  继续，22.754 秒完成，新增 124 个目录，累计 **523,435** 个目录节点。
  D10 起无新 frontier；所有已发现目录均已展开，当前边界为 **0**。
  新增分层结果、日志和检查点包共 52 个对象、5,382,584 字节，已归档至
  `s3://swh2p4/new-account-588301174757/route-a/d9-d16-v1/`，全部通过
  S3 SHA-256/长度校验。见
  [`ROUTE_A_D9_D16_20261026.md`](data/raw/swh_p4/ROUTE_A_D9_D16_20261026.md)。
  revision/origin 全量图遍历、URL 解析和 128 CSV 文件回连已完成；
  共 10,107 个 origin URL，13,458,905 行已映射，6,852 行没有 revision
  入口。2026-10-26 15:51:17 UTC 完成核心归档：46 个对象、
  1,088,105,719 字节全部通过 S3 长度/SHA-256 校验；最终标记读回一致。
- **路线 B 暂停扩量**：D0→D1 已完成，D1→D2 至 D7→D8 以及后续
  revision/origin 关联均未启动。若路线 A 不可行，再按预算决定是否
  继续 B；不能把每轮约 $51 的历史实测视为剩余费用上限。
- **旧 8 目录探针单独保存**：第二次 Glue 运行已超时。桶内最终
  `status.json` 记录 7/8 个目录已映射、147 条证据、1 个不同 origin URL：
  `https://github.com/verified-network-toolchain/petr4`。第 8 个目录仍待查；
  探针不是全量 origin 穷举。详情见 `DIRECTORY_PROBE_8_20261021.md`。

### 历史执行清单：目录 → revision → origin URL（revision 路径已完成）

**本阶段已按用户确认启动并完成 8 目录闭环**：缓存汇总、读取器扩展、
本地图遍历和 origin URL 查询均已完成；随后用户确认全量 A4，
首次启动为 2026-10-26 13:26:53 UTC，固定截止为 2026-10-27 01:26:53 UTC。
旧的 D0→D1 预算/截止时间不适用于本阶段。A4 运行记录见
[`ROUTE_A_ORIGIN_FULL_20261026.md`](data/raw/swh_p4/ROUTE_A_ORIGIN_FULL_20261026.md)。

#### 当前输入与完成边界

| 项目 | 已核实数量/状态 |
| --- | --- |
| 原始 `.p4` 文件名命中 | 14,060,834 行，128 个 CSV |
| 仓库发现采用的 candidate | 13,465,757 行、122,392 个唯一 content ID；质量标记仅供参考 |
| D0：不同文件直属父目录 | 65,321 个 |
| 本地图累计可达目录 | 523,435 个，全部已展开，frontier 为 0 |
| 已知目录 node ID → 哈希映射 | 179,614 个；其余目录暂用图内编号 |
| 全量目录 → revision 统一表 | 317,804 条关系、317,804 个不同 revision，已合并并归档 |
| seed → revision | 2,109,390 条关系，65,321 个 seed 全部已处理 |
| revision/origin 图遍历 | 已查询 345,529 个节点、保存 1,804,774 条边；frontier=0、无截断 |
| 不同 origin 节点 | 10,107 个，22 字节 SWHID Range 映射全部完成 |
| 非目录节点 → origin 闭包 | 6,748,910 条，33,350 轮后收敛，独立核验无缺失传播边 |
| seed/revision/origin 证据 | 53,812,259 条，已流式导出 |
| revision 路径 URL（历史运行） | 10,107 个 origin URL，全部通过 SHA1(url) 校验；8 目录的 57 条样本证据完全一致 |
| 全量文件回连 | 128/128 CSV、13,465,757/13,465,757 行已验证；S3 归档验收完成 |
| revision 路径 D0 覆盖（历史运行） | 64,984 个 mapped，337 个 no_revision_entry |
| release/snapshot 补漏 | 337 个 D0 全部得到 URL，新增 53 个 origin URL、372 条 D0→origin 关系 |
| 合并后的 URL 证据 | 65,321 个 D0 均有至少一个 origin；共 10,201 个不同 SWH origin URL；统一 Athena view 和本机清单已完成 |
| revision 路径文件行（历史运行） | 13,458,905 行 mapped；6,852 行 no_revision_entry，全部原行保留并可关联补漏结果 |

固定使用 Graph `2026-06-04`。本地输入包括：

- `route-a-d0-d1-v1/full-d0-d1-v1/`：D0 编号映射、目录父边及
  `shards/node_edges/part-*.parquet` 中的 `revision_nodes` 列。
- `route-a-d2-d8-v1/layers/d02/`～`d08/`：父边、revision 邻居和检查点。
- `route-a-d9-d16-v1/`：最新目录总表、已知哈希映射，以及
  `layers/d09/`～`d16/` 的父边、revision 邻居和检查点。

以上目录均位于 `/mnt/8T/xgr/swh-graph-poc-20260604/` 下。对这些种子，
目录遍历已收敛；后续关联使用完整边图，不再套用旧的逐 seed 8 层或
16 层路径过滤，也不能用全局 `minimum_depth` 替代真实 seed 路径。

#### A1：先在本地合并全量缓存，并统计 revision 数量

- [x] 验证三阶段完成标记、输入指纹、图版本及相关文件摘要，生成本次
  独立运行身份和输出目录。
- [x] 合并全部目录父边，按 `(child_node_id, parent_node_id)` 去重。
- [x] 展开 D0 检查点的 `revision_nodes`，联合后续各层的
  `revision_neighbors.parquet`，按 `(directory_node_id, revision_node_id)`
  去重。查询对象覆盖全部已展开目录，不只取末层或无目录父边的节点。
- [x] 输出目录数、父边数、直接目录/revision 对数、唯一 revision 数，
  以及有/无直接 revision 的目录数；关系条数不能当作不同提交数。
- [x] 核对原始邻居类型计数并记录 `rel`/`snp` 入口缺口；已为原来
  无 revision 入口的 337 个 D0 补读这些邻居。其余 D0 的此类入口
  仍需按首页第 1 项全量审计，不能据此宣称来源路径已穷尽。
- [x] 保留 D0 映射及全部边，待找到相关根目录后恢复 seed 关联。按需
  分区生成唯一 `(seed_node_id, root_directory_node_id, candidate_revision_node_id)`；
  不枚举所有重复路径，也不提前把 1,346 万文件行附到每条图边。

**A1 已完成（2026-10-26）**：本地只读汇总耗时 6.321 秒，使用三阶段
已核验归档，没有 AWS 计算作业。结果为 523,435 个目录、870,763 条不同
父边、317,804 条目录→revision 关系，涉及 317,804 个不同 revision 节点，
其中 294,269 个目录有直接 revision 邻居。D0 的 345,515 条父边与旧路线 B
逐对核对一致。结果在
`/mnt/8T/xgr/swh-graph-poc-20260604/route-a-revision-origin-v1/merge/`，
包括 `directory_nodes.parquet`、`parent_edges.parquet`、
  `directory_revision.parquet` 和 `seed_nodes.parquet`；已补传并核对至
  `s3://swh2p4/new-account-588301174757/route-a/revision-origin-v1/merge/`。

**A1 仅读取本地缓存，不调用 SWH API，不扫描 AWS `revision` ORC，
也不需要先反解 523,435 个目录哈希。** 现有图内 `dir→rev` 边表达的就是
`revision.directory = 对应目录哈希` 这一关系；只有需要提交标识/元数据
时才按需反解 revision SWHID 或回查 ORC。

#### A2：补充通用读取器，并做离线验证

- [x] 为反向图增加 `rev`、`rel`、`snp`、`ori` 等节点的带类型邻接输出。
  原目录读取器及二进制保持原位置和哈希，新实现使用独立构建目录，
  避免破坏已有目录检查点的身份校验。
- [x] 覆盖历史提交到后续提交、直接 snapshot 引用、多层 release、
  多 origin、共享节点和中断恢复等关系；类型或资源限制必须明确报状态。
- [x] 先通过语法/构建检查与小型人工图测试，再打开真实大图。

**A2 已完成（2026-10-26）**：新增独立 `origin` Rust 读取器，支持
revision/release/snapshot/origin 节点的带类型邻接输出；未修改原目录读取器
或其检查点。Rust 离线单元测试通过，release 构建成功。

反向查询的常见路径是：

```text
候选文件 → D0 → … → 某个提交的根目录 → 含文件的revision R1
                                           ↓
                                  后续revision R2/R3（可能多步）
                                           ↓
                                  release（如有）→ snapshot → origin
```

`R1→R2/R3` 是在反向图中沿提交历史走向后续提交。snapshot 通常引用
分支头，R1 不一定直接被引用。`rel`、`snp` 的实际连接以图边为准，
不假定每条路径都具有上述所有节点。到 origin 后停止扩展该节点。

#### A3：先用原来的 8 个目录跑通 URL 闭环

- [x] 复用旧探针的 7 个已知关联目录和 1 个对照目录，以当前完整目录
  边恢复它们的入口；不再进行昂贵的目录逐层 ORC 扫描。
- [x] 把这 8 个目录对应的唯一入口合并为共享 frontier，批量读取反向图。
  每个节点只查询一次，保留所有关系边；共享节点后续新增 seed 关联
  必须能通过已保存边恢复。找到一个 origin 后仍保留其余待查分支。
- [x] 对发现的 origin node ID 去重，通过公共映射文件的按需 Range GET
  反解 `swh:1:ori:<hash>`；复用已有缓存，校验对象版本、记录长度、
  `206/Content-Range`、节点类型和返回内容，失败保留待解析状态。
- [x] 将 origin 哈希批量关联 AWS `origin(id, url)` ORC。主方案为一次
  批量 Athena 查询，按已核实 schema 只取 `id,url`；提交前确认账号、
  输出位置、扫描限制和任务预算。过滤少量 ID 不保证只扫描少量源数据，
  须记录实际扫描字节。此步骤不重建 revision/history/snapshot 全表索引。
- [x] 核验已知证据：复核旧 7 对目录/revision 和可复现的 origin 路径，
  输出真实 URL；旧探针只找到一个 URL，不要求新结果只能有一个。
  对第 8 个目录和未完成分支给出具体状态，不把空结果解释为没有仓库。
- [x] 保存样本完成时间、节点/边数、RSS、Range 请求数/字节、Athena
  扫描字节及成本估算，再提出全量 origin 映射的时间和预算。

**A3 已完成（2026-10-26）**：8 个目录共享查询完成，查询 928 个非目录
节点、2,172 条图边，得到 9 个 origin 节点和 57 条 seed→origin 证据；
没有截断、待查 frontier 或节点/边上限触发。9 个 origin 均解析并通过一次
Athena `origin(id,url)` 查询得到 URL，扫描 **9,186,925,980 字节**（约
8.55 GiB），Athena 查询 ID 为
`dc1decae-dd5c-4e1c-bf86-7b0afb16708f`。样本运行总耗时约 44.5 秒，
保守任务记录约 **$1.0007**，另有实际 Athena/S3 账单项；未超过本阶段
授权的 **$100、10 小时**上限。9 个 URL 见样本目录的
`repository_urls.txt`，证据在 `seed_origin.parquet`。

本阶段用户已确认 A1～A3，运行上限为 **10 小时、AWS $100**，并继续保留
**$10 S3 存储预算**。随后用户已单独确认 A4 全量映射，任务上限为
**$100、12 小时**，固定截止见本文件开头；样本扫描量不作为全量硬上限。

样本另建议限制为最多 **1,000,000 个唯一非目录节点、10,000,000 条
遍历边**；达到任一限制保存检查点并报告截断。这些是资源保护条件，
不代表在限制内一定找到所有 URL。预算控制为本任务控制器的保守预留，
不是 AWS 账户级实时扣费封顶。开发/离线检查耗时另行报告。

#### A4：全量 origin 映射（已完成并验收归档）

- [x] 根据 A1 的 revision 数和 A3 的实测扩展规模，说明共享节点带来的
  实际工作量，确定全量时间、节点/边上限及新增 AWS 预算，取得确认。
- [x] 对全部入口使用共享 frontier 和全局查询缓存。revision 历史长度
  独立于目录 D16；完整映射须遍历相关非目录可达节点至 frontier 为空。
  有截止时间、节点/边限制或未解析记录时，单独报告剩余范围。
- [x] 合并 origin 后集中解析 URL，沿已保存边恢复 seed/revision/origin
  关系；最后按原始 CSV 分区回连 candidate，保留原输入身份及行标识。
  不按 content ID 去掉不同目录/仓库的文件命中。
- [x] 保存去重 URL 清单、全部关联证据和未映射清单，验收后归档到原桶。
  仓库下载、每个 origin 的最新已确认 P4_16 revision 选择属于后续阶段。

全量实现：`data/tools/run_swh_route_a_origin_full.py`，启动脚本为
`data/tools/run_swh_route_a_origin_full.sh`，tmux 会话 `swh-route-a-full`。
工作目录为 `/mnt/8T/xgr/swh-graph-poc-20260604/route-a-revision-origin-v1/full/`；
S3 前缀为 `s3://swh2p4/new-account-588301174757/route-a/revision-origin-v1/full/`。

全量过程中发现并修复了私有 AWS 凭证刷新、Athena 客户端、幂等 token、
输出字段歧义和 origin 哈希格式等实现问题；已经完成的本地图阶段均保留。
早期使用完整 `swh:1:ori:...` 去关联 ORC 的 40 位 origin.id，查询成功但结果
为空，扫描 16,862,352,831 字节；该空结果不作为映射完成证据。修正后
原始哈希、完整 SWHID 与图内 node ID 分列，并验证 SHA1(url)=origin.id。

文件回连按 0–63、64–127 两组 CTAS 独立恢复；每组 64 个 source_shard，
符合 Athena 每次 CTAS 至多 100 分区的限制。保存全部 candidate 字段、
原始 source_csv/source_row、seed_node_id 与映射状态。文件表 `file_seed/`
和 53,812,259 条证据表 `results/seed_origin/` 采用 D0 关联，另提供完整
`file_origin` Athena view。保留全部 13,465,757 条文件命中，不按 content 去重。
每次 Athena 查询由独立工作组强制限制为 32 GiB；任务预留和原始截止时间
持久化，停止时取消仍在运行的 Athena 查询。这是本任务保护，不是账户余额封顶。
工作组允许 CTAS 自定义结果目录，32 GiB 扫描限制依然有效；如强制统一
输出位置，Athena 会拒绝带 external_location 的 CTAS。

2026-10-26 15:42 UTC 文件回连通过：13,458,905 行已映射 URL（99.9491%），
6,852 行对应的 337 个 D0 没有 revision 入口，状态为 no_revision_entry。
2026-10-27 已用全部 128 个公共 revision ORC 分片独立复查它们的 1,478 个
祖先目录，仍为 0 条 revision 匹配；随后通过 release/snapshot 分支补出
337/337 个 D0 的 URL，旧状态仅表示没有 revision。详情见
[`MISSING_REVISION_CROSSCHECK_20261027.md`](data/raw/swh_p4/MISSING_REVISION_CROSSCHECK_20261027.md)。
Athena 完整关联视图：`swh_p4_20260604.p4_file_origin_425ca2322f7030ec`。
本次包括早期空结果查询在内，累计扫描 49,649,462,040 字节，按 $5/TB
约 $0.248247，不含少量 S3 请求与存储。15:51:17 UTC 归档完成，
退出码 0；从首次启动计耗时 8,663.382 秒。46 个核心归档对象的大小及
服务端 SHA-256 全部通过；最终 complete.json/archive.json 已再次读回
并与本地逐字节核对。验收记录、更新文档和结束日志补传到 `postrun/`。

#### 输出、恢复与验收约定

以下汇总目录阶段和 A4 的实际产物；路径相对于各阶段输出目录。
A4 的 `results/unresolved.parquet` 记录图截断/ID/URL 错误，当前为 0 行；
337 个无 revision 入口的 D0 单独保存在 `results/seed_summary/`，不能用
unresolved 的 0 行代替文件映射覆盖统计。

| 产物 | 主要字段/作用 |
| --- | --- |
| `directory_nodes.parquet` | `node_id, minimum_depth`，523,435 个目录 |
| `seed_nodes.parquet` | `seed_directory_id, seed_node_id`，65,321 个 D0 |
| `parent_edges.parquet` | `child_node_id, parent_node_id`，保留全部父边 |
| `directory_revision.parquet` | `directory_node_id, revision_node_id`，直接根目录关系 |
| `seed_revision.parquet` | `seed_node_id, root_directory_node_id, candidate_revision_node_id` |
| `results/traversal_edges.parquet` | 反向查询的起终节点、类型，含历史/release/snapshot/origin 证据 |
| `results/origin_urls.parquet` | `origin_node_id, origin_id, origin_swhid, origin_url, status` |
| `results/revision_origin.parquet` | 含候选文件的 revision → origin，6,532,814 条关系 |
| `results/seed_origin/` | seed、根目录、含文件 revision、origin、URL 和状态；完整路径边另存 |
| `results/seed_summary/` | 每个 D0 的 URL 数及状态，包括没有 revision 入口的 D0 |
| `results/repository_urls.csv` | 按 origin/URL 去重的仓库发现清单，10,107 行；尚未归并 `.git` 等 URL 变体 |
| `results/unresolved.parquet` | 图截断、ID 解码和 URL 错误 |
| `file_seed/batch_000_063/`、`file_seed/batch_064_127/` | 原 CSV 分区的全部 candidate 字段及 D0/状态 |
| `results/file_origin_view.sql` | 按 D0 关联文件表和证据表，保留一文件多仓库关系 |

图内编号与 SWHID/哈希必须分列。`candidate_revision` 是有文件证据的
原提交；用于连接 snapshot 的后续 `tip_revision` 另列，不能认为后者仍
含该文件。当前无标签图的目录边只提供节点路径；完整相对路径/分支名
需要额外标签或仓库内容验证，不能凭节点编号生成。

- **恢复粒度**：采用固定节点批次（初始建议 512 个）和 URL 查询分区。
  先原子提交数据，再写输入摘要、输出摘要/行数及完成标记；只恢复身份、
  图版本和输入均匹配的检查点。重启不重置原截止时间和已预留预算。
- **日志**：保存 `run.log`、`events.jsonl`、`status.json`、读取器 stderr
  和退出码。长任务在 tmux 运行，每约 30 秒记录 UTC 开始/当前时间、
  阶段、已查询/待查节点、各类型数量、边数、origin/URL 数、速率、RSS、
  请求/字节和预算。已知工作量阶段输出百分比及阶段 ETA；图扩展总量
  未知时全流程 ETA 标为未知，不把当前层 ETA 当作全部剩余时间。
- **状态**：分别标明 `mapped`、`exhausted_without_origin`、
  `no_revision_entry`、`time_limited`、`budget_limited`、`node_edge_limited`、
  `id_decode_failed`、`url_unresolved` 和错误；缺 revision 的非 revision
  入口另行保留。`mapped` 仅表示已有证据，完整性另看 frontier/未解析数，
  不把已有一个 URL 或任务正常退出当作全部 origin 已列完。
- **完成验收**：核对边类型/方向、唯一键、seed 关联、已知证据、结果
  哈希/行数与未完成范围。只有实际遍历和映射均结束，才声明相应范围完成。
- **保存位置**：工作目录为
  `/mnt/8T/xgr/swh-graph-poc-20260604/route-a-revision-origin-v1/`，
  S3 为 `s3://swh2p4/new-account-588301174757/route-a/revision-origin-v1/`；
  缓存汇总 `merge/`、8 目录样本 `sample8/`、全量运行 `full/` 分别保存身份。
  已归档并核对大小与 SHA-256，最终完成标记已上传和读回。

### S3 数据保留与清理

**2026-10-26 09:02 UTC 最终验证通过**：当前身份已切回
`588301174757`，原桶 `swh2p4` 的列举、读取旧结果、写入新文件和读回
核对全部成功；Athena、Glue、EMR Serverless 控制面查询及公共 SWH 桶
小文件读取成功。用户确认后续实验结果继续保存到 **`s3://swh2p4/`**，
新结果统一使用 `new-account-588301174757/` 下的独立运行前缀。
本次未创建其他桶、未启动查询或 Spark 作业。

- [x] 实测旧桶读写，写入 664 字节独立验证文件并读回比对一致。
- [x] 核对旧 D0→D1 完成记录：65,321 个 D0、114,293 个新 D1。
- [x] 明确结果位置为原桶；不切换到预留的新实验桶。
- [ ] 下次启动 Glue/EMR 前，创建并配置 `58` 的执行角色和实际作业资源。
- [ ] Athena 查询提交时明确设置输出为
  `s3://swh2p4/new-account-588301174757/athena-results/`；当前 `primary`
  工作组尚未配置默认输出位置，旧账号数据库也未迁入新账号。
- [ ] Spark 作业提交前处理临时文件及提交器可能需要的删除权限；当前
  `58` 对旧桶没有删除权限，本次未改动权限。

API 证据见
[`AWS_ACCOUNT_58_VERIFICATION_20261026.json`](data/raw/swh_p4/AWS_ACCOUNT_58_VERIFICATION_20261026.json)。
管理账号已增加仅限 `us-east-1`、成员账号 `588301174757` 的 SWH 实验
组织策略例外；详见
[`AWS_EXPERIMENT_PERMISSIONS_20261026.md`](data/raw/swh_p4/AWS_EXPERIMENT_PERMISSIONS_20261026.md)。

2026-10-26 已给新账号 `588301174757` 配置 `swh2p4` 的非公开跨账号
读写桶策略，保持 ACL 禁用与四项阻止公共访问。桶策略读回和
`IsPublic=false` 已核对。随后用新账号实测：当前 `AccountFullAccessRole`
已有 `AdministratorAccess`，但列举与读取均返回 `AccessDenied`；IAM 模拟
显示所需 S3 操作为 `explicitDeny`、`AllowedByOrganizations=false`。
随后用户切换至管理账号 `177835492717` 并授权修改，已定位根节点区域
SCP `p-9rxh6gc0`，增加仅限新账号、指定桶和所需 S3 操作的例外。
`UpdatePolicy` 成功、读回一致；策略模拟确认目标访问已放行。
调整后真实读写已在 09:02 UTC 用新账号验证通过，见上方记录。
此前成员账号 Cost Explorer 返回 $0（估算）；管理账号本月账单数据尚未
可用。后续用管理账号 Billing `GetCredits` 查到有效抵扣金共 **$180**，
估算剩余同为 **$180**，到期日 **2027-10-26 UTC**；管理账号和成员账号
`588301174757` 的抵扣金共享均已启用。该额度不能用于旧桶所有者账号的
S3 存储预算。余额证据与查看入口见
[`AWS_CREDIT_BALANCE_20261026.md`](data/raw/swh_p4/AWS_CREDIT_BALANCE_20261026.md)。访问详情见
[`S3_CROSS_ACCOUNT_ACCESS_20261026.md`](data/raw/swh_p4/S3_CROSS_ACCOUNT_ACCESS_20261026.md)。

**预算约束（2026-10-23 用户要求）**：预留 **$10 用于 S3 保存**，不把这部分
额度计入后续图查询/Glue/Athena 的可用预算。新收费任务开始前须核对最新
可用抵扣金、适用服务和有效期，扣除预留额及尚未出账的已发生费用后再
确定任务上限；不能沿用早先的余额数字。本地遍历本身不产生 AWS 计算费，
仍须核算实际 S3 请求、传输及结果存储。

这是项目预算约束，不是已在 AWS 账户中锁定 $10；本次未创建 AWS Budgets、
修改 IAM 或变更抵扣金配置。普通预算告警依赖延迟的账单数据，不能保证
在余额恰好剩 $10 时实时停止。全量驱动现已实现持久化的请求/传输保守
预算预留和截止时间，属于本任务限制，不是 AWS 账户级实时扣费封顶。
2026-10-23 启动前显示余额 $30.36；扣除 $10 和额外 $2 缓冲，本次执行
预算为 $18.36，不超过用户授权的 $25。

2026-10-22 清理前清点 `swh2p4`：22,366 个对象、1,608,308,424,678 字节，
均为 S3 Standard；桶未启用版本控制或生命周期清理。按已核实的
`us-east-1` 首档 $0.023/GiB·月计算，清理前月化存储费约 $34.45。

- **必须保留**：`filtered_v2/candidate_csv/` 的 128 份候选清单，
  `candidate-parent-index-v1/` 的 65,321 个 D0，
  `glue-results/swh-p4-20260604/d0-parent-scan-v2/` 的已完成父边/D1；
  旧探针中的 `status.json`、`origin-mapping.csv`、`origin-evidence.json`、
  `repository-urls.txt` 和小型扫描证据也保留。后续关联和审计需要它们。
- **已清理（2026-10-22）**：仅
  `s3://swh2p4/glue-results/swh-p4-20260604/probe8-complete-v1/lookup-indexes/`。
  其中 `snapshot_branch` 1,093,272,219,310 字节、
  `revision_history` 474,930,095,433 字节、`release` 12,422,024,209 字节；
  合计 16,419 个对象、1,580,624,338,952 字节，约占清理前桶容量
  98.28%，相当于月化存储费约 $33.86。删除请求成功，复查该前缀为
  0 个对象；按删除前快照扣除这批对象计算，桶内剩余约 25.78 GiB，
  月化约 $0.59（未含请求和未来新增数据）。这不是已结算账单。
- 这三个索引是旧探针用**整张 ORC 表**构建的反查缓存，不是只含 8 个目录。
  清理前同一探针、相同源码清单和配置可复用；现在若要再次使用必须
  重新计算，并产生新的 Glue 费用。D0→D8 的 `directory_entry` 父目录扫描不使用
  它们；未来全量 revision/origin 程序也不会自动复用，需明确实现兼容。
  本次已同时删除该索引前缀下的数据和完成标记，没有留下指向缺失
  Parquet 的索引 `complete.json`。未删除整个 `probe8-complete-v1/`；
  复查其 `status.json`、`origin-mapping.csv`、`origin-evidence.json`、
  `repository-urls.txt` 均存在，D0→D1 的 `complete.json` 也存在。
- `file-screen/` 约 18.38 GiB，`athena-results/` 约 6.27 GiB，
  `preprocess/` 约 0.86 GiB；相对大型索引省钱有限。旧筛查产物仍可用于
  复核与溯源，暂不把它们列为清理目标。

## 已完成

完整阶段统计见 [`RESULTS_SUMMARY_20261022.md`](data/raw/swh_p4/RESULTS_SUMMARY_20261022.md)。

- [x] 扫描 128 个 `directory_entry` ORC 分片，得到 14,060,834 条 `.p4`
  文件名命中，原始三元组无重复。
- [x] 单文件初筛得到 13,465,757 条 `p4_16_candidate` 命中，涉及
  122,392 个唯一内容；质量排除行仍保留在仓库发现输入中。
- [x] 从全部 128 个 candidate CSV 汇总出 65,321 个唯一直属父目录；
  索引保存于 `s3://swh2p4/file-screen/swh-p4-20260604/full-128-20261020/candidate-parent-index-v1/`。
- [x] 记录 7 个各含 223,218 条候选的异常大目录 ID；见
  `CANDIDATE_PARENT_INDEX_V1.md`。
- [x] 0–7 号候选输入和早期 Glue 目录反查试跑未产出 URL；这与后来的
  8 目录探针结果是不同尝试。
- [x] 对 7 个异常大目录和 1 个对照目录完成 AWS Graph Athena 有界试查；
  7 个目录各直接命中一个 revision 根目录，但直接 snapshot 分支命中为 0。
  三条查询扫描 683,581,288,325 字节，估算 $3.418。详情见
  `DIRECTORY_PROBE_8_20261021.md`。这三条 Athena 查询当时得到 0 个 URL。
- [x] 列全 2026-06-04 导出的 13 张 ORC 表；本次关联需要的 7 张表
  均确认 128 个对象，保存 key、ETag 和大小清单。
- [x] 启动完整 8 目录 Glue Spark 探针：2026-10-21 11:54:15 UTC，
  job `swh-p4-probe8-complete-v1`，run
  `jr_c304b81d99edc939edb62d7f0fc21e3231adbb927932ee89a7f87c23de7f8df1`。
  20 个 G.1X、180 分钟超时、失败不自动重试。第一次运行在快照索引
  shuffle 时因临时盘耗尽而失败，2,741 秒约 $6.70，仓库 URL 仍为 0。
- [x] 将快照索引改成源 ORC 小批次提交，保留原配置指纹及已完成的
  `revision_history`、`release` 索引；本地 Spark 中断恢复测试通过。
- [x] 2026-10-21 12:51:30 UTC 提交修复后的第二次 Glue 运行，run ID
  `jr_3571a9cf24d505fe5bb40976c0f2240e0a521f6b8fc9a38396f45d90f1211bc6`；
  修复代码已部署并核对 Glue `ScriptLocation`。该运行于当日
  15:51:47 UTC 超时；最后提交的结果是 7/8 目录已映射到同一 origin，
  另一个目录仍未完成父边查询。
- [x] 完成 D0→D1 全量父目录扫描：16 个 G.1X，128/128 个 ORC 分片；运行
  26,078 秒（约 7 小时 15 分钟）、417,249 DPU 秒，Glue 计算费约 $51.00；
  连同失败 v1 的 $0.63，已知累计计算费约 $51.63，不含附加费用。输出
  345,693 条父边、114,293 个新 D1；10,525 个 D0 未观察到父边。详见
  `D0_PARENT_SCAN_20261021.md`。
- [x] 路线 A 本地图读取器及 16 个 D0 的一跳样本校验通过：10 个父目录对、
  9 个无父边 D0 与路线 B 一致；7 对已知目录→revision 关系一致。
  另核实 B 的 345,693 条带标签边对应 345,515 个不同目录对。
  这不代表路线 A 全量 D0→D1 或 URL 映射完成。
- [x] 随后完成路线 A 全部 65,321 个 D0 的一跳父目录计算及与 B 的全量
  集合对账，耗时约 29 分 20 秒；345,515 个目录对、114,293 个新 D1、
  10,525 个无父边 D0 均一致。2026-10-26 已完成 S3 补传和验证；
  随后 D2～D16 也已完成归档。URL 的全量映射现已完成计算和文件回连。
- [x] 复核旧探针涉及的 8 个目录：新本地图样本得到 57 条证据、9 个
  origin URL；全量结果与该样本逐项一致，缺失和额外均为 0。
  旧探针的单 URL 结果不是完整 origin 清单。

## 双路线计划

两条路线都从同一份 65,321 个唯一直属父目录开始，最终需要输出
`candidate file -> directory -> revision -> origin URL` 的证据关系。
`directory_id` 是文件直属父目录，不是仓库 ID；一个目录或 revision
可能关联多个 origin。两条路线都只负责发现候选仓库，不替代后续的
仓库下载、P4_16 复核和完整仓库归档。

输入目录及样本已暂存在：
`s3://swh2p4/file-screen/swh-p4-20260604/full-128-20261020/graph-api-directory-probe-v1/inputs/`。
目录索引 SHA-256：
`58fa1b5ea1844ef19e6d15f8ad9e041065265673a8ec6a95308961ea56ee45f0`。
此路径中的 `graph-api` 是历史命名；当前计划使用公开 S3 Graph 数据，
不需要 `SWH_API_TOKEN`。HTTP API 探针不属于以下任一路线的执行依赖。

### 路线 A：本地压缩图读取 + AWS 输入/输出

本地机器负责加载和遍历压缩反向图；AWS S3 保存候选输入、源 ORC 数据、
检查点归档及最终结果。图的节点映射按需从公共 S3 做 Range GET，URL
解析可在 AWS 上从 `origin` ORC 表生成小型映射。复制到自己的 S3 桶不是
前置条件，也不会自动提供图查询能力。

2026-10-23 基础检查记录见
[`ROUTE_A_PREFLIGHT_20261023.md`](data/raw/swh_p4/ROUTE_A_PREFLIGHT_20261023.md)。
随后经确认完成了小样本读取，见
[`ROUTE_A_SAMPLE_20261023.md`](data/raw/swh_p4/ROUTE_A_SAMPLE_20261023.md)。
全量 D0→D1 随后经用户授权于 2026-10-23 00:53:42（北京时间）启动，
01:23:02 完成并通过全量对账，见
[`ROUTE_A_FULL_D1_20261023.md`](data/raw/swh_p4/ROUTE_A_FULL_D1_20261023.md)；2026-10-26
已完成 S3 补传，见
[`ROUTE_A_D1_ARCHIVE_20261026.json`](data/raw/swh_p4/ROUTE_A_D1_ARCHIVE_20261026.json)。
D2～D8 随后于 2026-10-26 完成并归档，见
[`ROUTE_A_D2_D8_20261026.md`](data/raw/swh_p4/ROUTE_A_D2_D8_20261026.md)。
随后 D9～D16 及统一 revision/origin 映射均已完成计算，详见本文件顶部
的当前数量和 [`ROUTE_A_ORIGIN_FULL_20261026.md`](data/raw/swh_p4/ROUTE_A_ORIGIN_FULL_20261026.md)。

- [x] **样本运行前核对**：本机磁盘、内存、直连网络具备样本运行条件；
  图文件大小/首尾抽样和节点类型压缩包完整测试通过，旧 Glue 探针已结束。
  主图/EF/MPHF 未做全文件哈希。任何新 AWS 扫描都要先独立估价。
- [x] **图文件下载**：使用
  `data/tools/download_swh_compressed_graph_poc.sh` 下载反向 `.graph`、
  `.ef`、`.fmphgo` 和压缩节点类型，约 272.6 GB 下载、约 286 GB 展开
  占用。脚本默认并行下载 4 个对象，每个对象使用 4 个 HTTP Range 连接；
  `SWH_GRAPH_DOWNLOAD_JOBS=1..4` 和 `SWH_GRAPH_CONNECTIONS_PER_FILE=1..8`
  可调节，总连接数上限 16。`aria2c` 的 `.aria2` 控制文件支持分段续传，
  也能接续先前 curl 顺序下载的部分文件；中断后保留数据及控制文件。
  定期汇报总进度、速度和 ETA（按已分配数据块近似计算，避免稀疏文件
  长度虚报），完成后校验大小及控制文件消失。主日志写入 `download.log`，
  单文件 aria2 输出写入各自的 `.aria2.log`。要求源 HTTP 支持 Range；
  若不支持分段续传，脚本报错并保留已有数据，不自动从头重下。
  下载完成不代表查询完成。
  2026-10-23 00:12:01（北京时间）直连续传完成，四个文件大小均匹配、
  `.aria2` 断点文件均消失；首尾各 4 KiB 与公开 S3 一致，节点类型压缩包
  完整测试通过。随后已解压节点类型、补齐 `.properties`，完成读取器和
  16 个目录的一跳查询；随后完成全部 D0→D1，并于 2026-10-26 完成 D2～D8。
- [x] **ID 映射验证**：实现/采用兼容 SWH Graph 发布版本的 Rust/WebGraph
  读取器。用一个已知目录 SWHID，经 `.fmphgo` 与 `.fmphgo.order` 的
  按需 Range 读取得到 node ID；实际打开转置图、核对节点类型及邻居，
  并与现有 Athena 目录到 revision 样本交叉检查。校验 offset、字节序、
  记录宽度及文件版本。16 个目录均通过精确正反映射，7 对已知
  目录→revision 关系与 Athena 一致；证据在样本报告及本地 JSON 中。
- [x] **D0→D1 样本校验**：复用上述 65,321 个 D0；将其 40 位目录哈希
  转成 `swh:1:dir:<hash>`，映射为图 node ID。先选已有的 8 个探针目录，
  再选含多父边和无父边的 D0 样本。从 `graph-transposed` 读取每个 D0 的
  **一跳**邻居，只保留节点类型为 `dir` 的直接父目录；按需将父节点 ID
  反解成目录 SWHID。与路线 B 的直接父边核对方向、ID 和集合；同时记录
  SWHID 映射成功率、Range 请求数/字节、每目录耗时、峰值内存。样本不符
  时先检查转置方向、节点类型、导出版本和映射顺序，不直接扩到全量。
  实测 16/16 目录集合一致，10 个父目录对、9 个无父边目录；Range
  38 次、正文 1,976 字节，峰值 RSS 16.1 GiB。当前驱动仅支持样本，
  随后的全量驱动已实现分片提交/恢复，启动前的 3 项离线测试通过；
  历史样本驱动本身仍只支持样本。
- [x] **全量 D0→D1 本地计算**：样本通过后，使用已校验的本地 D0 CSV
  作为输入，记录其 SHA-256、行数及图文件版本/大小。按固定分片处理全部
  **65,321** 个不同 D0，完整枚举每个 D0 的一跳 `dir` 父节点，保留
  `(child_id, parent_id)` 多父关系，并分别记录输入节点未映射、邻居解码
  失败和完整遍历后无父节点。父 node ID→SWHID 的随机 Range 读取要
  去重、缓存并按相邻偏移合并；先由样本测出请求量/成本，再确定并发。
  每分片在本地先写数据、核对计数，再写完成标记；恢复时只跳过输入指纹、
  图版本和输出都匹配的分片。日志记录开始时间、已完成 D0/总数、父边数、
  映射失败数、请求数、速度和 ETA。工作目录与路线 B 输出隔离，不覆盖
  `d0-parent-scan-v2/`。本地结果固定放在
  `/mnt/8T/xgr/swh-graph-poc-20260604/route-a-d0-d1-v1/`，保存
  `parent_pairs`、`no_parent`、`unresolved`、分片标记和运行摘要。
  **已完成**：实际全量产物在上述目录的 `full-d0-d1-v1/` 子目录，与样本
  隔离；后台任务已退出。每 1,024 个 D0 提交一片映射，每
  512 个 D0 提交一片图父边，去重父编号每 4,096 个提交一片反解。
  日志含开始/截止时间、百分比、速度、阶段 ETA 和保守预算计数。
- [x] **与路线 B 的 D1 对账**：从
  `s3://swh2p4/glue-results/swh-p4-20260604/d0-parent-scan-v2/` 读取已完成的
  `parent_edges/`、`frontier_d1/`、`d0_without_observed_parent/` 和
  `complete.json`，先核验该层 128/128 分片完成。对 B 的父边按
  `(child_id, parent_id)` 去重后与 A 的对应集合双向差集；另比较
  `distinct(parent_id) - D0` 所得新 D1 集合及完整遍历后无父节点的 D0
  集合。报告 A 独有、B 独有、未映射/未解码数、每个差异的示例与证据，
  不能只比较总行数：B 的 345,693 条边按
  `(child_id, parent_id, name, perms)` 去重，A 的无标签图无法核对
  `name/perms` 或同一对目录之间的多条命名边。本次核实 B 按目录对
  去重为 **345,515** 对，这是全量对账目标。验收目标是双方父目录对
  集合、新 D1 集合和无父节点 D0 集合一致，且没有未解释的解码失败；
  若不一致，先抽查公共 ORC 原始行、图边和版本差异，不能用 B 的结果
  偷填 A 的缺口或把差异目录误报为根目录。将双向差集、汇总计数、
  输入/图版本指纹和抽样证据写成独立的 `comparison.json`；验收后再把
  A 的结果及报告同步到桶内独立前缀，不覆盖路线 B 的真值输出。
  **实际验收通过**：`comparison.json` 和本地 `complete.json` 均为
  `status=passed`；父目录对、新 D1、无父边集合的双向差集全部为空，
  输入正反映射通过 65,321/65,321，未解析 0。
- [x] **补传路线 A 的 D1 结果**：2026-10-26 09:51:14 UTC 已用 `58`
  账号完成归档至
  `s3://swh2p4/new-account-588301174757/route-a/d0-d1-v1/`。
  共 16 个对象、47,418,374 字节（约 45.22 MiB）：10 份结果、4 份身份/
  控制/预算/对账文件、1 份归档清单和最后上传的 `complete.json`。
  所有对象均核对 S3 `HeadObject` 返回的 SHA-256 和大小；完成标记与
  清单读回、前缀对象数及总大小再次核对通过。本地已补写 `archive.json`。
  一个 Parquet 网络上传中断后仅补传缺失对象，未重跑图计算或启动 AWS
  计算任务。证据见
  [`ROUTE_A_D1_ARCHIVE_20261026.json`](data/raw/swh_p4/ROUTE_A_D1_ARCHIVE_20261026.json)。
- [x] **继续路线 A 生成 D2～D8**：2026-10-26 用户明确下一阶段顺序为
  先完成目录层，再统一关联 revision。以已完成的 114,293 个新 D1 为输入，
  在本地反向图逐层枚举直接 `dir` 父节点，依次生成 D2～D8；AWS 不再
  每层扫描 `directory_entry` ORC。下一层 frontier 按 node ID 去重，并
  排除已经完整展开的目录；仍须保留全部父边，包括父节点已经访问的边，
  通过原始 candidate→D0 及逐层父边恢复 seed 关联。每层或层内批次保存
  数据、校验摘要和完成标记，日志包含节点数、层数、速度、ETA、请求量
  及耗时。无目录父边的节点退出后续 frontier，但保留到最终 revision
  查询；有第 8 层 frontier 的相关 seed 标记深度边界，不能据此宣称找全。
  用户随后已授权启动；2026-10-26 10:07:04 UTC 启动，44.219 秒完成，
  退出码 0，tmux 为 `swh-route-a-d2-d8`。本阶段使用 node ID，不逐目录反解
  SHA1；已知 D0/D1 哈希映射单独保留。每层输出归档至
  `s3://swh2p4/new-account-588301174757/route-a/d2-d8-v1/`。
  D0～D8 累计 523,311 个唯一目录节点；本次保存 524,951 条父边，D8
  边界尚有 2,568 个未展开节点。本地逐层集合验收通过；全部 47 个归档
  对象的大小和 SHA-256 已与 S3 核对，日志和检查点包也已保存。
  运行和查看日志方式见
  [`ROUTE_A_D2_D8_20261026.md`](data/raw/swh_p4/ROUTE_A_D2_D8_20261026.md)。
- [x] **继续扩展到 D16**：复用 D8 的 2,568 个边界节点及完整 visited
  集合，旧层保持独立。D9 新发现 124 个目录，D10 起 frontier 为空，
  D11～D16 空层完成；总集合为 523,435 个目录节点，目录父边遍历已
  收敛。新增 297 条父边、2,711 条缓存 revision 邻接关系；本地核验与
  S3 归档均通过。见
  [`ROUTE_A_D9_D16_20261026.md`](data/raw/swh_p4/ROUTE_A_D9_D16_20261026.md)。
- [x] **D16 后统一关联 revision**：执行上方 A1，优先合并全部已缓存
  的直接 revision 邻居，统计唯一 revision；完整父边和 D0 映射一并保留。
- [x] **8 目录 revision→origin→URL 验证**：执行上方 A2/A3；新读取器
  及样本使用独立身份，确认运行限制后启动。URL 经 origin 哈希批量关联
  `origin(id,url)`，不全表扫描 revision/history/snapshot 来重建索引。
- [x] **revision 路径 URL 映射**：已执行上方 A4，共 10,107 个 origin URL。
  图遍历 frontier 为 0，未触发截断；release/snapshot 分支另见补漏结果。

不下载 469 GB 的 `.fmphgo.order` 和 1.29 TB 的 `node2swhid.bin`；当前
设计是按输入 SWHID/输出父目录和 origin 节点做 Range 读取。
全部 D0 的映射和一跳遍历已验证：113,438 次 Range GET、43,412,250 字节
正文，映射记录缓存命中 23,398 次；D2～D8 随后使用图内编号在本地完成，
不再逐目录请求反解。8 目录的 revision→origin→URL 路径和全量路径
均已验证，全量 origin URL 全部通过 SHA1(url) 身份校验。
若后续随机 Range 太慢或存在未解释的解码
问题，应停止扩量并重新评估，不要自动改为下载 2 TB 完整映射。

### 路线 B：纯 AWS ORC + Glue/Spark 查询

**当前进度：D0→D1 已完成。只有选择继续路线 B 时，才启动
D1→D2，并逐层运行至 D7→D8。**
D0→D1 使用 16 个 G.1X 扫描全部 128 个 ORC 分片，耗时 26,078 秒
（约 7 小时 15 分钟）、417,249 DPU 秒；v2 Glue 计算费约 $51.00，
加失败 v1 后已知累计约 $51.63，不含 S3/CloudWatch 等附加费用。结果为
345,693 条父边、137,691 个边上不同父目录、114,293 个新 D1，以及
10,525 个未观察到父边的 D0。详见[运行记录](data/raw/swh_p4/D0_PARENT_SCAN_20261021.md)。

按这一轮实测作粗略外推，D2～D8 还需 **7 轮全量 `directory_entry`
扫描**，每轮约 7 小时 15 分钟、约 $51 Glue 计算费；剩余部分合计约
**51 小时、$357 Glue 计算费**。这是估算，不是报价或硬上限：各轮的
frontier 大小、join/shuffle/spill 和重试会影响耗时与 DPU 费用，且未计
S3、CloudWatch 等费用。D0 阶段曾设置的 $110 上限只覆盖当时那一轮，
不能覆盖 D2～D8；启动剩余七轮前要先确认可用预算。后续 revision、
snapshot/origin 关联尚未估价。

此路线不在本地下载 273 GB 压缩图。Glue/Spark 或 Athena 直接读取公共
S3 上的 ORC 表。当前讨论的基线是：将全部 65,321 个 seed 合并为一批，
由 D0 开始逐层查 `directory_entry` 最多 **8 轮**，然后对累计的目录只批量查
`revision` **一次**，最后关联 snapshot/origin 得到 URL。这里的“纯 AWS
查询”指 ORC/Spark 路线；普通 Spark SQL 不能直接将压缩图二进制对象
当作邻接表查询。

**具体顺序**：D0 为 65,321 个去重的直属目录。第 k 轮以 D(k-1) 的
目录 ID 匹配全表 `directory_entry.target`（只取 `type=dir`），得到
`directory_id` 父目录作为 Dk。Spark 不会自动去重：每轮显式保存
`frontier_ids(depth, directory_id)`，按目录 ID 去重，用于下一轮匹配；
另存 `parent_edges(depth, child_id, parent_id, name)`，只按完整边键去重。
不同 seed 或多条路径可共享一个 `frontier` ID，但父边不能丢。为减少
每轮关联量，暂不把 1,346 万文件行或 `seed_id` 附到每条父边上；原始
`candidate -> D0` 关系和逐层父边分别保留，待找到 revision 后再回溯
`root -> ... -> D0`，还原每个 seed 的全部路径。
某个目录在**全部 128 个分片查完**后没有父边，只从下一轮 frontier
移除；它仍保留在已访问目录及最终 revision 查询中。“无父边”不等于
“已证实是仓库根目录”。
D2～D8 每轮都以已完成的上一层 frontier 为输入，扫描全部 128 个
`directory_entry` 分片，输出该层完整父边、去重后的下一层 frontier 和
分片/轮次完成标记。每轮结束后验证计数和输出，再启动下一轮；恢复时
只跳过输入配置、ORC 分片身份和输出均匹配且有完成标记的分片。完成
D8 后，将 D0～D8 的**所有唯一目录**一次性与
`revision.directory` 关联。目录是 revision 根目录只证明存在对应提交，
**还不是仓库 URL**；需要继续通过 revision 历史、snapshot 分支、
`origin_visit_status` 和 `origin` 解析 URL。同一目录可能既是某个
revision 的根，又是另一个仓库的子目录，因此命中 revision 也不能在
前 8 轮中作为停止查父目录的理由。深度 8 是成本上限，不是完备性证明；
凡到 D8 仍有 frontier 的 seed 均标记为 `depth_limited`，因为未查第 9 层。

- [ ] **开始 D2～D8 前检查**：确认 D0→D1 的 `complete.json`、128 个
  分片输出和 frontier 位于预期 S3 前缀；固定每轮输入/输出前缀、配置
  指纹及源对象 ETag/大小，避免覆盖已完成层。为每层配置分片级检查点、
  进度/ETA、失败状态和显式 resume；不能把单轮作业成功当作七层全部完成。
- [ ] **逐层生成 D2～D8**：每轮使用上一层去重 frontier，对全部 128 个
  `directory_entry` ORC 分片并行匹配 `type=dir AND target=frontier_id`，
  保存该轮全部父边及去重后的新 frontier。每轮全分片完成后验收输出和
  计数，再继续下一层；保留多父边和路径关系，不按 1,346 万个文件逐行
  查图，不因 revision 或 URL 命中而跳过父边。
- [ ] **一次性 revision 关联**：D0～D8 的目录集合去重后，对
  `revision.directory` 做一次批量关联，保留全部 `(root_id, revision_id)`
  命中；再解析 `revision_history`、release/snapshot 分支、
  `origin_visit_status` 和 `origin`。只对命中的根目录沿已保存的父边
  反向回溯到各个 D0，保留多父目录、多路径及对应 seed；最后按
  `seed_directory_id = candidate.directory_id` 回连原始候选文件行，保留
  文件名、content ID 和来源 CSV 行。未匹配和深度截断单独输出；复用
  旧索引必须先明确选择复用并验证配置指纹、分片清单和完成标记；
  若已清理旧探针索引则直接从公共 ORC 设计批量关联，不得假设缓存仍在。
  证据表写入 `swh2p4`。
- [ ] **预算决策与备选优化**：运行 D2～D8 前核对当前余额和预算；当前
  基于 D0→D1 实测的七轮估算约 $357 Glue 计算费、约 51 小时，明显高于
  之前 D0 阶段的 $110 上限。若预算不足，先暂停全量七轮并评估“一次
  扫描建 `target` 反向索引”方案；该方案也要计入 shuffle、写入和后续
  S3 存储费用，未经评估不启动。

### 两条路线共同的输出与选择条件

- [ ] 每条映射证据至少保留 seed `directory_id`、父目录路径、revision
  SWHID、snapshot/origin SWHID、origin URL、图层/表来源、处理状态和
  未解析原因；最终再按目录关联候选文件名、content ID 和来源 CSV 行。
- [ ] 允许一个目录对应多个 revision 或 origin；输出“未找到”“受深度或
  资源限制而截断”“数据关系缺失”等不同状态，不能把未映射记录静默丢弃。
- [ ] 路线 B 已用于 D0→D1；当前先验证图文件已下载的路线 A。不要同时
  建设两套全量索引。选择后续路线前，先比较同一批
  最多 8 个 seed 的正确率、完成时间、可恢复性、传输/磁盘占用、AWS
  DPU/扫描成本及 S3 请求/存储成本。
- [ ] 两条路线的映射证据本身不是版本级 P4_16 确认。下载完整历史后，
  用实际 revision、文件路径及已有内容筛查结果核对来源；最新已确认
  P4_16 revision 只决定训练视图，不限制镜像与 issue 的收集范围。

### 已完成的路线调查

- [x] 调查公开压缩图、查询依赖和部署限制。常规单向反向图加完整
  SWHID 映射约 2.024 TB；官方读取路径使用本地文件加载/内存映射，
  不是 S3 托管查询服务。对象体积、来源与工程假设见
  `PREBUILT_GRAPH_FEASIBILITY_20261021.md`。
- [x] 确认 `history-hosting` 小图没有 `directory_entry` 边，不能从任意
  candidate 父目录找 revision 根目录；provenance Parquet 在当前 AWS
  身份下返回 403，暂时不能作为替代。
- [x] 评估并实现压缩图按需映射路线：必要图对象约 272.6 GB 下载、
  约 286 GB 展开占用，SWHID/node ID 映射按需 Range 读取；目录层
  已完成 D0→D16 并验收归档，revision/origin 全量流程及文件回连也已实测。

## 映射与仓库收集

- [x] 使用本地图关联所有可达目录（含直属目录）与直接 revision，
  再沿已缓存的非目录反向边连接历史、release、snapshot 和 origin。
  保存 revision 路径的根目录、含文件 revision 及图边；一目录/提交
  多 origin 关系保留。随后先对无 revision 入口的 337 个 D0 补查
  release/snapshot 路径，再对全部 523,435 个目录完成该类入口审计。
- [x] 写出 revision 路径的目录/文件→revision→origin URL 证据表、
  URL 清单和 `no_revision_entry` 清单；按 `directory_id` 回连全部
  13,465,757 条候选命中，保留其中 6,852 条的旧缺口状态。补充路径
  已独立归档，尚未合并进新的文件级表。
- [x] 全量审计 release/snapshot 入口，发现 41 个新 URL、594 条
  新 D0→origin 关系，并验收归档；见
  [`SNAPSHOT_AUDIT_20261027.md`](data/raw/swh_p4/SNAPSHOT_AUDIT_20261027.md)。
- [x] 生成两路统一文件级证据视图与本机 10,201 行下载清单，
  保留多对多关系；见
  [`UNIFIED_ORIGIN_DOWNLOAD_LIST_20261027.md`](data/raw/swh_p4/UNIFIED_ORIGIN_DOWNLOAD_LIST_20261027.md)。
  65,321 个目录与 10,201 个原始 URL 都不是已确认的规范仓库数。
- [x] 将下载需要的 candidate revision 图内编号反解为原始哈希/SWHID；
  恢复准确文件路径和分支时读取标签或仓库内容，不凭无标签图节点猜测。
- [ ] 将 `.git` 等 URL 别名归并为规范仓库，保留目前已发现的 10,201 条
  原始 origin URL 及全部证据；后续覆盖待复核内容时该数量可能增加，
  不能直接称为不同规范仓库数。
- [x] 查询候选 revision 日期；创建 revision/ID 外部表并对 128 个公共 ORC
  分片执行 bounded Join。实际命中 317,804 行、扫描 301,587,506,341 字节，
  按 $5/TB 约 $1.51；结果和每分片 resume 状态归档到
  `s3://swh2p4/new-account-588301174757/route-a/revision-dates-v1/`。
- [x] 将 6,532,814 条 revision/origin 关系接上日期并按 origin 生成首候选。
  10,107 个 origin 中 10,106 个有 revision date；结果归档到
  `s3://swh2p4/new-account-588301174757/route-a/revision-rank-v1/`。
- [x] 专项处理唯一缺日期的 opam petr4 origin：两条 SWH `synthetic=true`
  的 tar revision 原始日期均为空，按包版本 `0.1.1 > 0.1` 修正首候选，
  但保留日期空值及未确认状态。后续读取
  `s3://swh2p4/new-account-588301174757/route-a/revision-rank-v2/`
  的首候选表；v1 全量日期关系表继续保留。
- [ ] 对全部 10,201 个原始 origin URL 规范去重，按规范仓库下载可获取的
  完整历史 Git 对象、refs 和 commit 索引到本地；包/归档下载可获取的
  历史版本。初轮已完成 9,250/10,069 个 Git 镜像和 3,665 个归档，
  其余失败/不可访问项及 API 终态见结果总览。按 tracker 独立保存
  issues/合并请求及相关记录。补齐缺口后核对
  候选 revision 和已有 P4_16 筛查证据，另选最新已确认版本作为训练
  视图；保留原始 origin provenance、内容去重关系及不可获取的历史缺口。
- [ ] 后续复核 199,369 条仍需判定的 `needs_review` 文件命中；原始
  review 为 202,389 条，其中 3,020 条已识别为空/非源码。当前
  candidate 集不能误称为全部可能含 P4_16 的仓库。

## 历史成本估算与状态快照

`directory_entry` ORC 压缩对象约 29.77 TB。此前一个 ORC 分片的
Athena 父边探针扫描约 169.7 GB，约 $0.85；按 128 片同量级外推，
**仅一轮 Athena** 全表扫描约 $109。这是 Athena 历史扫描费估算，不能
用作 Glue 费用。Glue 的新基准来自已完成的 D0→D1：26,078 秒、417,249
DPU 秒，按 $0.44/DPU 小时估算约 $51.00。若 D2～D8 每轮相近，七轮
约 51 小时、$357 Glue 计算费；实际费用随各层执行和重试变化，且不含
S3、CloudWatch、revision/snapshot/origin 查询费用。D0→D8 目录边扫描阶段
按此估算累计约 $408.63 Glue 计算费（含失败 v1 的已知费用）；这不是
总项目费用或硬上限。ORC 全对象约 29.77 TB
是压缩对象大小，不等于每轮 Spark 的实际扫描字节或计费 DPU 用量。

路线 A 的已知一次性传输量约 272.6 GB，展开后约 286 GB；D0→D1
实测约 29 分 20 秒，113,438 次 Range GET。D2～D8 使用已有图内编号，
实测 44.219 秒，无网络图查询；这些目录层已替代路线 B 对应的全表扫描。
revision/origin 全量路径及 128 CSV 回连现已实测：2026-10-26 13:26:53 UTC
开始，15:42:11 UTC 完成文件回连；归档结束时间见全量运行报告。
Athena 累计扫描 49,649,462,040 字节，按 $5/TB 约 $0.248247，另计 S3。

2026-10-21 的三次 Athena 有界试查没有得到 URL。8 目录 Glue 探针的
第一次运行因快照索引 shuffle 临时盘耗尽而失败，运行 2,741 秒，
计算费粗估约 $6.70。第二次运行执行到服务端 180 分钟超时；它已保存
7/8 个目录、147 条证据、1 个 origin URL，第 8 个未完成，不能把
其余扫描结果称为全量覆盖。旧索引的最新容量与去留见上方 S3 小节；
此前关于第二次运行仍为 `RUNNING` 或 0 URL 的记录已过时。

全量 65,321 个 seed 已处理：revision 路径映射 64,984 个；其余 337 个
没有 revision 入口，但 release/snapshot 补漏已全部得到 URL。路线 A
目录和非目录遍历均已收敛，文件回连验收通过；
S3 归档完成状态见本文件开头与全量运行报告。下一阶段涉及新 AWS
操作时须核对账单并确定预算；旧的约 $33 余额和 D0 的 $110 作业上限不能
代表当前可用余额或后续预算。完整图调查、
对象大小及实现假设见 `PREBUILT_GRAPH_FEASIBILITY_20261021.md`。
