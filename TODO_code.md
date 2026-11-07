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

### 已选映射路线：本地图

固定使用 SWH Graph `2026-06-04`，本地数据位于
`/mnt/8T/xgr/swh-graph-poc-20260604/`。从 65,321 个文件直属父目录
D0 沿图中父目录边扩展至 D16 并收敛，再沿 revision 历史及
release/snapshot 边找到 origin；按需反解节点 ID、关联公共 `origin`
ORC 得到 URL，最后回连 128 个 candidate CSV。目录边、revision 和
origin 映射及文件回连都已完成并验收，后续无需再次扫描全量
`directory_entry` ORC。详见
[`ROUTE_A_D9_D16_20261026.md`](data/raw/swh_p4/ROUTE_A_D9_D16_20261026.md)、
[`ROUTE_A_ORIGIN_FULL_20261026.md`](data/raw/swh_p4/ROUTE_A_ORIGIN_FULL_20261026.md)
及[统一来源清单](data/raw/swh_p4/UNIFIED_ORIGIN_DOWNLOAD_LIST_20261027.md)。
这些来源证据不能代替文件在仓库历史中的版本核验。

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
导出为 `candidate_repositories.csv`；GitHub 补集尚未编译确认 P4_16，
也尚未下载新增仓库的完整历史。

2026-11-06 已启动
[`screen.py`](data/pipelines/github_p4_supplement/screen.py) 的单文件全量
筛查，tmux 会话 `github-p4-screen`，日志
`data/raw/github_p4_supplement/screen/run.log`。两个独立账号的
`GITHUB_API_TOKEN` 和 `GITHUB2_API_TOKEN` 均已验证可用，各有
5,000 次/小时 REST 主配额。启动前已完成 120 个唯一 blob 的试跑，
其中 106 个为静态 `p4_16_likely`；这不是编译确认。仓库树样本有
113 个完成，慢仓库不会再阻塞其他 worker。检查点、原始 blob 和
`status.json` 在 `data/raw/github_p4_supplement/screen/`，运行中断可
用 README 中相同命令恢复。单仓库超过 128 次子树请求会单列
`tree_limit`，后续专项补查。

1. [ ] **冻结搜索与身份清单。** 已从搜索检查点导出 CSV，记录搜索
   时间、`complete/failed/unresolved` 分区、GitHub 仓库 ID、规范 URL、
   搜索来源和已知 `.p4` 路径。按 GitHub ID 去重，并与已有 SWH origin/
   本地镜像对账；同一仓库复用已有源码与证据，保留全部搜索命中。
   3 个 `unresolved` 分区列为覆盖缺口；无文件搜索命中的仓库仍保留，
   不能计为阴性。名称相同仅是对账线索，重命名后需核实 GitHub ID。
2. [ ] **先找文件，暂不下载完整历史（运行中）。** 已有 `extension:p4` 路径的仓库
   可先按搜索命中的 blob SHA 定点取内容，之后核实该路径与当前 HEAD
   的关系；其余仓库按固定的默认分支 HEAD SHA 查询 GitHub Git Trees，
   筛出 `.p4` 文件。递归 tree 若返回 `truncated=true`，逐层遍历
   未覆盖的子树；截断、404/403、重命名和权限错误单列状态。只有完整
   检查过该 HEAD 的树，才能标记 `no_p4_on_default_head`；这个状态
   **不表示其他分支或历史版本没有 `.p4`**。搜索索引里已过期的路径
   也要记录为 `stale_hit`，不能冒充当前 HEAD 文件。
3. [ ] **只取 `.p4` 字节并按 Git blob OID 缓存（运行中）。** 记录仓库 ID、HEAD
   SHA、路径、blob OID、字节数、抓取时刻和错误；相同 blob 只取和判定
   一次，但保留它在所有仓库/路径/版本的关系。先查本地 SWH 内容缓存
   是否已有相同 SHA-1 Git blob，再对缺失对象调用 GitHub blob 接口。
   超限、截断、LFS 指针或获取失败须明确标记，不能当作非 P4_16。
4. [ ] **复用 SWH 的版本与质量判定（运行中）。** GitHub 文件字节及 Git blob
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

**单文件筛查的执行顺序与请求量：** 先从搜索检查点导出并去重仓库
清单，与现有 SWH 镜像对账。对已有文件搜索证据的 2,101 个仓库，优先
读取命中的 `.p4` blob；对其余 43,854 个仓库，先固定默认分支 HEAD，
读取 tree 找 `.p4` 路径，再只读取这些路径的 blob。普通仓库在此阶段
无需 `git clone`、下载压缩包或抓取非 `.p4` 源码。每个唯一 blob 复用
现有 SWH 单文件版本/质量函数判定一次，结果回连到全部仓库和路径；
需要 P4C 确认时，才定点获取 include 和目标头文件。这样得出的是
当前搜索快照/HEAD 的证据；其他分支和历史版本按上面的第 7 步补查。
确认新增仓库后，才进入完整历史、commit 和 issue 的下载阶段。

执行前先以 100～500 个仓库试跑，分别抽取有/无文件搜索命中的仓库，
记录每仓 HEAD/tree/blob 请求数、`.p4` 命中率、tree 截断率、下载
字节、403/429 和实际吞吐，再按剩余工作估计 ETA。仅 43,854 个无
文件搜索命中的仓库，假设每仓一次 tree 请求就已经需要至少 43,854
次；按单个普通认证 GitHub REST 主配额 **5,000 次/小时** 粗算，光
这部分请求的理论下界约 **8.8 小时**。固定 HEAD、额外子树、blob
下载、配额等待及失败重试都使实际时间更长；不能据此承诺全量完成
时间。搜索阶段观测到的代码搜索 10 次/分钟、仓库搜索 30 次/分钟，
不适用于 tree/blob 请求。运行时以 `x-ratelimit-remaining`、
`x-ratelimit-reset` 和 403/429 响应为准，同时对次级限流退避，不能
仅按线程数提高并发。

**机器与账号：** 先用当前机器和 token 测量瓶颈。若瓶颈是本机带宽、
CPU 或磁盘，其他机器可分片并共享去重/检查点；若瓶颈是 GitHub API
配额，同一 token 分散到多台机器不会增加配额。同一 GitHub 账号的
多个 token 也不能直接按 token 数乘以主配额；独立账号的实际配额和
次级限制须分别从响应头核实，不能假设线性加速。无须在试跑前准备
另一台机器或账号。每轮记录已查/总数、各状态数量、请求速率、
开始时间和滚动 ETA；按仓库和 blob 持久化检查点以便恢复。

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
