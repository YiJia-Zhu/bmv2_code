# SWH `.p4` 文件到仓库 URL：图映射、时间成本与恢复评估

状态：2026-10-19。本文评估**仓库发现阶段**，即把已有的 `.p4` 文件名命中映射到 SWH origin URL；不包括读取源码判定 P4_16、下载完整仓库、训练数据清洗。本文中的 `DIR_*`、`REV_*` 等是演示代号，不是实际 SWH ID。

## 当前进度与结论

- Athena 已扫完 SWH Graph `2026-06-04` 的 128 个 `directory_entry` ORC 对象；结果位于 `s3://swh2p4/athena-results/swh-p4-20260604/p4-directory-hits/`，CSV 合计 3,366,228,106 字节。它只有文件名、直接父目录和内容 ID，**没有仓库 URL**。本地证据：[`athena_run_summary.json`](data/raw/swh_p4/discovery/athena_run_summary.json)。
- Glue 5.0 的 schema probe 已成功，用 2 个 G.1X worker 运行 72 秒、145 DPU 秒。七张表各抽查一个 ORC 对象，Spark 读取的列见 [`glue_graph_schema_probe_20261019.json`](data/raw/swh_p4/discovery/glue_graph_schema_probe_20261019.json)。这是 schema 核验，不是图映射或吞吐压测。
- [`swh_glue_graph_mapper.py`](data/tools/swh_glue_graph_mapper.py) 有本地小图测试，**尚无真实 Graph 的端到端成功运行**。目前不能给出可信的“跑完需要 X 小时、花 Y 美元”承诺。
- Mapper 已加入按 Athena CSV 分片索引和完成阶段的 resume：`--CANDIDATE_MANIFEST_URI`、`--SHARD_INDICES`、固定 `--MAPPING_ID`，重跑时加 `--RESUME=true`。每个 CSV 先存独立 `normalized_hit_shards/<index>/`；选中的分片合并为一次 `normalized_hits/`，后面的图连接共用这一批命中。阶段数据写完后创建独立完成记录，记录中的配置指纹匹配才跳过；失败后的新尝试使用新 S3 路径。**目录闭包和 revision BFS 的中途层数不能跨 Glue run 续跑**。
- 2 个 G.1X worker 对单个目录做全前缀父边反查曾在 15 分钟硬超时后结束，消耗 1,801 DPU 秒，无完整结果；这**不能线性推算**分布式批处理的吞吐，但足以说明不能把当前 mapper 直接当作已验证的生产作业。
- 2026-10-19 的两分片试跑读入 Athena CSV index 85（52,279 条）和 94（45,442 条），共得到 **97,721 条去重 `.p4` 命中**。云端 `--RESUME=true` 已验证跳过两个 CSV 分片和合并阶段。继续映射时，2 个 G.1X 在约 9.6 分钟内仍未完成 `directory_closure`，已主动停止；**没有得到 origin URL**。细节和费用见下表。

## 真实 ORC 表与连接字段

以下是 `s3://softwareheritage/graph/2026-06-04/orc/<表名>/` 下抽样 ORC 的实际 schema；每表有 128 个 ORC 对象。这里只列映射相关字段，详细类型见上述 Glue manifest。

| 表 | 关键原始列（实际类型） | 在映射中的用途 |
| --- | --- | --- |
| `directory_entry` | `directory_id:string`, `name:binary`, `type:string`, `target:string`, `perms:int` | `type=file` 时 `target` 是内容 ID；`type=dir` 时 `target` 是子目录 ID。`name` 需从 bytes 解码。 |
| `revision` | `id:string`, `directory:string`, `committer_date:timestamp` | `directory` 是该 revision 的**根目录 ID**；`id` 是 revision ID。 |
| `revision_history` | `id:string`, `parent_id:string`, `parent_rank:int` | `id` 是子提交，`parent_id` 是父提交；用于从候选旧提交走向后代分支 tip。 |
| `snapshot_branch` | `snapshot_id:string`, `name:binary`, `target:string`, `target_type:string` | 将 snapshot 的分支指向 revision；分支名也需解码。 |
| `origin_visit_status` | `origin:string`, `visit:bigint`, `date:timestamp`, `status:string`, `snapshot:string`, `type:string` | `snapshot` 关联 snapshot，`origin` 关联 origin。 |
| `origin` | `id:string`, `url:string` | 得到仓库的 SWH origin URL；这个 URL 不一定在 GitHub。 |
| `release` | `id:string`, `target:string`, `target_type:string` | 可选地将直接指向 revision 的 release tag 解成 revision tip。 |

注意：`directory_entry.directory_id` **不是仓库 ID**。一个目录对象可以被多个根目录或多个 revision 复用；同一 revision 也可能出现在多个 origin 的历史中。因此正确输出是“文件命中 × revision × snapshot × origin”的多对多关系，另行生成去重 URL 清单。`repository_name` 只是 URL 最后一个路径段去掉 `.git` 的显示名，不能替代唯一的 `origin_id` / `origin_url`。

## 具体例子：`router.p4` 如何找到仓库

假设 Athena 从 `directory_entry` 筛出一条 `.p4` 命中（实际 ID 为 40 位十六进制，下方用代号）：

```text
directory_entry: directory_id=DIR_PKG, name=b"router.p4", type=file, target=CONTENT_1
Athena CSV:      directory_id=DIR_PKG, name=router.p4, type=file, content_id=CONTENT_1
```

此时仅知道文件直属 `DIR_PKG`。Spark 在同一表读取 `type=dir` 的行，将目录边反向连接：

```text
directory_entry: directory_id=DIR_SRC,  name=b"pkg", type=dir, target=DIR_PKG
directory_entry: directory_id=DIR_ROOT, name=b"src", type=dir, target=DIR_SRC

DIR_ROOT --src--> DIR_SRC --pkg--> DIR_PKG --router.p4--> CONTENT_1
```

因此候选路径为 `src/pkg/router.p4`。再关联 `revision.directory = DIR_ROOT`：

```text
revision: id=REV_OLD, directory=DIR_ROOT, committer_date=2020-01-01
```

这**证明 REV_OLD 的完整根目录含有该 `.p4` 路径**。但 snapshot 的当前分支 tip 可能已经是后续提交，甚至 tip 本身已删除 `router.p4`。所以沿 `revision_history` 的父子边由旧到新追踪：

```text
revision_history: id=REV_MID, parent_id=REV_OLD
revision_history: id=REV_TIP, parent_id=REV_MID
snapshot_branch:  snapshot_id=SNAP_1, name=b"refs/heads/main",
                  target=REV_TIP, target_type=revision
origin_visit_status: origin=ORIGIN_1, visit=17, snapshot=SNAP_1, status=full
origin: id=ORIGIN_1, url=https://github.com/team/router.git
```

结果：`REV_OLD` 的 `src/pkg/router.p4` 属于 `https://github.com/team/router.git` 的历史，`revision_depth=2`，`revision_relation=ancestor_of_branch_tip`。若另一个 origin 的 snapshot 也引用 `REV_OLD` 或其后代，就保留第二个 URL；不会任意只选一个仓库。若 `snapshot_branch.target` 直接等于 `REV_OLD`，深度为 0。最终 `origin_revision_candidates/` 保存完整证据链，`repository_candidates_csv/` 给出去重的 URL、显示名及 GitHub 标记。

当前脚本只处理直接指向 revision 的分支，及“一层 release 直接指向 revision”的分支；没有解析 snapshot branch alias、多层 release 链。历史深度上限会导致漏召回；如果没有提供 `revision_history`，只能命中直接分支 tip。`unmapped_revision_candidates/` 是没有 origin 映射成功的候选 revision，不等同于所有未解析 alias 的明细。这里的 URL 表示 SWH 记录的 origin，不保证今天还能 `git clone`，也不证明文件是 P4_16。

## 规模、时间与美元估算

2026-10-19 对公开 S3 的七个表前缀做了只读对象列表统计；每表 128 个对象。数值是**压缩 ORC 对象大小**，不是 Spark 实际读取量或账单量：

| 表 | 对象字节数 | 约合 TB（十进制） |
| --- | ---: | ---: |
| `directory_entry` | 29,773,222,404,015 | 29.773 |
| `revision` | 764,515,460,406 | 0.765 |
| `revision_history` | 248,102,334,884 | 0.248 |
| `snapshot_branch` | 448,941,188,241 | 0.449 |
| `origin_visit_status` | 104,332,778,391 | 0.104 |
| `origin` | 16,858,059,775 | 0.017 |
| `release` | 15,796,179,002 | 0.016 |
| **合计** | **31,371,768,404,714** | **31.372** |

其中 `directory_entry` 占 94.9%。文件名扫描此前由 Athena 实际计量 10,509,202,350,395 字节，但那次只投影 `.p4` 查询所需列；不能据此认为目录父边扫描也只会读同样字节。当前 Spark `read_directory_edges()` 使用 `directory_id/name/type/target/perms` 等列，并做全局去重与多轮 join。多轮 revision 历史追踪也会产生 shuffle 和中间结果。

仅计算**假定一次读取全部 31.372 TB 压缩对象**的理想单遍耗时，假定集群能持续取得下列有效聚合吞吐，且忽略启动、解码、shuffle、写 S3 和重复读取：

| 假定有效吞吐 | 一次读完约需 |
| ---: | ---: |
| 0.25 GiB/s | 32.46 小时 |
| 1 GiB/s | 8.12 小时 |
| 4 GiB/s | 2.03 小时 |

这些吞吐量**没有实测**，不是 2/20/50 个 worker 的性能预测，也不是实际作业的严格下界：ORC 列裁剪可能少读，反复 join 又可能多读。实际 mapper 还要计算目录闭包、多个 revision/branch/origin join，以及 `count()`、`dropDuplicates()`、`checkpoint()`、输出 Parquet/CSV；目录图存在复用与扇出，可能出现内存、磁盘或 shuffle 瓶颈。同一目录若经多个父目录或路径可达，闭包行数可能远大于候选目录数；当前目录闭包按路径去重，异常循环只靠深度上限止住。**现有版本的跑完时间无法可信估计，也可能在超时前始终无法完成**。

按现有清单使用的 us-east-1 Glue G.1X 估算单价 **0.44 USD / DPU·小时**，且 1 个 G.1X worker 计 1 DPU，公式是 `worker 数 × 运行小时 × 0.44 USD`。下面只是**预算情景**，不是完成时间预测：

| 情景 | Glue 计算费约 |
| --- | ---: |
| 2 worker × 1 小时 | $0.88 |
| 20 worker × 6 小时 | $52.80 |
| 20 worker × 24 小时 | $211.20 |
| 20 worker × 72 小时 | $633.60 |
| 50 worker × 24 小时 | $528.00 |
| 50 worker × 72 小时 | $1,584.00 |

已实测的单目录探针 1,801 DPU 秒按该单价约 $0.22，且**没有完成映射**。上述费用未包含 S3 请求/中间结果存储、日志、可能的跨区域流量、失败重跑，也不代表账号当前余额或抵扣额度。已有 Athena 文件名扫描属于已发生的独立费用，不应再计入未来 Glue 计算费。单价需在正式运行前用 [AWS Glue 定价页](https://aws.amazon.com/glue/pricing/) 和账号账单核对。

### 两分片实际试跑

以下费用只按 `DPUSeconds × $0.44 / 3600` 估算 Glue 计算费；不是账单实扣值。Glue 5.0 job `swh-p4-graph-map-pilot` 设为 2 个 G.1X、15 分钟硬超时、0 次自动重试，输出在 `s3://swh2p4/glue-results/swh-p4-20260604/graph-map-v2/runs/pilot-85-94-20261019/`。

第一轮成功作业使用了旧的 `_SUCCESS` 判定，Glue 并未生成该文件；在确认该作业状态为 `SUCCEEDED` 且三个阶段都有元数据和 Parquet 后，分别补上 `_COMPLETE` 标记，供新版读取。此后新阶段使用独立 JSON 完成记录，失败重试写新路径，不依赖覆盖删除权限。

| 运行 ID 前缀 | 结果 | 执行秒数 | DPU 秒 | 估算 Glue 费 |
| --- | --- | ---: | ---: | ---: |
| `jr_62034c6d` | 两个 CSV 规范化成功，97,721 条 | 110 | 220 | $0.0269 |
| `jr_4a430b98` | 旧版恢复误判 `_SUCCESS` 缺失，写覆盖失败 | 120 | 240 | $0.0293 |
| `jr_9d3164ff` | 修复后成功跳过两个分片及合并阶段 | 92 | 184 | $0.0225 |
| `jr_3e169d2b` | 已恢复 CSV，目录闭包扫描中主动停止 | 575 | 1,151 | $0.1407 |
| **合计** | **仍无仓库 URL** | | **1,795** | **$0.2194** |

Graph 作业的 CloudWatch 日志显示，一个 Spark stage 有 **221,850 个 task**，在约 9 分钟时只完成前 **144 个**（当时的进度快照，约 0.0649%）。各 task 成本和耗时不一定相同，不能把这个比例线性外推为精确 ETA；但 2 个 G.1X 显然无法在 15 分钟内完成这个 stage。费用也不能按“两个 CSV × 64”推全量，因为最重的 Graph 扫描是共享步骤，并且当前作业没有跑完它。下一步要先改变目录边访问策略或大幅扩容并做新的有界试验，不能据此承诺 128 分片的总时长和总价。

仅作资源量级参考：如果该 stage 的 **全部** 221,850 个 task 都平均用 11 秒、每个 executor 同时跑 4 个 task、除 driver 外的 worker 都满载、且扩大 worker 数后 task 时间不变，那么 20 个 G.1X 的这个 stage 约 8.92 小时 / $78.49；50 个约 3.46 小时 / $76.09。这是**单一 stage 的理想并行模型**，不含其他 Graph stage、shuffle、重试、S3 等费用，不是全量完成时间预测。

### 0 号 CSV 实测

2026-10-19 单独选择 `--SHARD_INDICES=0`，输入是 `part-00000-005adbfc-3c54-4ca6-b407-4f00cb1d6023.csv`（19,487,431 字节）。运行 ID 为 `pilot-0-20261019`，输出位于 `s3://swh2p4/glue-results/swh-p4-20260604/graph-map-v2/runs/pilot-0-20261019/`。

| Glue 运行 ID 前缀 | 资源与结果 | 执行秒数 | DPU 秒 | 估算 Glue 费 |
| --- | --- | ---: | ---: | ---: |
| `jr_a9f8e8d9` | 2 个 G.1X；CSV 规范化成功，82,488 条去重 `.p4` 命中 | 81 | 162 | $0.0198 |
| `jr_4562f921` | 20 个 G.1X；复用 CSV，目录边扫描中主动停止 | 337 | 6,747 | $0.8246 |
| **合计** | **无目录闭包或仓库 URL** | | **6,909** | **$0.8444** |

第二次运行的 CloudWatch 日志确认 `normalized_hit_shards/00000` 和 `normalized_hits` 均为 `resumed`。目录扫描的 Spark stage 仍有 **221,850 个 task**；在运行日志的两个快照之间，约 104 秒内完成了 846 个 task，约 **8.13 task/s**。如果这一速率从头到尾保持不变，仅这个 stage 剩余工作约需 **7.53 小时、$66.28**（20 个 G.1X、$0.44/DPU 小时）。这只是单一 stage 的外推，未包含去重/shuffle、目录闭包的其他深度、revision、snapshot、origin 阶段；无法据此声称 0 号 CSV 能在 8 小时内完成，也无法给出全量总价。

这组吞吐来自试跑时的脚本版本。随后本地 mapper 去掉了目录边全局提前去重和未使用的 `perms/source_orc` 列，并修复了分片重试路径；本地 Spark 测试通过，但这些变更**尚未在真实 Graph 上测得新吞吐**。上面的 7.53 小时不能视为新版脚本的实测 ETA。

尤其不能将 0 号 CSV 的运行时间乘以 128：128 个 CSV 合并为一次批次时，最重的全局目录边扫描是共享的；相反，如果按 128 个独立 Glue 作业运行，会重复付费扫描相同的大表。0 号 CSV 与 128 个 CSV 的闭包规模、复用路径和后续 revision 扇出也不一定线性。

作为预算预留而非 ETA：如果给 20 个 worker 设 24 小时硬上限，单次 Glue 计算费上限约 $211；72 小时约 $634。到达上限时作业可以失败且没有最终仓库清单，重跑还会再次产生费用。当前证据无法证明 20 个 worker 在上述任一时限内会完成。

## Batch 与 resume 边界

最小的输入恢复单位是 Athena CSV 分片（manifest `objects[].index`），不是单个 `.p4` 文件名。一次 Glue 运行可选择 `--SHARD_INDICES=85,94` 试跑，或最终选择 `all`；同一批只构造一次目录闭包并扫描共享的 Graph 表。128 个 CSV 分成 128 个独立图作业会重复扫描约 29.77 TB 的 `directory_entry`，没有成本优势。增加分片会改变批次指纹，必须使用新的 `MAPPING_ID`；不能把两分片试跑的 `directory_closure` 直接当作 128 分片结果。

首轮可设 `--STOP_AFTER_STAGE=normalized_hits`，只测 CSV 读取和规范化；随后同一个 `MAPPING_ID`、相同分片索引和 Graph 参数，去掉该参数并加 `--RESUME=true` 继续图阶段。若已有完成阶段与当前配置指纹不符，作业拒绝复用。输入身份使用 manifest 中每个 CSV 的 URI、大小和 ETag；当前实现不在运行时重新向 S3 `HeadObject` 核验 ETag。

| 环节 | 当前是否能 resume | 依据 |
| --- | --- | --- |
| Athena `.p4` 文件名扫描 | **能** | 128 个对象各有独立 query ID、稳定 CSV key、manifest；`run_swh_athena_p4.py --resume` 跳过核验过的 `complete` 分片。此环节已完成。 |
| Glue schema probe | 不需要 | 七个单文件 footer 的小作业已成功。 |
| Glue CSV 输入 | **按分片可恢复** | 每个选中的 manifest index 有独立 Parquet 和完成标记；已完成分片跳过。 |
| Spark 目录祖先闭包 | **只在整阶段完成后可恢复** | 完整闭包写出后跳过；阶段中途超时须重算。`CHECKPOINT_URI` 仍只是单次 Spark 作业的 lineage checkpoint。 |
| Spark revision 后代追踪 | **只在整阶段完成后可恢复** | 完整 origin 映射写出后跳过；BFS 中途超时须重算，默认上限 `MAX_REVISION_DEPTH=10000`。 |
| 派生结果写入 | **按完成阶段可恢复** | 阶段 Parquet/CSV 和 `_stage_meta/` 完成标记、指纹核验后复用；未完成阶段会重写。 |

因此**现在不建议直接启动 128 分片全量 Glue mapper**。先做有预算与硬超时的两分片试算，观察目录边扫描、每层 frontier、shuffle、耗时与 DPU 秒。若目录闭包无法完成，需要可复用的按 child directory ID 分桶/分片反向索引，或评估 SWH compressed graph；当前阶段标记不能解决一次图阶段内部的 29.77 TB 扫描。正式阶段还要明确哪些 alias/release/history 截断允许进入 `unmapped`，以免把候选 URL 清单误称为“所有含 `.p4` 的仓库”。
