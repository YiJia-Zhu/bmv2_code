# 四机下载交接

## 初轮结果（2026-11-05 核对）

四份本地 `manifests/state.sqlite3` 和 `status.json` 均已停止，
`final=true`、`pause_reason=queues_processed_review_gaps`；最后一个分片
于 2026-11-03 09:50:13 UTC 更新。10,069 个 Git 目标中，9,250 个
bare mirror 已校验完成、201 个 `failed`、618 个 `inaccessible`。
成功镜像的逐仓库可达 commit 计数合计 17,738,711，镜像字节数合计
584,468,607,659；fork 间的 commit 可能重复，字节数不是网络流量。

初轮 API 页任务 352,497 个 `complete`，392 个 `failed`；重排前 HTTP
401/403/404/410/422 分别为 1,048/9/2,995/30/4 个终态；
归档对象 3,665 个 `complete`、42 个 `failed`。`gitlab.com` 的
920 个旧 401 已于 2026-11-05 重排为 `pending`，等待手动启动 API 补抓。
已完成镜像与 API 页不等于原站不可访问历史、LFS 和外部子模块均已收齐。
逐阶段统计见 `../../swh_p4/RESULTS_SUMMARY_20261022.md`；
下一步见 `../../../../TODO_code.md`。

### 补抓 gitlab.com 旧 401

2026-11-05 已在四个分片停止时执行过一次 `--apply`，并为每个分片
生成 `manifests/state.before_gitlab_401_20261105T093908Z.sqlite3` 备份。
重排前的数量分别为 56、632、154、78；目前 920 个任务均为 `pending`，
旧 `gitlab.com` 401 为零。**无需再执行重排**。于本目录手动启动：

```bash
SWH_HISTORY_PROXY=http://127.0.0.1:7892 bash ./run_all.sh --api-only
```

今后若出现新的旧 401，`--apply` 会为每个有命中任务的分片备份 SQLite，再只把
`host='gitlab.com' AND status='unavailable_http_401'` 改为 `pending`。
它不修改其他 GitLab 实例、其他 HTTP 状态或已完成任务。

```bash
python3 retry_failed.py --gitlab-401
python3 retry_failed.py --gitlab-401 --apply
```

上面的 `run_all.sh --api-only` 只处理 API 队列，不重跑 Git 镜像或归档。
四个分片默认共用
项目根目录 `.env` 中的 `GITLAB_TOKEN`；按 Ctrl+C 可停止并保留检查点。
成功读取 issue/MR 列表后还可能发现更多评论或分页任务，因此最终 API
请求数可能大于 920。
补抓结束后检查四份 `status.json` 的 `api_status`，并抽查新增页面。

以下是 2026-10-27 的原始交接记录，表中的仓库数和启动状态仅作为
历史基线，不代表上述最终状态。

GitHub 仓库包含每个分片的 `run.sh`、`collector/*.py`、`input.csv`、
本说明和 `split_manifest.json`。四份 `input.csv` 内容相同，是约
2.8 MB 的任务清单，不含源码。SQLite 检查点、已下载的 Git 镜像、
API 页面、归档和本地 Git helper 仍被 `.gitignore` 排除。仅克隆 GitHub
仓库无法恢复原有进度；仍须从原机器复制对应 `shard_i/` 的检查点及
已下载数据。若没有检查点，收集器会从头处理该分片，包括已完成的仓库。
`run.sh` 会在缺少检查点时直接退出，防止误启动。

原收集器于 2026-10-27 09:38:08 UTC 正常停止（`final=true`），原始目录
`../swh_full_history/` 保留不变。四份检查点对应相同输入 CSV 的
`--shard-count 4 --shard-index 0..3`，按规范 Git 仓库 ID 的 SHA-256
取模分片；非 Git 来源按 origin ID 分片。它们不是 CSV 的连续四段。

| 目录 | Git 总数 | 已校验 Git | 待尝试 Git | 已保存 API 页 | 已保存归档 | 本地目录大小 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `shard_0` | 2,458 | 66 | 2,392 | 107 | 26 | 4.9 GiB |
| `shard_1` | 2,593 | 68 | 2,525 | 86 | 1,992 | 8.6 GiB |
| `shard_2` | 2,551 | 71 | 2,480 | 77 | 180 | 9.0 GiB |
| `shard_3` | 2,467 | 67 | 2,400 | 102 | 172 | 2.3 GiB |

四份 Git 总数 10,069，已校验 272，待尝试 9,797。原来 `failed` 和
`inaccessible` 的 Git 项已重置为可重试；它们的原错误仍在数据库中。
原来失败的 API 页和归档也重置为可重试。HTTP 401/403/404/410 等
API 不可访问状态保留，等待凭据或单独补查，因此 `queues/api.csv`
包含它们，但它们不会被调度器自动重试。每份 `handoff.json` 及总表
`split_manifest.json` 记录详细数量。

## 迁移和启动

四个分片都在同一台机器时，在本目录运行 `bash ./run_all.sh`。它并行启动
四份原有 `run.sh`，各自复用 `manifests/state.sqlite3` 并追加写入对应的
`run.log`。按 Ctrl+C 会向四组工作进程发送终止信号。启动前可用
`python3 retry_failed.py` 查看已失败任务；`python3 retry_failed.py --apply`
会先为每份数据库创建时间戳备份，再把 Git、API 和归档中 `failed` 的
任务改为可重试。已完成任务、`inaccessible` 和 HTTP 终态不受影响。
请在收集器停止后执行重排，勿与单个分片的 `run.sh` 同时启动。

统一入口默认读取项目根目录的 `.env`，也可设置
`SWH_HISTORY_ENV_FILE=/path/to/.env`；四份代码均识别
`GITHUB_API_TOKEN`。若需要代理，可设置 `SWH_HISTORY_PROXY`。
四份并发共享同一个 GitHub token 的 API 配额，不会得到四倍额度。
2026-11-01 已备份四份检查点，并把 643 个 Git、672 个 API 页和
70 个归档的 `failed` 状态重排为可重试；原错误保留在数据库中。

每台机器只复制其中一个 `shard_i/` **整个目录**。包括 `input.csv`、
`manifests/state.sqlite3`、`repos/`、`trackers/`、`packages/`、
`collector/` 和 `toolchain/`；不要只复制 `queues/*.csv`。复制结束后在
目标机器的分片目录运行：

```bash
./run.sh
```

`run.sh` 默认直连。若目标机器本地有 7892 HTTP 代理，可运行：

```bash
SWH_HISTORY_PROXY=http://127.0.0.1:7892 ./run.sh
```

没有自动从代理切换至直连的逻辑。归档 `.part` 只有在远端提供有效
ETag 或 Last-Modified 且 Range 匹配时才按字节续传；Git 对象库保留，
中断的 `fetch` 会重试，但未完成的单个 pack 不保证字节续传。
已校验 Git、已保存的 API 页和完整归档会复用。四台机器分别使用独立
输出目录，不能并发写同一份 SQLite。

目标机需要 Python 3.8+、Git、足够空闲空间，LFS 需要 `git-lfs`。
每份带有本机编译的 Git/OpenSSL HTTP helper；它依赖目标机与当前机
兼容的 Linux 库和 `/usr/lib/git-core` 布局。若不兼容，运行时指定
`--git-exec-path /usr/lib/git-core` 使用目标机自带 helper；先检查目标机
的 Git/TLS 可用性。脚本默认在低于 100 GiB 空闲空间时暂停，可按机器
容量在 `./run.sh --min-free-gib N` 中调整。单独运行分片时，
`shard_0/1` 默认读取项目根目录 `.env`，`shard_2/3` 默认读取各自的
`shard_i/.env`；也可以统一设置 `SWH_HISTORY_ENV_FILE`。无 GitHub token
时，四台机器若共用同一出口 IP，匿名 API 配额不会变成四倍。

进度：

```bash
python3 collector/swh_history_status.py --root . --watch 10 --active
```

同一机器上需要在后台运行时，可在对应分片目录手动启动 tmux 会话；
`run.sh` 已把输出追加到该目录的 `run.log`。每个分片再次运行同一
`run.sh` 即从该目录检查点恢复。原始全量目录不要同时重启，否则会
与四份任务重复下载。全部完成后，以 `origin_id`、`repo_id`、`task_id`
和 `asset_id` 合并四份结果，并保留各自来源分片记录。
