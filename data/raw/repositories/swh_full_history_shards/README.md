# 四机下载交接

GitHub 仓库包含每个分片的 `run.sh`、`collector/*.py`、本说明和
`split_manifest.json`。`input.csv`、SQLite 检查点、已下载的 Git 镜像、
API 页面、归档和本地 Git helper 被 `.gitignore` 排除。仅克隆 GitHub
仓库无法恢复原有进度；仍须从原机器复制对应 `shard_i/` 的完整数据，
或在目标机器用同一输入 CSV 初始化新的分片任务。

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
容量在 `./run.sh --min-free-gib N` 中调整。认证信息放在每台机器的
`shard_i/.env`，无需复制原项目 `.env`；无 GitHub token 时，四台机器
若共用同一出口 IP，匿名 API 配额不会变成四倍。

进度：

```bash
python3 collector/swh_history_status.py --root . --watch 10 --active
```

同一机器上需要在后台运行时，可在对应分片目录手动启动 tmux 会话；
`run.sh` 已把输出追加到该目录的 `run.log`。每个分片再次运行同一
`run.sh` 即从该目录检查点恢复。原始全量目录不要同时重启，否则会
与四份任务重复下载。全部完成后，以 `origin_id`、`repo_id`、`task_id`
和 `asset_id` 合并四份结果，并保留各自来源分片记录。
