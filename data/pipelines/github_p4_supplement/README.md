# GitHub P4 补集发现

此流程独立于 SWH Graph 和 BigQuery/GHArchive Plan B。它发现当前 GitHub
可搜索的公开仓库，并按 GitHub 仓库 ID 去重；已知 SWH 来源按规范 URL
名称对账。输出是**候选仓库**，不是已确认 P4_16 的完整仓库集合。

## 搜索范围

- 仓库搜索：`language:P4`、`topic:p4`、`topic:p4lang`、`p4 in:name`、
  `"P4 programming" in:readme`。超过 1,000 条时按 `created` 日期拆分。
- 文件搜索：`extension:p4`，按文件 `size` 字节范围拆分。命中说明 GitHub
  搜索索引里有 `.p4` 路径；它不证明文件是 P4_16。单个日期或字节大小仍
  超过 1,000 条时记录 `unresolved`，不误报完整覆盖。
- GitHub Search 每个查询最多能取前 1,000 条。分区在分页中超过该限制、
  或分页结果变化导致预期页面为空时，也继续拆分重查；已保存的页面与命中
  保留。旧版因此产生的 `failed` 分区会在下一次 `run` 启动时自动重新入队。
- GitHub 搜索只覆盖可索引的公开内容；私有、删除、未索引、非默认分支、
  索引滞后和仅历史版本含 `.p4` 的仓库均可能漏掉。`SWH` 列名匹配也无法
  自动识别仓库改名，后续需按 GitHub ID/重定向核对。
- `p4 in:name` 有噪声；仅仓库搜索命中的记录必须检查仓库树中的 `.p4`
  才能进入实际代码下载。即使文件搜索命中，P4_16 仍需单文件复核。

## 运行与恢复

项目根 `.env` 需设置 `GITHUB_API_TOKEN`、`GITHUB_TOKEN` 或 `GH_TOKEN`。
不打印 token。输出默认存于被 Git 忽略的
`data/raw/github_p4_supplement/`。初次初始化、限量试跑和恢复：

```bash
python3 data/pipelines/github_p4_supplement/search.py init
python3 data/pipelines/github_p4_supplement/search.py run --kind repository --max-requests 25
python3 data/pipelines/github_p4_supplement/search.py run --kind all
python3 data/pipelines/github_p4_supplement/search.py status
python3 data/pipelines/github_p4_supplement/search.py export
```

`--max-requests 0` 表示跑到当前队列完成；`--proxy URL` 可选。GitHub
Search 对仓库与代码搜索使用不同配额，程序按返回的速率头串行节流，
遇限流退避。SQLite `search.sqlite3` 记录分区、页码、错误和仓库证据；
`pages/<seed>/<job_id>/<page>.json.gz` 留存原始响应及请求时间。写好页面
后才记 SQLite 进度，因此中断后可重放已保存页而不重复请求。
`candidate_repositories.csv` 为导出视图，包含搜索来源、代码搜索证据、
与 SWH 清单的名称匹配标志；不直接追加到四个 shard 的 `input.csv`。

2026-11-05 已完成各 25 次仓库搜索和 10 次文件搜索的小样本，随后在
tmux `github-p4-supplement` 启动 `run --kind all` 从同一检查点续跑。
运行日志为 `data/raw/github_p4_supplement/run.log`。查看进度：

```bash
python3 data/pipelines/github_p4_supplement/search.py status
tail -f data/raw/github_p4_supplement/run.log
tmux attach -t github-p4-supplement
```

前台 tmux 中按 Ctrl+C 停止后可再次执行 `run --kind all` 恢复；同一
输出目录的第二个 `run` 实例会被独占锁拒绝。搜索结束后运行
`export` 生成 CSV，再检查 `status` 中是否存在 `failed`/`unresolved`。

## 单文件筛查

2026-11-06 搜索队列有 232 个 `complete`、229 个已拆分 `split` 和
3 个 `unresolved` 分区。导出的 `candidate_repositories.csv` 含 45,955 个
GitHub ID；其中 2,101 个仓库有 `.p4` 文件搜索证据。`unresolved`
是搜索覆盖缺口，后续筛查结果不能消除它。

`screen.py` 从搜索 SQLite 初始化仓库、路径和 blob OID，再读取搜索命中
blob 与默认分支 Git tree。GitHub 两个独立账号的
`GITHUB_API_TOKEN`、`GITHUB2_API_TOKEN` 已分别验证可用，程序按
token 的 REST 速率头节流。默认树请求若截断会逐层读取子树；单仓库超过
128 次子树请求会记为 `tree_limit` 失败，待专项补查，不当作无 P4。
内容按 Git blob OID 缓存和校验，并复用 SWH 单文件版本、复核及质量函数。
`p4_16_likely` 只是静态证据，不是编译确认。

```bash
python3 data/pipelines/github_p4_supplement/screen.py init
SWH_HISTORY_PROXY=http://127.0.0.1:7892 \
  python3 -u data/pipelines/github_p4_supplement/screen.py run \
  --phase all --workers 20 --token-interval 0.75 \
  >> data/raw/github_p4_supplement/screen/run.log 2>&1
python3 data/pipelines/github_p4_supplement/screen.py status
python3 data/pipelines/github_p4_supplement/screen.py export
```

输出在 `data/raw/github_p4_supplement/screen/`：
`screen.sqlite3` 是仓库、文件关系和 blob 检查点；
`raw_blobs/` 保留已验证的原始文件；`status.json` 和 `run.log`
记录每阶段进度、吞吐、ETA 与每个 token 的请求数。中断后重跑相同
`run` 命令即可续跑，单实例文件锁防止同时改写。瞬时错误保留
`pending`，下一轮重试；终态 `failed` 要先检查原因，再用
`screen.py retry-failed` 显式重新入队。`export` 生成
`file_evidence.csv` 和 `repository_screen.csv`，含全部仓库关系及
无法判定状态。默认分支无 `.p4` 只表示当前 HEAD 未发现，不能排除历史。
