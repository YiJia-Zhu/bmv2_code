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

## 下一个阶段

先检查 `jobs` 是否全部 `complete`，把 `failed`、`unresolved` 和
`incomplete_results` 作为明确覆盖缺口；然后对新增候选核实真实 `.p4`
文件，逐文件判断 P4_16，并将确认的新仓库交给独立的完整历史下载清单。
不将 `language:P4` 或关键词命中直接作为 P4_16 证明。
