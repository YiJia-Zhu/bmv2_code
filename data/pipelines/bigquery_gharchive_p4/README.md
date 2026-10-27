# 方案 B：BigQuery + GHArchive + GitHub

本目录是独立的 P4 仓库级采集流水线。后续自动化应先读取 `pipeline.json` 获取当前
状态、阶段、命令、输入输出和安全门槛，不需要从 A/B 混合方案文档推断执行状态。

## 目标

从 BigQuery GitHub 文件索引和 GHArchive 候选中发现仓库，检查当前默认分支最新
commit。只有完整 Git tree 中至少一个 tracked `.p4` 文件被严格判定为 P4_16，才
下载并保留整个项目。P4 文件占仓库 1% 也不影响准入。

本方案不等于 SWH Graph 全覆盖。只存在于 SWH、当前 GitHub 已删除且没有候选记录的
仓库可能遗漏；这部分由独立的 A 方案在 AWS 环境准备后补齐。

## 目录边界

```text
data/pipelines/bigquery_gharchive_p4/
├── collector.py
├── pipeline.json
├── queries/github_p4_repositories.sql
└── tests/test_collector.py

data/raw/gharchive_p4/
├── discovery/
├── events/
├── manifests/
├── snapshots/
└── derived/p4_context/
```

程序和运行数据均不写入 `data/raw/swh_p4/`。旧入口
`data/tools/collect_gharchive_p4.py` 只是兼容启动器，新的规范入口是本目录下的
`collector.py`。

## 数据流

```text
BigQuery github_repos.files -- 精确筛选 .p4 path
                         \
                          -> 合并标准化仓库名 -> GitHub 最新默认分支 tree
                         /                         -> 只获取 .p4 做 P4_16 gate
GHArchive/已知组织候选                              -> 通过后下载完整项目
                                                    -> repository-context
```

BigQuery 已包含 `repo_name` 和 `path`，因此全局 `.p4` 查询是 B 方案的主要文件名
发现入口。GHArchive 只有事件和仓库名，不证明仓库包含 `.p4`。对不超过 10,000 个
GHA 候选可以使用 `--repository-candidates` 做一次 BigQuery 交叉检查；更大的列表应
执行一次全局查询后在本地合并，避免分批重复扫描公开表。

## 安全门槛

- BigQuery 必须先带 `--dry-run-only` 和 `--max-bytes-billed`；
- 删除 `--dry-run-only` 前必须人工确认估算扫描量和费用上限；
- GHArchive 下载和 GitHub 快照收集属于网络批处理，执行前必须确认时间范围或 `--limit`；
- 首轮快照收集固定使用 `--limit 20`；
- 不打印、不提交 `data/token.json`、client secret 或 GitHub token；
- `p4_14`、`unknown`、`p4_16_conflicted` 和不可读 `.p4` 全部拒绝，不下载整仓库。

## 命令

离线验证：

```bash
python3 -m py_compile \
  data/pipelines/bigquery_gharchive_p4/collector.py \
  data/pipelines/bigquery_gharchive_p4/tests/test_collector.py

python3 -m unittest \
  data/pipelines/bigquery_gharchive_p4/tests/test_collector.py -q
```

BigQuery 费用预检：

```bash
python3 data/pipelines/bigquery_gharchive_p4/collector.py discover-bigquery \
  --billing-project ornate-grail-473307-m8 \
  --oauth-token-file data/token.json \
  --execute --dry-run-only \
  --max-bytes-billed 1073741824
```

执行查询前检查：

```text
data/raw/gharchive_p4/discovery/bigquery_dry_run.json
```

GHArchive 使用明确的小时间窗口起步：

```bash
python3 data/pipelines/bigquery_gharchive_p4/collector.py download-gharchive \
  --start-hour YYYY-MM-DD-H --end-hour YYYY-MM-DD-H

python3 data/pipelines/bigquery_gharchive_p4/collector.py index-gharchive \
  --event-root data/raw/gharchive_p4/events/gharchive

python3 data/pipelines/bigquery_gharchive_p4/collector.py discover-gharchive
```

合并候选并做 20 个项目的小样本：

```bash
python3 data/pipelines/bigquery_gharchive_p4/collector.py merge-discovery \
  --input data/raw/gharchive_p4/discovery/bigquery_p4_repositories.jsonl \
  --input data/raw/gharchive_p4/discovery/gharchive_repositories.jsonl

python3 data/pipelines/bigquery_gharchive_p4/collector.py collect-snapshots \
  --repositories data/raw/gharchive_p4/manifests/repositories.jsonl \
  --limit 20

python3 data/pipelines/bigquery_gharchive_p4/collector.py build-context
```

## 输出判定

`manifests/snapshots.jsonl` 中只有 `contains_p4_16=true` 且状态为 `downloaded` 的
项目能够进入派生 repository-context。未通过的候选写成 `status=rejected`，完整 tree
仍用于审计，但 `content_mode=p4_gate_only`，不会产生仓库 tarball。

完整仓库优先保存 commit tarball；过大的仓库切换到分层代码 payload，但始终保留完整
tree manifest。派生视图不会修改 raw 快照。
