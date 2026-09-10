# BMv2 编译失败分析

本报告由 `classify_bmv2_results.py` 根据 `p4c-bm2-ss` 返回码、原始 stderr、源码路径和归一化角色生成。原始仓库和日志不修改。

## 总览

- 候选：964
- 编译成功：705（原始候选通过率 73.13%）
- 失败：259
- `positive_compile_clean.jsonl`：705 条，定义上为 100% 返回码 0。

## 失败分类

| 分类 | 数量 | 处理方式 |
|---|---:|---|
| 错误回归样例 | 84 | 保留为负例，不混入正集 |
| 编译中间产物 | 20 | 排除，避免重复计算 |
| 缺失 include/生成依赖 | 1 | 补 include；无法重建的单独保留 |
| 非 v1model/目标不匹配 | 14 | 移到 target-specific 集合 |
| BMv2/p4c 能力限制 | 38 | 记录为后端限制，不改源码 |
| 项目片段 | 24 | 需要 wrapper 或完整构建上下文 |
| 语法/类型失败 | 78 | 作为待修复源样例保留 |

## 代表性样例

### 错误回归样例（预期失败）（84）
- `jafingerhut__p4-guide:exit-statement/err-exit-1.p4`
- `jafingerhut__p4-guide:loop-test-progs/err-const-is-not-compile-time-known-value1.p4`
- `jafingerhut__p4-guide:loop-test-progs/err-loop-var-cannot-be-used-in-slice1.p4`

### 编译中间产物（20）
- `jafingerhut__p4-guide:loop-test-progs/tmp/loop-var-can-be-declared-before-loop1-0001-FrontEnd_28_FrontEndDump.p4`
- `jafingerhut__p4-guide:loop-test-progs/tmp/loop-var-can-be-declared-before-loop1-0002-FrontEnd_70_FrontEndLast.p4`
- `jafingerhut__p4-guide:loop-test-progs/tmp/loop-var-exprs-not-constant1-0001-FrontEnd_28_FrontEndDump.p4`

### 缺失 include/生成依赖（1）
- `jafingerhut__p4-guide:tcp-options-parser/look-for-ts-tcp-option/look-for-ts-tcp-option.p4`

### 非 v1model / 目标不匹配（14）
- `jafingerhut__p4-guide:ipdk/dash_experiment1/dash_acl.p4`
- `jafingerhut__p4-guide:ipdk/dash_experiment1/dash_outbound.p4`
- `jafingerhut__p4-guide:ipdk/dash_experiment1/dash_parser.p4`

### BMv2 后端或 p4c 能力限制（38）
- `jafingerhut__p4-guide:action-profile-and-selector/action-profile.p4`
- `jafingerhut__p4-guide:action-profile-and-selector/action-selector.p4`
- `jafingerhut__p4-guide:p4runtime/v1model-p4runtime-most-types3.p4`

### 项目片段/缺少 wrapper（24）
- `jafingerhut__p4-guide:ipdk/dash_experiment1/dash_pipeline.p4`
- `jafingerhut__p4-guide:ipdk/dash_pipeline/dash_pipeline.p4`
- `jafingerhut__p4-guide:p4runtime-translation/sonic-pins-example/mod-2024-nov-29/sai_p4/fixed/ingress_cloning.p4`

### 源代码语法/类型失败（78）
- `jafingerhut__p4-guide:calling-convention/calling-convention.p4`
- `jafingerhut__p4-guide:checksum/checksum-ipv4-with-options.p4`
- `jafingerhut__p4-guide:checksum/debug-utils.p4`

## 结论

不能要求原始仓库中的所有 `.p4` 文件都 100% 编译：其中包含故意报错的 p4c 回归测试、目标专用代码、只在构建系统中使用的 include 片段和编译器中间文件。后续 BMv2/LLM 通过率应以 `positive_compile_clean.jsonl` 为分母；负例和不完整项目继续保留用于数据审计和错误修复任务。
