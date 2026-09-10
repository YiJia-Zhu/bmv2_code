# BMv2 编译失败分析

本报告由 `classify_bmv2_results.py` 根据 `p4c-bm2-ss` 返回码、原始 stderr、源码路径和归一化角色生成。原始仓库和日志不修改。

## 总览

- 候选：396
- 编译成功：381（原始候选通过率 96.21%）
- 失败：15
- `hf_positive_compile_clean.jsonl`：381 条，定义上为 100% 返回码 0.

## 失败分类

| 分类 | 数量 | 处理方式 |
|---|---:|---|
| 错误回归样例 | 0 | 保留为负例，不混入正集 |
| 编译中间产物 | 0 | 排除，避免重复计算 |
| 缺失 include/生成依赖 | 0 | 补 include；无法重建的单独保留 |
| 非 v1model/目标不匹配 | 0 | 移到 target-specific 集合 |
| BMv2/p4c 能力限制 | 8 | 记录为后端限制，不改源码 |
| 项目片段 | 3 | 需要 wrapper 或完整构建上下文 |
| 语法/类型失败 | 4 | 作为待修复源样例保留 |

## 代表性样例

### BMv2 后端或 p4c 能力限制（8）
- `hf_Akira1906__CuckooGuard-SYN-Flood-Defense-P4:raw-repositories/Akira1906__CuckooGuard-SYN-Flood-Defense-P4/demo-split-proxy/implementation/p4src/split-proxy-old.p4`
- `hf_Akira1906__CuckooGuard-SYN-Flood-Defense-P4:raw-repositories/Akira1906__CuckooGuard-SYN-Flood-Defense-P4/demo-split-proxy/implementation/p4src/split-proxy-part1.p4`
- `hf_Akira1906__CuckooGuard-SYN-Flood-Defense-P4:raw-repositories/Akira1906__CuckooGuard-SYN-Flood-Defense-P4/demo-split-proxy/implementation/p4src/split-proxy-part2.p4`

### 项目片段/缺少 wrapper（3）
- `hf_jafingerhut__p4-guide:raw-repositories/jafingerhut__p4-guide/p4runtime-translation/sonic-pins-example/mod-2024-nov-29/sai_p4/instantiations/google/fabric_border_router.p4`
- `hf_jafingerhut__p4-guide:raw-repositories/jafingerhut__p4-guide/p4runtime-translation/sonic-pins-example/mod-2024-nov-29/sai_p4/instantiations/google/tor.p4`
- `hf_jafingerhut__p4-guide:raw-repositories/jafingerhut__p4-guide/p4runtime-translation/sonic-pins-example/mod-2024-nov-29/sai_p4/instantiations/google/wbb.p4`

### 源代码语法/类型失败（4）
- `hf_ebiken__p4srv6:raw-repositories/ebiken__p4srv6/archive/p4src/switch.p4`
- `hf_cornell-netlab__capisce:raw-repositories/cornell-netlab__capisce/capisce/examples/bf4_failing/ndp_router_16.p4`
- `hf_FredShi97__p4:raw-repositories/FredShi97__p4/exercises/p4_upf/main.p4`

## 结论

不能要求原始仓库中的所有 `.p4` 文件都 100% 编译：其中包含故意报错的 p4c 回归测试、目标专用代码、只在构建系统中使用的 include 片段和编译器中间文件。后续 BMv2/LLM 通过率应以 `positive_compile_clean.jsonl` 为分母；负例和不完整项目继续保留用于数据审计和错误修复任务。
