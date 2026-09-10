# BMv2 编译失败分析

本报告由 `classify_bmv2_results.py` 根据 `p4c-bm2-ss` 返回码、原始 stderr、源码路径和归一化角色生成。原始仓库和日志不修改。

## 总览

- 候选：3968
- 编译成功：2439（原始候选通过率 61.47%）
- 失败：1529
- `hf_p4llms_repaired_positive_compile_clean.jsonl`：2439 条，定义上为 100% 返回码 0.

## 失败分类

| 分类 | 数量 | 处理方式 |
|---|---:|---|
| 错误回归样例 | 37 | 保留为负例，不混入正集 |
| 编译中间产物 | 37 | 排除，避免重复计算 |
| 缺失 include/生成依赖 | 68 | 补 include；无法重建的单独保留 |
| 非 v1model/目标不匹配 | 16 | 移到 target-specific 集合 |
| BMv2/p4c 能力限制 | 681 | 记录为后端限制，不改源码 |
| 项目片段 | 57 | 需要 wrapper 或完整构建上下文 |
| 语法/类型失败 | 633 | 作为待修复源样例保留 |

## 代表性样例

### 错误回归样例（预期失败）（37）
- `hf_p4llms_allp4files__cslev:p4_double_precision/p4c/testdata/p4_16_bmv_errors/reused-counter.p4`
- `hf_p4llms_allp4files__cslev:p4_double_precision/p4c/testdata/p4_16_bmv_errors/no-header-bmv2.p4`
- `hf_p4llms_allp4files__cslev:p4_double_precision/p4c/testdata/p4_16_bmv_errors/nested-headers-bmv2.p4`

### 编译中间产物（37）
- `hf_p4llms_allp4files__jafingerhut:p4-guide/loop-test-progs/tmp/loop-var-in-range-modifiable-in-body1-0002-FrontEnd_70_FrontEndLast.p4`
- `hf_p4llms_allp4files__jafingerhut:p4-guide/loop-test-progs/tmp/loop-var-in-list-elems-modified1-0001-FrontEnd_28_FrontEndDump.p4`
- `hf_p4llms_allp4files__jafingerhut:p4-guide/loop-test-progs/tmp/loop-var-exprs-not-constant1-0003-MidEnd_47_MidEndLast.p4`

### 缺失 include/生成依赖（68）
- `hf_p4llms_allp4files__multi-modal-network:ngsdn-tutorial/p4src/v1model/bmv2_GEO_MF.p4`
- `hf_p4llms_allp4files__multi-modal-network:ngsdn-tutorial/p4src/v1model/fabric_v1model.p4`
- `hf_p4llms_allp4files__multi-modal-network:ngsdn/p4src/v1model/bmv2_IP_ID_GEO_MF_NDN_FlexIP.p4`

### 非 v1model / 目标不匹配（16）
- `hf_p4llms_allp4files__multi-modal-network:ngsdn/p4src/tna/include/headers.p4`
- `hf_p4llms_allp4files__Jenny176:p4/ipdk/dash_pipeline/dash_acl.p4`
- `hf_p4llms_allp4files__Jenny176:p4/ipdk/dash_pipeline/dash_parser.p4`

### BMv2 后端或 p4c 能力限制（681）
- `hf_p4llms_allp4files__zolboo1024:sdn_shaping/padding.p4`
- `hf_p4llms_allp4files__zolboo1024:sdn_shaping/multi_queueing.p4`
- `hf_p4llms_allp4files__p4-programs:MatReduce-examples/c2/p4src/origin.p4`

### 项目片段/缺少 wrapper（57）
- `hf_p4llms_allp4files__multi-modal-network:ngsdn-tutorial/p4src/v1model/include/control/checksums.p4`
- `hf_p4llms_allp4files__multi-modal-network:ngsdn-tutorial/p4src/v1model/include/headers.p4`
- `hf_p4llms_allp4files__multi-modal-network:ngsdn-tutorial/p4src/v1model/include/control/host_meter_table.p4`

### 源代码语法/类型失败（633）
- `hf_p4llms_allp4files__L-Law1iet:P4-SVM/src/main.p4`
- `hf_p4llms_allp4files__zolboo1024:sdn_shaping/v1model.p4`
- `hf_p4llms_allp4files__hc-tec:p4asm_p4code/main.p4`

## 结论

不能要求原始仓库中的所有 `.p4` 文件都 100% 编译：其中包含故意报错的 p4c 回归测试、目标专用代码、只在构建系统中使用的 include 片段和编译器中间文件。后续 BMv2/LLM 通过率应以 `positive_compile_clean.jsonl` 为分母；负例和不完整项目继续保留用于数据审计和错误修复任务。
