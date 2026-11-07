# P4 文档知识语料 TODO

状态：OLMo/Dolma 3 定向召回规划稿，2026-11-06；种子试采已开始，
尚未下载 Dolma 3、训练分类器或运行召回。此计划从代码收集完成后的**文档知识**出发，
不从代码文件数推算文档数量。

考虑到硬盘空间紧缺，本任务放到ssh xiegr@172.22.11.186上目录storage/xgr/zhuyijia/bmv2_code上完成，注意尽量保证git同步，我已经手动同步.env过去，你拥有完全控制这个目录以及访问这个服务器的权限


## 目标与边界

建立可追溯、可重建的 P4 文本语料，服务于 P4_16 语言知识、
v1model/BMv2 和 PSA 等架构知识、编译/调试说明、使用场景及问答。
原始文档与可训练文本分开保存。P4_14、P4_16、架构版本、编译器版本
和厂商扩展分别标注；不能把不同版本的同名语法混成一个“当前事实”。
教程中的代码块与说明文字分别标注，不能把代码片段、
完整源码和基准题混作同一种样本。

首轮从下文列出的 P4 站点、课程、规范和问答中定向收集种子文档，
用于正例标注、分类器训练和覆盖率核对；批量候选召回只读取
OLMo 3 发布的 Dolma 3 数据。不另行下载 Common Crawl/FineWeb、
不启动全网爬取或其他网页池。Dolma 3 的网页部分本身源于
Common Crawl；这里的“只用 OLMo”限定的是批量候选池，
不排斥为种子采集公开的 P4 专题页面。

## 已核实的论文方法与本项目选择

| 参考 | 原文实际做法 | 对 P4 召回的决定 |
| --- | --- | --- |
| OpenCoder §2.1.2 | 从 Common Crawl 标注约 50 万条代码类种子，训练 fastText 分类器；按分类命中分析域名，人工审核 URL 路径，补入漏判种子，迭代三轮。中文处理前用了 BPE 分词。 | 采用“高精度种子 + CPU fastText 二分类 + 域名/路径误差分析 + 迭代”的流程；训练目标改成 P4 文档相关性，不沿用其泛代码分类器。 |
| FineWeb §4 | Llama 3 给 46 万网页打 0–5 教育分；冻结 Snowflake Arctic Embed M，训练线性回归预测分数，再以 3 分为阈值。全文推理报告约 6000 H100 小时。 | 这是教育质量评分，不是 P4 领域召回；不在全池用嵌入模型推理，也不照搬 3 分阈值。 |
| OLMo 3 §3.4.1 | Dolma 3 网页池先清洗去重，再用蒸馏为 fastText 的 WebOrganizer 分 24 个主题；另用 fastText 质量分类器打分，在每个主题内分 20 个 5% 质量档。 | 主题与质量档可以排序扫描，但都不是 P4 相关性标签；仍须单独训练 P4 分类器并验证漏召。 |

**模型决策：**首轮用 CPU 原生 fastText 监督式二分类
`P4_document` / `other`。`predict` 的正类分数用于排序和阈值，
不是经校准的真实概率。阈值在独立验证集上按召回率、人工复核量
和每 GB 候选数选定；不预先指定 0.5。OLMo 原有质量分只作为
排序/分析特征，不替代领域标签。额外的“教学价值”、P4 版本、
架构、内容类型在召回后标注，避免一个模型同时承担不同目标。

## 唯一批量数据源与下载边界

| 数据集 | 用途和已核实规模 | 决定 |
| --- | --- | --- |
| [Dolma 3 Pool](https://huggingface.co/datasets/allenai/dolma3_pool)，修订 `6462556697df1a8f5c953727e9c686629ad98b68` | 9.31T token 来源池，其中 Common Crawl 网页 8.14T token、约 96.7 亿篇；仓库约 18 TB 压缩数据。目录为 `data/common_crawl-<topic>-<vigintile>/shard_*.jsonl.zst`；另有 olmOCR PDF。 | **主召回池**。先网页；PDF 只在评估发现论文/规范覆盖缺口后按同一 OLMo 池扩展。 |
| [Dolma 3 150B 样本](https://huggingface.co/datasets/allenai/dolma3_mix-150B-1025)，修订 `afa92bfb22366821c5e6cd427cdd036b34b713ef` | 约 135.8 GB 压缩，含 8450 万网页。 | 可取少量 shard 验证读取器；抽样未命中不代表全池未收录。无需下载完整 150B。 |
| [Dolma 3 6T Mix](https://huggingface.co/datasets/allenai/dolma3_mix-6T)，修订 `689a3ea2d8217e64d73a5058913fa43ad15e81aa` | 约 4.41 TB 压缩，是 Olmo 3 32B 的训练混合，网页仅是其中一部分。 | 只用于后续核对“召回文档是否进入实际训练混合”，不代替较大的来源池。7B 专用 mix 的数据卡明确要求其他用途改用主 mix。 |

以上是 2026-11-06 查询数据卡/API 的快照，开始执行前重新固定修订、
分页枚举全部文件并核对每个文件大小/哈希。数据卡的 ODC-BY 不自动
解决网页原作者的训练与再分发许可；命中后仍单独审核原始 URL 和
归属。Pool 数据卡未列出完整行级字段，首个 shard 必须检查 `text`、
URL、时间、source 与 metadata 的实际结构；URL 缺失时不能声称
完成 URL 路径分析或源站许可审核。

远端 `/storage` 当前约 14 TB 可用，小于整个 Pool 仓库约 18 TB。
下载只以 shard 为单位，校验后流式解压、处理和生成可续跑 checkpoint；
不将全池镜像或解压到磁盘。原始 shard 可在保存版本、路径、上游哈希、
本地校验与命中记录后释放，保留可重取的精确指针。任何大批量下载
均在试点测出吞吐、费用/时间和峰值空间之后排程。

## P4 种子 URL 与标注

以下是待核对的发现入口，不把整个站点、搜索结果或相邻主题自动
标为正例。每个入口展开为具体页面、文档、章节或问答，保留标题、
规范/架构版本、原始 URL、发布时间、获取时间、许可、原件哈希。
种子原件与从 OLMo 召回的候选分开存放和计数。

1. **P4.org 站点图。** 从 https://p4.org/ 入队，覆盖其 Developer
   Day、Working Group、Specifications、Blogs、Forum、Publications
   等栏目及逐层子页面。规范查找 P4 语言、PSA、TNA、P4Runtime、
   v1model/BMv2 等公开资料；TNA 等厂商资料可能在站外，需记录
   指向的实际发布者与可访问性。https://forum.p4.org/ 单独作为
   问答入口，保留问题、回答、作者和帖子时间。
2. **苏黎世网络课程。** 从 https://adv-net.ethz.ch/2018 到
   https://adv-net.ethz.ch/2025 逐年核对课程页面及 Material
   栏目，收集公开讲义、幻灯片/PPT、PDF 与课程链接；同时登记
   ETH Zurich 相关网络课程中实际讲授 P4 的页面。年份页是否
   存在、材料版本和许可逐项核对，不能把课程目录当成正文样本。
3. **P4Lang 项目站点与 Wiki。** 从 https://github.com/p4lang/
   枚举项目，只跟进各项目公开的 GitHub Wiki、GitHub Pages、
   独立文档站和发布说明链接；本轮不从仓库文件树抽取 README、
   源码或 `.p4` 文件。不存在的 Wiki 记录为无入口，不生成空样本。
4. **P4.org LinkedIn。** 登记官方 LinkedIn 主页的 Articles 和
   Documents 两个模块及各篇公开文章/文档 URL。页面若要求登录
   或限制访问，只记指针与可见元数据，不绕过限制。
5. **搜索发现。** 用 Google 搜索 P4、BMv2、Tofino、
   Vitis Networking P4、P4 eBPF 的博客、文字教程和专题网站；
   对 Tofino 与 Vitis Networking P4 另查 `user guide`、
   `IP manual`、`product guide`、`pdf` 等组合词。搜索结果只
   产生待复核 URL，优先官方/原作者文档；不能把搜索结果摘要
   直接当训练文本，厂商手册须核对版本和分发许可。
6. **协议 RFC。** 以 https://www.rfc-editor.org/rfc/ 为官方入口，
   对 RFC 791–2000 范围先按标题/主题筛选与包头、协议解析、
   转发或 SDN 相关的条目，而非整段范围无差别抓取；明确列入
   RFC 768、791、7149、7348、7426、8200、8321、8926、
   9293、9817 等。每篇以 `rfcNNNN.html` 或官方 PDF 为样本，
   记录编号、状态和修订/勘误，供 P4 header 与协议知识标注。
7. **EuroP4。** 登记 EuroP4 '26 的活动页、议程、论文/演讲
   和公开幻灯片入口；若 2026 材料尚未发布，只保留待检查 URL
   和状态，不用历史年份材料冒充 2026 资料。
8. **Stack Overflow P4 问答。** 以 P4、BMv2、Tofino、
   Vitis P4、P4 eBPF 等词检索问题与回答，记录原帖 URL、
   问答时间、标签、作者和采纳/评分状态。可参考
   [StackExchange Archive 2024](https://archive.org/details/stackexchange)
   的 Stack Overflow dump 定向查询；只抽出经复核的 P4 问答
   作为种子，不把整个 dump 加入 OLMo 批量候选池。

P4.org 的站点发现采用持久化 FIFO 队列：取队首 URL，抓取并保存
一份页面/文档原件及元数据作为样本，提取页面中每一个子链接，
规范化 URL 后入队；维护 `queued`/`visited` 状态，避免重复、重定向
循环、日历/搜索无限翻页和带跟踪参数的 URL 膨胀。站内链接按
P4.org 范围继续抓取；站外链接同样记录在发现队列，但仅在属于
上表已批准的来源范围、公开可访问且许可/访问规则允许时自动抓取，
其余标为 `pending_review`。HTML 子页、PDF、PPT、问答分别记录
内容类型和提取状态；按域名限速、可中断续跑。页面中的每一个
链接都有发现记录，但不是每一个链接都自动成为 P4 正例。

首批目标为约 200–500 条正例文档/章节，覆盖不同站点、版本和任务；
从 OLMo 小样本随机抽取及主动挖掘约 1000–3000 条负例，包含
GPU P4、医学/课程编号 P4、非 P4 的普通网络文章、泛编程网页、
纯代码或导航页等难负例。“P4”单词本身不是正例证据。
标注准则为：正文确实解释 P4 语言、架构、编译/运行工具或实践，
且不是只有无说明的源码。将含糊样本留在 `uncertain`，不强制
塞入二分类训练集。训练/验证按站点和文档系列分组，防止同一规范
转载或相邻章节泄漏；另保留独立的 OLMo 随机样本估计真实精度。

## 执行阶段

1. [ ] **数据清单与读取器试点。** 在远端固定 Pool revision，分页
    枚举 web/PDF shard、topic、质量档、大小与哈希；记录磁盘余量、
    网络和 CPU 限额。先取 1–2 个约百 MB 的 web shard 验证
    JSONL/zstd、URL/metadata 和断点恢复；不提前下载 150B 或全池。
2. [~] **种子发现、定向采集与标注。** 核对上述八类入口；先实现
    P4.org 持久化队列、URL 规范化、visited 去重、原件保存和
    断点续跑，再采集课程材料、项目 Wiki、RFC 和经审核的问答。
    建立 URL/章节级正例和硬负例，固定标注规则及按站点分组的
    train/dev/test。基准题、隐藏参考代码和 holdout 仓库先登记，
    不能进入分类器种子。
3. [ ] **CPU 基线与训练。** 先比较 URL/术语规则与 fastText 二分类。
    fastText 使用词/字符 n-gram，保留 `P4_16`、`p4c`、`BMv2`、
    `v1model` 等关键写法；只在需要中文种子时试 OpenCoder 的 BPE。
    记录线程数、模型大小、训练时间、每秒文档数/压缩 GB 数。
    在不同站点、主题和质量档的验证集上画精确率-召回率曲线；
    选满足 P4 正例召回目标（暂定至少 95%）且人工审核量可承受的
    阈值。再抽查规则和低分区漏判；单纯的 F1 最高点不作为阈值。
4. [ ] **小规模 OLMo 召回。** 从 Pool 的软件开发、软件、
    科技、电子硬件及随机其他主题，跨高/中/低质量档取分层 shard
    （初始约 24–48 个、压缩量上限约 5 GB）。先测全量 fastText
    与“高召回术语/URL 粗筛后 fastText”两路；只有粗筛在独立抽样
    中未产生不可接受的漏召，才把它用于全池加速。人工复核高分、
    阈值附近、规则命中但模型漏判以及双路均拒绝的样本。
5. [ ] **决定扫描范围并分批召回。** 根据试点的实际 MB/s、
    每十亿文档候选数、分层漏判和预计总耗时，先扫描高产 topic/
    质量档，再扩大到其余档与主题。若只扫描部分 shard，manifest
    必须写 `partial` 和未扫描清单，不能宣称“全 OLMo 召回”。
    shard 下载/处理并发按磁盘、网络和 CPU 实测设限；每个 shard
    独立校验、原子 checkpoint、可重试，处理后只保留命中文档
    原始行及必要审计信息。运行阶段不使用 GPU。
6. [ ] **复核、去重、去污染。** 人工审核候选是否真属 P4，并标记
    P4_14/P4_16、架构、来源类型和质量；从命中站点/路径中发现
    漏判 URL，复核后回灌种子并重训，记录迭代版本。正文精确哈希
    和近似聚类只用于候选去重，保留不同规范版本。冻结 BMv2
    benchmark 后做题面、参考代码、家族和 holdout 的去污染。
7. [ ] **生成训练视图和覆盖报告。** 仅将许可、内容质量与去污染
    均通过的记录进入 train；保留 rejected、uncertain、只存指针
    三类。报告分主题/质量档/站点/版本的命中率、人工精度、
    漏召估计、去重前后文档与 token 数、分类耗时及未覆盖知识点。
    是否扩展到 Pool 的 olmOCR PDF 在此后依据缺口决定，不引入
    其他批量网页来源。

## 数据契约与目录

计划在远端 `/storage/xgr/zhuyijia/bmv2_code` 执行，源代码用 Git
同步；`.env` 已由用户同步，不写入 manifest、日志或 Git。
文档数据独立于 P4 源码树：

```text
data/raw/p4_text/seed_registry.jsonl
data/raw/p4_text/seed_crawl/queue.jsonl
data/raw/p4_text/seed_crawl/discovered_links.jsonl
data/raw/p4_text/seed_crawl/<source_id>/... # 页面/PDF/PPT/问答原件
data/raw/p4_text/dolma3/shard_manifest.jsonl
data/raw/p4_text/dolma3/hit_rows/...      # 命中的原始 JSONL 行；非全池镜像
data/normalized/p4_text/labels.jsonl
data/normalized/p4_text/review_queue.jsonl
data/normalized/p4_text/recall_runs/<run_id>/shard_status.jsonl
data/normalized/p4_text/recall_runs/<run_id>/candidates.jsonl
data/normalized/p4_text/documents.jsonl
data/normalized/p4_text/occurrences.jsonl
data/normalized/p4_text/views/<version>/train.jsonl
data/normalized/p4_text/views/<version>/excluded.jsonl
data/normalized/p4_text/views/<version>/manifest.json
```

种子队列记录发现来源、原始/规范 URL、父 URL、域名、深度、
入队时间、抓取状态、HTTP/内容类型、许可/访问状态和失败原因；
保存的种子样本记录原件 SHA-256、提取器版本及页面子链接清单。
shard 清单至少包含 dataset ID/revision、相对路径、字节数、上游哈希
和本地 SHA-256、下载与处理状态、记录数、命中数、分类器版本、
阈值、耗时、错误与重试次数。候选至少包含原始 `id`、shard 路径、
行号、text 哈希、URL/metadata 原文、topic/质量档、规则命中原因、
fastText 原始分数、阈值版本、人工判定及剔除原因。URL 缺失明确
记 `null`，不可从正文猜造。`doc_id` 代表内容，`occurrence` 代表
每次来源出现；跨站转载去重后仍保存来源和版本证据。

每个视图 manifest 固定数据集修订、实际扫描 shard 列表和未扫描列表、
处理代码 commit、分类器/规则/阈值、采样种子、过滤和去重配置、
许可策略、评测集版本、样本 ID 与阶段计数。任何命中可由 pinned
shard 重取核对；不覆盖前一版，不修改保存的原始命中行。

## 首轮验收

- 下载前能列出固定修订、shard 总量/大小和抽样方案；试点能报告
  CPU 文档/s、压缩 MB/s、峰值内存/磁盘及投影总耗时。
- 八类种子入口及 P4.org 链接发现有可恢复队列、抓取/跳过原因
  和原件哈希；每个正例能追溯到具体页面或章节。
- 按站点留出的 P4 正例召回至少达到试点目标（暂定 95%），
  同时公开随机 OLMo 样本的人工精度、阈值附近和拒绝区漏判；
  不以均衡验证集的准确率代替真实池的候选精度。
- 每个扫描 shard 可恢复、可追溯；所有训练样本能反查 OLMo
  原始行、shard/revision、分类器与许可审核状态。
- 版本保留、跨来源去重和 benchmark 去污染均有可复核报告；
  公开原始数据、可公开训练、仅内部使用和仅保留指针分开统计。
- 全池未扫描完就明确报告覆盖比例与缺失范围，不以候选页数
  代替 P4 知识覆盖或训练有效性。

## 2026-11-06 远端种子试采记录

- 执行位置：`xiegr@172.22.11.186:/storage/xgr/zhuyijia/bmv2_code`；
  使用 `data/pipelines/p4_text_seeds/collector.py` 的 SQLite FIFO 队列，
  原件和提取文本在远端 `data/raw/p4_text/seed_crawl/`，不纳入 Git。
  运行时逐域限速 1 秒、页面深度至多 2、单响应上限 8/15 MB。
- P4.org 首页、规范目录、ETH 2018–2025 年入口、P4 Forum 首页与
  10 篇指定 RFC 均已保存。另抓 P4_16 v1.2.5、PSA v1.2、
  P4Runtime v1.4.1 的 HTML 规范和 4 份 ETH P4 讲义 PDF。
  Developer Day、Working Groups、Publications、Blog、EuroP4 '26
  的入口页和两条 Forum 主题 HTML/公开 JSON 也已保存。
  共 44 条已抓、190 条待抓、4 条错误；4 条错误均为 34–45 MB
  的 ETH 讲义超过 15 MB 限制，已保留 URL 和失败原因。
- 规范目录发现 P4_16、PSA、PNA、P4Runtime 多版本链接。
  P4.org 站外链接、ETH 跨年份目录和其他外站材料只保留发现记录，
  等待来源/许可复核。GitHub Wiki、LinkedIn、Google 搜索、
  EuroP4 专题页和 Stack Overflow 尚未专项采集；不能将入口数
  当正例数。所有待审样本的标签仍是 `pending`。
- 离线整理入口为 `prepare.py`：校验原件和文本哈希，按正文哈希
  合并文档，保留每次 URL 出现，并生成独立 `review_queue.jsonl`。
  当前为 44 条 URL 出现、42 份不同可读正文、40 条 `pending`
  待审样本：两条旧 working-draft HTML 是空正文，两条 Forum
  HTML 因有结构化 JSON 版本而仅保留在来源表。Forum JSON 保留
  逐帖作者、时间和抓取/预期帖子数；长主题如抓取数不足需补取回复。
  人工标签应写入单独 `labels.jsonl`，避免重跑覆盖审核结果。
  当前数据只完成采集和整理，未产生训练正例。

## 参考资料

- [OpenCoder](ref_paper/opencoder.pdf) §2.1.2：fastText、域名/URL 路径迭代召回。
- [FineWeb](ref_paper/fineweb.pdf) §4：教育评分器及推理成本。
- [Olmo 3](https://arxiv.org/html/2512.13961) §3.4.1：Dolma 3 网页池、fastText 主题/质量分档。
- [Dolma 3 Pool 数据卡](https://huggingface.co/datasets/allenai/dolma3_pool)、[150B 样本数据卡](https://huggingface.co/datasets/allenai/dolma3_mix-150B-1025)、[6T Mix 数据卡](https://huggingface.co/datasets/allenai/dolma3_mix-6T)：修订、规模、格式与使用范围。
