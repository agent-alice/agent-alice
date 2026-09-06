# 长摘要自动分区：范围与验收

关联 [Issue #2](https://github.com/agent-alice/agent-alice/issues/2)，补齐记忆迁移方案 §2.4 已有的长输入分区承诺。真实资料最终切换仍由 #19、#20 跟踪，合成测试不能关闭该验收。

## 任务基线与所有权

- 基线：`ade2d81bde636c04f8c6b6a9428711c6664b6b81`。
- 独立分支：`codex/issue-2-summary-partitions`；复用当前独立 worktree。
- 已核对既有摘要冻结、来源读取、候选覆盖校验、原子提交恢复，以及日历派发和 Service 完成回执；保留已验证的小批次实现。
- 本线拥有 `memory.py` 摘要与恢复区域、新摘要辅助模块、摘要模板及对应合成测试；不修改身份初始化区域。Service 身份与资源分代区域仍由各自负责人修改。
- 日历、MCP、Service 转发及发布兼容门槛属于共享接口，先将协议和影响范围交集成负责人协调，再按确认范围实现。不得修改其他协作者的 worktree。

## 已确认缺口

旧实现遇到单条来源超过 1 MiB、窗口超过 16 MiB 或 10,000 条来源时明确拒绝。这样避免了静默截断，但没有自动分区与完整覆盖推进。日历目前将期间清单交给一轮 Codex 任务；缺少分区级持久提交和失败续作。

目标是自动冻结完整来源版本并拆成有界 Codex 工作单元，所有叶子及归并输入都有来源证明。只有全部必要单元完成，宿主才生成最终窗口回执并追加原 L1–L4 目标。原始档案、手记与旧摘要版本继续保留。不得通过截断、调用者手工分割、模型自述或单独的组件测试宣称完成。

## 已确认的协议与实现

1. 小批次 schema 1 与既有来源 ID 保持原样；大窗口采用独立、显式版本化的分区计划，叶子按字节量和来源数双重限制，归并节点采用固定扇出及有界摘要。
2. 分片记录原来源 ID、完整文件 hash、记录位置与连续的原始字节区间。巨 JSONL 单行须完整扫描顶层时间字段，不能用正文中的伪字段或文件日期覆盖合法时间。坏来源及缺失不得静默丢弃。
3. 新增有界就绪单元查询与节点提交接口；候选沿用内容、引用、覆盖和缺口声明。节点提交幂等，异内容冲突；父节点只在全部子版本可核验时就绪。完整覆盖证明留在宿主侧，根节点不重复枚举全部原始来源。
4. 复用公开 Codex 执行与原生子任务。统筹任务查询就绪节点，每个原生子任务只读取自己的片段；模型循环与上下文管理仍由 Codex 执行。节点回执持久化，失败后再次查询同一计划只返回未提交工作；响应不明时以相同候选幂等重交。没有新增 Service 自动重派循环，统筹任务中断后需要恢复原生任务或显式续作。
5. 新协议使用 summary commit schema 2；SQLite schema 和旧批次读取仍为 1。schema 2 占位先于计划发布，保留完整 staging 后再写占位，重启可完成发布。旧 MemoryStore 在启动恢复时拒绝 schema 2。发布兼容负责人须在选择候选前消费 `SUMMARY_COMMIT_SCHEMA = 2` 和纯函数 `validate_summary_commit_header(value, *, supported_schema=2)`；本分支不修改发布选择器，也不证明旧候选已可安全激活。

`MemoryStore.summary_partition_next(batch_id, limit=4)` 只返回最多四个就绪节点描述，查询不等于领取；统筹者不应重复并发派发同一节点。`commit_summary_partition(batch_id, node_id, candidate)` 验证并持久提交节点；相同候选幂等，不同候选冲突。MCP 工具分别为 `memory_summary_partition_next` 和 `memory_commit_summary_partition`，两者进入明确白名单。旧 `commit_summary` 不能绕过分区根。

每片原文至多 65,536 字节，每个叶节点至多 128 KiB/64 个来源，归并扇出为 8，候选正文至多 16 KiB。计划的文件、原始记录、节点、上游覆盖均用有 hash 的 JSONL 目录保存；节点用根 hash 与子 hash 连接。片段不复制成新原文文件，读取定位到冻结原文件的原始字节范围。最终证明逐记录核对范围连续性、数量、版本和缺口；JSONL 的 LF、CRLF、bare CR 分行与旧来源 ID 一致。

后续层级若读取带分区证明的来源，仍使用分区协议。`upstream_ref` 追溯前层证明，`has_inherited_gaps` 保留祖先缺口；当前层片段数量与祖先片段数量不混成一个总数。来源目录保留多版本和手记，证明不表示重复资料已在语义上去重，也不证明模型的解释正确。

导入的 JSONL `coverage_ref` 与 Markdown `<!-- anima-coverage:batch:sha -->` 都属于输入契约：须能核验本地已提交计划和完整证明；缺失或不一致时明确拒绝，不能降级成普通来源并清除未知缺口。有效 Markdown 即使复制到其他日期，后续层仍保留引用。巨型 JSONL 的顶层引用键由流式扫描器识别；超过 1 MiB 的携引用记录明确拒绝，避免无界反序列化。此处不自动重建丢失的证明。

恢复除核对证明 hash，还独立重算记录、连续片段及缺口数量。最终提交先保存不可变的 `final-before.bin` 和 `finalization.json`，再写可重放 intent；重启从固定前像和根候选重新生成正文，不能只靠一起改动的 intent 正文与 after hash 授权写入。原目标存在时保留其完整前像，后续并发写入仍由前后 hash 冲突保护处理。

这是首次交付 schema 2；未部署的早期 Draft schema 2 状态若没有上述 finalization 证据，会保留原数据并拒绝恢复，不从可变 intent 猜测可信前像。schema 1 批次、数据库、旧事件和来源 ID 不迁移。回退只可选择能读取已产生状态的候选；不得删除新状态或用旧快照覆盖记录以迁就旧读取器。最终候选选择门槛由发布负责人组合验收。

## 流式扫描的实现依据

普通 `json.loads` 构造完整对象。已静态核对的 [ijson Python 后端](https://github.com/ICRAR/ijson/blob/master/src/ijson/backends/python.py) 会累计字符串 token 后再解析，也不提供本任务要求的连续原始字节范围；这不是对全部 ijson 后端的实测结论。因此使用独立的完整 JSON 语法扫描器，只保留短顶层时间元数据及顶层覆盖引用是否存在，不新增依赖，不使用前缀猜测。读取块至多 65,536 字节，嵌套深度至多 128，时间元数据至多 256 个字符；超限、重复时间键、坏尾、非法 UTF-8 明确报告。

扫描器初始提交为 `29d4d9877242e48ad10dffbc74475a1922229832`，CR 兼容修复提交为 `54c4be45a8c524dda7038cf09d8f804cac485e91`。专测包括 367 个固定种子的合法/非法变体（120 合法、247 非法），在三种读取块大小下与标准 `json.loads` 独立对照；另有旧读取器分行、行号与来源 ID 对照。扫描器 94 项源码合成测试通过，不替代最终同 wheel 验收。

集成方对 `29d4d987` 的独立审查另有 24,006 个严格标准库差分样本（5,410 合法、18,596 非法）零差异；64 MiB 普通正文、32 MiB 巨键/转义正文/选中时间样本的 Python 峰值约 266–330 KiB，时间字段超限明确错误。另 1,500 个合法 Unicode 范围和 1,500 个任意字节范围通过精确覆盖/hash/解码错误检查。对 `54c4be45` 的 CR 修复，使用 `io.TextIOWrapper(newline=None)` 独立核对 3,280 个换行序列×4 种块大小共 13,120 次，全通过。这些只证明对应冻结扫描器范围，不套用于整个 DAG 或最终 wheel。

## 合成验收矩阵

| 场景 | 必须观察的结果 |
|---|---|
| 巨单行、超 16 MiB 窗口、超 10,000 条记录 | 自动有界分区；冻结文件 hash 和全部字节覆盖可对账 |
| 时间字段位于巨正文之后、嵌套伪时间、Unicode/转义/CRLF 跨块 | 按真实顶层时间选择；不截断原字节；解析状态内存有界 |
| 损坏来源、非法/重复时间键、无法归属窗口 | 明确缺口或拒绝完整性声明；不得无来源跳过 |
| 来源追加、替换、删除 | 冻结版本可核验；改版创建新计划或显式冲突；不把旧覆盖冒充最新版本 |
| 叶子/归并失败、重启、重复同候选、异候选 | 已提交单元可恢复且不重复追加；冲突保留证据；unknown 先对账 |
| 缺失、重叠、错版本片段与漏掉子节点 | 宿主拒绝提交完整窗口 |
| 最终提交中断及同期手记修改 | 复用前后 hash 恢复和冲突保护；手记与新记录保留 |
| L1→L2→L3→L4 | 每层完整闭合窗口；跨周/月边界与继承缺口可追溯 |
| 旧小批次、旧候选读取新协议 | 小批兼容；不支持的新状态在切换前拒绝；回退不丢新增数据 |

验收记录分别列出源码合成测试、安装 wheel、真实 Codex 受控运行及真实模型证据。最终冻结产物与受控 native 结果见下节；接口提案、旧基线和模型自述不算通过。

独立复审已用行为测试复现并修复：输出与候选脱钩、locator 来源字段伪造、CR 行号变化、跨层缺口消失、恢复时提交目标重定向，以及旧提交入口绕过分区根。额外故障测试覆盖发布前占位、根回执后中断、目标写入后回执中断、同期手记冲突与空 Markdown 的显式缺口。

定向组合命令：

```text
python -m pytest alice/tests/test_summary_stream.py alice/tests/test_summary_partitions.py alice/tests/test_summary_partition_recovery.py alice/tests/test_summary_partition_review.py alice/tests/test_summary_partition_wiring.py alice/tests/test_memory.py alice/tests/test_memory_bounds.py alice/tests/test_memory_archive_acceptance.py alice/tests/test_memory_summary_acceptance.py alice/tests/test_legacy.py alice/tests/test_journal.py alice/tests/test_calendar.py alice/tests/test_config.py -q
```

早期 DAG 提交 `e5a5ca1955053b57cfb5ff89fd2d055ed1d5a7d8` 的结果：294 passed，exit 0，41.72 秒；Python 3.13.9。存在一条 FastMCP/Pydantic 的既有未解析注解警告。范围为源码合成行为与实际 MCP 注册/转发，不套用于后续修复或其他产物，不包含付费模型、真实私有资料或最终产物激活。

全量私人来源的最终停写归档、真实模型摘要质量、日历统筹任务中断后的自动重派、最终组合候选及部署仍由集成验收。存储开销随冻结来源及证明数量增长；写入失败会明确失败并保留持久证据，不设置丢弃原文的磁盘配额。已有最终目标渲染仍沿用完整追加与前后 hash 恢复机制，巨型目标文件的渲染成本未在此重写。

## 最终固定候选验收（2026-09-07）

产品恢复修复为 `888e37f67f6f9517a4f5a676340ca7739320cf11`；包含 native fixture 与独立探针的冻结源码为 `b00a1103770a994b3b37783e06743c6e41e887c0`。后续本文件的证据更新不改源码或测试。固定源码仅构建一次，未激活、合入或部署。

- wheel SHA256：`c5f516079a9f6331206914a47151ab97b8ce548d3e947f67d7ee60dea2d94105`。
- source fingerprint：`4a385d70842095f42e2422cfc0add2e3a60ad6039d450daf9085c981767cc875`。
- 环境：macOS、Python 3.13.9、codex-cli 0.153.4；Codex binary SHA256 `a30ec314bbd0e3721632234d07db7c99855db3b9f1e32dbe8c791947f07e7629`，配对 Code Mode host SHA256 `fdd977821def000939dd48da48b39d581845470671135bd4642584eeb0762a6b`。

可复现命令如下，在 `alice/` 执行。变量分别指向测试解释器、明确的真实 Codex binary、隔离发布目录、私有报告位置、该候选的 venv Python 与当前源码的绝对路径。不要用开发环境解释器替代候选解释器；不要传 `--case smoke` 作为边界验收。

```sh
"$ALICE_TEST_PYTHON" tools/check.py --source . --codex-binary "$ALICE_CODEX" --native \
  --release-home "$ALICE_RELEASE_HOME" --report "$ALICE_CHECK_REPORT" --timeout 900
"$ALICE_CANDIDATE_PYTHON" -I "$ALICE_SOURCE/tests/fixtures/summary_partition_probe.py" \
  > "$ALICE_PROBE_REPORT"
```

| 本次实际检查 | 结果 | 范围 |
|---|---|---|
| `ruff check src tests tools` | exit 0 | 静态检查 |
| `pytest tests -m 'not native and not live and not artifact' -q` | 1115 passed，13 deselected；181.37 秒 | 冻结源码合成单元；非选中层级不混为 skipped |
| `pytest tests/test_artifact_smoke.py -m 'not live and not native' -q` | 1 passed；7.19 秒 | 同候选安装入口 |
| `pytest tests --ignore=tests/test_native_code_mode.py -m 'native and not live' -q` | 10 passed，1117 deselected；49.82 秒 | 同候选、真实 binary、受控 Responses/MCP |
| `pytest tests/test_native_code_mode.py -m 'native and not live' -q` | 1 passed；10.84 秒 | 固定 Codex/Code Mode host 配对 |
| 候选 Python `-I` 独立探针，默认全部 case | 3 个完整边界 + 8 个故障 guards 全通过，exit 0 | 同 wheel 的结构与覆盖，不调用模型或原生进程 |

所有选中 pytest 用例均为零 failure、error、skip；源码层有 9 条 warning（8 条 pytest `record_property`/xunit2 提示及 1 条既有 FastMCP/Pydantic 注解提示）。`tools/check.py` 总 exit 0。报告属于本分支 policy 5 的验证结果；其中 `promotable` 不替代发布线对 summary capability 2 的最终组合检查或集成负责人授权。

`test_native_children_complete_large_partition_and_resume_after_leaf_restart` 本次在 native 集合实际执行，14.663 秒。超过 1 MiB 的完整原文由真实 Codex 的 12 个原生子任务节点逐页读取、hash 对账与提交，包含两层归并；第一叶子后重启服务，再由同一主线程的第二个统筹 turn 续作。原生 thread/turn 证据、完整覆盖、唯一根追加及同请求重放均验证。JUnit 明确记录 `partition_same_installed_wheel=True`，并校验服务包来自该候选 venv。受控 Responses 不调用付费模型；该测试不证明真实模型理解或摘要质量。原生子线程检索使用已知父任务活动引用逐个读取，不宣称线程列表可枚举所有原生历史。

| 独立探针边界 | 原始字节／记录 | 叶子／总节点 | 读取页数 | Python 跟踪分配峰值 | 耗时 |
|---|---:|---:|---:|---:|---:|
| 单条 17 MiB、时间字段在尾部 | 17,825,847／1 | 137／159 | 4,511 | 3,848,756 B | 5.32 秒 |
| 多条合计超过 16 MiB | 17,897,386／1,024 | 147／170 | 5,289 | 3,208,566 B | 7.39 秒 |
| 10,001 条短记录 | 758,967／10,001 | 157／181 | 10,181 | 3,370,117 B | 31.49 秒 |

三组均完整核对冻结文件、每条来源的父 ID 与连续字节范围、所有叶子和归并读取、全部 coverage hash、零缺口、最终唯一根标记；重新打开 MemoryStore 并重交保持计划和目标字节不变。这里的重新打开发生在同一 Python 进程，真实进程重启由上一项 native fixture 覆盖。峰值包含 fixture/store 初始化后探针本身和恢复校验的 Python 分配，不等同于 RSS 或 native 内存。

冻结原文每组仅额外保存一份。三组的总状态逻辑占用分别为 36,799,186／39,130,106／29,471,148 B；文件系统分配为 40,321,024／45,584,384／67,321,856 B；文件数为 1,085／1,891／10,923。短记录的每片定位与证明元数据开销明显，不能把内存有界解释为磁盘开销恒定。

八项 guards 在同 wheel 上实际验证：已提交缺口计数篡改、pending 正文与 after hash 联动伪造、小/巨 JSONL 引用缺证明、真实 L2→L3 与 L3→L4 Markdown 导入缺证明、非法或截断保留标记、有效 Markdown 复制到新日期保留祖先缺口。合计发生 8 次预期拒绝；拒绝前后的原始资料、目标及 intent 按场景保持不变。源码专项 13 项在旧 `e5a5ca1` 全部失败、冻结修复全部通过；旧基线缺少整体 embedded 检查，不将其描述为仅非法标记修复的孤立对照。

集成方两名独立复审者对冻结 `888e37f` 回放原反例：恢复方向 24 项通过，确认覆盖计数和 pending 正文伪造的两项阻断解除；导入方向真实 L1 JSONL、L2 Markdown、L3 Markdown 三种缺证明副本均明确拒绝且保留字节，三种完整复制 store/proof 的正例均分区提交并保留祖先缺口。原失败证据保留，不用新版结果覆盖旧报告。

私有原始报告由集成负责人保留；公开仅记录必要 hash 与统计：完整 check 报告 SHA256 `ffc1bc4cebfa0177669872affc763170e8f00f18474b679c48032508c25fb176`；独立 probe 报告 SHA256 `f69721152074b7cd4f213b6340c88ce4e49e8de5485fd0e59ca8b1cca8414567`；native JUnit SHA256 `69a830953dcb8285f8ec575fc1bcdde8485513ddea1ae86ec1d771f21ca76a30`。probe 实际加载的三个模块 hash 已与该 wheel 解包字节及冻结源码独立核对一致。

真实模型、真实私有资料停写归档、身份连续性、最终 summary capability 2 组合发布/回退与部署仍未在本线验收。PR 保持 Draft，#2/#19/#20 不据这些合成结果关闭。
