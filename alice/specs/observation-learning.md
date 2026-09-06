# 观察、纠错与资源约束验收

本工作线对应 Issue #4，拥有 `business.py`、`collector.py`、`resources.py`、`evaluation.py`、技能模板和对应合成测试。复用基线已有分页链、字段缺失、独立差异、发布读回和资源账本，只补可观察缺口。共享 CLI/MCP/config、服务接线及发布门槛仍由 #1/#3 集成。

## 观察 v1 可选扩展

页和独立视图可保留 `truncated`、`http_status`、白名单 `error`、`cache_age_seconds` 与 `cache_max_age_seconds`。采集器从 HTTP 读取有界缓存元数据，将失败原因同时保存在 observation 页中；仅保留 observation 再评分也不会丢失权限或限制诊断。响应正文、认证头值与原始分页 query 不进入诊断。

文档可声明 `freshness={as_of: 带时区 ISO8601, max_age_seconds: 非负数}`；采集 Python 接口的 `max_age_seconds` 可建立此策略，最终采集时间用于 `as_of`。缺失时效策略的老 v1 保持可读，报告 `coverage.freshness=not_checked`，只能说明声明范围内的快照。收到陈旧缓存、明确截断、权限失败、独立来源重用或不匹配时，总计保持 unknown。未观测到条目不等于确认不存在；完整空集合与真实零仍可判定。

读回可带 `intent.sent_at`，并对证据的缓存、截断、权限与观察时间判定；早于动作的读取不能确认该动作。HTTP 成功、同内容但未绑定动作的对象以及模型自评都不足以确认。旧 intent 没有时间时为 `temporal_check=not_checked`，不得宣传为已验证时间先后。函数不发布内容，也不授权自动重试。

这些是 v1 的兼容字段和保守语义变更，没有改数据库 schema 或持久 ID。已有记录原样保留，新记录使用新 receipt ID；不要重用旧 ID 覆盖旧 summary。历史 summary 不会自动重算；需要原始 observation 才能以新判定器追加重新评测证据。回退代码可读取老格式，但旧判定器会忽略新增元数据，因此不能在回退后据这些记录做新的完整性/结算决策；先停止自动决策并由集成线核对能力。保留所有新观察、金额凭证、token 事件和评测文件，不用旧快照覆盖运行数据。虚拟预算保持禁用。

## 固定任务集与评分

`tests/fixtures/observation_learning/tasks.json` 只存任务输入，`oracle.json` 是人工从合成事实推导的检查项，`correction.json` 记录失败案例、原因假设、修改和范围。版本为 `observation-learning-v1`，共 20 例：1 个训练原例、14 个未见形状、5 个反例。15 例预期发现观察/对账缺口。反例包含真实零、完整空集合、完整多页、有效缓存与正确绑定的读回，防止“全部 unknown”制造成功。

从 `alice/` 在已安装锁定依赖的隔离环境运行：

```sh
python -m pytest tests/test_business.py tests/test_collector.py tests/test_resources.py \
  tests/test_task_resources.py tests/test_evaluation.py -q
python -m alice_codex.evaluation \
  --tasks tests/fixtures/observation_learning/tasks.json \
  --oracle tests/fixtures/observation_learning/oracle.json \
  --report "$EVIDENCE_DIR/candidate.json" \
  --baseline-report "$EVIDENCE_DIR/baseline.json" \
  --correction tests/fixtures/observation_learning/correction.json
```

`EVIDENCE_DIR` 必须是仓库外的新证据目录，报告文件不允许覆盖。没有基线报告时省略最后两个参数可单独评分，不能生成纠正链。评分器逐例执行现有纯函数，仅给候选输入副本，不给 oracle；比较独立检查项，输出结果与输入/参考答案/实现/评分器 hash。报告中的 `passed`、计数和状态不能自证：纠正比较会重新评分实际输出。测试另以自填 passed、第一页截断、always-unknown 及伪造证据绑定证明错误候选被拒绝。

评分分别记录 passed、failed、error、not_run；缺口发现、漏检、误报、完整评分正确数；旧失败未解决、重复失败与新增回归；每例和总体耗时、执行中人工介入、原生 token 观察与金额凭证。候选报告内的 `candidate_evaluation_sha256` 指去除 `correction_chain` 后的规范化 JSON hash，避免自引用。输入/套件空缺、证据错配或纠正链错误不能通过；纠正链错误保留当轮已经执行的评分，命令退出非零。

实际基线使用 `main` 的业务源码 `18ba7803c6f81211716f614c0f394eec7c290383`，并非刻意替换为缺陷脚本：同一 20 例得到 12 passed、8 failed、0 error、0 not_run，退出 2。15 个预期缺口中发现 8、漏检 7；完整评分正确的缺口只有 7（范围不匹配虽报不完整，仍错误输出 known 总计）。原始报告留在仓库外，PR 记录其必要 hash 与候选结果。

若需重新复现基线，可在基线独立 worktree 保持其 `business.py` 原样，将本 PR 的 `evaluation.py` 临时复制到该 worktree 的同名包中（不要提交），然后用 `PYTHONPATH` 显式指向该 worktree 的 `alice/src`，输入本 PR 的绝对路径 fixtures。新报告会记录基线 commit、业务模块 hash 和源码 dirty；dirty 来自临时评分器，不能声称基线包含新评分器。不要给基线套候选结果。

本套件是公开离线规则评测，“未见”指未在训练原例出现，开发者可以阅读这些公开变体。通过只产生 `regression_passed_transfer_not_tested`，不会升级技能、修改记忆或授权发布。历史真实模型学习证据属于基线；本候选的真实模型迁移未运行。新的真实模型评测必须单独费用授权、隔离凭据及明确限制；不将原生 token 推算为美元，不用旧结果证明新产物。

## 任务策略与集成契约

`TaskPolicy(max_elapsed_seconds, max_attempts, max_retries, retry_wait_seconds, unchanged_wait_seconds)` 的五项限额均必须显式提供。`TaskUsage(started_at, attempts=0, consecutive_failures=0, last_outcome=None, last_finished_at=None, last_checked_at=None)` 保存 caller 已观察的事实。`policy.decide(usage, now=..., busy=...)` 是纯决策，返回 `allowed/state/reasons/next_attempt_at/remaining/recovery_required/observed_at`。

时间使用持久 UTC epoch 秒、由 caller 注入，包含等待时间。不得把进程 monotonic 时间跨重启保存。每次决策原子保存 `last_checked_at=max(旧水位, observed_at)`；重启加载原 usage，时钟回拨不能降低水位或退还预算。派发前先原子增加 attempts、保存 running 状态，结束后保存独立判定的结果。`complete` 必须来自目标结果检查，不是 native completed；`unchanged` 必须来自结构化观察，不从 completed 猜测。

- `ready` 允许派发，但仍须同时满足旧 `ResourceLedger.can_dispatch` 和原授权。
- `waiting` 表示忙碌、时钟回拨、失败重试等待或无变化等待；不消耗新尝试。
- 次数/重试限额只限制下一次派发：派发前扣除后，最后一次合法运行在尚有时间时仍返回 `waiting/task_busy`。忙碌任务的时间水位到达 deadline 时返回 `exhausted/task_time_exhausted`，即使系统时钟回拨也须停止；不能用次数已归零提前杀掉已准入运行。
- `exhausted` 表示时间、尝试、连续重试限制或剩余时间不足完成等待，需显式恢复。
- `reconciliation_required` 表示 unknown 或没有活动进程却仍为 running；延长限额也不能跳过对账。
- `complete` 保留已独立确认的完成事实，不再派发。

例如 `TaskPolicy(120,4,1,5,30)` 与 `TaskUsage(100,1,1,"failed",105)` 在 now=106 返回 retry_wait，next_attempt_at=110。在合成无变化循环中只允许 t=100/130/160/190 四次尝试，t=220 耗尽。恢复应显式扩大限额并保留 usage 和所有新增数据，不能重置计数。原生累计 token 继续只记用量，金额凭证走原账本，TaskPolicy 不修改虚拟预算。

本模块尚不持久化、不调度、不打断已运行的 native turn。#1/#3 负责按目标原子保存策略/usage、结构化 unchanged 事件、真实 deadline 中断、公共配置入口、重启及同一安装产物组合验收。单元/合成测试通过不能把这些接线、生产部署或自主能力标为完成。
