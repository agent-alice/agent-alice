# 调度与恢复运行契约

对应 Issue #1。Alice 只使用 [Codex App Server 公开接口](https://learn.chatgpt.com/docs/app-server)，不实现模型循环。本文描述调度适配器的行为和验收入口；源码、安装产物、部署与真实模型能力分别验收。

## 状态、所有者与重试

| 状态 | 可观察含义 | 重启或重试规则 |
| --- | --- | --- |
| `pending` | 到期窗口已持久化，尚未发送 | 可由有效 lease 的唯一 owner claim |
| `claimed` | owner 正检查准入，尚未发送 | lease 过期后恢复为同 ID 的 pending |
| `sending` | 发送前记录已落盘，RPC 可能已开始 | lease 过期后变 unknown；不能凭超时重发 |
| `accepted` / intent `queued` | 原生 turn 或队列已确认接收 | 用 thread ID、turn ID、client ID 对账，不算任务成功 |
| `unknown` | 无法确定接收或完成结果 | 占用任务容量、阻止该 job 后续派发，等待原生历史或独立回执 |
| `completed` | 观察到 Codex completed；摘要另验 committed 批次 | 不重发；外部业务结果仍需独立判定器 |
| `failed` | 原生失败/中断或确定的永久拒绝 | 不将同窗口自动重新执行 |
| `cancelled` | 未发出窗口的 job 已删除、禁用或修订 | 保留历史，不能借重试恢复旧定义 |

每个 Scheduler 实例使用随机 UUID 作为进程代次/lease owner。SQLite 单事务维护唯一 lease、claim、窗口及游标。默认 lease 30 秒，每 10 秒续租；busy 查询最多等待 lease 的一半与 dispatch timeout 的较小值；派发确认默认上限 120 秒。丢失 lease、取消或超时都不提供“未执行”的证明。控制服务另用文件锁保证唯一入口；所属 Codex 进程以 PID、出生时间、命令和 socket 身份核对，不能按进程名杀进程。

`request_id` 在一个 Alice 实例中全局唯一，指纹绑定 target 与文本。所有 submit 入口共用准入锁，保护跨 target 的 ID 与自动任务容量；锁只持有到原生确认，不等模型完成。同 ID 同输入返回已有回执，即便现在暂停；同 ID 不同输入明确拒绝。自动准入的容量查询也能处理普通关闭后留下的未加载根：同 ID resume 后才核对容量，仅明确无 rollout 的无输入根可替换；无关的已暂停根不会被容量查询唤醒。原生未决意图与可见 active roots 的并集占用自动任务容量，避免确认和状态可见性之间的竞态。手动输入保留原生排队语义；原生 TUI 在 Alice 之外直接发起的并发不受此锁原子控制，Codex 自身的准入仍有最终决定权。

`DeferredDispatch` 只用于确定尚未发出输入的暂停、忙碌或依赖等待。它是内部异常，沿用现有控制面错误通道。Scheduler 校验有效 lease、原 owner 和 sending 状态后把同事件退回 pending；若期间定义改变则 cancelled。窗口、ID 和已推进的游标不会丢失。任何可能已发送的异常继续保持 unknown。新实现不会为确定未发送的暂缓写入永久 failed intent。

## 到期窗口与摘要依赖

稳定事件 ID 保留既有 `anima:` UUID5 命名和 job revision，不重命名。停机积压持久化为包含首尾到期时间的范围；摘要 planner 展开范围内全部关闭期间，超过 4096 期间明确要求分区。heartbeat 只合并尚未发送的 pending 窗口，保留原起点与 catch_up 标志。合并不等于完成，也不证明模型会处理所有业务窗口。

摘要按目标期间检查低层依赖，历史中无关期间的 failed 不再永久阻塞新窗口。相关在途/未知事件仍阻塞；失败期间的解除需要完整覆盖的已完成同层窗口，不能用部分覆盖掩盖失败。L4 使用重叠的完整周作为来源，需等待跨月尾周关闭。日历依赖只判断已持久化事件，不能替代原始资料完整性和来源版本验收；实际摘要完成仍检查 commit receipt。

## 暂停、配额和进程恢复

原生 interrupted 通知先同步持久化任务暂停；main 被打断时持久化全局暂停。停止树使用原生 Goal 暂停、input queue 暂停、turn interrupt、后代发现和真实终态观察；ack 本身不算停止。迟到响应不能解除暂停或继续派发。恢复与提交共用准入/输入锁，暂停代次栅栏拒绝并发暂停之后返回的恢复确认；恢复中收到配额耗尽时也持久保留待停树义务。普通退出与崩溃重启保留暂停，不自动启动新主线程。只有从未有输入/意图的空 thread 在 Codex 未保存 rollout 时可以替换。对精确的 thread-not-loaded 错误先按原 ID resume，只有随后明确无 rollout 才考虑替换；替换成功前保留原 alias，成功后继承暂停。TUI 的 turn/started 和 userMessage 通知先同步落 has_input，不依赖异步归档工人。停止时对这种明确不存在的空 alias 返回 absent_empty_thread 证据并保持暂停，不为了停止而创建新根；有过输入或存在未知意图的线程不能走此分支。

明确原生 quota 耗尽除阻止新 automatic 输入，还暂停已有自动根的整棵原生树。task/intent 的 `automatic` 字段记录自动归属；一旦根承载自动工作，手动 follow-up 不能撤销它的归属，因为同根上仍可能存在自动 Goal、队列和子代理。这里是根的归属，不是当前轮的来源判定：同根后来经原生 TUI 直接输入的手动轮也可能被停止。混合根采用保守的整树暂停，不能宣称只中断自动轮；纯手动独立根保持原有策略。`resource_pause_pending` 在发出停止前落盘，重启补完未确认的停止；配额恢复不能自行解除已有暂停。未能证明停树时服务关闭并终止自有 Codex。

控制进程 SIGKILL 后，新实例核对并回收原 Codex 孤儿，再恢复原 thread/intent 关联。Codex SIGKILL 后服务停止派发并保持人工暂停；native inProgress/缺失终态只保留待对账状态，不推断 completed，不换 ID 重放。公开 inventory 分页扫描 archived 与未 archived 列表，并核对父链；只接管属于已登记 roots 的后代。

`service.recover_owned_server(config, state)` 是独立的异步孤儿清理入口，供持有 `service.lock` 且已确认旧 daemon 退出的调用者使用；`Service._recover_orphan` 委托给它。入口不构造 `Service`，不打开业务 SQLite、不验证或迁移业务 schema，也不写 Alice runtime、别名或暂停状态，因此业务数据库损坏或来自更新版本仍可清理已确认的自有进程。调用者提供已读取的 runtime，清理只使用 server 身份及可读取的 task root ID；runtime 无法读取或 server 身份不能验证时，不能推断进程归属。

清理逐个尝试所有有效根的原生 `stop_tree`，精确的根 not-loaded/not-found 错误先按同 ID resume 再停树；这可能更新 Codex 自己的原生状态，但不会重建根、提交输入或恢复 Alice 派发。一个空别名无 rollout 或其他单根异常不会跳过其余根。原生连接或停树失败后仍按出生身份和进程组再次核对，仅终止自己的原有组。函数正常返回只证明原组已退出或原进程已不存在，不证明无法访问的原生后代、脱离原组的进程或外部副作用都已消除；不能据此把未知任务标记完成。supervisor 的锁与退出顺序由集成线接入，本入口自身不取得第二把锁。

### 首次 TUI 与空根持久化

Codex 0.153.4 的空 `thread/start` 结果可能仅在内存中：`thread/read(includeTurns=false)` 成功不等于 TUI 可以 `resume`。`ephemeral=false`、两种 historyMode 及 excludeTurns 都不能替代持久化。Alice 在新 owned root 返回给调用者前，通过公开 `thread/inject_items` 追加一条真实的 developer 宿主注册事实，然后验证同 ID `thread/resume`。该记录不是用户消息、助手回答、模型问候或历史重建；不发起 turn，也不授权 Goal。Alice 身份仍由原 `thread/start` 的 developerInstructions 提供，根 ID、人工暂停及 `has_input=false` 保持不变。

alias 与绑定根 ID 的 `bootstrap` 元数据在原生写入前落盘：version 1 的 pending → sending → ready；请求异常为 unknown，取消可保留 sending。ready 只表示同 ID 的原生 rollout 已可恢复，不表示公开历史找到了初始化记录。原生 developer raw items 不一定出现在 `thread/items/list`，因此空列表不能授权再次注入。sending/unknown 遇到 no-rollout 时拒绝重试或换根；后续同 ID resume 成功即可对账 ready，无需重注。真正不明的初始化由集成负责人核对，不自动重放。

已有未标记的根先尝试原 ID resume，成功便不追加任何记录。对仍 loaded 的旧空根，只有精确 `-32600 no rollout found for thread id <当前ID>`、Alice 没有输入/意图且元数据 idle 才允许补宿主记录；每次等待后重查输入证据。初始化前不请求完整历史：未物化的 paginated 根可能对 turns/list 或 read(true) 返回 `list_turns is not supported yet`。已有真实工作不进入补写分支。Python 锁不能原子隔离直接原生 TUI 的输入；最后一次元数据检查与注入之间仍可能出现外部输入，因此记录只陈述始终真实的宿主注册事实，不断言“没有发生过用户输入”，也不覆盖已有消息。

## 兼容与回退

本次不变更 runtime version 1、SQLite schema 1、事件 ID、路径和 CLI/config/MCP 接口。新增的 runtime 字段可由旧版本读取/保留；`bootstrap` 绑定确切根 ID，安全替换从未持久化的旧空 alias 时重新初始化其 pending 状态，不能沿用旧根的 ready。回退保留新增元数据和原生宿主记录，不删除或重建 rollout。旧数据缺少 `automatic` 时不推断原生 Goal 的来源，需集成负责人在暂停状态下核对旧根归属。旧版错误写成 failed 的窗口不会被自动重置，因为旧记录不足以证明没有外部副作用。

回退前暂停并确认自有进程退出，保留当前 runtime/SQLite/记忆及所有新记录，再运行 schema 兼容的旧代码；不能把旧快照覆盖当前数据。旧代码没有本次准入与配额停树保护，回退不等于这些能力仍在。真实数据迁移、生产唯一派发者和部署由集成负责人完成。

## 可运行验收与证据边界

在隔离环境安装固定依赖并从本源码目录运行；`ALICE_CODEX` 必须指向显式验证的真实二进制。原生测试为 Codex CLI 0.153.4、私有 HOME/CODEX_HOME、localhost Responses fixture 和合成资料，不读取真实账户凭据，不调用真实模型。

```sh
python -m pip install -c requirements.lock -e '.[dev]'
python -m pytest tests -m 'not native and not live and not artifact' -q
ALICE_TEST_CODEX_BINARY="$ALICE_CODEX" python -m pytest tests -m 'native and not live' -q
python tools/check.py --source . --codex-binary "$ALICE_CODEX" --native \
  --release-home "$ALICE_PRIVATE_RELEASE_HOME" --report "$ALICE_PRIVATE_REPORT"
```

- `test_service.py`、`test_scheduler.py`、`test_store.py`、`test_calendar.py`：重复 ID、原子准入、并发 claim、发送边界暂停、跨窗口/时区依赖、配额停树及持久待停止恢复。属合成传输与隔离本地数据证据。
- `test_owned_recovery.py`：业务存储不可用时独立清理、逐根停止、精确同 ID 恢复及身份变化拒绝发送信号；使用合成 RPC 和隔离自有进程，不作为真实 Codex 协议证据。
- `test_native_service.py`：实际 CLI + 真实 Codex + 实际必需 MCP；重复请求、原生中断/迟到响应、普通重启、控制进程 SIGKILL 后孤儿回收、活动 Codex SIGKILL 后恢复、MCP 初始化失败且原数据保留。
- `test_thread_bootstrap.py`：合成 RPC 验证 alias/状态先落盘、developer-only 宿主记录、未知初始化不重放、原 ID/暂停保持及真实输入保护。`test_native_tui.py`：真实 Codex、隔离 PTY 与实际 chat 入口，核验首次输入框和冷启动同 ID 恢复；无模型请求的启动验收不代替对话能力评估。
- `test_rpc.py` 的 native 用例：公开协议和真实 V2 子代理执行、发现与停止。`main` 是持久受控根；独立命名 target 通过 thread/start 建立另一受控根；V2 子代理由 Codex 原生协作创建，Alice 只按父链识别并停止，不建立另一代理循环。
- `test_artifact_smoke.py`：安装 wheel 的实际入口和故障演练，所用 fake Codex 不作为原生协议证据。完整检查器另将 `ALICE_ARTIFACT_PYTHON` 指向同一个 wheel 的解释器，native service 子进程以 `-I` 运行该产物。

最终 PR 附确切 commit、完整测试命令、数量、退出状态、wheel/source/Codex hash。私有日志和安装路径留在仓库外。测试失败、跳过、超时和未运行项目分别记录，不套用基线 #6 的旧报告。

仍需 #3 统一合入与部署；#4 的 TaskPolicy/TaskUsage 与结构化 unchanged/progress 反馈接线后才能验收持久预算、失败退避和无变化等待，不能把 completed 自然语言当作无变化证据。已归档 V2 子代理若不出现在当前 Codex 公共 inventory 中，双视图扫描仍无法证明其历史穷尽；该边界与运行中 V2 发现/停止分别报告。真实模型能力、生产切换和私有资料的完整归档不由本轮合成或受控原生结果证明。
