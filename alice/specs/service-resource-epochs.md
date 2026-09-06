# Service 的 AppServer 用量分代接线

Refs #4、#18；基于任务策略 #23 与资源账本 #24。资源账本的统计及 schema 2 迁移遵循 [token-epochs.md](token-epochs.md)。此接线只改 Service，不新增 token 预算、不改原生模型循环。TaskPolicy 继续独立限制时间、尝试、重试与等待，不使用原生 token 或金额计数。

## 持久来源与启动顺序

`runtime.json` 保持 version 1，增加可选的 `resource_epochs` 映射。旧文件缺少映射时按无分代历史读取，不把旧 token、旧 PID 或旧线程猜成任何新 epoch。新启动由宿主生成 opaque UUID，并追加以下记录：

```json
{"resource_epochs": {"opaque-epoch-id": {"state": "prepared", "server": null}}}
```

获得实际自有子进程身份后，同一次原子保存将记录改为 `bound`，`server` 存其 `pid`、`birth` 和 `identity`；原有顶层 `server` 同时增加 `resource_epoch_id`。既有 bound 记录不因停机或下一次启动而删除，供已保存的分代用量回查；顶层 `server` 仍在停机后清空。原有运行字段、任务、暂停和回执保持原样。

顺序固定为：持有 Service 锁 → 恢复并停止已绑定的旧自有 server → 对账任务策略 → 持久保存新 prepared → spawn → 验证自有 child 身份并持久保存 bound → 连接 RPC → 同步创建 CodexClient 和固定 listener → initialize → 发现归属及归档 → 开放控制入口。绑定保存失败时没有 listener，也不能降级写入 legacy 区域；清理仍通过已持有的 child 句柄停止并等待进程。

`prepared` 表示启动结果尚未绑定，不能凭顶层 `server=null` 判断没有孤儿。重启发现未对账的 prepared 会拒绝再次自动 spawn。只有明确未尝试 spawn，或有自有 child 句柄且确认已经退出，才可在清理时保存 `aborted`。没有句柄、没有可核对的身份时保留 prepared，要求人工或后续显式流程对账；不按进程名查杀，不清除记录后重试。

因此，**已绑定 server 的普通重启及 SIGKILL 恢复，与 prepared→spawn→bound 之间的极早崩溃窗口不同**：后者保守停止，需要对账，本改动不声称所有启动时刻都能自动恢复。

## 监听与早到通知

每个 listener 捕获注册时的 epoch、RPC 和 CodexClient。更换 Service 的当前连接、当前 client 或顶层 server，不能重新归属旧 listener 的通知。同一活进程重新建立 RPC 连接时沿用原 epoch；只有新 spawn 创建新代。迟到的旧连接 token 仍属于旧代，旧连接的其它生命周期通知不更新新任务状态。

listener 在 `initialize()` 之前安装。连接若已有留存通知，先通过 CodexClient 既有事件处理恢复可信原生 ancestry，再按顺序回放资源通知；缺失留存前缀时明确失败。未绑定 listener 的 `_on_notification` 直接收到 token 会拒绝，不继续采集无 scope 数据。

token 可能早于 `thread/start` 返回，或早于宿主成功保存线程 alias。每个 listener 最多缓冲 256 条、合计 1 MiB 尚待核对的参数。只有已成功落盘的宿主 root 及其可证明的原生子代才能进入账本；线程 ID 本身不能授权归属。用于判断的 CodexClient 只读视图共享原生 ancestry，root 集合独立限于已保存的宿主 alias，不重复实现所有权遍历或注册新监听。

保存 alias 成功、收到建立父子关系的 `thread/started`，或完成原生归属发现后，按原通知顺序释放已确认部分。一个尚未保存的新 root 不阻挡其它已保存 root 的合法子代理。写入失败或缓冲溢出会显式停止派发；RPC 会隔离 listener 异常，因此不能仅抛异常而让 Service 继续运行。确认前没有归属的通知不计入别人的账本，未确认区间不能被解释为消费为零。

资源 listener 保留至 native 停止及 RPC 关闭，确保停机尾部通知仍归原代。账本按 epoch+payload 去重，`last` 和上下文大小不追加到累计量；第一份累计快照是未知消费基线，金额凭证保持独立、虚拟预算保持禁用。

## 兼容、回退与验证

这是显式协调的 additive runtime 字段扩展，不重新编号已有 ID，不覆盖旧 token。模块常量 `alice_codex.service.RESOURCE_EPOCH_CAPABILITY = 1` 声明此 Service 理解 prepared/bound/aborted 的启动语义。**此声明本身不是激活或回退门槛**；发布负责人另行从已安装候选读取能力并接线拒绝逻辑，缺失声明视为 0，未知或损坏的数据也须拒绝。组合验证完成前不声称安全回退。

仅能读写 ResourceLedger schema 2 不足以成为 previous：受控反例中，旧 Service 虽保留未知 JSON 字段，仍忽略 `prepared` 并到达下一次 spawn 边界；当前 Service 则拒绝启动且保留待对账记录。安全 previous 必须同时具有新 Service 分代语义、资源 schema 2 兼容性，并通过安装产物的原生生命周期验证和独立发布门槛。停止写入并核对进程来源后才能切换代码；禁止用旧 `runtime.json` 或数据库快照覆盖新增记录。本改动不迁移活动库或部署，previous 双候选由集成负责人准备验证。

`tests/test_service_resource_epochs.py` 用真实 ResourceLedger/CodexClient 与合成进程、RPC、故障边界验证保存顺序、早到/迟到、重连、去重、所有权、缓冲上限和拒绝路径。`tests/test_native_service_resource_epochs.py` 则必须提供明确固定的 Codex 与 sibling `codex-code-mode-host` 路径及各自 SHA256，以及安装当前源码的隔离解释器；缺失配置失败，不算跳过通过。

native 案例运行未包装的已安装 CLI/Service，使用隔离 HOME/CODEX_HOME、只被 localhost provider 接受的合成 Bearer 和有限 Responses SSE。普通重启、Service SIGKILL、Codex SIGKILL 各运行 4 个受控响应：provider 单次 total 为 80、90、30、40。此固定二进制恢复会话时报告继承的累计快照，因此第一代观察为 80→170，第二代为 170→200→240；第一份之后的变化分别为 90、70。观察峰值和 410、变化和 160 均不是账户账单，也不能把继承的 170 当作新消费；其它进程重启后计数降低的变体由合成用例单独验证，不从此 native 案例推断。另保留合成 legacy 999，验证不会混入分代和。双外部 RPC 订阅只核对通知，并不伪称 Service 必然收到重复；重复投递另由合成 listener 测试注入。

测试源码与候选包一致、固定 pair 双 hash、用量数和退出状态分别记录。付费模型请求为零，不读取真实账户凭据或向真实网站发布。此层证明公开协议和 Service 生命周期，不能证明真实模型能力、业务学习或正式迁移完成。
