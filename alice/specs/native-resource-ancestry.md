# 原生 V2 子线程资源归属

Issue #4 的 token 观察依赖明确的原生父子关系。受控 Codex 0.153.4
会在父线程发送 `item/completed`，其中 `item.type=subAgentActivity`、
`kind=started`、`agentThreadId` 指向新子线程；不保证另发该子线程的
`thread/started`。这是原生 typed item，不是模型输出中的 ID 或 agentPath。
保存的 `thread/read` 同时提供 `parentThreadId` 和
`source.subAgent.thread_spawn.parent_thread_id`，可交叉核验。

CodexClient 在发送活动的父线程已属于持久根的原生树时记住父边。
Service 同步使用该关系，将暂存 token 按接收次序写入已绑定的资源 epoch。
根 alias 尚未持久化时仍暂存，不能通过临时注册绕过持久化门槛。
不同父边、字段矛盾或循环会隔离该线程及后代；旧边只保留用于判断
停止整棵树的证据已不完整，不允许改归另一个根或用刷新清除隔离。
`thread/read` 的返回 ID 必须与请求一致，否则不接纳返回的关系。

漏事件或重连后只知道子线程 ID 时，可在连接 initialize 完成后，通过公开
`thread/read(includeTurns=false)` 读取该 ID 及它明确声明的父 ID。
每个连接和 epoch 只有一个合并工作任务；每个仍积压的 ID 只尝试一次，
单条链最多 16 次读取、5 秒。正常原生活动路径不需要逐 token 请求。
不可用、矛盾、外部根和超时均不能证明归属。仍保留 256 条、1 MiB 的
原始积压上限，超限明确停派；提高容量不属于归属修复。

归属工作任务不加入先被取消的普通后台任务列表。受控子进程退出且原生
reader 完成尾帧排空后，Service 等它最多 1 秒；未结束的读取必须取消并
await，再关闭 Codex/RPC/数据库。已经进入关闭的 observer 不接受新工作，
关闭返回后不再记账。所有回调与补查始终使用其捕获的 Codex 和 epoch。
关闭仍有积压时明确留下失败并写入私有服务日志；取消关闭也必须完成读取
清理。旧 observer 的失败保留在其自身，不能暂停后来替换的新连接；账本
对象也在建立 observer 时捕获，不能通过可变的 Service 字段写入新实例。

此修复不改变 schema、持久 ID、数据库路径、token payload 去重或金额凭证。
回退代码须保留新增账本记录；旧版本的 V2 归属缺陷仍在，不能把兼容读取
解释为适合重新激活。token 仍是累计观察，epoch 间重叠未知，不能作为实际
消耗或账单。虚拟预算保持禁用；真实模型费用和真实网站发布须另有授权。

## 验证边界

合成回归先证明 `9 child × 35` 通知在旧实现溢出，再验证同步归属、迟到活动、
漏事件公开读取、错误 ID、冲突和循环、单 worker、超时、跨 epoch 和关闭顺序。
停机不能在矛盾子线程从清单消失后静默宣告成功。

真实摘要测试的 `ALICE_SUMMARY_RESOURCE_RECEIPTS=1` 模式要求显式的安装候选
和 Codex/Code Mode 同目录二进制 hash。透明测试 wrapper 保存 Service 实际
接收的 token 原帧；独立判定器逐项核对 `(epoch, payload hash)` 对应的
SQLite 事件及 receipt。两次进程 epoch、12 个原生子节点、2 个协调 turn、
原始来源分页/hash/完整覆盖和停止后重启断言都必须通过。原始帧仅保存在
隔离测试目录，报告只交付统计和 hash。localhost 合成 Responses 不证明
真实模型的摘要质量或自主学习；失败旧候选的材料不得删除或改写。

正式发布检查仅在 `native_summary` 组显式开启严格回执模式，不能依赖父进程
环境透传。诊断模式不会进入发布子进程；单独使用 fixture 时，两种 wrapper
同时开启会明确拒绝，不能通过后一次赋值静默替换另一种观察。
