# 原生用量的进程分代观察

Refs #4、#18。资源账本使用公开 `thread/tokenUsage/updated` 通知，不改 Codex 的模型循环或上下文压缩。金额只来自独立的金额凭证；虚拟预算默认禁用。

## 缺口与边界

同一个原生线程跨 AppServer 重启时，累计 `tokenUsage.total` 可能重置。schema 1 只按 thread 保存高水位，因此会把新一代数据判作乱序；新代最终超过旧高水位后，继续与旧代比较也会丢失范围信息。仅凭计数下降无法区分重置和迟到通知，不能自动猜测新一代。

API 为 `ResourceLedger.record_token_usage(params, *, event_id=None, epoch_id=None)`。返回值仍为 `recorded`、`state`、`increase`。调用方必须从自有 AppServer 的生命周期确认 epoch；它不是原生账单标识，也不能证明初始计数为零。

Service 所有者后续独立接线：

1. 每次启动新的自有 AppServer 前，持久保存 opaque epoch 和实例关联。PID 本身会重用，不足以充当持久 ID。
2. 注册 listener 时捕获该实例的固定 epoch；同进程重连复用它。旧进程迟到通知不得读取可变的“当前 epoch”。
3. 只记录已确认自有线程。未知来源或旧记录保留无 scope；不能根据总数、turn ID、连接次数或请求重试推导 epoch。
4. 保持原通知内容，不把 `last`、上下文窗口或压缩后的上下文大小重写为消费。适配器若提供 `event_id`，必须复用同一证据 ID。

本资源 PR 不修改 Service。现有不传 epoch 的调用继续进入 legacy 区域，因此本 PR 的合成通过不代表活动服务已经完成分代接线。

## 去重与统计

已指定 epoch 时，事件按 `(epoch, payload fingerprint)` 去重，高水位按 `(epoch, thread)` 保存。一个 epoch/thread 的第一份完整累计快照是基线，`increase=None`。后续完整且每个必需字段都不下降时，返回相对上一个高水位的增量；任一必需字段下降则保留通知为 `out_of_order_or_counter_reset`，不推进高水位。不完整数据保留为 `unknown`。可选 cache-write 字段单独保留，缺失不填零，不加入必需字段合计。

回执 ID 全局绑定到 `(epoch, fingerprint)`；无 epoch 的旧 ID 也不能被换代。相同证据在同代的新 alias 被持久绑定后才返回重复；alias 之后指向其它 payload 或 epoch 会报冲突。不同代的相同 payload 是独立快照，须使用不同的回执 ID；未传 ID 时系统按 epoch 和内容生成。回执、事件和高水位在一个事务内提交，重试和并发不能重复累加。

`status()['tokens']` 的字段含义：

| 字段 | 含义 |
|---|---|
| `threads` / `legacy_unscoped` | 旧的无分代观察，保持隔离，不混入分代合计 |
| `epochs[epoch][thread]` | 原始第一份完整快照、高水位、第一份之后的高水位变化、事件数及不确定事件数 |
| `sum_epoch_high_water_marks` | 各代各线程已观察峰值的算术和；可能包含继承或重叠历史，不是总消费 |
| `sum_observed_increases_after_first` | 各代各线程第一份完整快照之后的高水位变化之和；不是账户账单，也不补算初始未知区间 |
| `state` | 已收到的计数观察是否完整且无乱序证据；`known` 不表示生命周期消费已知 |
| `epoch_boundary_state` | 始终 `unverified`：进程分代不能证明快照之间没有历史重叠 |
| `actual_usage_total` / `cost_microusd` | 始终 `None`，不根据快照推算消费或金额 |

没有完整分代快照时，两种合计均为 `None`。未知事件保留在事件数中，后续完整记录不会抹去不确定性。`last` 或 context window 改变而 `total` 不变时，观察增量为零。

## 显式 schema 1 → 2 迁移与回退

新空账本直接创建 schema 2。已有 schema 1 用普通构造器打开会明确拒绝，必须由集成负责人安排迁移；不会静默加表并让旧读者忽略新记录。

迁移前停止所有该库写入者，保留恢复依据，准备并验证能读写 schema 2 的回退代码。调用 `ResourceLedger.migrate_v1(path)` 在 `BEGIN IMMEDIATE` 事务中验证数据库，保留原六表的结构、内容和 ID，增加 `token_epoch_events`、`token_epoch_checkpoints`、`token_epoch_receipts` 三表。原始通知不会被追溯分代。`settings` 新增 `token_epoch_migration_v1_to_v2` 回执，包含版本和六表原记录数量；原设置不覆盖。

成功返回 `{migrated: True, receipt: ...}`。重复调用返回 `migrated: False` 和同一份回执；迁移后新增记录保留。原本新建的 schema 2 没有迁移历史，返回 `False` / `None`。损坏、未来版本、缺失表或列、冲突的半迁移表或回执会被拒绝；缺失文件不会被新建。DDL、回执及版本一起提交，失败全部回滚。

schema 1 代码会拒绝 schema 2。升级后只允许回退到已验证且兼容 schema 2 的代码，保留所有新通知、金额、观察与回执；禁止把旧数据库快照覆盖到当前运行数据。本改动不执行任何 preview/正式库迁移或部署。

## F01 合成回归与证据层级

以下合成夹具复用了已有受控验收的计数数值，替换了所有来源 ID。测试不读取原始私有日志、不启动真实模型。

| 代 | total | last.total | 新实现的 total 增量 |
|---|---:|---:|---:|
| A | 11992 | 11992 | 未知（第一份基线） |
| A | 25125 | 13133 | 13133 |
| B | 13291 | 13291 | 未知（第一份基线） |
| B | 27719 | 14428 | 14428 |
| B，压缩观察 | 27719 | 6844 | 0 |
| B | 40788 | 13069 | 13069 |
| B | 54990 | 14202 | 14202 |

旧实现实际回放结果：3 个事件被判为乱序/重置，只有单线程峰值 54990。新实现保存 A=25125、B=54990，峰值和 80115，代内高水位变化和 54832。上述数值均不证明账户消费；特别是压缩观察的 6844 不能叠加为费用或消费。

`tests/test_token_epochs.py` 另验新代起点高于旧峰值、同代下降、子计数下降、缺字段、上下文变化、跨代重复、持久 alias、并发与 legacy 隔离；`tests/test_resource_migration.py` 使用独立合成 v1 库验证原文保留、失败回滚、幂等及 schema 拒绝。

复验从仓库根目录运行：

```sh
PYTHONPATH=alice/src python -m pytest alice/tests/test_token_epochs.py alice/tests/test_resource_migration.py alice/tests/test_resources.py -q
python alice/tools/check.py --source alice --release-home /path/to/isolated-candidate --report /path/to/private-report.json
```

第二条命令在未显式提供真实二进制时只验证源码和合成安装产物；不能宣称 native/live 通过。对应确切 commit、命令、测试数量、退出状态和产物 hash 在 PR 中交接。真实模型新增调用和费用均为零；账户剩余额度未查询，金额、原生消费与发布准备状态不能由这些测试推断。
