# 长摘要与资源分代的组合失败诊断

关联 #2/#3；本记录是失败定位，不是发布通过或真实资料验收。基线为 `7ea32d40923585058731d800998085147c437dcb`，从该版本创建独立诊断分支。原记忆分区分支、候选和报告保持冻结。

## 原失败与实际原因

组合 wheel SHA256 为 `3af7884a592280e5c2e3f48e74730ca44e41b8d99d8261ff03790d8416dc3c20`，配对 binary/Code Mode host 分别为 `4ca47945439f9251fe35f4cbe071369192cd9a6c5a3a17b75c7a11ad548a9c7f` / `207984ae6d639c39370fc01ca9b1c79f72487a842b0b70407c92ed0a7c295d6f`。原 native summary 为 1 failed、零 error/skip，exit 1。其他配对的通过结果不套用于此候选。

原 `partition-call-315` 读取一份 112 字符的中间摘要。原生事件先记录 MCP `isError=true` 与 ENOENT，随后 Code Mode 返回完整的 289 字符 `Script failed`，再出现子任务和父任务的 Responses 断流。它没有 yielded cell 或截断提示，且摘要文件及 hash 仍与 manifest 一致。fixture 因不存在成功标记而报“页面不可见”，该消息掩盖了真正的工具错误。

经集成负责人授权，在全新 owned localhost/basetemp 上进行一次旁路日志诊断，保留同一冻结产品及原断言：**1 failed，7.89 秒，exit 1**。这不是重跑发布门槛或改写原失败报告。实际 `Service._fail` 原因及触发时状态为：

- `Native resource observation buffer could not be preserved`，来自 `_ResourceEpochListener.receive`。
- 未归属 pending 恰为 **256 条**，达到既有 `MAX_PENDING=256`；仅 **104,192 字节**，未达到 1 MiB 字节限额。
- `known_parents` 为空，durable roots 只有主线程；pending 有 9 份不同观察内容。
- 重启后七个完整叶子各有 35 条 token 通知，末叶有 4 条，第一归并有 7 条，合计 256。下一通知触发保护性停机，控制 socket 随后消失，导致 MCP ENOENT。
- 第一阶段的显式 stop 时，也仍有该叶子的 35 条未归属通知。

问题是大量原生 V2 子任务的父链未及时获得验证和归属，资源观察被长期留在 unresolved buffer。不能通过增大缓冲区、延迟、降低分页完整性要求或忽略未知用量来修复。

## 本分支改动与证据范围

本分支只拥有 native summary 的测试诊断，不修改 `service.py`、`codex.py`、发布选择器、持久 schema 或事件 ID。`ALICE_SUMMARY_DIAGNOSTICS=1` 选择可选测试服务包装器：它记录原 `_fail` 的原因和 pending 计数，然后调用原处理；不更改归属、限额、派发或原生行为。输出只写调用者给定的私有测试路径。

诊断运行后补充了 fixture 失败消息的工具状态分类和三个纯合成回归；未再次运行 native。`value is not None`、全部分页/来源/hash/覆盖/唯一根断言保持。实际 Script failed、仍在运行的 cell 和截断 JSON 都必须失败，不能推进 offset 或提交节点。三项检查 **3 passed，exit 0**；Ruff 通过。

在 `alice/` 的诊断入口如下。解释器、候选与 binary 均须显式选定，basetemp/report 必须使用新的自有路径，不覆盖其他人的失败证据。

```sh
ALICE_SUMMARY_DIAGNOSTICS=1 ALICE_ARTIFACT_PYTHON="$ALICE_CANDIDATE_PYTHON" \
ALICE_TEST_CODEX_BINARY="$ALICE_CODEX" PYTHONPATH=src \
"$ALICE_TEST_PYTHON" -m pytest tests/test_native_summary_partitions.py -m 'native and not live' -q \
  --basetemp="$ALICE_FRESH_TEMP" --junitxml="$ALICE_DIAGNOSTIC_REPORT"
PYTHONPATH=src "$ALICE_TEST_PYTHON" -m pytest tests/test_summary_native_output.py -q
```

原失败 JUnit SHA256：`709b30273572958a04c7da9578a9b7dc0bacb7ad22d8afbedcd41700dca70a13`；新诊断失败 JUnit SHA256：`80af2b29339c63861810dd9f7f2bd6bd9b365fd9aca52b4a871fd722ae0372fa`；私有 Service 失败旁路日志 SHA256：`745a5510e4bd91e8a45aaf526bbfda8e432cd13027fb98087138729fbd36a01e`。原始日志、fixture 副本和每文件 inventory 留在仓库外。

共享产品修复已交 Service/epoch 负责人。验收须验证：原生明确的子任务引用经公开 `thread/read` 核对父链后，观察归属到原 epoch 并持久化；未验证/外部线程仍不认领，旧 epoch 与新 epoch 不混写，既有 256/1 MiB 保护仍有效。修复后独立重验原 12 节点、2 个统筹 turn、服务重启、完整覆盖与唯一根断言，并核对每个子任务的 token 归属。该修复及最终组合候选目前仍待验。
