# Issue #2 代码与合成资料验收

本线负责 `memory.py`、`legacy.py` 及对应测试。`journal.py` 的既有补录契约经回归保留；线程发现与窗口调度由 #1 接线，CLI 与最终切换由 #3 集成。本文件说明兼容和复现方法，确切 commit、wheel hash、测试数量与资源测量附在关联 Draft PR。

## 完整归档与资源边界

- 原始文件按字节复制，manifest 保存路径、来源、长度、SHA-256、fingerprint 和增量变化；正文含认证相关词语不会整份排除，独立凭据文件仍排除。
- 复制按开始时的文件长度读取，块大小至多 1 MiB；来源增长、替换和截断都会触发复核。输入不能包含输出目录或指向本实例数据内部，默认白名单不会穿过符号链接父目录。
- 同一 `snapshot_id` 在档案发布、索引失败后可以恢复：验证原 manifest 与原档案，重做事务索引；不重新复制来源。未发布的 staging 失败会清理。若进程被 SIGKILL，遗留 `.snapshot-*` 不属于已发布快照，需在停写后单独核查清理。
- 未变文件先核对来源和前版档案 hash，直接建立硬链接；只为变更文件增加原始档案空间。不同内容的完整版本仍各自保留。硬链接要求档案位于同一文件系统，任何版本均不得就地编辑。
- 索引每条预览仍最多 65,536 字符。解析最多保留 1,048,576 字符，超限记录保留原始 source ID 和原档案，用既有 `parse_error=record_exceeds_parse_limit` 与 `indexed_truncated` 表明限制。超限不等于原始 JSON 已证实损坏。完整检索没有索引命中保证。
- `read_source` 按 65,536 字符块扫描，只保留请求范围；保留原有 Markdown record 0、其他文件行号、UTF-8 replacement 和换行归一化语义。原始字节须直接对档案 hash 核对。分页内存有界，但每次仍校验整个文件并扫描到目标记录，时间与文件大小相关。
- 超限摘要来源明确要求人工分区，不静默截断或忽略。1 MiB 是解析字符限额，复制使用字节限额；合法复杂 JSON 的瞬时 Python 对象开销仍与这个固定解析上限相关。

## 数据保留与恢复

- SQLite schema 仍为 1，表列、事件 ID、source ID 算法、目录布局和公共方法参数不变；既有精确 schema-0 索引仍通过验证后原地盖版本章，不删除 source rows。未知索引、档案、摘要 batch/intent 和显式旧计划版本拒绝。
- 既有 `last-seed.json` 记录最后完成的 seed。同 ID 重试只恢复尚未完成步骤，完成后的 seed 不再重建被用户删除的手记。新 seed 的快照祖先链必须包含最后完成的 seed；允许中间存在 `seed_workspace=False` 的仅归档快照，workspace 三方比较仍以最后实际 seed 为基线。回退旧 snapshot 或分叉会报冲突，原始档案仍保留。
- 新旧双方编辑或一方删除、另一方编辑均保留 workspace 与档案两侧内容并报告冲突。源删除不会自动删除新工作区内容。先仅归档、后第一次 seed 不会将缺失目标误判为用户删除。
- 摘要冻结校验来源集合和版本；提交保留旧摘要和手记。全坏来源允许提交逐项 `missing` 的显式缺口记录，不能作为已覆盖证据。正常已覆盖来源仍需要引用。
- pending intent 持久化后重新核对目标 hash；中断恢复只在目标仍等于 before 或 after 时完成，遇到新手记保留冲突。seed 在复制后同样复核目标。非协作外部 writer 仍存在检查与替换之间的短窗口，不能用这些检查代替停写和单 writer 隔离。
- 代码回退必须保留当前运行数据、所有新事件/手记/回执；不能把旧快照覆盖当前目录。旧代码能读取 schema 1，但不具备本次新增的行为保护，因此回退后不得继续执行旧版导入/摘要写操作，须由集成线先验证该候选的兼容性与隔离。

## 可复现测试

在 `alice/` 中用独立环境安装锁定依赖：

```sh
python -m pip install -c requirements.lock -e '.[dev]'
python -m pytest tests/test_memory.py tests/test_memory_bounds.py tests/test_memory_archive_acceptance.py tests/test_memory_summary_acceptance.py tests/test_legacy.py tests/test_journal.py -q
python -m pytest tests -m 'not native and not live and not artifact' -q
python tools/check.py --source . --release-home "$ALICE_TEST_RELEASE_HOME" --report "$ALICE_TEST_REPORT"
```

第三条入口构建并验证同一个 wheel：ruff、源码合成测试，以及通过已安装解释器执行真实 CLI/服务/MCP 入口的受控 fixture 测试。它未选择 native 或 live，不能标记为真实 Codex 或真实模型证据。原始报告留在仓库外。

记忆专门的安装产物验证：将上述六个测试文件复制到独立测试目录，在新的干净 venv 中安装同一 wheel 及锁定 pytest，移除 `PYTHONPATH`，使用该解释器的 `-I -m pytest` 运行。先确认导入的 `alice_codex.memory` 位于该 venv。大文件测试通过 JUnit properties 输出 `raw_bytes`、`index_bytes`、`peak_python_bytes`；进程峰值 RSS 需另外测量，不能把 tracemalloc 等同于全部进程资源。

合成资料覆盖完整原档、32 MiB 单行与 16 MiB Markdown、Unicode/损坏 JSONL、重复导入与索引中断、来源变化、跨版本引用、三方冲突、摘要冻结中断/并发/全坏来源和未来 schema 拒绝。

### 集成线的代码指针回退探针

`alice/tests/fixtures/memory_compat_probe.py` 只接收新建合成目录：创建 schema-1 资料、新事件与新手记，保存仅含 hash/相对路径的回执；检查阶段重开数据库、读取事件、拒绝旧 seed 并核对两份 hash。它不启动服务、不改变发布指针。

```sh
"$ALICE_CANDIDATE_A_PYTHON" -I "$ALICE_MEMORY_PROBE" prepare "$ALICE_SYNTHETIC_ROOT"
# 集成线将隔离服务的代码指针 A -> B；保持同一个 runtime 数据目录。
"$ALICE_CANDIDATE_B_PYTHON" -I "$ALICE_MEMORY_PROBE" check "$ALICE_SYNTHETIC_ROOT"
# 集成线再将隔离服务的代码指针 B -> A，重新运行原 A 的解释器。
"$ALICE_CANDIDATE_A_PYTHON" -I "$ALICE_MEMORY_PROBE" check "$ALICE_SYNTHETIC_ROOT"
```

未来 schema 发布门槛在合成 `runtime` 的另一份隔离副本中将 `memory-state/sources.sqlite3` 的 `PRAGMA user_version` 改为 99，再由发布线确认候选激活被拒绝、发布指针和数据 hash 未变；不要修改正在演练的数据或恢复旧快照。同 wheel 的两个候选只验证指针与数据保留，不能宣称跨版本代码迁移。

## 保持开放的验收项

真实私有资料最终停写与切换、全部明确来源覆盖、部署/回退到真实服务、真实模型能力、完整平台矩阵由集成负责人独立验收。`final=True` 仅检查采集窗口内观察到的变化，不证明 writer 已停止。`journal.backfill` 仅表示传入 owned IDs 的有限 canonical 扫描；不重建全部通知，不证明全部旧线程已被发现。快照和索引的内存仍随文件数/记录数元数据增长，本次资源样本不代表真实资料规模的全部容量上限。
