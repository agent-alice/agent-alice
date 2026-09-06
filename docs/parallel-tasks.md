# 并行开发 Alice

Issue 记录任务与验收，Codex 任务执行开发，PR 提供待评审的改动。创建 Issue 或 PR 不会自动启动开发任务。

## 共同起点

先把源码基线合入 `main`，再让所有任务从同一版本建立独立 worktree。基线仍待评审时，可以明确指定同一源码基线分支，但不能从只有项目说明的旧 `main` 开始实现。

在 Codex 桌面应用中添加本地仓库目录。新建任务时选择 **Worktree**，选定包含源码的起始分支，然后提交下面对应的任务说明。三个任务并行启动，每个任务分别提交自己的 PR。集成任务负责共享接口、完整测试和合入。

## 分工

表中的 Python 模块位于 `alice/src/alice_codex/`，`tools/`、打包及测试路径相对于 `alice/`。共享集成测试由集成负责人协调。

| 工作线 | Issue | 主要文件归属 | 交付 |
|---|---|---|---|
| 调度与恢复 | [#1](https://github.com/agent-alice/agent-alice/issues/1) | `service.py`、`codex.py`、`rpc.py`、`control.py`、`scheduler.py`、`calendar.py`、`store.py` 及对应测试 | 持久派发、打断、故障恢复的修复与证据 |
| 记忆与档案 | [#2](https://github.com/agent-alice/agent-alice/issues/2) | `memory.py`、`journal.py`、`legacy.py`、身份与分层摘要通用模板及对应测试 | 完整性、来源、增量和回退契约的修复与证据 |
| 观察与学习 | [#4](https://github.com/agent-alice/agent-alice/issues/4) | `business.py`、`collector.py`、`resources.py`、技能模板及对应测试 | 可执行判定器、缺口检测、纠错回归及未见变体 |
| 集成与发布 | [#3](https://github.com/agent-alice/agent-alice/issues/3) | `config.py`、`cli.py`、`mcp.py`、`releases.py`、`launchd.py`、`tools/`、打包、CI 和集成测试 | 公共接口接线、实际安装产物验证、合入与发布门槛 |

模块源码位于 `src/alice_codex/`。归属是协调约定，不代表不需要修改相邻接口：需要跨范围改动时，先把所需接口和调用示例交给集成负责人，再由约定的所有者修改。独立 worktree 隔离文件编辑，不隔离数据库、账户或运行进程；每个任务必须使用自己的临时测试数据。

## 可以直接用于新任务的说明

### 任务一：调度与恢复

```text
完成 https://github.com/agent-alice/agent-alice/issues/1。
先阅读 AGENTS.md、alice/AGENTS.md、alice/specs/development.md 和 docs/parallel-tasks.md。
在当前独立 worktree 创建自己的分支，先检查已有实现和 PR，列出 Issue 验收条件的真实缺口，避免重写已验证功能。
按分工修改调度与恢复模块。共享 CLI、配置和 MCP 接口的修改先交给集成负责人协调。
使用隔离数据验证重复请求、跨窗口派发、原生打断、暂停持久化和崩溃恢复；区分合成测试与真实 Codex 证据。
提交实现及必要测试，推送分支并创建关联 Issue 的 Draft PR。交付确切 commit、测试命令、结果、依赖和剩余限制。由集成负责人统一合入与部署。
```

### 任务二：记忆与档案

```text
完成 https://github.com/agent-alice/agent-alice/issues/2 的代码和合成资料验收。
先阅读 AGENTS.md、alice/AGENTS.md、alice/specs/development.md 和 docs/parallel-tasks.md。
在当前独立 worktree 创建自己的分支，核对现有迁移、摘要与档案实现，只补缺口。
按分工修改记忆模块；持久 schema、事件 ID 和公共接口发生变化时先与集成负责人协调。
用合成数据验证完整日志保存、大文件边界、来源变更、摘要提交和新旧数据冲突。真实私有资料的最终切换由集成负责人单独执行，本任务不得据合成测试关闭该验收项。
提交实现及必要测试，推送分支并创建关联 Issue 的 Draft PR。交付确切 commit、测试命令、结果、兼容说明和剩余限制。由集成负责人统一合入与部署。
```

### 任务三：观察与学习

```text
完成 https://github.com/agent-alice/agent-alice/issues/4。
先阅读 AGENTS.md、alice/AGENTS.md、alice/specs/development.md 和 docs/parallel-tasks.md。
在当前独立 worktree 创建自己的分支，核对现有观察契约、采集器、资源账本和学习评测，只补缺口。
按分工开发，通过合成网站/API 和可执行判定器验证缺页、缺字段、权限不足、矛盾来源，以及纠正后的未见变体。
原生 token 只记录用量观察，金额凭证另行记账；虚拟预算保持禁用。真实模型评测需有明确费用授权并使用隔离凭据；不得自动向真实网站发布内容。
提交实现及必要测试，推送分支并创建关联 Issue 的 Draft PR。交付确切 commit、评测命令、样本与失败数、消耗和剩余限制。由集成负责人统一合入与部署。
```

## PR 之后

集成负责人检查范围、接口与实际证据，按依赖顺序合入。某个 PR 合入后，尚未完成的分支更新到新的共同起点并处理冲突。测试改动涉及的组合行为，最终在干净环境验证同一个安装产物。代码合入、生产切换和能力验收是不同状态，不能互相替代。

如果是在继续现有 PR，将任务首句替换为“继续修复此 PR”，同时指定其现有分支和待解决的评审意见；继续向原 PR 推送提交，不另建重复 PR。
