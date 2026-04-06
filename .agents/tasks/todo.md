# Task: Design Claude Log Upgrade Implementation Roadmap (No Time Estimates)

## Plan
- [x] Reuse completed drift analysis and supplement design docs as baseline
- [x] Define phased implementation sequence (P0->P2)
- [x] For each phase, specify goals, module scope, acceptance criteria, and rollback strategy
- [x] Add cross-phase validation checklist and recommended execution order
- [x] Save roadmap under `.agents/plans/`

## Review
- Implementable roadmap delivered with 6 phases:
  - Phase 0: Baseline guardrails
  - Phase 1: Dedup/accounting fix
  - Phase 2: Agent/Subagent attribution
  - Phase 3: New event/error signal integration
  - Phase 4: UI/CLI exposure and policy convergence
  - Phase 5: rollout governance and migration communication
- File:
  - `.agents/plans/2026-03-27-claude-log-upgrade-implementation-roadmap.md`
- User requirement satisfied: no effort/time estimate included.

---

# Task: MiniMax 最近几天消息数异常调研（design handover）

## Plan
- [x] 审查 `Messages` 链路定义，确认是 raw 行数、dedup 后 entry 数，还是别的概念
- [x] 用最近 7 天真实日志做一轮初步调研，判断 MiniMax 高消息数是否来自重复快照或 subagent
- [x] 在 `.agents/handover/` 输出调研设计、任务拆解和 handover prompt
- [x] 在 `.agents/research/` 固化初步发现，避免后续重复采样

## Review
- 已确认当前默认 dedupe 为 `message-id-max`，不是 legacy `message_id + request_id`。
- 已确认原始日志层存在大量同一 `message.id` 的多快照 assistant 记录，但当前 daily 报表并不是直接按 raw 行数统计，否则数值会再高很多。
- 已确认当前 `Messages` 更接近“dedup 后 usage entries 数”，且默认包含 subagent；MiniMax 高消息日的 sidechain 占比很高，这是当前最值得验证的方向。
- 当前判断：
  - 更像“统计口径/命名 bug”或“默认包含 subagent 导致认知偏差”
  - 不像已经坐实的 reader 去重失效 bug
- 新增产物：
  - `.agents/research/2026-03-31-minimax-message-anomaly-initial-findings.md`
  - `.agents/handover/design-minimax-message-anomaly.md`
  - `.agents/handover/tasks-minimax-message-anomaly.md`
  - `.agents/handover/handover-20260331-132959.md`

---

# History: Analyze Claude Code Log Changes (Agent/Subagent) and Produce Design Plan

## Plan
- [x] Inspect existing parsing/aggregation pipeline and data models
- [x] Sample real `~/.claude/projects` logs to detect schema drift
- [x] Validate whether Agent/Subagent structures are present and how they are stored
- [x] Quantify mismatch risk (especially token dedupe and overcount risk)
- [x] Write detailed gap analysis notes in `.agents/research/`
- [x] Write detailed supplement design plan in `.agents/plans/` (analysis/design only, no implementation)

## Notes
- 重点风险是去重口径过时：当前逻辑依赖 `request_id`，但现代日志多缺失该字段。
- Agent 相关结构已普遍存在：`isSidechain`, `agentId`, `subagents/*.jsonl`, `progress.agent_progress`。

## Review
- 完成了代码链路与真实日志样本对照分析。
- 识别了必须补齐的差异项：新事件类型、Agent 归因、去重策略升级、限流信号扩展。
- 输出文档：
  - `.agents/research/2026-03-27-claude-log-drift-analysis.md`
  - `.agents/plans/2026-03-27-claude-log-agent-subagent-design-plan.md`
- 按用户要求，本次仅做分析与设计，不做代码实现。

---

# Task: 报表页面增加按模型消息数量追踪（design handover）

## Plan
- [x] 审查报表与聚合现状，确认消息计数字段是否已存在
- [x] 输出最小改动设计文档（仅扩展 table 报表视图）
- [x] 输出执行任务拆解与验收标准
- [x] 生成可直接执行的 handover prompt（含 Available/Referenced Documents）

## Review
- 已确认聚合层已有消息计数数据（`entries_count` 与 `model_breakdowns[*].count`），本次重点是 UI 展示接入。
- 已新增 handover 产物：
  - `.agents/handover/design.md`
  - `.agents/handover/tasks.md`
  - `.agents/handover/handover-20260330-160500.md`
- 交付策略：保持最小改动，不变更 reader/aggregator 计量口径，仅扩展报表列与测试。

---

# Task: 报表过滤持久化策略修正（design handover）

## Plan
- [x] 审查过滤持久化的真实实现与 daily/monthly 受影响路径
- [x] 明确最小改动策略、回退策略和验收标准
- [x] 在 `.agents/handover/` 生成设计、任务拆解和执行 handover

## Review
- 已根据用户修正更新约束：任何视图都不应持久化过滤器，不给 realtime 保留特例。
- 已确认当前真正持久化并影响行为的核心字段是 `model_filter`，持久化入口在 `LastUsedParams.save()`，全局合并入口在 `Settings.load_with_last_used()`。
- `last_days/start_date/end_date` 当前已是单次运行参数，不写入 `last_used.json`；本次仍需在 handover 中明确“所有过滤字段永不持久化”的策略，避免后续实现扩大范围或引入特例。
- 已新增 handover 产物：
  - `.agents/handover/design-report-filter-persistence.md`
  - `.agents/handover/tasks-report-filter-persistence.md`
  - `.agents/handover/handover-20260330-223009.md`
- 交付策略：所有过滤器仅对当前命令生效，并对旧配置中的脏过滤值做防御性清理。

---

# Task: 报表模式去除 Ctrl+C 终止依赖（design handover）

## Plan
- [x] 审查 daily/monthly 报表模式与 realtime 模式的实际退出路径
- [x] 确认根因是否为报表模式误用 live 阻塞等待
- [x] 输出最小改动设计文档、任务拆解与验收标准
- [x] 生成可直接执行的 handover prompt（含 Available/Referenced Documents）

## Review
- 已确认根因在 `src/claude_monitor/cli/main.py` 的 `_run_table_view(...)`：报表渲染完成后仍打印 `Press Ctrl+C to exit` 并执行 `signal.pause()` / 无限 `sleep`。
- 已确认这不是 realtime 刷新逻辑泄漏到 table UI，而是报表路径自身收尾逻辑错误。
- 已新增 handover 产物：
  - `.agents/handover/design-report-mode-no-ctrl-c.md`
  - `.agents/handover/tasks-report-mode-no-ctrl-c.md`
  - `.agents/handover/handover-20260331-093157.md`
- 交付策略：仅修正 `daily/monthly` 的一次性退出语义，不扩大到 realtime/session 生命周期重构。

---

# Task: monthly 报表按模型改造方案设计（对齐 daily，取消模型截断）

## Plan
- [x] 审查 `daily` 与 `monthly` 表格渲染链路差异，定位截断与分模型开关
- [x] 明确三项改造目标：模型列表不截断、每行按模型分列展示、Total 行按模型 breakdown
- [x] 产出最小改动实施方案（先设计，不改聚合口径）
- [x] 补充测试覆盖点与回归验证清单

## Review
- 已确认根因：
  - `monthly` 没有启用 `include_model_analysis=True`，导致未按模型拆分各列。
  - 模型名截断逻辑位于 `_format_models`（超过 3 个模型时显示 `...and N more`）。
  - Total 行已支持按模型 breakdown，但依赖 `model_breakdowns` 的完整性。
- 实施方向：
  - 统一 `daily/monthly` 的数据行渲染路径，均启用分模型分析。
  - 去除模型展示截断，保证每行显示所有模型。
  - 保留当前聚合口径（`entries_count` / `model_breakdowns[*].count`），仅做 UI 展示改造。

---

# Task: monthly 报表按模型改造实现（对齐 daily，不新增 weekly）

## Plan
- [x] 记录实现范围与验收项（仅 daily/monthly，保持统计口径不变）
- [x] 修改展示层：移除模型截断，统一 daily/monthly 分模型渲染路径
- [x] 更新测试：覆盖 monthly 分模型展示与模型不截断行为
- [x] 运行回归测试并修正失败项
- [x] 回填 Review（变更说明 + 验证结果）

## Review
- 已完成展示层改造：
  - `create_monthly_table(...)` 现在启用 `include_model_analysis=True`，与 `daily` 走同一分模型渲染路径。
  - `_format_models(...)` 已移除长列表截断，展示所有模型。
- 已完成测试更新：
  - 修正 `daily/monthly` 表结构断言（包含 `Messages` 列，总列数 9）。
  - 新增 `test_format_models_no_truncation`，覆盖 4+ 模型不截断。
  - 新增 `test_create_monthly_table_includes_per_model_analysis`，覆盖 monthly 分模型展示。
  - 修正 daily 分模型断言的列索引（Messages/Input/Output/Cost）。
- 验证结果：
  - `uv run pytest --no-cov src/tests/test_table_views.py -q` 通过
  - `uv run pytest --no-cov src/tests/test_aggregator.py -q` 通过
  - `uv run pytest --no-cov src/tests/test_cli_main.py -q` 通过
  - 说明：直接运行（带 coverage）会触发全局 `fail-under=70` 门槛，不代表本次改动用例失败。

---

# Task: Realtime burn rate unit change to tokens per second

## Plan
- [x] Identify the realtime display path and convert the displayed burn rate from tokens/min to tokens/sec
- [x] Update the velocity indicator thresholds so emoji categories stay aligned with the new display unit
- [x] Add regression tests for active and empty realtime session screens
- [x] Run targeted tests and record the result in this file

## Review
- Realtime burn rate now renders as tokens/sec in both active and empty session screens.
- Velocity indicators now use per-second thresholds, so the emoji still reflects the displayed unit.
- Added regression coverage in `src/tests/test_session_display.py` for the active screen and the no-active-session screen.
- Verification:
  - `uv run pytest --no-cov src/tests/test_session_display.py src/tests/test_display_controller.py -q`
  - Result: passed
