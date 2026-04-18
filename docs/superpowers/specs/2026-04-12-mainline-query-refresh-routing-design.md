# Mainline Query Refresh Routing Design

**Goal**

重构 `microcap_top100_mom16_biweekly_live.py` 的查询入口，在不牺牲“最新”和“准确”前提下，尽可能缩短以下命令的响应时间：

- `信号`
- `实时信号`
- `成分股名单`
- `进出名单`
- `实时进出名单`
- `净值图`
- `净值表现`

**Context**

- 当前主线脚本在 `main()` 中无论查询什么命令，都会先调用 `build_base_context()`。
- `build_base_context()` 会串行触发 `build_refreshed_panel_shadow()`、`ensure_strategy_files()`、`load_close_df()`、`run_signal()`，并在部分路径中进一步触发 `load_member_snapshot()`。
- 这导致原本只依赖 `costed_nav_csv` 的 `净值图` / `净值表现` 查询，也会经历完整的主线刷新和静态成员快照构建。
- 用户明确要求所有结果必须优先保证最新、准确，不能为了速度回退到旧结果；优化空间只来自“减少不必要的刷新与计算”，而不是降低数据新鲜度标准。
- 本次改造范围仅限当前主线脚本 `microcap_top100_mom16_biweekly_live.py`，不覆盖 `poe_bots/microcap_top100_poe_bot.py`。

**Approved Scope**

- 保留主线脚本单文件形态，不做跨文件重构。
- 将查询入口从“统一重链路”改为“按命令分流到最小必要刷新链路”。
- 将“刷新工件”和“输出查询结果”解耦。
- 继续要求所有命令优先刷新到最新交易锚点与最新可得实时行情。
- 保留现有输出文件命名与主口径：
  - 默认主线仍为 `v1.0`
  - 表现口径仍为 `v1.0 + costed`
- 保留现有 `实时` 查询短时缓存机制，但只允许在同一历史锚点和极短快照窗口内复用。

**Non-Goals**

- 不改动 `v1.1 / v1.2` 逻辑与入口。
- 不将主线脚本拆分为多个模块文件。
- 不引入“超时后返回旧结果”的降级逻辑。
- 不新增独立后台刷新进程或自动调度任务。
- 不改变已有策略口径、调仓规则、成本模型或实时信号判定逻辑。

**Design**

主线脚本新增一层“工件刷新路由”，把现有查询拆成两层：

1. 工件刷新层：只负责把某类查询依赖的数据刷新到最新。
2. 命令路由层：根据命令类型，仅调用最小必要的刷新函数，再生成或读取结果。

核心原则：

- 所有命令先刷新历史锚点。
- 之后只刷新该命令真正依赖的工件。
- 禁止 `净值图` / `净值表现` 这类查询为无关依赖触发成员快照。
- 禁止 `成分股名单` / `进出名单` 这类查询为无关依赖重建表现输出。
- 实时类命令允许复用同一历史锚点下、`realtime_cache_seconds` 窗口内的实时快照缓存，但复用条件必须严格校验。

**Architecture**

在 `microcap_top100_mom16_biweekly_live.py` 中引入以下概念函数，保持单文件实现：

- `refresh_history_anchor(args, paths) -> tuple[Path, pd.Timestamp]`
  - 仅封装并复用现有 `build_refreshed_panel_shadow()`。
  - 输出最新 `panel_shadow` 路径和最新历史交易锚点日期。

- `ensure_strategy_nav_fresh(args, paths, panel_path, target_end_date) -> None`
  - 仅保证 `index_csv`、`proxy_turnover`、`costed_nav_csv` 到 `target_end_date`。
  - 复用现有 `ensure_strategy_files()` 主逻辑。

- `ensure_base_signal_fresh(args, paths, panel_path, target_end_date) -> dict[str, object]`
  - 在 `ensure_strategy_nav_fresh()` 之后，计算闭市 `result`、`latest_signal`、`summary`。
  - 不主动加载成员快照。

- `ensure_static_members_fresh(args, paths, panel_path, target_end_date, base_context) -> dict[str, object]`
  - 在基线信号已准备好后，才校验/构建 `target_members` 与 `changes_df`。
  - 仅供 `成分股名单`、`进出名单`、`实时进出名单` 使用。

- `handle_performance_query_fast(args, paths, panel_path, target_end_date, query_text) -> None`
  - 在 `costed_nav_csv` 已刷新的前提下，直接从表现源切窗口并导出图表与摘要。
  - 不构造完整 `build_base_context()`。

- `build_query_context_for_command(args, query) -> dict[str, object]`
  - 新的总路由，根据命令类型决定调用哪组刷新函数。

**Command Routing Matrix**

每个命令的依赖工件与禁止刷新项如下：

- `信号`
  - 必需：
    - `panel_shadow`
    - `index_csv`
    - `costed_nav_csv`
    - 闭市 `result` / `latest_signal`
  - 禁止默认触发：
    - `load_member_snapshot()`
    - 表现图导出

- `实时信号`
  - 必需：
    - `panel_shadow`
    - `index_csv`
    - `costed_nav_csv`
    - 闭市 `result` / `latest_signal`
    - 实时行情快照
  - 允许：
    - 复用同一 `latest_anchor_trade_date` 下、短时窗口内的实时缓存
  - 禁止默认触发：
    - 静态成员快照构建
    - 表现导出

- `成分股名单`
  - 必需：
    - `panel_shadow`
    - `index_csv`
    - `costed_nav_csv`
    - 静态成员缓存
  - 禁止默认触发：
    - 实时行情
    - 表现导出

- `进出名单`
  - 必需：
    - `panel_shadow`
    - `index_csv`
    - `costed_nav_csv`
    - 静态成员缓存
  - 禁止默认触发：
    - 实时行情
    - 表现导出

- `实时进出名单`
  - 必需：
    - `panel_shadow`
    - `index_csv`
    - `costed_nav_csv`
    - 静态成员缓存
    - 实时行情快照
  - 允许：
    - 优先走现有 fast path，失败后回退完整实时路径
  - 禁止默认触发：
    - 表现导出

- `净值图`
  - 必需：
    - `panel_shadow`
    - `index_csv`
    - `costed_nav_csv`
  - 禁止默认触发：
    - `load_member_snapshot()`
    - `target_members` / `changes_df`
    - 实时行情

- `净值表现`
  - 必需：
    - `panel_shadow`
    - `index_csv`
    - `costed_nav_csv`
  - 禁止默认触发：
    - `load_member_snapshot()`
    - `target_members` / `changes_df`
    - 实时行情

**Consistency Rules**

为了保证“尽可能快”同时不牺牲最新性，缓存与复用必须遵守以下一致性规则：

- `panel_shadow`
  - 视为历史锚点真源。
  - 每次查询都先刷新。
  - 最新锚点日期由 `panel_shadow` 决定。

- `index_csv` / `proxy_turnover` / `costed_nav_csv`
  - 只要末日期小于 `target_end_date`，必须刷新或重建。
  - 若执行模型元数据不匹配，必须重建，不允许复用旧文件。

- 静态成员缓存
  - 必须绑定以下键：
    - `latest_rebalance`
    - `prev_rebalance`
    - `effective_rebalance`
    - `rebalance_effective_date`
    - `STATIC_CONTEXT_CACHE_VERSION`
  - 仅当这些键全部匹配时才允许复用。

- 实时缓存
  - 必须绑定以下键：
    - `latest_anchor_trade_date`
    - `snapshot_time`
    - 查询类型
  - 只有在同一历史锚点、且 `cache_age_seconds <= realtime_cache_seconds` 时才允许复用。
  - 不允许跨历史锚点复用实时结果。

- 表现导出文件
  - 始终视为派生物，不参与“是否最新”的判断。
  - 每次 `净值图` / `净值表现` 查询都可直接覆盖导出。

**Failure Handling**

本次设计不引入“快但可能旧”的降级。失败处理原则如下：

- 若历史锚点刷新失败：
  - 整个查询失败。
  - 不回退到旧 `panel_shadow` 结果冒充最新。

- 若 `index_csv` / `costed_nav_csv` 无法刷新到最新锚点：
  - 依赖它们的命令全部失败。
  - 不返回旧净值图、旧净值表现或旧信号。

- 若静态成员缓存构建失败：
  - `成分股名单` / `进出名单` / `实时进出名单` 失败。
  - `信号` 与 `净值图` / `净值表现` 不受影响。

- 若 fast realtime path 失败：
  - `实时信号`、`实时进出名单` 可自动回退到现有完整实时路径。
  - 若完整实时路径也失败，则命令失败。

- 若表现导出失败：
  - 只影响 `净值图` / `净值表现`。
  - 不影响 `信号` 或名单类命令。

**Expected Performance Impact**

本次重构预期提速来源如下：

- `净值图` / `净值表现`
  - 不再通过 `build_base_context()` 构造完整上下文。
  - 预期从“整条主线刷新 + 成员相关计算 + 导图”收缩为“刷新历史锚点 + 校验成本净值 + 导图”。

- `信号`
  - 默认不再为成员调仓摘要触发 `load_member_snapshot()`。

- `成分股名单` / `进出名单`
  - 静态成员结果与闭市信号计算解耦，可复用已确认有效的静态成员缓存。

- `实时信号` / `实时进出名单`
  - 保持最新和准确前提下，继续使用短时实时缓存减少同窗口重复抓取。

**Implementation Constraints**

- 只改 `microcap_top100_mom16_biweekly_live.py`。
- 优先复用现有函数，不重写策略逻辑。
- 保持命令字面量兼容现有用法。
- 保持输出文件路径与命名不变，避免破坏外部依赖。
- 允许新增少量内部辅助函数，但不做大规模风格性重构。

**Testing**

至少覆盖以下回归场景：

- `净值表现` 查询：
  - 会刷新到最新 `target_end_date`
  - 不调用成员快照构建
  - 仅依赖 `costed_nav_csv` 成功导出

- `净值图` 查询：
  - 会刷新到最新 `target_end_date`
  - 不调用成员快照构建
  - 输出 `performance_curve.png`

- `信号` 查询：
  - 不因默认查询而触发 `load_member_snapshot()`
  - 仍能输出最新闭市信号

- `成分股名单` / `进出名单`：
  - 当静态缓存命中时不重复重建
  - 当最新调仓日变化时会重建

- `实时信号` / `实时进出名单`：
  - 同锚点短时缓存可复用
  - 锚点变化后缓存失效
  - fast path 失败时能回退完整路径

- 端到端验证：
  - 七类命令仍然全部可用
  - 所有命令都优先要求最新锚点

**Risks**

- 单文件继续增长，内部边界若处理不好，后续维护成本仍会偏高。
- 若命令路由判断不完整，容易出现“该快的不够快”或“该刷的没刷到位”。
- 现有 `build_base_context()` 被弱化后，必须补足测试，否则容易遗漏旧分支行为差异。

**Recommendation**

按以下顺序实施最稳妥：

1. 先新增命令分流函数与最小依赖刷新函数。
2. 先为 `净值图` / `净值表现` 拆出快路径并补测试。
3. 再为 `信号` 去掉默认成员快照依赖。
4. 最后再整理 `成分股名单` / `进出名单` / `实时进出名单` 的静态缓存分流。
