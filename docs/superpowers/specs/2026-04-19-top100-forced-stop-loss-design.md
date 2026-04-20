# Top100 Forced Stop Loss Design

**Goal**

在 `microcap_top100_mom16_biweekly_live.py` 主线 `v1.0 + costed` 中加入单笔强制止损执行层：当单笔持仓的累计 `costed net` 收益低于阈值时，于当日收盘平仓；止损后放弃当前旧信号，只有基础信号先经历至少一个 `cash` 空窗后，下一次新 `on` 信号才允许重新入场。

**Context**

- 当前主线的原始仓位轨迹来自 `run_signal()`，其底层调用 `analyze_microcap_zz1000_hedge.py::run_backtest()`，只生成 gross 持仓与收益。
- `v1.0` 的实用净值口径由 `scan_top100_momentum_costs.py::apply_cost_model()` 在 gross 结果上叠加股票端进出场与调仓成本。
- 用户要求的触发口径是 `costed net`，而不是 gross。
- 用户要求先做阈值扫描，范围固定为 `2% / 3% / 4% / 5%`，步长 `1%`。
- 本次工作默认线仍为 `v1.0`，不改 `v1.1 / v1.2`。

**Scope**

- 在主线脚本内增加一层“执行状态机”，输入为原始 gross 结果和换手表，输出为带强制止损后的执行结果与净值。
- 支持 `2% / 3% / 4% / 5%` 单笔止损阈值扫描。
- 将净值重建路径接到新执行层上，以便用真实主线数据验证。
- 增加单元测试覆盖止损触发、旧信号锁定、空仓期解锁和扫描输出。

**Non-Goals**

- 不重写底层 `analyze_microcap_zz1000_hedge.py::run_backtest()` 的原始信号逻辑。
- 不更改既有 Top100 成分股生成和实时查询快路径。
- 不把当前默认主线静默改成某一个固定止损阈值。

**Chosen Approach**

采用“后处理执行层”而不是“底层回测核直接改造”。

原因：

1. 止损触发口径是 `costed net`，必须同时看到 gross 收益、进出场成本、调仓成本和单笔状态。
2. 现有成本模型已经独立存在于 `scan_top100_momentum_costs.py`，复用这一层比把成本判断塞回底层 gross 回测核更干净。
3. 执行状态机可以明确地区分“原始基础信号”和“实际执行持仓”，从而精确实现“旧信号失效，必须经历一次基础空仓后才能再入场”。

**Execution Semantics**

1. 单笔定义
- 当执行持仓从 `cash` 切换到非 `cash` 时，开启一笔新交易。
- 单笔累计收益按该笔从入场日至当前日的 `costed net` 日收益连乘累计。

2. 触发条件
- 若某交易日在收盘后，该笔累计 `costed net` 收益 `<= -threshold`，则该日记为止损日。
- 止损日保留当日价差收益，并计入当日收盘卖出成本，因此实际实现亏损可能略超过阈值；这是预期行为，符合“按当日收盘价平仓”的执行口径。

3. 止损后的锁定规则
- 止损后，立即退出持仓，并设置 `blocked_until_signal_reset = True`。
- 当基础原始信号仍连续为 `on` 时，执行层必须维持 `cash`，不允许因旧信号继续持有或重新入场。
- 只有当基础原始信号先出现至少一个 `cash` 状态，才清除阻塞。
- 阻塞清除后，下一次基础信号从 `cash -> on` 的新入场机会才允许重新开仓。

4. 成本处理
- 正常持有日：沿用现有 gross spread return 与已有成本规则。
- 正常入场/离场：沿用 `ENTRY_COST` / `EXIT_COST`。
- 正常调仓日：沿用换手表上的 `two_side_cost_rate`。
- 止损离场日：除当日持有收益外，还应额外计入离场单边成本。
- 止损后的阻塞空仓日：不再计入持有收益或调仓成本。

**Code Design**

在 `microcap_top100_mom16_biweekly_live.py` 增加以下职责明确的函数：

- `apply_single_trade_forced_stop_loss(...)`
  - 输入：`gross_result`、`turnover_df`、`stop_loss_threshold`
  - 输出：带执行层字段的结果表，包括：
    - `base_holding`
    - `base_next_holding`
    - `executed_holding`
    - `executed_next_holding`
    - `trade_id`
    - `trade_return_net`
    - `forced_stop_triggered`
    - `signal_reset_seen`
    - `entry_exit_cost`
    - `rebalance_cost`
    - `total_cost`
    - `return_net`
    - `nav_net`

- `run_forced_stop_loss_scan(...)`
  - 对阈值 `0.02/0.03/0.04/0.05` 分别运行执行层。
  - 汇总输出各阈值的收益/回撤/信号次数/止损次数等对比表。

- `rebuild_costed_nav_from_proxy_turnover(...)`
  - 保持现有默认逻辑可用。
  - 增加显式入口，允许用 `stop_loss_threshold` 生成带止损的 costed NAV，供扫描和验证使用。

**Testing Requirements**

必须先写失败测试，再实现：

1. 当单笔累计 `costed net` 收益跌破阈值时，止损日标记为触发，并在后续执行层转为空仓。
2. 止损后如果基础信号持续 `on`，执行层不得重入。
3. 只有当基础信号先出现一次 `cash`，后续新的 `on` 才可重新入场。
4. 阈值扫描固定生成 `2% / 3% / 4% / 5%` 四档结果。

**Verification**

- 运行新增测试文件，确认 red -> green。
- 在真实本地数据上运行一次最小扫描命令，确认主线脚本能基于现有 `proxy_turnover` 生成带止损结果。

**Risks**

- 现有成本模型按“原始信号持仓”推断成本，接入执行层后必须完全改用“执行持仓”来判定进出场与调仓，否则止损后的成本会记错。
- 需要严守“基础信号 reset 后才解锁”，避免把连续 `on` 误判成新信号。
