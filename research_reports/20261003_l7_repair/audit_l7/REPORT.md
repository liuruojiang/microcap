# L7 独立对抗审计：交付脚本与 PR #68（2026-10-03）

**结论：PR #68 暂不宜合并。** 其新增的参数身份守卫可覆盖既有 8 个漂移点，但发现一个生成端缺陷和一个交付门缺陷：收盘当天发生成员变动时，三版正式入口均把只到当天的历史 NAV 日期传给执行日推导函数，使下一交易日不可得并把本应执行的动作标为 `actionable=False`；交付门又接受这一漏报，也接受 `actionable=True` 且执行日为 `2099-01-01`。这属于会影响成员交易指令的脚本缺陷，不代表历史 PIT/ST 或资金账户已获认证。

## 独立反例（已运行）

**生成端：** `microcap_top100_mom16_biweekly_live_v2_0.py` 的 `augment_close_confirmed_signal_with_member_contract` 从 `official_index` 取 `> latest_rebalance` 的首个日期作为执行日。三版正式生成入口均传入各自 `out.index`，它的最后一天就是本次已完成的收盘日。在临时样本中，2026-09-03 为最新收盘日及当日再平衡日、成员 1 入 1 出，真实函数用截至 09-03 的历史 index 返回：`required=true, actionable=false, execution_date=''`。该反例已运行；仓库现有 `test_real_core_next_session_member_contract_is_accepted` 人为给 index 添加 09-04 后才得到 `actionable=true`，没有覆盖生产当日只有历史日期的情形。

使用仓库已有 `tests/test_top100_delivery.py` 的临时工作区样本，保留两个再平衡日各 100 个唯一代码；把最新一日最后一个代码 `000100` 改为 `000101`，形成真实 1 入 1 出。随后在 v2.0/v2.3/v2.5 三份最终 CSV 中共同写入 `required=True`、`official=True`、来源日 `2026-09-03`、进入/退出均为 1，其他身份/净值字段不变。

| 注入 | `inspect_outputs(root, '2026-09-03')` 实际返回 | 风险 |
| --- | --- | --- |
| `actionable=True`、`execution_date=2099-01-01` | `ok=True, errors=[]` | 错误执行日期可进入完整交付快照。 |
| `actionable=False`、`execution_date=2026-09-04` | `ok=True, errors=[]` | 当日应执行的名单变动可被最终 CSV 漏报。 |

反例仅写 Python `TemporaryDirectory`，没有改正式文件或仓库测试。原因为 [`scripts/top100_delivery.py`](../../../scripts/top100_delivery.py) 的 `canonical_member_rebalance` 只派生来源日、数量和是否需要变更；`inspect_outputs` 第 301–309 行只在 CSV 自报 `actionable=True` 后检查执行日存在且晚于信号日，没有验证**应有** actionable 值，也没有验证执行日为下一正式交易日。`tests/test_adversarial_microcap_delivery.py` 当前只测试空、过去、同日执行日期；缺少远期执行日与漏报两种反例。

## 修复建议与合并条件

1. 修复生成端：从独立正式交易日历推导当次收盘后的第一个交易日；不要用只到当前收盘的 NAV `out.index` 预测未来交易日。三版入口共用该规则，并在日历不可用时关门。
2. 从已验证的成员血缘和 `expected` 日期推导应有 `member_rebalance_actionable`。来源日等于 `expected` 且名单有变化时，三版最终 CSV 不得自报不可执行；来源日早于 `expected` 时，不得自报当前可执行。
3. 将执行日同**首个下一正式交易日**核对，不能只要求 `execution > expected`。核对 `member_rebalance_official` 的适用语义，并避免把历史执行日期渲染为今日交易。
4. 增加“只有截至当日的历史 NAV index”以及两个交付门变异测试，分别证明旧实现误判、新实现拒绝；保留 2026-09-30 三版真实最终 CSV 的只读通过校验，运行交付/成员动作回归。之后再合并 PR、刷新主工作区、更新远端自动化固定提交并运行无邮件验证。

## 其他审阅边界

- `verify_release` 只比对三份策略源码和 frozen-tail authority；`scripts/top100_delivery.py`、`scripts/realtime_state_bundle.py`、`scripts/top100_cloud_delivery.py` 等交付校验代码不在远端逐字节校验集合。它们若在本机与远端出现差异，`verify_release` 本身不会报错。这是可追踪的发布身份缺口；本次 PR 合并后的整组主工作区验证应核对这些脚本与远端策略提交的内容身份。是否将其纳入硬门，可在不改变业务数据的前提下补强。
- `top100_cloud_delivery.pack` 使用 `git rev-parse HEAD` 作为 `release_sha`，没有独立验证调用者工作树干净、来源文件与该 HEAD 相同；在非官方干净 checkout 中调用时可能误标源码身份。正式云端流程若始终使用干净固定 checkout，此项风险较低，但包装函数本身不是独立签名。可通过 dirty-worktree 反例补测。
- 本审计不重新评价 L1/L2 的历史存活、ST 进入退出、权益调整和交易日历，也不把 L5 资金缺口记为脚本已修复。PR 中的年化与 ST 限价代码已由此前隔离预演证明 NAV 不改写；仍不能据此发表正式历史收益。
