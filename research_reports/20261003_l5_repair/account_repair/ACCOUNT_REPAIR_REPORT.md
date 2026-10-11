# L5 共同现金账户修复：隔离诊断

**结果：已消除“股票现金不足时，理论互换债务仍悬空”的静默通过路径；四条正式 v2.3 信号绑定的纸面账户均在严格共同现金合同下 fail closed，不能形成 241 日完整收益。** 没有覆盖 L2/L5 旧账、正式 `outputs/` 或生产策略。

## 合同和实现

- 从 L2 的 `version_integration/strategy_replay.py` 复制账户到本目录的 `common_cash_replay.py`。唯一实质账务改动：在每日权益事件入账后、T+1 收盘股票订单前，将上一日 0.8 倍理论指数空头的当日盈亏及每日 0.03% 拖累划入**同一股票现金**；划拨后现金为负即停止，不给予信用额度。`prefunded` 仍禁止用同日卖款买入，`netted` 仍允许；成交价、0.3% 每侧费、T/T+1、逐股权益事件和冻结数量逻辑保持原样。
- 四条输入分别为 `signal_audit/{prefunded,prefunded_off,netted,netted_off}` 中由旧正式 v2.3 ON / 基础信号 OFF 绑定的 CSV。运行前逐一核对 `binding_result.json` 中的 SHA，再复制并读回。旧 L5 分支代理信号仅用于历史缺陷反例，未用于新四账。
- 模型仍是**零初始占款的理论指数互换**，没有期货保证金率、期货合约乘数/展期、经纪商授信、成交回执或融资。它不是 IM 实盘期货账户，也不是可执行业绩认证。

| 新账户 | 最后完成结算日 / 行数 | 首个阻断日 | 股票订单前共同现金缺口 |
| --- | --- | --- | ---: |
| `prefunded` ON | 2026-01-20 / 72 | 2026-01-21 | ¥3,440.20 |
| `prefunded` OFF | 2026-01-20 / 72 | 2026-01-21 | ¥3,440.20 |
| `netted` ON | 2025-10-23 / 11 | 2025-10-24 | ¥5,417.85 |
| `netted` OFF | 2025-10-23 / 11 | 2025-10-24 | ¥5,417.85 |

阻断发生在 ON/OFF 模块首次差异之前，不能据此计算模块收益差。此前旧分支代理账显示的 2026-02-27 `netted` ¥4,449.32 缺口是**旧路径**上的必要现金覆盖失败；资金划拨改变买单接受、持仓及以后对冲名义额，因此不是新路径的首缺口。

## 反例和复现

`strict_variation_guard.py` 在四条旧账中对 `cash + swap_margin` 做逐日上界现金覆盖门槛；`settlement_path_check.py` 按旧逐笔订单和执行价/费，在划拨后重放到**首个订单分歧即停止**。旧账的首分歧为 `prefunded` 2025-11-14、`netted` 2025-12-01：原账因现金拒绝的买单，在共同现金下会获准。脚本中实际运行的**含 0.3% 每侧费**合成例：起始现金 ¥100，变动额支出 ¥40，同日卖出 ¥50 后，旧日终 `cash + swap_margin = ¥19.58` 虽为正，`prefunded` 的买单含费需 ¥90.27，但划拨后买入预算只有 ¥60，因此应拒。若忽略手续费，该例对应日终 ¥20、买单 ¥90；此处均指同一个时点缺陷。两项旧分支代理检查仅为反例，新账结论以正式绑定信号的 `common_cash_result.json` 为准。

复现命令：

```powershell
python -X utf8 research_reports/20261003_l5_repair/account_repair/strict_variation_guard.py
python -X utf8 research_reports/20261003_l5_repair/account_repair/settlement_path_check.py
python -X utf8 research_reports/20261003_l5_repair/account_repair/run_common_cash_cases.py
```

输入 SHA、源码 SHA、行情补查响应 SHA、四账 daily/holdings/orders/rights SHA 与阻断原因在 `guard_result.json`、`settlement_result.json`、`common_cash_result.json`。旧 L5 241 日 ON/OFF 业绩不再可用于共同现金合同；正式 Full/10Y/5Y/3Y/1Y 全部 N/A。要重算完整近期账户，需另定可验证的资金来源或会在划拨前限制股票买入/对冲规模的规则，并重新逐日运行；不以负现金或旧后续订单补齐。
