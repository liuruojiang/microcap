# L7 回放、续接与隔离恢复执行检查（2026-10-03）

**冻结旧代理尾段的批量／增量／重复执行一致性 PASS；今日正式整组交付门禁未通过。** 本项仅验证已保存的旧纸面状态能否按现有代码可重复续接，不认证历史成员、真实成交或正式收益。全部写入只发生在 `research_reports/20261003_l7_diagnostic/replay_executor/` 与自动清理的临时目录；未执行正式 `refresh-all`、历史迁移、部署或邮件发送。

## 真实数据上的隔离回放

- [隔离执行脚本](run_isolated_incremental.py)读取正式已保存代理、100 只有效成员、换手记录、面板和本地价格缓存，将 2026-09-17 收盘的已执行组合及代理指数复制到临时目录。只对临时副本调用正式 `try_extend_proxy_index_without_rebalance`；价格缓存刷新入口在该进程内关闭，以冻结现有缓存。一次续至 09-30、先续至 09-23 再续至 09-30 均返回成功。第二次对已到 09-30 的副本重复同日调用返回 `False`，副本哈希不变。8 个新交易日的 `daily_return`、`close` 与正式保存代理均逐值零差；[机器结果](isolated_incremental.json)。
- [独立尾段算术](audit_tail.py)不调用上述正式尾段计算函数，从 100 只执行成员的真实价格缓存独立重算 09-18 至 09-30。100 份均使用 raw close，adjusted 缓存数为 0；8 日最大单日收益绝对差 `6.94e-17`，批量终值最大差 `5.82e-11`，分批最大差 `8.73e-11`。这是**旧 raw 价格口径的算术复演**，不是权益调整或成交可行性认证；逐日数值见 [CSV](tail_replay.csv)，哈希及来源计数见 [JSON](tail_replay.json)。
- 隔离脚本核对六个正式输入文件的运行前后原始 SHA-256 未变。旧代理在 09-17 以后没有新的再平衡事件；有效成员文件恰为 100 只、日期 09-17。分批恢复保留已执行组合，不重新按目标名单排名。

## 故障注入与已有防线

运行以下项目原有测试，第一次合计 **187 passed**，全在临时测试目录中执行；主 agent 加严最终身份校验后，再复跑交付与状态一致性子集 **122 passed**：

```powershell
python -X utf8 -m pytest -q tests/test_frozen_member_continuation_20260917.py tests/test_effective_member_state_transport_20260917.py tests/test_top100_delivery.py tests/test_adversarial_delivery_state_consistency.py
python -X utf8 -m pytest -q tests/test_reference_summary_delivery_20260917.py tests/test_realtime_state_guard_20260915.py tests/test_realtime_member_action_contract.py
python -X utf8 -m pytest -q tests/test_top100_delivery.py tests/test_adversarial_delivery_state_consistency.py
```

覆盖已执行种子缺失／重复／无价格、桥接日重复换仓、收盘换仓对下一日收益的影响、持仓缓存运输、缺件打包失败、整组版本缺失、最终 CSV 版本身份和持仓比例、同日内容变化、刷新中共享输入变化、并行刷新锁、过期日报摘要和成员行动字段。测试结果只说明相关代码行为，不能替代真实云端恢复或邮件交付。

## 正式门禁与限制

本地旧输出读回到 2026-09-30：panel 8,738 日行、代理 4,067 日行、基础 costed NAV 4,050 日行、v2.0 4,033 日行、v2.3/v2.5 各 3,988 日行；turnover 432 个事件，最后事件 09-17。主 agent 加严最终身份校验后，重新只读运行 `top100_delivery.inspect_outputs(root, '2026-09-30')`：16 个流、三个版本最终 CSV 身份和持仓一致性均通过、错误 0。然而 `validate_manifest` 仍失败：清单所记三份策略源码的内容哈希与当前本地源码均不同，尽管清单原状态为 `complete`。[最终读回结果](delivery_readback.json)保留各流行数和最终 CSV 的版本、修订号、成员行动日期。旧 manifest 不可自动重签。

另外，只读 `realtime_state_bundle.validate_state(root,max_anchor_age_days=None)` 发现独立刷新证明 `verified_on=2026-10-02`，当前日期为 2026-10-03，门禁报陈旧；三个日频状态锚仍为 2026-09-30，换手为事件日 09-17。没有利用旧证明续签或直接改 manifest。正式 `refresh-all` 会改写整个工作区的历史输出，且先要求远端源码一致；此项 L7 未执行，历史 lineage 的精确哈希迁移闸保持有效。

以上 PASS 仅限冻结旧代理的无再平衡续接与局部恢复；不将它升级为 whole-workspace delivery PASS，也不作为 v2.0/v2.3/v2.5 真实可执行收益证据。
