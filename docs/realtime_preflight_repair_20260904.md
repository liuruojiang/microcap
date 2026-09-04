# 定时实时查询的跨日轻量预检

2026-09-04修复：昨天生成的独立历史刷新证明不能自动充当今天的证明；定时任务也不能因此调用完整refresh_state。

正式调用顺序（工作目录为本仓库）：

1. `python -X utf8 scripts/realtime_state_bundle.py preflight --max-anchor-age-days 5`
2. `python -X utf8 scripts/realtime_state_bundle.py validate --max-anchor-age-days 5`
3. 保持`TOP100_REALTIME_REQUIRE_STATE=1`，串行运行v2.0、v2.3、v2.5的`实时信号`。

preflight仅通过正式指数历史加载器确认最近已完成交易日，验证现有面板/代理/成本NAV一致和当前成员完整、当日报价名称无ST，再在状态文件及价格缓存哈希未改变时写当日证明。它不刷新面板/NAV，不调用全量重建。历史缺日、当前ST成员、缺报价或并发状态变动仍失败关闭；须另行修复状态。

validate仍为只读校验。两类命令不可混淆。定时任务本次保持原14:30时间。

回归包含独立目标不匹配拒绝、并发哈希变化拒绝、当日完整成员名称通过、旧日名称/当前ST拒绝。2026-09-04真实预检通过后，三个官方盘中入口均退出0，100/100当日报价。
