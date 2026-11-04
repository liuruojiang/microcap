# 2026-10-29 隔离诊断重放契约

Runner：`run_three_version_diagnostic_20261101.py`。仅语法验证通过，尚未运行数据重放，也未声明验收通过。

运行前由主 Agent 完成 `prepare_replay_code_20261101.py`，核对新 ST 安装/全池覆盖报告，确认 5438 证券均有目标日期所需实际证据。不得将未完成报告或自填 `approved` 标识作为通过凭据。已有 share 144 未裁决仍是诊断限制，不能因本 runner 运行完成晋级。

输入 contract JSON 必须包含 `as_of: "2026-10-29"`、`session_column`（默认 `date`）与 `required_evidence` 列表。每项为 `role,path,sha256`，路径使用绝对路径，角色至少有 `base_assembly`、`ratio_install`、`st_install`、`independent_sessions`。`independent_sessions` 必须是独立交易日历 CSV，覆盖模型开始至 10/29，不能用价格日期冒充。其他三项分别引用实际基础层安装清单、已核对的 ratio 安装报告及完成的 ST 安装报告；主 Agent 应审查报告语义，runner 校验精确字节哈希，不能自动替代人工/上游覆盖审定。

链路使用运行目录中实际函数：`base_mod.build_local_proxy_bundle` 全 5438 池构造目标成员、受限成交、收盘执行的收益与换手（单边成本参数由原函数保留）；`base_mod.rebuild_costed_nav_from_proxy_turnover` 完整重算底层成本 NAV；v2.0 按实际入口先 `run_signal`、`apply_momentum_gap_exit_buffer` 再 `apply_v2_0_execution`；v2.3/v2.5 使用各自真实 `build_*_result`，common index 与本次新算 v2.0 索引核对。未自行复写策略公式、禁用正常 overlay 或改成本。

网络连接在执行时拒绝，未调用 `generate_*`/当日刷新/迁移入口；源文件根目录须全部位于隔离 runtime。诊断产物只写 `runtime_20261029/diagnostic_replay_20261029`，已有该目录则拒绝覆盖。不会导入旧正式单 NAV，也不会构造批准股本基线。

执行方式：`python -X utf8 -B research_reports/20261029_st_history_repair/run_three_version_diagnostic_20261101.py --runtime <绝对runtime路径> --contract <绝对contract路径>` 只校验静态证据；增加 `--execute` 才启动重放。先完成 ST 证据，不要现在启动。缺数据/证据/模块将明确失败，不允许降级网络补齐。

完成后仍需核对实际成员、三个版本规则/成本、历史差异及迁移批准；这不是 `top100_delivery refresh-all/check` 或云端/邮件交付替代品。输出 `diagnostic_result.json` 记录三个最终流、proxy、turnover、base、members、panel 的行数与精确哈希；三个流和 panel 必须到 10/29。
