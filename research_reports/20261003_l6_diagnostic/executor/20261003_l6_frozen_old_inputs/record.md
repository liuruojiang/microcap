# L6 v2.3 过热参数局部稳健性：冻结旧输入诊断

## Run Metadata

- Run id: `20261003_l6_frozen_old_inputs`；日期 2026-10-03；Top100 v2.3；参数组 `feature_window_trigger_recovery`。
- Git 基点 `7dc9d934f7f24fade5236247db71ec4fd7f18471`；启动前已有 L1/L2 源码、输出及报告未提交改动，见 `scan_meta.json.git_status_before`。本次只新建本目录。
- 标签：旧成员池、理论指数对冲、同收盘纸面成交的隔离诊断。正式可认证业绩 N/A。

## Research Question

当前 v2.3 过热参数为价差 NAV 年化波动率窗口 10 日、触发 26%、恢复 20%。冻结 LB25、半衰期 2.5、R² OFF、退出缓冲 0.08、信号对冲 1.0、执行对冲 0.8、成本、数据和时钟，扫描窗口 9/10/11、触发 25.5/26/26.5%、恢复 19.5/20/20.5% 的 27 格（含正式基线）；另列 27/21% 旧组合与关停过热 OFF，共 29 条。无已授权的生产晋级门槛；不会以样本内最高年化自动改规则。上游数据或执行合同变化须同源重跑。

## Implementation Anchor

- 正式入口：`microcap_top100_mom16_biweekly_live_v2_3.py`。正式函数路径：`v2_0.embedded_context._load_embedded_base_context()` → `_close_df_from_base` → `build_v2_3_common_index` → `build_spread_log_wls_gross` → `apply_overheat_defense` → `summarize_required_windows`。
- 运行器 `run_scan.py` 仅临时修改四个过热参数并在 `finally` 中恢复；OFF 用无限大触发哨兵关闭过热。当前基线对冻结官方 3,988 行净收益、净值、三项费用、WLS 分数、过热特征及八个状态列逐日 parity，数值容差 `1e-12`；OFF 对正式 `apply_cost(gross, turnover)` 逐日 parity。

## Data Snapshot

- L4 `executor/meta.json` 封存的源码及数据 SHA 是输入门槛；运行前、中、后核验，详情见本次 `scan_meta.json`。panel shadow 8,738 行、proxy index 4,067 行、proxy turnover 432 行、base costed NAV 4,050 行、v2.0 NAV 4,033 行、官方 v2.3 NAV 3,988 行，最新行情日 2026-09-30（turnover 最新事件 09-17）。每条本次日流 3,988 行，2010-05-05 至 2026-09-30。
- `mnt_strategy_data_cn.csv` seed 仅至 2026-03-18，未直接用于策略；实际输入为封存 refreshed panel。旧 A 股日期索引由正式函数构造，中国时区收盘序列。原历史 PIT/ST、权息、休市伪行未在本层纠正。
- 嵌入式 loader 执行联网指数校验：Eastmoney 连接失败，Sina legacy 成功，2026-02-06 至 09-30、157 个重合日，最大精度差 0.005；没有改变输入 SHA，复现有环境依赖。

## Cost and Execution Assumptions

- 旧正式费率：入场单边 `0.003`、退出单边 `0.003`，再平衡费由 turnover 映射；净收益为 `(1+realized_return)*(1-total_cost)-1`，244 交易日年化。过热收盘特征确定当日 `next_holding` 和纸面收盘费，实际 `holding` 次行生效。
- 无真实 T+1 成交、限价涨跌停、实股数、IM 期货张数与统一现金/保证金账户；价差空头仅 CSI1000 指数理论腿。L5 资金缺口按用户指令不阻断本次条件研究，但正式业绩仍 N/A。目标波动、现金收益、融资均 OFF。

## Runtime Override Plan

- `run_scan.py` 的 `try/finally` 恢复四个过热参数，其他参数不变。正式基线是 27 网格的一格，OFF 和旧 27/21% 为额外参照。
- `daily/*.csv.gz` 含每候选逐日价差、特征、状态、净收益、净值、费用和目标尺度；路径与 SHA 在 `scan_meta.json.daily_files`。
- 未主动写 cache；loader 可能联网校验。原工作树 dirty，隔离诊断不触发生产迁移或刷新。

## Commands

```powershell
python -X utf8 D:\Codex\home\skills\quant-param-scan\scripts\init_quant_param_scan_run.py --root research_reports/20261003_l6_diagnostic/executor --project 'microcap Top100' --strategy v2.3 --subsystem overheat_defense --parameter-group feature_window_trigger_recovery --repo 'D:\动量策略\微盘股对冲策略' --entrypoint microcap_top100_mom16_biweekly_live_v2_3.py --date 20261003 --slug l6_frozen_old_inputs
python -X utf8 research_reports/20261003_l6_diagnostic/executor/20261003_l6_frozen_old_inputs/run_scan.py
python -X utf8 D:\Codex\home\skills\quant-param-scan\scripts\finalize_quant_param_scan_run.py research_reports/20261003_l6_diagnostic/executor/20261003_l6_frozen_old_inputs --decision keep_default --stability-label mixed_window_tradeoff
python -X utf8 D:\Codex\home\skills\quant-param-scan\scripts\check_quant_param_scan_artifacts.py --phase complete --strict research_reports/20261003_l6_diagnostic/executor/20261003_l6_frozen_old_inputs
```

## Output Files

- `scan_summary.csv`：29 × 5 窗口长表；`window_metrics.csv`：29 条宽表。收益、波动、回撤用小数比例（`0.23` = 23%）；Sharpe 按正式函数 `annual / annualized_vol`。
- `daily/*.csv.gz`：逐日日流；`scan_meta.json`：输入/源码/输出 SHA、参数、成本、parity；`command_log.txt`：命令轨迹。

## Full-Sample Results

仅旧流条件算术：正式基线年化 23.2634%、最大回撤 -12.9243%；OFF 23.8119%、-16.2935%。邻域 `w9_t265_r195` 年化 23.5787%、最大回撤 -12.9243%，年化比基线高约 0.315 个百分点。完整 29 行见 `window_metrics.csv`。

## Window Results

正式基线 10Y/5Y/3Y/1Y 年化依次 19.3918/23.0953/17.4010/6.4539%，回撤 -12.9243/-11.7155/-9.2678/-9.2678%。`w9_t265_r195` 的对应年化为 19.8937/24.4801/19.5330/15.0818%，回撤 -12.9243/-11.7155/-9.2594/-7.5885%；最近一年差距较大，不能凭尾段选参数。完整窗口见 `scan_summary.csv`。

## Stability Classification

- Label: `mixed_window_tradeoff`。邻近格子的旧流路径有相同也有明显分叉，窗口、触发、恢复间有状态路径依赖。OFF 在旧流 Full 年化稍高、回撤恶化约 3.37 个百分点。历史池污染和真实账户缺口比局部参数差异更重要。

## Decision

- Decision: `keep_default`。正式 v2.3 10 日/26%/20% 不变，候选只作研究；正式 Full/10Y/5Y/3Y/1Y 年化及回撤全部 N/A。
- 独立对抗审计与整层裁决由主 Agent 汇总；L6 后停下等用户确认。

## User-Facing Summary

同源旧输入 27 格邻域已跑，基线对旧官方 3,988 行逐日一致。局部候选旧流较好，但不构成正式参数或历史准确性结论。

## Finalization

- Finalized at: 2026-10-03T12:03:27+08:00
- Decision: keep_default
- Stability label: mixed_window_tradeoff
- Complete checker: PASS
