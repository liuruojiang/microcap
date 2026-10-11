# L6 v2.3 核心 14 路源码与时间轴复核（2026-10-03）

**条件裁决：旧纸面模型内的源码入口、单轴参数、同源同费及一行目标持仓滞后通过；可执行 T+1 与真实对冲仍未获证。** 审计只读 `core_executor/run_core.py`、`meta.json`、`parity.csv`、14 份 `daily/*.csv.gz`、70 行 `scan_summary.csv`，并对照正式 `microcap_top100_mom16_biweekly_live_v2_3.py`；未重跑、未修改核心流。

## 源码与参数

- 正式 v2.3 是 LB25、半衰期 2.5、入场阈值 0、退出缓冲 .08、`R2_ENTRY_GATE=0`（OFF）、信号价差空头 1.0、执行指数空头 .8、过热 vol10/.26/.20。`run_core.py` 读取同一冻结 panel、指数、turnover、v2.0/v2.3 NAV，通过正式 `_close_df_from_base`、`build_v2_3_common_index`、`build_v2_3_result`、`summarize_required_windows` 生成路径；`meta.json.source_and_input_sha256` 与 L4 封存指纹相同。形式基线 3,988 行的净收益、NAV、费用、WLS、R²、过热特征及状态对旧官方流的检查见 `parity.csv`，最大净收益差 `9.99e-17`、NAV 差 `3.55e-15`、状态零错行。
- 运行器每次用 `override` 仅设置声明的 LOOKBACK、HALFLIFE、入场阈值、退出缓冲或 R² 门槛；`try/finally` 恢复。由于正式函数的 Python 默认参数在定义时固定，运行器对 `log_wls_score_and_r2` 与 `exp_weights` 做局部包装，使运行期 LOOKBACK/HALFLIFE 实际传入正式计算，而不是只改变元数据。逐日读回核查：14 路每条的 `lookback`、`halflife`、`entry_threshold`、`momentum_gap_exit_buffer`、`r2_window`、`r2_entry_gate`、信号/执行对冲比例及三个过热参数均恰好等于正式基线加该路唯一声明改动；其余列不变。各路同为 2010-05-05 至 2026-09-30、3,988 行。
- 额外旧点 `lookback_27/40`、`halflife_3p00`、`exit_0p00` 与邻域同批，不从旧报告拼接；`r2_on_0p08` 是唯一 R² ON，`r2_window` 仍为正式 25，其他 13 路 R² OFF。LOOKBACK 轴固定 R² 窗口 25，符合“只改 LOOKBACK”的口径；入场/退出/R² 轴的外生 WLS 分数和过热波动特征由运行器与基线逐日核对。过热模块、理论对冲费率和 turnover 成本始终启用。

## 信号、费用和收益时点

- 正式 `build_spread_log_wls_gross` 在 T 日收盘价计算 `spread_nav` 与 WLS，依据分数和 R² 门槛得到 T 日 `next_holding`；`current_active = signal_on.shift(1)`，把 T−1 的目标持仓用于 T 日理论指数价差收益。`apply_overheat_defense` 再用截至 T 日的滚动 10 日价差波动决定 T 日目标及过热退出/恢复，当前持仓决定 T 日已有仓位收益，T 日收盘扣目标改变产生的入退费用。参数变化没有引入显式未来行。
- 我独立读取 14 条压缩日流核查 `holding[T+1] == next_holding[T]`，总错行 0；所有 `holding=cash` 且 `next_holding=cash` 的 `return_net` 非零行 0；所有路径的日期完全相同。正式基线中 `r2_entry_gate=0`、信号对冲 1.0、执行对冲 .8；ON 路门槛 .08。单轴持仓变化来自相同收盘输入与费用函数，而非跨不同旧扫描的指标拼接。
- 上述“一行滞后”是**纸面日流的状态关系**。收盘特征确定当日目标并在同一收盘行扣换仓成本，不证明已提前下单、T+1 可成交、限价/停牌可成交、真实期货结算或共同现金账户；不能据此把 L5 资金缺口抹掉或把本诊断称为正式可执行回测。

## 发现与限制

- 初查时 `core_executor/CORE_EXECUTOR_REPORT.md` 曾写“9 个候选/10 路/50 行”，与最终 **14 路/70 行**不符；核心执行者已更新该报告并列明四个补测旧点。最终扫描覆盖以 `meta.json`、逐日流和 70 行结果表为准。
- 本审计的源码和时间轴核对通过，仅适用于 L4 封存的旧成员池与理论指数对冲。正式 Full、10Y、5Y、3Y、1Y 可认证业绩仍为 N/A；历史 PIT/ST、公司行为、休市伪行、可执行价及资金结算没有在本层修复。
