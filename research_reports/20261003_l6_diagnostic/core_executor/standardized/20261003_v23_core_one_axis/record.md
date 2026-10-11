# Quant Parameter Scan Record — L6 v2.3 核心单轴

## Run Metadata

- 项目：Microcap Top100；版本：v2.3；组：`L6_CORE_ONE_AXIS`。
- 此目录由 `quant-param-scan` initializer 创建，标准表**从已完成的真实同批核心重放归一化**。归一化没有重跑策略。Git、时间与初始工作区状态见 `scan_meta.json` 和 `command_log.txt`。
- Source-change rule：`research_only_no_source_change`。未修改正式策略、生产配置或 `outputs/`；旧工作区修改原样保留。

## Research Question

- 在同一冻结旧输入和当前正式完整路径下，检验 WLS lookback、half-life、入场分数、退出缓冲和旧 R² 门槛的局部邻域及旧记录候选。
- 基准：现行正式 25/2.5/entry0/exit.08/R² OFF/信号对冲1/执行对冲.8/过热 10 日 26%/20%。
- 候选：lookback 24/26/27/40，half-life 2.25/2.75/3.0，entry −.01/+.01，exit .07/.09/0，R² ON .08（窗口25）；加正式基线共 14 路。各候选只改本组字段，不把 R² OFF 称为整个动量 OFF。
- 成功标准是旧纸面模型重放的公式一致与反例可检；**正式历史准确性和生产晋级不在本组可认证范围内**。

## Implementation Anchor

- 正式入口：`microcap_top100_mom16_biweekly_live_v2_3.py`；同批执行者：`../../run_core.py`；原始结果：`../../scan_summary.csv`、`../../daily/*.csv.gz`、`../../meta.json`、`../../parity.csv`。
- 核心执行者调用正式 loader、`build_v2_3_common_index`、`build_v2_3_result`、`summarize_required_windows`。当前正式分支对冻结官方 v2.3 逐日净收益、NAV、成本、状态 parity 通过；最大净值差 `3.55e-15`。
- 归一化脚本 `normalize_existing.py` 逐日读取 14 条旧输入真实重放结果：年化和最大回撤从原始表百分数转小数，并从逐日收益独立核到 `1e-12`；年化波动用 `std(ddof=1) * sqrt(244)`，Sharpe 用年化收益除以年化波动。未补造行情或候选收益。原表和逐日文件 SHA 在 `scan_meta.json.source_hashes`。

## Data Snapshot

- 旧公开原价重建 Top100 代理及中证1000现货指数收盘，2010-05-05 至 2026-09-30，共 3,988 个收益日。panel 8,738 行、proxy index 4,067 行、base costed NAV 4,050 行、v2.0 4,033 行、v2.3 3,988 行；成员换仓是 432 条事件，末事件 09-17。各输入路径、日期及 SHA 见元数据。
- Asia/Shanghai A 股旧纸面交易日历；历史休市伪行、PIT/ST、公司行为和未获批准的正式血缘迁移仍阻断正式结果。2010 年以来的指数空头是理论代理，不是 IM 真实历史。无独立新样本外数据。
- 本次转换读取原始结果，没有读写市场数据缓存或正式产物；仅写本目录标准表和元数据。

## Cost and Execution Assumptions

- 旧纸面模型按成员换仓事件计费，进场/离场各扣 0.3%，理论执行指数空头 0.8 倍，日拖累 `0.0003 × 0.8`。背景信号使用 1.0 倍价差；目标波动、现金收益、融资 OFF。
- 旧收盘状态及时钟保留；未建 L5 共同现金与真实 IM 合约结算/保证金/整手/换月。`cost_total` 是**逐日费率和**，不是货币费用或初始本金累计成本；真实资金占用 N/A。

## Runtime Override Plan

- 在原始 `run_core.py` 中用 context manager 临时改一个参数并恢复，所有候选读取同一 9 月 30 日冻结输入；转换脚本没有参数覆盖或策略调用。
- 正式基线在同批，14 条路径均有 Full、10Y、5Y、3Y、1Y。持续/离散阈值无唯一自然 OFF；exit=0 是无缓冲对照，R² OFF 是正式设置。无择时 always-on 属另一模型，不能假冒这些单轴的 OFF。
- 旧 lookback 27/40、half-life 3.0 是旧记录观察点；即便旧纸面收益较高，也不自动成为正式候选。

## Commands

```powershell
python -X utf8 research_reports/20261003_l6_diagnostic/core_executor/run_core.py
python -X utf8 D:/Codex/home/skills/quant-param-scan/scripts/init_quant_param_scan_run.py --root research_reports/20261003_l6_diagnostic/core_executor/standardized --project microcap_top100 --strategy v2.3 --subsystem core_parameters --parameter-group L6_CORE_ONE_AXIS --repo . --entrypoint microcap_top100_mom16_biweekly_live_v2_3.py --date 2026-10-03 --slug v23_core_one_axis
python -X utf8 research_reports/20261003_l6_diagnostic/core_executor/standardized/20261003_v23_core_one_axis/normalize_existing.py
python -X utf8 D:/Codex/home/skills/quant-param-scan/scripts/finalize_quant_param_scan_run.py research_reports/20261003_l6_diagnostic/core_executor/standardized/20261003_v23_core_one_axis --decision keep_default --stability-label diagnostic_mixed
python -X utf8 D:/Codex/home/skills/quant-param-scan/scripts/check_quant_param_scan_artifacts.py --phase complete --strict research_reports/20261003_l6_diagnostic/core_executor/standardized/20261003_v23_core_one_axis
```

## Output Files

- `scan_summary.csv`：14 候选 × 5 窗口，标准 long 表，百分数已转为小数。
- `window_metrics.csv`：14 候选，每窗口年化收益、波动、Sharpe、回撤。
- `scan_meta.json`：源文件精确哈希、正式参数、数据和成本、转换说明、决策。
- `command_log.txt`：初始化、转换、finalize 命令。
- `normalize_existing.py`：只转换现有结果，可重放并对照原始表和逐日收益。

## Full-Sample Results

旧输入诊断正式元组年化 23.2634%、最大回撤 −12.9243%。lookback 27 为 23.9599%/−12.8431%，40 为 23.7361%/−11.7155%；half-life 3.0 为 24.3783%/−15.7360%。这些仅是已见旧输入纸面值，正式可认证绩效 **N/A**；不能以较高旧样本内年化替换正式参数。完整五窗口和差额见 CSV。

## Window Results

lookback 40 最近一年仅 3.7839%/−11.5250%，half-life 3.0 最近一年 −1.5303%/−15.7360%，显著弱于正式 6.4539%/−9.2678%；局部峰值与近年稳健性不一致。旧 R² ON .08 全样本 21.5632%/−15.9475%。所有窗口详见标准表及原始表。

## Stability Classification

- Label：`diagnostic_mixed`。
- 小邻域表现并非全对称稳定：lookback 24 最近一年变差，half-life 2.75/3.0 出现近年负收益和更深回撤；exit .09 亦放大近年回撤。旧观察点不证明可晋级。
- 费用与名义资本仍是旧比例模型；PIT/ST、日历、权益和 L5 资金路径变化后需整组重算。

## Decision

- `keep_default`：保持现行正式参数，不进行生产变更。此决策仅是当前旧输入 L6 局部诊断，不是 L6 整层 PASS 或历史准确性认证。
- 过热/对冲轴的同批结果及独立对抗审计已完成，整层裁决见 `../../../L6_DECISION.md`；上游 L1/L2/L5 门槛未闭合，正式五窗口收益及最大回撤均为 N/A。

## User-Facing Summary

当前核心参数在旧纸面模型上公式对齐，局部邻域与旧观察值已核；若以旧记录的单一全样本收益选点，会忽略最近一年反向表现。保持正式参数，待真实数据/账户与全层审计完成后再谈正式结论。

## Finalization

- Finalized at: 2026-10-03T12:11:08+08:00
- Decision: keep_default
- Stability label: diagnostic_mixed
- Complete checker: PASS
