# v2.3 / v2.5 脚本数学对抗审查

本子任务已修复 v2.5 的 WLS 零阈值数值误差、价格校验后未归一化，以及异常收益/成本/NAV未失败关闭的问题。最新真实官方样本 3988 日的新旧执行路径、成本、净收益和 NAV 完全一致。结论属于本地脚本修复与诊断验证，未部署、未发邮件、未实际交易。

## 范围及数据门槛

- 审查范围按用户纠正限定为 v2.0/v2.3/v2.5；本子任务只编辑 `microcap_top100_mom16_biweekly_live_v2_5.py`，未修改 v2.6。
- 重点完整阅读 v2.5 的 `validate_close_df`、`microcap_nav`、`exp_weights`、`log_wls_score_and_r2`、有效日期/官方日期覆盖检查、`build_microcap_log_wls_gross`、`apply_cost`、成本保留校验、`apply_no_target_vol`、`build_v2_5_result`；同时审阅当前停用的 target-vol、融资、scale-change 成本接口及正式输出/摘要路径，明确正式 v2.5 不调用该 target-vol 接口。
- 独立核对 v2.3 的 always-on spread NAV、25 日/半衰期 2.5 的 WLS、1.0x 信号对冲/0.8x 执行对冲、退出缓冲、vol10 热度退出及恢复的真实函数，查看 v2.0 嵌入成本模型 `map_rebalance_apply_costs`、`apply_cost_model` 与正式执行分派。v2.3 修复由另一子 Agent 独占实施。
- 主 Agent 在 pristine source 上先完成 `scripts/top100_delivery.py refresh-all` 与 `check`，见 `pre_refresh_all.log`、`pre_delivery_check.json`。最新完整收盘交易日为 **2026-09-30**，国庆闭市期间未冒用 2026-10-07 为交易日。
- 真实复算前再次读回：panel 8738 行、proxy 4067 行、base costed NAV 4050 行、正式 v2.0 NAV 4033 行、v2.3/v2.5 各 3988 行，均截至 2026-09-30；turnover 432 行，最后调仓日 2026-09-17，覆盖最近实际调仓。
- 发现普通 `*_base_nav.csv` 导出仍停于 2026-09-15，**未使用该过期导出**。真实复算通过正式 context 使用的 `base_mod.load_close_df(panel_shadow, proxy_index)`、`base_mod.run_signal` 与 v2.3/v2.5 的真实构建函数读取同批已验证数据，内存复算并与正式 costed CSV 独立比对。

## 可复现缺陷及修复

### 1. 常量窗口出现极小正斜率，零阈值继续持仓

纯合成 diagnostic：80 日价格先从 100 上涨至 150（25 日），然后恒定 55 日。旧 v2.5 的末尾 WLS score 为 `1.490069103414721e-30`，当前与下一持仓均保持 long。数学上完整恒定窗口斜率应为精确 0，正式 EXIT_THRESHOLD=0 应退出。

根因是大截距的加权均值与中心化存在浮点残差。修复为每个 log NAV 窗口先减最新值，再使用原有斜率和 R² 公式。这是截距平移，代数定义不变；没有增加阈值 epsilon、调整半衰期或交易参数。修后常量 score/R² 精确为 0，价格横盘尾已回到 cash。

真实官流最大 score 变化 `1.7763568394002505e-15`，零阈值符号变化 0；最小绝对 score `0.000551671463022938`，真实样本没有绝对 score 小于 `1e-10` 的行。

### 2. numeric-string 价格通过验证后运算失败

旧 `validate_close_df` 用临时数值表验证，却未返回归一化后的价格；`microcap_nav` 对原始字符串做除法，抛 `TypeError: unsupported operand type(s) for /: 'str' and 'str'`。

修复为返回独立数值副本，NAV及 gross 构建采用归一化副本，不修改调用方原表。合成与真实最新样本均验证文本/浮点价格产生相同 holding、净收益、成本和 NAV。

### 3. 坏收益/成本被填零或继续累乘

旧 no-target 路径将 NaN/非法文本净收益填零，保留 ±inf，允许负成本和大于等于 100% 的成本继续输出。合成输入产生过 `return_net=[0, inf]`、`nav_net=[1, inf]`，并可能丢掉真实交易成本。

修复为 gross `return`、costed `return_net`、`total_cost`、`overlay_pre_cost_return` 必须有限；成本需处于 `[0,1)`。异常报告含首个故障字段、日期和值，不静默修补坏收益。

### 4. NAV异常、破产与复利溢出未被拦截

旧 WLS 将 0/负数/NaN/inf NAV 变成 NaN 并跳过窗口；旧 no-target 路径接受 `return_net=-1/-1.01`，以及累计复利溢出到 inf 或下溢到 0。

修复为 log-WLS NAV 与派生 microcap/costed NAV 必须有限且严格为正，日净收益需大于 -1。两日 `1e200` 净收益及 300 日 -99% 的异常复利输入均失败关闭。

### 5. 持仓、gross 与成本后净收益缺一致性检查

旧函数可接受现金日非零市场 gross，以及少扣入场费的净收益。修复在正式固定 1x 无 target-vol 分支核对 `return_net=(1+overlay_pre_cost_return)*(1-total_cost)-1`，现金 holding 的 pre-cost return 必须为零。

允许现金行存在合法入场/退出成本，保留既有 close / next-open 的记账语义。没有把 cash 行所有净收益强制清零，也未消除合法入场费。

## 验证证据

- `math_before_tests.log`：第一组旧源 23 FAIL / 6 PASS，保存了常量尾、字符串价格、坏 gross/净收益/成本的真实失败输出。
- `math_domain_before_tests.log`：第二组域反例旧逻辑 12 FAIL，包含 NAV 非正/非有限、破产、累计 overflow/underflow、漏扣成本和现金 gross。
- `math_domain_final_tests.log`：本任务 41 项诊断反例与既有 v2.5 scan/promotion 回归共 **71 PASS**。
- `math_real_parity.py` / `math_real_parity.json`：可复现、只读真实官流诊断复算。七个实际源/官方 CSV 的 SHA256 在前后完全相同，无官方输出改写。
- v2.3 与 v2.5 各 3988 日（2010-05-05—2026-09-30）新旧 `holding`、`next_holding`、`return_net`、`nav_net`、`current_execution_scale`、`total_cost`、`overlay_pre_cost_return` 逐点完全一致。
- v2.5 新流与官方 CSV 最大净收益差 `9.974659986866641e-17`、最大 NAV 差 `1.4210854715202004e-14`，属于 CSV 回读精度；所有字段在 `1e-12` 容差内一致。
- 独立 WLS 用带截距的加权 `numpy.linalg.lstsq` 方程求解，没有复用源码的中心化表达式。真实 v2.5 最大 score/R² 误差分别 `6.52811138479592e-14` / `3.164135620181696e-14`；v2.3 分别 `1.715294573045867e-14` / `4.75175454539567e-14`。
- 独立合成逐日 ledger 同时覆盖 close 与明确 next-open execution_date、持仓滞后一日、入场/退出/名单调仓成本、净收益乘法与复利；prefix 测试验证后续价格不会改变早期 holding、净收益或成本。
- 实时/信号子 Agent 对最终 v2.5 修复独立注入破产、两日 `1e308` 溢出、成本算式矛盾、现金 gross、常量尾、字符串价格，整套独立检查 **63 PASS**，见 `signal_independent_final.log`。
- 本子 Agent 对 v2.3 最终源（SHA256 `dab988798bd1e262f2b8f738620b72794a38a79c55fa4d7c017b770c618a250a`）再独立只读攻击 11 个情形：-100%/-101% 活跃收益、NaN/inf、两日复利溢出、300 日下溢、4 种坏 mapped 成本均被拒绝；不可执行 cash 行的市场收益未计入净收益。结果 PASS，见 `math_cross_review_v23.json`。
- 只读复核 v2.3 信号残留及失效清理补丁：最新自身热度值覆盖继承参考，已停用风险字段明确清空；当前 summary 不会掩盖旧版 realtime CSV；清理拿 realtime lock 并再次核对身份，能保留同时更新的当前版本 writer。未发现本轮可复现的新回归。v2.3 最终域补丁落地后重跑本任务真实 parity，结果继续 PASS，官方产物哈希不变。

命令均用 `python -B -X utf8`，pytest 用 `-p no:cacheprovider`，测试临时根限定在本任务目录，未新增策略缓存。

## 回退与边界

- 恢复备份：`.codex_backups/20261007_143135/microcap_top100_mom16_biweekly_live_v2_5.py`。原文件 SHA256 `b5b90ed35dc06427050c6ba0b56535a7b9ace62242d3d8e9f139f25b754d9acc`，已亲自核对备份与改前源一致；备份 manifest 共 42 文件由主 Agent 验证。
- v2.5 正式 LOOKBACK=20、HALFLIFE=3、ENTRY/EXIT=0、无对冲、无 target-vol、无现金日收益、无融资均保持原值。停用的 target-vol research API 未晋级或启用。
- 这是脚本/接口正确性审查，不等价于重新认证 PIT 成分池、ST 公告时点、企业行为权利、股票逐笔成交可执行性、资金/保证金账户或完整历史收益真实性。未在本任务发布新的收益/回撤表，也未进行参数扫描。
- 官方历史重写保护未放宽。正常官流执行路径已证实不变；后续若其他修复涉及历史交易路径改变，仍需执行原有 exact-hash 迁移与二次无迁移审计。
