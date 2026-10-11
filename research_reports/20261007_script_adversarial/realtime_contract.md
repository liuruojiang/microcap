# v2.3 / v2.5 信号与脚本契约对抗审查

审查日期：2026-10-07。修复状态：v2.3 本地脚本修复与诊断反例通过；历史执行路径未改变。v2.5 由数学 Agent 编辑，本 Agent 仅独立读审和攻击。未修改正式参数、生产配置、邮件或外部部署。

## 范围与新鲜度

已静态审查 v2.3/v2.5 的依赖版本门槛、CLI 输出路径、log-WLS、执行 `holding` / `next_holding`、成本适配、过热风控、官流索引、内存缓存、历史重写审计、收盘输出暂存/提升、实时构建、元数据合并、最终 CSV 身份与清理。追溯 v2.0 内嵌 `load_realtime_base`、`_build_signal_row`、`run_realtime_query_with_fresh_state`、实时锚点/报价可执行性、名单动作契约和收盘名单动作契约。

先完成静态审查，主 Agent 执行 `scripts/top100_delivery.py refresh-all` 和独立 `check`，两项成功后才执行诊断。证据：`pre_refresh_all.log`、`pre_delivery_check.json`。

最新已完成交易日为 2026-09-30。读回 panel 8,738 行、proxy index 4,067 行、base costed 4,050 行、v2.0 官流 4,033 行、v2.3/v2.5 官流各 3,988 行，日频终点一致；turnover 432 行，末次成分事件 2026-09-17，与成员事件一致。turnover 为事件表，不伪填到日频终点。

可恢复备份：`.codex_backups/20261007_143135/manifest.json`。修复前 v2.3 源与备份原始 SHA256 均为 `ce49dee8cc9d42f63ff4cca4f3bd7744c1331dfa62b54974756af1d0f63345aa`。

## 发现并修复的真实缺陷

| 缺陷 | 旧逻辑反例 | 修复与影响 |
|---|---|---|
| v2.3 退役实时文件被 summary 认证并无条件保护 | summary 被判定当前，realtime CSV 仍为退役 revision / target-vol 开启；`incompatible_v2_3_outputs` 返回空列表，清理又跳过该文件 | 独立检查实时 CSV 的单行、版本、revision、所有有效核心参数/开关；仅保护当前身份文件，清理时取得实时写锁再次验证，保护并发新写者 |
| v2.0 风控数值污染 v2.3 最终信号 | reference summary 的 `overheat_metric=999` 与 v2.3 原生 feature `0.21` 并存；feature 为 NaN 时还能保留参考旧值 | 活跃风控值无条件取本版结果；`overheat_metric` 与原生 feature 同源，缺失保持 NaN；禁用 peak/decay、参考版单笔收益/旧波动率等数值清空，reset 状态关闭 |
| log-WLS 恒定窗口出现非零浮点斜率 | 80 日恒定 NAV=1.5/2.0 的理论零斜率变为约 -2.32e-30 / -4.64e-30，R2 可有伪值 | 每窗 log NAV 减窗内最后值，代数平移后保持原方程；恒定窗口 score/R2 精确为零，不新增 epsilon 或改变阈值 |
| 非有限/破产信号 NAV 被默默转成无效窗口 | 正价格 hedge 100→230 可导致 1×信号 spread 收益 ≤-1；非法 NAV 为 0/负数/NaN/inf 时 log-WLS 静默输出缺失 | 派生 spread 收益、累计 NAV 和 log-WLS 输入必须有限且保持正 NAV，异常指出首个日期并拒绝继续；防静默删样本 |
| 过热执行函数接受破产、非法成本和累计 NAV 崩溃 | active return=-1/-1.1、映射成本为负/≥1/NaN/inf、连续 1e308 收益溢出、400 次 -90% 收益下溢，旧执行函数全部继续 | 对实际 active 收益、映射/总成本、return_net 及累计 NAV 校验数值域；cash 上的诊断收益仍不作为实际负债，现金实际收益保持零 |

实现集中于 `microcap_top100_mom16_biweekly_live_v2_3.py`；回归与独立攻击集中于 `tests/test_script_signal_adversarial.py`。身份兼容检查只证明版本身份，不替代当天实时快照与独立交易日锚点门槛。

## 反例与验证

合成数据仅用于脚本故障注入，不是策略表现。正式参数与成本记账时点保留。

- 旧源码运行新反例：**14 failed / 1 passed**。故障在修改源码前留证，见 `realtime_contract_old_red.log`。
- v2.3 修复后的初次反例：**44 passed**，见 `realtime_contract_new_green.log`。覆盖所有核心身份字段的缺失/矛盾、损坏/多行 CSV、加锁清理与并发重写、毒化参考数值、4 种持仓投影、严格 NAV 域、独立加权最小二乘与缩放不变性。
- 相关现有套件：**132 passed / 1 failed**，见 `realtime_contract_regression.log`。失败 `test_tracked_frozen_tail_authority_matches_post_rebalance_seed`：HEAD 权威记录 9/17 原始 seed hashes，但 HEAD 三个文件已是 9/30 tail；该测试/权威文件本 Agent 未改动，由主 Agent 与数据 Agent 核实。
- 独立检查交付动作新门槛：v2.0/v2.3/v2.5 原生信号函数的 cash→cash、cash→active、active→cash、active→active 共 **12 passed**；未观察到正常固定 1 倍转换被误拒，见 `signal_delivery_action_gate.log`。
- 独立攻击数学 Agent 的最终 v2.5 修复：account return=-1/-1.1、累计溢出、净收益/成本公式矛盾、cash gross 泄漏、恒定/先涨后平坦尾部退出及数字字符串规范化均通过；连同 v2.3 与交付正常动作，初版完整新套件 **63 passed**，见 `signal_independent_final.log`。
- 追加 v2.3 过热数值域反例在修复前 **9 failed / 1 passed**，见 `v23_overheat_domain_old_red.log`。最终本文件新测试 **73 passed**；联同两份现有 v2.3 对抗套件 **89 passed**，见 `signal_contract_complete.log`。cash 行诊断值 `[-1.1, NaN, inf]` 保持实际 `return_net=0` / `nav_net=1`。

命令：

```text
python -B -X utf8 -m pytest -q -p no:cacheprovider tests/test_script_signal_adversarial.py
python -B -X utf8 -m pytest -q -p no:cacheprovider tests/test_v2_3_adversarial.py tests/test_v23_plain_promotion.py tests/test_v23_identity_adversarial.py tests/test_adversarial_v23_plain_20260904.py tests/test_realtime_member_action_contract.py tests/test_microcap_v2_review_remediations.py
python -B -X utf8 research_reports/20261007_script_adversarial/verify_v23_signal_parity.py
```

新一轮临时目录均显式位于本次审查目录。诊断不调用版本正式生成器、不写官方 outputs。

## 真实官流不变量

`verify_v23_signal_parity.py` 只读新鲜 base close/turnover 与 v2.0/v2.3 官流，并导入备份原 v2.3 源。首先按交付合同的 CRLF→LF 哈希核验同批数据，旧重算与 v2.3 已写官流匹配后才比较修复。

v2.3 3,988 日旧/新 `holding`、`next_holding`、signal_on、全部过热执行状态、`total_cost`、`return_net`、`nav_net` **逐点完全一致**；WLS 斜率最大绝对变化 `8.881784197001252e-16`，真实斜率符号变化为 0。100/500/1,500/3,000 日的前缀重算与完整重算的同期执行流一致。官方输入原始哈希在运行前后保持一致。

证据：`v23_signal_real_parity.json`、`v23_signal_real_parity.log`。这是当前数据下的脚本执行路径 parity，不是新收益报告、历史 PIT 数据或可成交性的全面认证。

最终 v2.3 源 SHA256：`dab988798bd1e262f2b8f738620b72794a38a79c55fa4d7c017b770c618a250a`；追加过热域校验后再次执行真实 parity，通过且 JSON 已更新该源码哈希。`git diff --check` 通过。

## 未覆盖及剩余边界

- 未把历史 L1/PIT universe、历史 ST、企业行为权益、停复牌/限价和可成交性重认证纳入本次信号脚本修复。既有这些约束不由本次 PASS 消除。
- 当天为假期，未发布、拉取或模拟成当天可执行的真实盘中信号；实时验证为源码追溯和隔离故障注入。
- 未测试 SMTP、外部 workflow、生产部署或真实下单。
- Full/10Y/5Y/3Y/1Y 收益/回撤：N/A，本任务验证脚本缺陷与同数据执行不变量，没有发布策略表现或参数候选。
- HEAD seed authority 的既有测试冲突在总报告中保留其后续处理结果，不能把本分支局部测试通过写成整个项目重新认证完成。
