**第二轮独立数学复核：v2.0 / v2.3 / v2.5**

结论：本轮独立测试 **68 PASS，0 FAIL，0 SKIP**。发现的两处 v2.5 上游成本合同遗漏已由主 Agent 修复，新增攻击均被拒绝。三版正常同批历史执行与原始完整依赖栈精确一致；与正式 CSV 的细微差异仅来自回读浮点精度。本轮不发布新的业绩结论，不认证实盘账户或历史输入准确性。

本子 Agent 仅新增 `tests/test_doublecheck_math_adversarial.py` 和本目录 `math_*` 证据文件；生产源码、正式输出、authority 均只读。生产补丁由主 Agent 实施。读取并遵守了项目 AGENTS、quant-research、user-quant-conventions 和 microcap-version-workflow；未刷新官流、改历史/参数、部署、发信、下单或删除目录。

**新的失败反例与影响**

以下均为隔离的上游依赖故障注入，未在正常正式收益流观察到。不能据此声称正式 NAV 存在漏费或漏日。它们在第一轮补丁前后都存在，属于补充遗漏，未发现第一轮数学补丁产生正常输入误修。

| 反例 | 修复前实际接受的行为 | 隔离影响 | 本轮修复与验证 |
| --- | --- | --- | --- |
| `apply_cost_model` 删除一个 gross 日期 | 只验证 costed 索引为 gross 子集，241 行可变成 240 行 | 删除 2025-02-19 的 0.002395290680910822 收益后仍输出 NAV；示例末 NAV 从 1.4019430260228554 变为 1.3985929892692712 | gross/costed 索引要求完整、同序相等；缺日、重复、逆序、额外日都阻断 |
| 上游保留 `entry_exit_cost=0.003`，同时把 `total_cost=0` 和 `return_net` 改成未扣费值 | 净收益与总费乘法恒等式仍成立，费用组件矛盾未被检查 | 2025-01-29 原净收益 -0.0030000000000000027 变为 0；末 NAV 比正常值高 1.003009027081245 倍 | 上游必须返回三成本组件；各组件有限且处于 [0,1)，组件合计必须等于 total_cost |

原始失败证据为 [math_suite.log](D:/动量策略/微盘股对冲策略/research_reports/20261007_audit_doublecheck/math_suite.log)（47 项中 45 PASS / 2 FAIL）和 [math_fault_injection_evidence.json](D:/动量策略/微盘股对冲策略/research_reports/20261007_audit_doublecheck/math_fault_injection_evidence.json)。失败用例可以独立执行，不依赖第一轮的 856 PASS 结论。

修改后没有把 `apply_no_target_vol` 的最小研究输入强制扩展成完整上游 schema；最小输入仍可以带合法现金日交易费用。完整成本字段要求施加在真实 `apply_cost` 依赖合同处。

**独立基准与对抗方法**

1. WLS 采用成对差分恒等式：斜率为 `sum(i<j, wi*wj*(xj-xi)*(yj-yi)) / sum(i<j, wi*wj*(xj-xi)^2)`，`y=log(NAV)`，年度分数为斜率乘 244。全样本使用成对计算；另用 75 位 Decimal 计算权重和对输入浮点数的精确对数进行点核对。没有复用生产的中心化点积或第一轮的 `numpy.linalg.lstsq` 基准。
2. 24 个解析/高精度用例覆盖两版、NAV 量级 `1e-200 / 1 / 1e200`、正负斜率、常量和 `1e-13` 小斜率。常量分数/R² 精确为零；小正斜率符号不被零修复吞掉，输入原表保持不变。
3. 独立逐日账本从两腿价格自行计算收益、信号和执行状态。v2.0 用 Mom16 原始差；v2.3 用 1.0 对冲的信号 spread、执行 0.8 对冲、vol10 的 0.26/0.20 防御；v2.5 用无对冲 WLS、零阈值。账本自行映射事件日期与费用，按 `(1+实际持仓收益)*(1-费用)-1` 计算净收益并复利。
4. v2.3 的风险触发只改变下一行持仓，blocked cash 行实际收益为零；重入不会捕获同日收盘才知道的市场收益。现金行合法入场/退出费用允许净收益为负。v2.5 close 与明确 next_open 两种费用路径均与独立账本相符。
5. 两版的边界不同：v2.3 已有持仓在分数等于 0 或 -0.08 时保留，低于 -0.08 退出；现金在分数等于 0 不进入。v2.5 在分数等于 0 退出。两版 current holding 均滞后 next holding 一行。边界测试使用隔离分数注入，正式参数未改。
6. 14 个合成 prefix 用例在 7 个截断点逐点比较持仓、费用、收益及 NAV，并修改后续价格攻击未来泄漏。真实两版各 7 个不同截断点亦精确通过。两版逐点 costed NAV 不高于相同实际持仓路径的未扣费 NAV。
7. 新成本合同的 18 个拒绝用例包含缺日、重复、乱序、额外日、三组件缺列、两个细分成本字段各 NaN/±inf/负数/文字以及同步改总费/净收益；两个正常组件用例保留 close/next_open 费用路径。

68 项组成：WLS 24、状态边界 2、三版/next_open 独立账本 4、prefix 14、合法现金费 4、成本合同拒绝 18、合法上游组件 2。最初扩展执行的 66 PASS / 2 FAIL 来自本测试 `_prices(60)` 长度夹具错误，未进入生产函数；已修为截取完整价格路径，保留原边界断言。最终证据为 [math_final2_suite.log](D:/动量策略/微盘股对冲策略/research_reports/20261007_audit_doublecheck/math_final2_suite.log) 和 [math_final2_suite.xml](D:/动量策略/微盘股对冲策略/research_reports/20261007_audit_doublecheck/math_final2_suite.xml)。

**真实同批读回与旧/新执行**

新鲜度依据本轮 [baseline_and_freshness.json](D:/动量策略/微盘股对冲策略/research_reports/20261007_audit_doublecheck/baseline_and_freshness.json)，读回路径、行数、日期及 SHA256 后再执行。panel 8,738 行、proxy index 4,067 行、base costed 4,050 行、正式 v2.0 4,033 行、v2.3/v2.5 各 3,988 行，日频终点均为最新完成收盘 **2026-09-30**。turnover 432 个事件，末事件 2026-09-17，全部 close；未制造假期/后续调仓事件。

比较基线使用 `.codex_backups/20261007_143135` 的完整原始 v2.0/v2.3/v2.5 依赖栈。旧 v2.3/v2.5 的 lazy loader 明确绑定旧 v2.0，避免只对比旧外层函数接新基座。v2.0 重演正式的第二次 Mom16 调用链，未把 4,050 日基础流冒充 4,033 日正式流。

| 版本与期间 | 原始完整栈/最终当前栈：关键列最大差 | 当前栈/正式 CSV：最大差 | 当前栈/独立账本：最大差 |
| --- | ---: | ---: | ---: |
| v2.0：4,033 日，2010-03-01—2026-09-30 | 0 | 3.552713678800501e-15 | 4.336808689942018e-19 |
| v2.3：3,988 日，2010-05-05—2026-09-30 | 0 | 3.552713678800501e-15 | 4.336808689942018e-19 |
| v2.5：3,988 日，2010-05-05—2026-09-30 | 0 | 1.4210854715202004e-14 | 0 |

关键列包括 holding、next_holding、signal_on、current_execution_scale、entry_exit_cost、rebalance_cost、total_cost、overlay_pre_cost_return、return_net、nav_net，日期完整索引一致。三版正常真实交易路径、费用、实际收益及 NAV 均未因两轮源码修复改变。

全样本独立 WLS 最大分数差：v2.3 `1.7763568394002505e-15`，v2.5 `5.329070518200751e-15`，分数符号变化均为 0。每版另核对 21 个 Decimal 点（16 个最接近零的分数加首/尾与不同历史点）：v2.3 分数/R² 最大差 `4.016318527755303e-15 / 7.882583474838611e-15`，v2.5 `8.813262619700168e-15 / 3.774758283725532e-15`。证据为 [math_v23_decimal_spots.csv](D:/动量策略/微盘股对冲策略/research_reports/20261007_audit_doublecheck/math_v23_decimal_spots.csv) 与 [math_v25_decimal_spots.csv](D:/动量策略/微盘股对冲策略/research_reports/20261007_audit_doublecheck/math_v25_decimal_spots.csv)。

真实 v2.3/v2.5 分别有 94 / 126 个 holding=cash 且有费用的交易日；这些合法现金费用没有被清零，现金实际市场收益均为零。事件表存在 `YYYY-MM-DD` 与 `YYYY-MM-DD 00:00:00` 混合表示，本轮审计用 `format='mixed', errors='raise'` 保留有效日期；未以 coercion 把有效明确执行日变成 NaT。

全部真实比较、7 个正式输入/输出的前后 SHA 与最终源稳定性记录在 [math_real_independent.json](D:/动量策略/微盘股对冲策略/research_reports/20261007_audit_doublecheck/math_real_independent.json)。实际执行前后正式产物 SHA 完全不变，4 份相关生产源在真实测试过程中也保持同一 SHA。

最终本轮真实执行源：

- shared base：`a8429df05baa894122ab982b0345443ca183f41ee115f142f779edb2dd1c48e5`
- v2.0：`bdf7d6fafc4230da4d2b248586837ea50204e49742dd67d8662b880261443171`
- v2.3：`615cb400d827727f524f08ecf738b49bc237e41cfda5823c26d218beeb3b834c`
- v2.5：`42b5ff2cbf455677312466250f3d06076a853f28becabf8b7209a42daf860267`

**复现与限制**

在项目根目录执行；每次 pytest 使用新的临时根，避免重用时清空旧证据目录：

```powershell
$mathTemp = 'research_reports/20261007_audit_doublecheck/math_repro_' + (Get-Date -Format 'yyyyMMdd_HHmmss_ffff')
python -B -X utf8 -m pytest tests/test_doublecheck_math_adversarial.py -q -p no:cacheprovider --basetemp=$mathTemp
python -B -X utf8 tests/test_doublecheck_math_adversarial.py --real
```

精确费用合计校验不能替代按真实执行状态和交易事件独立重算成本；本轮已对当前正常同批正式流完成这项重算。测试不是对任意研究参数或所有潜在输入空间的穷尽证明，75 位 Decimal 是选定的 42 个真实窗口点核对，全样本则用另一条成对公式。

本轮没有认证 point-in-time 成员/ST、企业行为权益、交易日源本身、停牌/涨跌停可成交性、实际成交价/回执、股票股数/期货张数/保证金/现金账户；0.8 对冲和固定 futures_drag 是源码收益模型中的假设。真实官流 parity 不能证明这些假设历史上可执行。

Full/10Y/5Y/3Y/1Y 年化收益和最大回撤均为 **N/A：本轮仅审脚本数学、时序和成本合同，不开展表现研究或参数晋级**。正式发布/全工作区同步门仍由主 Agent 管理，本轮未绕过本地/远端源码差异阻断。

最终身份补丁后复验：主 Agent 对 v2.3/v2.5 追加了 CSV 唯一表头身份门。数学未改；本子 Agent 已重新实际执行整个 --real 诊断，独立账本、Decimal、原始完整依赖栈、官方 CSV 和 prefix 检查继续 PASS，真实执行前后正式 SHA 不变。此处最终源 SHA 来自这次实际执行结果，不是手工替换后跳过测试。日志为 [math_real_final_identity.log](D:/动量策略/微盘股对冲策略/research_reports/20261007_audit_doublecheck/math_real_final_identity.log)。68 项合成数学测试的既有通过证据保留，最终源码全库验证由主 Agent 统一执行。
