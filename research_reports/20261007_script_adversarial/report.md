**v2.0、v2.3、v2.5 多智能体脚本对抗审查与修复报告**

审查日期：2026-10-07。三个子 Agent 分别审查数据与血缘、信号与实时契约、数学与成本，主 Agent 审查交付门、整合补丁并执行最终验证。范围按用户纠正限定为 v2.0、v2.3、v2.5 及其共享基座，未修改 v2.6。

脚本修复与本地回归完成：**856 项通过，0 失败、0 错误、0 跳过**，包含本次新增的 233 项故障注入与不变量用例。真实同批历史复算中，三个版本的逐日持仓、执行规模、成本、净收益及净值保持一致，正式参数未变。修复尚未部署，历史数据准确性与实盘执行认证仍保留既有未完成项。最终结果以本报告和 final_suite 为准；分报告保留了修复过程中的失败、局部回归和交叉攻击记录。

审查覆盖正式入口的版本身份、依赖门槛、价格与日期载入、证券元数据与 ST 缓存、代理内容指纹、历史重写保护、信号和执行持仓、WLS、过热防御、成本日期、收益与净值数值域、实时锚点、锁和并发清理、最终 CSV、名单排序与动作字段。停用的历史参数和研究接口没有被启用。

主要修复如下。表中反例均为隔离诊断输入，不能当作策略业绩。

| 范围 | 可复现缺陷及影响 | 修复行为 |
| --- | --- | --- |
| 共享数据基座 | 收盘后可把来源中的未来日期选为最新收盘锚点 | 先排除晚于北京时间今天的日期，盘前继续排除未收盘当日 |
| 共享数据基座 | 无效日期被静默删除，损坏记录隐藏 | 对无效日期、NaT 明确报错，保留合法暖机语义 |
| 共享价格载入 | 中间缺一腿价格时 dropna 压缩交易日，改变动量和执行滞后 | 在有效 proxy 窗口保留两腿日期并集；缺价、NaN、inf、非正价阻断 |
| 历史重写门 | 有限值改成 inf、inf 改符号，或不同文字都转成 NaN，可绕过比较 | 保留既有数值容差，检查非有限值及无法解析的原始单元格 |
| 独立基础脚本 | 删除/插入历史日期或删除关键列只比较交集，未阻断改写 | 核对历史日期集合、关键列和逐列冻结内容；合法新增尾部仍允许 |
| 表现入口 | 部分缺失收益、非正 NAV、破产收益、收益与 NAV 矛盾可通过 | 校验有限收益、正 NAV、收益大于 -100% 及相邻 NAV 比率 |
| 证券缓存 | 非 ST 分支复用外来 symbol、显式过期政策或漂移 ST 名字 | 检查身份、显式政策和当前快照，异常走既有刷新/历史迁移门 |
| 实时缓存 | 缺少 proxy metadata 时跳过兼容性和内容指纹 | 嵌入和独立入口均要求 metadata 存在，缺失则禁止复用 |
| 成本映射 | 明确 next-open execution_date 又滞后一日；窗口前已付费用在首行重收；坏费率未拒绝 | 明确执行日直接映射，仅 legacy 信号日期加一会话；跳过窗口前已付费用，校验日期与费率 |
| v2.3 实时文件 | 当前 summary 可认证退役/损坏实时 CSV，清理又无条件保护它 | 独立检查实时单行 CSV 的版本、revision、参数和开关；清理持锁再次核对，保留并发合法新写者 |
| v2.3 信号字段 | 参考 v2.0 的旧风控数值污染 v2.3 原生信号 | 活跃热度值来自本版函数；NaN 不回填旧值，停用风控字段清空 |
| v2.3 / v2.5 WLS | 理论恒定窗口产生微小非零斜率，零阈值下可错误继续持仓 | 每窗 log NAV 减最后值，代数定义不变；恒定窗口斜率/R² 精确为零，不增加 epsilon |
| v2.3 信号与防御 | 对冲价跳变可产生破产 spread NAV；活跃收益、成本及累计溢出/下溢未拒绝 | 检查实际收益和派生正 NAV；cash 诊断市场收益仍不计入实际收益 |
| v2.5 价格输入 | 数字字符串通过临时校验，随后对原字符串运算报 TypeError | 返回并使用数值副本，保持调用方原表不变 |
| v2.5 收益与账户 | NaN/文字净收益填零；inf、坏成本、破产及累计溢出/下溢继续输出 | 校验 gross/net/pre-cost、成本区间和派生正 NAV，异常指出字段及日期 |
| v2.5 固定规模执行 | 可接受现金日非零市场收益或漏扣交易成本 | 校验净收益乘法公式和 cash pre-cost=0；保留合法现金行入场/退出费用 |
| 最终交付动作 | 合法持仓与规模掩盖矛盾 signal_label、开平仓文本或动作别名 | 由 current/next holding 推导固定 1 倍动作，逐项检查标签及可选动作字段 |
| 当前成员排序 | 100 唯一代码仍可有重复/缺失 rank | 最近两次名单必须具有完整且唯一的 rank 1–100，并使用规范 ASCII 六位代码 |

实际源码修改集中在以下五个文件；另新增四份对抗测试，更新三份已有测试夹具以反映真实合同和冻结种子。

- [共享基础脚本](D:/动量策略/微盘股对冲策略/microcap_top100_mom16_biweekly_live.py)
- [v2.0 正式入口及嵌入基座](D:/动量策略/微盘股对冲策略/microcap_top100_mom16_biweekly_live_v2_0.py)
- [v2.3 正式入口](D:/动量策略/微盘股对冲策略/microcap_top100_mom16_biweekly_live_v2_3.py)
- [v2.5 正式入口](D:/动量策略/微盘股对冲策略/microcap_top100_mom16_biweekly_live_v2_5.py)
- [三版交付检查](D:/动量策略/微盘股对冲策略/scripts/top100_delivery.py)

新鲜度由修改前正式 refresh-all、独立 check 和写回文件读回共同证明。两条正式命令均成功，最新完成收盘为 **2026-09-30**。没有把假期 10-07 当作交易日，也没有使用停于 09-15 的普通 base_nav 导出。

| 同批输入/成本流 | 行数 | 首日 | 最后日 |
| --- | ---: | --- | --- |
| refreshed panel | 8,738 | 1990-12-19 | 2026-09-30 |
| 公开/本地重建 Top100 proxy index | 4,067 | 2010-01-05 | 2026-09-30 |
| base costed NAV | 4,050 | 2010-01-28 | 2026-09-30 |
| v2.0 正式 costed NAV | 4,033 | 2010-03-01 | 2026-09-30 |
| v2.3 正式 costed NAV | 3,988 | 2010-05-05 | 2026-09-30 |
| v2.5 正式 costed NAV | 3,988 | 2010-05-05 | 2026-09-30 |
| proxy turnover 事件表 | 432 | 2010-01-14 | 2026-09-17 |

turnover 是事件表，09-17 为最近实际成员调仓，与成员记录一致；没有伪造后续换仓事件来填满日频终点。输入路径与 SHA256 见 [真实数据读回](D:/动量策略/微盘股对冲策略/research_reports/20261007_script_adversarial/math_real_parity.json)，正式刷新证明见 [pre_delivery_check.json](D:/动量策略/微盘股对冲策略/research_reports/20261007_script_adversarial/pre_delivery_check.json)。

正式定义保持原值：v2.0 为 Mom16、固定 1 倍、执行对冲 0.8；v2.3 为 WLS 25 日/半衰期 2.5、R² 入口门关闭、退出缓冲 0.08、vol10 过热 0.26/恢复 0.20、信号对冲 1.0/执行对冲 0.8；v2.5 为 WLS 20 日/半衰期 3、入场/退出阈值 0、无对冲、无 target-vol。未开启融资或停用 overlay。AST 对比确认原有 13/37/33 个顶层标量分别未变，见 [参数不变量](D:/动量策略/微盘股对冲策略/research_reports/20261007_script_adversarial/parameter_invariants.json)。

成本使用实际正式函数，保持 holding/next_holding 的原执行映射和净收益乘法记账。当前真实 432 条名单成本事件全为 close，修复前后成本逐点相等；next-open 的错移与窗口前重收由独立明细反例验证。gross 和 costed 分开比较，没有将基础 4,050 日 helper 曲线代替正式 v2.0 的 4,033 日调用链。

| 真实复算 | 同数据旧/新结果 | 独立核对 |
| --- | --- | --- |
| v2.0 正式 4,033 日 | 收盘输入、基础信号/成本和正式调用链逐列精确一致 | 冻结关键列与官方 CSV 匹配，NAV 回读最大差 3.55e-15 |
| v2.3 正式 3,988 日 | 持仓、下一持仓、规模、成本、净收益、NAV 精确一致 | CSV NAV 最大差 3.55e-15；WLS 分数符号零变化；100/500/1,500/3,000 日前缀因果检查通过 |
| v2.5 正式 3,988 日 | 持仓、下一持仓、规模、成本、净收益、NAV 精确一致 | CSV NAV 最大差 1.42e-14；WLS 分数符号零变化；数值/数字字符串输入结果一致 |

上述 CSV 差异为回读浮点精度，所有比较均在 1e-12 内。独立 WLS 使用带截距的加权 numpy.linalg.lstsq，不复用源码中心化公式，最大分数误差 v2.3 为 1.72e-14、v2.5 为 6.53e-14。当前真实样本的持仓不受这次零斜率修复影响；“先上涨后长期横盘”的隔离反例已从错误持仓修正为现金。

正式价格、基础及三版成本净值、成员、turnover 和 authority 与修复前备份逐文件一致。必要刷新只更新 tracked base_summary 的新鲜度/展示元数据，没有迁移历史或手改 authority。读回证据分别在 [共享基座 parity](D:/动量策略/微盘股对冲策略/research_reports/20261007_script_adversarial/data_real_parity.json)、[两版数学 parity](D:/动量策略/微盘股对冲策略/research_reports/20261007_script_adversarial/math_real_parity.json) 和 [v2.3 因果/信号 parity](D:/动量策略/微盘股对冲策略/research_reports/20261007_script_adversarial/v23_signal_real_parity.json)。

新增测试覆盖情况如下，均进入最终 856 项全库回归。

| 测试文件 | 本次用例数 | 主要验证 |
| --- | ---: | --- |
| test_script_data_adversarial.py | 82 | 日期、坏价格、缓存身份、改写绕过、成本日期、数值域 |
| test_script_delivery_adversarial.py | 37 | 最终信号动作、成员 rank 与规范代码 |
| test_script_math_adversarial.py | 41 | 零斜率、字符串、账户收益/费用、破产与复利边界 |
| test_script_signal_adversarial.py | 73 | 退役实时身份、锁与并发清理、版本字段污染、独立数学与信号攻击 |

修复前同源反例已留下失败证据，例如数据组原备份为 60 FAIL/22 PASS；失败数量是输入变体数，不是独立 bug 数。不同 Agent 随后互相攻击补丁：信号 Agent 检查 v2.5，数学 Agent 检查 v2.3，数据 Agent 检查最终交付门和冻结种子。三个版本原生生成器的 12 种合法固定 1 倍持仓转换均通过，没有为收紧校验误拒正常动作。

既有 frozen-tail 夹具曾把合法延伸至 09-30 的完整文件误当 09-17 原始种子。修复后的夹具仍逐项锁定原种子前缀 SHA、未变成员/换手文件全量 SHA、相同尾部日期集合及 authority 绑定；没有放宽历史改写保护。该阶段失败已在最终全库回归中解决。

初轮全库有 7 项外部日报合同测试因旧源路径不存在而跳过。已只读获取实际远端 automation 提交 a89e9099f33ce291830a1415893a6e7cec5eb4cd 的精确源文件快照，未使用与远端不同的本地旧 checkout；SHA256 为 2fee97afec037470290a3be97538aa528ecc5f7ea63b7f9c3b37ee4aa38578f4。补测后最终零跳过，快照身份见 [external_digest_source_identity.json](D:/动量策略/微盘股对冲策略/research_reports/20261007_script_adversarial/external_digest_source_identity.json)。这验证的是脚本合同，未发邮件或运行生产 workflow。

最终全库耗时 56.30 秒，2 条 warning 来自防篡改测试故意向 ZIP 插入重复名称；相应拒绝测试均通过。12 份改动/新增 Python 文件语法检查通过，git diff --check 通过。证据：[最终日志](D:/动量策略/微盘股对冲策略/research_reports/20261007_script_adversarial/final_suite.log)、[JUnit 结果](D:/动量策略/微盘股对冲策略/research_reports/20261007_script_adversarial/final_suite.xml)、[最终源码哈希](D:/动量策略/微盘股对冲策略/research_reports/20261007_script_adversarial/final_source_manifest.json)、[验证摘要](D:/动量策略/微盘股对冲策略/research_reports/20261007_script_adversarial/validation_summary.json)。

在项目根目录可复现最终测试及只读 parity：

```powershell
$env:PYTHONDONTWRITEBYTECODE = '1'
$env:MICROCAP_DIGEST_SOURCE = (Resolve-Path -LiteralPath 'research_reports/20261007_script_adversarial/external_digest_source.py').Path
python -B -X utf8 -m pytest tests -q -p no:cacheprovider --junitxml='research_reports/20261007_script_adversarial/final_suite.xml'
python -B -X utf8 research_reports/20261007_script_adversarial/data_real_parity.py
python -B -X utf8 research_reports/20261007_script_adversarial/math_real_parity.py
python -B -X utf8 research_reports/20261007_script_adversarial/verify_v23_signal_parity.py
```

发布状态为 **仅本地修复**。修改前完整交付检查通过的策略版本为 eefd27d81d2c9c9f1438975c4d5c221d1b81bf96；修复后的正式 check 已实际执行并阻断，原因是 `Local core/authority differs from remote release: scripts/top100_delivery.py`。这是本地补丁尚未同步远端的真实状态，不能表述为部署或全工作区交付同步成功。记录见 [post_local_delivery_check.json](D:/动量策略/微盘股对冲策略/research_reports/20261007_script_adversarial/post_local_delivery_check.json)。未提交、推送、上线、发信或下单。

历史认证边界以当前仓库 [L7 结案记录](D:/动量策略/微盘股对冲策略/research_reports/20261003_l7_repair/L7_SYNC_CLOSEOUT.md) 和 [L8 决策](D:/动量策略/微盘股对冲策略/research_reports/20261003_l8_diagnostic/L8_DECISION.md) 为准。当前数据是公开/本地重建 Top100 代理；完整 PIT 成员/ST、公司权益事件、2018 历史休市伪行、真实成交价/回执、股数/期货张数/保证金/现金和共享资金账户仍未完成准确性认证。本次正常曲线 parity 不解除这些限制；必要历史纠正仍须精确哈希迁移与无迁移参数的第二次清洁审计。当天假期未取得生产盘中快照，实时验证仅为源码与隔离反例。

Full/10Y/5Y/3Y/1Y 年化收益和最大回撤均为 N/A：本轮不开展新策略表现研究、参数扫描或候选晋级，不重发既有业绩作为脚本认证结果。

修复前 42 份源码/正式产物已有验证过的可恢复备份：[主备份清单](D:/动量策略/微盘股对冲策略/.codex_backups/20261007_143135/manifest.json)。三份既有测试夹具分别另保存在 [交付夹具备份](D:/动量策略/微盘股对冲策略/.codex_backups/20261007_143616/manifest.json) 和 [冻结种子夹具备份](D:/动量策略/微盘股对冲策略/.codex_backups/20261007_144120/manifest.json)。用户已有的历史研究文件保持原状。

分工与反例细节：[数据与血缘审查](D:/动量策略/微盘股对冲策略/research_reports/20261007_script_adversarial/data_lineage.md)、[信号与实时契约审查](D:/动量策略/微盘股对冲策略/research_reports/20261007_script_adversarial/realtime_contract.md)、[数学与成本审查](D:/动量策略/微盘股对冲策略/research_reports/20261007_script_adversarial/strategy_math.md)。

平台以 `blocked by policy` 拒绝删除 7 个测试隔离临时目录，删除未执行，已停止该目标且没有换工具重试。这些目录位于 D:/动量策略/微盘股对冲策略/research_reports/20261007_script_adversarial/ 下：data_baseline_tmp、data_fixed_tmp、data_regression_tmp、data_backup_baseline_tmp、data_final_tmp、data_locked_final_tmp、data_locked_before_tmp。可由用户手动或通过平台正式渠道处理；测试源码、报告、备份和必要复现证据应保留。
