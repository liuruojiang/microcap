# v2.0 / v2.3 / v2.5 第三轮多智能体对抗复核

本轮重新派出三个独立 Agent，从数据与历史边界、数学与费用账本、原生入口与最终文件合同三个方向攻击上一轮源码；主 Agent 额外攻击真实交付文件并统一实施修补。上一轮 PASS 没有替代本轮验证。**确认并修补 18 组遗漏或合法输入误拒；最终全库 1189 PASS、0 FAIL、0 ERROR、0 SKIP**，耗时 341.72 秒。覆盖实际加载、构建、生成、清理和交付路径，v2.6 排除。完整计数和证据哈希见本目录 `validation_summary.json`。

已知正常路径与本轮冻结完整旧依赖栈的三版持仓、状态、费用、收益、NAV 精确相同；独立价格与费用账本最大差不超过 4.34e-19，保存 CSV 的差异在浮点序列化范围内。正式策略参数未变。工程审计通过不表示完整历史输入、成交可执行性或正式生产交付认证通过。

## 本轮新增发现与修补

| 组数 | 上一轮遗漏及可复现影响 | 本轮修补 |
|---:|---|---|
| 1 | 合法日期与午夜文字混排误拒；候选哈希会丢失中间合法日期 | 逐行验证并规范完整日频日期，保留坏日期、时区、非午夜、重复会话拒绝；候选哈希绑定每个实际会话 |
| 1 | 正式加载及重建入口在校验前丢掉 turnover 行，可漏收 0.002 费用 | 在原始消费端验证事件，合法行完整保留，坏行在写 NAV 前拒绝 |
| 3 | 同一调仓事件重复收费；显式执行日掩盖非会话信号日；混合 timing 随行序改变整条成本语义 | 拒绝重复事件、窗口内非会话信号及混合 close/next_open 事件表 |
| 2 | 真正暖机缺值的不同表示被误判历史改写；整数 approved=1 被当作批准 | 双方真实缺值等价，同时保留缺值转文字的拒绝；批准字段必须为实际 JSON 布尔 True |
| 2 | v2.5 同步清零费用、total/net/nav 仍算术自洽；v2.0/v2.3 接受损坏或移日的费用映射 | 对照原始事件日期和 gross 执行状态独立推导应付费用，核对真实消费者 |
| 2 | v2.3 坏 spread NAV 被变成零波动，推迟防御；上游可以改变持仓或下一持仓 | 验证正有限 spread NAV、合法状态及成本模块保留 gross 状态 |
| 2 | 上游缺首日/活动日仍缩短样本；v2.3 依赖合同未锁定批准 entry/exit 费率 | 完整覆盖请求索引与有效信号日期；绑定两项 0.003 费率，实时直接构建同步保护 |
| 1 | 三版损坏实时文件可被当作 current 保留，历史名单可被改成当日行动 | 同时核对版本、日频标签、快照时区、前一完整收盘锚点、实际状态/尺度及成员日期；v2.0 清理加锁后重新检查，保护并发合法写入 |
| 2 | 日频标签的时刻/时区后缀被截断；最终动量或 WLS 值与权威 NAV 矛盾仍可认证 | 拒绝非法完整日期；核对各版实际 mom/gap/score 字段，保留 v2.0 原生无 WLS schema |
| 2 | 整条 NAV 缺失或中途变更 version 仍能通过；单行 CSV 多列/少列被接受 | 全部正式 NAV 与 signal 每行核对版号；拒绝行宽与表头不符 |

18 是去重问题组数，变体失败和多入口覆盖没有重复计为独立 bug。数据 7 组、数学 6 组、状态 3 组、主 Agent 交付 2 组。隔离损坏反例不表示真实历史文件曾发生相同损坏。候选日期哈希旧迁移入口会先拒混合日期，因此没有把旧哈希问题说成已经成功绕过正式迁移。

新增共享 `scripts/top100_data_contracts.py` 供实际日期、事件费用和实时合同消费者调用；该文件同时纳入远端发布比较、delivery 输入哈希和 cloud 恢复源码清单。修改范围限共享主入口、三版入口及交付脚本。生产源码由主 Agent 唯一写入，三个子 Agent 独立写测试和证据。

## 独立红绿证据与真实正常路径

| 方向 | 上一轮冻结源的反例 | 修补后独立验收 |
|---|---|---|
| 数据边界 | 同一 57 项：40 FAIL / 17 PASS | 57 PASS，另 125 项数据/ST 回归 PASS |
| 数学与账本 | 25 项：22 FAIL / 3 PASS | 新增 25 项及必要数学/防御回归共 170 PASS |
| 实时/最终文件 | 原生主批 31 FAIL / 4 PASS，成员反例 2 FAIL，另 4 控制 PASS | 最终 46 PASS；37 项反例回归、9 项合法控制，包含收盘后锚点、二字段成员绕过和并发控制 |
| 主 Agent 交付 | 13 项：12 FAIL / 1 PASS | 最终完整测试中的 13 项全部 PASS，使用真实三版文件副本 |

最终全库包含上一轮 1048 项及本轮新增 141 项；各子组回归与全库有重叠，不叠加为总用例数。8 条 warning 为两项故意损坏 ZIP 的重复条目、5 次原生诊断的 public/local proxy 来源提示，以及一个空事件拼接的 pandas 弃用提示；没有失败或跳过。

旧源冻存在 `.codex_backups/20261007_200436`，同本轮起点与上一轮最终 18 份源 SHA 精确一致；数学对照明确加载完整旧依赖栈，而非旧外层接新基座。数据测试统一指定冻结源根，含 lazy loader 的 canonical module 注册。状态原生入口保留真实重算、历史审计、暂存提交及清理，仅隔离可能写正式基座的加载与刷新边界。交付 CLI 用真实完整产物副本，发布/刷新门显式隔离，不能将其 exit 0 说成正式交付成功。

三版正常完整样本重演的关键列与冻结栈最大差为 0。独立账本差异 v2.0/v2.3 为 4.3368e-19、v2.5 为 0；正式保存 CSV 的最大差分别为 3.5527e-15、3.5527e-15、1.4211e-14。现金的市场收益为 0，保留现金开平仓实际应付费用。26% 过热触发、20% 恢复、下一会话才切换持仓的边界控制通过；未改 v2.3 既有暖启动定义。

合法旧日期实时快照继续保留，等价 UTC 时刻与无时区午夜日频文字均接受。成员即时行动 True 的完整真实原件覆盖限制见状态报告，未将手写或函数控制说成实际成交证据。

## 新鲜度与正式交付状态

先实际执行正规 `top100_delivery.py refresh-all`，因本地源码与远端 release 不同 exit 1。它按合同把正式 `outputs/top100_delivery_manifest.json` 留为 **blocked**；没有恢复旧 complete manifest 或绕过 lineage 门。最终正规 `check` 同样 exit 1，具体阻断为 `scripts/top100_cloud_delivery.py` 的本地/远端差异。远端 main 独立读回为 `eefd27d81d2c9c9f1438975c4d5c221d1b81bf96`。本轮仅完成本地工程修补，未声称正式同步完成。

最新完成收盘由独立交易日历确认是 **2026-09-30**。七份实际输入与同日成功刷新后的文件原始 SHA 一致，随后只用于诊断，不发布新的正式收益、持仓或信号：

| 实际输入/流 | 行数 | 最新日期 |
|---|---:|---|
| panel | 8738 | 2026-09-30 |
| proxy index | 4067 | 2026-09-30 |
| turnover | 432 | 2026-09-17，真实调仓事件表 |
| base costed NAV | 4050 | 2026-09-30 |
| v2.0 NAV | 4033 | 2026-09-30 |
| v2.3 costed NAV | 3988 | 2026-09-30 |
| v2.5 costed NAV | 3988 | 2026-09-30 |

turnover 432 条全保留，都是 close，含 431 个日期字符串和一个合法午夜字符串。事件表不补造假日或日频事件。7 个真实输入 raw SHA、28 个正式产物的 delivery 规范化 SHA 和 4975 个 metadata 文件字节均再次核对；清单在 root_official_artifact_check.json。正式 blocked manifest 是本轮预期状态变化，不包含在“28 个产物不变”中。

## 验证纠正、回退与边界

初次集成全库为 94 FAIL / 1085 PASS / 7 SKIP，保留 root_full_probe.log/xml，未当作最终验收。失败逐项读回，旧合法夹具缺少新必填的共享源码、版号、完整实时时间及费用字段；补齐字段后受影响 285 项通过，最后四项来自 v2.0 借用 v2.3 夹具中不存在于其原生 NAV 的 WLS 字段，改用真实 schema 后 12 个转换控制通过。损坏输入断言未删。外部 digest 七项改用已核实远端精确提交的源快照，绑定原始 SHA，不以另一版本代替。

根交付红证的初次 CSV 夹具受 Windows 双换行读取影响，后以原始 newline-aware 读取纠正，采用 root_delivery_before2 的 12 FAIL/1 PASS；状态组其他夹具纠正详见 state_report.md。补丁后因换行或函数 namespace 的并发测试断言纠正，不计为新增生产 bug。数学逐例日志会在完整回归继续追加；原子组验收时点的摘要另行保留，最终追加日志与汇总哈希以 validation_summary.json 为准。

回退备份有 `20261007_200436`（原五生产源及原 manifest）、`20261007_202622`（cloud 源）以及 `20261007_2030_test_fixtures`、`20261007_2038_test_fixtures`（本轮修改的旧测试夹具）。备份逐字节校验后才修改。共享 helper 是新增文件。保留用户已有脏改动和研究文件；未 commit、push、部署、发信、下单或执行历史迁移。恢复源码须整组审查并重新通过正规交付检查，不得把备份 complete manifest 单独复制回来认证新源码。

第一轮被平台拒绝删除的七个旧临时目录仍保留在 `research_reports/20261007_script_adversarial` 下，本轮没有重试删除或换工具绕过。当前证据目录也保留供复现。

完整 PIT 成员/ST 历史、公司权益、停牌/涨跌停成交、真实滑点与期货保证金、统一自融资现金账户、2018 等历史非交易日 proxy 源及正式部署/邮件交付没有由本轮认证。Full / 10Y / 5Y / 3Y / 1Y 年化收益与最大回撤均 **N/A：本轮是脚本及合同复核，不开展新策略业绩或晋级研究**。

## 证据及复现

- [数据审查](D:/动量策略/微盘股对冲策略/research_reports/20261007_audit_round3/data_review.md)、[数学账本](D:/动量策略/微盘股对冲策略/research_reports/20261007_audit_round3/math_report.md)、[状态合同](D:/动量策略/微盘股对冲策略/research_reports/20261007_audit_round3/state_report.md)
- [最终机器汇总](D:/动量策略/微盘股对冲策略/research_reports/20261007_audit_round3/validation_summary.json)、[最终源码指纹](D:/动量策略/微盘股对冲策略/research_reports/20261007_audit_round3/final_source_manifest.json)、[正式文件只读核对](D:/动量策略/微盘股对冲策略/research_reports/20261007_audit_round3/root_official_artifact_check.json)
- [最终完整测试日志](D:/动量策略/微盘股对冲策略/research_reports/20261007_audit_round3/final_verified_suite.log)、[JUnit](D:/动量策略/微盘股对冲策略/research_reports/20261007_audit_round3/final_verified_suite.xml)
- [正规 refresh-all 阻断](D:/动量策略/微盘股对冲策略/research_reports/20261007_audit_round3/pre_refresh_all.log)、[最终正规 check](D:/动量策略/微盘股对冲策略/research_reports/20261007_audit_round3/post_delivery_check.log)

从项目根运行，并使用新的 basetemp 路径，避免 pytest 清除既有证据：

```powershell
$env:PYTHONDONTWRITEBYTECODE = '1'
$env:MICROCAP_DIGEST_SOURCE = (Resolve-Path -LiteralPath 'research_reports/20261007_script_adversarial/external_digest_source.py').Path
python -B -X utf8 -m pytest tests -q -p no:cacheprovider --basetemp=research_reports/20261007_audit_round3/replay_new_full
python -B -X utf8 tests/test_round3_math_ledger.py --real
python -B -X utf8 research_reports/20261007_audit_round3/root_final_checks.py
```
