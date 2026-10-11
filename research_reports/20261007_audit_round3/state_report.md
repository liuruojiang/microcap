**第三轮独立复核：最终文件与实时状态合同**

本组独立确认三类上一轮遗漏，主 Agent 修复后，最终完整回归为 **46 PASS、0 FAIL、0 ERROR**，日志耗时 79.59 秒：37 项反例回归和 9 项合法控制。范围为真实 v2.0、v2.3、v2.5，v2.6 排除；生产补丁由主 Agent 唯一实施，本组只新增测试和证据。

修复前主批为 **31 FAIL、4 PASS、0 ERROR**，额外兼容控制 **4 PASS**，两项实时成员动作反例 **2 FAIL**。晚增的两字段成员动作和收盘后锚点共四例，再通过逐字节冻结的原 matcher 函数独立补录：旧函数全部接受，最终函数全部拒绝，两个真实原件控制新旧都接受。这四项仅是准确原函数范围的诊断，与完整原生 CLI 证据分别记录。

这份报告仅证明隔离脚本及文件合同行为。没有部署、发信、下单、批准历史迁移或输出新业绩结论。正式刷新本轮因本地与远端源码不同被阻断，正式 delivery manifest 保持 blocked。

| 确定发现 | 原生反例与影响 | 最小修复范围 |
| --- | --- | --- |
| S1：损坏实时文件仍受保护 | v2.0 原生 `信号` 完整重算后仍原样保留 `version=9.9`、缺失历史锚点、仓位 9 倍的实时 CSV；v2.3/v2.5 原生入口也保留后两类文件。各版隔离重算的日期、holding、next_holding、return_net 与正式流一致，不能用成功重算替代实时文件认证。另 12 个 matcher 反例证实缺 snapshot_time、未来 quote/anchor、日期不符、矛盾 official bit 与 trade_state 都能被标成 current。再以两版实际 intraday CSV 证明，把早于当前锚点的 09-17 历史名单动作强改为 actionable=True 仍被接受。 | 三版 realtime 保护门同时核对身份和内部时间/状态一致性及实时成员动作日期；v2.0 取消无条件保护，并在实际清理时重查受锁保护的当前文件。合法旧日期快照继续保留，不按今天的 TTL 自动删除。 |
| S2：最终日频标签被截断认证 | 三版 `latest_signal.date` 改为 `2026-09-30 12:00:00` 或 `2026-09-30T00:00:00+08:00` 后，隔离的原生 delivery `check` 仍 exit 0、errors 空。根因是 csv_info 直接用 state._parse_date 丢弃 T/空格后的全部后缀。 | 最终日频 CSV 校验完整时间标签，拒绝非午夜/带时区日期；保留合法无时区午夜的旧序列化格式。 |
| S3：最终动量诊断可与权威 NAV 分离 | 三版最终 signal 的 momentum_gap 改成 -999，或删去 microcap_mom，原生 delivery `check` 均接受。持仓/仓位对照未覆盖实际解释交易信号的 mom/score 字段。 | 要求本版真实必需字段有限且与同版 costed NAV 最后一行一致；v2.3/v2.5 同时核对 WLS 别名，保留 v2.0 无 WLS 字段的真实 schema。 |

**控制与职责边界**

三版原生收盘入口均保留原始合法旧实时文件；真实整套文件隔离副本的原生 delivery `check` 为 exit 0。这四项控制与 31 个攻击使用同一真实文件来源，防止把不完整合法夹具或无关门阻断误报为修复。

最终共运行 **11 次完整原生收盘生成/审计/提交/清理**：七个损坏实时文件均被移除，三版原始合法旧快照逐字节保留；第十一项在 v2.0 stale-file 已选定、清理获得锁之前，由原生 atomic writer 在原生 realtime 锁下写入真实合法快照。清理重新获得同一锁后重查合法性，并保留 writer 实际提交的完整字节，提交 SHA256 与最终 SHA256 相同。它是确定次序的并发交错控制，不依赖调度概率。

新增的两项成员反例只改变实际 intraday 行的 actionable=False 为 True：原始 member signal date=09-17，v2.3/v2.5 anchor 分别为 09-29/09-28，execution date 为空，required=True。原生实时生成器要求名单信号日等于前一完成收盘锚点，才可能标记即时执行会话。这里是确切历史动作矛盾，不能与正式收盘行的次会话计划混为一谈。

最终另含两项双字段绕过：同时令 actionable=True、execution_date=quote，但保留真实 09-17 历史成员信号日期；最终 matcher 仍拒绝。两项收盘后时间攻击将真实 snapshot_time 改为当日 16:00+08:00，此时独立官方日历函数已认定最新完成收盘为 quote 当日，不能仍带前一日锚点。新 helper 初版曾接受该矛盾，修复前直接证据保存在 `state_new_helper_after_close_probe.json`，最终两版均拒绝。

真实完整生产输入中未取得合法 actionable=True 的 intraday CSV，不能宣称该分支已经用真实当日成员动作完成生产认证。已有三版真实非 actionable 快照控制全部通过；`test_realtime_member_action_contract.py` 另外提供原生成员动作定义控制，但其生成夹具属于 diagnostic。没有手写不完整 True 行充当生产合法控制。

另四项控制证明，v2.3/v2.5 的 snapshot_time 转成完全等价的 UTC 时间仍可保留，最终日频 date 的无时区 `YYYY-MM-DD 00:00:00` 和 `YYYY-MM-DDT00:00:00` 格式仍合法。时间区分针对日频会话标签；snapshot_time 本身可带显式时区。

原生 signal 反例保留真实重算、成本、历史改写审计、暂存/提交及 stale-output 清理流程，仅把可能写正式基座的上游 loader 替换成由已验证真实输入重建的只读 context。delivery 反例使用实际完整产物副本、真实参考摘要和真实原生 main；远端发布及基础 freshness 门显式隔离，只检验最终文件语义。隔离命令成功不能表述为正式交付成功。

三版参数迁移 evidence 的源码绑定 previous、candidate、audit、策略源码及基座输入哈希；交付门仍要求 clean 的第二次历史审计。本轮没有生成批准报告或执行迁移。v2.0 既有单字段 blocked_until_signal_reset 元数据布尔迁移白名单在 2026-07-11 修复记录中已有说明；本组保留其源码边界说明，未将其视为新授权、也未认证整个历史链。

**真实输入与修复前证据**

新鲜度门由主 Agent 先完成，本组再读取 `baseline_and_freshness.json` 并逐字节独立验证七份实际输入。最新完成收盘为 **2026-09-30**；turnover 是事件表，最后实际成员变化为 2026-09-17。真实使用 panel 8738 行、proxy 4067 行、turnover 432 行、base costed 4050 行、v2.0 4033 行、v2.3/v2.5 各 3988 行。没有用旧导出替代新鲜度证明。

修复前 fixture 已校验 **31 份实际输入/正式及实时文件的 raw SHA256 不变**。最终扩大绑定至所有实读基础输入、真实参考摘要及 authority，共 **36 份文件**；七份生产补丁源码和测试源码也逐份前后 SHA256 相同。修复前源码与逐例证据保留在 `state_old_findings.json`；最终完整绑定保存在 `state_final_validation.json`。

初测 `state_old_red.log` 不计入发现数量：v2.0 自定义 prefix 同时改变基座 panel 路径，提前触发 freshness 拒绝；delivery 副本遗漏真实 base_summary，提前被 reference gate 拒绝。随后分别改为默认 prefix 加隔离输出目录、复制真实完整摘要，并增加成功控制。更正版 `state_confirmed_old_red.log` 的 31 失败全为到达目标行为后的确定断言，不再包含夹具阻断。

最终补丁首两批各为 45 PASS + 1 新并发控制夹具失败，也不计作生产缺陷：`state_new_green.log` 的真实合法文件已保留，但原生文本写入转换 Windows 换行，原件字节不是 writer 的实际提交字节；`state_new_green_final.log` 的新增预断言错误读取 v2.0 外层属性，真实 matcher 在嵌入 namespace。夹具修正后先单跑并发控制 1 PASS，再完整运行 `state_final_verified` 得到 46 PASS。失败日志未覆盖或删除。

新共享 helper 已实际确认在 delivery `RELEASE_FILES`、`input_hashes` 和 cloud `SOURCE_FILES` 三处绑定，证据为 `state_dependency_binding.json`。这仅证明本地发布依赖合同完整；正式远端发布和交付 gate 并未因此通过。

**最终生产源码绑定**

| 文件 | 最终 raw SHA256 |
| --- | --- |
| microcap_top100_mom16_biweekly_live.py | f6408e3b4191de78e8e4d7e14a1fea388d42f6ce55b67de38c92302384cb9710 |
| microcap_top100_mom16_biweekly_live_v2_0.py | 87725ea0664ef529e360c6f4f1c9a07924c54f6b7e23462d9f85914ecbecb442 |
| microcap_top100_mom16_biweekly_live_v2_3.py | b933287913acec63f4e0027fd71ce4af4f4e72cc012bba7d58db7d89624eb48f |
| microcap_top100_mom16_biweekly_live_v2_5.py | a5675e6d9a10379fd3f0ab7ffaac738d9983fa1a4413588b03fc2876667765db |
| scripts/top100_delivery.py | 4027af3fa1681eb3a3c0632fe20fea9606d41a6f41b0a4adba98d8a5aa8fbe7f |
| scripts/top100_cloud_delivery.py | 2741d0b8a009cffa8069781d6f8a7072425e5f3017e1825bb6d035e519c08962 |
| scripts/top100_data_contracts.py | 6c2303e6fa8298fe91320882d5cdd4436fd39fb2e8ca594a67cdec50f97b6288 |

七份输入的新鲜度行数/起止日期、36 份实际文件的完整 raw SHA256、冻结旧六份源代码 SHA256、最终测试源码 SHA256 和正式 blocked manifest SHA256 均在机器可读最终清单中。报告只针对上述冻结的最终源码；其后源码改动需要相应验证。

旧 cloud 文件不在主冻结包中，但原文件已单独备份于 `.codex_backups/20261007_202622/scripts/top100_cloud_delivery.py`，本组独立读回 SHA256 为 `3243733dff1fbd0daeec831748ebfe2d6da3487ca4e3ca7849a5a2c1f361307f`。最终清单分别记载每份旧源码的备份来源；只有本轮新增的共享 helper 没有旧源码，不将缺少主冻结包条目误称为旧仓库没有文件。

Full / 10Y / 5Y / 3Y / 1Y 年化收益及最大回撤均 N/A：本轮只复核文件合同与经济路径不变性，不做新策略业绩研究。

**证据入口**

- [独立反例源码](D:/动量策略/微盘股对冲策略/tests/test_round3_state_contract.py)
- [确定修复前日志](D:/动量策略/微盘股对冲策略/research_reports/20261007_audit_round3/state_confirmed_old_red.log) 与 [JUnit](D:/动量策略/微盘股对冲策略/research_reports/20261007_audit_round3/state_confirmed_old_red.xml)
- [逐例原生入口与 matcher 证据](D:/动量策略/微盘股对冲策略/research_reports/20261007_audit_round3/state_confirmed_old_red_probes.jsonl)
- [误杀兼容控制](D:/动量策略/微盘股对冲策略/research_reports/20261007_audit_round3/state_old_controls.log)
- [实时成员动作修复前反例](D:/动量策略/微盘股对冲策略/research_reports/20261007_audit_round3/state_member_old_red.log) 与 [实际原始日期字段](D:/动量策略/微盘股对冲策略/research_reports/20261007_audit_round3/state_member_old_red_probes.jsonl)
- [真实数据、源码哈希及初测纠正清单](D:/动量策略/微盘股对冲策略/research_reports/20261007_audit_round3/state_old_findings.json)
- [最终 46 PASS 日志](D:/动量策略/微盘股对冲策略/research_reports/20261007_audit_round3/state_final_verified.log) 与 [JUnit](D:/动量策略/微盘股对冲策略/research_reports/20261007_audit_round3/state_final_verified.xml)
- [最终逐例入口、并发控制及前后哈希证据](D:/动量策略/微盘股对冲策略/research_reports/20261007_audit_round3/state_final_verified_probes.jsonl)
- [最终源码、36 份实读文件及边界清单](D:/动量策略/微盘股对冲策略/research_reports/20261007_audit_round3/state_final_validation.json)
- [晚增反例的冻结旧函数/最终函数对照](D:/动量策略/微盘股对冲策略/research_reports/20261007_audit_round3/state_frozen_matcher_before_after.json)
- [共享 helper 三处依赖绑定](D:/动量策略/微盘股对冲策略/research_reports/20261007_audit_round3/state_dependency_binding.json)

复现命令应使用新的独立 basetemp，保留本轮证据目录。Python 使用 -B 和 PYTHONDONTWRITEBYTECODE，pytest 使用 no:cacheprovider。所有本组修复前、夹具纠正和最终日志分别保留。
