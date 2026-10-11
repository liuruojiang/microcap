# 第二轮独立合同审查：v2.0 / v2.3 / v2.5

结论：本组独立构造并实际跑出了 **31 个失败反例**，归属下面 4 组合同遗漏。主 Agent 修补生产代码后，本组最终 **89 PASS、0 FAIL、0 ERROR、0 SKIP**。最后一次旧 fixture 修正另做了 **4 PASS** 的正反验证。本结论仅覆盖本报告明确列出的合同、原生调用流程和锁交错；不表示正式交付、远端上线、邮件送达或实际交易成功。

机器可读汇总：[contract_summary.json](D:/动量策略/微盘股对冲策略/research_reports/20261007_audit_doublecheck/contract_summary.json)。独占测试：[test_doublecheck_contract_adversarial.py](D:/动量策略/微盘股对冲策略/tests/test_doublecheck_contract_adversarial.py)。

## 真实输入与范围

主 Agent 在本轮重核最新完整收盘日为 **2026-09-30**；基准为第一轮同日正式刷新后的真实文件。本组读取该证明，再在 fixture 建立及结束时逐个核对 raw SHA256；结束及最后一次 fixture 验证后仍全部一致。

| 输入 | 行数 | 最新日期 |
| --- | ---: | --- |
| panel | 8,738 | 2026-09-30 |
| proxy index | 4,067 | 2026-09-30 |
| proxy turnover | 432 | 2026-09-17（名单事件流） |
| base costed NAV | 4,050 | 2026-09-30 |
| v2.0 NAV | 4,033 | 2026-09-30 |
| v2.3 costed NAV | 3,988 | 2026-09-30 |
| v2.5 costed NAV | 3,988 | 2026-09-30 |

新鲜度证据：[baseline_and_freshness.json](D:/动量策略/微盘股对冲策略/research_reports/20261007_audit_doublecheck/baseline_and_freshness.json)。turnover 是事件记录，9 月 17 日的末行不要求冒充 9 月 30 日的逐日记录。本组不计算或发布收益指标，不刷新官流，不改 authority、历史、策略参数，不部署、发信或下单。生产源和旧测试由主 Agent 修改；本组仅写独占测试及 `contract_*` 证据。

## 实际失败反例与最终修复

| 组 | 漏洞输入及修补前实际行为 | 最终源码位置与结果 |
| --- | --- | --- |
| C1，20 个红例 | 真实 final CSV 的复制品保留原有 NAV、日期和版本参数，分别改成 `official_close_confirmed_signal=False`、盘中 `signal_timing`、v2.3/v2.5 的 `strategy_version=v2.0`、有名单变化却 `member_rebalance_state=none`、`target_position_scale=9`、矛盾 `position_transition`、固定 1x 下 `scale_trade_required=True`。按实际组刷新顺序先 inspect，再写 complete manifest，再调用 delivery `main(["check"])`，旧门全部返回 exit 0。 | [validate_final_signal](D:/动量策略/微盘股对冲策略/scripts/top100_delivery.py:190)、[正式收盘门](D:/动量策略/微盘股对冲策略/scripts/top100_delivery.py:200)、[动作及规模别名](D:/动量策略/微盘股对冲策略/scripts/top100_delivery.py:230)、[名单 state](D:/动量策略/微盘股对冲策略/scripts/top100_delivery.py:356) 现在拒绝这些输入；缺字段、NaN、Inf 和不可解析规模也有最终绿证。 |
| C2，5 个红例 | 真实 v2.5 realtime 文件复制品分别给错误 `version`、错误 `strategy_version`、删 `version`、增加错误身份第二行、启用 overheat。原生 `v25.main(["--v25-output-prefix", prefix, "信号"])` 完成真实历史生成、成本、rewrite 检查及输出 staging 后，旧清理逻辑仍保留同一错误文件，字节不变。原因包括旧读取仅 `nrows=1` 且漏查版本/overheat。 | [v2.5 realtime validator](D:/动量策略/微盘股对冲策略/microcap_top100_mom16_biweekly_live_v2_5.py:1453) 现在要求恰好单行、两个版本字段和原生 flags；原生 CLI 能清理这些错误文件。 |
| C3，1 个红例 | v2.3 原生 realtime 同时含两个 overheat 字段，保持 `overheat_enabled=True` 但改 `overheat_overlay_enabled=False`。原生完整信号 CLI 在修补前保留矛盾文件。 | [v2.3 原生 alias 检查](D:/动量策略/微盘股对冲策略/microcap_top100_mom16_biweekly_live_v2_3.py:1282) 现在要求已有 alias 同为 True；原生清理绿证。 |
| C4，5 个红例 | final CSV 前置一个 `version=9.9` 重名列：Pandas 主列得到 9.9，DictReader 得到后面的真实版本，三版旧 delivery 都接受。realtime CSV 末尾追加一个 `version=9.9`：Pandas 主列仍为真实版本，DictReader 得到 9.9，v2.3/v2.5 原生 CLI 都保留文件。同一 CSV 有两个不同身份含义。 | [csv_info 唯一表头门](D:/动量策略/微盘股对冲策略/scripts/top100_delivery.py:103)、[canonical members 门](D:/动量策略/微盘股对冲策略/scripts/top100_delivery.py:124)、[v2.3 同一 handle 验表头并解析](D:/动量策略/微盘股对冲策略/microcap_top100_mom16_biweekly_live_v2_3.py:1266)、[v2.5 同一 handle 验表头并解析](D:/动量策略/微盘股对冲策略/microcap_top100_mom16_biweekly_live_v2_5.py:1459) 拒绝重名。另补 `rank=999` 与原生 rank 1–100 同名并存反例，最终也拒绝。 |

红证：[第一批 25 FAIL / 4 PASS](D:/动量策略/微盘股对冲策略/research_reports/20261007_audit_doublecheck/contract_before2_suite.xml)、[v2.3 alias 1 FAIL](D:/动量策略/微盘股对冲策略/research_reports/20261007_audit_doublecheck/contract_v23_before_suite.xml)、[重复表头 5 FAIL](D:/动量策略/微盘股对冲策略/research_reports/20261007_audit_doublecheck/contract_duplicate_before_suite.xml)。这些是 31 个失败测试输入，不是 31 个独立根因；对应 `.log` 及 [contract_probes.jsonl](D:/动量策略/微盘股对冲策略/research_reports/20261007_audit_doublecheck/contract_probes.jsonl) 留有具体运行结果及各阶段源哈希。

审查过程还保留了两类本组测试编写错误：`contract_before_suite.xml` 的 25 个 setup error 源于真实 turnover 混合 ISO 日期格式的解析；`contract_after_suite.xml` 的唯一失败源于把 v2.3 的 wrong-version 注入值误设为原值 2.3。修正后才运行最终套件；这两份不能作为生产漏洞红证。

## 原生关系、合法行为与并发

- 有名单差集时 `member_rebalance_state=rebalance` 且 `member_rebalance_required=True`，没有差集则 `none/False`。是否 actionable 还依赖 dated execution 合同，历史名单变化不能成为当前执行指令。
- `position_transition` 等于 current holding 与 next holding 是否不同。三版固定 1x 下，`scale_trade_required=False` 在开仓、平仓、持有和保持现金四种转换中均合法；开仓的规模差 1 不能误判为独立规模调仓。
- next 规模别名均须等于 `next_session_actionable_scale`，`execution_scale` 等于 current 规模，四个 delta 别名均等于 next 减 current。原生构造器的 **3 版本 × 4 合法转换，12 项** 全部通过。
- v2.3/v2.5 原生完整信号 CLI 在隔离输出目录中复算，各自 **3,988 行**日期、holding、next_holding、return_net 与真实正式成本流一致，收益列容差 `1e-12`。只有可能写官流的上游 loader 被替换成已从真实输入独立重建的同一 context；实际版本策略、成本、rewrite、staging 和清理步骤仍执行。
- 两个 deterministic writer handoff 测试让合法 realtime writer 在“首次判陈旧”与“cleanup 取原生锁再检查”之间发布。原生 v2.3/v2.5 cleanup 均保留新合法字节。这是指定交错的验证，未声称穷尽多进程竞争。
- 实际持有活 PID 的 generation lock 被设为一小时前，stale 阈值为 0；原生 v2.5 CLI 超时退出且不进入 generator、不抢活锁，退出锁上下文后才由持有人清理。

最终独立套件：[contract_final_suite.xml](D:/动量策略/微盘股对冲策略/research_reports/20261007_audit_doublecheck/contract_final_suite.xml)、[contract_final.log](D:/动量策略/微盘股对冲策略/research_reports/20261007_audit_doublecheck/contract_final.log)：**89 PASS，136.27 秒**。分组为 final 文件 61、v2.5 原生清理 6、v2.3 原生清理 2、合法 writer 2、合法转换 12、活锁 1、成员重名表头 1、v2.3 身份对应反例 4。

## 旧 fixture 修正是否掩盖问题

第一轮补充 native 必须字段的 pair/workspace/realtime fixture 保留了原有错误身份、动作和执行日期拒绝断言；它们补足真生成器已经输出的版本、正式收盘、member-state 字段。冻结 lineage fixture 的全文件哈希旧断言与已批准追加尾部不兼容，因此改为精确 seed prefix。本组独立读取 Git HEAD 的 authority 和跟踪文件，CRLF 统一为 LF 后分别计算 4,059/4,042 个冻结行加表头的 SHA256；均等于 authority 固定值，合法尾部只增加 9 月 18、21、22、23、24、28、29、30 日。对冻结经济单元做一个字节的研究副本扰动会改变 prefix SHA，仍被拒绝。见 [contract_fixture_audit.json](D:/动量策略/微盘股对冲策略/research_reports/20261007_audit_doublecheck/contract_fixture_audit.json)。本组没有改 authority 或官流。

主 Agent 的首次最终全库运行 **1047 PASS / 1 FAIL**，唯一失败为 `test_real_core_next_session_member_contract_is_accepted`：v2.0 调真实 augmenter，而 v2.3/v2.5 peers 手写 required/actionable/counts 等 8 字段，遗留 fixture 的 `member_rebalance_state=none`。新门正确拒绝。

修正为 peers 也调用真实 augmenter符合实际生产调用链：[v2.3](D:/动量策略/微盘股对冲策略/microcap_top100_mom16_biweekly_live_v2_3.py:2019)、[v2.5](D:/动量策略/微盘股对冲策略/microcap_top100_mom16_biweekly_live_v2_5.py:2199) 都会调用同一个 [v2.0 生成器](D:/动量策略/微盘股对冲策略/microcap_top100_mom16_biweekly_live_v2_0.py:11822)，由实际名单差集设置 state。本组最后额外跑“该合法案例 + 三版 state 矛盾拒绝”共 **4 PASS**，并直接读回三版临时 final CSV：均 rebalance、required/actionable/official=True、signal=2026-09-03、execution=2026-09-04、1 入 1 出。正反断言均保留，未放宽生产门。证据：[contract_fixture_verification_suite.xml](D:/动量策略/微盘股对冲策略/research_reports/20261007_audit_doublecheck/contract_fixture_verification_suite.xml)、[contract_fixture_final_verification.json](D:/动量策略/微盘股对冲策略/research_reports/20261007_audit_doublecheck/contract_fixture_final_verification.json)。修正后的全库最终验收由主 Agent 负责。

## 冻结源码与边界

最终 89 项测试绑定下面 raw SHA256，最后定稿时独立复核未变：

| 文件 | raw SHA256 |
| --- | --- |
| v2.0 | `bdf7d6fafc4230da4d2b248586837ea50204e49742dd67d8662b880261443171` |
| v2.3 | `615cb400d827727f524f08ecf738b49bc237e41cfda5823c26d218beeb3b834c` |
| v2.5 | `42b5ff2cbf455677312466250f3d06076a853f28becabf8b7209a42daf860267` |
| top100_delivery.py | `bc82ffcb7a960293a665d1e71c8d7c43cb252c4b5be146204ad1236eb1d5e0df` |
| 独占测试 | `4b24e3a6840f9145df36771efeb3f6875d2d901060a64bd56fc926a3c9b7c36a` |

final CSV 的 CLI 测试只在复制出的临时 workspace 中隔离 remote release、target-date 和 base-state 门，用来证明 final 文件能否错误自认证。该隔离只存在测试进程，没有改正式程序的远端发布门，不能用于正式 `refresh-all/check` 的整组验收。真实实时行情新鲜度、报价锚点发布、PIT 历史准确性、成交价、融资和实际可执行收益不由本组身份/清理测试认证。所有临时目录和红证均保留，没有删除目录。当前本组已证明的合同遗漏均有原生或 CLI 绿证，没有尚未修复的已复现合同缺陷。
