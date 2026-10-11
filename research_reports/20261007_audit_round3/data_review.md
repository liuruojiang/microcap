# 第三轮独立数据、冻结与事件费用边界复核

本组复核完成：上一轮源码仍有 **7 组已实跑的遗漏或合法输入误拒**。同一份最终 57 项诊断，冻结旧源为 **40 failed、17 passed**，主 Agent 整合后的源码为 **57 passed**；受影响的数据与 ST 保护回归另为 **125 passed**。本组当前合计 **182 passed、0 failed、0 errors、0 skipped**。失败数量是输入变体数，不是 40 个独立问题；没有把上一轮已测反例再次计为新发现。

范围为 v2.0、v2.3、v2.5 的真实共享函数与消费链，v2.6 排除。源文件补丁由主 Agent 顺序实施，本 Agent 仅写 `tests/test_round3_data_boundary.py` 与本目录 `data_*` 证据。没有改生产、正式 `outputs/`、缓存、authority，也没有部署、发信、下单、历史迁移或删除。

## 已执行的反例与修补

| 组 | 最小输入及旧源实际行为 | 新源复核结果 |
| --- | --- | --- |
| D3-1：混合合法日期误拒及候选哈希日期丢失 | 同列 `YYYY-MM-DD` 与 `YYYY-MM-DD 00:00:00`，两个行序都合法且为无时区午夜标签。旧 daily/freeze/close/turnover 入口因单格式推断拒绝。旧 `_candidate_frame_sha256` 将合法中间午夜文字日期转为空；中间日 09-02 与 09-03、同首末日/行数/收益均得 `24dd4fbb…`。 | daily 入口按混合格式解析并保留 NaT、时区、午夜及重复会话检查；候选哈希验证并规范每个日期。正常格式表示获得同一规范哈希，不同中间会话仍获不同哈希；三版迁移对照同时验证合法候选接受、改中间日拒绝。旧 verifier 原本会先拒混合候选，没有宣称旧端到端迁移可以越过。 |
| D3-2：正式读取链在费用校验前静默删事件 | 2 行事件中，第二行是合法午夜日期，或缺失/NaT/坏信号日，另含缺日期且 `inf` 成本变体。真实 `embedded_context._load_embedded_base_context` 先 `errors=coerce/dropna`，2 行变 1 行；shared/mainline 的真实 `rebuild_costed_nav_from_proxy_turnover` 也漏费或成功写出坏输入对应的隔离 NAV。 | 统一 turnover 契约先验证原始事件。合法 2 行保留，费用实收；坏事件在 NAV 写入前拒绝，不靠下游映射器再补漏。 |
| D3-3：重复事件双收费用 | 同一 rebalance 事件重复两次，费率各 0.006；mapper、loader、embedded 三路均接受，映射为 0.012。 | 规范事件身份重复即拒绝。 |
| D3-4：显式执行日掩盖非会话信号日 | 结果会话为 09-04/07/08，信号写周六 09-05、执行写周一 09-07；旧函数接受。 | 窗口内信号日也须精确命中会话。已有窗口前显式已付费用、首窗口执行费用的合法边界仍通过既有回归。 |
| D3-5：事件行序改变整条策略的费用语义 | 同表含 `close` 与 `next_open`，仅反转行序，真实 `apply_cost_model` 的 entry/exit 从 `[0.003,0,0.003]` 变 `[0,0.003,0]`。 | 策略整体采用同一 timing 的模型拒绝混合表；合法统一 close 和 next_open 的进入/退出语义保留。 |
| D3-6：真正暖机空值被误报历史改写 | 冻结列所有行双方都 `isna=True`，仅 np.nan/None/pd.NA 表示不同；shared/mainline 六个变体均错报 rewrite。 | 双方真正缺值不计改写；原始 `isna XOR` 保留。上一轮缺值到字面 `nan`/`None`/`<NA>` 的六项负例仍拒绝。 |
| D3-7：迁移批准字段类型未严格校验 | 所有精确证据都匹配，只把 JSON `approved: true` 改成整数 `approved: 1`；三版以 `== True` 接受。 | 必须是实际布尔 True。真实布尔批准对照仍接受，12 个逐项篡改哈希对照仍拒绝。本项没有证明任意哈希迁移可以越过审核。 |

ST 历史证据保护本轮没有新发现。其同证券身份漂移、源失败保留字节、可靠来源历史变更须迁移、短代码/整数代码及外来证券等既有负例作为回归保留，未重新计数为新问题。

## 费用进入实际执行收益的证据

反例调用真实 `run_signal` 产生 `holding=long_microcap_short_zz1000`，再调用真实费用函数。只隔离刷新/锁/参考摘要 I/O，未复制策略公式。该价格与日期序列是明确诊断输入，不是市场业绩或真实交易。

| 09-02 诊断字段 | 旧读取链 | 同一 gross + 完整合法原始事件 | 新读取链 |
| --- | ---: | ---: | ---: |
| 原始/消费事件行数 | 2 / 1 | 2 / 2 | 2 / 2 |
| `rebalance_cost` | 0 | 0.002 | 0.002 |
| `return_net` | 0.007232936794319578 | 0.005218470920731022 | 0.005218470920731022 |

[旧 active 探针](data_probe_before_active/data_probes_result.json) 与 [新探针](data_probe_after/data_probes_result.json) 各实际执行 14 个观察项。旧 active 证据读回 40 个输入/消费/费用/报告文件，新证据读回 34 个：坏输入被更早拒绝，因此其消费输出没有生成。新合法事件的完整原始事件费用 CSV 与实际消费费用 CSV 原始 SHA 相同 `49c722a2…`。初版只检映射的探针也保留为 `data_probe_before/`，收尾结论采用 active 对照。

## 同批真实输入只读复核

先读取主 Agent 本轮 [新鲜度证明](baseline_and_freshness.json)：最新完成收盘为 **2026-09-30**，真实输入与同日成功刷新原件相同。正规 `refresh-all` 已尝试但被 local/remote source gate 阻断，正式 manifest 保持 blocked；未绕过门禁。收到证明后才启动隔离诊断。

[真实输入读回](data_real_checks.json) 核对 7 份输入：panel 8738、proxy 4067、基础 costed 4050、v2.0 4033、v2.3/v2.5 各 3988 行，6 个日频流末日均 09-30；turnover 为 432 条事件、末个实际事件 09-17，事件表不伪装日频流。各输入没有坏日期、重复会话或附带时刻标签；实际两腿对齐 4067 行，价格全部有限。四条基础/版本 costed 现存流通过有限收益、正 NAV 与逐日 NAV 链式一致性检查；这不是新版本全史重建或正式认证。

真实 loader 保留 **432/432** 事件，全部为 close，432 条显式有效执行日，0 条缺失、0 条非空解析失败、0 个重复 rebalance。执行日期文字为 431 条日期加 1 条合法午夜时间，没有重复上一轮诊断解析错误。四条流各截最近 100/500/1500 行时，直接映射费用与先映射全史再截窗的最大差均为 0。

4975 份本地 metadata 只读检查：4975 可解析，1274 当前 policy、3701 legacy 缺 policy、520 非空 ST 历史；未发现 symbol 不匹配。全部文件字节在检查前后相同，内容清单 SHA 为 `affc8d2e…`。这只是 schema/内容读回，不证明完整历史 ST 证据。7 份正式输入原始 SHA 也全不变。

## 复现与证据

[机器汇总](data_summary.json) 绑定当前 7 个生产源、冻结源、同一测试文件、正式输入与证据文件 SHA，并由实际 XML 读出计数。关键日志为 [57 项旧源](data_final_baseline.log)、[57 项新源](data_final_current.log)、[125 项数据/ST 回归](data_st_regression.log)。初次 30 项、追加暖机、base 重建与日期哈希红证保留，不把它们叠加为额外问题。

```powershell
$env:PYTHONDONTWRITEBYTECODE = '1'
$env:MICROCAP_ROUND3_SOURCE_ROOT = '.codex_backups/20261007_200436'
python -B -X utf8 -m pytest -q tests/test_round3_data_boundary.py -p no:cacheprovider --basetemp=research_reports/20261007_audit_round3/data_replay_new_old
$env:MICROCAP_ROUND3_SOURCE_ROOT = ''
python -B -X utf8 -m pytest -q tests/test_round3_data_boundary.py -p no:cacheprovider --basetemp=research_reports/20261007_audit_round3/data_replay_new_current
python -B -X utf8 -m pytest -q tests/test_doublecheck_data_adversarial.py tests/test_script_data_adversarial.py tests/test_security_meta_preserve_20260917.py -p no:cacheprovider --basetemp=research_reports/20261007_audit_round3/data_replay_new_st
python -B -X utf8 research_reports/20261007_audit_round3/data_probes.py --out-dir research_reports/20261007_audit_round3/data_replay_new_probes
python -B -X utf8 research_reports/20261007_audit_round3/data_real_checks.py
```

复跑使用新的隔离目录，避免 pytest 自动清理既有证据。Full/10Y/5Y/3Y/1Y 年化及最大回撤均 **N/A：本轮不开展策略表现或参数研究**。完整 PIT 成员/ST 历史、公司权益、成交可执行性、统一资金账户、历史迁移第二次无选项 clean audit 及正式远端交付没有由这些工程诊断认证；全库回归与最终交付裁决由主 Agent 汇总。
