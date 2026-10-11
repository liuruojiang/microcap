# 第二轮独立数据、冻结与成本日期复查

第一轮补丁仍有 **5 组可复现遗漏，其中 ST 缓存刷新会破坏既有历史证据，属于误修**。没有把第一轮 856 项通过作为结论前提。新增 35 个独立正反边界用例；本轮起始源码备份 `.codex_backups/20261007_161253` 为 **26 failed、9 passed**。主 Agent 已整合共享源码修复，新增 35 项及受影响回归最终 **125 passed、0 failed、0 errors、0 skipped**；最终全库裁决由主 Agent 汇总。

本 Agent 仅新增本测试和 `data_*` 审查证据。生产源码补丁均由主 Agent 顺序实施，未刷新、写入官方输出或证券 metadata，未迁移历史、部署、发信、下单或删除目录。反例数据是诊断输入，不是市场业绩。

## 发现、输入与影响

以下行号先引用 **本轮修复前冻结源码**，所以即使随后整合导致行号移动，失败输入仍可重现。共享函数由 v2.0、v2.3、v2.5 的正式调用链使用。

| 问题 | 原位置与可执行输入 | 实际失败行为与影响 | 已整合的修复及边界 |
| --- | --- | --- | --- |
| **P1：身份漂移绕过历史 ST 保护** | `161253/microcap_top100_mom16_biweekly_live_v2_0.py:2857` 的 `_preserve_security_meta_evidence`，配合 `load_security_meta`。缓存为同一证券、非空 `st_intervals=[2020-01-02..2021-01-04]`，政策过期或 `current_st_snapshot_name` 漂移；名称/公告源失败。 | 第一轮新增的 loader 校验会触发真实 builder；旧 preserve 在任一 identity 项变化时直接返回 candidate，最终把缓存写成空历史区间且没有迁移异常。即使来源成功但历史起点改成 2020-01-03，也因身份变化绕过审核。 | 同证券先保护非空历史，不以 policy/snapshot 是否相同决定是否保护。身份漂移且源失败时抛 `SecurityMetaHistoryCorrectionRequired`，保留原文件字节；可靠来源改历史仍进入精确迁移门。来源可靠且区间完全不变的身份升级允许。外来 symbol 的历史不绑定本证券。`"1"` / 整数 `1` 和 `000001` 同证券归一化攻击也通过。 |
| **P2：原始缺值与同显示文字漏检** | 共享冻结比较 `161253/...v2_0.py:8785`、独立入口 `161253/...live.py:3850`。混合数值列 `1.0, np.nan, 2.0` 的 NaN 改成字符串 `nan`，或 None→`None`、pd.NA→`<NA>`。 | 两端数值化后都是 NaN；`equal_nan=True` 且 `astype(str)` 显示相同，审计清洁通过。缺值到损坏文字的历史改写被隐藏。 | 所有比较分支额外核对原始 `isna()` 的 XOR。真正未变的暖机 NaN 保留，非法字符串变更仍拒绝；不改变原数值容差。 |
| **P2：日频日期可以含重复会话/时区** | `161253/...v2_0.py:8641`、`161253/...live.py:3704` 的 normalise，加到两腿价格同一日的 12:00 记录。 | 原午夜 + 12:00 被认作两个日期，30 个会话变成 31 行；按行数计算的动量窗口和滞后失真。时区日期也缺少统一日频契约。 | 日期规范化入口要求无时区且为午夜日标签，拒绝附带时刻的 daily close/frozen frame。真实 7 份输入都符合此契约，未发现误拒。 |
| **P2：显式成本执行日仍被挪动/可早于信号** | `161253/...v2_0.py:917` 的成本映射。执行日 09-07 12:00，窗口 09-07/08/09；或窗口含 09-04/07/08 而执行日写 09-05；或 execution_date=09-01、rebalance_date=09-02。 | 12:00/非会话日期被 `searchsorted(left)` 移至下一个会话；早于信号的执行费用仍被计入。已有 execution_date 还会掩盖 NaT/文字损坏的 rebalance_date。 | 两种日期都须合法无时区日标签；显式执行日不得早于信号，窗口内日期须精确命中会话。合法窗口前已付费用跳过，明确首会话执行费用仍保留。 |
| **P2：legacy next-open 在截短窗口首行重收** | `161253/...v2_0.py:938` 只跳过 `side=left` 的窗口前成本。next-open 事件 rebalance=09-01、无 execution_date；窗口从 09-21 开始。 | 首行错误收取 0.006。把事件改成紧邻窗口前的 09-18，函数也无法区分是否应在首行执行：仅靠裁剪后的日期索引没有足够日历信息。 | 窗口前 legacy next-open 必须提供 explicit execution_date，否则 fail closed。不猜首会话，也不把所有窗口前事件静默丢弃。现有真实 turnover 均 close，合法 close legacy 保留。 |

最重要的 ST 误修还做了前后对照：第一轮修复前 `.codex_backups/20261007_143135` 的 loader 没有进入该危险 builder 分支，原始两项数据保留检查及保留控制共 **3 passed**（该结果不代表它的旧身份校验正确）；第一轮补丁同输入确实覆盖了历史。证据在 [data_before_first_meta.log](data_before_first_meta.log)。

## 35 项实际攻击与合法边界

反例不依赖模拟 builder 的成功返回，ST 测试调用真实 `load_security_meta → build_security_meta → _preserve_security_meta_evidence` 链，只把网络源及文件路径替换为隔离输入。可靠公告源测试使用真实 ST 公告区间函数，未复制它的解析公式。

- 共享及独立冻结门：真实缺值与同显示文字的 6 个变体、未变暖机 NaN、同一会话两行、带时刻及时区日标签。
- 成本：close/next-open 显式时刻、周末非会话、执行早于信号、坏/NaT 信号日、远早于/紧邻窗口前 legacy next-open、窗口前明确已付成本、首会话明确执行费用。
- ST：源失败与可靠来源改历史两类攻击；policy/snapshot 漂移、短代码/整数代码、来源可靠且历史区间不变的正常身份升级、外来证券区间不能继承、同身份源失败保留原证据。

扩展红证 [data_final_baseline.log](data_final_baseline.log) / [XML](data_final_baseline.xml) 是 **26 failed、9 passed**，对应同一冻结源码。失败是输入变体数，不是 26 个独立 bug。当前新增 35 项与第一轮数据/ST 受影响回归最终 **125 passed、0 failed、0 errors、0 skipped**，见 [data_final_regression.log](data_final_regression.log) / [XML](data_final_regression.xml)。两个旧 `inserted_date` 夹具曾用 12:00 日期，提前触发新日频门；主 Agent 将其改成合法午夜历史插入 2026-08-31，仍保留 RuntimeError 的历史日期集合审核断言，没有放宽异常类型。初次失败证据保留在 [data_regression.log](data_regression.log)。

## 真实同批输入只读复核

主 Agent 先独立确认最新已完成收盘仍为 **2026-09-30**，读回 SHA 与第一轮同日正式 refresh-all/check 后相同，证据 [baseline_and_freshness.json](baseline_and_freshness.json)。收到确认后才运行真实样本诊断。没有把假期 10-07 视为新交易日，也没有绕过本地/远端尚未同步的正式发布门。

[data_real_checks.json](data_real_checks.json) 独立读取 7 份实际输入：panel 8738、proxy 4067、基础 costed 4050、v2.0 4033、v2.3/v2.5 各 3988 行，末日均 09-30；turnover 432 条事件，末日 09-17。7 份输入均无 NaT、重复会话或附带时刻标签。实际对齐价格 4067 行且全有限；四条基础/版本 costed 曲线均通过正 NAV、有限收益及逐日 NAV 链式一致性检查。

432 条 turnover 都是 close，**432 条全部有显式有效执行日，0 条 legacy 缺 execution_date、0 条非空字段解析失败**。源列的前 431 行为 `YYYY-MM-DD`，2026-09-17 尾行是 `2026-09-17 00:00:00`；全部解析后都是午夜会话标签，在 proxy 会话中且没有早于信号。费率无非法值。实际费用映射分别截取最近 100/500/1500 行，与先映射全史再裁剪完全一致。

收尾独立读回发现并修正了本报告的诊断解析错误：旧脚本 `pd.to_datetime(errors='coerce')` 从首行推断单一日期格式，将非空的尾行午夜时间文本误判为 NaT，错误报告“431 explicit / 1 legacy”，并曾误写为“初始 legacy 事件”。这不是原数据缺失或 dropna 的行索引错套；正式成本函数逐行 `pd.Timestamp` 一直正确解析尾行。诊断脚本现用 `format='mixed'`，并分别统计原始字段缺失和非空解析失败，真实重跑后为 **432 / 0**。修正前脚本/证据保留为 `data_real_checks_before_execution_parse_fix.*`，当前结论以 `diagnostic_parser_revision=2` 的 [data_real_checks.json](data_real_checks.json) 为准。

只读检查 4975 份本地证券 metadata：全部可解析、身份与当前 ST snapshot 无漂移；1274 份当前 policy，3701 份 legacy 缺 policy；520 份非空历史区间。未发现本轮 ST 故障在当前真实缓存中已经发生。缺 policy 或 schema 合法都不能证明完整 PIT ST 历史已认证。该轮读后官方输入原始 SHA 全部不变。

## 复现与边界

```powershell
$env:PYTHONDONTWRITEBYTECODE = '1'
# 使用新的隔离目录名，避免 pytest 对已有 basetemp 自动清理。
$env:MICROCAP_DOUBLECHECK_SOURCE_ROOT = '.codex_backups/20261007_161253'
python -B -X utf8 -m pytest -q tests/test_doublecheck_data_adversarial.py -p no:cacheprovider --basetemp='research_reports/20261007_audit_doublecheck/data_replay_new_baseline'
$env:MICROCAP_DOUBLECHECK_SOURCE_ROOT = ''
python -B -X utf8 -m pytest -q tests/test_doublecheck_data_adversarial.py -p no:cacheprovider --basetemp='research_reports/20261007_audit_doublecheck/data_replay_new_current'
python -B -X utf8 research_reports/20261007_audit_doublecheck/data_real_checks.py
```

Full/10Y/5Y/3Y/1Y 年化和最大回撤均 **N/A：本轮只查代码补丁和数据/成本契约，不开展表现研究**。完整历史 PIT 成员/ST、2018 历史休市伪行、公司权益、真实成交及统一资金账户仍以仓库 L7/L8 决策的未认证边界为准，不能由正常曲线 parity 或这些单元用例解除。
