# 共享数据基座与历史血缘脚本对抗审查（2026-10-07）

已修复 9 组可复现的数据、缓存、历史改写和成本时点问题。最终隔离反例 **82 passed**；对同一份修复前源码备份运行相同用例为 **60 failed、22 passed**。失败数是反例变体数量，不是 60 个独立 bug。正常真实数据上，旧/新收盘价、基础信号、基础成本计算和正式 v2.0 调用链逐列精确相等；正式 v2.0 的全部冻结关键列与已有发布 CSV 匹配，NAV 最大绝对浮点差仅 `3.552713678800501e-15`。

本轮仅做脚本修复和计算一致性检查，没有迁移历史成员、改变参数、改写 authority、发邮件或下单。v2.3/v2.5 的版本函数由其他审计 Agent 负责；本报告负责它们共用的 v2.0 嵌入基座，以及独立 `microcap_top100_mom16_biweekly_live.py` 中对应入口。

## 修复与复现

| 缺陷 | 旧实现反例与影响 | 修复位置及行为 |
| --- | --- | --- |
| 收盘后可选择未来日期 | 在 2026-09-30 16:00 输入 09-29、09-30、10-08，旧函数返回 10-08；未来来源行进入 panel 的刷新截止日。 | 两份脚本的 `latest_closed_history_date` 始终先限定日期不晚于北京时间今天；只有未来行则拒绝建立收盘锚点。盘前仍排除当日未完成收盘。 |
| 无效日期静默删除 | `_normalise_dated_frame` 删除 NaT 日期列行，DatetimeIndex 的 NaT 也未明确拒绝，可能隐藏损坏记录。 | 两份脚本对无效日期和 NaT 明确失败；保留排序、重复日期拒绝和合法暖机缺值语义。 |
| 非有限值及混合文字可绕过历史改写审计 | v2.0 的 `inf > inf` 返回 False，有限值改为无穷及无穷改符号可能未检出；数值列中两个无法解析的不同字符串均被强制转换成 NaN，也被视为相同。 | 两份脚本用带既有容差的 `isclose(equal_nan=True)` 比较数值，同时比较无法解析的原始单元格。真实暖机 NaN 不制造误报。 |
| 独立基础脚本忽略历史集合/关键列改动 | 删除历史日期、插入已发布日期范围内的新日期、删去关键列，原独立脚本仅比较交集或跳过缺列。 | 独立脚本采用共享版本的历史集合、关键列和逐列冻结逻辑；其默认绝对容差保留 `1e-10`、相对容差保留 0。合法新尾部仍允许，历史缺失/插入/缺列拒绝。 |
| 缺失价格压缩交易窗口 | 中间某个交易日的 hedge/proxy 行缺失或价格 NaN，旧 `dropna()` 让日期消失，悄悄改变动量与执行滞后。 | 两份 `load_close_df` 只按有效 proxy 的首末窗口裁剪，保留窗口内部并集中的每个会话，随后验证价格；任一腿缺失、NaN、inf、非正价均阻断。 |
| 表现入口接受损坏收益与 NAV | 部分 NaN 被后续 `fillna(0)` 当成零收益；inf、非正 NAV、低于或等于 -100% 的收益、与收益不一致的 NAV 也可通过。 | 两份 `validate_performance_frame` 要求有限收益/正 NAV、有效收益范围，并独立核对相邻 NAV 比率与当天收益。合法曲线逐行保留。 |
| 当前非 ST 的证券缓存忽略身份与政策漂移 | 外来 symbol、显式过期政策、已从当前 ST 快照摘除但旧缓存仍有 ST 名字，均可直接复用。 | 嵌入 `FREQ_SOURCE.load_security_meta` 核 symbol、显式政策和当前快照名字；不匹配即沿现有数据重取/历史改写门处理，不把当前 ST 倒投到过去。 |
| 实时缓存缺少 metadata 仍被放行 | proxy、costed 和 turnover 日期都新鲜时，缺失 `proxy_meta` 的分支跳过兼容性和内容指纹校验。 | 嵌入及独立两入口的 `reusable_cached_proxy_end_for_realtime` 都要求 metadata 文件存在，缺失即返回不可复用。 |
| 成本日期错移、重收和非法费率 | next-open 已提供 `execution_date` 时仍再向后找一日，费用延迟；执行日早于结果窗口的费用重新映射到首日；NaN/inf/负数/100% 费率未阻断。 | `COST_SOURCE.map_rebalance_apply_costs` 对显式执行日直接映射，仅 legacy 的 next-open 信号日加一会话；已付窗口前成本跳过；索引、执行时序、日期及费率须有效。正式 close 的 432 条事件没有改变计算。 |

## 真实数据、新鲜度与 parity

主 Agent 在修改前执行正式 `top100_delivery.py refresh-all` 和独立 `check`，均成功。本 Agent 随后从写回文件重新读取日期/行数；最新完成收盘为 **2026-09-30**。

| 输入/正式流 | 行数 | 首日 | 最后日 |
| --- | ---: | --- | --- |
| `v2_0_base_panel_refreshed.csv` | 8738 | 1990-12-19 | 2026-09-30 |
| 公开/本地 Top100 proxy index | 4067 | 2010-01-05 | 2026-09-30 |
| 基础 costed NAV | 4050 | 2010-01-28 | 2026-09-30 |
| proxy turnover 事件 | 432 | 2010-01-14 | 2026-09-17 |
| 正式 v2.0 costed NAV | 4033 | 同正式文件 | 2026-09-30 |
| 正式 v2.3 costed NAV | 3988 | 同正式文件 | 2026-09-30 |
| 正式 v2.5 costed NAV | 3988 | 同正式文件 | 2026-09-30 |

turnover 是换仓事件流，09-17 是最新正式事件；不把事件流的末日误要求为 09-30。三份正式成本流均通过有限收益、正 NAV 和收益/NAV 链式一致性检查。完整路径和 SHA256 在 [data_real_parity.json](data_real_parity.json)。

同源复核保留真实正式调用链：`load_close_df` 的 4067 日 → 基础 `run_signal` 的 4050 日 → v2.0 原入口从基础价格列再次运行信号 → 正式 4033 日，再执行同一成本模型。没有把第一层 4050 日 helper 曲线冒充正式 v2.0。old/new 四层均用 `check_exact=True` 逐列一致；与现有正式 CSV 的冻结关键列比较不允许任何尾行豁免。脚本见 [data_real_parity.py](data_real_parity.py)，只读官方输入，输出证据写在本任务目录。

实际 4975 份证券 metadata：symbol 与文件名全部匹配，`st_intervals` 全部为 list，meta_version 全部 2；3701 份缺少新版 notice-policy 字段，1274 份带当前 v3 字段。当前 ST 快照 201 行；metadata 的当前 ST 名字与该快照 **零漂移**。因此没有把“旧文件缺 policy”直接升级为 3701 次历史重建：保留已批准内容指纹下的 legacy 非 ST 读取，显式不匹配仍拒绝。缺字段不是历史政策已经被完全证实的证据。

## 验证、独立交叉审查与恢复

- 最终新增反例：`python -B -X utf8 -m pytest -q tests/test_script_data_adversarial.py -p no:cacheprovider`，**82 passed**，无 skip。结果：[data_fixed_after.txt](data_fixed_after.txt)。
- 用 `MICROCAP_DATA_AUDIT_BASELINE_ROOT=.codex_backups/20261007_143135` 指向同一备份，执行同一组反例，**60 failed、22 passed**：[data_backup_baseline.txt](data_backup_baseline.txt)。所有人工反例仅进入隔离临时目录。
- 数据/冻结尾段/metadata/现有 v2.0 对抗回归：修改最后一处独立 metadata gate 前 **277 passed**；最后两行 gate 和新增同源 case 之后仅运行受影响的 82 项小组，主 Agent 再负责全库最终验证。记录：[data_regression.txt](data_regression.txt)。
- 真实 parity：`python -B -X utf8 research_reports/20261007_script_adversarial/data_real_parity.py`，零退出，三条正式流有限值/正 NAV/链式一致性全部通过；不生成官方输出或发起数据刷新。
- 独立复核主 Agent 的 `top100_delivery.py` ASCII 代码、rank 1–100、signal_label/仓位动作一致性修复，未发现反例；生成器及用户界面的其他合同由对应 Agent 独立负责。
- 旧 authority fixture 的失败是将合法延伸到 09-30 的 HEAD 全文件误当 09-17 种子。独立读取 HEAD 证明 proxy/base costed 的 4059/4042 行种子前缀 SHA 都精确匹配 authority，成员/换手/有效成员三份文件整体 SHA 匹配，尾部 8 行 metadata 与 authority 绑定。主 Agent 的新 fixture 仍锁这些精确种子前缀，不重写 authority、不把整段尾部哈希冒充种子。
- 可恢复备份：[manifest.json](../../.codex_backups/20261007_143135/manifest.json)，已由主 Agent 逐文件 SHA 核验，包含本 Agent 修改的两份脚本；源码差异及编码检查 `git diff --check` 通过。

全库先行运行的 7 个 skip 来自独立邮件摘要 fixture，默认旧 worktree 下的 `build_microcap_realtime_digest.py` 已不存在，需 `MICROCAP_DIGEST_SOURCE` 指向那个独立项目的实际源文件。该缺失不会把邮件摘要测试算作 PASS；本报告的 82 项没有跳过用例。

收尾时，平台对删除本 Agent 隔离临时目录的操作返回 `blocked by policy`，删除未执行，已停止该清理目标且没有换工具重试。仍待用户手动或平台正式渠道处理的目录都在本报告目录内：`data_baseline_tmp`、`data_fixed_tmp`、`data_regression_tmp`、`data_backup_baseline_tmp`、`data_final_tmp`、`data_locked_final_tmp`、`data_locked_before_tmp`。测试源码、日志、parity 证据、报告及备份均应保留。

## 当前结案边界

以项目中的 [L7_SYNC_CLOSEOUT.md](../20261003_l7_repair/L7_SYNC_CLOSEOUT.md) 和 [L8_DECISION.md](../20261003_l8_diagnostic/L8_DECISION.md) 为当前状态：L7 代码/交付同步已结案；L8 三版冻结旧代理的纸面计算与交互审计均为条件 PASS，现有正式参数保留。旧的“L2 尚暂停”记忆已经不是当前项目状态。

这次脚本检查不改变未认证事项：完整历史 PIT ST/成员、2018 历史休市伪行、公司权益事件、真实成交价与回执、真实股数/期货张数/保证金/现金及共享资金账户。当前正常曲线 parity 不能为这些缺口背书；必要数据更正仍需精确哈希迁移和无迁移参数的第二次清洁重跑。对 panel/proxy 同时缺少、且未出现在任一腿中的历史日期，单靠本 loader 的并集也不能认定日历完整，必须由独立交易所日历门和历史迁移证据处理。本文没有发布新收益/回撤排名，也没有启动参数扫描或自动晋级。
