# 本地日报独立对抗审查

发现并修复实际盘中发布截止漏洞；本地日报当前明确是 Codex 任务内报告，不是 SMTP 邮件。远端发送、收件箱送达须由主协调和发送审查智能体另行取证，本智能体没有发送邮件。

2026-10-07 22:39:16 北京时间独立查询 `scripts.exchange_calendar`：`akshare.tool_trade_date_hist_sina` 确认当日休市，最新完成交易日为 2026-09-30。人工审查不启动定时任务；实际定时任务的休市规则要求在测试、状态、行情和邮件操作之前退出。

## 实证问题

1. **实际实时入口允许收盘后快照。** 修补前，v2.0 内嵌 base 真实日历 guard 与 written-row 兼容合同都接受上海时区 15:00:00、15:15:00、15:29:59，以及 UTC 等价时点。原因是独立日历按 15:30 判断完成收盘；仅检查上一完成 session 无法执行本地 15:00 硬截止。隔离反例首次实跑为 2 PASS、8 FAIL，保存在 `deadline_results.xml`，失败确为应拒绝却被接受。
2. **本地离线门禁清单尚未集中。** 审查时实际 `ic-im/automation.toml` 主微盘命令只有 7 文件；有效后续追加条款另列 9 文件，累计 16。执行主命令的 173 项不能代表全部累计门禁完成。主协调应统一清单及明确执行一次，保留 60 秒硬上限。
3. **旧打包器测试默认跳过。** `tests/test_adversarial_delivery_plain_20260904.py` 默认从已不存在的 `D:/Codex/worktrees/v23-plain-digest-20260904/scripts/build_microcap_realtime_digest.py` 读取打包器，未设置 `MICROCAP_DIGEST_SOURCE` 时 7 个打包器反例会跳过。本轮明确指向当前正式同步自动化仓库打包器，完整 16 文件实际 368 PASS、0 FAIL/ERROR/SKIP，pytest 55.82 秒、外层 56.90 秒。正式任务应固定当前真实打包器并拒绝源码缺失/跳过冒充通过。

## 修补与路由

已先备份并逐项核对原始 SHA-256：`.codex_backups/20261007_224521`。仅修改：

- `scripts/top100_data_contracts.py`：共享时点合同。实际发布同时核对上海快照时间和当前时间严格早于 15:00:00；归档行只检查其快照时点，保留晚间审阅合法历史盘中行的兼容语义。
- `microcap_top100_mom16_biweekly_live_v2_0.py`：真实三版本共享入口位于 `BASE_SOURCE` 内嵌 base，编译文件为 `<embedded_top100_base>`；它不是同名 shared main 的 guard。真实 `assert_realtime_anchor_precedes_quote_trade_date` 增加时点门禁；错误被正式 BLOCKED 呈现识别。三版本实际写 CSV 前共同调用的 `overlay_mod._apply_realtime_meta_columns_to_signal_row` 对完整发布 meta 再校验，防止采集通过后计算跨越 15:00。仅复制不完整来源字段的辅助测试保持原语义。
- `microcap_top100_mom16_biweekly_live.py`：旧 shared main 的 actionable 路径亦核对快照和当前截止时刻。
- 新增 `tests/test_realtime_publication_deadline.py`：34 个正式回归案例。

v2.3 / v2.5 源码、收益规则、参数和策略身份没有改动。真实三版路由均为：各自 builder → `v2_0.realtime_core.load_realtime_base` → 内嵌 base actionable guard；计算后 → `v2_0.overlay_mod._apply_realtime_meta_columns_to_signal_row` → 各自 CSV 写入。慢算反例分别运行三版真实 builder、隔离计算/写入和时钟；14:59:59 完成对照均写一次，15:00 完成均拒绝且写入调用为零。

## 验收与边界

- 修补后的最终专项 **83 PASS，0 FAIL/ERROR/SKIP，4.97 秒**：正式截止 34 例、最初独立反例 10 例、原生成员日期合同、独立实时日历、v2.5 合法归档清理、旧来源字段保留及实际锚点重要回归。
- 完整原 16 文件的 **368 PASS** 是修补前独立审计运行；修补后上述 83 项通过。主协调负责最后完整门禁、发布和三版本整组刷新，不能把两次不同范围计数相加当一次回归。
- 初始真实 panel/proxy/base NAV/三版本最终流读回均为 2026-09-30；turnover 是事件表、最后 2026-09-17，共 432 条。初始整组 manifest 校验 PASS。独立 16 文件门禁前后 inputs/artifacts 完全一致，最终源码修补后以正式交付的 CRLF 归一化哈希核对：已有正式输出逐项未变。
- 本智能体没有调用生产 refresh、signal、SMTP、交易或推送；没有修改 NAV/manifest、版本收益参数或全局自动化配置。主协调必须在发布源码后执行真实整组 refresh-all/check，再进行云端代码/工件和发送验收。
- 当前本地 prompt 多次明确“不调用 SMTP”“本地生成不等于已发 Gmail”“intent 无 marker 属发送未知，不自动重发”。今天休市下既有本地/正常云端日报应退出；验证邮件若由主协调发送，必须清楚标注验收测试并与正式日报标记隔离。工作流成功、validation-only、SMTP 接受和收件箱送达各自取证。

证据：`calendar.json`、`local_prompt_snapshot.json`、`freshness_and_manifest_before.json`、`freshness_and_manifest_after.json`、`full16_execution.json`、`full16.log`、四份 XML、`deadline_backup.json`、`deadline_source.diff`、`local_agent_result.json`。
