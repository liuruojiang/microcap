# Workspace Defaults

Use this repo-local file as the source of workspace guidance for the Top100 microcap strategy. Do not depend on a user-home `AGENTS.md` or any machine-local Codex configuration path.

- The Top100 mainline defaults to `microcap_top100_mom16_biweekly_live_v2_0.py` (`v2.0`) unless the user explicitly asks for another version.
- For signal, realtime signal, performance, holdings, and member queries, default to `v2.0`.
- Before any Top100 current-data strategy test, signal output, chart generation, or parameter/combo comparison, refresh the selected strategy data to the latest locally available trading date and prove freshness before using metrics. Isolated software regression tests and explicitly dated frozen-data compatibility checks do not require a market-data refresh and must not publish their results as current strategy performance.
- Freshness proof is mandatory: read back and record the latest dates and row counts for the refreshed panel, proxy index, proxy turnover, base costed NAV, and every version/candidate return stream used in the test. Daily streams must share the latest close-confirmed trading date. Proxy turnover is an event stream: its latest date must match the latest member-rebalance signal date and must not exceed the target session; do not fabricate daily turnover rows. Failed freshness checks must be explicitly marked blocked.
- Do not use stale official costed NAV CSVs, older `outputs/` exports, or same-base helper recomputes as a substitute for refresh proof. If refresh cannot advance the base costed NAV and all compared streams to the latest available trading date, stop and report the refresh blocker; do not publish performance, leverage, holdings, or charts as formal results.
- When only a short tail is stale and no new rebalance date exists between the current costed NAV end and the target trading date, prefer an auditable no-new-rebalance tail extension over a broad `--force-refresh`; still verify the extended proxy index, base costed NAV, and downstream version streams by reading the written artifacts.
- The default practical/live performance caliber is `v2.0 + costed`; do not silently mix `gross` and `costed`.
- Prefer fast command-aware query routing in `microcap_top100_mom16_biweekly_live.py`; use the old full `build_base_context()` path only if requested or unavoidable.
- Treat `outputs/` as disposable export space. Keep current core artifacts for `v1.0`, `v1.1`, `v1.2`, `v1.6`, `v2.0`, `v2.3`, and `v2.5`.

# Member And Lineage Guardrails

- A published current Top100 list must contain exactly 100 unique ranked symbols and must have zero intersections with current `ST`, `*ST`, or `PT` names/codes. Treat this as a hard publish gate, not a warning.
- Historical backtests must use point-in-time ST entry/exit intervals under the current notice policy; never project the current ST snapshot backward through history.
- Build the historical member universe from the full historical security master, not the current listed universe or a narrow recent candidate set.
- Proxy compatibility must include the security-metadata content fingerprint. A metadata-content change requires rebuilding the proxy and every downstream formal stream.
- Historical rewrites remain fail-closed. A necessary lineage correction requires an approved exact-hash migration report, followed by a second run without the migration option that must finish with a clean rewrite audit.
- After a v2.0 lineage correction, rerun and read back v2.3/v2.5 before reporting them; do not reuse their earlier official outputs.

# Signal Query Defaults

- Return the final signal first; keep process details out unless there is a failure, stale-data risk, or version caveat.
- Distinguish `信号` from `实时信号`: `信号` is close-confirmed; `实时信号` needs same-day intraday snapshot, `snapshot_time`, and `latest_anchor_trade_date`.
- Never answer market-hours realtime requests with only the previous close-confirmed row.
- Before publishing any realtime signal, verify that `latest_anchor_trade_date` is the latest completed historical close immediately before the realtime `quote_trade_date` snapshot. If an intervening completed trading day is missing, refresh the selected version first; if refresh cannot rebuild the strategy NAV through that date, report the realtime signal as blocked and do not output a trading signal.
- For version-specific realtime without an official route, use the current fast realtime snapshot, then recompute the named version from real strategy functions and parameters.
- For `v2.0` + `v2.3` realtime signal requests, keep the fast answer path only after the anchor check above passes: run the official `v2.0` realtime command first; for `v2.3`, prefer reusing the freshly written `outputs/microcap_top100_mom16_biweekly_live_v2_0_realtime_signal.csv` plus local `outputs/microcap_top100_mom16_biweekly_live_v2_0_nav.csv` and recompute with the real `v2.3` source functions. Do not trigger a full v2.3/v2.0 historical base refresh for a simple realtime query unless the fast path is unavailable, the anchor check fails, or the user explicitly asks to debug the official v2.3 entrypoint.
- If the official `v2.3` realtime entrypoint blocks on free index-history refresh such as `1.000852` / `sh000852` or leaves a stale base-build lock, report that as a route/data-source issue and fall back to the fast recompute path above only when the anchor check passes; otherwise block the realtime signal instead of publishing a stale-anchor result.
- For `v1.4 实时信号`, apply the v1.4 source logic on a realtime quote snapshot; do not treat the v1.4 close-confirmed script as realtime.
- Minimal signal reply: version, time, current holding, next holding, trade state, execution scale, microcap momentum, hedge momentum, momentum gap, quote coverage, and stale/fallback warning.

# Overlay Research Guardrails

- Before interpreting stop-loss, take-profit, drawdown, or reentry overlays, verify executed `holding` / `next_holding` state controls realized `return_net`.
- After overlay-triggered early exits, blocked `cash` days must have `return_net = 0` and must not inherit base `gross` return.
- Suspiciously strong overlay results require lagged-trigger or equivalent sanity checks before trusting the table.

# New Strategy Test Standard

- New strategy tests and candidate promotions must follow `docs/new_strategy_test_standard_process.md`; every display/report must include full sample, 10Y, 5Y, 3Y, and 1Y annualized return plus max drawdown, or explicit `N/A` reasons.

# Desktop And Git Defaults

- For local images/files in Codex desktop, prefer repo-relative paths or a documented local alias when one is explicitly provided in the current session.
- Keep remote `origin = git@github.com:liuruojiang/microcap.git` unless the user asks otherwise.
- Use the configured Git credential or SSH setup for this machine; do not hard-code a user-specific SSH key path in repo instructions.

# Production Email Deployment Gate

- Whole-workspace synchronization is NOT complete when only Git, a cloud run, or `realtime_state_bundle.py validate` passes. That validator covers base state only.
- After synchronizing executable strategy code/authority or refreshing a historical base, use `python -X utf8 scripts/top100_delivery.py refresh-all`, then `python -X utf8 scripts/top100_delivery.py check`. Both must pass before claiming local synchronization complete or resuming parameter scans. The second command independently checks the latest completed session and live remote core/authority, and reads back all three final streams against the delivery manifest. Documentation-only changes do not trigger these gates or require market-data regeneration.
- On Windows, `scripts/run_top100_daily_synced.ps1` invokes both unchanged gates using native strategy code, as cloud daily reports do. No external compatibility profile is required; `-AutomationRoot` remains an optional backwards-compatible argument. The launcher does not replace either gate.
- Preserve dirty local changes with a verified backup and reviewed merge. Deployment in a separate worktree does not discharge synchronization of this primary workspace. Final NAVs are ignored by Git and must be regenerated/verified separately. Never reset or copy a single NAV to bypass lineage checks.
- A failed/interrupted group refresh leaves an incomplete delivery manifest. Never describe a partial version refresh, stale manifest, or `scope=base_state_only` result as whole-delivery success. Restoring an already approved seed requires exact base/source hashes and a backup; any new lineage needs new approval.

- A Microcap production-email change is not deployed merely because a local sample or workflow run succeeded. Before declaring synchronization complete, verify the exact strategy commit checked out by the automation workflow is reachable from the remote repository, read back the final CSV identity and dated member-action fields for v2.0/v2.3/v2.5, and record the remote strategy SHA, remote automation SHA, workflow run, and mail status. Validation-only deployment must explicitly record sending as skipped; it does not require an extra acceptance email and must not claim inbox delivery. When an actual send is requested, record its delivered-message evidence separately.
- The final CSV is the authority for strategy identity, holdings, and actions. Stdout may supply presentation fields but must never backfill a missing or contradictory final-CSV identity.
- A member rebalance is actionable only on its dated execution session. Historical and preview changes may be reported as context but must not be rendered as a current trade instruction.
- After the close, do not relabel a close-confirmed signal as realtime. A realtime publication must retain the previous-completed-session anchor invariant; use an explicitly close-confirmed correction when that invariant cannot hold.

# Close Confirmation Time (2026-10-23 user approval)

- Today is eligible for close-confirmed research at or after 15:00 Asia/Shanghai, replacing the prior 15:30 buffer. This does not prove market-data completeness: require the same-day complete source data, independent exchange calendar, 100 unique members with no ST overlap, and full v2.0/v2.3/v2.5 delivery gates. Intraday publication still ends strictly before 15:00. Do not relabel stale rows or change historical records.

# 测试、发布与同步执行流程（2026-11-02 用户要求，后续强制遵循）

本节适用于本项目后续所有测试、发布和同步任务；用于限定验证范围、复用有效证据和及时结束交付，不放宽上述数据真实性、血缘、交易日期及正式交付门槛。这里规定的是执行流程，不代表打包、缓存或发布程序已经完成性能改造。现有程序尚不支持的优化不得通过跳过检查、伪造清单或手工修改指纹实现。

## 1. 开始前确定范围和结束条件

- 先读取实际差异与已有记录，确定修改类别和验收条件；仅涉及发布或数据时确认适用的版本、提交、数据包和交易日期。小改不另建计划文件或审批流程。
- 区分代码发布、主工作区同步、云端验收、最新交易日更新和实际邮件发送。仅完成其中一项不得宣称全部完成；未授权发送时采用不发送邮件的验收方式。
- 已授权修复只处理本次目标及阻断项：本次必要门槛失败、结果错误、版本或数据不一致、交易动作误报。无关旧问题不因被发现就升级为阻断项；非阻断问题记入既有待办，不增加参数研究、历史补数、清理、全面审计或重构。
- 同一主工作区或云端发布目标只保留一个写入流程。已有任务正在刷新或发布时先确认其状态，避免同时改文件、抢锁或重复发布；独立只读检查可以并行。

## 2. 按影响选择最小充分验证

| 修改类型 | 必需验证 | 不自动触发 |
| --- | --- | --- |
| 文档、AGENTS、交接说明 | 内容、编码、链接及差异检查 | pytest、行情刷新、历史重建、云端发布 |
| 邮件文案或展示 | 受影响渲染与字段测试，日期、版本及成员动作语义检查 | 全市场补数、策略历史重算、额外实发邮件 |
| 发布、同步、超时或传输逻辑 | 对应契约测试、相关环境验证、准确提交与产物身份检查；部署时执行适用正式门槛 | 无依据的全历史重建 |
| 策略计算、成本、持仓或共享数据处理 | 受影响行为与重要不变量测试；共享基础变化覆盖三版；必要的真实数据一致性检查 | 无关参数扫描、无关模块修复 |
| 历史数据、ST、成员资格或计价口径 | 影响范围重建、精确血缘迁移审批、正常模式改写审计和三版完整交付 | 将必要审计降为抽样或绕过失败 |

- 软件回归使用隔离夹具，避免意外读取生产缓存；真实数据集成验收可只读使用明确身份的生产快照，两者不得串用。保留故障注入语义，不删断言、跳过失败或用真实归档替代夹具来获得通过。
- 开发阶段先跑针对性测试，候选稳定后运行所需完整回归。已有结果通过后，只有相关代码、依赖、配置、输入或运行环境变化，或出现新失败，才重跑受影响检查。
- 云端现有必需回归与跨系统验证保留；不要仅为减少几分钟测试而增加复杂缓存机制。优先消除整包重复处理和误触发全量抓取。
- Python 测试默认使用 `-B` 或 `PYTHONDONTWRITEBYTECODE=1`，pytest 使用 `-p no:cacheprovider`；临时文件集中在明确任务目录。文档改动不写测试。

## 3. 先判定失效原因，再决定刷新范围

- 代码指纹变化不自动等于行情数据失效。先检查具体变更、有效旧状态包和已有兼容路径；不得把 `--force-refresh`、`--allow-full-rebuild` 或全市场重抓当作默认排障步骤。
- 仅代码兼容性变化时，优先在隔离目录复用准确哈希的已批准状态包，验证新代码读取或重算结果与原结果一致，再走项目支持的认证流程。不能直接改指纹让旧状态通过；无法证明兼容时保持阻断。
- 有真实历史数据或元数据内容变化时，仍按血缘规则重建受影响基础及下游版本。新交易日必须刷新相应输入；周末或非交易日不因自然日期不同就否定覆盖最新完成交易日的有效数据。
- 短尾更新且无新调仓时优先使用现有可审计尾部延伸。外部接口使用现有有界超时及备用源；同因失败没有新证据时不重复启动昂贵任务。

## 4. 同一候选只验证和封装必要次数

- 重型验证复用现有回执中与代码、依赖/配置、输入哈希、日期和检查版本绑定的证据；只记录适用字段，不另建通用证据缓存系统。相关条件变化时重做受影响检查；路径、文件时间或旧 stdout 成功不能单独作为复用依据。
- 优先使用现有工具支持的共享不可变基础与独立版本输出，避免人工额外复制整套历史、重复恢复到多个目录或重复打包。必须保留版本隔离，不能让并发版本互相覆盖输出。
- 接收端继续核对包哈希、文件清单、完整性和最终 CSV；不能用发送端已通过作为不检查落盘结果的理由。整包语义校验的复用只能通过已有可信实现，不能临时绕过正式门槛。
- 现有程序内部仍会重复校验或封装时，按原门槛完成一次，将具体瓶颈记为独立优化事项，不在普通小修中顺手改造整套交付架构。
- 上传前优先检查现有可信差量发布路径是否适用；差量必须逐字节还原同一已验证完整包并核对最终哈希。远端写入超时先查询是否已成功，不盲目重复创建发布或上传。
- 保留有恢复价值的备份；同一未变化状态不重复备份。主工作区已有修改必须保护，隔离副本通过不能代替主工作区同步。
- 新版验收前保留旧正式配置和可用回退点；切换后失败按已有路径恢复匹配的代码、配置与兼容数据，不能只回退代码或将旧信号冒充当前信号。普通小改不另做灾难恢复演练。

## 5. 一次交付、分别记账、达到条件即结束

- 最终候选执行适用的本地 `refresh-all`、独立 `check` 及准确版本的云端验收，最后对照三版 CSV。提交和数据包固定、两端无写入依赖时可并行验收；启动器已有有效通过证据时不手工重复执行。
- 失败先定位阶段。若 `refresh-all` 已通过，仅 `check` 临时失败，确认代码、输入、产物和目标日期未变、清单完整后只重试 `check`；日期推进、清单不完整或相关状态变化时，按正式入口重做必要阶段，不默认重跑整个启动器。
- 发布验收记录固定候选的实际数据日期。若跨过 15:00 导致独立检查要求新交易日，保留已完成的固定日期兼容性和发布证据，将新交易日刷新作为明确的后续阶段；不能关闭日期检查、沿用旧日期宣称最新同步完成，也不因日期推进而重跑未受影响的软件测试、历史审计或旧包上传。
- 复用现有交付回执，集中记录策略 SHA、自动化 SHA、包哈希、目标日期、本地两项门槛、云端运行、三版身份及动作对照、邮件状态和剩余阻断。不要为了收尾再生成多套互相重复的报告。
- 长步骤记录起止时间及阶段，区分测试、刷新、恢复、校验、打包、上传和排队；并行耗时不累加为总墙钟耗时。停滞时依据超时、日志和进程变化排障；无新证据不重复重启，也不无限报“只剩同步”。
- 约定的适用门槛全部通过即交付，不再附加新一轮审计、美化或测试。若受阻，明确已完成部分、具体阻断及最少后续动作，不把部分完成称为全部完成。


## 本地实时日报固定入口（2026-11-03已激活）

本地ic-im自动化先读 D:\Codex\home\automations\ic-im\scripts\REALTIME_RUNBOOK.md，并经 realtime_daily_gate.py daily 固定入口执行当次完整专项。active绑定的是认证离线测试环境，不能作为生产信号或当日报价。专项通过后仍须实际运行正式生产状态门禁与研究入口，并在Codex任务发布；本地不调用SMTP。新ST证据候选、历史迁移及种子身份继续遵循原精确审阅/批准门槛，不以软件验收冒充当日数据可用。


## 日报故障按阶段修复（2026-11-04 用户要求，后续修复必须遵循）

- 修复前先读真实失败日志、退出码、进程/锁与工件，找出最后一个可核验成功阶段；说明失败点、影响范围、可复用阶段和必须重跑阶段的理由。默认只修失败组件及其受影响下游，不能因小修直接重跑全部流程。
- 文档只检查内容/编码/链接/差异；展示故障复用原最终 CSV/result.json 只重渲染；发送故障先核原 intent/SMTP receipt/marker/INBOX，只恢复发送事务。未知发送状态先核实，不删去重证据或更换通道重发。
- 先执行原错误反例及受影响契约测试，候选稳定后运行适用正式回归；同轮相同源码/依赖/配置/fixture/输入/环境下已通过的验证不重复。例行每日完整专项及生产门槛仍必须按合同完成，局部修复测试不得冒充它们。
- 按依赖和实际哈希决定失效范围。无关文档提交、HEAD 变化或 mtime 不自动使全部行情/历史/证明失效；相关源码、来源、日期或输入变化必须重验受影响闭包。跨源码复用走正式兼容入口，不手改指纹、manifest 或回执。
- 临时云端进入下一昂贵阶段前，确认已完成成果有可恢复工件；优先复用现有包与增量证据，失败时保存成功成果和原始错误。未完成检查点不得充当完整包或发布许可。不支持续接时说明实际限制及最小重跑范围，不能假称已支持。
- 临时网络错误按原次数限制只重试失败请求/品种；403、安全拒绝、错日、来源冲突、ST/成员/历史变更仍按原硬门槛处理。测试成功不等于日报生成，SMTP 接受不等于 INBOX。
- 同一工作区/发布目标保留唯一执行者；另一修复仍活动时不重复 dispatch、不更换其分支、不覆盖其回执。用户要求的当前流程继续按原任务推进，独立文档工作不介入其运行。
- 不降低策略参数、日期/来源/成员ST/raw-share/哈希链、历史批准、盘中截止或发送边界；不在小修中扩展架构、压测或恢复延期事项。
