# L7 独立审计 A：源码、版本身份与状态续接（2026-10-03）

## 结论

**修复前，本地三版最终 CSV 的现有参数值与源文件中的正式常量相符，但交付身份校验存在可复现的漏检。** 将既有最终行中的 v2.0 lookback／对冲比、v2.3 lookback／half-life／执行对冲比／过热窗口及启停、v2.5 过热启停单独改错，`plain_vXX_identity` 仍返回 `True`。这是身份门的 P1 守卫缺口，不是已经观测到正式输出被改坏或真实订单出错。主 Agent 随后在本地修复，独立复核见末节。原始证据见 [identity_probe.json](identity_probe.json)，原探针复现 `python -X utf8 research_reports/20261003_l7_diagnostic/source_state_audit/identity_probe.py`（修复后重跑会正确显示拒绝）。

## 对象与边界

- 源码：`microcap_top100_mom16_biweekly_live_v2_0.py`、`_v2_3.py`、`_v2_5.py`、公共 `microcap_top100_mom16_biweekly_live.py`、`scripts/realtime_state_bundle.py`、`scripts/top100_delivery.py`、`scripts/top100_cloud_delivery.py`。三个策略源文件在本工作区已有修改；本审计没有改它们、正式输出或参数。
- 实际输入：本地三版 `outputs/*_latest_signal.csv`，均为 2026-09-30 的旧公开代理、理论成交和成本化纸面输出。脚本记录了原文件 SHA-256，测试只在内存里改变字典字段。
- 不声称最新生产发布、云端邮件送达、真实交易回执或 PIT 数据的重新认证；完整生产链需要远端 SHA、工作流、最终 CSV 和消息读回。L1/L2/L5 限制仍在。

## 源码路径

1. 批量与增量：`realtime_state_bundle.refresh_state` 默认 `allow_full_rebuild=False`，顺序为刷新面板、`ensure_strategy_files` 延伸/重算代理与成本净值、构建实时上下文、静态成员、刷新证明；`ensure_strategy_files` 有直接复用、无新换仓尾部延伸、依据 turnover 重算、全量重建几条分支。成功后还核代理、成本净值、面板日期等于独立目标日。未做本次重新批量/增量逐日同一性实跑；此项由主 Agent 的交付读回与另一审计互补。
2. 三版历史正式写入：v2.0 `generate_v2_0_outputs`，v2.3 `_generate_v2_3_outputs_unlocked`，v2.5 `_generate_v2_5_outputs_unlocked`。后两版先载入 v2.0 正式索引并构造共同索引；三版都有历史 rewrite 审计，正常路径 `clean`。各版产物进入 `staged_output_bundle`，先将全部文件备份后按顺序推广，summary 最后，后置 freshness 校验失败则回滚。未见直接把试验候选写入正式三版输出的路径。
3. 查询状态：`信号` 是 close-confirmed 历史生成；`实时信号` 先核最新完成收盘锚点再取当日快照，失败走明确 blocked/异常，而不能把历史行改称实时。v2.0 用 `_v2_file_lock` 保护生成与实时写入，v2.3/v2.5 复用此锁并用独立文件名。
4. 整组交付：`top100_delivery.refresh_all` 先比对远端 main 源码/authority，再刷新公共基座，按 v2.0→v2.3→v2.5 运行；输入哈希不得变、目标日不得跨越、读回最终 CSV/NAV、写完整清单。`check` 再核远端、清单哈希、基座及最终流。它要求历史 rewrite audit 为 `clean`，拒绝单次迁移结果直接当作完整交付。
5. 恢复：状态 ZIP 在解包前校验条目、路径、字节数与 SHA；`restore_state` 本身逐文件原地写入，若中断可留下部分新状态。云端整组 `top100_cloud_delivery.restore` 先在临时目录校验全包、给目标文件留备份，持整组锁恢复，失败将整组 manifest 置 `blocked`，后续检查应拒绝混合状态。它不自动回滚所有已写文件，因此中断后需要以备份/已验证 bundle 重新恢复并跑 `refresh-all`/`check`，不能直接发信号。

## 对抗反例与建议断言

| 版本 | 有效字段单点篡改 | 当前函数返回 | 建议拒绝条件 |
| --- | --- | --- | --- |
| v2.0 | `lookback` 16→17；`fixed_hedge_ratio` .8→.7 | 均 `True` | 必须等于 16、.8 |
| v2.3 | `lookback` 25→26；`halflife` 2.5→3.0；`execution_hedge_ratio` .8→.7；`overheat_feature_window` 10→9；`overheat_enabled` True→False | 均 `True` | 必须等于 25、2.5、.8、10、True |
| v2.5 | `overheat_enabled` False→True | `True` | 必须等于 False |

当前 `top100_delivery` 的 summary 校验 v2.0 主要看 revision/退出缓冲/关闭目标波动与过热；v2.3 看 revision/R²/过热双阈值；v2.5 看 lookback/half-life/进出阈值/目标波动，故应同步给 summary 中存在的有效字段设值检查。v2.5 最终行的 lookback、half-life、双零对冲和进退阈值已由当前身份函数检查，无需重复扩张到所有诊断列。任何修复都应同时覆盖 `tests/test_top100_delivery.py` 第 54–111 行的简化 fixture，并加入上述变异被拒断言；`tests/test_adversarial_microcap_delivery.py` 复用该 fixture。

## 已执行验证

- 只读变异探针：三版原始最终行均被接纳；所列 8 个单字段错误仍被现有身份函数接纳。没有修改磁盘上的 CSV。
- 现有针对性测试：`python -X utf8 -m pytest -q tests/test_top100_delivery.py tests/test_microcap_v2_review_remediations.py -k 'identity or staged_output_bundle or dead_pid_lock' --tb=short`，**11 passed**。这覆盖旧版身份、产物推广回滚和死锁恢复，未覆盖上述参数漂移，因而测试通过与本反例并不矛盾。
- 未对旧纸面模型重新输出绩效；正式 Full/10Y/5Y/3Y/1Y 年化和回撤继续 N/A。

## 修复后复核（独立只读）

主 Agent 本地修改了 `scripts/top100_delivery.py` 和简化测试夹具；本审计没有改代码。已逐项审阅 diff 和 [主 Agent 的修复验证](../root_identity_repair_verification.json)：8 个原有反例均由接纳转为拒绝，三个真实最终行仍通过。该验证中的 validator SHA-256 `112097de07c73e3a6269d0074986501cd6476b30e800a76c947ceeb5b4dcca78` 与复核时磁盘文件一致。

独立读取现有真实文件确认兼容：v2.0 成本 NAV/展示 NAV 各 4,033 行，历史行本来没有 `lookback`、`fixed_hedge_ratio`，但最终信号有两列；v2.3/v2.5 各 3,988 行，历史 NAV 本来没有 `overheat_enabled`，最终信号有此列。因此历史行采用“列存在才核”、最终信号强制存在、summary 强制值的组合没有误拒真实旧 schema。用修复后代码重新执行 `inspect_outputs(root, '2026-09-30')`，结果 `ok=True`、错误 0、读回 16 条流及 28 项产物哈希。

新增的 v2.3 summary 校验覆盖 WLS 25/2.5、执行对冲 .8、过热启用和 10 日窗口；v2.0 summary 补 16 日和 .8；v2.5 summary 补过热关闭。当前正式源码中 v2.3 `BASE_HEDGE_RATIO` 与 `EXECUTION_HEDGE_RATIO` 为同一常量；summary 的 `fixed_hedge_ratio` 虽未新增单独核验，但它是该执行比的别名，不构成本次同级有效路径漏检。没有发现新的结论相关 P1。

**限制：** 这证明本地身份守卫和现有旧纸面产物兼容，不能替代远端源码同步、`refresh-all`／`check`、真实批量/增量同一性实跑或工作流与邮件读回。修复后的交付验收须按项目整组门禁执行。
