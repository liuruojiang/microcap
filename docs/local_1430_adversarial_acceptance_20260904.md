# 本地 Codex 14:30 微盘路径：配置与离线对抗验收

验收时间：2026-09-04 23:27（Asia/Shanghai）。范围仅本地定时任务中的微盘 v2.0/v2.3/v2.5 部分，遵守用户新增的“同时核验本地 14:30 与 GitHub 18:00 不受影响”要求；本文不代替另一代理的 GitHub 验收，也不修改或执行 IC/IM 策略。

## 结论与证据边界

- 保存的本地任务仍 ACTIVE、每日北京时间 14:30；本次只读配置，没有变更调度、IC/IM 参数、账本、automation memory。
- 原 prompt 指定的微盘离线专项：117 passed，退出码 0，进程耗时 11.453337 秒；硬超时 60 秒。
- 原专项加本轮 4 个新增攻击文件：242 passed，退出码 0，进程耗时 14.526164 秒；同样硬超时 60 秒。
- 随后扩大为全部 5 个新增攻击文件（含 v2.3 身份反例）：250 passed，退出码 0，进程耗时 17.783398 秒；硬超时仍为 60 秒。
- 当前机器预算内通过不保证未来机器负载下不会超时；原有失败/超时 BLOCKED 原则保留。
- 这是配置检查和隔离单元回归，不是 14:30 盘中实际运行证明。未运行正式 sync、preflight、refresh、实时 CLI、GitHub workflow 或 SMTP；不声称已获得当天盘中报价或交付成功。

## 配置只读核验

源文件：`D:/Codex/home/automations/ic-im/automation.toml`。

读前后 SHA-256 一致：`3dd9aa342805e7439f3b056b31be2b5c39bba9b363e77030ca3f19db9a91ffe9`。

- id：ic-im。
- 名称：微盘股与 IC/IM 每日实时信号（独立）。
- status：ACTIVE。
- 保存调度：每日 14:30:00；prompt 明确北京时间及 14:47 截止、不追补过期盘中信号。
- execution_environment：local。
- 任务 cwd：`D:/动量策略/IC和IM滚动套利`，目录存在。
- 微盘段明确要求在 `D:/动量策略/微盘股对冲策略` 运行，不把微盘命令误投 IC/IM cwd。
- 当前 `python` 实际解析：`C:/Python314/python.exe`；本次回归显式使用该解释器及 `-X utf8`。

## 微盘调用链与参数

已逐项核对保存 prompt、`scripts/top100_cloud_delivery.py`、`scripts/realtime_state_bundle.py`、`scripts/top100_delivery.py` 的实际入口。

1. 先用正式交易日历确认是否交易日；休市不生成实时信号。
2. 任何生产状态操作前执行离线专项，60 秒上限；不运行完整 pytest。
3. 日历动态计算上一已完成交易日。`top100_cloud_delivery.py sync --expected-date YYYY-MM-DD` 与 `realtime_state_bundle.py preflight --max-anchor-age-days 5 --expected-date YYYY-MM-DD` 使用同一天，禁止缓存末日替代。
4. sync 只复用或传输整组已验证状态；源码的 sync 分支先检查整体 manifest，必要时下载工件恢复，不触发历史重建。
5. preflight 独立核验后，运行 `realtime_state_bundle.py validate --max-anchor-age-days 5`，继而 `top100_delivery.py check`；base_state_only 不能代替整组通过。
6. 以上通过后，设置 `TOP100_REALTIME_REQUIRE_STATE=1`，官方 v2.0 → v2.3 → v2.5 串行运行。prompt 禁止并发、清变量或通过策略入口触发重建。
7. 要求当日 CSV、100/100 当日报价、100 唯一成员及 ST 零交集；微盘失败不能遮蔽 IC/IM 结果。14:35/14:40/14:45 有限重试，14:47 截止。

本轮代码加固不会要求改变此调用顺序或参数。正式 source/authority 或 final artifacts 尚未完成整组同步时，check 按设计 BLOCKED，而不是放行旧身份；发布验收必须由主代理另行完成。

## 实际离线回归

工作目录：`D:/动量策略/微盘股对冲策略`。通过 Python `subprocess.run(..., timeout=60, capture_output=True)` 硬超时执行；不是仅给模型口头时间预算。

原 prompt 命令：

```powershell
C:/Python314/python.exe -X utf8 -m pytest -q tests/test_realtime_preflight.py tests/test_top100_delivery.py tests/test_top100_cloud_delivery.py tests/test_adversarial_microcap_delivery.py tests/test_realtime_exchange_calendar.py tests/test_exchange_calendar_provider.py
```

结果：`117 passed in 8.55s`；包含进程启动的耗时 11.453337 秒；exit 0。

在上述同一次 pytest 再追加以下四个文件：

- `tests/test_adversarial_v20_plain_20260904.py`
- `tests/test_adversarial_v23_plain_20260904.py`
- `tests/test_adversarial_delivery_plain_20260904.py`
- `tests/test_adversarial_delivery_state_consistency.py`

结果：`242 passed in 12.21s`；包含进程启动的耗时 14.526164 秒；exit 0。涉及合成攻击和临时文件测试，不属于历史绩效。

最终增加 `tests/test_v23_identity_adversarial.py` 后，原 6 文件 + 新 5 文件合并同一 pytest 进程实测：`250 passed in 13.76s`；进程耗时 17.783398 秒，exit 0，60 秒硬超时未触发。该身份套件若发现本地真实产物则也只读验收现有 NAV/信号身份；其余构造输入明确为隔离反例。

## 建议补充 prompt（本代理未修改）

1. 把上述五个新增攻击文件加入既有微盘 60 秒离线专项，保留缺测试/失败/超时即本策略 BLOCKED，不盘中安装依赖。
2. 发布实时结果前，从最终 realtime CSV 核对身份：v2.0 `plain_mom16_fixed1_20260904`；v2.3 `plain_lb25_hl2p5_r2off_vol10_26_20_20260904`。版本名不能替代 revision。
3. 最终 CSV 是持仓、动作与仓位权威，不从 stdout 回填缺失/矛盾字段；非法持仓标签、非有限仓位、cash 非零或 active 非一均 BLOCKED。v2.0/v2.3 active 标签为 `long_microcap_short_zz1000`；v2.5 为 `long_microcap_top100`。
4. 新的信号 mode、参数和当前/下一状态检查必须落到本次最终 realtime CSV，确认 intraday 语义、上一已完成交易日锚点及当日快照；v2.3 核对 R² OFF、信号对冲 1、执行对冲 0.8、入场 0、退出缓冲 0.08、过热 26%/20%、现金收益和融资关闭。实时信号与历史日末信号语义不同，不直接把 intraday 的 next_holding 与昨日 costed NAV 最后一行强制相等；须使用同一快照的真实策略输出核对。历史整组 check 仍作为运行前前置门禁。
5. 本地 Codex 研究输出不等于 GitHub Gmail 交付成功；现有禁止本地 SMTP 补发条款继续保留。

新增建议只涉及微盘验收与身份，不改变 IC/IM、14:30 时间、重试安排或故障隔离。

## 主智能体后续实际入口与配置验收

上述建议已通过正式automation更新工具追加到原prompt，并读回确认原全文前缀、调度14:30、ACTIVE、模型、工作区及IC/IM配置均未变化。更新证据及备份见 `research_reports/20260904_v20_v23_adversarial/local_automation_update.json`。

随后在收盘后测试实际state-only路径：sync复用整组状态成功，validate及whole check也通过；但联网preflight连续两次在免费指数历史源 `1.000852` / Sina 上超时（41.948秒、43.272秒）。实际预检未通过，故没有继续运行三个实时入口或发布盘中结果。两次失败均确认基础输入和三版NAV字节未变。此为当前外部源阻塞，相关历史抓取函数本轮未修改；不能以250项离线测试通过替代完整14:30实时成功。失败日志、原样重试结果及最终双日报验收结论见总审计记录。
