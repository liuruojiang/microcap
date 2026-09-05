# v2.5 对抗测试与日报交付验收（2026-09-05）

## 最终结论

- v2.5 对抗测试发现的旧版实时 CSV 串版风险已修复并部署。
- 三版本收盘确认 CSV 的成员变更人数误用缓存口径问题已修复并部署。
- 本地整组 `refresh-all` 与独立 `check` 均通过，数据截止 2026-09-04。
- Codex 正式任务为每天 14:30 的“微盘股与 IC/IM 每日实时信号（独立）”，状态 ACTIVE；旧 14:45 Top100 单独任务为 PAUSED。
- GitHub 纠正版运行 33955286409 成功，Gmail 收件箱已看到 2026-09-05 16:31 收到的最新纠正版。

## 发现并修复的问题

1. v2.5 同名实时输出可能遗留退休版 `17日 / 入场46% / 退出25%`，且旧逻辑会在收盘刷新后保留该文件。现已加入完整版本身份校验和并发锁内清理；旧身份不能进入日报。
2. 三版收盘确认信号曾从静态/实时缓存继承成员变更人数，旧云端包显示 19/19，而正式 point-in-time 成员谱系为 13/13。现已统一从正式谱系重算，并在整组交付闸门中校验日期、人数、标签与 turnover。

## 代码与部署

- 策略远端主线：`b10064622ace49bcaa5038d41423df03a17a9b14`
- 策略 PR：<https://github.com/liuruojiang/microcap/pull/52>
- GitHub 日报自动化主线：`4eba24979f8fe621b1f5c02172e37526373deef0`
- 自动化 PR：<https://github.com/liuruojiang/codex-daily-automation-probe/pull/63>

## 验证证据

- 主工作区完整测试：521 passed。
- 干净策略工作区完整测试：514 passed，1 skipped。
- GitHub 自动化工作区完整测试：254 passed，94 subtests passed。
- 本地交付：`refresh-all` 和随后独立 `check` 均为 `ok=true`、`scope=whole_workspace_delivery`、`release_sha=b1006462...`。
- 本地与云端正式数据日期均为 2026-09-04：基础面板 8,721 行；v2.0 成本净值 4,016 行；v2.3 与 v2.5 各 3,971 行。
- 云端最终 CSV：
  - v2.0：`plain_mom16_fixed1_20260904`，lookback 16，目标波动率关闭。
  - v2.3：`plain_lb25_hl2p5_r2off_vol10_26_20_20260904`，lookback 25，半衰期 2.5，目标波动率关闭。
  - v2.5：`plain_lb20_hl3_entry0_exit0_20260905`，lookback 20，半衰期 3，入场/退出阈值 0，目标波动率关闭。
- 三版成员事件一致：信号日 2026-09-03，执行日 2026-09-04，13 进/13 出；在 2026-09-05 日报中 `member_rebalance_actionable=False`。
- v2.5 对抗复算：持仓衔接错误 0；毛收益归因误差 0；成本后最大误差 `1.11e-16`；独立 WLS 最大误差 `2.22e-16`；未来价格扰动不影响历史信号。

## GitHub 邮件验收

- 运行：<https://github.com/liuruojiang/codex-daily-automation-probe/actions/runs/33955286409>
- 自动化 SHA：`4eba24979f8fe621b1f5c02172e37526373deef0`
- 策略 SHA：`b10064622ace49bcaa5038d41423df03a17a9b14`
- `Verify and pack all three final deliveries`、`Send Gmail`、`Mark digest delivered` 均成功。
- 邮件主题：`[纠正版][收盘确认][无需操作] 微盘股 v2.0/v2.3/v2.5 日报 - 2026-09-04`。
- Gmail 收件箱回读时间：2026-09-05 16:31（北京时间）。

本次只修复研究与日报交付链路，没有连接交易接口或发送订单。
