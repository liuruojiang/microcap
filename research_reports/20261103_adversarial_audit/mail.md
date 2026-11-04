# 日报发送可靠性对抗审计（2026-11-03）

审计对象：自动化 `b8465db723ad5c34d4a442e2d0ad1a594c8878d4`，策略 `193d9309fcfa0ac365d3016b5c804ff07190ecd7`。只读生产；执行了隔离 mock API 反例，未发邮件、未启动工作流、未改代码或生产数据。既有云端 1264/1073 回归不重复运行。

## 结论

发现两个 P2 可靠性缺口。没有证据说明 11/2 补发邮件已经重复。主审已通过 Gmail RFC822 Message-ID 查询确认该邮件 SENT+INBOX、唯一命中；本审检查本地正式回执与发送链路相符。SMTP 接受、收件、去重、同步不可混为同一结论。

### P2：恢复工作流与日常工作流未共用发送锁或 intent

位置：自动化 `.github/workflows/microcap-close-recovery.yml:8,35,78,82`；`.github/workflows/microcap-realtime-digest.yml:56,703,713`；`scripts/check_microcap_delivery.py:240`。

两个 concurrency group 分别是 `microcap-close-recovery-20261102` 与 `microcap-realtime-digest`。恢复预检只查正式 marker/receipt 和 recovery 专属 intent；日常预检只查正式 marker/receipt 和 normal intent。它们在任何已完成 marker 出现前均可通过检查，之后没有共享原子占用或发信前复核。

执行反例：冻结日常 gate 的 now 为 11/2 18:00，mock artifact 服务中只有 `microcap-close-recovery-20261102-send-intent`。真实 `gate.main()` 输出 `should_send=true`；它实际只查询正式 marker、旧 marker、正式 receipt、normal intent 四个名称，完全没有查询 recovery intent。这与恢复工作流不同 concurrency group 结合，可使同一日期并发正常发送与恢复发送。反方向恢复工作流也未查 normal intent。

边界：恢复工作流固定 11/2，并且 `check` 要求最新完成交易日仍为 11/2，因此风险窗口被限定；当前已有正式 11/2 marker，现时直接重跑会被 guard 阻止。不能把此结构缺口说成当前已经重复。建议共享同一 concurrency group，并让两条发送路径使用同一日期/模式 intent 身份；已成功的临时恢复流可退役或关闭 dispatch。

### P2：跨午夜接受的有效收盘回执无法自动恢复缺失 marker

位置：`scripts/check_microcap_delivery.py:159-163`，同时 `main()` 的 delivery_date 来自重跑当天（约 205 行）。

若 D 日启动的正常收盘任务直到 D+1 凌晨 SMTP 接受，随后 marker 上传失败，回执已经上传，下一轮无法恢复：对 D 调用校验时 `accepted_at` 的北京时间必须等于 D，因而拒绝；对 D+1 校验时 signal_day 又不等于 day，也拒绝。主流程只按新自然日期查 artifact，还看不到前一日 intent/receipt。结果会保留“发送状态不确定”的人工阻塞，而不是可靠地自动恢复。这里未观察到自动重复旧日报，但可造成漏发/无法重试和运行状态长期错误。

执行反例：真实 `verify_smtp_receipt`，metadata signal_date=2026-11-02、status=OK、close_confirmed；receipt accepted_at=2026-11-02T16:05:00Z（北京时间 11/3 00:05），全部内容哈希、run/job/send/upload 身份有效，当前时刻为 11/3 09:00，day=11/2。实际结果：`BLOCKED: SMTP receipt dates do not match delivery`。除了 GitHub 下载/API 使用隔离 mock，校验逻辑没有替换。

建议将交付身份绑定到明确的 signal session，并以该次受控运行的开始/结束时间验证晚到接受，保留禁止未来/错误日期与交易指令语义。不要直接放宽为任意历史接受日期。

## 已证实的保护

- `send_report.py` 不自行重试 SMTP；原子 receipt 写入失败抛错，不伪报发送成功。
- 正常 OK 邮件先持久化 intent，再 SMTP；SMTP 或回执状态未知时后续正常同日运行保持 fail-closed。
- 正式 marker 要求三版验证、完整包、SMTP 及 receipt 上传均成功；validation_only 禁用发信和正式写标记。
- 最终 CSV 身份、成员执行日期、三版完整门槛没有因共享工作区而被本发送链路跳过。
- 旧临时恢复流的 receipt 若 marker 恰好失败，普通 `verify_smtp_receipt` 还会因 workflow path 非日常路径而拒绝恢复（129 行）。这是上面恢复设计不统一的额外表现，人工处理可避免误重发，不能称完全自动恢复。

## 边界

未做真实 SMTP 故障注入、没有新增实发邮件；传输层成功和收件可靠性仅由已有实际发送与 Gmail 证据证明。外部服务宕机、GitHub artifact 长期过期和收件方垃圾邮件规则不在本次实证覆盖之内。

## 可执行复现

源码：`repro_mail_reliability.py`；实际执行输出：`repro_mail_reliability.out.json`。在策略仓根目录执行：

```powershell
python -B -X utf8 research_reports/20261103_adversarial_audit/repro_mail_reliability.py
```

已执行此持久化脚本，退出码 0，两项 `reproduced=true`。它只 mock GitHub API/下载、时间与输出，运行真实 `verify_smtp_receipt` 和 `main`。没有导入发送模块，也没有 SMTP、真实 API 或生产写入；`smtp_calls=0` 为此脚本调用设计的说明，而非对远端 SMTP 的观测。
