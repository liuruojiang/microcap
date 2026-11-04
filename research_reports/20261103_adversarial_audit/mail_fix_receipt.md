# 发送可靠性修复回执

修改自动化仓（尚未提交、推送）：
- scripts/check_microcap_delivery.py
- .github/workflows/microcap-close-recovery.yml
- tests/test_microcap_midnight_delivery_recovery.py（新增15个参数化测试用例）

修复：已完成一次性恢复流禁用job，并共用日常concurrency；遗留恢复intent仍fail-closed。收盘模式15点前仅检查前一自然日未完成证据，receipt存在才恢复该日marker；仅有intent则阻断，15点后保持当前日期。跨午夜SMTP接受需受GitHub run/job/send-step可信时间约束，原run与job开始北京日必须等于signal日，整个send job不超过180分钟。

备份：变更前完整状态已在Git b8465db723ad5c34d4a442e2d0ad1a594c8878d4（这些文件无预先未提交修改）。未改主工作流。

实际执行：
python -B -m pytest -p no:cacheprovider tests/test_microcap_midnight_delivery_recovery.py tests/test_microcap_delivery_gate.py tests/test_microcap_delivery_acceptance.py tests/test_microcap_adversarial_delivery.py -q

结果：81 passed in 1.80s，exit 0。
所有发送/API使用隔离mock；未发送邮件、未启动云端、未改正式数据。git diff --check 对本次文件通过。
