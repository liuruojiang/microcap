# 2026-11-01 三版本历史修复发布证据

截至 2026-10-30，v2.0 / v2.3 / v2.5 正式流已完成历史迁移及不带迁移选项的 clean 重跑，行数为 4,049 / 4,004 / 4,004。正式收益与净值对独立候选逐行差异为零。此处仅证明数值、最终信号和精确种子身份；整组 `refresh-all` / `check` 与远端云日报验收仍待完成。

- [combined_review.json](combined_review.json) 绑定三份批准报告、原准备报告、三份授权记录和正式读回。
- [formal_three_version_readonly.json](formal_three_version_readonly.json) 记录正式三版本逐行对照、最终 CSV 身份、日期及非交易说明。
- `v2_*_approved_migration_report.json` 是实际迁移使用的批准报告；其中机器绝对路径保留原始字节用于哈希核验，移机时按本目录文件名及 combined review 绑定识别附件。
- `v2_*_authorization_binding.json` 明确由助手执行用户既有模型与部署授权，`user_separately_reviewed_generated_hashes=false`；不声称用户逐个审阅哈希。
- `archived_rewrite_audit_receipt.json` 记录三份原迁移差异审计的哈希。完整审计 CSV、原始来源及备份留在研究归档；本目录是便于审阅的小型证据包，并非全量市场数据包。

正式 frozen-tail authority 位于 `outputs/microcap_top100_mom16_biweekly_live_v2_0_base_frozen_tail_authority.json`。它绑定 10/30 的六个精确种子文件，哈希使用真实 `base_mod._file_sha256`（仅规范 CRLF/LF），只授权无新增调仓的尾部续算，不授权重新改写历史。旧 `restore_approved_top100_seed.py` 限定 2026-09-03 种子，不能用于恢复此次种子。

保留的边界：数据为公开/本地代理而非官方 Wind 868008.WI；ST 认证限于已取得并核验的来源范围；经济股本口径不等于完整公告时点认证；退市计零为保守模型假设，非真实成交、非现金回收。此前获准的一封验收邮件已经发送，本次未新增发送，也未下单。
