# 2026-09-17 微盘股日报修复状态

用户已在审阅精确哈希报告后回复“继续”，批准9月4日起历史更正。审批记录见 candidate/user_approval.json。代码已部署远端主分支，本地Codex实际恢复链和云端无邮件实际生成链已完成验收。没有额外发送邮件；本次验收不证明一封新邮件已投递。

## 已完成
- 策略远端主分支 e10e6fc1a3ec0105f2282fb3c1290914c879a09e，PR64/65/66；主工作区已同步。
- 修复摘要不刷新、短窗口重置执行成员、缓存包缺持仓/完整排名数据、证券历史查询降级、巨潮旧HTTP403、连续多日续接起点漂移/成员重构/10日上限，以及历史名称被当作当前ST过滤的问题。
- approved proxy/turnover/effective base已应用，basecosted经正式函数生成；三final均经正式CLI生成及无迁移开关重跑，未拷贝final NAV。所有字段与批准候选一致，历史重写检查clean。
- 9月3日及之前冻结前缀不变；三流4025/3980/3980行截至9月17日。原始审批报告和候选不改写。
- 正式来源补齐10只旧格式证券资料，ST违规0、policy不匹配0；当前100成员发布门槛保留。
- 最终完整策略测试586 passed / 7 skipped。实际refresh_state已走到completed并返回refresh_source=fresh；生产版本refresh-all和独立check均ok。
- 4975证券raw/share/security_meta完整缓存齐全。4317份共享股本缓存按官方resolver原字节落地，复制前后metadata fingerprint完全相同。
- 最终三个costed文件的SHA仍等于candidate/official_migration_execution.json的clean_rerun记录。

## 云端验收进展
- 完整4975证券恢复包已公开发布：microcap-recovery-20260917/microcap-approved-state-20260917.zip，70,837,440 bytes，SHA256 3d1476b87c873823a2d7cc160d2384a7d4ee9272d7344517ecea3684119d5628。GitHub asset digest与本地一致。
- 第一次实际无邮件验收35206851693发现未执行步骤的缺失输出被数值条件误判成功，导致备用恢复步骤跳过；已停止该次运行，没有发送邮件。证据 outputs/repair_20260917_cloud_first_attempt.log。
- PR98已把数值成功条件换成实际成功才写出的validated=true；最终426 tests、119 subtests全部通过。自动化远端main d67de5703d050f689acf1aeb0b15a2f89894e24f。
- 第二次默认恢复链实际验收35207480791已completed/success，validation_only=true；未传入手工seed。缓存未命中后，默认approved release fallback实际成功，完整缓存校验、刷新、三个版本生成、整组校验与邮件正文全部通过。
- 下载云端最终bundle后核验14982文件及28项final artifact hash；v2.0/v2.3/v2.5日期、正式身份、成员操作日期字段与本地一致，三条成本净值所有列逐值一致（浮点容差1e-12，Windows/Linux CSV字节序列化不同）。证据 cloud_run_35207480791/final_artifact_verification.json。
- 发送、发送意图、SMTP回执、送达标记和正式缓存保存步骤均为skipped。诊断工件名与正式恢复工件隔离，不冒充正式邮件送达。
- 实际云验收：https://github.com/liuruojiang/codex-daily-automation-probe/actions/runs/35207480791。代码已生效；未来定时运行及真实SMTP投递只能按发生后的证据验证，不能预称成功。
- 本地Codex实际sync/preflight/validate/check全部退出0；最后独立whole_workspace_delivery检查ok，日期2026-09-17，证据codex_actual_chain_verification.json。
- 正式恢复链为新版缓存→同策略版本成功正式工件→批准release包，最终仍须完整缓存与三版本状态校验。validation_only云端运行不得发信/写正式cache或送达标记。尚未执行额外邮件发送。

## 证据与回退
outputs/repair_20260917_final_release_tests.log；outputs/repair_20260917_production_refresh.log；outputs/repair_20260917_production_check.json；candidate/final_readback_verification.json。初始脏代码、云状态恢复前及正式迁移前均有.codex_backups备份，具名stash保留；用户其他研究与数据改动未清理。
