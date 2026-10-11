# 发送验收模式第二次独立复核

本次用真实本地 v2.0/v2.3/v2.5 最终 CSV，以及上轮下载的三版真实云端 signal 输出日志，检验新增 --delivery-test-id。没有调用 SMTP，没有写入生产 outputs。准备数据截止 2026-09-30，与已核对的最新完成交易日一致。

合法控制：主题明确标为「发送验收测试／收盘确认」，保留 2026-09-30 和验收编号；正文先说明历史收盘样本及「不作为今日交易指令」，三个版本均齐全。测试只能使用 close_confirmed；实时模式冲突在生成 metadata 前拒绝，原 15:00 截止与锚点合同仍保留。

复核发现并修复 CSV 结构歧义：最终信号必须恰有一行，表头必须唯一、非空且没有边界空格，行宽必须等于表头列数，CSV 引号语法严格验证。重复 --signal-csv 或 --result 版本不能覆盖；发送测试的 CSV 和结果映射必须恰有 v2.0、v2.3、v2.5。缺失、额外、重复版本参数在生成 metadata 前失败。共享 builder 修改前另行备份并读回核验，备份在自动化仓库 .codex_backups/20261007_230126。

新增正式 tests/test_microcap_final_csv_shape.py，连同已有内容、假日截止、验收模式和去重隔离测试共 189 tests + 30 subtests PASS，证据为 testmode_shape_regression.xml。

独立真实产物反例共 27 项，全部通过。将修改前备份与当前代码同样送入实际工作流 Send Gmail 条件求值：旧代码有 17 项坏形状输入在其他门禁假定通过的情况下允许发送步骤；新代码的 26 项坏输入均无法激活发送步骤，合法控制可以激活。这是对代码和条件的诊断反例，不表示那些坏输入曾真实发出邮件。原有 69 个内容反例在严格 CSV 修复后再次全部通过。

隔离检查：测试编号命名空间与正式当日发送标记分开；两个生产 cache-save 步骤都要求 delivery_test_id 为空；测试的 verified state 和 whole delivery 产物名称分流到 validation 名称，不能以正式 recovery/whole-delivery 名称被下次生产消费。非法验收编号、与 correction／validation-only／实时模式冲突已由现有测试覆盖。

末端发送器也重新检查：含 delivery_test_id 且 status=FAILED 的 metadata 在 send_report 调用 mail sender 前 ValueError，mock sender 未被调用，记录在 failed_test_sender_guard_probe.json。该检查没有连接邮件服务。

机器证据：test_mode_real_artifact_doublecheck.json、after_csv_shape_fix.json、testmode_shape_regression.xml。该组证明样本内容、结构门禁、工作流条件和测试状态隔离；实际 SMTP 接受与 Gmail 收件箱读回由主 Agent 单独验收。
