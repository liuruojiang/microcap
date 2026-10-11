# Windows 原生单条信号兼容性回归与修复

真实主工作区的三版 CSV 都只有一条数据行。原生原子导出使用 CRCRLF 换行，csv.reader 将每处换行解析为额外的完全空记录 []；每个文件共出现 header、空记录、信号行、空记录，而 pandas 正确显示一行数据。该现象不是多条信号。

此前新增的严格结构校验误计这些空记录，构成兼容性回归。本次仅忽略完全空的 [] 记录；带逗号的空字段行、quoted-empty 行、长短行、非空重复信号仍参与计数并拒绝。表头唯一、三版本完整及重复参数拒绝规则保持。

修改前备份 .codex_backups/20261007_231652 已核验，原 builder SHA256 为 59a117c2279ac4f40c457d312afa52ecec2bbbc33acdc2a53c77c4f9536a8a53。修复后 builder SHA256 为 d0e7ee6316f0e638fc2274c513e966ae1a44ea838df4f9333a1cd5670221b340。

正式测试包括 CRCRLF、LF／CRLF 纯空行合法对照，以及空字段行、quoted-empty、CRCRLF 非空重复信号拒绝反例。当前所选七组共 202 tests + 30 subtests PASS，证据为 windows_newline_regression.xml。

直接读取 root 刚生成的三版 primary 最终 CSV，使用 root 的完整 local_close_v2_0.txt、local_close_v2_3.txt、local_close_v2_5.txt 构建历史验收邮件样本；本 Agent 没有重新运行信号，也没有改动 primary 输出。结果为 status=OK、publication_mode=close_confirmed、signal_date=2026-09-30，三版本齐全，主题明确验收测试，正文明确历史样本不作为今日交易指令。

机器证据为 windows_native_compatibility.json；完整生成 metadata 在 native_windows_preview/metadata.json。本组未发送邮件。
