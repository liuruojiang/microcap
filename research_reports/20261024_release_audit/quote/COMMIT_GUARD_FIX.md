# 实时最终提交门禁修复（2026-10-24）

已修复本轮独立审计复现的发布时序窗口。v2.5 在早期完整检查后还会计算指纹；三版还需序列化、写临时文件，原子替换也可能因文件占用等待重试。这些工作中跨过 15:00 或报价来源超过 120 秒后，旧路径仍可写入实时 CSV。

三版现仅在实时 CSV 路径传入 `before_replace` 回调。临时文件准备完成后，每次原子替换尝试之前，无条件重跑真实 `assert_realtime_meta_is_actionable` 与 `assert_realtime_quote_integrity_matches_signal`。回调在 OSError 捕获之外，缺证、过期、截止时间等失败直接中止；保留旧目标文件并清理临时文件。历史 NAV、参数、指纹计算定义及其他普通原子写入调用不变。

最终验收：

- `commit_final_tests.xml`：188 项通过（33 新增 + 155 既有报价、截止、状态和成员发布合同），14.83 秒。
- 最终测试源码仅整理标准库 StringIO 导入后，再执行 `commit_frozen_tests.xml`：33 项通过，4.33 秒。
- 新测试 `tests/test_realtime_commit_guard_20261024.py`：三版真实 builder、真实 guard、真实 atomic writer；仅上游模型输入和流逝时钟为明确的合成夹具。覆盖序列化、临时文件完成、替换失败重试三个阶段的截止/来源超龄/正常对照；v2.5 真实指纹完成后超时；缺失发布证明。
- 拒绝用例核对旧文件字节保留、未发生越界替换、临时文件清理；正常对照核对生成 CSV 内容及真正替换次数。
- 本次 v2.0 嵌入源码差异仅 `OVERLAY_SOURCE`；未更改 BASE_SOURCE/FREQ_SOURCE。v2.3/v2.5 同样仅写入工具与实时调用点。未写生产 outputs/caches。

`pre_commit_guard_backup/hashes.json`、`commit_guard_identity.json` 及三份 `commit_guard_v*.diff` 绑定备份、修复源码与新增测试。早期 `commit_tests.xml` 是开发过程中 NameError 的失败记录，已由最终 188 项通过取代；不得将其当作当前失败。原 `REVIEW.md`、`*_reproduction.json`、`test_quote_release_boundaries.py` 保留修复前发现及重现实证，其中旧漏洞断言不作为修复后验收脚本。修复后采用 tests/ 内新增回归。

本证据不等于全量历史数据回归或远端部署成功；这些由主任务独立执行和记录。本子任务未发送邮件或远端写入。
