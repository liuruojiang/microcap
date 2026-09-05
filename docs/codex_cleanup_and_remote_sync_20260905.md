# Codex 临时测试清理与远端同步记录（2026-09-05）

## 清理边界

- 已清理 `D:\Codex\worktrees` 下本轮 v2.0、v2.3、v2.5、指数源和对抗验收产生的临时工作树。
- 已清理 `D:\Codex\home\automations\ic-im` 下 `adversarial-20260904` 与 `delivery-hardening-20260904` 两组临时副本。
- 已清理 `D:\Codex\tmp_pycache` 字节码缓存。
- 共移除 18 个 Git 工作树，删除前逐一核对为已合并、主线祖先或与主线补丁等价；工作树内容合计 3,224,835,957 字节，另有 2,018,657 字节缓存。
- 保留正式自动化仓库 `D:\Codex\workspaces\codex-daily-automation-probe`，并保留防止版本回退、成员数错误和过期信号复发的正式回归测试。

## 可恢复备份

两处被主线后续实现取代、但工作区曾有改动的旧草稿已备份到：

- `.codex_backups/20260905_codex_test_cleanup/obsolete_v23_digest_dirty_files.zip`
- `.codex_backups/20260905_codex_test_cleanup/superseded_delivery_preflight_draft.zip`

备份目录受 `.gitignore` 排除，不进入远端仓库。

## 同步范围

- 正式策略远端基线：`b10064622ace49bcaa5038d41423df03a17a9b14`，包含最新 v2.0、v2.3、v2.5 与 v2.5 过期信号/成员数失败关闭修复。
- 正式日报自动化远端基线：`4eba24979f8fe621b1f5c02172e37526373deef0`。
- 本次后续同步仅补充交付检查的同日短时证明复用、对应回归测试和本轮参数研究文档，不改变 v2.0、v2.3、v2.5 的正式参数。
