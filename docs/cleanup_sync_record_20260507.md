# 2026-05-07 清理与同步记录

## 范围

- 仓库: 微盘股对冲策略
- 分支: `main`
- 目标: 清理测试文件和缓存，并把保留的实时信号脚本与输出数据同步到远端。

## 已删除

- `tests/`
- `__pycache__/`

## 删除原因

- `tests/` 按本次要求清理。
- `__pycache__/` 为 Python 运行缓存，可由解释器自动再生成，不应作为正式产物保留。

## 已保留

- `microcap_top100_mom16_biweekly_live*.py` 正式版本脚本。
- `run_top100_v1_6_v1_8_realtime_signals.py` 实时信号辅助脚本。
- `outputs/` 中已有的正式输出和本地刷新结果。
- `.codex_backups/` 备份目录。

## 备份

- 删除前备份目录: `.codex_backups/20260507_210215`

## 验证

- 清理后执行正式入口语法检查。
- 清理后删除 `py_compile` 重新生成的 `__pycache__/`。
- 推送前执行 `git diff --check`。
