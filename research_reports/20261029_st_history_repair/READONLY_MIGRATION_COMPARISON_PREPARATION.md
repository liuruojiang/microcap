# 只读迁移比较报告入口（仅准备）

脚本：`prepare_readonly_migration_comparison_20261101.py`。本轮只做源码语法检查，未运行报告、策略、刷新或迁移。

真实29候选、30尾段和share资格复核完成后，再填写 `readonly_migration_comparison_contract.template.json` 的真实路径与SHA256并另存新契约。模板里的REQUIRED占位符故意不能执行。旧正式三版本流和成员必须指向事前冻结副本，不使用正在修改的正式输出。

## 输入与行为

- 绑定source snapshot、ST安装、metadata安装、ratio安装、base assembly、独立交易日历的精确报告哈希。`source_file_manifests` 指向已有明确的 `files: {relative_path: sha256}` 清单及根目录，逐个核实真实源文件；不同格式先在研究范围准备透明映射，不触发任何刷新。
- 29 result必须确实结束在10/29，30三个实际流全部完整结束在10/30并满足独立交易日历；核对result中每个文件SHA，30 tail不得改写29已完成收益/NAV。
- 旧、新members均非空，每期100唯一代码且rank=1..100；空members直接失败，不能产生可复核状态。30有效成员必须来自实际带日期的100成员文件。
- share资格报告须精确匹配新候选members、争议区间和日历。其 `share_approved=false` 是正常待审批，不视为缺数据。`changed_dates=0`只表示该资格模型没有目标排名变化，不能证明源股本历史已完全解决。
- 若提供既有share source裁决报告，通过 `share_resolution` 绑定；报告须给 `candidate_members_sha256`、`intervals_sha256`、`unresolved_symbols`、`source_status`。未提供时保留争议区间涉及证券为未由资格报告解决，不伪造解决记录。
- 输出 `comparison.json` 和三份逐日逐列差异CSV，包含输入完整hash、成员进出/排序差异、真实source争议状态、实际目标成员影响和待用户审批状态。
- 材料完整可以 `ready_for_review=true`，但始终 `approved=false, ready_for_migration=false`。源争议仍未解决时 `migration_status=blocked_unresolved_share_source`；否则只是 `awaiting_user_review`，不是批准。

## 执行边界

只在实际30结果完成后，由主代理调用：

```text
python -X utf8 -B research_reports/20261029_st_history_repair/prepare_readonly_migration_comparison_20261101.py --contract <真实新契约> --output <全新研究报告目录>
```

入口只导入Python标准库，不导入刷新能力的audit main、策略模块或交易接口。输入在输出前再次核hash；输出必须为不存在的新研究目录。此报告不替代安装后的三版本refresh-all/check、第二次clean rewrite audit或本地/云端真实交付验收。
