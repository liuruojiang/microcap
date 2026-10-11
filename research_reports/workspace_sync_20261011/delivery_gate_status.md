# 2026-10-11 本地整理后的正式数据门禁

代码、历史研究和工作副本已合并并同步到 GitHub main；正式数据整组验收仍为 **BLOCKED**。不能把 Git 工作区干净或云环境安装成功解释为三版本正式交付通过。

按 AGENTS.md 执行了 `scripts/top100_delivery.py refresh-all` 和 `check`，两条命令均失败。独立核对的最新已完成交易日为 2026-10-09；失败原因是共享历史基线及 V2.0、V2.3、V2.5 最终流包含非交易日 2018-06-18。

该日期在刷新前的已备份输入中已经存在；[上交所当年休市公告](https://www.sse.com.cn/disclosure/announcement/general/c/c_20180606_4567900.shtml)明确其为端午节休市日。逐文件原始 SHA-256 核对确认本次刷新未改动任何已有经济 CSV；只更新了日期新鲜度、文件时间证明和阻塞清单。详见同目录 `delivery_gate_audit.json`。

本次没有删除历史行、重算历史净值、添加白名单或批准新血缘。`outputs/top100_delivery_manifest.json` 保留真实阻塞状态。后续需要单独形成可审阅的 exact-hash 历史修正及迁移报告，取得对应授权后，再整组运行 refresh-all 与 check；通过前不得宣称本地主工作区正式数据同步完成或恢复参数扫描。

该记录属于整理及诊断证据，不是正式策略参数变更、实盘晋级或交易指令。
