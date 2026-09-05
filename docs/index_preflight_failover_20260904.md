# 2026-09-04 行情换源修复验收

后续：2026-09-05 已按用户授权扩展到正式历史刷新并发布，见 `docs/index_source_deployment_20260905.md`。本页保留上一轮仅本地预检的历史记录。

## 结论与范围

本地日报正式预检此前在东方财富及新浪旧历史接口阻塞；现接入独立校验的新浪静态历史 → 腾讯历史回退链，正式命令已通过。仅用于确认最新已完成交易日，不参与历史价格、净值、成本、持仓或策略参数计算。

本次没有修改或发布 GitHub 工作流，没有修改本地 14:30 / GitHub 18:00 调度；不能将本地修复说成云端已经部署。GitHub 上一轮的交付证据不代表本次新增适配器已经发布。

## 实际接口探测

2026-09-04 23:55 北京时间，目标 sh000852，请求 2026-08-15 至 2026-09-04：

| 接口 | 本次观测 | 处理 |
|---|---|---|
| 新浪静态历史 finance.sina.com.cn | 1.868 秒，15 个交易日，末日 09-04 | 接入首选 |
| 腾讯历史 proxy.finance.qq.com | 2.935 秒，15 个交易日，末日 09-04 | 接入回退 |
| 东方财富 push2his.eastmoney.com | 连接被远端断开 | 未选入新预检链 |
| 新浪旧历史 money.finance.sina.com.cn | 前次超时，本次 HTTP 200、4.357 秒 | 间歇恢复，不再依赖 |
| 中证官网 csindex.com.cn | HTTP 200，但 16 行中包含周六 08-15 | 隔离，未静默清洗后采用 |

新浪静态与腾讯的 15 日收盘价最大差为 0.005 点（显示精度差异）；未将任何替代价格写回正式历史。接口可用性仅代表本次实测，并非永久可用保证。

## 校验与正式入口

`scripts/index_history_preflight.py`：每次请求 timeout=8 秒，不使用付费兜底；独立交易日历验证目标日期及区间完整性；拒绝缺日、重复日、非交易日、非有限数、非正价格、负成交量和 OHLC 越界。腾讯只接收目标指数的原始 day 序列。新浪仅执行本地已安装解码器，远端代码不执行。两源都失败继续阻断，不用缓存旧行情放行。请求 timeout 不是整个请求的绝对墙钟上限；正式验收的各命令另有 60 秒进程硬超时。

真实 CLI 验收，2026-09-04 23:54:34 至 23:56:13：

| 命令 | 秒 | 结果 |
|---|---:|---|
| `scripts/top100_cloud_delivery.py sync --expected-date 2026-09-04` | 2.010 | 通过 |
| `scripts/realtime_state_bundle.py preflight --max-anchor-age-days 5 --expected-date 2026-09-04` | 17.709 | 通过，实际选择新浪静态历史 |
| `scripts/realtime_state_bundle.py validate --max-anchor-age-days 5` | 1.183 | 通过 |
| `scripts/top100_delivery.py check` | 8.746 | 通过 |
| v2.0 / v2.3 / v2.5 官方 `实时信号` | 23.516 / 19.746 / 23.725 | 收盘后锚点保护正确拒绝；不是盘中发布成功 |

100/100 当日成员名称已核验，ST 交集为 0。基础输入及三版 NAV 文件前后 SHA-256 均不变。先前首次腾讯回退的预检也真实通过（15.735 秒）；后续修正了新浪编码载荷尾部解析及 UTC 日期标签转换，并重新执行全部入口。

回归命令：`C:/Python314/python.exe -X utf8 -m pytest tests -q`，最终 **500 passed, 2 warnings，20.18 秒**。两条 warning 来自已有故意构造重复 ZIP 条目的篡改测试。属性测试检查顺序变化不改变价格、重复校验幂等，例测覆盖失效回退、全源失效、目标日期不能绕过日历，以及远端 JavaScript 不执行。

## 现存数据读回（本次不发布回测绩效）

| 文件类别 | 行数 | 最后日期 |
|---|---:|---|
| refreshed panel | 8721 | 2026-09-04 |
| proxy index | 4050 | 2026-09-04 |
| base costed NAV | 4033 | 2026-09-04 |
| v2.0 NAV | 4016 | 2026-09-04 |
| v2.3 NAV | 3971 | 2026-09-04 |
| v2.5 NAV | 3971 | 2026-09-04 |
| proxy turnover（调仓事件表，非每日流） | 431 | 2026-09-03 |

不填造 09-04 调仓行。仅做数据预检/回归；没有新收益率、回撤、杠杆或持仓结论，费用与执行假设未改。

## 文件与回滚

改动：`scripts/realtime_state_bundle.py`、新增 `scripts/index_history_preflight.py`、对应回归测试；诊断脚本及源响应见 `research_reports/20260904_index_preflight_failover/`，其中 `source_probe.json` 与 `local_routes_acceptance.json` 为机器可读证据。

编辑前备份位于 `.codex_backups/20260904_235037/`，包含原 `scripts/realtime_state_bundle.py` 和 `tests/test_adversarial_microcap_delivery.py`。回滚应仅恢复该次差量，保留已有其他工作区修改；不可直接覆盖此后新增改动。没有删除数据。
