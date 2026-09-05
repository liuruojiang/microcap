# Microcap Top100 Strategy

本仓库维护 A 股微盘 Top100 策略、对冲/风控叠加层、数据刷新和正式信号/业绩产物。

## 当前正式口径

- 2.0 / 2.3 对抗审计修复记录：`docs/v20_v23_adversarial_audit_20260904.md`。交易参数不变；回撤纳入初始本金，R² OFF 真正旁路，最终 CSV / NAV 和日报时序采用失败关闭校验。
- 默认主线：`microcap_top100_mom16_biweekly_live_v2_0.py`
- 默认表现口径：`v2.0 + costed`
- v2.0当前规则：16天相对动量、退出缓冲0、过热OFF、目标波动率OFF；持仓时固定1倍微盘多头及0.8倍股指对冲。修订标识`plain_mom16_fixed1_20260904`，正式成本净值为`microcap_top100_mom16_plain_fixed1_v2_0_costed_nav.csv`。旧targetvol15文件只作历史回滚依据，不能作为当前日报或信号来源。v2.3/v2.5各自按下列正式规则运行。
- v2.3当前规则：动量25天、指数加权半衰期2.5天、R²过滤OFF、入场0、退出缓冲0.08；10日波动率过热26%触发/20%恢复；执行对冲0.8、信号对冲1、目标波动OFF。修订标识`plain_lb25_hl2p5_r2off_vol10_26_20_20260904`。原R²0.08/恢复19.5%的成本文件仅作历史回滚依据。
- v2.5当前规则：微盘股单边20日对数WLS动量、指数加权半衰期3天、入场0、退出0；不对冲中证1000，R²/止损/回撤/衰减/过热/目标波动率/现金收益/融资全部OFF。修订标识`plain_lb20_hl3_entry0_exit0_20260905`，正式成本净值为`microcap_top100_mom16_lb20_hl3_entry0_exit0_no_targetvol_v2_5_costed_nav.csv`。旧17日、入场46%、退出25%文件只作历史回滚依据。
- 正式下游版本：`v2.3`、`v2.5`；替换记录见`docs/v23_plain_promotion_20260904.md`和`docs/v25_plain_promotion_20260905.md`。
- 数据源：公开/本地重建 Top100 代理，不是官方 Wind `868008.WI`
- 历史股票池：完整历史证券主表；历史 ST 使用时点化进入/撤销区间

任何信号、名单、回测或图表都必须先刷新并读回新鲜度。正式日频流的最新日期必须一致；调仓表的最新日期是最近调仓事件日期。

## 常用命令

同步主目录并生成、验收三个正式版本（默认入口）：

```powershell
python -X utf8 scripts/top100_delivery.py refresh-all
python -X utf8 scripts/top100_delivery.py check
```

只有两条命令均退出 0 才能称为整套同步完成。检查覆盖远端正式核心代码/冻结基准、独立最新收盘日、底层数据、v2.0/v2.3/v2.5 成本净值、显示净值、最终信号、clean 历史审计及内容哈希。调仓表核对最近调仓日，而非强行要求每天有调仓。

`scripts/realtime_state_bundle.py validate` 只检查底层状态包，输出 `scope=base_state_only`，不代表三个版本已同步。单版本入口仍用于查询/诊断；不能将一次单版本成功当作全组交付。查询改写了产物或输入变化后，旧的整组验收清单会失效。

`outputs/top100_delivery_manifest.json` 是本地整组验收记录。刷新中断、数据源失败或并发更新均失败关闭；不要手动将状态改成 complete。Git 不携带全部最终产物，独立工作树/云端成功后仍须在日常主目录运行上述命令。


查询 v2.0 收盘确认信号或表现：

```powershell
python microcap_top100_mom16_biweekly_live_v2_0.py 信号
python microcap_top100_mom16_biweekly_live_v2_0.py 表现 近1年
```

刷新历史成员涉及的 ST 元数据：

```powershell
python scripts\refresh_recent_st_metadata.py --since 2010-01-01 --max-workers 16
```

## 发布门槛

- 当前名单必须恰好 100 只，代码唯一、排名 1—100。
- 当前 ST、*ST、PT 名称/代码交集必须为 0。
- 历史成员的时点化 ST 违规必须为 0。
- 代理元数据内容指纹必须与当前证券元数据缓存一致。
- 历史改写默认拒绝；受审计迁移后必须在无迁移参数下再次运行并得到 `clean`。
- v2.0 数据血缘变化后必须重新生成 v2.3/v2.5，不能沿用旧输出。

## 测试

```powershell
python -B -m pytest tests\test_top100_data_guards.py tests\test_microcap_v2_review_remediations.py -q -p no:cacheprovider
python -B -m py_compile microcap_top100_mom16_biweekly_live_v2_0.py microcap_top100_mom16_biweekly_live_v2_3.py microcap_top100_mom16_biweekly_live_v2_5.py
```

## 文档

- 工作区硬规则：`AGENTS.md`
- 查询规则：`QUERY_RULES.md`
- 新策略测试标准：`docs/new_strategy_test_standard_process.md`
- 当前数据血缘：`docs/microcap_top100_post_p0_lineage_20260629.md`
- 2026-08-20 ST/历史股票池事故复盘：`docs/microcap_live_member_st_filter_incident_20260820.md`

`outputs/`、本地行情缓存和 `.codex_backups/` 主要是可重建或本机审计产物；不要把旧导出文件当作新鲜数据证明。
