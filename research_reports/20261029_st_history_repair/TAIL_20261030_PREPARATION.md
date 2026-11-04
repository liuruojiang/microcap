# 10/29 至 10/30 隔离诊断尾段准备

状态：仅准备，未联网、未执行、未批准。ST 全通过不解除 share144 历史股本来源边界；正式迁移 readiness 仍需按新成员和真实资格过滤复审股本影响。

## 输入与目录

- 冻结 runtime：`replay/runtime_20261029`（实际目录由主代理核对）。仅在该研究 runtime 或其独立候选副本执行；禁用 SHARED，导入模块必须属于该根。
- 已完成的 `diagnostic_replay_20261029/{proxy.csv,members.csv,turnover.csv,proxy_meta.json,base_costed.csv,v2_0.csv,v2_3.csv,v2_5.csv,diagnostic_result.json}`；读回 SHA、行数、最新日均核对 2026-10-29。
- `replay/independent_sessions_20261030.csv`，明确唯一递增，10/29 下一交易日恰为 10/30；与真实 sessions_for_day 返回结果比较。
- 主面板须有完整 10/30 microcap/hedge 所需真实市场输入，不得仅追加日期；前三版本必须最终均到 10/30。
- 全 5438 metadata 安装报告、源码 manifest、归档 pointer 与 registry、29 ratio 完整闭包保持冻结。
- 新输出单独使用 `diagnostic_tail_20261030`，禁止覆盖 29 诊断或正式 outputs。

## 执行成员与无调仓判定（先于任何网络请求）

1. 读取新生成 turnover 所有 execution_date/effective_date/return_start_date，要求日期均可解析且属于独立日历。
2. execution_date 或 effective_date 落在 (10/29,10/30]：停止 100 尾段，转完整新调仓范围。return_start_date=10/30 且 execution/effective<=10/29 合法；不能把首收益日误判执行日。任何 return_start 早于 execution/effective 均拒绝。
3. 新 members 的 rebalance_date 不能落在尾段；调用实际 `base.build_biweekly_rebalance_dates(calendar)` 同样不能存在新增调仓日。
4. 不直接把最后目标名单视为执行成员。复用 `base._proxy_target_members_map_from_saved_members(members_path)` 与 `base._reconstruct_effective_members_from_saved_targets(target_members_map, calendar_through_29, pd.Timestamp("2026-10-29"), workers)`，由真实买卖限制重放得到 members_after。此步骤使用 `validated_source_batch(root)` 与 runner 的 `frozen_metadata`，不允许修补 metadata。或者在真实全池模拟处保留其返回的 effective_members_map（需另行评审 runner 变更）。
5. 执行成员必须恰 100 唯一证券，与最新执行对应；当前 ST 交集为零。将实际结果写 dated effective_members.csv、记录成员/turnover/有效成员 SHA；不伪造 frozen_tail_authority 或 share baseline approval。

## 可复用真实数据接口（本次只列出，未调用）

```python
from scripts.stock_daily_inputs import refresh_daily_inputs
raw_receipts = refresh_daily_inputs(root=root, symbols=actual_100,
    as_of="2026-10-30", allow_network=True, force_source_query=False)

# root 为隔离候选；每只返回独立真实请求/状态回执，首失败停组。
# 复权须为新收盘后实际取得的因子/除权参考源；不能以29旧回执冒充30。
from scripts.stock_return_pipeline import load_return_prices
for symbol in actual_100:
    load_return_prices(root=root, symbol=symbol,
        raw_path=root/".microcap_index_cache/prices_raw"/(symbol+".csv"),
        master=master, as_of="2026-10-30", allow_network=True)

# 断网消费，要求真实 raw/status/ratio 全证据；显式停牌才可估值延续。
from scripts.stock_tail_returns import load_tail_returns
for symbol in actual_100:
    ret = load_tail_returns(root=root, symbol=symbol,
        raw_path=root/".microcap_index_cache/prices_raw"/(symbol+".csv"),
        master=master, tail_dates=pd.to_datetime(["2026-10-29","2026-10-30"]))
```

重复运行前读已有 raw/status/ratio 回执；源失败保留 attempt，不把未返回当停牌或零收益。隔离 raw 更新后比对 29 及以前原价字节/数值未重写；metadata 不随 raw30 自动更新，29历史 metadata 是已冻结输入。

## 收益与三版本读回

沿用实际无调仓尾段公式：100 只 10/30 经复权日收益有限且齐全后取等权均值，proxy30.close=proxy29.close*(1+均值)。只能在旧 proxy 全行精确保留的基础追加一天；历史 NAV 不能复制冒充重算。写入完整 consumption proof（29 full_rebuild_anchor、实际100、成员三文件 SHA、尾段日期）。

调用实际 `base.rebuild_costed_nav_from_proxy_turnover`、`base.load_close_df`、`base.run_signal`、`base.apply_momentum_gap_exit_buffer`、`v20.overlay_mod.apply_v2_0_execution`、v23/v25 的 `build_*_common_index` 与 `build_*_result`；沿用已验证 diagnostic runner 的调度顺序及日期/数值读回规则。三流均完整重算，比较截至29全部旧行（按现有允许尾字段合同判定），30全流同日，并复查成本/holding控制收益。

## 不能直接使用的外层入口

`base.try_extend_proxy_index_without_rebalance` 当前会在末尾无条件读取 FROZEN_TAIL_AUTHORITY_PATH；内嵌 `refresh_price_cache_tail` 又同时刷新 share。未批准诊断不得通过造 authority 或 share baseline 来使此入口成功。使用上面的真实数据/收益函数组装研究候选，报告保持 approved=false、formal_written=false、share_scope=diagnostic_pending_reaudit。其结果不自动满足生产 delivery gate。

## 交付证据

记录根路径/源码manifest、29旧帧与新完整帧SHA、日历SHA、执行日判定、100有效成员及当前ST交叉、每只raw/status/ratio新回执、29历史不变检查、三流与proxy/base/panel的最新日期和行数、share144新成员资格重审状态。完成后主代理再决定迁移报告准备；这里无任何批准。


## 独立100数据 stage 最小复制（本轮新增，未运行）

先用冻结29代码的 `actual_members`（内部调用真实执行重建函数）生成 effective_members.csv。单独 execution_proof.json 记录 member_derivation=actual_reconstruct_effective_members_from_saved_targets、as_of=2026-10-29、effective_members_sha256、source_manifest_sha256，以及实际 members/turnover/calendar 的哈希。该 proof 是推导回执而非批准，不能手工把 target100 改标签。

```powershell
python -B -X utf8 research_reports/20261029_st_history_repair/seed_executed100_daily_stage_20261101.py --runtime <runtime29> --members <实际effective_members.csv> --proof <实际execution_proof.json> --stage <research>/daily_executed100_20261030
# 完成检查后加 --execute，仅复制不联网，不改runtime。
```

此工具只复制100只完整raw、status sidecar及递归path/hash证据、ratio当前manifest/adjusted/source及manifest所指完整旧bundle、全master。旧bundle的raw锚必须保留，否则真实store会拒绝验证前缀。ST归档和registry不改写；seed记录冻结source_manifest与新ST pointer SHA，最终装配必须再核相同身份。该stage不是可运行策略根，也不是已安装全池。

在受控独立进程导入冻结runtime的scripts后，使用前述真实接口：refresh_daily_inputs(root=stage,symbols=seed.symbols,as_of=10/30)；然后逐只 load_return_prices(root=stage,...allow_network=True)。两者首失败停止，不开全池、不用share刷新外层入口。可用真实新回执覆盖30；旧回执时间不足则必须实际请求，不能手改时间。

安装前读回：raw截至29的日期/close_raw全等且无删日（保留原始文件作备份）；status新raw_sha256、master/lifecycle、真实tradestatus覆盖29/30；ratio anchor_date不变、截至29 adjusted值在store既定1e-12范围内无收益重写、as_of30且qfq/hfq实际新回执在收盘后。调用 load_tail_returns(root=stage,tail_dates=[29,30]) 断网验证100只全部通过，写manifest包含每只新raw/status/ratio与全部绑定文件SHA。已知401/403/429停组，不删除stop标记重试。

最终仅把经过上述验证的100输入闭包复制到一个新的隔离tail候选根，29原runtime保留原样；回读每项hash及ST/source身份不变。该安装不移植生产authority、不批准share、不改正式outputs。随后才运行离线run_isolated_tail_20261030.py。未取得实际100名单与新源回执前，状态必须是“准备完成，数据未就绪”。
