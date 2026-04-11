# 2026-04-07 Microcap Live 修正记录

## 背景

- `Top100 live` 与 `Top50 live` 的历史锚点停在 `2026-03-18`，导致 `信号` 与 `实时信号` 不能代表最新状态。
- 本地 `.microcap_index_cache` 的 `prices_raw/share_change` 为空，直接全量重建会退化成从零抓全市场，耗时长且不稳定。
- EastMoney 在当前环境下频繁断开，导致对冲腿历史补数和个股尾部补数容易失败。

## 本次修正

- 在 [microcap_top100_mom16_biweekly_live.py](C:\Users\Administrator.DESKTOP-95I7VVU\Desktop\动量策略\微盘股对冲策略\microcap_top100_mom16_biweekly_live.py) 增加历史锚点 freshness 检查。
- `信号` 改为只接受最新已收盘锚点；锚点过期时直接阻断。
- `实时信号` 改为先校验历史锚点，再叠加实时快照；默认不允许把快照硬接到过期序列上。
- 增加最近窗口增量延长逻辑，不再每次从 2010 年全量重建代理序列。
- 最近窗口重建时只处理候选池，而不是全市场 4975 只股票。
- 价格缓存读取改为本地优先，并支持从 sibling workspace 的共享 `.microcap_index_cache` 回退。
- 个股尾部补数主链改为 `Sina -> EastMoney -> 共享缓存 -> akshare(仅无缓存时)`。
- 对冲腿 `000852` 历史补数增加 `Sina` fallback，避免 EastMoney 断开时无法生成新的 `panel_shadow`。
- 在 [analyze_top100_rebalance_frequency.py](C:\Users\Administrator.DESKTOP-95I7VVU\Desktop\动量策略\微盘股对冲策略\analyze_top100_rebalance_frequency.py) 去掉硬编码 `END_DATE = 2026-03-18`，改为动态使用当天日期。
- 在 [fetch_wind_microcap_index.py](C:\Users\Administrator.DESKTOP-95I7VVU\Desktop\动量策略\微盘股对冲策略\fetch_wind_microcap_index.py) 增加共享缓存 fallback、Sina/EastMoney 取数 fallback 和增量合并缓存逻辑。

## 已刷新结果

- `Top100 live` 已刷新到 `2026-04-07`。
- `Top50 live` 已刷新到 `2026-04-07`。
- 两套策略的 `信号` 与 `实时信号` 均可基于最新锚点运行。

## 备注

- 最近窗口目前使用增量 recent-extension 方式更新，不是一次性全市场全历史重刷。
- `实时信号` 仍然较慢，当前环境下大约需要 5-6 分钟完成一次完整实时快照。
