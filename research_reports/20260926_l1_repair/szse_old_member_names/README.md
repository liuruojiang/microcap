# 深市旧 Top100 两截面历史简称核对（隔离研究）

**结果：派生的深交所历史简称 CSV 与已保存的局部发行人公告相符，但不能单靠它签发完整历史 ST 状态。** 2010-01-04 旧初始名单中有 58 个深市成员，2010-01-14 旧目标中有 60 个，共 65 只；[逐行表](old_szse_two_date_names.csv)均由当日前最后一条 `new_name` 得到简称，没有回填当前名，也没有使用未来一条 `old_name` 猜起始状态。两日唯一呈 ST 前缀的旧深市成员是 `000010`（均为 `S ST华新`），其 2007-03-21 变更行及 01-14 的发行人公告 ST 证明一致。`000019` 在 01-14 的表内为 `深深宝Ａ`，与既有发行人公告核查相符。

本地 `.microcap_index_cache/sz_name_change_short.csv` 的 SHA-256 为 `856e18b71a21c14eba80b7111316ad5a99a30660d007248b95773affb8caca93`，字段为 `change_date,symbol,old_name,new_name`，共 7,345 行、3,105 只代码，日期从 1994-01-03 至 2026-04-30；两截面的 118 行在派生表的相邻前后事件间均无可见断链。另将[两日低市值退市候选的官方公告证据](../st_timing/initial_2010_st/two_date_st_status_evidence.csv)中的 20 个深市股日独立交叉：[交叉表](known_ST_crosscheck.csv)在简称和 ST 前缀上均 20/20 相合。这里的“相合”限于已有公告样本，不能外推全市场或全时段。

正式源码的 `fetch_sz_name_change_history()` 表明该 CSV 是从[深交所历史简称 XLSX 接口](https://www.szse.cn/api/report/ShowReport?SHOWTYPE=xlsx&CATALOGID=SSGSGMXX&TABKEY=tab2)的中文四列派生，并在缓存存在时直接返回 CSV。本地没有该 XLSX 原始字节；本轮访问原接口 HTTPS 在 TLS 握手结束，HTTP 请求超时，因而无法重新读回 XLSX 的原始表头、日期覆盖或完整性说明。派生表本身最早只列 1994 年变更事件，不能证明首个事件以前的起始简称；也不能凭相邻行连续就排除交易所源漏行或后续修订。对两日旧成员，虽然每只都找到当日前事件，**表未提供可验证的全量事件账承诺**，118 行均只能视为日期限定交叉源。`000010` 的官方 ST 证据独立成立；其余成员不能因 CSV 无 ST 前缀就自动认证为非 ST。

用 `python -X utf8 research_reports/20260926_l1_repair/szse_old_member_names/audit.py` 重算，五份输入均在脚本中固定 SHA-256。脚本只写本目录，不改正式源码、缓存、`outputs/` 或 NAV。L1 保持 FAIL。
