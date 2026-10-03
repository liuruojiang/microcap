# L1：上交所年度 ST 专项表与 2010 年 1 月公告窗

核查日：2026-10-02。结论：公开的上交所年度 **实施 / 撤销特别处理专项表**增加了独立官方佐证，但仍不是逐日证券主档，不能单靠“年度表中无此股”签发全期 PIT 状态。`600603` 在 2010-01-04、01-14 属 ST 的指定日期证据很强；`600137` 在这两日属普通股的证据也进一步增强，但若采用现行严格的“排除未记录短暂往返”标准，仍未取得交易所逐日档案。L1 继续 FAIL；本次没有修改正式源码、缓存、`outputs/` 或 NAV。

## 本次读回的官方原件

| 文件 | 直接观测 | 证据边界 |
| --- | --- | --- |
| 上交所[《2009 市场资料》](https://www.sse.com.cn/aboutus/publication/factbook/documents/c/10170573/files/073e042b8b87407b919e3d8e75ee91a5.pdf)，本地 [`sse_factbook_2008.pdf`](sse_factbook_2008.pdf)，PDF 第 85—86 页 | “2008 年其他特别处理公司（截至 2008-12-31）”列 `600603 ST兴业`；“2008 年取消特别处理的公司”列 `600137 浪莎股份`。 | 600603 是年末快照；600137 是年度事件，实施日仍由 [2008-06-11 发行人公告](https://static.cninfo.com.cn/finalpage/2008-06-11/40404058.PDF)证明为 06-12。不能从该两页推断逐日全窗。 |
| 上交所[《2010 市场资料》](https://www.sse.com.cn/aboutus/publication/factbook/documents/c/10170572/files/36d0635dee474838943e931bcc04c3df.pdf)，本地 [`sse_factbook_2009.pdf`](sse_factbook_2009.pdf)，PDF 第 214—216 页 | 2009 年“退市风险警示”实施表 21 行、“特别处理”实施表 4 行、“取消特别处理”11 行及“撤销退市风险警示”4 行；逐表核对，均未列 `600137` 或 `600603`。表格给实施起始日，部分给公告日。 | 这是上交所回顾性专项表，比一般公司更名表更相关；但仅凭无行不能证明表对所有短暂变更绝无漏报或迟列。 |
| 上交所[《2016 市场资料》](https://www.sse.com.cn/aboutus/publication/factbook/documents/c/10170566/files/23ad0cdde73846fa88b1ee417d306eb9.pdf)，本地 [`sse_factbook_2015.pdf`](sse_factbook_2015.pdf)，PDF 第 128 页 | 标题“2015 年实施退市风险警示的公司”下，第 24 行 `600247` 实施日为 **2014-07-01**，第 25 行 `600145` 为 **2014-05-05**。 | 直接反例：按表头年份归类会错列事件。年度专项表应按**行内实施日**读取，并逐件与发行人公告核对；不能作为无条件完备的年度变更日志。 |

原件的 URL、PDF 字节数、SHA-256 和页数见 [`source_manifest.json`](source_manifest.json)。下载与哈希复现：`python -X utf8 research_reports/20260926_l1_repair/st_timing/sse_yearbook_event_chain/fetch_yearbooks.py`。其中 2008/2009/2015 年原件 SHA-256 分别为 `c962379b144ace00f189065ce7d1d32ea8f5d7fe47e3e617adc8383a291262c2`、`ff1c1b811bba65e3f4051e7570eb01513b88dd37c0c940efa3311701535a266b`、`aa93cf2e9b746b6b0246e5adfd3d2706910171e7ea2f75dd0aa05c63445aa24a`。这些是**本次取得的官方 PDF 字节哈希**，不是逐日数据文件哈希。

## 2010 年首两个截面的公告可知范围

对巨潮历史披露接口 `https://www.cninfo.com.cn/new/hisAnnouncement/query`，分别用 `stock=600137,gssh0600137` 和 `600603,gssh0600603`，`column=sse`、`tabName=fulltext`、类别/关键词为空、`seDate=2010-01-01~2010-01-14` 查询。两股各返回 `totalAnnouncement=0`、`hasMore=false`；查询参数、原始响应 gzip、响应 SHA-256、读回数量存于 [`jan2010_query_manifest.json`](jan2010_query_manifest.json)。复现：`python -X utf8 research_reports/20260926_l1_repair/st_timing/sse_yearbook_event_chain/query_jan_2010.py`。

这个 0 只表示**该巨潮接口和参数的发行人披露集合内**没有 1 月上半月新公告，不能证明交易所其他渠道没有独立状态记录。`600137` 的 2008-06-12 摘帽事件与[2009-10-27 三季报简称快照](../600137_2008_2010_window/REVIEW.md)已是目标日前可知资料；`600603` 有[2009-12-29 发行人首页 ST 简称](../600603_initial_probe/DATE_BOUNDED_ST_AUDIT.md)和 2010-01-30 后续 ST 简称夹证。上述年度专项表提供另一交易所来源的相容交叉，但后出的年册不能冒充 2010-01-04 当时可读取的逐日主档。

## 全期重放的使用限制

这些年册可以生成**官方事件候选表**，逐项以公告首次披露日、实施日、旧新简称、停复牌及日行情交叉；不能把年册中未出现的代码机械判为全期非 ST，也不能把 2008 年末的 `600603 ST` 一直推到 2012 年。此前的[第三方逐日状态反例](../PUBLIC_DAILY_NAME_LAST_PROBE.md)同样阻止把 BaoStock `isST` 单源升级为官方 PIT 主数据。对 600603 旧目标的 41 行，只能保留“强疑点”；严格的逐日闭合及更广泛股票池、股本、权益、成交可执行性与全样本复算仍未完成。
