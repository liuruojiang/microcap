# 600077 上交所个股历史收盘价公开接口探查

**结论：能免费读回。**2026-09-26 从上海证券交易所官网[600077 成交概况页](https://www.sse.com.cn/assortment/stock/list/info/turnover/index.shtml?COMPANY_CODE=600077)加载的官网脚本追到 `query.sse.com.cn` 的 JSONP 接口，直接以股票代码和目标日期查询，HTTP 200。返回 `id=600077`、`closeTxDate=2010-01-04`、`closePrice=11.96`；另一个目标日返回 `2010-01-14`、`11.71`。均未登录、未付费。此为**交易所网站所列历史收盘价**的直接核验，足以排除 01-04 因供应商价格差一分钱而改变旧门槛结论的担忧。它不单独证明股本、ST 或纸面交易可执行性。

## 官网页面到接口的来源链

1. [成交概况页](https://www.sse.com.cn/assortment/stock/list/info/turnover/index.shtml?COMPANY_CODE=600077) HTML 含 `search_stockListOver`、查询日期控件及 `js_files=/xhtml/home/public/querySearch/searchKCB_hq.js`。页面标注“此栏目为历史数据”；脚本对日期的允许范围为 `1990-12-19` 至 `2022-01-07`，2010 年两日均在范围内。[页面 HTML 原件](sse_turnover_page_600077.html)。
2. [官网加载的脚本](https://www.sse.com.cn/xhtml/home/public/querySearch/searchKCB_hq.js)中，`search_stockListOver` 分支先用 `commonQuery.do`、`sqlId=COMMON_SSE_ZQPZ_GP_GPLB_C`、`productid=600077` 取 `SECURITY_CODE_A`，然后以该代码作 `FUNDID` 请求 `security/fund/queryNewAllQuatAbel.do`。表格把 `result[0].closePrice` 明确显示为“收盘价（元）”。[脚本读回副本](sse_searchKCB_hq.js)。
3. 实测前一步[代码查询原响应](sse_600077_code_lookup_response.txt)为 HTTP 200、唯一 `result`，其中 `SECURITY_CODE_A=600077`、`TYPE=0`。页面用现今公司名称作标题；本次历史价格依据证券代码和历史成交日期精确配对，并非依据现今名称反推历史名称。

| 日期 | 实际 GET 请求（页面原生参数，JSONP 回调名固定供复现） | HTTP | `result` 中精确日期项 | `closePrice` | 原响应 SHA-256 |
| --- | --- | ---: | --- | ---: | --- |
| 2010-01-04 | [接口完整 URL](https://query.sse.com.cn/security/fund/queryNewAllQuatAbel.do?jsonCallBack=jsonpCallbackProbe&FUNDID=600077&inMonth=201001&inYear=2010&searchDate=2010-01-04) | 200 | `id=600077`; `closeTxDate=2010-01-04` | **11.96 元** | `3aaccb55ff74e5864e5079a44321264ff519914bec32e091eba6941b4506bd3c` |
| 2010-01-14 | [接口完整 URL](https://query.sse.com.cn/security/fund/queryNewAllQuatAbel.do?jsonCallBack=jsonpCallbackProbe&FUNDID=600077&inMonth=201001&inYear=2010&searchDate=2010-01-14) | 200 | `id=600077`; `closeTxDate=2010-01-14` | **11.71 元** | `adb1e25196d0177aed3dc18a30be781e2a9a0c0bf226ba8a57681ba66c781c9c` |

原始 HTTP **响应体字节**分别留于[01-04 JSONP](sse_600077_20100104_response.txt)和[01-14 JSONP](sse_600077_20100114_response.txt)；请求带官网成交概况页 `Referer` 和常见浏览器 `User-Agent`，没有认证字段。返回 `result` 各有四项，另三项为月度、年度及当前附近统计，**不能直接把所有项当日线**；本表只采用 `id` 与 `closeTxDate` 同时命中的日项。01-04 日项还有 `closeMarketValue=190305.63` 万元；原公告股本 `159,118,417` 股 × 11.96 元 = `1,903,056,267.32` 元 = `190305.626732` 万元，与官网市价总值按两位小数四舍五入相符。01-14 对应 `closeMarketValue=186327.67` 万元，与 11.71 元和同一股本也相符。

**价格口径限度：**该交易所页面标注“收盘价（元）”，接口字段为 `closePrice`，请求不带复权或调整参数，并提供同日市价总值；这些事实支持将它用作当日交易所挂牌报价（未复权收盘）的交叉核验。页面与响应没有单独的“前复权/后复权/原价”枚举字段，不能声称接口显式返回了调整类型标识；若未来发生口径冲突，还需以交易所当日行情原始档案裁决。此处两日价格与现有三家逐日原价源一致。

## 其他官网路径及取得条件

- [行情报表](https://www.sse.com.cn/market/price/report/)加载的 `search_price_2021.js` 请求当下行情 `list/exchange/...`，字段含 `last` 和 `prev_close`，**没有目标历史日期参数**；不能代替 2010 年个股日项。
- [股票成交概况历史页](https://www.sse.com.cn/market/stockdata/overview/day/index_his.shtml)加载 `search_addhsl.js`，其 `commonQuery.do`、`sqlId=COMMON_SSE_SJ_GPSJ_CJGK_DAYCJGK_C` 对 `searchDate=2010-01-04` 实测 HTTP 200，但仅返回 3 个市场分组聚合行，字段为市场总市值、成交量等，**没有证券代码或个股收盘价**。[聚合原响应](sse_20100104_overview_response.txt)。
- [上证所信息网络有限公司“行情历史数据”产品](https://www.sseinfo.com/services/assortment/historical/)提供历史日 K 线；[公开价格页](https://www.sseinfo.com/services/cpfwjg/)列出日 K 线订购费用。产品收费不影响上交所官网上述个股成交概况旧接口可公开读回这两个日期，故本案无需购买或绕过授权。

本探查只增补研究证据文件；未改正式源码、缓存、`outputs/` 或 NAV。L1 整体判定仍取决于全池历史股本、ST、价格和权益链的闭合。
