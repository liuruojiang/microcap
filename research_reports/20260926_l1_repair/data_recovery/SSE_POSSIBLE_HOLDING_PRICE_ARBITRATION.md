# 上交所裁决潜在持有区间的沪市收盘价差

**结论：**冻结的[`possible_holding_date_differences.csv`](price_challenge/possible_holding_date_differences.csv)共 13 股日，实际其中 **11 个沪市股日**、2 个深市股日。2026-09-26 用上交所官网公开“成交概况”接口逐日读回，沪市 **11/11** HTTP 200、`id` 与 `closeTxDate` 唯一精确命中；上交所 `closePrice` **11/11 与腾讯未复权收盘一致，0/11 与新浪原价一致**。11 行没有缺日或同日重复，新浪相对交易所的偏离绝对值为 0.01–0.07 元。深市两行未调用上交所接口，也未在此裁决。

## 方法与原始证据

[官网 600077 接口来源链实测](600077_SSE_PRICE_PROBE.md)已经固定：上交所[个股成交概况页](https://www.sse.com.cn/assortment/stock/list/info/turnover/index.shtml?COMPANY_CODE=600145)加载 `searchKCB_hq.js`，其历史查询以 `FUNDID`、`inMonth`、`inYear`、`searchDate` 请求 `https://query.sse.com.cn/security/fund/queryNewAllQuatAbel.do`，页面表格把 `closePrice` 标为“收盘价（元）”。本次复用此公开、无需登录的官网接口，并逐行要求返回项同时满足 `id=证券代码`、`closeTxDate=目标日`，不把接口附带的月度、年度或当前行当成目标日。所有目标日都早于官网历史页的 2022-01-07 日期上限。

新写的独立[查询脚本](sse_holding_price_arbitration/probe.py)只读上述冻结价差 CSV；按每个沪市股日发送原生参数、保存**未改动的 JSONP 响应体字节**，以 `Decimal` 比价。逐行完整请求 URL、HTTP、四项返回中的精确匹配数、`id/closeTxDate/closePrice`、新浪/腾讯值、比对标志和响应 SHA-256 在[裁决明细 CSV](SSE_POSSIBLE_HOLDING_PRICE_ARBITRATION.csv)；[清单](sse_holding_price_arbitration/manifest.json)还记录冻结输入和输出 CSV 的 SHA-256，以及 11 份响应各自的字节数和哈希。请求附官网成交概况页 `Referer` 与常见浏览器 `User-Agent`，不含认证字段。

| 日期 | 股票 | 新浪（元） | 腾讯（元） | 上交所（元） | 上交所−新浪（元） | 原 JSONP 响应 | 响应 SHA-256 |
| --- | --- | ---: | ---: | ---: | ---: | --- | --- |
| 2015-12-03 | 600145 | 7.06 | 7.05 | **7.05** | −0.01 | [原件](sse_holding_price_arbitration/sse_600145_20151203.jsonp) | `0bebe8aa89fcab670212f267aae1981c428f8c16c3a7818a0e4ce22acab45cc4` |
| 2015-12-03 | 600213 | 14.99 | 14.97 | **14.97** | −0.02 | [原件](sse_holding_price_arbitration/sse_600213_20151203.jsonp) | `2b3ffa75ba3d88d6cf7ceee9262dc56213154a0e9a696077d78ebd35c3bb3ae2` |
| 2017-02-17 | 600306 | 15.12 | 15.10 | **15.10** | −0.02 | [原件](sse_holding_price_arbitration/sse_600306_20170217.jsonp) | `cda30226d6ac2befe2fd59e5ce3b29e76fa85f6c910a22f7f91a21f05ed6c76c` |
| 2017-10-11 | 600306 | 13.86 | 13.87 | **13.87** | +0.01 | [原件](sse_holding_price_arbitration/sse_600306_20171011.jsonp) | `5501804d883e6d63182610ea701a36a6f91fa8fbe890316a31c0ab97e7389319` |
| 2017-10-18 | 600306 | 13.41 | 13.40 | **13.40** | −0.01 | [原件](sse_holding_price_arbitration/sse_600306_20171018.jsonp) | `2bee21be37282dc12bdd5176b46763978b16de93915c4a4b5920e120c8263cf7` |
| 2018-08-15 | 600306 | 6.65 | 6.66 | **6.66** | +0.01 | [原件](sse_holding_price_arbitration/sse_600306_20180815.jsonp) | `cb83413d100a3abd90584fcf360fb6f7c00fba26c6b42f61591bc5f1ee383e74` |
| 2018-08-07 | 600634 | 2.12 | 2.13 | **2.13** | +0.01 | [原件](sse_holding_price_arbitration/sse_600634_20180807.jsonp) | `ef868cef9d8a3d5b643a7689689badc216a474aab451ed8777a01af1898ca3e7` |
| 2015-12-03 | 600766 | 16.70 | 16.63 | **16.63** | −0.07 | [原件](sse_holding_price_arbitration/sse_600766_20151203.jsonp) | `49852c9dc75290c7fc8c7d5b78358094ccf19b66a3630d25dded8548c9a66087` |
| 2018-04-24 | 600766 | 8.82 | 8.81 | **8.81** | −0.01 | [原件](sse_holding_price_arbitration/sse_600766_20180424.jsonp) | `73b18572a0c44145d95c97f105ee0fef2f7bffd55b99dac2d2db81b52a3912de` |
| 2018-05-17 | 600767 | 5.93 | 5.94 | **5.94** | +0.01 | [原件](sse_holding_price_arbitration/sse_600767_20180517.jsonp) | `9d5fe932e14acc3c51298dd052a2590005b18f6f1b873fbdfb26ff0ea3d298e0` |
| 2018-05-28 | 600767 | 5.90 | 5.93 | **5.93** | +0.03 | [原件](sse_holding_price_arbitration/sse_600767_20180528.jsonp) | `be3515ec2ff35d2adad96f0c6a4a47ceb6ec439502dec904736c1a3ae735755a` |

**两行未裁决：**`002473` 2019-03-18（新浪 9.03 元、腾讯缺日）和 `300362` 2021-08-05（新浪 0.75 元、腾讯 0.82 元）属于深圳证券交易所；明细 CSV 标为 `out_of_scope_sz`，上交所字段空白。

**口径和影响边界：**上交所页面称“收盘价”，请求没有复权参数，接口同日还给市价总值；响应没有单列复权类型字段。本裁决支持以网站挂牌日收盘对这 11 个双源价差取舍，并不能证明两家供应商历史文件全部正确或错误。冻结清单的“可能持有”只由上一旧信号日的市值门槛作宽松代理，未验证这些股票在补齐股池、ST 与股本时点、交易约束后的真实持仓。因此不能把 11 个价差直接换算为旧正式 NAV 错误或发布修正绩效。L1 继续 **FAIL**；本轮仅写研究区，未修改正式源码、缓存、`outputs/` 或 NAV。
