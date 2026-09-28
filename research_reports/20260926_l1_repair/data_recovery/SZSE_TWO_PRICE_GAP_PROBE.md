# 深市两条持有价差：深交所官网有界探针

核验日：2026-09-26。仅 L1 研究取证；未修改正式价格、收益、缓存、`outputs/` 或 NAV。

## 结果

| 股票、日期 | 官方发行人原件与交易状态 | 价格判断 |
| --- | --- | --- |
| **002473，2019-03-18** | 深交所公告托管的[发行人原件](https://disc.static.szse.cn/download/disc/disk01/finalpage/2019-03-18/654b32b2-7b01-4ef2-bfa5-cb037c5091ed.PDF)明载当日停牌一天，2019-03-19 开市复牌并撤销 `*ST`。 | 新浪 9.03 是 03-15 已有的收盘价在停牌行的沿用值，**不是 03-18 可成交原价**。腾讯该日缺价不能解释为零收益；须按正式停牌持有估值规则单独处理。独立 BaoStock 不复权日线也报 03-18 `tradestatus=0, volume=0, close=9.03`，03-19 `tradestatus=1, close=9.93`。 |
| **300362，2021-08-05** | [2021-08-06 发行人原件](https://static.cninfo.com.cn/finalpage/2021-08-06/1210685888.PDF)称 07-19 进入退市整理交易期，至 08-06 已交易 15 个交易日；结合该窗交易日计数，08-05 属交易期。独立 BaoStock 不复权日线报当日 `tradestatus=1, volume=11,951,588` 股。 | BaoStock 当日 `low=0.75, close=0.82`，恰好对应新浪 0.75 与腾讯 0.82 的争议值；**倾向新浪混入日内低价、0.82 为收盘价**。但本轮未能从深交所原始逐股日线直接读回 08-05 收盘价，故 `0.82` 仍是第三方交叉证据，不能称已由交易所价格定案。 |

## 官网路径实测及原始留痕

- 深交所“[行情信息／历史行情](https://www.szse.cn/market/trend/index.html?code=002473)”存在日线入口。对 `www.szse.cn/api/market/ssjjhq/getHistoryData?cycleType=32&marketId=1&code={code}` 分别请求 002473、300362，均在本环境 8 秒读取超时，**没有收到响应体**，因此既不能认定接口无需令牌可读，也不能认定它要求授权。请求 URL 与异常原文记录在[manifest.json](szse_two_price_gap_probe/manifest.json)。先前对网页入口的直接请求也遇到 TLS EOF／超时；没有把网络故障伪称为接口无数据。
- 深交所静态站可读回[2021 年 8 月统计月报的一页原始 HTML](https://docs.static.szse.cn/www/market/periodical/month/W020210907547542491329.html)，其内容是月报统计表，不含 300362 的 08-05 个股日线原价；该路径只能提供聚合／专题统计，不能解决此逐日价格争议。原响应保存于 [szse_202108_month_section.html](szse_two_price_gap_probe/szse_202108_month_section.html)。
- 两份发行人原 PDF、静态月报 HTML、独立 BaoStock 原日线 CSV 均保存于 [szse_two_price_gap_probe](szse_two_price_gap_probe/)；每个成功响应的 URL、HTTP 状态、字节数与 SHA-256 见 [manifest.json](szse_two_price_gap_probe/manifest.json)。复现脚本是 [probe.py](szse_two_price_gap_probe/probe.py)，BaoStock 使用 `frequency=d, adjustflag=3`。PDF 原件提供交易状态，不提供 300362 的当日精确收盘价。

## L1 处理建议

002473 的日期争议已由原公告解决：标记停牌，不能据 9.03 假定当日成交，也不能把腾讯缺行作 0 收益。300362 有真实成交，0.75 与 0.82 是不同价格字段；在取得深交所逐股历史日线或另一等效一手行情原件前，将 `0.82` 记为**高可信第三方候选收盘价、官方未定案**，不据此静默改写正式持有收益。
