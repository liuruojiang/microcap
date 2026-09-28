# L1 最后一轮公开逐日简称 / ST 来源探针

探针日：2026-09-26。范围是无需付费令牌、可批量取得沪市历史逐日简称或 ST 状态的公开入口，重点反查 `600603` 的 2004—2016 状态链、2010 期初和近年。只写 L1 隔离研究目录；正式策略源码、缓存、`outputs/`、NAV 未改。

## 裁决先行

**找到一个可用的免费逐日二元 ST 交叉源：BaoStock `query_history_k_data_plus(..., isST)`。** 本机 `baostock` 匿名 `login()` 成功，逐股多年日线可批量提取，实测 `600603` 2004-01-02—2026-09-24 有 5,522 日行；其 2014、2016 年状态跳转准确落在发行人公告的实施日。它显著提高寻找旧 ST 漏判的效率，但**不能作为 2010—2026 全样本正式 ST 状态的唯一来源**：它是事后查询的第三方二元字段，不给历史简称、公告首次可得时点和原件 ID；退市尾行有确定的 `isST=0` 假阴性；全池覆盖和后期数据修订尚未认证。官方原件与点时状态链仍须保留。

未找到同时满足“无需申请、覆盖 2004—2026、逐日可批量、证券简称确为历史点时值”的沪市**交易所原始**公开接口。[上证所信息网络历史产品](https://www.sseinfo.com/services/assortment/historical/)提供历史证券基本信息文件，但页面导向业务申请；[CIIS iDATA](https://www.ciis.com.hk/hongkong/en/idata/idataintro/index.shtml)提供上交所来源的公告及参考数据，但本机无目标年份的授权文件。产品存在不等于本次取得或验证了该文件的字段、覆盖与价格。Tushare [历史名称](https://tushare.pro/document/2?doc_id=100)、[逐日 ST](https://tushare.pro/document/2?doc_id=397)接口有适用字段，然而本机 Pro 初始化因缺少 token 失败；逐日 ST 文档还列 3000 积分门槛。此前 [搜狐有日期更名探针](SOHU_NAME_EVENT_PROBE.md)和[新浪/AKShare 顺序探针](ALT_SOURCE_PROBE.md)已证明第三方简称事件表存在漏记或缺少生效日期，不能作负面证明。

## BaoStock 实测与原件交叉

接口：本机 `baostock` Python 包，匿名 `bs.login()`，随后 `bs.query_history_k_data_plus('sh.600603', 'date,code,close,volume,tradestatus,isST', start_date='2004-01-01', end_date='2026-09-24', frequency='d', adjustflag='3')`；其他股票同接口。`adjustflag=3` 是未复权；`tradestatus=1` 表示交易，`isST=1` 表示二元 ST。可复现的实际代码为 [`public_daily_name_last_probe.py`](public_daily_name_last_probe.py)，读回结果、逐股 CSV SHA256、转折日和旧成员连接结果为 [`summary.json`](public_daily_name_last_probe/summary.json)及对应 CSV。保存的是 **BaoStock 客户端解码后的行**，不是 TCP 原始响应字节；不要把 CSV 哈希描述为供应商原始传输哈希。

| 检验 | BaoStock 观察 | 官方对照 |
| --- | --- | --- |
| 600603 `*ST→ST` | 2004-05-10 仍 `isST=1` | [2004-04-30 原公告](https://static.cninfo.com.cn/finalpage/2004-04-30/14016913.html)规定该日起仍为 ST。 |
| 600603 `ST→*ST` | 2012-01-19 仍 `1` | [2012-01-18 原公告](https://static.cninfo.com.cn/finalpage/2012-01-18/60458361.PDF)规定次日生效。 |
| 600603 完全退出 | 2014-03-07 `1`，03-10 `0` | [2014-03-07 原公告](https://static.cninfo.com.cn/finalpage/2014-03-07/63645680.PDF)规定 03-10 改回大洲兴业。 |
| 600603 再进入 | 2016-02-05 `0`，02-15 `1` | [2016-02-05 原公告](https://static.cninfo.com.cn/finalpage/2016-02-05/1201973250.PDF)规定 02-15 实施。 |
| 600137 完全退出 | 2008-06-11 `1`，06-12 `0` | [2008-06-11 原公告](https://static.cninfo.com.cn/finalpage/2008-06-11/40404058.PDF)规定 06-12 实施。 |
| 600506 先 `*ST`、降 `ST`、完全退出 | 2009-03-13 `0→1`、2010-11-11 保持 `1`、2012-08-24 `1→0` | [进入](https://static.cninfo.com.cn/finalpage/2009-03-12/50084440.PDF)、[降级仍 ST](https://static.cninfo.com.cn/finalpage/2010-11-10/58645540.PDF)、[退出](https://static.cninfo.com.cn/finalpage/2012-08-23/61464917.PDF)。 |
| 600241 同类转折 | 2020-04-27 `0→1`、2021-05-18 保持 `1`、2023-05-31 `1→0` | [进入](https://static.cninfo.com.cn/finalpage/2020-04-24/1207586688.PDF)、[降级仍 ST](https://static.cninfo.com.cn/finalpage/2021-05-15/1209981072.PDF)、[退出](https://static.cninfo.com.cn/finalpage/2023-05-30/1216934551.PDF)。 |

600603 全段 CSV 共 5,522 日行，448 行 `tradestatus=0`；其 CSV SHA256 `06b72028b88cd5deeee4d919e6c4e3a60f8866a3dec25237e444f4a62cdf81dc`。本次独立的[有界对抗审计](BAOSTOCK_INDEPENDENT_CHALLENGE.md)另在 2003-01-01—2016-03-31 取得 3,214/3,214 供应商交易日，无字段空缺，并复核以上公告边界；这是同供应商内部日历连续性，不能据此证明历史原始状态未被回填。

### 期初 51 个股日和 600603 的 41/40 口径

从发行人原件夹证的[`two_date_st_status_evidence.csv`](initial_2010_st/two_date_st_status_evidence.csv)固定 2010-01-04 的 24 行、2010-01-14 的 27 行，共 29 只现已退市股票、51 个股日。本轮另以 BaoStock 查两日附近历史行情，51/51 唯一命中、51/51 `isST` 与官方原件状态一致、51/51 `tradestatus=1` 且成交量正。冻结对照为[`initial_51_official_vs_baostock.csv`](public_daily_name_last_probe/initial_51_official_vs_baostock.csv)，SHA256 `c97a8619ce18395e1bea1f4e1359a568be4a0b685043e95e5c416b388eb7b52e`。[另一独立审计](BAOSTOCK_INITIAL_51_CHALLENGE.md)还核实其未复权收盘与本地新浪原价 51/51 一致，并抽验额外 12 个已证 ST 错目标，12/12 为 `isST=1`。这些是**有限样本交叉验证**，不能扩展成所有旧股、所有年份均无漏判。

`600603` 的“41 行”是正式旧代理目标文件 [`..._base_proxy_members.csv`](../../../outputs/microcap_top100_mom16_biweekly_live_v2_0_base_proxy_members.csv)在 **2010-01-14 至 2011-08-25** 的 41 个调仓目标日。本次 41/41 有 BaoStock 对应日行，41/41 `isST=1`、`tradestatus=1`、成交量正，冻结连接表 [`600603_2010_2011_old_proxy_targets.csv`](public_daily_name_last_probe/600603_2010_2011_old_proxy_targets.csv) SHA256 `be44f56e848efb01bf8afacce0e4ed46df38178b8a35dd6e3dfabb4df2d05f1b`。

[`full_replay/members_by_rebalance.csv`](../full_replay/members_by_rebalance.csv)为**实际有效调仓成员**，该段只有 40 行，末日 2011-07-14；与旧目标比少的是 **2011-08-25**。[`replayed_effective_members_by_rebalance.csv`](../corporate_actions/replayed_effective_members_by_rebalance.csv)另含 **2010-01-04 初始种子**，故也是 41 行，但仍无 2011-08-25。这两个“41”不是同一集合；初始种子日及有效成员中的 40 个调仓日也均 `isST=1`、有成交。这是第三方和原件的强支持，**不把 41 行自动加到原件闭合的 287 行错误目标下界**。

## 严格反例与正式使用范围

- **带历史日期参数不等于点时简称。** 实测 `bs.query_all_stock(day='2010-01-04')` 返回 1,976 个证券，其中 `sh.600603` 的 `code_name='广汇物流'`、`sz.002113` 的 `code_name='*ST天润'`，均为当时尚未发生的后见名称；2010 年官方/发行人资料分别为 `ST兴业`、`天润发展`。该接口的 `code_name` 不得进入历史 ST 判定。它是明确的未来信息反例。
- **退市尾行二元状态失真。** [独立对抗审计](BAOSTOCK_INDEPENDENT_CHALLENGE.md)读回 `600070` 2025-04-30、`000787` 2013-02-08 的终止尾行：此前 ST=1，尾行 `tradestatus=0`、成交量空、`isST=0`，并无对应撤销 ST 公告。0 不能解释为可交易期间正常状态。
- **停牌收盘价陈旧。** 本轮 2010 年对 `000787`、`000805` 各取得 242 行，却全部 `tradestatus=0`、成交量为 0、收盘价全年恒为 4.68 / 1.88；`600003` 35/35 也是停牌、成交量 0、收盘价恒为 3.87。它们说明“API 返回日线”不能证明有可成交原价或可入选旧组合。独立审计的 `600687` 停牌日也沿用上日 10.30 收盘。
- **可用作有条件的排查线索。** 对 `tradestatus=1`、成交量正且日期在证券实际上市存续期内的行，`isST=1` 可优先触发官方公告正文及生效日核验；`isST=0` 也不能单独认证从未 ST。二元字段不能区分 `ST/*ST/PT`，没有公告日、知晓时点、变更理由与历史修订记录。退市、停牌及全市场漏行须另作核验。

**全样本裁决：不足以完成 2010—2026 正式 L1 修复。** BaoStock 给了高价值、无需付费令牌的逐日对抗筛查输入，已使 600603 的 41 个旧目标行成为强疑点；但全历史证券主档（含退市）覆盖、官方事件时点、真实成交、股本及权益、重放与精确哈希迁移仍未闭合。L1 保持 FAIL，不推进 L2 或晋级任何正式 NAV。
