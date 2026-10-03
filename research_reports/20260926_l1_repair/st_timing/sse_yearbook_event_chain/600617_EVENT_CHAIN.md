# 600617：2009 年入 ST 至 2014 年首次完整摘帽

核查日：2026-10-02。**结论：旧元数据漏掉 2009-04-30—2010-04-29 及 2011-12-22—2013-01-30 的 ST 状态。**冻结旧 v2.0 调仓目标中，600617 于 2010-01-14—04-22 连续 8 个目标日入选；2010-01-04 初始种子另有 1 行。这 9 个日期均处在发行人已公告的 `ST联华` 状态。这是旧历史成员排除错误的证据，不是修正后组合或收益。正式源码、缓存、`outputs/`、NAV 未改，L1 仍 FAIL。

## 发行人原件事件链

以下日期是巨潮公告索引日；实施日期以公告正文为准。原 PDF、前两页文字摘取及逐件字节 SHA-256 记录于 [`600617_chain/transition_pdf_manifest.json`](600617_chain/transition_pdf_manifest.json)。

| 索引日 | 发行人公告及 SHA-256 | 正文所载实施日和状态 |
| --- | --- | --- |
| 2009-04-29 | [实行其他特别处理](https://static.cninfo.com.cn/finalpage/2009-04-29/51947726.PDF)，`8778ef741d5b964a8fb8d2891c901623628292ba3a9c3f74f5739827704d7e48` | 2009-04-29 停牌；**04-30** `联华合纤 → ST联华`，5% 涨跌幅。首页提前显示 `ST`，不能把首页当实施日。 |
| 2010-04-30 | [实行退市风险警示](https://static.cninfo.com.cn/finalpage/2010-04-30/57900859.PDF)，`c2cec9627fb7b06046112b7020658ff6de93a78c9825ef24c6afb3bc1e7cfa05` | 04-30 停牌；**05-04** `ST联华 → *ST联华`，继续 5% 涨跌幅。正文第二节有“其他特别处理”套语，但标题、简称和第一节均明确是加星。 |
| 2011-12-21 | [撤销退市风险警示并实施其他特别处理](https://static.cninfo.com.cn/finalpage/2011-12-21/60349071.PDF)，`cc2b139aafe6b5d3660761062e4a8c9d82cbaa6a521c5b3f4005d2aaca7a33e1` | 12-21 停牌；**12-22** `*ST联华 → ST联华`，继续 5% 涨跌幅。**此举不是完全摘帽。** |
| 2013-04-27（索引） | [被实施退市风险警示](https://static.cninfo.com.cn/finalpage/2013-04-27/62441396.PDF)，`c11b3108224ccbb3999ddac395fc6554c996c97ce43a9d09b5da76a64b01e971` | **05-03** `ST联华 → *ST联华`。正文署名 04-27，却把“公告日”写为 **05-02**；不据此认定 04-27 即可用于交易决策，点时可得日保守按 05-02。 |
| 2014-03-24 | [撤销退市风险警示](https://static.cninfo.com.cn/finalpage/2014-03-24/63713707.PDF)，`a8e1d8c6cee0f84ee1ef4bb0e8421529f2af53ae1503208f5e073a8fb93816fc` | 03-24 继续停牌；**03-25** 复牌，`*ST联华 → 联华合纤`，涨跌幅 5%→10%。公告明确无其他风险警示情形，故为本链首次**完整**退出 ST。 |

中间交叉证据：[2013-01-31 预亏暨风险提示](https://static.cninfo.com.cn/finalpage/2013-01-31/62087422.PDF)（SHA `237ad0b937a47492d652a079b3a46732d85590f75e97fa360f3b3482464bd117`）及 [2013-04-12 提示](https://static.cninfo.com.cn/finalpage/2013-04-12/62352262.PDF)（SHA `4a4a44234ed13116a45b28163d55ef2ac1f9964cdfd0ceda2f7d33190a48ef09`）页眉均为 `ST联华`，正文说未来可能或将在年报后加星，**均非 2013-01-31 生效事件**。[2014-03-15 摘帽申请](https://static.cninfo.com.cn/finalpage/2014-03-15/63679727.PDF)（SHA `cbb76a3ec39f93d533e1fbf24bbac1e62602b5c3714cd6459442161014d8b21a`）仍待交易所批准，也非摘帽实施日。

## 索引覆盖与旧数据影响

巨潮无公告类别限制地查询 `stock=600617,gssh0600617`、`column=sse`、`seDate=2009-04-29~2014-03-25`、每页 30 件，返回 **18 页／511 件唯一公告**。查询参数、各页原响应及 SHA 在 [`600617_chain/notice_manifest.json`](600617_chain/notice_manifest.json)，完整标题、索引日和原件 URL 在 [`600617_chain/all_notices.csv`](600617_chain/all_notices.csv)（SHA-256 `8b22ce3ffab6c83582ffb7f721d8d5a727f8443821e29176b206e17f21ed3eae`）；可复现入口为 [`audit_600617_notices.py`](audit_600617_notices.py) 与 [`fetch_600617_chain_pdfs.py`](fetch_600617_chain_pdfs.py)。标题筛查未发现上述节点间另一条撤销或重新实施 ST 公告；这只限定巨潮该查询范围，不等于所有交易所状态源的负面证明。

旧 `.microcap_index_cache/security_meta/600617.json` 的 `st_intervals` 为 `2010-04-30—2011-12-21`、`2013-01-31—2014-03-24`：第一段开始晚于真实入 ST，第二段开始把预警误作实施，中间将“*ST 降为 ST”误作完全退出。按上述已取得原件，二元特别处理状态应至少连续覆盖 **2009-04-30—2014-03-24**，停牌日另由交易状态判断；`ST` 与 `*ST` 的细分类转折分别为 2010-05-04、2011-12-22、2013-05-03。2014-03-25 才解除。

旧目标输入 [`outputs/…_base_proxy_members.csv`](../../../../outputs/microcap_top100_mom16_biweekly_live_v2_0_base_proxy_members.csv) 读回 600617 的 8 个目标日：2010-01-14、01-28、02-11、02-25、03-11、03-25、04-08、04-22；文件 SHA-256 `04a851bb6e7e67f181607c499a0ca3264443b6e3b655817ac0933532517f7517`。[`full_replay/members_by_rebalance.csv`](../../full_replay/members_by_rebalance.csv) 也有这 8 个有效成员行，SHA-256 `7563fdab9b4a37f35fc2a8e54c967fc6bf6e3ad6a4dc6f298aec59f4beb58b1c`。[`replayed_effective_members_by_rebalance.csv`](../../corporate_actions/replayed_effective_members_by_rebalance.csv)另有 2010-01-04 初始种子。旧目标中的后见名称 `国新能源` 不作为时点简称证据。公告事件和目标连接证明这 9 个旧成员股日需要重建，但不能直接推算正式净值变化。

范围限制：本报告闭合 **600617 进入 ST 至 2014-03-25 首次完整退出**的可见发行人状态转折，未认证 2014 年以后再入 ST、全沪市、全样本或点时公告传输延迟；2013-04-27 索引与正文 05-02 公告日冲突需在正式事件账中保留两字段，不能抹平。正式 L1 修复须与全历史证券主档、其余证券事件、逐日交易约束、精确哈希迁移及干净重跑一起完成。

