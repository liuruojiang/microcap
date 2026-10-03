# L1：冻结最早两日旧沪市成员 ST 对抗筛查

核查日：2026-10-02。**结果：在冻结旧 Top100 的沪市成员中，找到 5 只股票、9 个股日与旧元数据的“非 ST”判定冲突；其中 `600633`、`600617`、`600792` 三只／六个股日是本次新定位的强疑点。**这不是修正后名单或收益，L1 仍 FAIL。正式源码、缓存、`outputs/`、NAV 均未修改。

## 冻结输入与筛查口径

输入为[两日 117 股条件排序](../../data_recovery/conditional_two_date_rank.csv)中 `source=old_100` 的两组各 100 股，原文件 SHA-256 `f2af7b41b8e5c50c99d3f74d8870d78ce7696606ba8163dc5f0b8366b7bab59f`。其中沪市旧成员分别 **42**、**40** 股日。以本目录[上交所《2009 市场资料》](sse_factbook_2008.pdf) PDF 第 85 页 **2008-12-31 ST 快照**，和[《2010 市场资料》](sse_factbook_2009.pdf) PDF 第 214—216 页 **2009 年实施／撤销专项表**交叉；随后读回 `.microcap_index_cache/security_meta/<symbol>.json` 的 `st_intervals`，检查旧元数据在两目标日是否标 ST。

筛查规则只产生线索：年末 ST 而 2009 撤销表无退出行，或 2009 实施表有进入行。**“无退出行”不是可穷尽的否定证明**；上交所后续年册有[跨年错列反例](REVIEW.md)。具体输出 [`initial_old_sse_st_yearbook_screen.csv`](initial_old_sse_st_yearbook_screen.csv) 有 9 行，含旧排名、117 股条件排名、元数据文件 SHA-256 和年册触发字段；[`summary.json`](initial_old_sse_st_yearbook_screen_summary.json)记录文件哈希与计数。复现：`python -X utf8 research_reports/20260926_l1_repair/st_timing/sse_yearbook_event_chain/audit_initial_old_sse_st.py`。

| 股票 | 2010-01-04 旧／条件名次 | 2010-01-14 旧／条件名次 | 已有公告证据与状态实施日 |
| --- | ---: | ---: | --- |
| **600633 白猫股份，新发现** | 9／12 | 6／8 | [2009-03-23 实施公告](https://static.cninfo.com.cn/finalpage/2009-03-23/50424637.PDF)规定 **03-24** 起退市风险警示；[2009-12-25 公告](https://static.cninfo.com.cn/finalpage/2009-12-25/57435883.PDF)首页 `*ST白猫`，目标日后的 [2010-01-06](https://static.cninfo.com.cn/finalpage/2010-01-06/57472798.PDF)、[01-12](https://static.cninfo.com.cn/finalpage/2010-01-12/57494686.PDF) 首页仍为 `*ST白猫`。 |
| **600617 联华合纤，新发现** | 13／18 | 10／14 | [2009-04-29 实施公告](https://static.cninfo.com.cn/finalpage/2009-04-29/51947726.PDF)规定 **04-30** 起其他特别处理、简称 `ST联华`；[2009-12-31 公告](https://static.cninfo.com.cn/finalpage/2009-12-31/57457285.PDF)首页仍 `ST联华`。注意 04-29 公告首页已写新简称，实施日必须读正文，不能按页眉提前一天生效。 |
| **600792 马龙产业，新发现** | 53／63 | 28／37 | 2008 年末年册为 `*ST马龙`；[2009-03-19 公告](https://static.cninfo.com.cn/finalpage/2009-03-19/50320907.PDF)明确 **03-20** 仅由 `*ST` 降为 `ST`，继续 5% 限价；[2009-12-28](https://static.cninfo.com.cn/finalpage/2009-12-28/57443562.PDF)及[2010-01-11](https://static.cninfo.com.cn/finalpage/2010-01-11/57491007.PDF)发行人首页为 `ST马龙`。 |
| 600603 兴业 | 63／73 | 40／49 | 2008 年末年册为 `ST兴业`；[原日期限定审计](../600603_initial_probe/DATE_BOUNDED_ST_AUDIT.md)保存 2009-12-29 `ST兴业` 与 2010-01-30 `ST兴业` 两份发行人原件。此前已列强疑点，本轮增加独立交易所年册交叉。 |
| 600180 九发股份 | — | 49／58 | [既有正式旧目标 ST 下界](../certified_st_target_lower_bound.csv)已计入 01-14；[2008-06-26 实施公告](https://static.cninfo.com.cn/finalpage/2008-06-26/40807752.PDF)规定 **06-27** 起 `*ST`，[2011-12-14 公告](https://static.cninfo.com.cn/finalpage/2011-12-14/60321567.PDF)仅从 `*ST` 降为 `ST`，没有完全摘帽。2008 年末年册为 `*ST九发`。 |

五只的旧元数据在相应目标日均无生效的 ST 区间，9 行也均位于该受限 117 股排序的条件前 100。`600180` 已在先前经原件认证的旧目标 ST 错误下界中，**不能重复计数**；`600603` 的整个 41 行旧目标仍不能因两个目标日和年末快照而整体升级。新三只的 6 个股日也不自动写入原来的正式认证下界，需采用同样的完整公告、生效日与目标日验收口径。

## 发行人原件与公告窗核对

对新增三只分别查询巨潮 **2009-09-01—2010-01-14** 不限公告类别的发行人披露：`600633` **17** 件、`600617` **20** 件、`600792` **24** 件，每股一页完整返回；该范围内标题无“撤销”“实施”“特别处理”“风险警示”状态变更字样。`600633` 于 2010-01-06 的标题含“风险特别提示”，正文首页仍 `*ST`，不是摘帽事件。`600617` 在 2009-12-31 之后至 01-14 无该查询中的新披露；`600792` 在 01-11 的公告仍 `ST`。进入事件另按各实施日前后窄窗查询并取得三份原件。原始响应及每页 SHA-256 在 [`three_issuer_notice_manifest.json`](three_issuer_notice_manifest.json)、[`entry_window_query_manifest.json`](entry_window_query_manifest.json)；PDF URL、字节 SHA-256、本地原件与首页摘录在 [`issuer_pdf_manifest.json`](issuer_pdf_manifest.json)。复现依次运行 `probe_three_issuers.py`、`probe_entry_dates.py`、`fetch_issuer_pdfs.py`（均在本目录）。

这些是**巨潮该参数下的有界完整性**与发行人首页夹证，并非所有交易所发布渠道、全历史简档或独立逐日文件的完整性证明。公告查询时间戳没有逐件复核到开盘前；所引用的关键进入事件均早于 2010 两目标日，不依赖目标日同日披露。旧策略的同时收盘选股与成交、股本和权益等缺口也没有因 ST 筛查而解决。
