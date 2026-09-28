# L1 历史 ST 扩展核验：旧目标股前 30 只

## 结论与范围

本批按旧回测目标行数降序，从有 ST 公告标题线索、且未计入此前 18 只已证错股的旧目标股中冻结前 30 只。它们合计涉及旧目标 **4,421 行**。截至本报告，公告正文与生效日闭合后，新增可证的错误入选目标 **37 行、2 只**：600506 为 35 行，600241 为 2 行；与此前独立的 250 行不重叠，因此全 L1 当前已证下界为 **至少 287 行**。这不是全量错行数，也不表示其余 28 只或全策略已通过 ST 核验。

本批只改动 `research_reports/20260926_l1_repair/st_timing/` 诊断目录。未改正式代码、缓存、代理成员或净值；错行不自动推导绩效差值。

## 官方原件覆盖及可复算性

固定名单与旧目标行数见 [`frozen_top30.csv`](expansion_top30/frozen_top30.csv)；对应从 1990–2026 全市场六类公告归档中摘出的 262 条去重线索见 [`frozen_notice_leads.csv`](expansion_top30/frozen_notice_leads.csv)。262 份 CNInfo 官方原件均已获取：248 份 PDF、14 份早期官方 HTML，来源 URL、公告日、原件 SHA256、字节数、抽取字符数及本地路径逐件保存在 [`notice_pdf_manifest.csv`](expansion_top30/notice_pdf_manifest.csv)。重新读取原件与抽取文本后，262 份原件的长度和 SHA256 均与清单相符，见 [`source_audit_summary.json`](expansion_top30/source_audit_summary.json)。这些哈希是原件完整性证据，不把标题检索或文本抽取成功当作状态结论。

新增错行的冻结清单为 [`new_proven_bad_targets.csv`](expansion_top30/new_proven_bad_targets.csv)，SHA256 `08c960ecfa59817c6610ee7191c48fa20d8ebc34e69a1736bab9bac05bf1ca5c`。每行含旧目标日、名次和 ST 区间边界。逐事件原件 URL、生效日、SHA256 在 [`body_proved_event_evidence.csv`](expansion_top30/body_proved_event_evidence.csv)；核验代码为 [`st_expansion_certify.py`](st_expansion_certify.py)。

## 新增的正文闭合区间

| 证券 | 正文所证状态链 | 错误旧目标行 | 最早错误目标日 | 官方证据 |
| --- | --- | ---: | --- | --- |
| 600506 香梨股份 | 2009-03-13 起 `*ST 香梨`；2010-11-11 仅撤销退市风险警示，**继续其他特别处理**为 `ST 香梨`；2012-08-24 复牌并改回 `香梨股份`，完全退出 ST | 35 | 2010-11-18，原名次 24 | [进入公告](https://static.cninfo.com.cn/finalpage/2009-03-12/50084440.PDF)、[降级仍 ST 公告](https://static.cninfo.com.cn/finalpage/2010-11-10/58645540.PDF)、[完全退出公告](https://static.cninfo.com.cn/finalpage/2012-08-23/61464917.PDF) |
| 600241 时代万恒 | 2020-04-27 起 `*ST 时万`；2021-05-18 仅撤销退市风险警示，**继续其他风险警示**为 `ST 时万`；2023-05-31 改回 `时代万恒`，完全退出 ST | 2 | 2021-05-20，另有 2021-06-03 | [进入公告](https://static.cninfo.com.cn/finalpage/2020-04-24/1207586688.PDF)、[降级仍 ST 公告](https://static.cninfo.com.cn/finalpage/2021-05-15/1209981072.PDF)、[完全退出公告](https://static.cninfo.com.cn/finalpage/2023-05-30/1216934551.PDF) |

关键语义是 `*ST→ST` **不能**记作退出。600506 退出公告正文同时回顾了 2009 和 2010 两次生效变更；600241 降级公告正文明确写明 `*ST 时万→ST 时万` 及生效日。由此把旧目标日期逐日与闭合区间相交，得到 600506 在 2010-11-18 至 2012-05-17 的 35 行，以及 600241 在 2021-05-20 和 2021-06-03 的 2 行。上述 6 份 PDF 各有本地 SHA，独立审计可用 URL 重下比对。

## 未闭合项与下一步

其余公告不能因标题写“撤销”、只有风险提示、或未检索到新公告就判定非 ST。逐件待审的 **222 份**状态线索原件及其证券、公告日期、公告 ID、标题、URL、SHA256、相对旧目标期间位置、未闭合原因，见 [`unresolved_state_notice_review.csv`](expansion_top30/unresolved_state_notice_review.csv)；逐股汇总见 [`top30_symbol_disposition.csv`](expansion_top30/top30_symbol_disposition.csv)。其中 55 份在首个旧目标日前，须闭合期初状态；126 份在该股旧目标时段内，须按正文确认进入、`*ST→ST`、完全退出的实际生效日；41 份在最后旧目标日后，不能反推历史状态。该表也保留已证两只的其他上下文公告，避免误以为整只股票的全历史均已审完。

优先续审 **600603 兴业**：2003-04-30 公告 `10693002` 使其自 2003-05-12 入 `*ST`；2004-04-30 官方 HTML `14016913` 正文写明自 2004-05-10 `*ST 兴业→ST 兴业`，仍受 5% 涨跌幅限制；2012-01-18 公告 `60458361` 的公告头仍写 `ST 兴业`，次日再入 `*ST`。这条状态链可能覆盖 **41 个** 2010–2011 年旧目标行，见 [`title_overlap_triage_only.csv`](expansion_top30/title_overlap_triage_only.csv)。

为追查其中是否有短暂完全退出，另行按 **2004-05-10 至 2012-01-19**、`stock=600603,gssh0600603`、无关键词/类别限制查询 CNInfo 全部公司公告：服务端报 353 条，12 页取得 353 个唯一 ID，逐页原始响应 SHA256 及条数见 [`page_manifest.csv`](expansion_top30/600603_full_filings/page_manifest.csv)、全部标题与原件地址见 [`all_announcement_index.csv`](expansion_top30/600603_full_filings/all_announcement_index.csv)。标题索引没有期间内的撤销 ST/其他特别处理公告；2010-08-04 [公司更名公告](https://static.cninfo.com.cn/finalpage/2010-08-04/58259083.PDF)正文明确写“证券简称不变，仍为 ST兴业”，原件 SHA256 `ec6246e49fa88823c1a94f9b8e2f74e3343fe43215c066ccc81f34885adb5dcf`。[2009 年报](https://static.cninfo.com.cn/finalpage/2010-03-03/57644728.PDF)、[2010 年报](https://static.cninfo.com.cn/finalpage/2011-02-25/59043229.PDF)、[2011 半年报](https://static.cninfo.com.cn/finalpage/2011-07-26/59724935.PDF)、[2011 年报](https://static.cninfo.com.cn/finalpage/2012-01-18/60458105.PDF)的股票简况均为 `ST 兴业`；这些及季度报告原件逐件哈希见 [`periodic_source_manifest.csv`](expansion_top30/600603_full_filings/periodic_source_manifest.csv)。[上交所 2010 年上市公司表](https://www.sse.com.cn/aboutus/publication/yearly/documents/c/10061083/files/51de7c7f4ad14ec89d6df9dc732849f2.pdf)与[2011 年上市公司表](https://www.sse.com.cn/aboutus/publication/yearly/documents/c/10061082/files/a21d89f75cc64fb5bbcabeb3fdc84ee9.pdf)也标 `ST 兴业`。

这些正向快照与完整公司公告标题索引高度支持长期 ST，但公告标题检索的“无命中”以及年度/季度快照仍不能**严格排除两次快照之间的短暂完全退出及再进入**；上交所逐日历史简称或可证明穷尽的状态变更链尚未取得。因此 **41 行均未计入已证 37 行**，全 L1 已证下界仍为 287 行。此为本轮最后的证据边界，不以无公告推断非 ST。

其他优先续审的具体证券/公告包括：600455 的 `57870606`（2010-04-26 实施提示）、`62279073`（2013-03-27 其他风险警示撤销）；300163 的 `1219924604`（2024-04-30 实施）、`1223720778`（2025-05-30 撤销）；600137 的 2002–2008 年期初状态链 `616302`、`10691907`、`16502279`、`33699715`、`40404058`；600768 的早期退出 `10383103` 和 2015 风险提示 `1200595684`。它们均在逐件表中附官方 URL 与 SHA，不据标题直接作状态判断。两份 PDF 的文本抽取少于 50 字（600241 `1216362534`、600506 `1212307371`），若用于结论须 OCR/人工核验；本批 37 行的六份关键 PDF 不依赖这两份。

如继续 L1，先对这 222 份线索中的期初和与旧目标重叠部分逐件核正文、简称与生效日，并补公告标题检索难以穷尽的历史简称来源，再对剩余旧目标股及新增退市候选做同样闭合；只有完成全候选 PIT ST 状态及其他 L1 缺口，才能重建正式链路。本批到此停止，等待本层确认。
