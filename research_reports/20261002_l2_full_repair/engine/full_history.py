"""Extend the frozen 2010 paper account until the first unproved settlement.

This is a diagnostic continuation of the older isolated candidate.  It does
not consume a NAV or infer cash from proxy returns.  Order quantities remain
frozen at the prior close; execution prices only accept or reject them.
"""

from __future__ import annotations

import csv
import hashlib
import json
import sys
from datetime import date as Date, timedelta
from decimal import Decimal as D, ROUND_HALF_UP, getcontext
from pathlib import Path

import baostock as bs


getcontext().prec = 40
ROOT = Path(__file__).resolve().parents[3]
HERE = Path(__file__).resolve().parent
OLD = ROOT / "research_reports/20260926_l2_repair/fixed_share_engine"
sys.path.insert(0, str(OLD))
from ledger_contract import freeze_orders, settle_next_close  # noqa: E402
from recent_slice import event_contract  # noqa: E402

PRICES = ROOT / ".microcap_index_cache/prices_raw"
MEMBERS = ROOT / "outputs/microcap_top100_mom16_biweekly_live_v2_0_base_proxy_members.csv"
CALENDAR = ROOT / "outputs/wind_microcap_top_100_biweekly_thursday_16y_cached.csv"
TRADE_CALENDAR = ROOT / "research_reports/20261002_l2_full_repair/data/trade_calendar_evidence/baostock_trade_calendar_20100222_20260930.csv"
TRADE_CALENDAR_MANIFEST = ROOT / "research_reports/20261002_l2_full_repair/data/trade_calendar_evidence/manifest.json"
TRADE_CALENDAR_EVIDENCE = ROOT / "research_reports/20261002_l2_full_repair/data/trade_calendar_evidence/EVIDENCE.md"
TRADE_CALENDAR_FETCHER = ROOT / "research_reports/20261002_l2_full_repair/data/trade_calendar_evidence/fetch_baostock_calendar.py"
RIGHTS = ROOT / "research_reports/20260926_l1_repair/corporate_actions/cninfo_distribution_events.csv"
CNINFO_MANIFEST = ROOT / "research_reports/20260926_l1_repair/corporate_actions/cninfo_source_manifest.json"
EVENT_CANDIDATES = ROOT / "research_reports/20261002_l2_full_repair/data/event_candidates_2010_20260930.csv"
PRECISION_AUDIT = ROOT / "research_reports/20261002_l2_full_repair/data/event_conflict_classification.csv"
ISSUER_EVIDENCE = ROOT / "research_reports/20261002_l2_full_repair/data/suspension_ex_evidence"
LEDGER = ROOT / "research_reports/20260926_l1_repair/corporate_actions/rights_event_ledger_2010_2014.csv"
RESTRUCTURING_EVIDENCE = ROOT / "research_reports/20260926_l1_repair/st_timing/600633_full_chain/600633_EVENT_CHAIN.md"
FIRST = OLD / "continued_replay.json"
EXISTING_BARS = [
    ROOT / "research_reports/20260926_l2_repair/clock/baostock_raw_open_windows.csv",
    OLD / "extra_bars_20100212.csv",
]
CACHED_BARS = HERE / "queried_baostock_bars.csv"
COVERAGE = HERE / "queried_baostock_coverage.json"
FIELDS = "date,code,close,preclose,volume,tradestatus,isST"

# These are issuer-proved events whose ex-date falls inside a verified
# suspension.  BaoStock repeats the last traded price while suspended; that
# vendor placeholder must not be booked alongside a new cash/share right.
# The mark here values existing holdings only and is never an executable OHLC.
SUSPENDED_EX_MARKS = {
    "600211": {
        "ex_date": "2010-09-07", "resume_date": "2010-09-08",
        "last_trade_date": "2010-09-06", "raw_anchor": D("15.36"),
        "theoretical_mark": D("14.63"), "cash_per_share": D("0"),
        "bonus_per_share": D("0.1"), "halt_placeholder": D("15.36"),
        # Only unrestricted shares received 1:10 in this stock-reform
        # follow-up.  The exchange reference reflects total dilution, not
        # the recipient's 10% entitlement: 15.36 * 13871 / 14558.9 = 14.63.
        "total_shares_before_10k": D("13871"),
        "total_shares_after_10k": D("14558.9"),
    },
    "000810": {
        "ex_date": "2011-06-17", "resume_date": "2012-02-10",
        "last_trade_date": "2011-05-12", "raw_anchor": D("14.00"),
        "theoretical_mark": D("13.80"), "cash_per_share": D("0.20"),
        "bonus_per_share": D("0"), "halt_placeholder": D("14.00"),
    },
}

# Each entry below is anchored to a dated issuer implementation notice kept in
# data/suspension_ex_evidence.  This is intentionally not a global tolerance.
ISSUER_CHECKED_PRECISION_EVENTS = {
    ("002034", "2013-06-27"): (D("0"), D("2.859022")),
    ("300062", "2013-07-17"): (D("0"), D("1.002524")),
    ("300120", "2014-06-11"): (D("2.003731"), D("1.001865")),
}

# This restructuring created shares for creditors and investors, not for
# existing shareholders.  The normalized vendor row resembles an ordinary
# 10-for-10 bonus; never turn it into an account entitlement by supplying a
# missing credit date.
NON_SHAREHOLDER_REORG = {
    ("600423", "2018-12-18"): {
        "announcement_date": "2018-12-12", "record_date": "2018-12-17",
        "bonus_per_10": D("10"), "issuer_file": "600423_20181212_reorganization.pdf",
        "issuer_sha256": "7a3f03ea4d7ac18c66d36598efbbb16f6251a4dbfc60ecd39a53bc8bc6412604",
        "cninfo_original_row_sha256": "f754305bf46225fa262ef6c8505a431108e3732452ed0b701b216b83e9ded08f",
    },
}

# The candidate omits the actual depository credit date.  The dated issuer
# notice proves 7/13 as the first listing session for unrestricted new shares.
# Keep them as a valued pending right after 7/10 ex, then release them to the
# paper account's tradeable position.  This is not a claim about broker credit.
ISSUER_CHECKED_CREDIT_EVENTS = {
    ("603330", "2018-06-26"): {
        "announcement_date": "2018-06-19", "record_date": "2018-06-25",
        "cash_pay_date": "2018-06-26", "paper_release_date": "2018-06-27",
        "bonus_per_10": D("3"), "cash_per_10_gross": D("2.58"),
        "issuer_file": "603330_20180619_issuer_mixed.pdf",
        "issuer_sha256": "2b22bfc7bc798a0d8d051c2a9773f422d34fab694d1306288c71f52ce836eb5a",
        "cninfo_original_row_sha256": "0a37a40ff61e8440bad0bc331eb20093b6ae63bc8d5f54102952ff4180db0e72",
    },
    ("603088", "2018-06-22"): {
        "announcement_date": "2018-06-15", "record_date": "2018-06-21",
        "cash_pay_date": "2018-06-22", "paper_release_date": "2018-06-25",
        "bonus_per_10": D("4"), "cash_per_10_gross": D("1.5"),
        "issuer_file": "603088_20180615_issuer_mixed.pdf",
        "issuer_sha256": "480b6af1ad3d43552d906f6f7cbcea9c1a4b2e9a5314adeaaa08a43ba24468a7",
        "cninfo_original_row_sha256": "5c027fa9c4356435da12b4202563a8b8081f63254c3ea31929ea609b078f0ea7",
    },
    ("603031", "2018-06-07"): {
        "announcement_date": "2018-06-01", "record_date": "2018-06-06",
        "cash_pay_date": "2018-06-07", "paper_release_date": "2018-06-08",
        "bonus_per_10": D("4"), "cash_per_10_gross": D("1.5"),
        "issuer_file": "603031_20180601_issuer_mixed.pdf",
        "issuer_sha256": "0705aaf7dc9595ef0ea1bb178bad26916caae0e1eb32f3b33508c787b18150d4",
        "cninfo_original_row_sha256": "8fd4e986607b9bd4a34da2929d4b190be7480c81dd492fdc9608b17899c5d294",
    },
    ("600513", "2017-05-19"): {
        "announcement_date": "2017-05-12", "record_date": "2017-05-18",
        "cash_pay_date": "2017-05-19", "paper_release_date": "2017-05-22",
        "bonus_per_10": D("3"), "cash_per_10_gross": D("0.86"),
        "issuer_file": "600513_20170512_issuer_mixed.pdf",
        "issuer_sha256": "5ed5f2220804d8d71ac39061e9e73f3f4680c25b6c998122c018a777201d9338",
    },
    ("600513", "2016-07-06"): {
        "announcement_date": "2016-06-30", "record_date": "2016-07-05",
        "cash_pay_date": "2016-07-06", "paper_release_date": "2016-07-07",
        "bonus_per_10": D("3"), "cash_per_10_gross": D("0.6"),
        "issuer_file": "600513_20160630_issuer_mixed.pdf",
        "issuer_sha256": "afe4005e2a3182d819343fb0b990504335fa6126458b5bc0561ab67d4850464a",
    },
    ("600719", "2015-11-30"): {
        "announcement_date": "2015-11-24", "record_date": "2015-11-27",
        "cash_pay_date": "", "paper_release_date": "2015-12-01",
        "bonus_per_10": D("10"), "cash_per_10_gross": D("0"),
        "issuer_file": "600719_20151124_issuer_bonus.pdf",
        "issuer_sha256": "91c32510cbabeb5c1a599c4b900d6c817da1b1503a83f1e40b2e786f7ee75248",
    },
    ("600985", "2015-07-10"): {
        "announcement_date": "2015-07-03", "record_date": "2015-07-09",
        "cash_pay_date": "2015-07-10", "paper_release_date": "2015-07-13",
        "bonus_per_10": D("5"), "cash_per_10_gross": D("1.7"),
        "issuer_file": "600985_20150703_issuer_mixed.pdf",
        "issuer_sha256": "8dd86ad3ca9344131809e1a38c1e0dd181b9426c0448a8de4d50f08980946f7c",
    },
}

ISSUER_CHECKED_SINGLE_SOURCE_CASH_EVENTS = {
    ("600796", "2018-06-27"): {
        "announcement_date": "2018-06-21", "record_date": "2018-06-26",
        "cash_pay_date": "2018-06-27", "cash_per_10_gross": D("0.3"),
        "issuer_file": "600796_20180621_issuer_dividend.pdf",
        "issuer_sha256": "c0e8e52e11528f70650b43d1a48ff4078394841d2ed9ba0b5ee487d44e2ae63c",
        "cninfo_original_row_sha256": "4557e2fb859a2f5b8c749603df2d4340a9ff78faff5c9d9a403913e72a31ac87",
    },
    ("002778", "2018-06-15"): {
        "announcement_date": "2018-06-07", "record_date": "2018-06-14",
        "cash_pay_date": "2018-06-15", "cash_per_10_gross": D("0.7"),
        "issuer_file": "002778_20180607_issuer_dividend.pdf",
        "issuer_sha256": "eb75b5d9518345cfd50222cdabe7a33b5482cac198f0cf3a18501916de8a8388",
    },
    ("603033", "2018-05-21"): {
        "announcement_date": "2018-05-11", "record_date": "2018-05-18",
        "cash_pay_date": "2018-05-21", "cash_per_10_gross": D("1.5"),
        "issuer_file": "603033_20180511_issuer_dividend.pdf",
        "issuer_sha256": "36cb0ce8869ac318b36a1615561eaca1982a30dbd55946f67e0f7bb498b0f4f8",
    },
    ("300478", "2017-06-07"): {
        "announcement_date": "2017-06-01", "record_date": "2017-06-06",
        "cash_pay_date": "2017-06-07", "cash_per_10_gross": D("1.5"),
        "issuer_file": "300478_20170601_issuer_dividend.pdf",
        "issuer_sha256": "6ce5cfc87620e740b380f09b2ccaccdd3ca6b4b8ab3a90e4229d382a861dcc5b",
    },
    ("300423", "2017-05-26"): {
        "announcement_date": "2017-05-19", "record_date": "2017-05-25",
        "cash_pay_date": "2017-05-26", "cash_per_10_gross": D("0.5"),
        "issuer_file": "300423_20170519_issuer_dividend.pdf",
        "issuer_sha256": "aad834fa11a91d266fc32c0423546f648ed5548df15ff43c4488cee0ca8f330e",
    },
    ("600189", "2016-07-18"): {
        "announcement_date": "2016-07-08", "record_date": "2016-07-15",
        "cash_pay_date": "2016-07-18", "cash_per_10_gross": D("0.5"),
        "issuer_file": "600189_20160708_issuer_dividend.pdf",
        "issuer_sha256": "87429d77b24a9652d8c2d689396dde11852c90f6d9689647021265d6f5b80143",
    },
    ("300461", "2016-06-16"): {
        "announcement_date": "2016-06-07", "record_date": "2016-06-15",
        "cash_pay_date": "2016-06-16", "cash_per_10_gross": D("1.5"),
        "issuer_file": "300461_20160607_issuer_dividend.pdf",
        "issuer_sha256": "2b71e38e0ff4adabac6e66ba07876e7cd872d70ffafe8786ee8758fb0ffa0830",
    },
    ("300046", "2016-06-15"): {
        "announcement_date": "2016-06-06", "record_date": "2016-06-14",
        "cash_pay_date": "2016-06-15", "cash_per_10_gross": D("2.5"),
        "issuer_file": "300046_20160606_issuer_dividend.pdf",
        "issuer_sha256": "0104460007b40cb68b31983a7cf580600cc0fb5f39e8d96b4c91e91a3a6ffb51",
    },
    ("002735", "2016-06-03"): {
        "announcement_date": "2016-05-27", "record_date": "2016-06-02",
        "cash_pay_date": "2016-06-03", "cash_per_10_gross": D("0.82"),
        "issuer_file": "002735_20160527_issuer_dividend.pdf",
        "issuer_sha256": "eeed3d9fecd6dd8ee69d8ad462d92fac76dd557880ceea5176a13fd8cfec951a",
    },
    ("300459", "2015-07-14"): {
        "announcement_date": "2015-07-07", "record_date": "2015-07-13",
        "cash_pay_date": "2015-07-14", "cash_per_10_gross": D("1.0"),
        "issuer_file": "300459_20150707_issuer_dividend.pdf",
        "issuer_sha256": "26d6cac21df0023a3600a36a9f51cb272e12f0d90192764821122b750006250f",
    },
    ("002088", "2015-07-09"): {
        "announcement_date": "2015-07-03", "record_date": "2015-07-08",
        "cash_pay_date": "2015-07-09", "cash_per_10_gross": D("1.0"),
        "issuer_file": "002088_20150703_issuer_dividend.pdf",
        "issuer_sha256": "4b9966d80a62b5e18bebf3e1c5f7b523d3e1a95c2019c2e3d82e7a55970917ce",
    },
    ("600778", "2015-07-01"): {
        "announcement_date": "2015-06-24", "record_date": "2015-06-30",
        "cash_pay_date": "2015-07-01", "cash_per_10_gross": D("0.9"),
        "issuer_file": "600778_20150624_issuer_dividend.pdf",
        "issuer_sha256": "4665a8fae757f04fce8e4bd11713d6b6b485bf7737c1aa60efc43aad0189bb81",
    },
    ("300141", "2015-05-07"): {
        "announcement_date": "2015-04-29", "record_date": "2015-05-06",
        "cash_pay_date": "2015-05-07", "cash_per_10_gross": D("1.0"),
        "issuer_file": "300141_20150429_issuer_dividend.pdf",
        "issuer_sha256": "319387b47ec4ba41ee70ad868fc783190f9b0352b5d26f042ec8db2051b50533",
    },
    ("300420", "2015-05-19"): {
        "announcement_date": "2015-05-12", "record_date": "2015-05-18",
        "cash_pay_date": "2015-05-19", "cash_per_10_gross": D("1.2"),
        "issuer_file": "300420_20150512_issuer_dividend.pdf",
        "issuer_sha256": "8bd77e924c04cea16d9ac2cb85e36719cd0b455e5fc67e4930e607b80e22080a",
    },
    ("300421", "2015-05-28"): {
        "announcement_date": "2015-05-21", "record_date": "2015-05-27",
        "cash_pay_date": "2015-05-28", "cash_per_10_gross": D("2.2"),
        "issuer_file": "300421_20150521_issuer_dividend.pdf",
        "issuer_sha256": "2905f38ea61be6a73047812798c73b6575f8686250d9366eb42f80d2bfbca600",
    },
}

ISSUER_CHECKED_SINGLE_SOURCE_MIXED_EVENTS = {
    ("300423", "2016-05-20"): {
        "announcement_date": "2016-05-14", "record_date": "2016-05-19",
        "cash_pay_date": "2016-05-20", "share_credit_date": "2016-05-20",
        "bonus_per_10": D("2"), "cash_per_10_gross": D("1"),
        "issuer_file": "300423_20160514_issuer_mixed.pdf",
        "issuer_sha256": "6aaca9ae25b55c8153cf299bea517ee1f30745445f2dad6fc0b30564e8950188",
    },
}


def rows(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def verified_calendar() -> list[str]:
    """Remove the one independently proven proxy holiday, without altering inputs."""
    manifest = json.loads(TRADE_CALENDAR_MANIFEST.read_text(encoding="utf-8"))
    exchange_rows = rows(TRADE_CALENDAR)
    if (manifest.get("source") != "BaoStock anonymous query_trade_dates; exchange calendar"
            or manifest.get("date_range") != ["2010-02-22", "2026-09-30"]
            or manifest.get("sha256") != digest(TRADE_CALENDAR)
            or manifest.get("rows") != len(exchange_rows)
            or manifest.get("trading_rows") != sum(r["is_trading_day"] == "1" for r in exchange_rows)):
        raise ValueError("trading_calendar_evidence_mismatch")
    exchange = {r["calendar_date"]: r["is_trading_day"] for r in exchange_rows}
    if len(exchange) != len(exchange_rows):
        raise ValueError("trading_calendar_duplicate")
    proxy_days = [r["date"] for r in rows(CALENDAR)]
    if proxy_days != sorted(set(proxy_days)):
        raise ValueError("proxy_calendar_not_ordered_unique")
    in_scope = {d for d in proxy_days if d >= "2010-02-22"}
    missing = {d for d, is_trade in exchange.items() if is_trade == "1"} - in_scope
    excluded = [d for d in proxy_days if d >= "2010-02-22" and exchange.get(d) == "0"]
    unknown = [d for d in proxy_days if d >= "2010-02-22" and d not in exchange]
    if missing or unknown or excluded != ["2018-06-18"]:
        raise ValueError(f"proxy_exchange_calendar_mismatch:{sorted(missing)[:3]}:{unknown[:3]}:{excluded[:3]}")
    for r in rows(EVENT_CANDIDATES):
        if any(r.get(k) in excluded for k in
               ("record_date", "ex_date", "cash_pay_date", "share_credit_date", "payment_start", "payment_end")):
            raise ValueError("corporate_event_on_excluded_holiday")
    if any(r["rebalance_date"] in excluded for r in rows(MEMBERS)):
        raise ValueError("rebalance_on_excluded_holiday")
    return [d for d in proxy_days if d not in excluded]


def pos(v: str | None) -> bool:
    if not v:
        return False
    try:
        return D(v) > 0
    except Exception:
        return False


def verified_event_contract(r: dict, shares: D,
                            precision_audit: dict[tuple[str, str], dict],
                            precision_used: set[tuple[str, str]],
                            credit_used: set[tuple[str, str]],
                            credit_original_sha: dict[tuple[str, str], str]) -> dict:
    """Apply exact issuer exceptions and reject a non-shareholder reorganization.

    A genuine same-source duplicate or different distribution remains blocked.
    The CNInfo row is the dated implementation amount, while the independent
    Sina row may truncate a few final decimal places.  All fields must match
    the frozen classification evidence before overriding the conflict flag.
    """
    key = (r["symbol"].zfill(6), r["ex_date"])
    reorg = NON_SHAREHOLDER_REORG.get(key)
    if reorg:
        original_rows = [o for o in rows(RIGHTS)
                         if (o["symbol"].zfill(6), o["除权日"]) == key]
        issuer_file = ISSUER_EVIDENCE / reorg["issuer_file"]
        if (not issuer_file.is_file()
                or digest(issuer_file).lower() != reorg["issuer_sha256"]
                or len(original_rows) != 1
                or hashlib.sha256(json.dumps(original_rows[0], ensure_ascii=False,
                                             sort_keys=True).encode("utf-8")).hexdigest()
                    != reorg["cninfo_original_row_sha256"]
                or r["announcement_date"] != reorg["announcement_date"]
                or r["record_date"] != reorg["record_date"]
                or D(r["bonus_per_10"] or "0") != reorg["bonus_per_10"]):
            raise ValueError(f"non_shareholder_reorg_proof_mismatch:{r['record_date']}:{key[0]}")
        raise ValueError(f"non_shareholder_reorg_not_entitlement:{r['record_date']}:{key[0]}")
    credit_proof = ISSUER_CHECKED_CREDIT_EVENTS.get(key)
    if credit_proof:
        issuer_file = ISSUER_EVIDENCE / credit_proof["issuer_file"]
        expected_source = (("single_source_only", "cninfo=1;sina=0")
                           if key in {("603031", "2018-06-07"),
                                      ("603088", "2018-06-22"),
                                      ("603330", "2018-06-26")} else
                           ("two_source_match", "cninfo=1;sina=1"))
        if (not issuer_file.is_file()
                or digest(issuer_file).lower() != credit_proof["issuer_sha256"]
                or r["source"] != "cninfo"
                or (r["source_status"], r["source_rows"]) != expected_source
                or r["share_credit_date"]
                or any(r[field] != credit_proof[field] for field in
                       ("announcement_date", "record_date", "cash_pay_date"))
                or D(r["bonus_per_10"] or "0") != credit_proof["bonus_per_10"]
                or D(r["cash_per_10_gross"] or "0") != credit_proof["cash_per_10_gross"]):
            raise ValueError(f"issuer_credit_proof_mismatch:{key[1]}:{key[0]}")
        if r["source_status"] == "single_source_only":
            originals = [o for o in rows(RIGHTS)
                         if (o["symbol"].zfill(6), o["除权日"]) == key]
            if len(originals) != 1:
                raise ValueError(f"issuer_credit_original_not_unique:{key[1]}:{key[0]}")
            original = originals[0]
            if (original["实施方案公告日期"] != r["announcement_date"]
                    or original["股权登记日"] != r["record_date"]
                    or original["除权日"] != r["ex_date"]
                    or original["派息日"] != r["cash_pay_date"]
                    or original["股份到账日"]
                    or D(original["派息比例"] or "0") != credit_proof["cash_per_10_gross"]
                    or D(original["送股比例"] or "0") + D(original["转增比例"] or "0") != credit_proof["bonus_per_10"]):
                raise ValueError(f"issuer_credit_original_row_mismatch:{key[1]}:{key[0]}")
            row_sha = hashlib.sha256(
                json.dumps(original, ensure_ascii=False, sort_keys=True).encode("utf-8")
            ).hexdigest()
            if row_sha != credit_proof["cninfo_original_row_sha256"]:
                raise ValueError(f"issuer_credit_original_hash_mismatch:{key[1]}:{key[0]}")
            credit_original_sha[key] = row_sha
        # event_contract uses share_credit_date as the paper account release
        # gate.  Actual CSDC/broker posting time is not established here.
        clean = dict(r, share_credit_date=credit_proof["paper_release_date"])
        result = event_contract(clean, shares)
        credit_used.add(key)
        return result
    if r["source_status"] != "source_conflict_or_duplicate":
        return event_contract(r, shares)
    audit = precision_audit.get(key)
    # Only issuer-notice-checked events are accepted.  Other precision-labeled
    # rows still require their own issuer check before use.
    expected = ISSUER_CHECKED_PRECISION_EVENTS.get(key)
    if (expected is None or not audit
            or audit["kind"] != "ratio_precision_difference_le_0p000031"
            or D(r["bonus_per_10"] or "0") != expected[0]
            or D(r["cash_per_10_gross"] or "0") != expected[1]):
        raise ValueError(f"unresolved_distribution_conflict:{key[1]}:{key[0]}")
    cninfo = json.loads(audit["cninfo_rows"])
    sina = json.loads(audit["sina_rows"])
    if len(cninfo) != 1 or len(sina) != 1 or r["source"] != "cninfo":
        raise ValueError(f"precision_evidence_not_unique:{key[1]}:{key[0]}")
    c, s = cninfo[0], sina[0]
    if (c["record"] != s["record"] or c["record"] != r["record_date"]
            or c["announcement"] != r["announcement_date"]
            or s["announcement"] != r["announcement_date"]
            or abs(D(str(c["bonus"])) - D(str(s["bonus"]))) > D("0.000031")
            or D(str(c["bonus"])) != D(r["bonus_per_10"] or "0")
            or D(str(c["cash"])) != D(r["cash_per_10_gross"] or "0")
            or abs(D(str(c["cash"])) - D(str(s["cash"]))) > D("0.000031")):
        raise ValueError(f"precision_evidence_terms_mismatch:{key[1]}:{key[0]}")
    clean = dict(r, source_status="single_source_only")
    proof = event_contract(clean, shares)
    proof["source_status"] = "cninfo_high_precision_after_two_source_audit"
    precision_used.add(key)
    return proof


def require_ex_raw(source, item: dict, date: str) -> None:
    """A stale pre-ex raw mark plus a new right would double-count value."""
    special = SUSPENDED_EX_MARKS.get(item["symbol"])
    if special and date == special["ex_date"]:
        shares = item["record_shares"]
        if (shares <= 0 or item["cash_due"] != shares * special["cash_per_share"]
                or item["bonus_due"] != shares * special["bonus_per_share"]):
            raise ValueError(f"suspension_ex_right_terms_mismatch:{date}:{item['symbol']}")
    cash_event = source.cash_halt_candidates.get((item["symbol"], date))
    if cash_event and date == item["ex"]:
        shares = item["record_shares"]
        if (shares <= 0 or item["source_status"] != cash_event["source_status"]
                or item["cash_due"] != shares * D(cash_event["cash_per_10_gross"]) / D(10)
                or item["bonus_due"] != 0):
            raise ValueError(f"suspension_cash_right_terms_mismatch:{date}:{item['symbol']}")
    bonus_event = source.bonus_halt_candidates.get((item["symbol"], date))
    if bonus_event and date == item["ex"]:
        shares = item["record_shares"]
        if (shares <= 0 or item["source_status"] != "two_source_match"
                or item["bonus_due"] != shares * D(bonus_event["bonus_per_10"]) / D(10)
                or item["cash_due"] != 0
                or item["stock_credited"] != date):
            raise ValueError(f"suspension_bonus_right_terms_mismatch:{date}:{item['symbol']}")
    mixed_event = source.mixed_halt_candidates.get((item["symbol"], date))
    if mixed_event and date == item["ex"]:
        shares = item["record_shares"]
        if (shares <= 0 or item["source_status"] != mixed_event["source_status"]
                or item["bonus_due"] != shares * D(mixed_event["bonus_per_10"]) / D(10)
                or item["cash_due"] != shares * D(mixed_event["cash_per_10_gross"]) / D(10)
                or item["cash_paid"] != date or item["stock_credited"] != date):
            raise ValueError(f"suspension_mixed_right_terms_mismatch:{date}:{item['symbol']}")
    if date in source.price_map(item["symbol"]):
        return
    if hasattr(source, "mark"):
        _, basis = source.mark(item["symbol"],date)
        if basis in {"true_raw_close", "BaoStock_unadjusted_real_trade_supplement",
                     "issuer_verified_suspension_ex_theoretical_mark",
                     "verified_suspension_cash_theoretical_mark",
                     "two_source_verified_suspension_bonus_theoretical_mark",
                     "verified_suspension_mixed_theoretical_mark"}:
            return
    if date not in source.price_map(item["symbol"]):
        raise ValueError(f"unpriced_ex_action_no_raw_close:{date}:{item['symbol']}")


def side_limit_pct(source, symbol: str, date: str, is_st: str) -> D | None:
    """Only the dated exchange regimes we can identify from local symbol/date.

    None means the STAR first-five-session no-limit regime.  Unknown boards
    fail closed.  `isST` itself remains the frozen vendor diagnostic field.
    Official rules: SZSE 2020-08-24 ChiNext 20%; SSE STAR first 5 sessions
    unlimited then 20%; SSE/SZSE 2026-07-06 main-board ST 10%.
    """
    if is_st not in {"0", "1"}:
        raise ValueError(f"unknown_ST_status:{date}:{symbol}:{is_st}")
    if symbol == "600633" and date == "2011-09-29":
        # Exchange-announced first session after re-listing, N 浙传媒.
        return None
    pmap = source.price_map(symbol)
    first_raw = min(pmap)
    if first_raw >= "2019-01-01":
        listing = source.list_date(symbol)
        ordinal = sum(d <= date for d in pmap)
        if ordinal <= 5 and first_raw != listing:
            raise ValueError(f"first_five_listing_date_mismatch:{date}:{symbol}:{first_raw}:{listing}")
        if ordinal <= 5 and (symbol.startswith(("688","689"))
                or (symbol.startswith(("300","301")) and first_raw >= "2020-08-24")
                or (symbol.startswith(("0","6")) and first_raw >= "2023-04-10")):
            return None
    if symbol.startswith(("688", "689")):
        return D("0.20")
    if symbol.startswith(("300", "301")) and date >= "2020-08-24":
        return D("0.20")
    if symbol.startswith(("0", "3", "6")):
        if is_st == "1":
            return D("0.10") if date >= "2026-07-06" and not symbol.startswith(("300", "301")) else D("0.05")
        return D("0.10")
    raise ValueError(f"unsupported_limit_board:{date}:{symbol}")


def check_paper_trade(source, symbol: str, date: str, side: str, price: D) -> str | None:
    """Return a rejection reason; query public status only on a material edge.

    A local raw-close row is evidence of a traded daily close.  For a close
    that might be on the blocked side limit, confirm status, ST and preclose
    from BaoStock.  Missing raw requires an explicit suspension bar.
    """
    pmap = source.price_map(symbol)
    if symbol == "600633" and "2010-04-30" <= date < "2011-09-29":
        return "issuer_documented_suspension"
    if date not in pmap:
        bar = source.bar(symbol, date)
        if bar and bar["tradestatus"] == "0" and bar["volume"] in {"", "0", "0.0"}:
            return "suspended_or_zero_volume"
        raise ValueError(f"missing_raw_on_trade_date:{date}:{symbol}")
    earlier = [d for d in pmap if d < date]
    if not earlier:
        raise ValueError(f"missing_previous_true_close_for_limit:{date}:{symbol}")
    prev_date = max(earlier)
    prev = pmap[prev_date]
    sign = D(1) if side == "buy" else D(-1)
    possible = [side_limit_pct(source, symbol, date, st) for st in ("0", "1")]
    prior_market = source.previous_market_day.get(date)
    must_confirm_reference = (prev_date != prior_market or (symbol,date) in source.action_exdates)
    at_raw_estimated_limit = any(pct is not None and price == (prev * (1 + sign*pct)).quantize(D("0.01"), rounding=ROUND_HALF_UP)
               for pct in possible)
    if not must_confirm_reference and not at_raw_estimated_limit:
        return None
    bar = source.bar(symbol, date)
    if not bar:
        raise ValueError(f"missing_limit_status:{date}:{symbol}")
    if bar["tradestatus"] != "1" or not bar["volume"] or D(bar["volume"]) <= 0:
        return "suspended_or_zero_volume"
    if price != D(bar["close"]):
        raise ValueError(f"raw_baostock_close_mismatch:{date}:{symbol}")
    pct = side_limit_pct(source, symbol, date, bar["isST"])
    if pct is None:
        return None
    if not bar["preclose"]:
        raise ValueError(f"missing_exchange_limit_reference:{date}:{symbol}")
    limit = (D(bar["preclose"]) * (1 + sign*pct)).quantize(D("0.01"), rounding=ROUND_HALF_UP)
    return "at_side_close_limit_diagnostic_ST_and_board" if price == limit else None


class Source:
    def __init__(self, namespace: str = "recent"):
        if namespace not in {"recent", "full"}:
            raise ValueError(f"invalid_source_namespace:{namespace}")
        self.bars_path = CACHED_BARS if namespace == "recent" else HERE / "full_queried_baostock_bars.csv"
        self.coverage_path = COVERAGE if namespace == "recent" else HERE / "full_queried_baostock_coverage.json"
        self.supplement_path = HERE / ("raw_close_supplements.csv" if namespace == "recent" else "full_raw_close_supplements.csv")
        self.raw: dict[str, dict[str, D]] = {}
        self.raw_paths: set[Path] = set()
        self.raw_supplements: dict[tuple[str,str], dict[str,str]] = {}
        self.bars: dict[tuple[str, str], dict[str, str]] = {}
        self.coverage: dict[str, list[str]] = {}
        self.queries: list[dict[str, str | int]] = []
        days = verified_calendar()
        self.previous_market_day = {days[i]: days[i-1] for i in range(1,len(days))}
        event_rows = rows(EVENT_CANDIDATES)
        self.action_exdates = {(r["symbol"].zfill(6),r["ex_date"]) for r in event_rows if r["ex_date"]}
        self.events_by_symbol: dict[str, list[dict[str, str]]] = {}
        self.cash_halt_candidates: dict[tuple[str, str], dict[str, str]] = {}
        self.cash_halt_used: dict[tuple[str, str], dict[str, str]] = {}
        self.single_cash_original_sha: dict[tuple[str, str], str] = {}
        self.bonus_halt_candidates: dict[tuple[str, str], dict[str, str]] = {}
        self.bonus_halt_used: dict[tuple[str, str], dict[str, str]] = {}
        self.mixed_halt_candidates: dict[tuple[str, str], dict[str, str]] = {}
        self.mixed_halt_used: dict[tuple[str, str], dict[str, str]] = {}
        self.single_mixed_original_sha: dict[tuple[str, str], str] = {}
        cninfo_manifest = json.loads(CNINFO_MANIFEST.read_text(encoding="utf-8"))
        if (cninfo_manifest.get("source") != "CNInfo webapi via akshare.stock_dividend_cninfo; full per-symbol history"
                or cninfo_manifest.get("events_sha256", "").lower() != digest(RIGHTS)):
            raise ValueError("cninfo_original_manifest_hash_mismatch")
        cninfo_ok = {x["symbol"] for x in cninfo_manifest["per_symbol"] if x["status"] == "ok"}
        cninfo_original: dict[tuple[str, str], list[dict[str, str]]] = {}
        for original in rows(RIGHTS):
            cninfo_original.setdefault((original["symbol"].zfill(6), original["除权日"]), []).append(original)
        for r in event_rows:
            symbol = r["symbol"].zfill(6)
            self.events_by_symbol.setdefault(symbol, []).append(r)
            key = (symbol, r["ex_date"])
            issuer_proof = ISSUER_CHECKED_SINGLE_SOURCE_CASH_EVENTS.get(key)
            if issuer_proof:
                issuer_file = ISSUER_EVIDENCE / issuer_proof["issuer_file"]
                if (not issuer_file.is_file()
                        or digest(issuer_file).lower() != issuer_proof["issuer_sha256"]
                        or any(r[field] != issuer_proof[field] for field in
                               ("announcement_date", "record_date", "cash_pay_date"))
                        or D(r["cash_per_10_gross"] or "0") != issuer_proof["cash_per_10_gross"]
                        or r["source_status"] != "single_source_only"
                        or r["source_rows"] != "cninfo=1;sina=0"):
                    raise ValueError(f"single_source_issuer_proof_mismatch:{symbol}:{r['ex_date']}")
            single_official = False
            if (r["event_type"] == "dividend_or_bonus"
                    and r["source"] == "cninfo"
                    and r["source_status"] == "single_source_only"
                    and r["source_rows"] == "cninfo=1;sina=0"
                    and symbol in cninfo_ok):
                originals = cninfo_original.get(key, [])
                if len(originals) == 1:
                    original = originals[0]
                    single_official = (
                        original["实施方案公告日期"] == r["announcement_date"]
                        and original["股权登记日"] == r["record_date"]
                        and original["除权日"] == r["ex_date"]
                        and original["派息日"] == r["cash_pay_date"]
                        and D(original["派息比例"] or "0") == D(r["cash_per_10_gross"] or "0")
                        and D(original["送股比例"] or "0") + D(original["转增比例"] or "0") == 0)
                    if single_official:
                        self.single_cash_original_sha[key] = hashlib.sha256(
                            json.dumps(original, ensure_ascii=False, sort_keys=True).encode("utf-8")
                        ).hexdigest()
                        if (issuer_proof and issuer_proof.get("cninfo_original_row_sha256")
                                and self.single_cash_original_sha[key]
                                != issuer_proof["cninfo_original_row_sha256"]):
                            raise ValueError(f"single_source_original_hash_mismatch:{symbol}:{r['ex_date']}")
            if (r["event_type"] == "dividend_or_bonus"
                    and r["source"] == "cninfo"
                    and ((r["source_status"] == "two_source_match"
                          and r["source_rows"] == "cninfo=1;sina=1")
                         or (issuer_proof and single_official))
                    and r["record_date"] < r["ex_date"]
                    and r["announcement_date"] <= r["record_date"]
                    and r["cash_pay_date"] >= r["ex_date"]
                    and D(r["cash_per_10_gross"] or "0") > 0
                    and D(r["bonus_per_10"] or "0") == 0
                    and not r["rights_per_10"]):
                key = (symbol, r["ex_date"])
                if key in self.cash_halt_candidates:
                    raise ValueError(f"duplicate_cash_halt_candidate:{symbol}:{r['ex_date']}")
                self.cash_halt_candidates[key] = r
            if (r["event_type"] == "dividend_or_bonus"
                    and r["source"] == "cninfo"
                    and r["source_status"] == "two_source_match"
                    and r["source_rows"] == "cninfo=1;sina=1"
                    and r["record_date"] < r["ex_date"]
                    and r["announcement_date"] <= r["record_date"]
                    and r["share_credit_date"] == r["ex_date"]
                    and D(r["bonus_per_10"] or "0") > 0
                    and D(r["cash_per_10_gross"] or "0") == 0
                    and not r["rights_per_10"]):
                key = (symbol, r["ex_date"])
                if key in self.bonus_halt_candidates:
                    raise ValueError(f"duplicate_bonus_halt_candidate:{symbol}:{r['ex_date']}")
                self.bonus_halt_candidates[key] = r
            mixed_issuer = ISSUER_CHECKED_SINGLE_SOURCE_MIXED_EVENTS.get(key)
            mixed_single_official = False
            if mixed_issuer:
                issuer_file = ISSUER_EVIDENCE / mixed_issuer["issuer_file"]
                originals = cninfo_original.get(key, [])
                if (not issuer_file.is_file()
                        or digest(issuer_file).lower() != mixed_issuer["issuer_sha256"]
                        or r["source"] != "cninfo"
                        or r["source_status"] != "single_source_only"
                        or r["source_rows"] != "cninfo=1;sina=0"
                        or symbol not in cninfo_ok
                        or len(originals) != 1
                        or any(r[field] != mixed_issuer[field] for field in
                               ("announcement_date", "record_date", "cash_pay_date", "share_credit_date"))
                        or D(r["bonus_per_10"] or "0") != mixed_issuer["bonus_per_10"]
                        or D(r["cash_per_10_gross"] or "0") != mixed_issuer["cash_per_10_gross"]):
                    raise ValueError(f"single_mixed_issuer_proof_mismatch:{symbol}:{r['ex_date']}")
                original = originals[0]
                mixed_single_official = (
                    original["实施方案公告日期"] == r["announcement_date"]
                    and original["股权登记日"] == r["record_date"]
                    and original["除权日"] == r["ex_date"]
                    and original["派息日"] == r["cash_pay_date"]
                    and original["股份到账日"] == r["share_credit_date"]
                    and D(original["派息比例"] or "0") == mixed_issuer["cash_per_10_gross"]
                    and D(original["送股比例"] or "0") + D(original["转增比例"] or "0") == mixed_issuer["bonus_per_10"])
                if not mixed_single_official:
                    raise ValueError(f"single_mixed_original_row_mismatch:{symbol}:{r['ex_date']}")
                self.single_mixed_original_sha[key] = hashlib.sha256(
                    json.dumps(original, ensure_ascii=False, sort_keys=True).encode("utf-8")
                ).hexdigest()
            if (r["event_type"] == "dividend_or_bonus"
                    and r["source"] == "cninfo"
                    and ((r["source_status"] == "two_source_match"
                          and r["source_rows"] == "cninfo=1;sina=1")
                         or (mixed_issuer and mixed_single_official))
                    and r["record_date"] < r["ex_date"]
                    and r["announcement_date"] <= r["record_date"]
                    and r["cash_pay_date"] == r["ex_date"]
                    and r["share_credit_date"] == r["ex_date"]
                    and D(r["bonus_per_10"] or "0") > 0
                    and D(r["cash_per_10_gross"] or "0") > 0
                    and not r["rights_per_10"]):
                if key in self.mixed_halt_candidates:
                    raise ValueError(f"duplicate_mixed_halt_candidate:{symbol}:{r['ex_date']}")
                self.mixed_halt_candidates[key] = r
        for path in EXISTING_BARS + ([self.bars_path] if self.bars_path.exists() else []):
            for r in rows(path):
                self.bars[(r["symbol"].zfill(6), r["date"])] = r
        if self.bars_path.exists():
            if self.coverage_path.exists():
                loaded = json.loads(self.coverage_path.read_text(encoding="utf-8"))
                self.coverage = {s: (v if isinstance(v,list) else ["2010-02-22",v]) for s,v in loaded.items()}
            else:
                # Pre-coverage cache was fetched over this exact interval.
                self.coverage = {r["symbol"].zfill(6): ["2010-02-22","2010-04-30"] for r in rows(self.bars_path)}

    def price_map(self, symbol: str) -> dict[str, D]:
        if symbol not in self.raw:
            path = PRICES / f"{symbol}.csv"
            if not path.is_file():
                raise ValueError(f"missing_raw_file:{symbol}")
            data = {}
            for r in rows(path):
                v = D(r["close_raw"])
                if v <= 0 or not v.is_finite() or r["date"] in data:
                    raise ValueError(f"bad_raw_file:{symbol}:{r['date']}")
                data[r["date"]] = v
            self.raw[symbol] = data
            self.raw_paths.add(path)
        return self.raw[symbol]

    def list_date(self, symbol: str) -> str:
        path = ROOT / ".microcap_index_cache/security_meta" / f"{symbol}.json"
        if not path.exists():
            raise ValueError(f"missing_listing_metadata:{symbol}")
        listed = json.loads(path.read_text(encoding="utf-8")).get("list_date")
        if not listed:
            raise ValueError(f"empty_listing_date:{symbol}")
        self.raw_paths.add(path)
        return listed

    def ensure_bars(self, symbol: str, date: str):
        interval = self.coverage.get(symbol)
        if (symbol, date) in self.bars or (interval and interval[0] <= date <= interval[1]):
            return
        code = ("sh." if symbol.startswith("6") else "sz.") + symbol
        if interval and date < interval[0]:
            start, end = Date.fromisoformat(date), Date.fromisoformat(interval[0])-timedelta(days=1)
        else:
            start = (Date.fromisoformat(interval[1]) + timedelta(days=1)) if interval else Date.fromisoformat(date)
            end = Date(2026,9,30)
        # The recent slice has at most one year left.  The early full-history
        # branch uses bounded 90-day windows so a single 2010 suspension does
        # not request sixteen years of bars before the next rights decision.
        if self.bars_path.name.startswith("full_") and not (interval and date < interval[0]):
            end = min(Date.fromisoformat(date)+timedelta(days=90),Date(2026,9,30))
        query = bs.query_history_k_data_plus(code, FIELDS,
            start_date=start.isoformat(), end_date=end.isoformat(), frequency="d", adjustflag="3")
        if query.error_code == "10001001":
            reauth = bs.login()
            if reauth.error_code != "0":
                raise ValueError(f"baostock_relogin_failed:{symbol}:{reauth.error_code}:{reauth.error_msg}")
            query = bs.query_history_k_data_plus(code, FIELDS,
                start_date=start.isoformat(), end_date=end.isoformat(), frequency="d", adjustflag="3")
        if query.error_code != "0":
            raise ValueError(f"baostock_query_failed:{symbol}:{query.error_code}:{query.error_msg}")
        count = 0
        while query.next():
            r = dict(zip(FIELDS.split(","), query.get_row_data()))
            r["symbol"] = symbol
            self.bars[(symbol, r["date"])] = r
            count += 1
        self.coverage[symbol] = [min(start.isoformat(),interval[0]) if interval else start.isoformat(),
            max(end.isoformat(),interval[1]) if interval else end.isoformat()]
        self.queries.append({"symbol": symbol, "rows": count, "code": code,
            "start_date": start.isoformat(), "end_date": end.isoformat()})

    def bar(self, symbol: str, date: str) -> dict[str, str] | None:
        self.ensure_bars(symbol, date)
        return self.bars.get((symbol, date))

    def mark(self, symbol: str, date: str, last: D | None = None) -> tuple[D, str]:
        pmap = self.price_map(symbol)
        actual = pmap.get(date)
        special = SUSPENDED_EX_MARKS.get(symbol)
        if actual is not None:
            if special and date == special["resume_date"]:
                bar = self.bar(symbol, date)
                if not (bar and bar["tradestatus"] == "1" and
                        D(bar["volume"] or "0") > 0 and
                        D(bar["close"]) == actual and
                        D(bar["preclose"]) == special["theoretical_mark"]):
                    raise ValueError(f"suspension_ex_resume_reference_mismatch:{date}:{symbol}")
            return actual, "true_raw_close"
        if special and special["ex_date"] <= date < special["resume_date"]:
            anchor = special["raw_anchor"]
            theoretical = special["theoretical_mark"]
            if pmap.get(special["last_trade_date"]) != anchor:
                raise ValueError(f"suspension_ex_raw_anchor_mismatch:{date}:{symbol}")
            if any(special["last_trade_date"] < d < special["resume_date"] for d in pmap):
                raise ValueError(f"suspension_ex_unexpected_true_trade:{date}:{symbol}")
            if last is not None and last not in {anchor, theoretical}:
                raise ValueError(f"suspension_ex_prior_mark_mismatch:{date}:{symbol}")
            if symbol == "600211":
                calc = (anchor * special["total_shares_before_10k"] /
                        special["total_shares_after_10k"]).quantize(
                            D("0.01"), rounding=ROUND_HALF_UP)
            else:
                calc = anchor - special["cash_per_share"]
            if calc != theoretical:
                raise ValueError(f"suspension_ex_formula_mismatch:{date}:{symbol}")
            bar = self.bar(symbol, date)
            if not (bar and bar["tradestatus"] == "0" and
                    bar["volume"] in {"", "0", "0.0"} and
                    D(bar["close"]) == special["halt_placeholder"] and
                    D(bar["preclose"]) == special["halt_placeholder"]):
                raise ValueError(f"suspension_ex_unproved_halt:{date}:{symbol}")
            return theoretical, "issuer_verified_suspension_ex_theoretical_mark"
        cash_halt = self.cash_suspension_mark(symbol, date, pmap, last)
        if cash_halt is not None:
            return cash_halt, "verified_suspension_cash_theoretical_mark"
        bonus_halt = self.bonus_suspension_mark(symbol, date, pmap, last)
        if bonus_halt is not None:
            return bonus_halt, "two_source_verified_suspension_bonus_theoretical_mark"
        mixed_halt = self.mixed_suspension_mark(symbol, date, pmap, last)
        if mixed_halt is not None:
            return mixed_halt, "verified_suspension_mixed_theoretical_mark"
        if last is None:
            earlier = [d for d in pmap if d < date]
            if earlier:
                last = pmap[max(earlier)]
        if symbol == "600633" and "2010-04-30" <= date < "2011-09-29":
            if last != D("7.28") or pmap.get("2010-04-29") != D("7.28"):
                raise ValueError(f"issuer_suspension_mark_anchor_mismatch:{date}:600633")
            return last, "issuer_verified_suspension_last_true_close"
        bar = self.bar(symbol, date)
        if (bar and bar["tradestatus"] == "1" and bar["volume"]
                and D(bar["volume"]) > 0 and bar["close"]):
            value = D(bar["close"])
            if value <= 0 or (last is not None and (symbol,date) not in self.action_exdates
                    and bar["preclose"] and D(bar["preclose"]) != last):
                raise ValueError(f"public_raw_supplement_previous_close_mismatch:{date}:{symbol}")
            pmap[date] = value
            self.raw_supplements[(symbol,date)] = {"date":date,"symbol":symbol,
                "close_raw":str(value),"preclose":bar["preclose"],
                "volume":bar["volume"],"tradestatus":bar["tradestatus"],
                "basis":"BaoStock_unadjusted_real_trade_supplement"}
            return value, "BaoStock_unadjusted_real_trade_supplement"
        if (bar and last is not None and bar["tradestatus"] == "0"
                and bar["volume"] in {"", "0", "0.0"} and D(bar["close"]) == last
                and D(bar["preclose"]) == last):
            return last, "verified_suspension_last_true_close"
        raise ValueError(f"unverified_missing_raw_close:{date}:{symbol}")

    def cash_suspension_mark(self, symbol: str, date: str,
                             pmap: dict[str, D], last: D | None) -> D | None:
        """Value a suspended ordinary cash ex-date with three independent checks.

        Dated CNInfo/Sina terms must agree.  A single-source event requires
        both the exact issuer implementation PDF and the unique original
        CNInfo API row, pinned by hash.  BaoStock zero-volume reference and
        later first true-trade preclose must match.  The latter is an audit
        check, never information available for an earlier trading signal.
        """
        candidates = [r for r in self.events_by_symbol.get(symbol, [])
                      if r["ex_date"] <= date and r["ex_date"]]
        if not candidates:
            return None
        r = max(candidates, key=lambda x: x["ex_date"])
        key = (symbol, r["ex_date"])
        if key not in self.cash_halt_candidates:
            return None
        before = [d for d in pmap if d < r["ex_date"]]
        after = [d for d in pmap if d >= r["ex_date"]]
        if not before or not after:
            raise ValueError(f"cash_halt_missing_true_trade_boundary:{date}:{symbol}")
        last_trade, resume = max(before), min(after)
        if date >= resume:
            return None
        if any(last_trade < x["ex_date"] < resume and x is not r
               for x in self.events_by_symbol[symbol]):
            raise ValueError(f"cash_halt_multiple_actions:{date}:{symbol}")
        anchor = pmap[last_trade]
        cash_per_share = D(r["cash_per_10_gross"]) / D(10)
        theoretical = (anchor - cash_per_share).quantize(D("0.01"), rounding=ROUND_HALF_UP)
        if theoretical <= 0 or last not in {None, anchor, theoretical}:
            raise ValueError(f"cash_halt_anchor_or_prior_mismatch:{date}:{symbol}")
        exbar, bar, resume_bar = (self.bar(symbol, d) for d in (r["ex_date"], date, resume))
        if not (exbar and bar and resume_bar
                and all(b["tradestatus"] == "0" and D(b["volume"] or "0") == 0
                        and D(b["close"]) == theoretical
                        and D(b["preclose"]) == theoretical for b in (exbar, bar))
                and resume_bar["tradestatus"] == "1"
                and D(resume_bar["volume"] or "0") > 0
                and D(resume_bar["close"]) == pmap[resume]
                and D(resume_bar["preclose"]) == theoretical):
            raise ValueError(f"cash_halt_reference_unproved:{date}:{symbol}")
        self.cash_halt_used[key] = {"symbol": symbol, "record_date": r["record_date"],
            "ex_date": r["ex_date"], "cash_pay_date": r["cash_pay_date"],
            "last_true_trade": last_trade, "raw_anchor": str(anchor),
            "cash_per_share_gross": str(cash_per_share),
            "theoretical_mark": str(theoretical), "resume_true_trade": resume,
            "resume_preclose": resume_bar["preclose"],
            "status": r["source_status"],
            "issuer_sha256": ISSUER_CHECKED_SINGLE_SOURCE_CASH_EVENTS.get(key, {}).get("issuer_sha256", ""),
            "cninfo_original_row_sha256": self.single_cash_original_sha.get(key, "")}
        return theoretical

    def bonus_suspension_mark(self, symbol: str, date: str,
                              pmap: dict[str, D], last: D | None) -> D | None:
        """Value ordinary pro-rata bonus shares credited on a halt ex-date.

        Different-shareholder stock-reform follow-ups cannot enter this path.
        Fractional paper shares remain a diagnostic approximation, not an
        exact retail depository account reconstruction.
        """
        candidates = [r for r in self.events_by_symbol.get(symbol, [])
                      if r["ex_date"] <= date and r["ex_date"]]
        if not candidates:
            return None
        r = max(candidates, key=lambda x: x["ex_date"])
        key = (symbol, r["ex_date"])
        if key not in self.bonus_halt_candidates:
            return None
        before = [d for d in pmap if d < r["ex_date"]]
        after = [d for d in pmap if d >= r["ex_date"]]
        if not before or not after:
            raise ValueError(f"bonus_halt_missing_true_trade_boundary:{date}:{symbol}")
        last_trade, resume = max(before), min(after)
        if date >= resume:
            return None
        if any(last_trade < x["ex_date"] < resume and x is not r
               for x in self.events_by_symbol[symbol]):
            raise ValueError(f"bonus_halt_multiple_actions:{date}:{symbol}")
        anchor = pmap[last_trade]
        bonus_per_share = D(r["bonus_per_10"]) / D(10)
        theoretical = (anchor / (D(1) + bonus_per_share)).quantize(
            D("0.01"), rounding=ROUND_HALF_UP)
        if theoretical <= 0 or last not in {None, anchor, theoretical}:
            raise ValueError(f"bonus_halt_anchor_or_prior_mismatch:{date}:{symbol}")
        exbar, bar, resume_bar = (self.bar(symbol, d) for d in (r["ex_date"], date, resume))
        if not (exbar and bar and resume_bar
                and all(b["tradestatus"] == "0" and D(b["volume"] or "0") == 0
                        and D(b["close"]) == theoretical
                        and D(b["preclose"]) == theoretical for b in (exbar, bar))
                and resume_bar["tradestatus"] == "1"
                and D(resume_bar["volume"] or "0") > 0
                and D(resume_bar["close"]) == pmap[resume]
                and D(resume_bar["preclose"]) == theoretical):
            raise ValueError(f"bonus_halt_reference_unproved:{date}:{symbol}")
        self.bonus_halt_used[key] = {"symbol": symbol, "record_date": r["record_date"],
            "ex_date": r["ex_date"], "share_credit_date": r["share_credit_date"],
            "last_true_trade": last_trade, "raw_anchor": str(anchor),
            "bonus_per_share": str(bonus_per_share), "theoretical_mark": str(theoretical),
            "resume_true_trade": resume, "resume_preclose": resume_bar["preclose"],
            "status": r["source_status"]}
        return theoretical

    def mixed_suspension_mark(self, symbol: str, date: str,
                              pmap: dict[str, D], last: D | None) -> D | None:
        """Value an ordinary pro-rata cash and share distribution during a halt."""
        candidates = [r for r in self.events_by_symbol.get(symbol, [])
                      if r["ex_date"] <= date and r["ex_date"]]
        if not candidates:
            return None
        r = max(candidates, key=lambda x: x["ex_date"])
        key = (symbol, r["ex_date"])
        if key not in self.mixed_halt_candidates:
            return None
        before = [d for d in pmap if d < r["ex_date"]]
        after = [d for d in pmap if d >= r["ex_date"]]
        if not before or not after:
            raise ValueError(f"mixed_halt_missing_true_trade_boundary:{date}:{symbol}")
        last_trade, resume = max(before), min(after)
        if date >= resume:
            return None
        if any(last_trade < x["ex_date"] < resume and x is not r
               for x in self.events_by_symbol[symbol]):
            raise ValueError(f"mixed_halt_multiple_actions:{date}:{symbol}")
        anchor = pmap[last_trade]
        cash_per_share = D(r["cash_per_10_gross"]) / D(10)
        bonus_per_share = D(r["bonus_per_10"]) / D(10)
        theoretical = ((anchor - cash_per_share) / (D(1) + bonus_per_share)).quantize(
            D("0.01"), rounding=ROUND_HALF_UP)
        if theoretical <= 0 or last not in {None, anchor, theoretical}:
            raise ValueError(f"mixed_halt_anchor_or_prior_mismatch:{date}:{symbol}")
        exbar, bar, resume_bar = (self.bar(symbol, d) for d in (r["ex_date"], date, resume))
        if not (exbar and bar and resume_bar
                and all(b["tradestatus"] == "0" and D(b["volume"] or "0") == 0
                        and D(b["close"]) == theoretical
                        and D(b["preclose"]) == theoretical for b in (exbar, bar))
                and resume_bar["tradestatus"] == "1"
                and D(resume_bar["volume"] or "0") > 0
                and D(resume_bar["close"]) == pmap[resume]
                and D(resume_bar["preclose"]) == theoretical):
            raise ValueError(f"mixed_halt_reference_unproved:{date}:{symbol}")
        self.mixed_halt_used[key] = {"symbol": symbol, "record_date": r["record_date"],
            "ex_date": r["ex_date"], "cash_pay_date": r["cash_pay_date"],
            "share_credit_date": r["share_credit_date"],
            "last_true_trade": last_trade, "raw_anchor": str(anchor),
            "cash_per_share_gross": str(cash_per_share),
            "bonus_per_share": str(bonus_per_share),
            "theoretical_mark": str(theoretical),
            "resume_true_trade": resume, "resume_preclose": resume_bar["preclose"],
            "status": r["source_status"],
            "issuer_sha256": ISSUER_CHECKED_SINGLE_SOURCE_MIXED_EVENTS.get(key, {}).get("issuer_sha256", ""),
            "cninfo_original_row_sha256": self.single_mixed_original_sha.get(key, "")}
        return theoretical

    def save(self):
        # The deterministic cache is a raw public-source observation, not a
        # strategy cache.  A query error leaves previously fetched rows intact.
        source_rows = [r for (s, d), r in sorted(self.bars.items()) if d >= "2010-02-22"]
        with self.bars_path.open("w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=FIELDS.split(",") + ["symbol"])
            writer.writeheader()
            writer.writerows(source_rows)
        if self.raw_supplements:
            with self.supplement_path.open("w",encoding="utf-8",newline="") as f:
                w=csv.DictWriter(f,fieldnames=list(next(iter(self.raw_supplements.values()))))
                w.writeheader()
                w.writerows(r for _,r in sorted(self.raw_supplements.items()))
        self.coverage_path.write_text(json.dumps(self.coverage, ensure_ascii=False, sort_keys=True, indent=2)+"\n", encoding="utf-8")


def main():
    HERE.mkdir(exist_ok=True)
    prior = json.loads(FIRST.read_text(encoding="utf-8"))
    calendar = verified_calendar()
    dates = [d for d in calendar if d > "2010-02-12"]
    by_signal: dict[str, list[str]] = {}
    for r in rows(MEMBERS):
        by_signal.setdefault(r["rebalance_date"], []).append(r["symbol"].zfill(6))
    target_rows = rows(MEMBERS)
    by_signal = {}
    for r in target_rows:
        by_signal.setdefault(r["rebalance_date"], []).append(r)
    for day, members in by_signal.items():
        by_signal[day] = [r["symbol"].zfill(6) for r in sorted(members, key=lambda x: int(x["rank"]))]
    actions: dict[tuple[str, str], list[dict[str, str]]] = {}
    for r in rows(EVENT_CANDIDATES):
        for key, kind in (("record_date", "record"), ("ex_date", "ex"), ("cash_pay_date", "cash_paid"), ("share_credit_date", "stock_credited")):
            d = r.get(key, "")
            if d and "2010-02-12" < d <= dates[-1]:
                actions.setdefault((d, kind), []).append(r)
    precision_audit = {(r["symbol"].zfill(6), r["ex_date"]): r for r in rows(PRECISION_AUDIT)}
    precision_used: set[tuple[str, str]] = set()
    credit_used: set[tuple[str, str]] = set()
    credit_original_sha: dict[tuple[str, str], str] = {}
    source = Source(namespace="full")
    login = bs.login()
    if login.error_code != "0":
        raise RuntimeError(f"baostock_login_failed:{login.error_code}:{login.error_msg}")
    branches = {}
    all_positions = []
    all_orders = []
    all_rights = []
    try:
        for funding in ("prefunded", "netted"):
            previous = prior["branches"][funding]
            if previous["first_strict_block"] or previous["last_conditional_date"] != "2010-02-12":
                raise ValueError(f"invalid_prior_branch:{funding}")
            end = previous["events"][-1]
            positions = {s: D(q) for s, q in end["positions_after"].items()}
            cash = D(end["cash_after"])
            last = {s: source.mark(s, "2010-02-12")[0] for s in positions}
            entitlements = []
            cash_receivable = D(0)
            pending_shares = {}
            frozen = None
            daily = []
            trades = []
            first_block = None
            suspensions = 0
            for i, date in enumerate(dates):
                snapshot = (dict(positions), cash, dict(last), [dict(x) for x in entitlements],
                    cash_receivable, dict(pending_shares), frozen, len(trades), len(all_positions),
                    len(all_orders), len(all_rights), len(daily))
                try:
                    entitlement_events = []
                    settled_orders = []
                    # Rights are fixed from the already-booked record-close
                    # shares.  Ex date creates economic receivables; payout
                    # and credit transfer those to spendable cash/shares.
                    for item in entitlements:
                        if item["ex"] == date:
                            require_ex_raw(source, item, date)
                            if item["active"]:
                                raise AssertionError("duplicate_ex_event")
                            item["active"] = True
                            cash_receivable += item["cash_due"]
                            if item["bonus_due"]:
                                pending_shares[item["symbol"]] = pending_shares.get(item["symbol"], D(0)) + item["bonus_due"]
                            entitlement_events.append({"phase": "ex", "symbol": item["symbol"],
                                "cash_due": str(item["cash_due"]), "bonus_due": str(item["bonus_due"])})
                        if item["active"] and item["cash_paid"] == date and item["cash_due"]:
                            cash += item["cash_due"]
                            cash_receivable -= item["cash_due"]
                            entitlement_events.append({"phase": "cash_paid", "symbol": item["symbol"], "amount": str(item["cash_due"])})
                        if item["active"] and item["stock_credited"] == date and item["bonus_due"]:
                            positions[item["symbol"]] = positions.get(item["symbol"], D(0)) + item["bonus_due"]
                            pending_shares[item["symbol"]] -= item["bonus_due"]
                            if pending_shares[item["symbol"]] == 0:
                                del pending_shares[item["symbol"]]
                            entitlement_events.append({"phase": "stock_credited", "symbol": item["symbol"], "shares": str(item["bonus_due"])})
                    # A corporate-action row for a currently held stock with
                    # no record-close entitlement is harmless if it was bought
                    # after record.  A pending entitlement with impossible
                    # event dates is not harmless and was checked at record.
                    prices = {}
                    basis_by_symbol = {}
                    suspended = []
                    for symbol in positions:
                        price, basis = source.mark(symbol, date, last.get(symbol))
                        prices[symbol] = price
                        basis_by_symbol[symbol] = basis
                        last[symbol] = price
                        if "suspension" in basis:
                            suspended.append(symbol)
                    suspensions += len(suspended)
                    pending_value = D(0)
                    for s, q in pending_shares.items():
                        pending_price = prices[s] if s in prices else source.mark(s, date)[0]
                        pending_value += q * pending_price
                    equity_before = cash + cash_receivable + pending_value + sum((q * prices[s] for s, q in positions.items()), D(0))
                    fees = D(0)
                    if frozen is not None:
                        blocked = set()
                        why = {}
                        for order in frozen:
                            symbol = order["symbol"]
                            if symbol not in prices:
                                prices[symbol], basis_by_symbol[symbol] = source.mark(symbol, date)
                            rejection = check_paper_trade(source, symbol, date, order["side"], prices[symbol])
                            if rejection:
                                blocked.add(symbol)
                                why[symbol] = rejection
                        result = settle_next_close(positions, cash, frozen, prices, blocked, funding)
                        settled_orders = [{"branch": funding, "signal_date": e["signal_date"],
                            "execution_date": date, "symbol": e["symbol"], "side": e["side"],
                            "frozen_shares": str(e["frozen_shares"]), "signal_raw_price": e["signal_price"],
                            "execution_raw_price": e["price_next"], "booked": e["booked"],
                            "reason": e["reason"], "gross": e["gross"], "fee": e["fee"]}
                            for e in result["events"]]
                        positions, cash = result["positions_after"], result["cash_after"]
                        fees = result["fees"]
                        last = {s: prices[s] for s in positions}
                        trades.append({"execution_date": date, "signal_date": dates[i-1],
                            "frozen_orders": len(frozen), "booked_sells": sum(e["booked"] and e["side"] == "sell" for e in result["events"]),
                            "booked_buys": sum(e["booked"] and e["side"] == "buy" for e in result["events"]),
                            "blocked": why, "fees": str(fees), "member_count": len(positions)})
                        frozen = None
                    equity_after = cash + cash_receivable + pending_value + sum((q * last[s] for s, q in positions.items()), D(0))
                    if abs(equity_before - fees - equity_after) > D("1e-20") or cash < 0:
                        raise AssertionError(f"account_conservation:{date}")
                    # A record-date close can create next-day cash/shares due;
                    # stop before claiming the close state is settled.
                    hit = [r for r in actions.get((date, "record"), []) if r["symbol"].zfill(6) in positions]
                    if hit:
                        for r in hit:
                            symbol, ex = r["symbol"].zfill(6), r["ex_date"]
                            if any(x["symbol"] == symbol and x["record"] == date and x["ex"] == ex for x in entitlements):
                                raise ValueError(f"duplicate_record_entitlement:{date}:{symbol}")
                            proof = verified_event_contract(r, positions[symbol],
                                precision_audit, precision_used, credit_used, credit_original_sha)
                            entitlements.append(proof)
                            entitlement_events.append({"phase": "record", "symbol": symbol,
                                "record_shares": str(positions[symbol]), "cash_due": str(proof["cash_due"]),
                                "bonus_due": str(proof["bonus_due"]), "source_status":proof["source_status"]})
                    if date in by_signal:
                        target = by_signal[date]
                        if len(target) != 100 or len(set(target)) != 100:
                            raise ValueError(f"target_invalid:{date}")
                        signal_positions = dict(positions)
                        if pending_shares:
                            # A right already valued at T may enter a T-close
                            # order only when the issuer-proved paper release
                            # precedes T+1 settlement.  It remains unavailable
                            # for any trade on T itself.
                            if i + 1 >= len(dates):
                                raise ValueError(f"pending_bonus_no_next_session:{date}")
                            next_date = dates[i + 1]
                            due_by_symbol: dict[str, D] = {}
                            for item in entitlements:
                                if (item["active"] and item["bonus_due"] > 0
                                        and item["stock_credited"] > date):
                                    if item["stock_credited"] != next_date:
                                        raise ValueError(f"pending_bonus_not_next_session:{date}:{item['symbol']}")
                                    symbol = item["symbol"]
                                    due_by_symbol[symbol] = due_by_symbol.get(symbol, D(0)) + item["bonus_due"]
                            if due_by_symbol != pending_shares:
                                raise ValueError(f"pending_bonus_rights_mismatch:{date}")
                            for symbol, q in pending_shares.items():
                                signal_positions[symbol] = signal_positions.get(symbol, D(0)) + q
                        price_t = dict(last)
                        for symbol in set(target) | set(signal_positions):
                            if symbol not in price_t:
                                price_t[symbol] = source.mark(symbol, date)[0]
                        # Receivable is economic equity but cannot fund orders
                        # until its payment date.  Settlement still uses cash.
                        frozen = freeze_orders(signal_positions, cash + cash_receivable, price_t, target)
                        for order in frozen:
                            order["signal_date"] = date
                            order["signal_price"] = str(price_t[order["symbol"]])
                    daily.append({"branch": funding, "date": date, "cash": str(cash),
                        "stock_value": str(equity_after-cash-cash_receivable-pending_value),
                        "cash_receivable": str(cash_receivable), "pending_share_value": str(pending_value),
                        "equity": str(equity_after),
                        "fees": str(fees), "held_count": len(positions),
                        "suspended_held_count": len(suspended), "frozen_orders_for_next": len(frozen or [])})
                    for symbol, q in sorted(positions.items()):
                        all_positions.append({"branch": funding, "date": date,
                            "state": "settled_shares", "symbol": symbol, "shares": str(q),
                            "valuation_price": str(last[symbol]),
                            "market_value": str(q * last[symbol]),
                            "price_basis": basis_by_symbol.get(symbol, "true_raw_close")})
                    for symbol, q in sorted(pending_shares.items()):
                        px, basis = source.mark(symbol, date)
                        all_positions.append({"branch": funding, "date": date,
                            "state": "pending_bonus_shares", "symbol": symbol, "shares": str(q),
                            "valuation_price": str(px), "market_value": str(q * px),
                            "price_basis": basis})
                    all_orders.extend(settled_orders)
                    if entitlement_events:
                        trades.append({"date": date, "corporate_events": entitlement_events})
                        for event in entitlement_events:
                            all_rights.append({"branch": funding, "date": date,
                                "phase": event["phase"], "symbol": event["symbol"],
                                "record_shares": event.get("record_shares", ""),
                                "cash_due": event.get("cash_due", ""),
                                "bonus_due": event.get("bonus_due", ""),
                                "amount": event.get("amount", ""), "shares": event.get("shares", "")})
                except (ValueError, AssertionError) as exc:
                    (positions, cash, last, entitlements, cash_receivable,
                        pending_shares, frozen, old_trade_count, old_position_count,
                        old_order_count, old_rights_count, old_daily_count) = snapshot
                    trades = trades[:old_trade_count]
                    del all_positions[old_position_count:]
                    del all_orders[old_order_count:]
                    del all_rights[old_rights_count:]
                    del daily[old_daily_count:]
                    first_block = {"date": date, "reason": str(exc)}
                    break
            branches[funding] = {"last_settled_date": daily[-1]["date"] if daily else "2010-02-12",
                "settled_days_after_prior": len(daily), "first_block": first_block,
                "suspension_marks": suspensions, "executed_rebalances": trades,
                "last_cash": str(cash), "last_positions": {s: str(q) for s,q in positions.items()},
                "last_cash_receivable": str(cash_receivable),
                "last_pending_shares": {s: str(q) for s,q in pending_shares.items()},
                "daily": daily}
    finally:
        bs.logout()
        source.save()
    all_daily = [r for b in branches.values() for r in b["daily"]]
    if all_daily:
        with (HERE / "full_history_daily_account.csv").open("w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(all_daily[0]))
            w.writeheader()
            w.writerows(all_daily)
    for filename, data in (("full_history_daily_positions.csv", all_positions),
                           ("full_history_settled_orders.csv", all_orders),
                           ("full_history_rights_events.csv", all_rights)):
        if data:
            with (HERE / filename).open("w", encoding="utf-8", newline="") as f:
                w = csv.DictWriter(f, fieldnames=list(data[0]))
                w.writeheader()
                w.writerows(data)
    manifest = {"status": "diagnostic_old_L1_inputs_no_formal_NAV", "capital_start": "1000000",
        "funding_contracts": ["prefunded", "netted_same_close_theoretical"],
        "fractional_shares": True, "adjustment": "raw_unadjusted", "fee_each_side": "0.003",
        "corporate_action_policy": "at record close freeze actual held shares; accept normalized official CNInfo single-source or two-source matched events; reject conflict/missing terms; at ex create gross cash and bonus receivable, later transfer to cash/shares",
        "proxy_nontrading_rows_excluded": ["2018-06-18"],
        "source": "BaoStock anonymous query_history_k_data_plus adjustflag=3; local raw cache; normalized event candidates",
        "precision_decisions_used": [{"symbol": s, "ex_date": d} for s, d in sorted(precision_used)],
        "issuer_credit_decisions_used": [{"symbol": s, "ex_date": d,
            "paper_release_date": ISSUER_CHECKED_CREDIT_EVENTS[(s, d)]["paper_release_date"],
            "actual_depository_credit_date": "unknown",
            "cninfo_original_row_sha256": credit_original_sha.get((s, d), "")}
            for s, d in sorted(credit_used)],
        "cash_halt_marks_used": [source.cash_halt_used[k] for k in sorted(source.cash_halt_used)],
        "bonus_halt_marks_used": [source.bonus_halt_used[k] for k in sorted(source.bonus_halt_used)],
        "mixed_halt_marks_used": [source.mixed_halt_used[k] for k in sorted(source.mixed_halt_used)],
        "queries": source.queries, "queried_bars_sha256": digest(source.bars_path),
        "detail_sha256": {p.name: digest(p) for p in (HERE / "full_history_daily_account.csv", HERE / "full_history_daily_positions.csv",
            HERE / "full_history_settled_orders.csv", HERE / "full_history_rights_events.csv") if p.exists()},
        "input_sha256": {str(p.relative_to(ROOT)).replace('\\','/'): digest(p) for p in
            [FIRST, MEMBERS, CALENDAR, TRADE_CALENDAR, TRADE_CALENDAR_MANIFEST,
             TRADE_CALENDAR_EVIDENCE, TRADE_CALENDAR_FETCHER,
             EVENT_CANDIDATES, PRECISION_AUDIT, RIGHTS, CNINFO_MANIFEST,
             RESTRUCTURING_EVIDENCE, *EXISTING_BARS, *sorted(ISSUER_EVIDENCE.glob("*.md")),
             *sorted(ISSUER_EVIDENCE.glob("*.pdf")),
             *sorted(ISSUER_EVIDENCE.glob("*.html")),
             *sorted(source.raw_paths)]},
        "code_sha256": digest(Path(__file__)), "source_module_sha256": digest(HERE / "recent_slice.py"),
        "contract_sha256": digest(HERE / "ledger_contract.py"),
        "branches": branches}
    (HERE / "full_history_result.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2)+"\n", encoding="utf-8")
    print(json.dumps({k: {"last": v["last_settled_date"], "days": v["settled_days_after_prior"],
        "block": v["first_block"], "rebalances": len(v["executed_rebalances"])}
        for k,v in branches.items()}, ensure_ascii=False))


if __name__ == "__main__":
    main()
