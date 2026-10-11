"""Independent admission test for one issuer-backed single-source cash event."""

import csv
import hashlib
import json
from decimal import Decimal as D
from pathlib import Path

import fitz

ROOT = Path(__file__).resolve().parents[1]
PDF = ROOT / "data/suspension_ex_evidence/300141_20150429_issuer_dividend.pdf"
CANDIDATES = ROOT / "data/event_candidates_2010_20260930.csv"
RAW_CNINFO = ROOT.parents[0] / "20260926_l1_repair/corporate_actions/cninfo_distribution_events.csv"
BARS = ROOT / "corporate_conflict_evidence/300141_baostock_unadjusted_window.csv"


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def records(path):
    with path.open(encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def admit(row, rivals, bars, source_row=None):
    """Exact historical audit gate; no live-time claim from the resume bar."""
    fixed = {
        "symbol": "300141", "announcement_date": "2015-04-29",
        "record_date": "2015-05-06", "ex_date": "2015-05-07",
        "cash_pay_date": "2015-05-07", "source": "cninfo",
        "source_status": "single_source_only", "source_rows": "cninfo=1;sina=0",
    }
    if any(row.get(k) != v for k, v in fixed.items()):
        return "issuer_identity_or_clock_mismatch"
    if not (row["announcement_date"] < row["record_date"] < row["ex_date"]):
        return "issuer_notice_too_late"
    if D(row["cash_per_10_gross"]) != D("1.000000"):
        return "issuer_cash_terms_mismatch"
    if source_row is not None and (
        source_row["实施方案公告日期"] != row["announcement_date"]
        or source_row["股权登记日"] != row["record_date"]
        or source_row["除权日"] != row["ex_date"]
        or source_row["派息日"] != row["cash_pay_date"]
        or D(source_row["派息比例"] or "0") != D(row["cash_per_10_gross"])
        or D(source_row["送股比例"] or "0") + D(source_row["转增比例"] or "0") != 0
    ):
        return "raw_official_line_mismatch"
    if D(row["bonus_per_10"] or "0") != 0 or row["rights_per_10"]:
        return "mixed_action"
    if any(x["symbol"] == row["symbol"] and "2015-01-22" < x["ex_date"] < "2015-05-20" for x in rivals):
        return "concurrent_right_during_halt"
    by = {x["date"]: x for x in bars}
    ex = by["2015-05-07"]
    resume = by["2015-05-20"]
    anchor = by["2015-01-22"]
    theoretical = D(anchor["close"]) - D(row["cash_per_10_gross"]) / D(10)
    if not (D(anchor["volume"]) > 0 and anchor["tradestatus"] == "1"
            and D(ex["volume"]) == 0 and ex["tradestatus"] == "0"
            and D(ex["close"]) == D(ex["preclose"]) == theoretical
            and D(resume["volume"]) > 0 and resume["tradestatus"] == "1"
            and D(resume["preclose"]) == theoretical):
        return "raw_price_boundary_mismatch"
    return "admit_historical_paper_valuation_only"


def main():
    assert sha(PDF) == "319387b47ec4ba41ee70ad868fc783190f9b0352b5d26f042ec8db2051b50533"
    with fitz.open(PDF) as doc:
        issuer = "".join(page.get_text() for page in doc)
    for phrase in ("证券代码：300141", "每10股派1.000000元人民币现金", "2015年05月06日", "2015年05月07日", "直接划入其资金账户"):
        assert phrase in issuer, phrase
    row = [x for x in records(CANDIDATES) if x["symbol"] == "300141" and x["ex_date"] == "2015-05-07"]
    assert len(row) == 1
    row = row[0]
    source = [x for x in records(RAW_CNINFO) if x["symbol"] == "300141" and x["除权日"] == "2015-05-07"]
    assert len(source) == 1
    src = source[0]
    assert (src["实施方案公告日期"], src["股权登记日"], src["派息日"], src["派息比例"]) == (row["announcement_date"], row["record_date"], row["cash_pay_date"], row["cash_per_10_gross"])
    bars = records(BARS)
    assert sha(BARS) == "59f677b7cfe30cf6bc987e6aee12af357cfc93c1d0276ea2409e025a231c9f76"
    assert admit(row, [], bars, src) == "admit_historical_paper_valuation_only"
    row_sha = hashlib.sha256(json.dumps(src, ensure_ascii=False, sort_keys=True).encode("utf-8")).hexdigest()
    manifest = json.loads((ROOT / "engine/full_history_result.json").read_text(encoding="utf-8"))
    active = [x for x in manifest["cash_halt_marks_used"] if x["symbol"] == "300141" and x["ex_date"] == "2015-05-07"]
    assert len(active) == 1 and active[0]["cninfo_original_row_sha256"] == row_sha
    mutations = {
        "official_row_cash_1p1": admit({**row, "cash_per_10_gross": "1.1"}, [], bars),
        "raw_cninfo_row_cash_1p1": admit(row, [], bars, {**src, "派息比例": "1.1"}),
        "official_row_wrong_record_date": admit({**row, "record_date": "2015-05-05"}, [], bars),
        "official_row_late_announcement": admit({**row, "announcement_date": "2015-05-08"}, [], bars),
        "official_row_mixed_bonus": admit({**row, "bonus_per_10": "1.0"}, [], bars),
        "second_right_same_ex_date": admit(row, [{**row, "bonus_per_10": "1.0"}], bars),
        "second_right_later_halt": admit(row, [{**row, "ex_date": "2015-05-11", "bonus_per_10": "1.0"}], bars),
    }
    assert all(x != "admit_historical_paper_valuation_only" for x in mutations.values())
    output = {
        "status": "PASS_exact_event_single_source_gate_only",
        "source_hashes": {"issuer_pdf": sha(PDF), "normalized_cninfo_file": sha(RAW_CNINFO), "candidate_file": sha(CANDIDATES), "baostock_window": sha(BARS)},
        "raw_cninfo_row": src,
        "raw_cninfo_row_sha256": row_sha,
        "fixed_event": {k: row[k] for k in ("symbol", "announcement_date", "record_date", "ex_date", "cash_pay_date", "cash_per_10_gross", "source_status")},
        "mark_path": {"last_true_trade_close": "13.91", "zero_volume_ex_reference": "13.81", "first_real_trade_preclose": "13.81"},
        "mutations_rejected": mutations,
        "admission_boundary": "Historical gross-paper valuation of this exact event only. The 2015-05-20 resume preclose is retrospective corroboration, unavailable on 2015-05-07; it cannot justify an ex-ante signal or a suspended fill. CNInfo normalized/API history retrieved in 2026 is not a preserved 2015 PIT source. Investor-specific tax and self-distributed cash are unverified.",
    }
    dest = Path(__file__).with_name("single_source_gate_adversarial_audit.json")
    dest.write_text(json.dumps(output, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(output, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
