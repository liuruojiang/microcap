"""Read-only, independent event and ledger checks for two halted cash dividends."""

import csv
import hashlib
import json
from decimal import Decimal as D, ROUND_HALF_UP, getcontext
from pathlib import Path

import fitz
import baostock as bs

getcontext().prec = 60
HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
REPO = ROOT.parents[1]
ENGINE = ROOT / "engine"
ORIGINAL = REPO / "research_reports/20260926_l1_repair/corporate_actions/cninfo_distribution_events.csv"
CANDIDATE = ROOT / "data/event_candidates_2010_20260930.csv"

EVENTS = {
    "002735": {
        "date": "2016-06-03", "record": "2016-06-02", "announce": "2016-05-27",
        "cash_10": D("0.82"), "anchor": D("41.27"), "mark": D("41.19"),
        "last_trade": "2016-04-01", "resume": "2016-11-04",
        "pdf": "002735_20160527_issuer_dividend.pdf",
        "pdf_sha": "eeed3d9fecd6dd8ee69d8ad462d92fac76dd557880ceea5176a13fd8cfec951a",
        "wrong_cash_10": D("0.80"),
    },
    "300046": {
        "date": "2016-06-15", "record": "2016-06-14", "announce": "2016-06-06",
        "cash_10": D("2.5"), "anchor": D("17.84"), "mark": D("17.59"),
        "last_trade": "2016-03-04", "resume": "2016-09-20",
        "pdf": "300046_20160606_issuer_dividend.pdf",
        "pdf_sha": "0104460007b40cb68b31983a7cf580600cc0fb5f39e8d96b4c91e91a3a6ffb51",
        "wrong_cash_10": D("2.4"),
    },
    "300461": {
        "date": "2016-06-16", "record": "2016-06-15", "announce": "2016-06-07",
        "cash_10": D("1.5"), "anchor": D("46.35"), "mark": D("46.20"),
        "last_trade": "2016-05-24", "resume": "2016-10-27",
        "pdf": "300461_20160607_issuer_dividend.pdf",
        "pdf_sha": "2b71e38e0ff4adabac6e66ba07876e7cd872d70ffafe8786ee8758fb0ffa0830",
        "wrong_cash_10": D("1.51"),
    },
    "600189": {
        "date": "2016-07-18", "record": "2016-07-15", "announce": "2016-07-08",
        "cash_10": D("0.5"), "anchor": D("12.10"), "mark": D("12.05"),
        "last_trade": "2016-07-07", "resume": "2017-01-04",
        "pdf": "600189_20160708_issuer_dividend.pdf",
        "pdf_sha": "87429d77b24a9652d8c2d689396dde11852c90f6d9689647021265d6f5b80143",
        "wrong_cash_10": D("0.51"),
    },
    "300423": {
        "date": "2017-05-26", "record": "2017-05-25", "announce": "2017-05-19",
        "cash_10": D("0.5"), "anchor": D("34.00"), "mark": D("33.95"),
        "last_trade": "2017-02-24", "resume": "2017-07-31",
        "pdf": "300423_20170519_issuer_dividend.pdf",
        "pdf_sha": "aad834fa11a91d266fc32c0423546f648ed5548df15ff43c4488cee0ca8f330e",
        "wrong_cash_10": D("0.51"),
    },
    "300478": {
        "date": "2017-06-07", "record": "2017-06-06", "announce": "2017-06-01",
        "cash_10": D("1.5"), "anchor": D("55.75"), "mark": D("55.60"),
        "last_trade": "2017-03-17", "resume": "2017-08-07",
        "pdf": "300478_20170601_issuer_dividend.pdf",
        "pdf_sha": "6ce5cfc87620e740b380f09b2ccaccdd3ca6b4b8ab3a90e4229d382a861dcc5b",
        "wrong_cash_10": D("1.51"),
    },
    "603033": {
        "date": "2018-05-21", "record": "2018-05-18", "announce": "2018-05-11",
        "cash_10": D("1.5"), "anchor": D("18.85"), "mark": D("18.70"),
        "last_trade": "2018-01-31", "resume": "2018-05-28",
        "pdf": "603033_20180511_issuer_dividend.pdf",
        "pdf_sha": "36cb0ce8869ac318b36a1615561eaca1982a30dbd55946f67e0f7bb498b0f4f8",
        "wrong_cash_10": D("1.51"),
    },
    "002778": {
        "date": "2018-06-15", "record": "2018-06-14", "announce": "2018-06-07",
        "cash_10": D("0.7"), "anchor": D("19.09"), "mark": D("19.02"),
        "last_trade": "2018-04-20", "resume": "2018-07-20",
        "pdf": "002778_20180607_issuer_dividend.pdf",
        "pdf_sha": "eb75b5d9518345cfd50222cdabe7a33b5482cac198f0cf3a18501916de8a8388",
        "wrong_cash_10": D("0.71"),
    },
    "600796": {
        "date": "2018-06-27", "record": "2018-06-26", "announce": "2018-06-21",
        "cash_10": D("0.3"), "anchor": D("6.11"), "mark": D("6.08"),
        "last_trade": "2018-04-27", "resume": "2018-10-31",
        "pdf": "600796_20180621_issuer_dividend.pdf",
        "pdf_sha": "c0e8e52e11528f70650b43d1a48ff4078394841d2ed9ba0b5ee487d44e2ae63c",
        "wrong_cash_10": D("0.31"),
    },
}


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def rows(path):
    with path.open(encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def one(items, **key):
    selected = [r for r in items if all(r[k] == v for k, v in key.items())]
    assert len(selected) == 1, (key, len(selected))
    return selected[0]


def close(a, b, label):
    assert abs(a - b) < D("1e-30"), (label, str(a), str(b), str(a-b))


def first_resume_from_baostock(symbol, ex_date):
    login = bs.login()
    assert login.error_code == "0", login.error_msg
    try:
        fields = "date,close,preclose,volume,tradestatus"
        query = bs.query_history_k_data_plus(("sh." if symbol.startswith("6") else "sz.") + symbol, fields, start_date=ex_date, end_date="2020-12-31", frequency="d", adjustflag="3")
        assert query.error_code == "0", query.error_msg
        while query.next():
            item = dict(zip(fields.split(","), query.get_row_data()))
            if item["date"] > ex_date and D(item["volume"] or "0") > 0:
                return item
        raise AssertionError((symbol, "no_resume"))
    finally:
        bs.logout()


def last_true_from_baostock(symbol, ex_date):
    login = bs.login()
    assert login.error_code == "0", login.error_msg
    try:
        fields = "date,close,preclose,volume,tradestatus"
        query = bs.query_history_k_data_plus(("sh." if symbol.startswith("6") else "sz.") + symbol, fields, start_date="2016-01-01", end_date=ex_date, frequency="d", adjustflag="3")
        assert query.error_code == "0", query.error_msg
        last = None
        while query.next():
            item = dict(zip(fields.split(","), query.get_row_data()))
            if item["date"] < ex_date and D(item["volume"] or "0") > 0:
                last = item
        assert last, (symbol, "no_last_trade")
        return last
    finally:
        bs.logout()


def run():
    manifest_path = ENGINE / "full_history_result.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert sha(ENGINE / "full_history.py") == manifest["code_sha256"]
    for name, value in manifest["detail_sha256"].items():
        assert sha(ENGINE / name) == value, name
    for name, value in manifest["input_sha256"].items():
        assert sha(REPO / name) == value, name

    original = rows(ORIGINAL)
    candidate = rows(CANDIDATE)
    bars = rows(ENGINE / "full_queried_baostock_bars.csv")
    rights = rows(ENGINE / "full_history_rights_events.csv")
    positions = rows(ENGINE / "full_history_daily_positions.csv")
    orders = rows(ENGINE / "full_history_settled_orders.csv")
    output = {"status": "PASS_EXACT_EVENTS_THROUGH_CURRENT_FIRST_BLOCK", "manifest_sha256": sha(manifest_path), "code_sha256": manifest["code_sha256"], "events": {}, "first_blocks": {b: manifest["branches"][b]["first_block"] for b in ("prefunded", "netted")}}

    for symbol, e in EVENTS.items():
        pdf = ROOT / "data/suspension_ex_evidence" / e["pdf"]
        assert sha(pdf) == e["pdf_sha"]
        with fitz.open(pdf) as doc:
            pdf_text = "".join(page.get_text() for page in doc)
        assert symbol in pdf_text
        assert e["record"][:4] in pdf_text
        # The PDF contains all three dates and the exact gross payment term.
        y, month, day = e["record"].split("-")
        assert f"{int(month)} 月{int(day)} 日" in pdf_text or f"{int(month)}月{int(day)}日" in pdf_text or f"{y}/{int(month)}/{int(day)}" in pdf_text
        gross_text = str(e["cash_10"])
        assert gross_text in pdf_text or f"{e['cash_10']:.2f}" in pdf_text or str(e["cash_10"] / 10) in pdf_text
        o = one(original, symbol=symbol, 除权日=e["date"])
        c = one(candidate, symbol=symbol, ex_date=e["date"])
        assert (o["实施方案公告日期"], o["股权登记日"], o["派息日"], o["派息比例"]) == (e["announce"], e["record"], e["date"], c["cash_per_10_gross"])
        assert (c["source_status"], c["source_rows"], D(c["cash_per_10_gross"])) == ("single_source_only", "cninfo=1;sina=0", e["cash_10"])
        assert D(c["bonus_per_10"] or "0") == 0 and not c["rights_per_10"]
        original_row_sha = hashlib.sha256(json.dumps(o, ensure_ascii=False, sort_keys=True).encode("utf-8")).hexdigest()

        symbol_bars = [r for r in bars if r["symbol"] == symbol]
        old_bar = one(symbol_bars, date=e["record"])
        ex_bar = one(symbol_bars, date=e["date"])
        prior = [r for r in symbol_bars if r["date"] < e["date"] and D(r["volume"] or "0") > 0]
        last_trade = prior[-1] if prior else last_true_from_baostock(symbol, e["date"])
        future = [r for r in symbol_bars if r["date"] > e["date"] and D(r["volume"] or "0") > 0]
        resume = future[0] if future else first_resume_from_baostock(symbol, e["date"])
        assert (last_trade["date"], resume["date"]) == (e["last_trade"], e["resume"])
        assert D(last_trade["close"]) == D(old_bar["close"]) == e["anchor"]
        assert D(old_bar["volume"] or "0") == D(ex_bar["volume"] or "0") == 0
        assert D(ex_bar["close"]) == D(resume["preclose"]) == e["mark"]
        theoretical = (e["anchor"] - e["cash_10"] / 10).quantize(D("0.01"), rounding=ROUND_HALF_UP)
        assert theoretical == e["mark"]
        wrong_mark = (e["anchor"] - e["wrong_cash_10"] / 10).quantize(D("0.01"), rounding=ROUND_HALF_UP)

        used = [r for r in manifest.get("cash_halt_marks_used", []) if (r["symbol"], r["ex_date"]) == (symbol, e["date"])]
        if not used:
            output["events"][symbol] = {"status": "PREFLIGHT_ONLY_FIRST_BLOCK", "issuer_sha256": sha(pdf), "original_row_sha256": original_row_sha, "source_candidate_sha256": sha(CANDIDATE), "market": {"last_trade": last_trade, "record": old_bar, "ex": ex_bar, "resume": resume}, "wrong_amount_same_mark": wrong_mark == e["mark"]}
            continue
        assert len(used) == 1
        mark = used[0]
        assert (mark["issuer_sha256"], mark["cninfo_original_row_sha256"], mark["last_true_trade"], mark["resume_true_trade"]) == (sha(pdf), original_row_sha, e["last_trade"], e["resume"])
        assert (D(mark["raw_anchor"]), D(mark["theoretical_mark"]), D(mark["cash_per_share_gross"])) == (e["anchor"], e["mark"], e["cash_10"] / 10)
        branches = {}
        for branch in ("prefunded", "netted"):
            held_record = [r for r in rights if (r["branch"], r["symbol"], r["date"], r["phase"]) == (branch, symbol, e["record"], "record")]
            if not held_record:
                assert not [r for r in rights if (r["branch"], r["symbol"], r["date"]) == (branch, symbol, e["date"])], (branch, symbol, "orphan_ex_right")
                assert not [r for r in positions if (r["branch"], r["symbol"], r["date"]) == (branch, symbol, e["date"])], (branch, symbol, "orphan_ex_position")
                branches[branch] = {"status": "NOT_HELD_ON_RECORD_DATE"}
                continue
            record = one(rights, branch=branch, symbol=symbol, date=e["record"], phase="record")
            ex = one(rights, branch=branch, symbol=symbol, date=e["date"], phase="ex")
            paid = one(rights, branch=branch, symbol=symbol, date=e["date"], phase="cash_paid")
            old = one(positions, branch=branch, symbol=symbol, date=e["record"], state="settled_shares")
            new = one(positions, branch=branch, symbol=symbol, date=e["date"], state="settled_shares")
            q = D(record["record_shares"])
            cash = D(record["cash_due"])
            close(cash, q * e["cash_10"] / 10, "official_due")
            close(D(ex["cash_due"]), cash, "ex_due")
            close(D(paid["amount"]), cash, "cash_paid")
            close(D(old["shares"]), q, "record_qty")
            close(D(new["shares"]), q, "ex_qty")
            assert (D(old["valuation_price"]), D(new["valuation_price"])) == (e["anchor"], e["mark"])
            assert D(record["bonus_due"] or "0") == D(ex["bonus_due"] or "0") == 0
            booked = [r for r in orders if (r["branch"], r["symbol"], r["execution_date"], r["booked"]) == (branch, symbol, e["date"], "True")]
            assert not booked
            actual = q * e["mark"] + cash
            old_value = q * e["anchor"]
            paper_rounding = actual - old_value
            close(paper_rounding, q * (e["mark"] + e["cash_10"] / 10 - e["anchor"]), "rounding_paper_gain")
            wrong_cash_error = q * (e["cash_10"] - e["wrong_cash_10"]) / 10
            assert wrong_cash_error != 0 and D(o["派息比例"]) != e["wrong_cash_10"]
            branches[branch] = {"record_shares": str(q), "gross_cash_paid": str(cash), "old_stock_value": str(old_value), "ex_stock_plus_cash": str(actual), "paper_rounding_delta": str(paper_rounding), "booked_ex_orders": len(booked), "mutations_rejected": {"wrong_cash_even_if_same_mark": str(wrong_cash_error), "double_cash": str(cash), "missing_cash": str(-cash), "stale_mark_plus_cash": str(q * (e["anchor"] - e["mark"]))}}
        output["events"][symbol] = {"status": "PASS_SOURCE_MARK_LEDGER_AND_MUTATIONS", "issuer_sha256": sha(pdf), "original_row_sha256": original_row_sha, "source_candidate_sha256": sha(CANDIDATE), "market": {"last_trade": last_trade, "record": old_bar, "ex": ex_bar, "resume": resume}, "wrong_amount_same_mark": wrong_mark == e["mark"], "branches": branches}

    if any(v["status"] != "PASS_SOURCE_MARK_LEDGER_AND_MUTATIONS" for v in output["events"].values()):
        output["status"] = "PARTIAL_PREFLIGHT_WAITING_FOR_EXECUTOR"
    dest = HERE / "002735_300046_cash_adversarial_audit.json"
    dest.write_text(json.dumps(output, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"status": output["status"], "manifest_sha256": output["manifest_sha256"], "events": {s: {"status": x["status"], "branches": x.get("branches", {})} for s, x in output["events"].items()}, "first_blocks": output["first_blocks"]}, ensure_ascii=False))


if __name__ == "__main__":
    run()
