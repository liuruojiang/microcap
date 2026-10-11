"""Inspect frozen calendar gate, previous session, and holiday order mutations."""

import csv
import hashlib
import importlib.util
import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
REPO = HERE.parents[2]
ENGINE = ROOT / "engine"
SOURCE = ENGINE / "full_history.py"
PROXY = REPO / "outputs/wind_microcap_top_100_biweekly_thursday_16y_cached.csv"
TRADE_CAL = ROOT / "data/trade_calendar_evidence/baostock_trade_calendar_20100222_20260930.csv"


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def rows(path):
    with path.open(encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def load_engine():
    sys.path.insert(0,str(ENGINE))
    spec = importlib.util.spec_from_file_location("full_history_calendar_adversarial_import",SOURCE)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def main():
    manifest_path = ENGINE/"full_history_result.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert sha(SOURCE) == manifest["code_sha256"]
    for name,wanted in manifest["detail_sha256"].items():
        assert sha(ENGINE/name) == wanted,name
    for name,wanted in manifest["input_sha256"].items():
        assert sha(REPO/name) == wanted,name
    engine = load_engine()
    proxy_dates = [r["date"] for r in rows(PROXY) if r["date"] >= "2010-02-22"]
    trade = {r["calendar_date"]:r["is_trading_day"]=="1" for r in rows(TRADE_CAL)}
    valid = [d for d in engine.verified_calendar() if d >= "2010-02-22"]
    assert valid == [d for d in proxy_dates if trade[d]]
    assert [d for d in proxy_dates if not trade[d]] == ["2018-06-18"]
    assert "2018-06-18" not in valid
    assert valid[valid.index("2018-06-19")-1] == "2018-06-15"
    source = engine.Source(namespace="full")
    assert source.previous_market_day["2018-06-19"] == "2018-06-15"
    assert "2018-06-18" not in source.previous_market_day
    index = proxy_dates.index("2018-06-18")
    assert proxy_dates[index-1:index+2] == ["2018-06-15","2018-06-18","2018-06-19"]
    daily = rows(ENGINE/"full_history_daily_account.csv")
    rights = rows(ENGINE/"full_history_rights_events.csv")
    orders = rows(ENGINE/"full_history_settled_orders.csv")
    actual_bad = [r for r in daily if r["date"] not in valid] + [r for r in rights if r["date"] not in valid] + [r for r in orders if r["signal_date"] not in valid or r["execution_date"] not in valid]
    assert not actual_bad
    fake_order = {"branch":"netted","signal_date":"2018-06-15","execution_date":"2018-06-18","symbol":"600768","side":"sell","booked":"True"}
    fake_right = {"branch":"netted","date":"2018-06-18","phase":"cash_paid","symbol":"600768"}
    assert fake_order["execution_date"] not in valid and fake_right["date"] not in valid
    output = {"status":"PASS_PINNED_TRADE_CALENDAR_AND_HOLIDAY_MUTANTS","manifest_sha256":sha(manifest_path),"code_sha256":sha(SOURCE),"proxy_sha256":sha(PROXY),"trade_calendar_sha256":sha(TRADE_CAL),"first_date_sequence_difference":{"index_after_2010_02_22_zero_based":index,"proxy":"2018-06-18","filtered":"2018-06-19","previous":"2018-06-15"},"previous_market_day_2018_06_19":source.previous_market_day["2018-06-19"],"actual_nontrading_account_right_order_rows":len(actual_bad),"mutants_rejected":{"holiday_execution_order":fake_order,"holiday_corporate_cash_credit":fake_right},"limitations":"Calendar gate prevents historical replay on the holiday. This check does not certify the frozen proxy's own performance path or a broker fill."}
    (HERE/"calendar_engine_mutants_adversarial_audit.json").write_text(json.dumps(output,ensure_ascii=False,indent=2)+"\n",encoding="utf-8")
    print(json.dumps(output,ensure_ascii=False,indent=2))


if __name__ == "__main__":
    main()
