"""Bounded BaoStock historical ST probe; writes diagnostics only."""

import csv
import hashlib
import json
from collections import Counter
from pathlib import Path

import baostock as bs


HERE = Path(__file__).resolve().parent
OUT = HERE / "public_daily_name_last_probe"
WINDOWS = {
    "sh.600603": ("2004-01-01", "2026-09-24"),
    "sh.600137": ("2006-01-01", "2009-01-01"),
    "sh.600506": ("2009-01-01", "2013-01-01"),
    "sh.600241": ("2020-01-01", "2023-12-31"),
    "sz.002113": ("2010-01-01", "2010-02-01"),
    "sz.000787": ("2010-01-01", "2010-12-31"),
    "sz.000805": ("2010-01-01", "2010-12-31"),
    "sh.600003": ("2010-01-01", "2010-12-31"),
}
FIELDS = "date,code,close,volume,tradestatus,isST"


def save_csv(path, fieldnames, rows):
    with path.open("w", newline="", encoding="utf-8-sig") as fh:
        writer = csv.writer(fh)
        writer.writerow(fieldnames)
        writer.writerows(rows)
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    OUT.mkdir(exist_ok=True)
    login = bs.login()
    if login.error_code != "0":
        raise RuntimeError(f"BaoStock anonymous login failed: {login.error_code}")
    summary = {}
    try:
        for code, (start, end) in WINDOWS.items():
            rs = bs.query_history_k_data_plus(code, FIELDS, start_date=start, end_date=end, frequency="d", adjustflag="3")
            if rs.error_code != "0":
                raise RuntimeError(f"{code}: {rs.error_code} {rs.error_msg}")
            if rs.fields != FIELDS.split(","):
                raise RuntimeError(f"{code}: unexpected fields {rs.fields}")
            rows = []
            while rs.next():
                rows.append(rs.get_row_data())
            path = OUT / f"{code.replace('.', '_')}_daily.csv"
            digest = save_csv(path, rs.fields, rows)
            if rows:
                dates = [r[0] for r in rows]
                if len(dates) != len(set(dates)) or dates != sorted(dates):
                    raise RuntimeError(f"{code}: duplicate or unordered dates")
            transitions = []
            for before, after in zip(rows, rows[1:]):
                if before[5] != after[5]:
                    transitions.append(dict(previous_date=before[0], previous_isST=before[5], date=after[0], isST=after[5]))
            summary[code] = dict(request_start=start, request_end=end, rows=len(rows), first_date=rows[0][0] if rows else None, last_date=rows[-1][0] if rows else None, isST_counts=dict(Counter(r[5] for r in rows)), suspended_rows=sum(r[4] == "0" for r in rows), zero_volume_rows=sum(float(r[3] or 0) <= 0 for r in rows), sha256_csv=digest, transitions=transitions)
        with (OUT / "sh_600603_daily.csv").open(encoding="utf-8-sig", newline="") as fh:
            daily = {r["date"]: r for r in csv.DictReader(fh)}
        for key, target_path in (
            ("600603_2010_2011_old_proxy_targets", HERE.parents[2] / "outputs" / "microcap_top100_mom16_biweekly_live_v2_0_base_proxy_members.csv"),
            ("600603_2010_2011_replay_effective_rebalances", HERE.parent / "full_replay" / "members_by_rebalance.csv"),
            ("600603_2010_2011_effective_bridge", HERE.parent / "corporate_actions" / "replayed_effective_members_by_rebalance.csv"),
        ):
            with target_path.open(encoding="utf-8-sig", newline="") as fh:
                target_dates = [r["rebalance_date"] for r in csv.DictReader(fh) if r["symbol"] == "600603" and "2010-01-01" <= r["rebalance_date"] <= "2011-12-31"]
            matched_rows = [(d, daily[d]["volume"] if d in daily else "", daily[d]["tradestatus"] if d in daily else "", daily[d]["isST"] if d in daily else "") for d in target_dates]
            target_csv = OUT / f"{key}.csv"
            target_digest = save_csv(target_csv, ("date", "volume", "tradestatus", "isST"), matched_rows)
            summary[key] = dict(rows=len(target_dates), unique_dates=len(set(target_dates)), first_date=min(target_dates), last_date=max(target_dates), matched=sum(d in daily for d in target_dates), isST_one=sum(d in daily and daily[d]["isST"] == "1" for d in target_dates), isST_zero=sum(d in daily and daily[d]["isST"] == "0" for d in target_dates), tradestatus_one_positive_volume=sum(d in daily and daily[d]["tradestatus"] == "1" and float(daily[d]["volume"] or 0) > 0 for d in target_dates), missing_dates=[d for d in target_dates if d not in daily], target_source=str(target_path), sha256_csv=target_digest)
        source = HERE / "initial_2010_st" / "two_date_st_status_evidence.csv"
        with source.open(encoding="utf-8-sig", newline="") as fh:
            official = list(csv.DictReader(fh))
        codes = sorted(set(r["symbol"] for r in official))
        initial_bao = {}
        for symbol in codes:
            code = ("sh." if symbol.startswith("6") else "sz.") + symbol
            rs = bs.query_history_k_data_plus(code, FIELDS, start_date="2010-01-04", end_date="2010-01-14", frequency="d", adjustflag="3")
            if rs.error_code != "0" or rs.fields != FIELDS.split(","):
                raise RuntimeError(f"initial {code}: {rs.error_code} {rs.error_msg}")
            while rs.next():
                row = dict(zip(rs.fields, rs.get_row_data()))
                initial_bao[(symbol, row["date"])] = row
        checks = []
        for row in official:
            sym, date = row["symbol"], row["target_date"]
            bao = initial_bao.get((sym, date), {})
            checks.append((sym, date, row["status"], bao.get("isST", ""), bao.get("tradestatus", ""), bao.get("volume", ""), bao.get("close", "")))
        path = OUT / "initial_51_official_vs_baostock.csv"
        digest = save_csv(path, ("symbol", "target_date", "official_status", "isST", "tradestatus", "volume", "close"), checks)
        summary["initial_51_official_crosscheck"] = dict(rows=len(checks), symbols=len(codes), matched=sum(r[3] in ("0", "1") for r in checks), trading_positive_volume=sum(r[4] == "1" and float(r[5] or 0) > 0 for r in checks), agree=sum((r[2] == "proven_ST") == (r[3] == "1") for r in checks if r[3] in ("0", "1")), mismatches=[dict(symbol=r[0], date=r[1], official=r[2], baostock=r[3], tradestatus=r[4], volume=r[5]) for r in checks if r[3] in ("0", "1") and ((r[2] == "proven_ST") != (r[3] == "1"))], sha256_csv=digest, official_source=str(source))
    finally:
        bs.logout()
    (OUT / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    for code, item in summary.items():
        print(code, "rows", item["rows"], "sha", item.get("sha256_csv"), "transitions", len(item.get("transitions", [])))
    print("600603_target_check", summary["600603_2010_2011_old_proxy_targets"])
    print("600603_replay_check", summary["600603_2010_2011_replay_effective_rebalances"])
    print("600603_bridge_check", summary["600603_2010_2011_effective_bridge"])


if __name__ == "__main__":
    main()
