"""Build an isolated daily economic-rights candidate for the verified early segment.

The output is a tax-before-receivable return stream, not an account ledger or
formal strategy performance. Stock and cash availability are separate columns.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[3]
OUT = Path(__file__).resolve().parent
FULL = ROOT / "research_reports/20260926_l1_repair/full_replay"
START = pd.Timestamp("2010-01-05")
END = pd.Timestamp("2014-09-18")


def _read_members() -> tuple[pd.DatetimeIndex, dict[pd.Timestamp, set[str]]]:
    base = pd.read_csv(FULL / "members_by_rebalance.csv", dtype={"symbol": str})
    seed = pd.read_csv(OUT / "initial_2010_01_04_executed_seed.csv", dtype={"symbol": str})
    seed["rebalance_date"] = "2010-01-04"
    members = pd.concat([seed[["rebalance_date", "symbol"]], base[["rebalance_date", "symbol"]]], ignore_index=True)
    members["symbol"] = members["symbol"].str.zfill(6)
    members["rebalance_date"] = pd.to_datetime(members["rebalance_date"])
    members = members.loc[members["rebalance_date"].le(END)]
    grouped = {date: set(rows["symbol"]) for date, rows in members.groupby("rebalance_date")}
    return pd.DatetimeIndex(sorted(grouped)), grouped


def _read_source() -> pd.DataFrame:
    files = [OUT / "cninfo_distribution_events.csv", OUT / "cninfo_distribution_events_initial_seed_extra.csv"]
    source = pd.concat([pd.read_csv(path, dtype={"symbol": str}) for path in files], ignore_index=True)
    source["symbol"] = source["symbol"].str.zfill(6)
    for date in ("实施方案公告日期", "股权登记日", "除权日", "派息日", "股份到账日"):
        source[date] = pd.to_datetime(source[date], errors="coerce")
    for value in ("送股比例", "转增比例", "派息比例"):
        source[value] = pd.to_numeric(source[value], errors="coerce").fillna(0.0)
    source = source.loc[
        source["除权日"].between(START, END)
        & source[["送股比例", "转增比例", "派息比例"]].sum(axis=1).gt(0)
    ].copy()
    if source.duplicated(["symbol", "除权日"]).any():
        raise AssertionError("duplicate CNInfo symbol/ex-date distribution")
    return source


def _sina_matcher() -> tuple[dict[tuple[str, pd.Timestamp], pd.DataFrame], dict[str, str]]:
    path = OUT / "sina_held_2010_2014_distribution_and_rights.csv"
    manifest_path = OUT / "sina_held_2010_2014_source_manifest.json"
    if not path.exists() or not manifest_path.exists():
        return {}, {}
    source = pd.read_csv(path, dtype={"symbol": str})
    source["symbol"] = source["symbol"].str.zfill(6)
    dividends = source.loc[source["indicator"].eq("分红")].copy()
    dividends["ex_date"] = pd.to_datetime(dividends["除权除息日"], errors="coerce")
    pairs = {key: group for key, group in dividends.groupby(["symbol", "ex_date"])}
    status = {row["symbol"]: row["status"] for row in json.loads(manifest_path.read_text(encoding="utf-8"))["per_symbol_indicator"] if row["indicator"] == "分红"}
    return pairs, status


def _raw_prices(symbol: str) -> pd.Series:
    frame = pd.read_csv(ROOT / ".microcap_index_cache/prices_raw" / f"{symbol}.csv", usecols=["date", "close_raw"])
    frame["date"] = pd.to_datetime(frame["date"], errors="coerce")
    frame["close_raw"] = pd.to_numeric(frame["close_raw"], errors="coerce")
    return frame.dropna().sort_values("date").drop_duplicates("date", keep="last").set_index("date")["close_raw"]


def main() -> None:
    rebalance_dates, members = _read_members()
    source = _read_source()
    sina, sina_status = _sina_matcher()
    raw_cache: dict[str, pd.Series] = {}
    rows: list[dict[str, object]] = []
    for _, src in source.sort_values(["除权日", "symbol"]).iterrows():
        symbol = src["symbol"]
        ex_date = src["除权日"]
        record_date = src["股权登记日"]
        ex_prior = rebalance_dates.searchsorted(ex_date, side="left") - 1
        record_prior = rebalance_dates.searchsorted(record_date, side="left") - 1 if pd.notna(record_date) else -1
        held_on_ex_date = bool(ex_prior >= 0 and symbol in members[rebalance_dates[ex_prior]])
        held_before_record = bool(record_prior >= 0 and symbol in members[rebalance_dates[record_prior]])
        if not held_on_ex_date:
            continue
        bonus_per_10 = float(src["送股比例"] + src["转增比例"])
        cash_per_10 = float(src["派息比例"])
        if symbol not in raw_cache:
            raw_cache[symbol] = _raw_prices(symbol)
        price = raw_cache[symbol]
        prior_prices = price.loc[price.index < ex_date]
        prev_trade = prior_prices.index[-1] if not prior_prices.empty else pd.NaT
        prev_price = float(prior_prices.iloc[-1]) if not prior_prices.empty else np.nan
        ex_price = float(price.loc[ex_date]) if ex_date in price.index else np.nan
        valid_price = bool(np.isfinite(prev_price) and prev_price > 0 and np.isfinite(ex_price) and ex_price > 0)
        raw_return = ex_price / prev_price - 1 if valid_price else np.nan
        bonus_delta = (ex_price * bonus_per_10 / 10.0) / prev_price if valid_price else np.nan
        cash_delta = (cash_per_10 / 10.0) / prev_price if valid_price else np.nan
        key = (symbol, ex_date)
        matched = sina.get(key)
        sina_implemented = bool(matched is not None and len(matched.loc[matched["进度"].eq("实施")]) == 1)
        if sina_implemented:
            check = matched.loc[matched["进度"].eq("实施")].iloc[0]
            sina_values = [pd.to_numeric(check.get(col), errors="coerce") for col in ("送股", "转增", "派息")]
            sina_values = [0.0 if pd.isna(value) else float(value) for value in sina_values]
            secondary_match = bool(
                abs(sina_values[0] - float(src["送股比例"])) <= 1e-4
                and abs(sina_values[1] - float(src["转增比例"])) <= 1e-4
                and abs(sina_values[2] - float(src["派息比例"])) <= 1e-4
                and pd.to_datetime(check.get("股权登记日"), errors="coerce") == record_date
            )
        else:
            secondary_match = False
        cash_paid = src["派息日"] if cash_per_10 > 0 else pd.NaT
        stock_credited = src["股份到账日"] if bonus_per_10 > 0 else pd.NaT
        dates_valid = bool(
            pd.notna(record_date) and record_date < ex_date
            and pd.notna(src["实施方案公告日期"]) and src["实施方案公告日期"] <= record_date
            and (cash_per_10 <= 0 or (pd.notna(cash_paid) and cash_paid >= ex_date))
            and (bonus_per_10 <= 0 or (pd.notna(stock_credited) and stock_credited >= ex_date))
        )
        status = (
            "candidate_complete"
            if held_before_record and valid_price and dates_valid and secondary_match
            else "unresolved"
        )
        reasons: list[str] = []
        if not held_before_record:
            reasons.append("not_held_before_record_date")
        if not valid_price:
            reasons.append("missing_raw_price_pair")
        if not dates_valid:
            reasons.append("incomplete_or_misaligned_action_dates")
        if not secondary_match:
            reasons.append("missing_or_disagreeing_secondary_source")
        rows.append({
            "symbol": symbol,
            "ex_date": ex_date,
            "record_date": record_date,
            "announcement_date": src["实施方案公告日期"],
            "cash_paid_date": cash_paid,
            "stock_credited_date": stock_credited,
            "held_on_ex_date": held_on_ex_date,
            "held_before_record_date": held_before_record,
            "bonus_per_10": bonus_per_10,
            "cash_per_10_gross": cash_per_10,
            "previous_trade_date": prev_trade,
            "previous_raw_close": prev_price,
            "ex_date_raw_close": ex_price,
            "old_raw_stock_return": raw_return,
            "bonus_return_delta": bonus_delta,
            "cash_return_delta_gross": cash_delta,
            "economic_stock_return_candidate": raw_return + bonus_delta + cash_delta,
            "proxy_day_delta_candidate": (bonus_delta + cash_delta) / 100.0,
            "secondary_source": "Sina Finance",
            "secondary_fetch_status": sina_status.get(symbol, "missing"),
            "secondary_values_match": secondary_match,
            "status": status,
            "unresolved_reasons": "|".join(reasons),
        })
    ledger = pd.DataFrame(rows)
    if len(ledger) != 300:
        raise AssertionError(f"expected 300 held ex-date events, found {len(ledger)}")
    if ledger.duplicated(["symbol", "ex_date"]).any():
        raise AssertionError("duplicate held ex-date event")
    ledger.to_csv(OUT / "rights_event_ledger_2010_2014.csv", index=False, encoding="utf-8-sig")
    official = pd.read_csv(FULL / "all_daily_returns.csv")
    official["date"] = pd.to_datetime(official["date"])
    official = official.loc[official["date"].between(START, END), ["date", "official_return"]].copy()
    accepted = ledger.loc[ledger["status"].eq("candidate_complete")].copy()
    settlement_rows = []
    for _, event in accepted.iterrows():
        for stage, date, per_10 in (
            ("ex_date_economic_entitlement", event["ex_date"], event["bonus_per_10"] + event["cash_per_10_gross"]),
            ("cash_available_not_new_return", event["cash_paid_date"], event["cash_per_10_gross"]),
            ("bonus_shares_available_not_new_return", event["stock_credited_date"], event["bonus_per_10"]),
        ):
            if pd.notna(date) and per_10 > 0:
                settlement_rows.append({"symbol": event["symbol"], "ex_date": event["ex_date"], "stage_date": date, "stage": stage, "quantity_or_cash_per_10_original_shares": per_10, "new_economic_return_at_stage": stage == "ex_date_economic_entitlement"})
    pd.DataFrame(settlement_rows).sort_values(["stage_date", "symbol", "stage"]).to_csv(
        OUT / "rights_settlement_timeline_2010_2014.csv", index=False, encoding="utf-8-sig"
    )
    daily_delta = accepted.groupby("ex_date").agg(
        accepted_events=("symbol", "size"),
        bonus_events=("bonus_per_10", lambda s: int(s.gt(0).sum())),
        cash_events=("cash_per_10_gross", lambda s: int(s.gt(0).sum())),
        bonus_delta=("bonus_return_delta", lambda s: float(s.sum()) / 100.0),
        cash_delta=("cash_return_delta_gross", lambda s: float(s.sum()) / 100.0),
    ).reset_index().rename(columns={"ex_date": "date"})
    daily = official.merge(daily_delta, on="date", how="left", validate="one_to_one")
    for col in ("accepted_events", "bonus_events", "cash_events", "bonus_delta", "cash_delta"):
        daily[col] = daily[col].fillna(0)
    daily["economic_rights_delta_candidate"] = daily["bonus_delta"] + daily["cash_delta"]
    daily["economic_return_candidate"] = daily["official_return"] + daily["economic_rights_delta_candidate"]
    daily["account_or_cash_reinvestment_validated"] = False
    daily.to_csv(OUT / "daily_economic_rights_candidate_2010_2014.csv", index=False, encoding="utf-8-sig")
    summary = {
        "events_held_on_ex_date": int(len(ledger)),
        "held_before_record_date": int(ledger["held_before_record_date"].sum()),
        "candidate_complete": int(ledger["status"].eq("candidate_complete").sum()),
        "unresolved": int(ledger["status"].eq("unresolved").sum()),
        "unresolved_reasons": ledger.loc[ledger["status"].eq("unresolved"), "unresolved_reasons"].value_counts().to_dict(),
        "daily_rows": int(len(daily)),
        "event_days_with_candidate_delta": int(daily["accepted_events"].gt(0).sum()),
        "first_event_delta": daily.loc[daily["accepted_events"].gt(0)].head(1).astype(str).to_dict("records"),
        "largest_absolute_day_delta": daily.loc[daily["economic_rights_delta_candidate"].abs().idxmax()].astype(str).to_dict(),
        "ledger_sha256": hashlib.sha256((OUT / "rights_event_ledger_2010_2014.csv").read_bytes()).hexdigest(),
        "daily_sha256": hashlib.sha256((OUT / "daily_economic_rights_candidate_2010_2014.csv").read_bytes()).hexdigest(),
    }
    (OUT / "rights_replay_summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
