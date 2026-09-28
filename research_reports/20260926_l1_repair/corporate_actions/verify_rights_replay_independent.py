"""Independent, read-only adversarial check of the early rights replay."""

from bisect import bisect_left
from pathlib import Path

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[3]
HERE = Path(__file__).resolve().parent
FULL = HERE.parent / "full_replay"
START = pd.Timestamp("2010-01-05")
END = pd.Timestamp("2014-09-18")


def norm_date(frame, fields):
    for field in fields:
        frame[field] = pd.to_datetime(frame[field], errors="coerce")
    return frame


def member_at(day, dates, positions):
    index = bisect_left(dates, day) - 1
    return positions[dates[index]] if index >= 0 else set()


def main():
    snapshot = pd.read_csv(FULL / "members_by_rebalance.csv", dtype={"symbol": str})
    seed = pd.read_csv(HERE / "initial_2010_01_04_executed_seed.csv", dtype={"symbol": str})
    seed["rebalance_date"] = "2010-01-04"
    snapshot = pd.concat([seed[["rebalance_date", "symbol"]], snapshot[["rebalance_date", "symbol"]]])
    snapshot = norm_date(snapshot, ["rebalance_date"])
    snapshot["symbol"] = snapshot["symbol"].str.zfill(6)
    snapshot = snapshot[snapshot.rebalance_date.le(END)]
    positions = {date: set(group.symbol) for date, group in snapshot.groupby("rebalance_date")}
    dates = sorted(positions)

    actions = pd.concat([
        pd.read_csv(HERE / "cninfo_distribution_events.csv", dtype={"symbol": str}),
        pd.read_csv(HERE / "cninfo_distribution_events_initial_seed_extra.csv", dtype={"symbol": str}),
    ], ignore_index=True)
    actions["symbol"] = actions.symbol.str.zfill(6)
    actions = norm_date(actions, ["股权登记日", "除权日", "派息日", "股份到账日", "实施方案公告日期"])
    for field in ["送股比例", "转增比例", "派息比例"]:
        actions[field] = pd.to_numeric(actions[field], errors="coerce").fillna(0)
    actions = actions[actions["除权日"].between(START, END)]
    actions = actions[(actions[["送股比例", "转增比例", "派息比例"]] > 0).any(axis=1)]
    assert not actions.duplicated(["symbol", "除权日"]).any()

    sina = pd.read_csv(HERE / "sina_held_2010_2014_distribution_and_rights.csv", dtype={"symbol": str})
    sina["symbol"] = sina.symbol.str.zfill(6)
    sina = sina[sina.indicator.eq("分红") & sina["进度"].eq("实施")].copy()
    sina = norm_date(sina, ["除权除息日", "股权登记日"])
    sina_lookup = {(symbol, date): group for (symbol, date), group in sina.groupby(["symbol", "除权除息日"])}

    cache = {}
    rebuilt = []
    for row in actions.itertuples(index=False):
        symbol = row.symbol
        ex = getattr(row, "除权日")
        record = getattr(row, "股权登记日")
        if symbol not in member_at(ex, dates, positions):
            continue
        before_record = symbol in member_at(record, dates, positions)
        if symbol not in cache:
            raw = pd.read_csv(ROOT / ".microcap_index_cache" / "prices_raw" / f"{symbol}.csv")
            raw.date = pd.to_datetime(raw.date)
            raw.close_raw = pd.to_numeric(raw.close_raw, errors="coerce")
            cache[symbol] = raw.dropna(subset=["date", "close_raw"]).sort_values("date").drop_duplicates("date", keep="last").set_index("date").close_raw
        prices = cache[symbol]
        prior = prices.loc[prices.index < ex]
        prior_price = float(prior.iloc[-1]) if len(prior) else np.nan
        ex_price = float(prices.loc[ex]) if ex in prices.index else np.nan
        price_ok = bool(np.isfinite(prior_price) and prior_price > 0 and np.isfinite(ex_price) and ex_price > 0)
        bonus = float(getattr(row, "送股比例") + getattr(row, "转增比例"))
        cash = float(getattr(row, "派息比例"))
        match = sina_lookup.get((symbol, ex))
        source_ok = False
        if match is not None and len(match) == 1:
            other = match.iloc[0]
            other_values = [pd.to_numeric(other[field], errors="coerce") for field in ["送股", "转增", "派息"]]
            other_values = [float(v) if pd.notna(v) else 0.0 for v in other_values]
            source_ok = bool(
                pd.Timestamp(other["股权登记日"]) == record
                and np.allclose(other_values, [float(getattr(row, "送股比例")), float(getattr(row, "转增比例")), cash], atol=1e-4, rtol=0)
            )
        dates_ok = bool(
            pd.notna(record) and record < ex
            and pd.notna(getattr(row, "实施方案公告日期")) and getattr(row, "实施方案公告日期") <= record
            and (cash == 0 or (pd.notna(getattr(row, "派息日")) and getattr(row, "派息日") >= ex))
            and (bonus == 0 or (pd.notna(getattr(row, "股份到账日")) and getattr(row, "股份到账日") >= ex))
        )
        accepted = before_record and price_ok and source_ok and dates_ok
        bonus_delta = ex_price * bonus / (10 * prior_price) if price_ok else np.nan
        cash_delta = cash / (10 * prior_price) if price_ok else np.nan
        rebuilt.append({
            "symbol": symbol, "ex_date": ex, "record_date": record, "held_before_record": before_record,
            "price_ok": price_ok, "source_ok": source_ok, "dates_ok": dates_ok, "accepted": accepted,
            "bonus": bonus, "cash": cash, "prior_price": prior_price, "ex_price": ex_price,
            "bonus_delta": bonus_delta, "cash_delta": cash_delta,
            "cash_paid": getattr(row, "派息日"), "stock_credited": getattr(row, "股份到账日"),
        })
    result = pd.DataFrame(rebuilt).sort_values(["ex_date", "symbol"])
    ledger = pd.read_csv(HERE / "rights_event_ledger_2010_2014.csv", dtype={"symbol": str})
    ledger.symbol = ledger.symbol.str.zfill(6)
    ledger.ex_date = pd.to_datetime(ledger.ex_date)
    compare = result.merge(ledger, on=["symbol", "ex_date"], validate="one_to_one")
    assert len(result) == len(ledger) == len(compare) == 300
    assert (compare.accepted == compare.status.eq("candidate_complete")).all()
    for independent, candidate in [("bonus_delta", "bonus_return_delta"), ("cash_delta", "cash_return_delta_gross")]:
        assert np.allclose(compare[independent], compare[candidate], atol=1e-12, rtol=0, equal_nan=True)
    assert int(result.accepted.sum()) == 291
    assert int((~result.price_ok & result.held_before_record).sum()) == 8
    assert int((~result.held_before_record).sum()) == 1
    good = result[result.accepted]
    assert int(good.bonus.gt(0).sum()) == 98
    assert int(good.cash.gt(0).sum()) == 279
    assert int((good.bonus.gt(0) & good.cash.gt(0)).sum()) == 86

    independent_daily = good.assign(delta=(good.bonus_delta + good.cash_delta) / 100).groupby("ex_date").delta.sum()
    daily = pd.read_csv(HERE / "daily_economic_rights_candidate_2010_2014.csv")
    daily.date = pd.to_datetime(daily.date)
    proxy = pd.read_csv(ROOT / "outputs" / "wind_microcap_top_100_biweekly_thursday_16y_cached.csv")
    proxy.date = pd.to_datetime(proxy.date)
    proxy = proxy[proxy.date.between(START, END)]
    check = daily.merge(proxy[["date", "daily_return"]], on="date", validate="one_to_one")
    assert len(check) == 1142
    assert np.allclose(check.official_return, check.daily_return, atol=1e-12, rtol=0)
    expected = check.date.map(independent_daily).fillna(0)
    assert np.allclose(expected, check.economic_rights_delta_candidate, atol=1e-12, rtol=0)
    assert np.allclose(check.official_return + expected, check.economic_return_candidate, atol=1e-12, rtol=0)

    timeline = pd.read_csv(HERE / "rights_settlement_timeline_2010_2014.csv", dtype={"symbol": str})
    timeline.symbol = timeline.symbol.str.zfill(6)
    timeline.ex_date = pd.to_datetime(timeline.ex_date)
    timeline.stage_date = pd.to_datetime(timeline.stage_date)
    stage_count = timeline.stage.value_counts().to_dict()
    assert stage_count == {
        "ex_date_economic_entitlement": 291,
        "cash_available_not_new_return": 279,
        "bonus_shares_available_not_new_return": 98,
    }
    assert timeline.new_economic_return_at_stage.sum() == 291
    assert not timeline.duplicated(["symbol", "ex_date", "stage"]).any()
    assert (timeline.new_economic_return_at_stage == timeline.stage.eq("ex_date_economic_entitlement")).all()
    expected_stages = []
    for action in good.itertuples():
        expected_stages.append((action.symbol, action.ex_date, action.ex_date,
                                "ex_date_economic_entitlement", action.bonus + action.cash, True))
        if action.cash > 0:
            expected_stages.append((action.symbol, action.ex_date, action.cash_paid,
                                    "cash_available_not_new_return", action.cash, False))
        if action.bonus > 0:
            expected_stages.append((action.symbol, action.ex_date, action.stock_credited,
                                    "bonus_shares_available_not_new_return", action.bonus, False))
    actual_stages = set((row.symbol, row.ex_date, row.stage_date, row.stage,
                         round(float(row.quantity_or_cash_per_10_original_shares), 12), bool(row.new_economic_return_at_stage))
                        for row in timeline.itertuples())
    assert len(actual_stages) == len(expected_stages) == 668
    expected_stages = set((sym, ex, date, stage, round(float(quantity), 12), is_new)
                          for sym, ex, date, stage, quantity, is_new in expected_stages)
    assert actual_stages == expected_stages
    assert int(((good.cash > 0) & (good.cash_paid > good.ex_date)).sum()) == 27
    assert int(((good.bonus > 0) & (good.stock_credited > good.ex_date)).sum()) == 5

    rights = pd.read_csv(HERE / "sina_all_held_rights_2010_2014_distribution_and_rights.csv", dtype={"symbol": str})
    rights.symbol = rights.symbol.str.zfill(6)
    rights = rights[rights.indicator.eq("配股")].copy()
    rights["除权日"] = pd.to_datetime(rights["除权日"], errors="coerce")
    rights["股权登记日"] = pd.to_datetime(rights["股权登记日"], errors="coerce")
    rights = rights[rights["除权日"].between(START, END)]
    rights = rights[rights["配股方案"].notna() & rights["股权登记日"].notna()]
    right_checks = [(r.symbol, str(r["除权日"].date()), r.symbol in member_at(r["股权登记日"], dates, positions), r.symbol in member_at(r["除权日"], dates, positions)) for _, r in rights.iterrows()]

    sample = good.sample(n=10, random_state=20260926).sort_values(["ex_date", "symbol"])
    print("classes", len(result), int(result.held_before_record.sum()), int(result.accepted.sum()), int((~result.price_ok & result.held_before_record).sum()), int((~result.held_before_record).sum()))
    print("candidate mix", int(good.bonus.gt(0).sum()), int(good.cash.gt(0).sum()), int((good.bonus.gt(0) & good.cash.gt(0)).sum()))
    print("first", independent_daily.index.min().date(), "max", independent_daily.idxmax().date(), independent_daily.max(), "days", len(independent_daily))
    print("settlement", stage_count, "cash_late", int(((good.cash > 0) & (good.cash_paid > good.ex_date)).sum()), "shares_late", int(((good.bonus > 0) & (good.stock_credited > good.ex_date)).sum()))
    print("rights", right_checks)
    print("random sample", [(r.symbol, str(r.ex_date.date()), round((r.bonus_delta + r.cash_delta) / 100, 12)) for r in sample.itertuples()])


if __name__ == "__main__":
    main()
