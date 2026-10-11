"""Independent, frozen-input v2.3 ON/OFF account check; diagnostic only.

Uses the saved base signal/gross rows and the separate turnover event table.
It does not import v2.3 accounting, cost mapping, or performance helpers.
"""

from __future__ import annotations

import hashlib
import json
from bisect import bisect_left, bisect_right
from pathlib import Path

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[3]
HERE = Path(__file__).resolve().parent
OLD_NAV = ROOT / "outputs/microcap_top100_mom16_lb25_hl2p5_r2off_eb0p08_vol10_oh26_recovery20_exec0p8_v2_3_costed_nav.csv"
TURNOVER = ROOT / "outputs/microcap_top100_mom16_biweekly_live_v2_0_base_proxy_turnover.csv"
ENTRY = EXIT = 0.003
TRADING_DAYS = 244


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def metrics(frame: pd.DataFrame, col: str) -> list[dict[str, object]]:
    end = frame["date"].iloc[-1]
    start = frame["date"].iloc[0]
    output = []
    for label, years in (("full", None), ("last_10y", 10), ("last_5y", 5), ("last_3y", 3), ("last_1y", 1)):
        boundary = start if years is None else end - pd.DateOffset(years=years)
        if start > boundary:
            output.append({"window": label, "annual_pct": None, "max_drawdown_pct": None,
                           "reason": f"insufficient history: {start.date()} > {boundary.date()}"})
            continue
        part = frame.loc[frame["date"] >= boundary]
        ret = part[col].to_numpy(float)
        wealth = np.cumprod(1.0 + ret)
        peaks = np.maximum(1.0, np.maximum.accumulate(wealth))
        output.append({
            "window": label,
            "start_date": str(part["date"].iloc[0].date()),
            "end_date": str(part["date"].iloc[-1].date()),
            "days": len(ret),
            "annual_pct": float(((wealth[-1] ** (TRADING_DAYS / len(ret))) - 1.0) * 100.0),
            "max_drawdown_pct": float(np.min(wealth / peaks - 1.0) * 100.0),
        })
    return output


def first_index(mask: np.ndarray) -> int | None:
    locations = np.flatnonzero(mask)
    return int(locations[0]) if len(locations) else None


def main() -> None:
    frozen = pd.read_csv(OLD_NAV, parse_dates=["date"]).sort_values("date").reset_index(drop=True)
    turns = pd.read_csv(TURNOVER)
    assert frozen["date"].is_unique and frozen["date"].is_monotonic_increasing
    dates = list(frozen["date"])
    daily_rebalance = np.zeros(len(frozen), dtype=float)
    for row in turns.itertuples(index=False):
        when = pd.Timestamp(row.execution_date)
        slot = bisect_left(dates, when) if row.execution_timing == "close" else bisect_right(dates, when)
        if slot < len(dates):
            daily_rebalance[slot] += float(row.two_side_cost_rate)

    gross = frozen["return_raw"].to_numpy(float)
    base_next = frozen["base_next_holding"].ne("cash").to_numpy(bool)
    base_current = frozen["base_holding"].ne("cash").to_numpy(bool)
    hotness = frozen["overheat_feature_value"].to_numpy(float)
    output = {}
    spread = frozen["spread_nav"].astype(float)
    independently_derived_vol = spread.pct_change(fill_method=None).fillna(0.0).rolling(10).std(ddof=1).to_numpy(float) * np.sqrt(TRADING_DAYS)
    comparable_vol = np.isfinite(independently_derived_vol) & np.isfinite(hotness)
    output["input_independence_checks"] = {
        "vol10_finite_rows": int(np.sum(comparable_vol)),
        "vol10_max_abs_delta": float(np.max(np.abs(independently_derived_vol[comparable_vol] - hotness[comparable_vol]))),
        "vol10_finite_mask_mismatch": int(np.sum(np.isfinite(independently_derived_vol) != np.isfinite(hotness))),
    }
    microcap = frozen["microcap_ret"].to_numpy(float)
    hedge = frozen["hedge_ret"].to_numpy(float)
    drag = frozen["futures_drag"].to_numpy(float)
    reconstructed_gross = np.where(base_current, np.nan_to_num(microcap) - 0.8 * np.nan_to_num(hedge) - drag, 0.0)
    output["input_independence_checks"]["gross_from_legs_max_abs_delta"] = float(np.max(np.abs(reconstructed_gross - gross)))
    output["input_independence_checks"]["base_holding_lag_mismatch"] = int(np.sum(base_current[1:] != base_next[:-1]))
    for mode in ("on", "off"):
        current = bool(base_current[0])
        defensive = False
        nav = 1.0
        holdings, nexts, costs, entry_exits, rebalances, net_returns, navs = ([] for _ in range(7))
        exits, reentries, blocks, risk_flags = ([] for _ in range(4))
        for i in range(len(frozen)):
            want = bool(base_next[i])
            exit_event = reentry_event = block_event = False
            was_defensive = defensive
            if mode == "on":
                hot = np.isfinite(hotness[i]) and hotness[i] >= 0.26
                cool = np.isfinite(hotness[i]) and hotness[i] <= 0.20
                if defensive:
                    if cool:
                        defensive = False
                        reentry_event = want
                    else:
                        want = False
                elif want and hot:
                    if current:
                        exit_event = True
                    else:
                        block_event = True
                    want = False
                    defensive = True
            paid_entry_exit = ENTRY if not current and want else EXIT if current and not want else 0.0
            paid_rebalance = daily_rebalance[i] if current and want else 0.0
            paid = paid_entry_exit + paid_rebalance
            realized = gross[i] if current else 0.0
            daily_net = (1.0 + realized) * (1.0 - paid) - 1.0
            nav *= 1.0 + daily_net
            holdings.append(current)
            nexts.append(want)
            entry_exits.append(paid_entry_exit)
            rebalances.append(paid_rebalance)
            costs.append(paid)
            net_returns.append(daily_net)
            navs.append(nav)
            exits.append(exit_event)
            reentries.append(reentry_event)
            blocks.append(block_event)
            risk_flags.append(was_defensive)
            current = want

        result = frozen[["date"]].copy()
        result["holding_active"] = holdings
        result["next_active"] = nexts
        result["entry_exit_cost"] = entry_exits
        result["rebalance_cost"] = rebalances
        result["total_cost"] = costs
        result["return_net"] = net_returns
        result["nav_net"] = navs
        result["overheat_exit_triggered"] = exits
        result["overheat_reentry_triggered"] = reentries
        result["overheat_block_entry_triggered"] = blocks
        result["overheat_risk_off"] = risk_flags
        result.to_csv(HERE / f"{mode}_independent_account.csv", index=False, encoding="utf-8-sig")
        output[mode] = {
            "rows": len(result), "first": str(result.date.iloc[0].date()), "last": str(result.date.iloc[-1].date()),
            "entry_events": int(np.sum((~np.asarray(holdings)) & np.asarray(nexts))),
            "exit_events": int(np.sum(np.asarray(holdings) & (~np.asarray(nexts)))),
            "rebalance_cost_days": int(np.sum(np.asarray(rebalances) > 0)),
            "sum_cost_rates": float(np.sum(costs)),
            "final_nav": float(navs[-1]), "cash_holding_days": int(np.sum(~np.asarray(holdings))),
            "cash_holding_nonzero_gross_days": int(np.sum((~np.asarray(holdings)) & (np.abs(gross) > 1e-13))),
            "overheat_exit_events": int(np.sum(exits)),
            "overheat_reentry_events": int(np.sum(reentries)),
            "overheat_block_events": int(np.sum(blocks)),
            "metrics": metrics(result, "return_net"),
        }
        if mode == "on":
            comparisons = {}
            for col in ("entry_exit_cost", "rebalance_cost", "total_cost", "return_net", "nav_net",
                        "overheat_exit_triggered", "overheat_reentry_triggered", "overheat_block_entry_triggered", "overheat_risk_off"):
                actual = result[col].to_numpy()
                official = frozen[col].to_numpy()
                if actual.dtype == bool:
                    mismatch = actual != official
                    max_delta = int(np.sum(mismatch))
                else:
                    delta = np.abs(actual.astype(float) - official.astype(float))
                    mismatch = delta > 2e-10
                    max_delta = float(np.nanmax(delta))
                fi = first_index(mismatch)
                comparisons[col] = {"mismatch_count": int(np.sum(mismatch)), "max_abs_delta": max_delta,
                                    "first_mismatch_date": str(dates[fi].date()) if fi is not None else None}
            for name, col in (("holding_active", "holding"), ("next_active", "next_holding")):
                mismatch = result[name].to_numpy(bool) != frozen[col].ne("cash").to_numpy(bool)
                fi = first_index(mismatch)
                comparisons[name] = {"mismatch_count": int(np.sum(mismatch)),
                                     "first_mismatch_date": str(dates[fi].date()) if fi is not None else None}
            output[mode]["official_parity"] = comparisons

        # Reconstruct each day from executed holding and costs, not the stored net-return column.
        expected = np.where(np.asarray(holdings), gross, 0.0)
        recomputed = (1.0 + expected) * (1.0 - np.asarray(costs)) - 1.0
        output[mode]["account_identity_max_abs"] = float(np.max(np.abs(recomputed - np.asarray(net_returns))))
        output[mode]["nav_cumprod_max_abs"] = float(np.max(np.abs(np.cumprod(1.0 + np.asarray(net_returns)) - np.asarray(navs))))
        output[mode]["cash_day_max_abs_net_minus_cost"] = float(np.max(np.abs(np.asarray(net_returns)[~np.asarray(holdings)] + np.asarray(costs)[~np.asarray(holdings)])))
        output[mode]["negative_fee_days"] = int(np.sum(np.asarray(costs) < 0))

    on = pd.read_csv(HERE / "on_independent_account.csv")
    off = pd.read_csv(HERE / "off_independent_account.csv")
    on_first = first_index(np.abs(on["return_net"].to_numpy() - off["return_net"].to_numpy()) > 1e-12)
    output["first_on_off_return_divergence"] = str(dates[on_first].date()) if on_first is not None else None
    output["first_on_off_state_divergence"] = str(dates[first_index(on.holding_active.to_numpy() != off.holding_active.to_numpy())].date())
    next_first = first_index(on.next_active.to_numpy() != off.next_active.to_numpy())
    output["first_on_off_next_state_divergence"] = str(dates[next_first].date()) if next_first is not None else None
    output["input_hashes"] = {"old_official_on_csv_sha256": digest(OLD_NAV), "turnover_csv_sha256": digest(TURNOVER)}
    # Fault injections: omission and double charging of a real, positive fee.
    fee_idx = first_index(on["total_cost"].to_numpy() > 0)
    baseline = on["nav_net"].to_numpy(float)
    if fee_idx is not None:
        old_cost = float(on.loc[fee_idx, "total_cost"])
        old_return = float(on.loc[fee_idx, "return_net"])
        realized = gross[fee_idx] if bool(on.loc[fee_idx, "holding_active"]) else 0.0
        omit = (1.0 + realized) - 1.0
        duplicate = (1.0 + realized) * (1.0 - 2.0 * old_cost) - 1.0
        output["injections"] = {
            "fee_date": str(dates[fee_idx].date()), "fee_rate": old_cost,
            "omit_first_return_abs_delta": abs(omit - old_return),
            "duplicate_first_return_abs_delta": abs(duplicate - old_return),
            "omit_final_nav": float(baseline[-1] * (1.0 + omit) / (1.0 + old_return)),
            "duplicate_final_nav": float(baseline[-1] * (1.0 + duplicate) / (1.0 + old_return)),
            "correct_final_nav": float(baseline[-1]),
        }
    full = output["on"]["metrics"][0]
    output["injections"]["wrong_annual_252_pct"] = float((baseline[-1] ** (252.0 / len(on)) - 1.0) * 100.0)
    output["injections"]["correct_annual_244_pct"] = full["annual_pct"]
    output["injections"]["wrong_minus_correct_annual_pp"] = output["injections"]["wrong_annual_252_pct"] - full["annual_pct"]
    output["injections"]["off_by_one_denominator_annual_pct"] = float((baseline[-1] ** (TRADING_DAYS / (len(on) - 1)) - 1.0) * 100.0)
    output["injections"]["off_by_one_denominator_minus_correct_pp"] = output["injections"]["off_by_one_denominator_annual_pct"] - full["annual_pct"]
    (HERE / "independent_account_audit.json").write_text(json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(output, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
