"""Rebuild three Top100 signal decisions from a new stock-account proxy.

This is a signal/clock diagnostic.  The input stock ledger is a separately
seeded recent account.  This script never reuses the old version signals or
claims that a proxy unit is an executable stock/hedge account.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[3]
HERE = Path(__file__).resolve().parent
ENGINE = ROOT / "research_reports/20261002_l2_full_repair/engine"
OLD_NAV = ROOT / "outputs/microcap_top100_mom16_plain_fixed1_v2_0_costed_nav.csv"
sys.path.insert(0, str(ROOT))

import microcap_top100_mom16_biweekly_live_v2_0 as v20  # noqa: E402
import microcap_top100_mom16_biweekly_live_v2_3 as v23  # noqa: E402
import microcap_top100_mom16_biweekly_live_v2_5 as v25  # noqa: E402


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def load_new_proxy(daily_path: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    daily = pd.read_csv(daily_path, dtype={"date": str})
    required = {"date", "equity", "fees", "stock_value", "cash"}
    missing = required - set(daily.columns)
    if missing:
        raise ValueError(f"stock ledger missing columns: {sorted(missing)}")
    daily["date"] = pd.to_datetime(daily["date"], errors="raise")
    if daily["date"].isna().any() or daily["date"].duplicated().any():
        raise ValueError("stock ledger has missing/duplicate sessions")
    daily = daily.set_index("date").sort_index()
    for col in ["equity", "fees", "stock_value", "cash"]:
        daily[col] = pd.to_numeric(daily[col], errors="raise")
    if not np.isfinite(daily[["equity", "fees", "stock_value", "cash"]].to_numpy()).all():
        raise ValueError("stock ledger has nonfinite account fields")
    if (daily.equity <= 0).any() or (daily.fees < 0).any():
        raise ValueError("stock ledger has nonpositive equity/negative fees")
    if len(daily) < 26:
        raise ValueError("stock ledger has insufficient 25-day warmup")

    # This is the same paid-fee holdings path with current fee removed from
    # each daily step.  It is NOT an independently simulated no-fee account.
    previous = daily.equity.shift(1)
    previous.iloc[0] = 1_000_000.0
    proxy_gross_step = (daily.equity + daily.fees) / previous - 1.0
    if "paper_gross_step" in daily:
        supplied = pd.to_numeric(daily.paper_gross_step, errors="raise")
        if not np.allclose(supplied, proxy_gross_step, rtol=0, atol=1e-10):
            raise ValueError("engine paper_gross_step disagrees with account identity")
    if (1.0 + proxy_gross_step <= 0).any():
        raise ValueError("proxy has nonpositive daily gross factor")
    proxy_close = 1000.0 * (1.0 + proxy_gross_step).cumprod()

    old = pd.read_csv(OLD_NAV, usecols=["date", "hedge_close"])
    old["date"] = pd.to_datetime(old["date"], errors="raise")
    old = old.drop_duplicates("date", keep="last").set_index("date").sort_index()
    if not daily.index.isin(old.index).all():
        missing_dates = daily.index.difference(old.index)
        raise ValueError(f"hedge close missing: {missing_dates[0].date()}")
    hedge = pd.to_numeric(old.loc[daily.index, "hedge_close"], errors="raise")
    if (hedge <= 0).any() or not np.isfinite(hedge).all():
        raise ValueError("hedge close invalid")
    close = pd.DataFrame({"microcap": proxy_close, "hedge": hedge}, index=daily.index)
    return daily, close


def v23_overheat_next(gross: pd.DataFrame) -> pd.Series:
    feature = gross.spread_nav.pct_change(fill_method=None).fillna(0.0)
    feature = feature.rolling(int(v23.OVERHEAT_FEATURE_WINDOW)).std(ddof=1)
    feature *= math.sqrt(float(v23.TRADING_DAYS))
    base_next = gross.next_holding.ne("cash")
    active = False
    risk_off = False
    values = []
    for date in gross.index:
        val = feature.loc[date]
        hot = pd.notna(val) and float(val) >= float(v23.OVERHEAT_TRIGGER_THRESHOLD)
        cool = pd.notna(val) and float(val) <= float(v23.OVERHEAT_RECOVERY_THRESHOLD)
        requested = bool(base_next.loc[date])
        if risk_off:
            if cool:
                desired, risk_off_next = requested, False
            else:
                desired, risk_off_next = False, True
        else:
            desired, risk_off_next = requested, False
            if desired and hot:
                desired, risk_off_next = False, True
        values.append(bool(desired))
        active, risk_off = bool(desired), bool(risk_off_next)
    return pd.Series(values, index=gross.index, dtype=bool)


def next_close_clock(signal_on: pd.Series) -> pd.DataFrame:
    """Signal at T close, position switch at T+1 close; T+2 earns new P&L."""
    idx = signal_on.index
    before = False
    rows = []
    pending: bool | None = None
    pending_date: pd.Timestamp | None = None
    for date in idx:
        before_today = before
        filled = pending is not None and pending != before_today
        # A fill occurs at today's close.  Today's entire close-to-close
        # return belongs to the position held before that close.
        after_today = pending if pending is not None else before_today
        rows.append({
            "date": date,
            "signal_at_close": bool(signal_on.loc[date]),
            "holding_through_close": bool(before_today),
            "order_filled_at_close": bool(filled),
            "filled_signal_date": pending_date.date().isoformat() if filled else "",
            "holding_after_close": bool(after_today),
        })
        before = bool(after_today)
        pending = bool(signal_on.loc[date])
        pending_date = pd.Timestamp(date)
    return pd.DataFrame(rows).set_index("date")


def run(daily_path: Path, branch: str) -> dict:
    daily, close = load_new_proxy(daily_path)
    gross20 = v20_module_signal(close)
    gross23 = v23.build_spread_log_wls_gross(close)
    gross25 = v25.build_microcap_log_wls_gross(close)
    zero_turnover = pd.DataFrame(columns=["rebalance_date", "two_side_cost_rate"])
    scaled20 = v20.overlay_mod.apply_v2_0_execution(gross20, zero_turnover)
    scaled23 = v23.apply_overheat_defense(gross23, zero_turnover)
    costed25 = gross25.copy()
    costed25["return_net"] = costed25["return"]
    costed25["total_cost"] = 0.0
    costed25["overlay_pre_cost_return"] = costed25["return"]
    scaled25 = v25.apply_no_target_vol(costed25)
    scores = {
        "v2.0": (scaled20, pd.to_numeric(gross20.momentum_gap, errors="coerce")),
        "v2.3": (scaled23, pd.to_numeric(gross23.annualized_log_wls_score, errors="coerce")),
        "v2.5": (scaled25, pd.to_numeric(gross25.annualized_log_wls_score, errors="coerce")),
    }
    merged = pd.DataFrame(index=close.index)
    merged["stock_proxy_gross_step"] = close.microcap.pct_change(fill_method=None)
    merged.iloc[0, merged.columns.get_loc("stock_proxy_gross_step")] = close.microcap.iloc[0] / 1000.0 - 1.0
    merged["stock_proxy_net_step"] = daily.equity.pct_change(fill_method=None)
    merged.iloc[0, merged.columns.get_loc("stock_proxy_net_step")] = daily.equity.iloc[0] / 1_000_000.0 - 1.0
    merged["stock_proxy_close"] = close.microcap
    merged["hedge_close"] = close.hedge
    manifest = {"branch": branch, "source_ledger": str(daily_path),
                "source_ledger_sha256": sha256(daily_path), "hedge_source": str(OLD_NAV),
                "hedge_source_sha256": sha256(OLD_NAV), "start": str(close.index.min().date()),
                "end": str(close.index.max().date()), "sessions": len(close),
                "stock_proxy_kind": "separate_recent_seed_fixed_shares_paper_account",
                "gross_step_note": "fee_removed_current_step_of_fee_paid_holdings_path",
                "signal_fill_contract": "T_close_decide_T_plus_1_close_fill_T_plus_2_return",
                "scale_source": "formal_v2_0_apply_v2_0_execution__v2_3_apply_overheat_defense__v2_5_apply_no_target_vol",
                "cost_input_for_scale_only": "empty_turnover_or_zero_cost; formal target-vol disabled in all three versions",
                "versions": {}}
    for version, (scaled, score) in scores.items():
        signal = scaled.next_holding.ne("cash")
        current_scale = pd.to_numeric(scaled.current_execution_scale, errors="raise")
        next_scale = pd.to_numeric(scaled.next_session_actionable_scale, errors="raise")
        if not np.isin(next_scale.to_numpy(), [0.0, 1.0]).all():
            raise ValueError(f"unexpected_dynamic_scale:{version}")
        if not next_scale.astype(bool).equals(signal):
            raise ValueError(f"scale_signal_mismatch:{version}")
        clock = next_close_clock(signal)
        prefix = version.replace(".", "_")
        merged[prefix + "_score"] = score.reindex(merged.index)
        merged[prefix + "_current_execution_scale"] = current_scale.reindex(merged.index)
        merged[prefix + "_next_session_actionable_scale"] = next_scale.reindex(merged.index)
        merged[prefix + "_scale_change_cost"] = pd.to_numeric(scaled.scale_change_cost, errors="raise").reindex(merged.index)
        for col in clock:
            merged[prefix + "_" + col] = clock[col].reindex(merged.index)
        manifest["versions"][version] = {
            "first_signal": str(signal.index.min().date()),
            "last_signal": str(signal.index.max().date()),
            "signal_rows": len(signal),
            "signal_switches": int(signal.ne(signal.shift(1)).sum()) - 1,
            "filled_switches": int(clock.order_filled_at_close.sum()),
            "latest_signal_active": bool(signal.iloc[-1]),
            "latest_held_through_close": bool(clock.holding_through_close.iloc[-1]),
            "scale_values": sorted(set(float(x) for x in next_scale)),
            "scale_change_cost_max": float(pd.to_numeric(scaled.scale_change_cost, errors="raise").max()),
        }
    HERE.mkdir(parents=True, exist_ok=True)
    output = HERE / f"recent_{branch}_rebuilt_signals.csv"
    merged.to_csv(output, index_label="date", encoding="utf-8")
    manifest["output"] = str(output)
    manifest["output_sha256"] = sha256(output)
    (HERE / f"recent_{branch}_signal_manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    return manifest


def v20_module_signal(close: pd.DataFrame) -> pd.DataFrame:
    return v20.base_mod.run_signal(close)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--branch", choices=["prefunded", "netted"], default="prefunded")
    parser.add_argument("--daily", type=Path)
    args = parser.parse_args()
    path = args.daily or ENGINE / f"recent_{args.branch}_daily.csv"
    print(json.dumps(run(path, args.branch), ensure_ascii=False, indent=2))
