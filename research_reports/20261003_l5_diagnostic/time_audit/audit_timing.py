"""Independent chronology and branch-proxy audit for L5 diagnostic accounts.

Reads frozen L2/L4 CSVs only. Does not import strategy or replay helpers.
"""

from __future__ import annotations

import hashlib
import json
import math
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[3]
HERE = Path(__file__).resolve().parent
L2 = ROOT / "research_reports/20261002_l2_full_repair/version_integration"
L5_EXEC = ROOT / "research_reports/20261003_l5_diagnostic/executor"
OLD = ROOT / "outputs/microcap_top100_mom16_lb25_hl2p5_r2off_eb0p08_vol10_oh26_recovery20_exec0p8_v2_3_costed_nav.csv"


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def bool_value(value: object) -> bool:
    if pd.isna(value):
        raise ValueError("missing boolean")
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        if value not in (0, 1):
            raise ValueError(f"bad boolean {value}")
        return bool(value)
    if str(value).lower() in ("true", "1"):
        return True
    if str(value).lower() in ("false", "0"):
        return False
    raise ValueError(f"bad boolean {value}")


def branch_audit(branch: str) -> tuple[dict, pd.DataFrame]:
    signal_path = L2 / f"recent_{branch}_rebuilt_signals.csv"
    daily_path = L2 / f"recent_{branch}_v2_3_strategy_daily.csv"
    order_path = L2 / f"recent_{branch}_v2_3_strategy_orders.csv"
    signal_manifest = json.loads((L2 / f"recent_{branch}_signal_manifest.json").read_text(encoding="utf-8"))
    account_manifest = json.loads((L2 / f"recent_{branch}_v2_3_strategy_manifest.json").read_text(encoding="utf-8"))
    s = pd.read_csv(signal_path, dtype={"date": str})
    d = pd.read_csv(daily_path, dtype={"date": str})
    o = pd.read_csv(order_path, dtype={"signal_date": str, "execution_date": str, "symbol": str})
    assert len(s) == len(d) == 241 and s.date.tolist() == d.date.tolist()
    assert s.date.is_unique and s.date.tolist() == sorted(s.date.tolist())
    assert sha256(signal_path) == signal_manifest["output_sha256"] == account_manifest["source_signal_sha256"]
    assert sha256(daily_path) == account_manifest["detail_sha256"]["daily"]
    assert sha256(order_path) == account_manifest["detail_sha256"]["orders"]
    dates = s.date.tolist()
    next_date = dict(zip(dates[:-1], dates[1:]))
    actual_bad_order_dates = [
        (r.signal_date, r.execution_date, r.symbol)
        for r in o.itertuples(index=False)
        if next_date.get(r.signal_date) != r.execution_date
    ]
    assert not actual_bad_order_dates, actual_bad_order_dates[:3]
    attempts = o.groupby("execution_date").size()
    assert all(int(row.executed_orders) == int(attempts.get(row.date, 0)) for row in d.itertuples(index=False))
    for row in s.itertuples(index=False):
        scale = row.v2_3_next_session_actionable_scale
        if pd.isna(scale):
            continue
        sig = bool_value(row.v2_3_signal_at_close)
        assert float(scale) == int(sig)
        day = d[d.date == row.date].iloc[0]
        assert bool_value(day.signal_at_close) == sig
        assert float(day.formal_next_scale) == float(scale)
    clock_errors = []
    same_day_injected_errors = []
    future_injected_errors = []
    for i in range(1, len(s)):
        prev = s.iloc[i - 1]
        cur = s.iloc[i]
        if pd.isna(prev.v2_3_signal_at_close):
            continue
        previous_signal = bool_value(prev.v2_3_signal_at_close)
        after = bool_value(cur.v2_3_holding_after_close)
        if previous_signal != after:
            clock_errors.append(cur.date)
        before = bool_value(cur.v2_3_holding_through_close)
        previous_after = bool_value(prev.v2_3_holding_after_close)
        changed_at_close = before != after
        if before != previous_after or bool_value(cur.v2_3_order_filled_at_close) != changed_at_close:
            clock_errors.append(cur.date)
        filled_signal_date = cur.v2_3_filled_signal_date
        if changed_at_close and filled_signal_date != prev.date:
            clock_errors.append(cur.date)
        if not changed_at_close and not pd.isna(filled_signal_date):
            clock_errors.append(cur.date)
        if not pd.isna(cur.v2_3_signal_at_close):
            if bool_value(cur.v2_3_signal_at_close) != after:
                same_day_injected_errors.append(cur.date)
        if i + 1 < len(s) and not pd.isna(s.iloc[i + 1].v2_3_signal_at_close):
            if bool_value(s.iloc[i + 1].v2_3_signal_at_close) != after:
                future_injected_errors.append(cur.date)
    assert not clock_errors, clock_errors[:3]
    assert same_day_injected_errors and future_injected_errors
    stock_state_gaps = [
        {"date": row.date, "model_holding_after_close": bool_value(row.v2_3_holding_after_close),
         "actual_held_symbols": int(d.iloc[i].held_count)}
        for i, row in enumerate(s.itertuples(index=False))
        if not pd.isna(row.v2_3_holding_after_close)
        and (bool_value(row.v2_3_holding_after_close) != (int(d.iloc[i].held_count) > 0))
    ]
    micro = pd.to_numeric(s.stock_proxy_close, errors="raise")
    hedge = pd.to_numeric(s.hedge_close, errors="raise")
    spread_step = micro.pct_change(fill_method=None).fillna(0) - hedge.pct_change(fill_method=None).fillna(0) - 0.0003
    spread = (1 + spread_step).cumprod()
    x = list(range(25))
    weights = [0.5 ** ((24 - j) / 2.5) for j in x]
    weights = [w / sum(weights) for w in weights]
    xbar = sum(w * j for w, j in zip(weights, x))
    denom = sum(w * (j - xbar) ** 2 for w, j in zip(weights, x))
    log_spread = [math.log(float(value)) for value in spread]
    scores = [float("nan")] * len(s)
    for i in range(24, len(s)):
        window = log_spread[i - 24:i + 1]
        ybar = sum(w * y for w, y in zip(weights, window))
        scores[i] = 244 * sum(w * (j - xbar) * (y - ybar)
                               for w, j, y in zip(weights, x, window)) / denom
    score_diff = [abs(scores[i] - float(s.at[i, "v2_3_score"]))
                  for i in s.index[s.v2_3_score.notna()]]
    max_score_diff = max(score_diff)
    assert max_score_diff < 1e-10
    valid = s.v2_3_signal_at_close.notna()
    valid_spread = spread.loc[valid]
    feature_valid = valid_spread.pct_change(fill_method=None).fillna(0).rolling(10).std(ddof=1) * math.sqrt(244)
    feature = feature_valid.reindex(s.index)
    core_on = False
    risk_off = False
    predicted = []
    core_targets = []
    for i in s.index[valid]:
        score = float(s.at[i, "v2_3_score"])
        core_on = score >= -0.08 if core_on else score > 0.0
        core_targets.append(core_on)
        fv = feature.loc[i]
        if risk_off:
            if pd.notna(fv) and fv <= 0.20:
                risk_off = False
                desired = core_on
            else:
                desired = False
        else:
            desired = core_on
            if desired and pd.notna(fv) and fv >= 0.26:
                risk_off = True
                desired = False
        predicted.append(desired)
    observed = [bool_value(x) for x in s.loc[valid, "v2_3_signal_at_close"]]
    assert predicted == observed
    sep = {}
    for day in ("2026-09-07", "2026-09-11", "2026-09-23", "2026-09-24", "2026-09-28", "2026-09-29", "2026-09-30"):
        i = dates.index(day)
        sr, dr = s.iloc[i], d.iloc[i]
        sep[day] = {
            "proxy_close": float(sr.stock_proxy_close),
            "score": float(sr.v2_3_score),
            "vol10_independent": float(feature.iloc[i]),
            "signal_at_close": bool_value(sr.v2_3_signal_at_close),
            "model_holding_through_close": bool_value(sr.v2_3_holding_through_close),
            "model_holding_after_close": bool_value(sr.v2_3_holding_after_close),
            "actual_held_symbols": int(dr.held_count),
            "pending_orders": int(dr.pending_orders),
            "executed_orders": int(dr.executed_orders),
        }
    s["independent_overheat_feature"] = feature
    return {
        "branch": branch,
        "inputs_sha256": {str(p.relative_to(ROOT)): sha256(p) for p in (signal_path, daily_path, order_path)},
        "sessions": len(s), "start": dates[0], "end": dates[-1],
        "signal_to_next_close_clock_errors": len(clock_errors),
        "stock_order_bad_next_session_dates": len(actual_bad_order_dates),
        "attempted_order_daily_count_errors": 0,
        "same_day_fill_injection_detected_rows": len(same_day_injected_errors),
        "future_signal_injection_detected_rows": len(future_injected_errors),
        "independent_score_overheat_signal_errors": 0,
        "independent_wls_score_max_abs_error": max_score_diff,
        "core_targets_long_days": sum(core_targets),
        "overheat_rejected_core_long_days": sum(a and not b for a, b in zip(core_targets, predicted)),
        "model_vs_actual_binary_stock_state_gaps": stock_state_gaps,
        "september": sep,
        "source_proxy_ledger": signal_manifest["source_ledger"],
        "source_proxy_ledger_sha256": signal_manifest["source_ledger_sha256"],
        "hedge_close_source": signal_manifest["hedge_source"],
        "hedge_close_source_sha256": signal_manifest["hedge_source_sha256"],
    }, s


def main() -> None:
    pref, ps = branch_audit("prefunded")
    net, ns = branch_audit("netted")
    p_mask = ps.v2_3_signal_at_close.notna()
    n_mask = ns.v2_3_signal_at_close.notna()
    overlap = p_mask & n_mask
    branch_diff = ps.loc[overlap, "v2_3_signal_at_close"].map(bool_value).reset_index(drop=True).ne(
        ns.loc[overlap, "v2_3_signal_at_close"].map(bool_value).reset_index(drop=True)
    )
    diff_dates = ps.loc[overlap, "date"].reset_index(drop=True)[branch_diff].tolist()
    proxy_diff = ps.stock_proxy_close.ne(ns.stock_proxy_close)
    old = pd.read_csv(OLD, dtype={"date": str})
    old_sep = {}
    for date in ("2026-09-07", "2026-09-30"):
        row = old[old.date == date].iloc[0]
        old_sep[date] = {
            "score": float(row.annualized_log_wls_score),
            "vol10": float(row.overheat_feature_value),
            "holding": str(row.holding), "next_holding": str(row.next_holding),
            "exit": bool_value(row.overheat_exit_triggered),
            "reentry": bool_value(row.overheat_reentry_triggered),
        }
    result = {
        "label": "L5 old-pool conditional time audit, no formal performance",
        "branches": {"prefunded": pref, "netted": net},
        "branch_comparison": {
            "same_index": ps.date.tolist() == ns.date.tolist(),
            "first_proxy_close_difference": ps.loc[proxy_diff, "date"].iloc[0],
            "overlap_signal_days": int(overlap.sum()),
            "different_signal_days": len(diff_dates),
            "first_signal_difference": diff_dates[0],
            "different_signal_dates": diff_dates,
        },
        "old_frozen": {"path": str(OLD), "sha256": sha256(OLD), "sep": old_sep},
    }
    interaction = {}
    for branch in ("prefunded", "netted"):
        paths = {
            mode: {
                "signal": L5_EXEC / f"{branch}_{mode}" / f"recent_{branch}_rebuilt_signals.csv",
                "daily": L5_EXEC / f"{branch}_{mode}" / f"recent_{branch}_v2_3_strategy_daily.csv",
                "orders": L5_EXEC / f"{branch}_{mode}" / f"recent_{branch}_v2_3_strategy_orders.csv",
            }
            for mode in ("on", "off")
        }
        if not all(p.exists() for mode in paths.values() for p in mode.values()):
            continue
        ons = pd.read_csv(paths["on"]["signal"], dtype={"date": str})
        offs = pd.read_csv(paths["off"]["signal"], dtype={"date": str})
        ond = pd.read_csv(paths["on"]["daily"], dtype={"date": str})
        offd = pd.read_csv(paths["off"]["daily"], dtype={"date": str})
        assert ons.date.tolist() == offs.date.tolist() == ond.date.tolist() == offd.date.tolist()
        sig_diff = ons.v2_3_next_session_actionable_scale.ne(offs.v2_3_next_session_actionable_scale)
        sig_diff &= ons.v2_3_next_session_actionable_scale.notna() & offs.v2_3_next_session_actionable_scale.notna()
        first_sig = ons.loc[sig_diff, "date"].iloc[0]
        first_actual = ond.loc[ond.held_count.ne(offd.held_count), "date"].iloc[0]
        first_notional = ond.loc[ond.short_notional_after_close.ne(offd.short_notional_after_close), "date"].iloc[0]
        first_margin = ond.loc[ond.swap_margin.ne(offd.swap_margin), "date"].iloc[0]
        off_gaps = [
            {"date": row.date, "target_after_close": bool_value(row.v2_3_holding_after_close),
             "actual_held_symbols": int(offd.iloc[i].held_count)}
            for i, row in enumerate(offs.itertuples(index=False))
            if not pd.isna(row.v2_3_holding_after_close)
            and (bool_value(row.v2_3_holding_after_close) != (int(offd.iloc[i].held_count) > 0))
        ]
        dlist = ons.date.tolist()
        assert first_actual == dlist[dlist.index(first_sig) + 1]
        assert first_notional == first_actual
        assert first_margin == dlist[dlist.index(first_actual) + 1]
        interaction[branch] = {
            "sha256": {f"{mode}_{kind}": sha256(path) for mode, group in paths.items() for kind, path in group.items()},
            "first_signal_difference_T_close": first_sig,
            "first_actual_held_symbols_difference_T_plus_1_close": first_actual,
            "first_short_notional_difference_T_plus_1_close": first_notional,
            "first_swap_margin_difference_T_plus_2_close": first_margin,
            "first_signal_on": bool_value(ons.loc[sig_diff, "v2_3_signal_at_close"].iloc[0]),
            "first_signal_off": bool_value(offs.loc[sig_diff, "v2_3_signal_at_close"].iloc[0]),
            "first_signal_score": float(ons.loc[sig_diff, "v2_3_score"].iloc[0]),
            "first_signal_overheat_feature": float((ps if branch == "prefunded" else ns).loc[
                (ps if branch == "prefunded" else ns).date == first_sig,
                "independent_overheat_feature"].iloc[0]),
            "off_target_vs_actual_stock_state_gaps": off_gaps,
        }
    result["l5_on_off_interaction_clock"] = interaction
    HERE.mkdir(parents=True, exist_ok=True)
    (HERE / "audit_result.json").write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"branches": {k: {"clock_errors": v["signal_to_next_close_clock_errors"],
                                           "order_errors": v["stock_order_bad_next_session_dates"]}
                                    for k, v in result["branches"].items()},
                      "branch_comparison": result["branch_comparison"],
                      "interaction": interaction}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
