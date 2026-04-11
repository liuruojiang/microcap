import json

import pandas as pd

import analyze_combined_drawdown_throttle as cd
import analyze_subb_leverage_scenarios as lev


def build_sleeve_weakness(nav_parts, *, sleeve_dd_threshold):
    dd = nav_parts.div(nav_parts.cummax()).sub(1.0)
    weak = dd < -sleeve_dd_threshold
    weak_count = weak.sum(axis=1)
    return {
        "drawdown": dd,
        "weak_flags": weak,
        "weak_count": weak_count,
    }


def apply_multisleeve_guard(
    base_ret,
    cash_ret,
    weak_count,
    *,
    moderate_count,
    severe_count,
    scale_moderate,
    scale_severe,
    recover_count,
    step_up,
    rerisk_frequency="daily",
    rebal_cost_bps=0.0,
):
    idx = base_ret.dropna().index
    cash_ret = cash_ret.reindex(idx).fillna(0.0)
    weak_count = weak_count.reindex(idx).fillna(0).astype(int)

    nav = 1.0
    peak = 1.0
    current_scale = 1.0
    daily_cost_per_unit = rebal_cost_bps / 10000.0
    rows = []

    for dt in idx:
        wc = int(weak_count.at[dt])
        base_r = float(base_ret.at[dt])
        cash_r = float(cash_ret.at[dt])
        prev_dd = nav / peak - 1.0

        if wc >= severe_count:
            target_scale = scale_severe
            state = "severe"
        elif wc >= moderate_count:
            target_scale = scale_moderate
            state = "moderate"
        elif wc <= recover_count:
            target_scale = 1.0
            state = "normal"
        else:
            target_scale = current_scale
            state = "hold"

        if target_scale < current_scale:
            new_scale = target_scale
        elif target_scale > current_scale:
            can_rerisk = True
            if rerisk_frequency == "weekly":
                can_rerisk = dt.to_period("W-MON") != idx[max(0, idx.get_loc(dt) - 1)].to_period("W-MON")
            elif rerisk_frequency != "daily":
                raise ValueError(f"Unknown rerisk_frequency: {rerisk_frequency}")
            new_scale = min(current_scale + step_up, target_scale) if can_rerisk else current_scale
        else:
            new_scale = current_scale

        turnover = abs(new_scale - current_scale)
        cost = turnover * daily_cost_per_unit if turnover > 0 else 0.0
        adj_r = new_scale * base_r + (1.0 - new_scale) * cash_r - cost

        nav *= 1.0 + adj_r
        peak = max(peak, nav)
        current_scale = new_scale

        rows.append(
            {
                "date": dt,
                "return": adj_r,
                "base_return": base_r,
                "cash_return": cash_r,
                "scale": new_scale,
                "target_scale": target_scale,
                "weak_count": wc,
                "state": state,
                "turnover": turnover,
                "cost": cost,
                "nav": nav,
                "peak": peak,
                "drawdown": nav / peak - 1.0,
                "prev_drawdown": prev_dd,
            }
        )

    return pd.DataFrame(rows).set_index("date")


def summarize_guard(run_df, weak_count):
    scale = run_df["scale"]
    states = run_df["state"]
    summary = {
        "avg_scale": float(scale.mean()),
        "median_scale": float(scale.median()),
        "min_scale": float(scale.min()),
        "share_below_1": float((scale < 0.999999).mean()),
        "avg_weak_count": float(weak_count.reindex(run_df.index).mean()),
        "share_weak_count_ge_2": float((weak_count.reindex(run_df.index) >= 2).mean()),
        "share_weak_count_ge_3": float((weak_count.reindex(run_df.index) >= 3).mean()),
        "moderate_share": float((states == "moderate").mean()),
        "severe_share": float((states == "severe").mean()),
        "hold_share": float((states == "hold").mean()),
        "n_scale_changes": int((run_df["turnover"] > 0).sum()),
        "annualized_turnover": float(run_df["turnover"].sum() / len(run_df) * 252.0),
    }
    return summary


def main():
    mod = lev.load_main_module()
    cn_close, cn_close_with_bond, cn_dk_close = lev.load_cn_data()
    us_rot_close, us_prod_daily = lev.load_us_prod_daily(mod)

    cn_result = mod.run_cn_strategy(cn_close_with_bond, mod.CN_EQUITY_CODES)
    cn_dk_result = mod.run_dk_strategy(cn_close, cn_dk_close)
    us_rot_result = lev.run_rotation_variant(
        mod,
        us_rot_close,
        target_vol=mod.US_ROT_TARGET_VOL,
        max_lev=mod.US_ROT_MAX_LEV,
        leverage_mode="futures_only",
        finance_spread_bps=0.0,
    )

    combined = cd.build_combined_daily_components(mod, cn_result, cn_dk_result, us_rot_result, us_prod_daily)
    base_ret = combined["base_ret"]
    cash_ret = us_prod_daily["BIL"].pct_change().reindex(base_ret.index).fillna(0.0)

    scenarios = {
        "baseline": None,
        "weakdd5_mild": {
            "sleeve_dd_threshold": 0.05,
            "moderate_count": 2,
            "severe_count": 3,
            "scale_moderate": 0.90,
            "scale_severe": 0.75,
            "recover_count": 1,
            "step_up": 0.02,
            "rebal_cost_bps": 2.0,
        },
        "weakdd5_balanced": {
            "sleeve_dd_threshold": 0.05,
            "moderate_count": 2,
            "severe_count": 3,
            "scale_moderate": 0.85,
            "scale_severe": 0.65,
            "recover_count": 1,
            "step_up": 0.01,
            "rebal_cost_bps": 2.0,
        },
        "weakdd6_mild": {
            "sleeve_dd_threshold": 0.06,
            "moderate_count": 2,
            "severe_count": 3,
            "scale_moderate": 0.90,
            "scale_severe": 0.75,
            "recover_count": 1,
            "step_up": 0.02,
            "rerisk_frequency": "daily",
            "rebal_cost_bps": 2.0,
        },
        "weakdd6_mild_weekly_rerisk": {
            "sleeve_dd_threshold": 0.06,
            "moderate_count": 2,
            "severe_count": 3,
            "scale_moderate": 0.90,
            "scale_severe": 0.75,
            "recover_count": 1,
            "step_up": 0.05,
            "rerisk_frequency": "weekly",
            "rebal_cost_bps": 2.0,
        },
        "weakdd5_mild_weekly_rerisk": {
            "sleeve_dd_threshold": 0.05,
            "moderate_count": 2,
            "severe_count": 3,
            "scale_moderate": 0.90,
            "scale_severe": 0.75,
            "recover_count": 1,
            "step_up": 0.05,
            "rerisk_frequency": "weekly",
            "rebal_cost_bps": 2.0,
        },
        "weakdd4_fast": {
            "sleeve_dd_threshold": 0.04,
            "moderate_count": 2,
            "severe_count": 3,
            "scale_moderate": 0.90,
            "scale_severe": 0.70,
            "recover_count": 1,
            "step_up": 0.02,
            "rerisk_frequency": "daily",
            "rebal_cost_bps": 2.0,
        },
    }

    output = {
        "baseline": cd.summarize_return_series(mod, base_ret),
    }

    for name, cfg in scenarios.items():
        if cfg is None:
            continue
        weakness = build_sleeve_weakness(combined["nav_parts"], sleeve_dd_threshold=cfg["sleeve_dd_threshold"])
        run_df = apply_multisleeve_guard(
            base_ret,
            cash_ret,
            weakness["weak_count"],
            moderate_count=cfg["moderate_count"],
            severe_count=cfg["severe_count"],
            scale_moderate=cfg["scale_moderate"],
            scale_severe=cfg["scale_severe"],
            recover_count=cfg["recover_count"],
            step_up=cfg["step_up"],
            rerisk_frequency=cfg.get("rerisk_frequency", "daily"),
            rebal_cost_bps=cfg["rebal_cost_bps"],
        )
        output[name] = {
            "performance": cd.summarize_return_series(mod, run_df["return"]),
            "guard": summarize_guard(run_df, weakness["weak_count"]),
            "config": cfg,
        }

    print(json.dumps(output, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
