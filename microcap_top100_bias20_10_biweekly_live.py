from __future__ import annotations

import microcap_top100_mom16_biweekly_live as base_mod
import analyze_microcap_zz1000_hedge as hedge_mod


base_mod.LOOKBACK = 20
base_mod.DEFAULT_OUTPUT_PREFIX = "microcap_top100_bias20_10_biweekly_live"
base_mod.DEFAULT_COSTED_NAV_CSV = (
    base_mod.OUTPUT_DIR / "microcap_top100_bias20_10_hedge_zz1000_biweekly_thursday_16y_costed_nav.csv"
)
base_mod.STRATEGY_TITLE = "Top100 Microcap Bias20:10 Biweekly"
base_mod.INDEX_CODE = "TOP100_BIAS20_10_BIWEEKLY_THURSDAY_PROXY"

BIAS_N = 20
BIAS_MOM_DAY = 10


def run_signal(close_df):
    result = hedge_mod.run_backtest(
        close_df=close_df,
        signal_model="bias_momentum",
        lookback=base_mod.LOOKBACK,
        bias_n=BIAS_N,
        bias_mom_day=BIAS_MOM_DAY,
        futures_drag=base_mod.FUTURES_DRAG * base_mod.FIXED_HEDGE_RATIO,
        require_positive_microcap_mom=base_mod.REQUIRE_POSITIVE_MICROCAP_MOM,
        r2_window=hedge_mod.DEFAULT_R2_WINDOW,
        r2_threshold=0.0,
        vol_scale_enabled=False,
        target_vol=hedge_mod.DEFAULT_TARGET_VOL,
        vol_window=hedge_mod.DEFAULT_VOL_WINDOW,
        max_lev=hedge_mod.DEFAULT_MAX_LEV,
        min_lev=hedge_mod.DEFAULT_MIN_LEV,
        scale_threshold=hedge_mod.DEFAULT_SCALE_THRESHOLD,
    )
    result.index = result.index.astype("datetime64[ns]")
    return result


def build_summary(
    result,
    latest_signal,
    latest_rebalance,
    prev_rebalance,
    next_rebalance,
    members_df,
    changes_df,
    capital,
    anchor_freshness,
):
    latest_row = latest_signal.iloc[0]
    last_result_row = result.iloc[-1]
    current_holding = last_result_row["holding"]
    next_holding = last_result_row["next_holding"]
    active_next = next_holding != "cash"
    trade_state = base_mod.compute_trade_state(str(current_holding), str(next_holding))
    hedge_notional = capital * base_mod.FIXED_HEDGE_RATIO if (capital is not None and active_next) else 0.0
    return {
        "strategy": base_mod.DEFAULT_OUTPUT_PREFIX,
        "core_params": {
            "top_n": base_mod.TOP_N,
            "exclude_current_st": True,
            "rebalance_schedule": "biweekly",
            "rebalance_weekday_anchor": base_mod.REBALANCE_WEEKDAY,
            "signal_model": "bias_momentum",
            "bias_n": BIAS_N,
            "bias_mom_day": BIAS_MOM_DAY,
            "hedge_column": base_mod.HEDGE_COLUMN,
            "fixed_hedge_ratio": base_mod.FIXED_HEDGE_RATIO,
            "futures_drag_per_day": base_mod.FUTURES_DRAG,
        },
        "latest_trade_date": str(result.index[-1].date()),
        "latest_rebalance_date": str(latest_rebalance.date()),
        "previous_rebalance_date": None if prev_rebalance is None else str(prev_rebalance.date()),
        "next_rebalance_date": None if next_rebalance is None else str(next_rebalance.date()),
        "history_anchor": anchor_freshness,
        "latest_signal": {
            "signal_label": latest_row["signal_label"],
            "current_holding": current_holding,
            "next_holding": next_holding,
            "trade_state": trade_state,
            "microcap_mom": float(latest_row["microcap_mom"]),
            "hedge_mom": float(latest_row["hedge_mom"]),
            "momentum_gap": float(latest_row["momentum_gap"]),
            "ratio_bias_mom": float(latest_row["ratio_bias_mom"]) if latest_row["ratio_bias_mom"] == latest_row["ratio_bias_mom"] else None,
            "microcap_close": float(latest_row["microcap_close"]),
            "hedge_close": float(latest_row["hedge_close"]),
        },
        "target_members": {
            "count": int(len(members_df)),
            "enter_count": int((changes_df["action"] == "enter").sum()) if len(changes_df) else 0,
            "exit_count": int((changes_df["action"] == "exit").sum()) if len(changes_df) else 0,
            "equal_weight": 1.0 / base_mod.TOP_N,
        },
        "capital_plan": {
            "gross_stock_capital": capital,
            "per_stock_target_notional": None if capital is None else capital / base_mod.TOP_N,
            "hedge_notional": hedge_notional,
        },
    }


def print_console_summary(summary):
    latest_signal = summary["latest_signal"]
    capital_plan = summary["capital_plan"]
    target_members = summary["target_members"]
    print(f"最新交易日: {summary['latest_trade_date']}")
    print(f"最新调仓日: {summary['latest_rebalance_date']}")
    print(f"下一调仓日: {summary['next_rebalance_date']}")
    print(f"当前信号: {latest_signal['signal_label']} -> 下期持仓 {latest_signal['next_holding']}")
    print(f"交易动作: {latest_signal['trade_state']}")
    print(
        "20:10乖离率动量: microcap={:.4%}, hedge={:.4%}, gap={:.4%}, ratio_bias_mom={:.4f}".format(
            latest_signal["microcap_mom"],
            latest_signal["hedge_mom"],
            latest_signal["momentum_gap"],
            latest_signal["ratio_bias_mom"] if latest_signal["ratio_bias_mom"] is not None else float("nan"),
        )
    )
    print(
        f"目标成分股: {target_members['count']} 只, 本次进入 {target_members['enter_count']} 只, "
        f"剔除 {target_members['exit_count']} 只"
    )
    if capital_plan["gross_stock_capital"] is not None:
        print(
            f"股票资金: {capital_plan['gross_stock_capital']:.2f}, "
            f"单票目标资金: {capital_plan['per_stock_target_notional']:.2f}, "
            f"对冲名义: {capital_plan['hedge_notional']:.2f}"
        )


base_mod.run_signal = run_signal
base_mod.build_summary = build_summary
base_mod.print_console_summary = print_console_summary


if __name__ == "__main__":
    base_mod.main()
