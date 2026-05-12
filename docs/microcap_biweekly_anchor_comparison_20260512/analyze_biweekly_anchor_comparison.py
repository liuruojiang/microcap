from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[2]
OUT_DIR = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import microcap_top100_mom16_biweekly_live as live

ANCHOR_DATE = pd.Timestamp("2016-01-07")
WINDOWS = {
    "1y": pd.DateOffset(years=1),
    "3y": pd.DateOffset(years=3),
    "5y": pd.DateOffset(years=5),
    "10y": pd.DateOffset(years=10),
}
MAX_WORKERS = 32
PANEL_SHADOW = ROOT / "outputs" / "microcap_top100_mom16_biweekly_live_panel_refreshed.csv"
PROXY_META = ROOT / "outputs" / "microcap_top100_mom16_biweekly_live_proxy_meta.json"


def build_fixed_anchor_rebalance_dates(
    trading_dates: pd.DatetimeIndex,
    anchor_date: pd.Timestamp = ANCHOR_DATE,
) -> pd.DatetimeIndex:
    dates = pd.DatetimeIndex(trading_dates).sort_values()
    if len(dates) == 0:
        return pd.DatetimeIndex([])
    week_start = dates.to_period("W-WED").start_time.normalize()
    anchor_week = pd.Timestamp(anchor_date).to_period("W-WED").start_time.normalize()
    week_offset = ((week_start - anchor_week).days // 7)
    selected = (week_offset % 2) == 0
    selected_dates = pd.Series(dates[selected], index=week_start[selected])
    return pd.DatetimeIndex(selected_dates.groupby(level=0).min().tolist())


def load_trading_dates() -> pd.DatetimeIndex:
    panel_path = PANEL_SHADOW if PANEL_SHADOW.exists() else live.DEFAULT_PANEL_PATH
    panel = pd.read_csv(panel_path, usecols=["date"])
    panel["date"] = pd.to_datetime(panel["date"])
    dates = panel["date"].drop_duplicates().sort_values()
    effective_start = pd.Timestamp(live.freq_mod.START_DATE)
    if PROXY_META.exists():
        try:
            meta = json.loads(PROXY_META.read_text(encoding="utf-8"))
            effective_start = pd.Timestamp(meta.get("effective_start_date") or meta.get("start_date") or effective_start)
        except Exception:
            pass
    dates = dates[dates >= effective_start]
    return pd.DatetimeIndex(dates)


def load_panel_path() -> Path:
    return PANEL_SHADOW if PANEL_SHADOW.exists() else live.DEFAULT_PANEL_PATH


def build_schedule_result(
    label: str,
    rebalance_dates: pd.DatetimeIndex,
    trading_dates: pd.DatetimeIndex,
    returns_df: pd.DataFrame,
    caps_by_date: dict[pd.Timestamp, dict[str, float]],
    buyable_df: pd.DataFrame,
    sellable_df: pd.DataFrame,
    name_map: dict[str, str],
) -> dict[str, object]:
    index_df, turnover_df, effective_members = live.freq_mod.simulate_rebalance_path(
        trading_dates=trading_dates,
        returns_df=returns_df,
        target_members_map=caps_by_date,
        rebalance_dates=rebalance_dates,
        buyable_df=buyable_df,
        sellable_df=sellable_df,
        one_side_cost_rate=0.003,
        top_n=live.TOP_N,
        execution_timing=live.EXECUTION_TIMING,
    )

    panel = pd.read_csv(load_panel_path(), usecols=["date", live.HEDGE_COLUMN])
    panel["date"] = pd.to_datetime(panel["date"])
    hedge = panel.set_index("date")[live.HEDGE_COLUMN].rename("hedge").astype(float)
    microcap = index_df.set_index("date")["close"].rename("microcap").astype(float)
    close_df = pd.concat([microcap, hedge], axis=1).sort_index().dropna()

    gross = live.run_signal(close_df)
    net = live.freq_mod.cost_mod.apply_cost_model(gross, turnover_df)
    live.ensure_overlay_pre_cost_return(net)

    return {
        "label": label,
        "rebalance_dates": rebalance_dates,
        "target_members": caps_by_date,
        "effective_members": effective_members,
        "index": index_df,
        "turnover": turnover_df,
        "gross": gross,
        "net": net,
    }


def metric_row(label: str, window: str, returns: pd.Series) -> dict[str, object]:
    returns = returns.dropna()
    metrics = live.hedge_mod.calc_metrics(returns)
    return {
        "schedule": label,
        "window": window,
        "start_date": str(pd.Timestamp(returns.index.min()).date()),
        "end_date": str(pd.Timestamp(returns.index.max()).date()),
        "days": int(len(returns)),
        "total_return_pct": float(metrics.total_return) * 100.0,
        "annual_pct": float(metrics.annual) * 100.0,
        "max_drawdown_pct": float(metrics.max_dd) * 100.0,
        "vol_pct": float(metrics.vol) * 100.0,
        "sharpe": float(metrics.sharpe),
    }


def build_metrics(results: list[dict[str, object]]) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for result in results:
        net = result["net"]
        returns = net["return_net"].astype(float)
        rows.append(metric_row(str(result["label"]), "full", returns))
        end = pd.Timestamp(returns.index.max())
        for label, offset in WINDOWS.items():
            part = returns.loc[returns.index >= end - offset]
            rows.append(metric_row(str(result["label"]), label, part))
    return pd.DataFrame(rows)


def build_signal_compare(results: list[dict[str, object]]) -> pd.DataFrame:
    rows = []
    for result in results:
        gross = result["gross"]
        net = result["net"]
        last = gross.iloc[-1]
        net_last = net.iloc[-1]
        rows.append(
            {
                "schedule": result["label"],
                "date": str(pd.Timestamp(gross.index[-1]).date()),
                "current_holding": str(last.get("holding")),
                "next_holding": str(last.get("next_holding")),
                "trade_state": live.compute_trade_state(str(last.get("holding")), str(last.get("next_holding"))),
                "microcap_mom": float(last.get("microcap_mom")),
                "hedge_mom": float(last.get("hedge_mom")),
                "momentum_gap": float(last.get("momentum_gap")),
                "nav_net": float(net_last.get("nav_net")),
            }
        )
    return pd.DataFrame(rows)


def build_rebalance_diff(current: pd.DatetimeIndex, fixed: pd.DatetimeIndex) -> pd.DataFrame:
    current_dates = {pd.Timestamp(dt).date() for dt in current}
    fixed_dates = {pd.Timestamp(dt).date() for dt in fixed}
    rows = []
    for dt in sorted(current_dates - fixed_dates):
        rows.append({"date": str(dt), "side": "current_only"})
    for dt in sorted(fixed_dates - current_dates):
        rows.append({"date": str(dt), "side": "fixed_only"})
    return pd.DataFrame(rows)


def write_summary(
    trading_dates: pd.DatetimeIndex,
    symbols: list[str],
    current_dates: pd.DatetimeIndex,
    fixed_dates: pd.DatetimeIndex,
    metrics: pd.DataFrame,
    signal_compare: pd.DataFrame,
    diff: pd.DataFrame,
) -> None:
    metric_pivot = metrics.pivot(index="window", columns="schedule", values="annual_pct").reset_index()
    dd_pivot = metrics.pivot(index="window", columns="schedule", values="max_drawdown_pct").reset_index()
    lines = [
        "# Microcap Top100 Biweekly Anchor Comparison",
        "",
        f"- Source panel: `{load_panel_path().relative_to(ROOT)}`",
        f"- Trading dates: {trading_dates.min().date()} to {trading_dates.max().date()} ({len(trading_dates)} rows)",
        f"- Universe symbols: {len(symbols)} from `freq_mod.load_current_universe()`",
        f"- Current schedule: existing floating-week `build_biweekly_rebalance_dates()`",
        f"- Fixed schedule: absolute anchor `{ANCHOR_DATE.date()}` with `W-WED` week buckets",
        f"- Cost model: `freq_mod.cost_mod.apply_cost_model()` on `return_net` / `nav_net`",
        "",
        "## Latest Signal",
        "",
        signal_compare.to_markdown(index=False),
        "",
        "## Annual Return (%)",
        "",
        metric_pivot.to_markdown(index=False),
        "",
        "## Max Drawdown (%)",
        "",
        dd_pivot.to_markdown(index=False),
        "",
        "## Rebalance Schedule Diff",
        "",
        f"- Current rebalance count: {len(current_dates)}",
        f"- Fixed rebalance count: {len(fixed_dates)}",
        f"- Differing date rows: {len(diff)}",
        "",
        "First differing rows:",
        "",
        diff.head(20).to_markdown(index=False) if not diff.empty else "No differences.",
        "",
        "## Interpretation",
        "",
        "The current floating-week schedule depends on the first week included in the input date range.",
        "The fixed-anchor schedule removes that start-date dependence, but it is a strategy-definition change.",
        "Adopting it should therefore be treated as a historical-result rewrite and compared before changing production defaults.",
    ]
    (OUT_DIR / "summary.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    trading_dates = load_trading_dates()
    current_dates = live.build_biweekly_rebalance_dates(trading_dates)
    fixed_dates = build_fixed_anchor_rebalance_dates(trading_dates)
    cap_dates = pd.DatetimeIndex(sorted(set(current_dates).union(set(fixed_dates))))
    symbols = live.freq_mod.load_current_universe()
    print(f"phase=cap_ranking symbols={len(symbols)} cap_dates={len(cap_dates)}", flush=True)
    _, caps_by_date, _, _ = live.freq_mod.load_cache_panels(
        symbols=symbols,
        trading_dates=cap_dates,
        cap_dates=cap_dates,
        max_workers=MAX_WORKERS,
        trade_constraint_mode=live.TRADE_CONSTRAINT_MODE,
        exclude_historical_st_from_caps=False,
    )
    name_map = live.load_name_map()
    current_targets = live.build_live_target_members_map(
        caps_by_date=caps_by_date,
        rebalance_dates=current_dates,
        name_map=name_map,
        top_n=live.TOP_N,
    )
    fixed_targets = live.build_live_target_members_map(
        caps_by_date=caps_by_date,
        rebalance_dates=fixed_dates,
        name_map=name_map,
        top_n=live.TOP_N,
    )
    selected_symbols = sorted(
        {
            str(symbol).zfill(6)
            for members in list(current_targets.values()) + list(fixed_targets.values())
            for symbol in members
        }
    )
    print(f"phase=daily_panels selected_symbols={len(selected_symbols)} trading_dates={len(trading_dates)}", flush=True)
    returns_df, _, buyable_df, sellable_df = live.freq_mod.load_cache_panels(
        symbols=selected_symbols,
        trading_dates=trading_dates,
        cap_dates=cap_dates,
        max_workers=MAX_WORKERS,
        trade_constraint_mode=live.TRADE_CONSTRAINT_MODE,
        exclude_historical_st_from_caps=False,
    )

    current = build_schedule_result(
        "current_floating",
        current_dates,
        trading_dates,
        returns_df,
        current_targets,
        buyable_df,
        sellable_df,
        name_map,
    )
    fixed = build_schedule_result(
        "fixed_anchor_2016_01_07",
        fixed_dates,
        trading_dates,
        returns_df,
        fixed_targets,
        buyable_df,
        sellable_df,
        name_map,
    )
    results = [current, fixed]

    metrics = build_metrics(results)
    signal_compare = build_signal_compare(results)
    diff = build_rebalance_diff(current_dates, fixed_dates)

    metrics.to_csv(OUT_DIR / "window_metrics.csv", index=False, encoding="utf-8")
    signal_compare.to_csv(OUT_DIR / "latest_signal_compare.csv", index=False, encoding="utf-8")
    diff.to_csv(OUT_DIR / "rebalance_date_diff.csv", index=False, encoding="utf-8")
    current["turnover"].to_csv(OUT_DIR / "current_floating_turnover.csv", index=False, encoding="utf-8")
    fixed["turnover"].to_csv(OUT_DIR / "fixed_anchor_2016_01_07_turnover.csv", index=False, encoding="utf-8")

    payload = {
        "anchor_date": str(ANCHOR_DATE.date()),
        "trading_start": str(trading_dates.min().date()),
        "trading_end": str(trading_dates.max().date()),
        "trading_rows": int(len(trading_dates)),
        "symbols": int(len(symbols)),
        "selected_symbols": int(len(selected_symbols)),
        "current_rebalance_count": int(len(current_dates)),
        "fixed_rebalance_count": int(len(fixed_dates)),
        "rebalance_diff_rows": int(len(diff)),
    }
    (OUT_DIR / "manifest.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    write_summary(trading_dates, symbols, current_dates, fixed_dates, metrics, signal_compare, diff)
    print(json.dumps(payload, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
