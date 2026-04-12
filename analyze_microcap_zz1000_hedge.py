from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parent
OUTPUT_DIR = ROOT / "outputs"
DEFAULT_PANEL = ROOT / "mnt_strategy_data_cn.csv"
DEFAULT_MICROCAP_CSV = OUTPUT_DIR / "wind_microcap_868008_data.csv"

DEFAULT_OUTPUT_PREFIX = "microcap_zz1000_hedge"

DEFAULT_MICROCAP_COLUMN = "868008.WI"
DEFAULT_MICROCAP_LABEL = "Wind Microcap Index"
DEFAULT_HEDGE_COLUMN = "1.000852"
DEFAULT_LOOKBACK = 20
DEFAULT_FUTURES_DRAG = 3.0 / 10000.0
DEFAULT_R2_WINDOW = 5
DEFAULT_R2_THRESHOLD = 0.0
DEFAULT_SIGNAL_MODEL = "momentum"
DEFAULT_BIAS_N = 60
DEFAULT_BIAS_MOM_DAY = 20
DEFAULT_TARGET_VOL = 0.20
DEFAULT_VOL_WINDOW = 30
DEFAULT_MAX_LEV = 1.5
DEFAULT_MIN_LEV = 0.1
DEFAULT_SCALE_THRESHOLD = 0.10
CN_TRADING_DAYS = 244

# The workspace does not currently include the Wind microcap index.
# Keep the default candidates generic so the script can be used as soon as
# the user adds a microcap column into the main panel.
DEFAULT_MICROCAP_CANDIDATES = [
    "868008.WI",
    "868008_WI",
    "万得微盘股指数",
    "WIND_MICROCAP",
    "wind_microcap",
    "microcap",
]


@dataclass
class Metrics:
    annual: float
    vol: float
    sharpe: float
    max_dd: float
    calmar: float
    total_return: float
    win_rate: float


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Backtest a hedged China microcap strategy: long microcap, short CSI 1000 "
            "proxy when microcap momentum is stronger than CSI 1000 momentum and "
            "microcap momentum is positive."
        )
    )
    parser.add_argument("--panel-path", type=Path, default=DEFAULT_PANEL)
    parser.add_argument(
        "--microcap-column",
        default=DEFAULT_MICROCAP_COLUMN,
        help="Column name in the main panel for the Wind microcap index. Default: 868008.WI",
    )
    parser.add_argument(
        "--hedge-column",
        default=DEFAULT_HEDGE_COLUMN,
        help="Column name used as the CSI 1000 futures proxy.",
    )
    parser.add_argument(
        "--lookback",
        type=int,
        default=DEFAULT_LOOKBACK,
        help="Momentum lookback window in trading days.",
    )
    parser.add_argument(
        "--signal-model",
        choices=("momentum", "bias_momentum"),
        default=DEFAULT_SIGNAL_MODEL,
        help="Signal model. 'momentum' uses relative momentum; 'bias_momentum' uses ratio/MA slope.",
    )
    parser.add_argument(
        "--bias-n",
        type=int,
        default=DEFAULT_BIAS_N,
        help="Moving-average window used by bias_momentum.",
    )
    parser.add_argument(
        "--bias-mom-day",
        type=int,
        default=DEFAULT_BIAS_MOM_DAY,
        help="Slope-fit window used by bias_momentum.",
    )
    parser.add_argument(
        "--futures-drag",
        type=float,
        default=DEFAULT_FUTURES_DRAG,
        help="Daily basis drag charged on active hedge days. Default = 3/10000.",
    )
    parser.add_argument(
        "--r2-window",
        type=int,
        default=DEFAULT_R2_WINDOW,
        help="Rolling window for R-squared filter on microcap/hedge ratio.",
    )
    parser.add_argument(
        "--r2-threshold",
        type=float,
        default=DEFAULT_R2_THRESHOLD,
        help="Minimum R-squared required to enter a position. Default 0 disables the filter.",
    )
    parser.add_argument(
        "--vol-scale-enabled",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Enable target volatility scaling on strategy daily returns.",
    )
    parser.add_argument("--target-vol", type=float, default=DEFAULT_TARGET_VOL)
    parser.add_argument("--vol-window", type=int, default=DEFAULT_VOL_WINDOW)
    parser.add_argument("--max-lev", type=float, default=DEFAULT_MAX_LEV)
    parser.add_argument("--min-lev", type=float, default=DEFAULT_MIN_LEV)
    parser.add_argument("--scale-threshold", type=float, default=DEFAULT_SCALE_THRESHOLD)
    parser.add_argument(
        "--microcap-csv",
        type=Path,
        default=None,
        help=(
            "Optional external CSV for the microcap index. Must contain a date column "
            "and a close column."
        ),
    )
    parser.add_argument("--microcap-date-col", default="date")
    parser.add_argument("--microcap-close-col", default="close")
    parser.add_argument("--output-prefix", default=DEFAULT_OUTPUT_PREFIX)
    parser.add_argument(
        "--require-positive-microcap-mom",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Require microcap momentum > 0 to enter the hedge trade. Default: true.",
    )
    return parser.parse_args()


def load_main_panel(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Main panel not found: {path}")
    panel = pd.read_csv(path)
    if "date" not in panel.columns:
        raise ValueError(f"'date' column missing in {path}")
    panel["date"] = pd.to_datetime(panel["date"])
    panel = panel.sort_values("date").drop_duplicates(subset="date")
    return panel.set_index("date")


def resolve_microcap_column(panel: pd.DataFrame, explicit_name: str | None) -> str:
    if explicit_name:
        if explicit_name not in panel.columns:
            available = ", ".join(panel.columns)
            raise ValueError(
                f"Microcap column '{explicit_name}' not found in panel. "
                f"Expected Wind microcap code is '{DEFAULT_MICROCAP_COLUMN}'. "
                f"Available columns: {available}"
            )
        return explicit_name

    for candidate in DEFAULT_MICROCAP_CANDIDATES:
        if candidate in panel.columns:
            return candidate

    available = ", ".join(panel.columns)
    raise ValueError(
        f"{DEFAULT_MICROCAP_LABEL} column not found. Add '{DEFAULT_MICROCAP_COLUMN}' into "
        "mnt_strategy_data_cn.csv or pass "
        f"--microcap-column / --microcap-csv. Available columns: {available}"
    )


def load_external_microcap_series(
    csv_path: Path,
    date_col: str,
    close_col: str,
) -> pd.Series:
    if not csv_path.exists():
        raise FileNotFoundError(f"Microcap CSV not found: {csv_path}")
    frame = pd.read_csv(csv_path)
    missing = [col for col in (date_col, close_col) if col not in frame.columns]
    if missing:
        raise ValueError(f"Microcap CSV missing columns: {missing}")
    frame = frame[[date_col, close_col]].copy()
    frame[date_col] = pd.to_datetime(frame[date_col])
    frame = frame.dropna(subset=[close_col]).sort_values(date_col).drop_duplicates(subset=date_col)
    return frame.set_index(date_col)[close_col].rename("microcap")


def build_close_df(args: argparse.Namespace) -> pd.DataFrame:
    panel = load_main_panel(args.panel_path)

    if args.hedge_column not in panel.columns:
        available = ", ".join(panel.columns)
        raise ValueError(
            f"Hedge column '{args.hedge_column}' not found in panel. Available columns: {available}"
        )

    hedge = panel[args.hedge_column].rename("hedge").astype(float)

    external_microcap = args.microcap_csv
    if external_microcap is None and DEFAULT_MICROCAP_CSV.exists() and args.microcap_column not in panel.columns:
        external_microcap = DEFAULT_MICROCAP_CSV
    args.resolved_microcap_csv = str(external_microcap) if external_microcap else None

    if external_microcap:
        microcap = load_external_microcap_series(
            csv_path=external_microcap,
            date_col=args.microcap_date_col,
            close_col=args.microcap_close_col,
        )
    else:
        microcap_col = resolve_microcap_column(panel, args.microcap_column)
        microcap = panel[microcap_col].rename("microcap").astype(float)

    close_df = pd.concat([microcap, hedge], axis=1).sort_index()
    close_df = close_df.dropna(how="all").ffill()
    close_df = close_df.dropna(subset=["microcap", "hedge"])
    if len(close_df) < args.lookback + 3:
        raise ValueError(
            f"Not enough data after alignment. Need at least {args.lookback + 3} rows, got {len(close_df)}."
        )
    return close_df


def build_output_paths(output_prefix: str) -> dict[str, Path]:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    return {
        "nav": OUTPUT_DIR / f"{output_prefix}_nav.csv",
        "signal": OUTPUT_DIR / f"{output_prefix}_latest_signal.csv",
        "summary": OUTPUT_DIR / f"{output_prefix}_summary.json",
        "curve": OUTPUT_DIR / f"{output_prefix}_curve.png",
    }


def calc_momentum(series: pd.Series, lookback: int) -> pd.Series:
    return series.div(series.shift(lookback)).sub(1.0)


def calc_bias_momentum(series: pd.Series, bias_n: int, mom_day: int) -> pd.Series:
    prices = series.values.astype(float)
    n = len(prices)
    result = np.full(n, np.nan)
    ma = series.rolling(bias_n).mean().values
    total_lookback = bias_n + mom_day - 1
    x = np.arange(mom_day, dtype=float)
    for i in range(total_lookback, n):
        bias_window = np.empty(mom_day)
        valid = True
        for j in range(mom_day):
            idx = i - mom_day + 1 + j
            if np.isnan(ma[idx]) or ma[idx] < 1e-10 or np.isnan(prices[idx]):
                valid = False
                break
            bias_window[j] = prices[idx] / ma[idx]
        if not valid or bias_window[0] < 1e-10:
            continue
        bias_norm = bias_window / bias_window[0]
        slope = np.polyfit(x, bias_norm, 1)[0]
        result[i] = slope * 10000
    return pd.Series(result, index=series.index)


def calc_rolling_r2(series: pd.Series, window: int) -> pd.Series:
    values = series.values.astype(float)
    result = np.full(len(values), np.nan)
    x = np.arange(window, dtype=float)
    x_mean = x.mean()
    ss_x = ((x - x_mean) ** 2).sum()
    for i in range(window - 1, len(values)):
        y = values[i - window + 1 : i + 1]
        if np.any(np.isnan(y)):
            continue
        y_mean = y.mean()
        ss_y = ((y - y_mean) ** 2).sum()
        if ss_y < 1e-12:
            result[i] = 0.0
            continue
        ss_xy = ((x - x_mean) * (y - y_mean)).sum()
        result[i] = (ss_xy ** 2) / (ss_x * ss_y)
    return pd.Series(result, index=series.index)


def calc_metrics(ret: pd.Series) -> Metrics:
    ret = ret.dropna()
    if ret.empty:
        raise ValueError("Return series is empty.")

    nav = (1.0 + ret).cumprod()
    years = (ret.index[-1] - ret.index[0]).days / 365.25
    annual = nav.iloc[-1] ** (1.0 / years) - 1.0 if years > 0 else np.nan
    vol = ret.std(ddof=1) * np.sqrt(CN_TRADING_DAYS)
    sharpe = annual / vol if pd.notna(vol) and vol > 0 else np.nan
    max_dd = ((nav - nav.cummax()) / nav.cummax()).min()
    calmar = annual / abs(max_dd) if pd.notna(max_dd) and max_dd != 0 else np.nan
    total_return = nav.iloc[-1] - 1.0
    monthly = ret.groupby(ret.index.to_period("M")).apply(lambda x: (1.0 + x).prod() - 1.0)
    win_rate = float((monthly > 0).mean()) if len(monthly) else np.nan
    return Metrics(
        annual=float(annual),
        vol=float(vol),
        sharpe=float(sharpe),
        max_dd=float(max_dd),
        calmar=float(calmar),
        total_return=float(total_return),
        win_rate=float(win_rate),
    )


def apply_vol_scaling(
    result: pd.DataFrame,
    trading_days: int,
    vol_window: int,
    target_vol: float,
    min_lev: float,
    max_lev: float,
    scale_threshold: float,
) -> pd.DataFrame:
    out = result.copy()
    realized_vol = out["return_raw"].rolling(vol_window).std(ddof=1) * np.sqrt(trading_days)
    scale_raw = (target_vol / realized_vol).clip(lower=min_lev, upper=max_lev).shift(1)
    if scale_threshold > 0:
        scale_arr = scale_raw.to_numpy(copy=True)
        last_scale = np.nan
        for i in range(len(scale_arr)):
            if np.isnan(scale_arr[i]):
                continue
            if np.isnan(last_scale):
                last_scale = scale_arr[i]
            elif abs(scale_arr[i] - last_scale) >= scale_threshold - 1e-9:
                last_scale = scale_arr[i]
            else:
                scale_arr[i] = last_scale
        scale_raw = pd.Series(scale_arr, index=out.index)
    weight = scale_raw.fillna(1.0)
    weight[out["holding"] == "cash"] = 1.0
    out["realized_vol"] = realized_vol
    out["scale_raw"] = scale_raw
    out["weight"] = weight
    out["return"] = out["return_raw"] * out["weight"]
    out["nav"] = (1.0 + out["return"]).cumprod()
    return out


def run_backtest(
    close_df: pd.DataFrame,
    signal_model: str,
    lookback: int,
    bias_n: int,
    bias_mom_day: int,
    futures_drag: float,
    require_positive_microcap_mom: bool,
    r2_window: int,
    r2_threshold: float,
    vol_scale_enabled: bool,
    target_vol: float,
    vol_window: int,
    max_lev: float,
    min_lev: float,
    scale_threshold: float,
    hedge_ratio: float = 1.0,
) -> pd.DataFrame:
    work = close_df.copy()
    work["microcap_ret"] = work["microcap"].pct_change(fill_method=None)
    work["hedge_ret"] = work["hedge"].pct_change(fill_method=None)
    work["microcap_mom"] = calc_momentum(work["microcap"], lookback)
    work["hedge_mom"] = calc_momentum(work["hedge"], lookback)
    work["momentum_gap"] = work["microcap_mom"] - work["hedge_mom"]
    work["ratio"] = work["microcap"] / work["hedge"]
    work["ratio_bias_mom"] = calc_bias_momentum(work["ratio"], bias_n, bias_mom_day)
    work["ratio_r2"] = calc_rolling_r2(work["ratio"], r2_window)

    if signal_model == "bias_momentum":
        valid_mask = work["ratio_bias_mom"].notna()
    else:
        valid_mask = work[["microcap_mom", "hedge_mom"]].notna().all(axis=1)
    valid_start = valid_mask[valid_mask].index.min()
    if pd.isna(valid_start):
        raise ValueError("No valid momentum history after alignment.")

    work = work.loc[valid_start:].copy()
    rows: list[dict[str, object]] = []
    holding = False

    for i in range(1, len(work)):
        date = work.index[i]
        active_ret = 0.0
        drag = futures_drag if holding else 0.0
        if holding:
            microcap_ret = work["microcap_ret"].iloc[i]
            hedge_ret = work["hedge_ret"].iloc[i]
            if pd.notna(microcap_ret) and pd.notna(hedge_ret):
                active_ret = float(microcap_ret - hedge_ratio * hedge_ret)
        if signal_model == "bias_momentum":
            signal_on = bool(
                pd.notna(work["ratio_bias_mom"].iloc[i])
                and work["ratio_bias_mom"].iloc[i] > 0.0
                and (
                    (pd.notna(work["ratio_r2"].iloc[i]) and work["ratio_r2"].iloc[i] >= r2_threshold)
                    if r2_threshold > 0
                    else True
                )
            )
        else:
            signal_on = bool(
                pd.notna(work["microcap_mom"].iloc[i])
                and pd.notna(work["hedge_mom"].iloc[i])
                and work["microcap_mom"].iloc[i] > work["hedge_mom"].iloc[i]
                and (
                    (work["microcap_mom"].iloc[i] > 0.0)
                    if require_positive_microcap_mom
                    else True
                )
                and (
                    (pd.notna(work["ratio_r2"].iloc[i]) and work["ratio_r2"].iloc[i] >= r2_threshold)
                    if r2_threshold > 0
                    else True
                )
            )
        day_ret = active_ret - drag
        next_holding = "long_microcap_short_zz1000" if signal_on else "cash"
        rows.append(
            {
                "date": date,
                "return_raw": day_ret,
                "holding": "long_microcap_short_zz1000" if holding else "cash",
                "next_holding": next_holding,
                "signal_on": signal_on,
                "microcap_close": float(work["microcap"].iloc[i]),
                "hedge_close": float(work["hedge"].iloc[i]),
                "microcap_ret": float(work["microcap_ret"].iloc[i]) if pd.notna(work["microcap_ret"].iloc[i]) else np.nan,
                "hedge_ret": float(work["hedge_ret"].iloc[i]) if pd.notna(work["hedge_ret"].iloc[i]) else np.nan,
                "microcap_mom": float(work["microcap_mom"].iloc[i]),
                "hedge_mom": float(work["hedge_mom"].iloc[i]),
                "momentum_gap": float(work["momentum_gap"].iloc[i]),
                "ratio_bias_mom": float(work["ratio_bias_mom"].iloc[i]) if pd.notna(work["ratio_bias_mom"].iloc[i]) else np.nan,
                "ratio_r2": float(work["ratio_r2"].iloc[i]) if pd.notna(work["ratio_r2"].iloc[i]) else np.nan,
                "futures_drag": drag,
                "active_spread_ret": active_ret,
            }
        )
        holding = signal_on

    result = pd.DataFrame(rows).set_index("date")
    if vol_scale_enabled:
        result = apply_vol_scaling(
            result=result,
            trading_days=CN_TRADING_DAYS,
            vol_window=vol_window,
            target_vol=target_vol,
            min_lev=min_lev,
            max_lev=max_lev,
            scale_threshold=scale_threshold,
        )
    else:
        result["weight"] = 1.0
        result["realized_vol"] = np.nan
        result["scale_raw"] = np.nan
        result["return"] = result["return_raw"]
        result["nav"] = (1.0 + result["return"]).cumprod()
    return result


def build_latest_signal(result: pd.DataFrame) -> pd.DataFrame:
    last = result.iloc[[-1]].copy().reset_index()
    last["signal_label"] = np.where(last["next_holding"] == "cash", "cash", "long_microcap_short_zz1000")
    return last[
        [
            "date",
            "signal_label",
            "next_holding",
            "microcap_close",
            "hedge_close",
            "microcap_mom",
            "hedge_mom",
            "momentum_gap",
            "ratio_bias_mom",
            "ratio_r2",
            "weight",
            "futures_drag",
        ]
    ]


def build_summary(
    result: pd.DataFrame,
    args: argparse.Namespace,
    close_df: pd.DataFrame,
) -> dict[str, object]:
    metrics = calc_metrics(result["return"])
    holding_series = result["holding"]
    active_series = holding_series != "cash"
    spell_ids = holding_series.ne(holding_series.shift()).cumsum()
    spell_frame = pd.DataFrame({"holding": holding_series, "spell_id": spell_ids})
    spells = spell_frame.loc[spell_frame["holding"] != "cash"].groupby("spell_id").size()

    yearly: dict[str, float] = {}
    for year in sorted(result.index.year.unique()):
        part = result.loc[result.index.year == year, "return"]
        if len(part) > 10:
            yearly[str(year)] = float((1.0 + part).prod() - 1.0)

    latest = result.iloc[-1]
    return {
        "strategy": args.output_prefix,
        "panel_path": str(args.panel_path),
        "microcap_column": args.microcap_column,
        "microcap_label": DEFAULT_MICROCAP_LABEL,
        "microcap_csv": getattr(args, "resolved_microcap_csv", None),
        "hedge_column": args.hedge_column,
        "lookback": args.lookback,
        "signal_model": args.signal_model,
        "bias_n": args.bias_n,
        "bias_mom_day": args.bias_mom_day,
        "r2_window": args.r2_window,
        "r2_threshold": args.r2_threshold,
        "vol_scale_enabled": bool(args.vol_scale_enabled),
        "target_vol": args.target_vol,
        "vol_window": args.vol_window,
        "max_lev": args.max_lev,
        "min_lev": args.min_lev,
        "scale_threshold": args.scale_threshold,
        "futures_drag_per_day": args.futures_drag,
        "require_positive_microcap_mom": bool(args.require_positive_microcap_mom),
        "entry_rule": (
            f"ratio_bias_mom(price/MA{args.bias_n}, slope_window={args.bias_mom_day}) > 0"
            if args.signal_model == "bias_momentum"
            else (
                "microcap_mom > hedge_mom and microcap_mom > 0"
                if args.require_positive_microcap_mom
                else "microcap_mom > hedge_mom"
            )
        ),
        "r2_filter_rule": (
            f"ratio_r2({args.r2_window}) >= {args.r2_threshold:.2f}"
            if args.r2_threshold > 0
            else "disabled"
        ),
        "return_rule_when_active": "microcap_return - hedge_return - futures_drag_per_day",
        "start_date": str(result.index[0].date()),
        "end_date": str(result.index[-1].date()),
        "n_days": int(len(result)),
        "active_days_pct": float(active_series.mean()),
        "cash_days_pct": float((~active_series).mean()),
        "signal_changes": int(result["signal_on"].ne(result["signal_on"].shift()).sum() - 1),
        "median_holding_spell": float(spells.median()) if len(spells) else 0.0,
        "latest_signal": {
            "date": str(result.index[-1].date()),
            "next_holding": str(latest["next_holding"]),
            "microcap_mom": float(latest["microcap_mom"]),
            "hedge_mom": float(latest["hedge_mom"]),
            "momentum_gap": float(latest["momentum_gap"]),
            "ratio_bias_mom": float(latest["ratio_bias_mom"]) if pd.notna(latest["ratio_bias_mom"]) else None,
            "ratio_r2": float(latest["ratio_r2"]) if pd.notna(latest["ratio_r2"]) else None,
            "weight": float(latest["weight"]) if pd.notna(latest["weight"]) else None,
            "microcap_close": float(close_df["microcap"].iloc[-1]),
            "hedge_close": float(close_df["hedge"].iloc[-1]),
        },
        "metrics": asdict(metrics),
        "yearly": yearly,
    }


def plot_nav(nav: pd.Series, output_path: Path) -> None:
    plt.figure(figsize=(12, 6))
    plt.plot(nav.index, nav.values, linewidth=1.8, label="Microcap / CSI1000 Hedge")
    plt.title("Microcap Long + CSI1000 Hedge NAV")
    plt.grid(alpha=0.25)
    plt.legend()
    plt.tight_layout()
    plt.savefig(output_path, dpi=160)
    plt.close()


def main() -> None:
    args = parse_args()
    output_paths = build_output_paths(args.output_prefix)
    close_df = build_close_df(args)
    result = run_backtest(
        close_df=close_df,
        signal_model=args.signal_model,
        lookback=args.lookback,
        bias_n=args.bias_n,
        bias_mom_day=args.bias_mom_day,
        futures_drag=args.futures_drag,
        require_positive_microcap_mom=args.require_positive_microcap_mom,
        r2_window=args.r2_window,
        r2_threshold=args.r2_threshold,
        vol_scale_enabled=args.vol_scale_enabled,
        target_vol=args.target_vol,
        vol_window=args.vol_window,
        max_lev=args.max_lev,
        min_lev=args.min_lev,
        scale_threshold=args.scale_threshold,
    )
    latest_signal = build_latest_signal(result)
    summary = build_summary(result=result, args=args, close_df=close_df)

    result.to_csv(output_paths["nav"], index_label="date", encoding="utf-8")
    latest_signal.to_csv(output_paths["signal"], index=False, encoding="utf-8")
    output_paths["summary"].write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    plot_nav(result["nav"], output_paths["curve"])

    print(json.dumps(summary, ensure_ascii=False, indent=2))
    print(f"saved {output_paths['nav'].name}")
    print(f"saved {output_paths['signal'].name}")
    print(f"saved {output_paths['summary'].name}")
    print(f"saved {output_paths['curve'].name}")


if __name__ == "__main__":
    main()
