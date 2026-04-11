from __future__ import annotations

import math
from pathlib import Path

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parent
US_DATA = ROOT / "mnt_strategy_data_us.csv"
OUT_CSV = ROOT / "vol_turn_scan_results.csv"
OUT_MD = ROOT / "mnt_vol_turn_scan_20260327.md"

TRADING_DAYS = 252
RF_ANNUAL = 0.04
FEE_BPS = 2.0


def norm_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def bs_call_price(spot: float, strike: float, t: float, sigma: float, r: float) -> float:
    if t <= 0:
        return max(spot - strike, 0.0)
    if sigma <= 1e-12:
        return max(spot - strike * math.exp(-r * t), 0.0)
    sqrt_t = math.sqrt(t)
    d1 = (math.log(max(spot, 1e-12) / max(strike, 1e-12)) + (r + 0.5 * sigma * sigma) * t) / (sigma * sqrt_t)
    d2 = d1 - sigma * sqrt_t
    return spot * norm_cdf(d1) - strike * math.exp(-r * t) * norm_cdf(d2)


def bs_put_price(spot: float, strike: float, t: float, sigma: float, r: float) -> float:
    if t <= 0:
        return max(strike - spot, 0.0)
    if sigma <= 1e-12:
        return max(strike * math.exp(-r * t) - spot, 0.0)
    sqrt_t = math.sqrt(t)
    d1 = (math.log(max(spot, 1e-12) / max(strike, 1e-12)) + (r + 0.5 * sigma * sigma) * t) / (sigma * sqrt_t)
    d2 = d1 - sigma * sqrt_t
    return strike * math.exp(-r * t) * norm_cdf(-d2) - spot * norm_cdf(-d1)


def bs_straddle_price(spot: float, strike: float, t: float, sigma: float, r: float) -> float:
    return bs_call_price(spot, strike, t, sigma, r) + bs_put_price(spot, strike, t, sigma, r)


def calc_daily_metrics(ret: pd.Series) -> dict[str, float]:
    ret = pd.Series(ret).dropna()
    if len(ret) == 0:
        return {"annual": np.nan, "vol": np.nan, "sharpe": np.nan, "max_dd": np.nan, "total": np.nan}
    nav = (1.0 + ret).cumprod()
    total = nav.iloc[-1] - 1.0
    years = len(ret) / TRADING_DAYS
    annual = nav.iloc[-1] ** (1.0 / years) - 1.0 if years > 0 else np.nan
    vol = ret.std(ddof=0) * math.sqrt(TRADING_DAYS)
    sharpe = annual / vol if vol and not np.isnan(vol) and vol > 0 else np.nan
    max_dd = ((nav - nav.cummax()) / nav.cummax()).min()
    return {"annual": annual, "vol": vol, "sharpe": sharpe, "max_dd": max_dd, "total": total}


def rolling_percentile_rank(series: pd.Series, window: int) -> pd.Series:
    def _rank(arr: np.ndarray) -> float:
        last = arr[-1]
        return float(np.mean(arr <= last))

    return series.rolling(window).apply(_rank, raw=True)


def build_signal(rv_short: pd.Series,
                 rv_long: pd.Series,
                 pct_rank: pd.Series,
                 low_q: float,
                 high_q: float,
                 slope_lb: int) -> pd.Series:
    rising = (rv_short > rv_short.shift(slope_lb)) & (rv_short > rv_long)
    falling = (rv_short < rv_short.shift(slope_lb)) & (rv_short < rv_long)
    long_vol = (pct_rank <= low_q) & rising
    short_vol = (pct_rank >= high_q) & falling
    signal = pd.Series(0.0, index=rv_short.index)
    signal = signal.mask(long_vol, 1.0)
    signal = signal.mask(short_vol, -1.0)
    return signal


def run_vol_turn_proxy(close: pd.Series,
                       bil_close: pd.Series,
                       short_w: int,
                       long_w: int,
                       rank_w: int,
                       low_q: float,
                       high_q: float,
                       slope_lb: int,
                       dte: int,
                       iv_mult: float,
                       iv_floor: float) -> tuple[pd.Series, pd.Series]:
    ret = close.pct_change()
    bil_ret = bil_close.pct_change().reindex(close.index).fillna(0.0)
    rv_short = ret.rolling(short_w).std() * math.sqrt(TRADING_DAYS)
    rv_long = ret.rolling(long_w).std() * math.sqrt(TRADING_DAYS)
    pct_rank = rolling_percentile_rank(rv_long, rank_w)
    raw_signal = build_signal(rv_short, rv_long, pct_rank, low_q, high_q, slope_lb)
    signal = raw_signal.shift(1).fillna(0.0)

    out = pd.Series(0.0, index=close.index, dtype=float)
    sigma = np.maximum(rv_short * iv_mult, iv_floor)
    step_t = 1.0 / TRADING_DAYS
    init_t = dte / TRADING_DAYS

    for i in range(len(close) - 1):
        sig = float(signal.iloc[i])
        if sig == 0.0:
            out.iloc[i + 1] = bil_ret.iloc[i + 1]
            continue
        s0 = float(close.iloc[i])
        s1 = float(close.iloc[i + 1])
        if not np.isfinite(s0) or not np.isfinite(s1) or s0 <= 0 or s1 <= 0:
            out.iloc[i + 1] = bil_ret.iloc[i + 1]
            continue
        vol0 = float(sigma.iloc[i]) if np.isfinite(sigma.iloc[i]) else iv_floor
        vol1 = float(sigma.iloc[i + 1]) if np.isfinite(sigma.iloc[i + 1]) else iv_floor
        k = s0
        price0 = bs_straddle_price(s0, k, init_t, vol0, RF_ANNUAL)
        price1 = bs_straddle_price(s1, k, max(init_t - step_t, step_t), vol1, RF_ANNUAL)
        pnl = sig * (price1 - price0) / s0
        trade_fee = FEE_BPS / 10000.0 if raw_signal.iloc[i] != raw_signal.iloc[i - 1] else 0.0
        out.iloc[i + 1] = bil_ret.iloc[i + 1] + pnl - trade_fee

    return out.dropna(), raw_signal


def main() -> None:
    us = pd.read_csv(US_DATA, parse_dates=["date"]).set_index("date").sort_index()
    data = us[["SPY", "QQQ", "BIL"]].dropna()

    configs: list[dict[str, float | int | str]] = []
    for asset, iv_floor in [("SPY", 0.14), ("QQQ", 0.18)]:
        for short_w in [5, 10]:
            for long_w in [20, 40]:
                for low_q in [0.20, 0.30]:
                    for high_q in [0.70, 0.80]:
                        for slope_lb in [3, 5]:
                            for dte in [21, 30]:
                                for iv_mult in [1.00, 1.10, 1.20]:
                                    strat_ret, signal = run_vol_turn_proxy(
                                        close=data[asset],
                                        bil_close=data["BIL"],
                                        short_w=short_w,
                                        long_w=long_w,
                                        rank_w=252,
                                        low_q=low_q,
                                        high_q=high_q,
                                        slope_lb=slope_lb,
                                        dte=dte,
                                        iv_mult=iv_mult,
                                        iv_floor=iv_floor,
                                    )
                                    if len(strat_ret) == 0:
                                        continue
                                    m = calc_daily_metrics(strat_ret)
                                    active_days = int((signal != 0).sum())
                                    long_days = int((signal > 0).sum())
                                    short_days = int((signal < 0).sum())
                                    trades = int((signal != signal.shift(1)).sum())
                                    configs.append({
                                        "asset": asset,
                                        "short_w": short_w,
                                        "long_w": long_w,
                                        "low_q": low_q,
                                        "high_q": high_q,
                                        "slope_lb": slope_lb,
                                        "dte": dte,
                                        "iv_mult": iv_mult,
                                        "annual": m["annual"],
                                        "vol": m["vol"],
                                        "sharpe": m["sharpe"],
                                        "max_dd": m["max_dd"],
                                        "total": m["total"],
                                        "active_days": active_days,
                                        "long_days": long_days,
                                        "short_days": short_days,
                                        "trades": trades,
                                    })

    res = pd.DataFrame(configs).sort_values(["sharpe", "annual"], ascending=[False, False]).reset_index(drop=True)
    res.to_csv(OUT_CSV, index=False)

    top = res.head(15).copy()
    with OUT_MD.open("w", encoding="utf-8") as f:
        f.write("# Vol-Turn Option Proxy Scan\n\n")
        f.write("This is a stylized option-proxy backtest using daily SPY/QQQ/BIL prices and Black-Scholes repricing.\n")
        f.write("It is not a real option-chain execution backtest.\n\n")
        f.write("## Top 15\n\n")
        f.write("| rank | asset | short_w | long_w | low_q | high_q | slope_lb | dte | iv_mult | annual | vol | sharpe | max_dd | active_days | long_days | short_days |\n")
        f.write("|---:|:---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n")
        for i, row in top.iterrows():
            f.write(
                f"| {i+1} | {row['asset']} | {int(row['short_w'])} | {int(row['long_w'])} | "
                f"{row['low_q']:.2f} | {row['high_q']:.2f} | {int(row['slope_lb'])} | {int(row['dte'])} | "
                f"{row['iv_mult']:.2f} | {row['annual']:.2%} | {row['vol']:.2%} | {row['sharpe']:.2f} | "
                f"{row['max_dd']:.2%} | {int(row['active_days'])} | {int(row['long_days'])} | {int(row['short_days'])} |\n"
            )

    print(f"saved {OUT_CSV}")
    print(f"saved {OUT_MD}")
    print(res.head(10).to_string(index=False))


if __name__ == "__main__":
    main()
