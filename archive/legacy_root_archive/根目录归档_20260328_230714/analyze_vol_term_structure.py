from __future__ import annotations

import math
from pathlib import Path

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parent
US_DATA = ROOT / "mnt_strategy_data_us.csv"
OUT_CSV = ROOT / "vol_term_structure_scan_results.csv"
OUT_MD = ROOT / "mnt_vol_term_structure_scan_20260327.md"

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


def run_term_structure_proxy(close: pd.Series,
                             bil_close: pd.Series,
                             short_w: int,
                             long_w: int,
                             low_ratio: float,
                             high_ratio: float,
                             front_dte: int,
                             back_dte: int,
                             front_iv_mult: float,
                             back_iv_mult: float,
                             iv_floor: float) -> tuple[pd.Series, pd.Series]:
    ret = close.pct_change()
    bil_ret = bil_close.pct_change().reindex(close.index).fillna(0.0)

    rv_front = ret.rolling(short_w).std() * math.sqrt(TRADING_DAYS)
    rv_back = ret.rolling(long_w).std() * math.sqrt(TRADING_DAYS)
    ratio = rv_front / rv_back

    raw_signal = pd.Series(0.0, index=close.index)
    raw_signal = raw_signal.mask(ratio >= high_ratio, 1.0)   # backwardation: long calendar
    raw_signal = raw_signal.mask(ratio <= low_ratio, -1.0)   # contango: short calendar
    signal = raw_signal.shift(1).fillna(0.0)

    out = pd.Series(0.0, index=close.index, dtype=float)
    front_sigma = np.maximum(rv_front * front_iv_mult, iv_floor)
    back_sigma = np.maximum(rv_back * back_iv_mult, iv_floor)
    step_t = 1.0 / TRADING_DAYS
    init_front_t = front_dte / TRADING_DAYS
    init_back_t = back_dte / TRADING_DAYS

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
        f0 = float(front_sigma.iloc[i]) if np.isfinite(front_sigma.iloc[i]) else iv_floor
        b0 = float(back_sigma.iloc[i]) if np.isfinite(back_sigma.iloc[i]) else iv_floor
        f1 = float(front_sigma.iloc[i + 1]) if np.isfinite(front_sigma.iloc[i + 1]) else iv_floor
        b1 = float(back_sigma.iloc[i + 1]) if np.isfinite(back_sigma.iloc[i + 1]) else iv_floor

        k = s0
        price0 = (
            bs_straddle_price(s0, k, init_back_t, b0, RF_ANNUAL)
            - bs_straddle_price(s0, k, init_front_t, f0, RF_ANNUAL)
        )
        price1 = (
            bs_straddle_price(s1, k, max(init_back_t - step_t, step_t), b1, RF_ANNUAL)
            - bs_straddle_price(s1, k, max(init_front_t - step_t, step_t), f1, RF_ANNUAL)
        )
        pnl = sig * (price1 - price0) / s0
        trade_fee = FEE_BPS / 10000.0 if raw_signal.iloc[i] != raw_signal.iloc[i - 1] else 0.0
        out.iloc[i + 1] = bil_ret.iloc[i + 1] + pnl - trade_fee

    return out.dropna(), raw_signal


def main() -> None:
    us = pd.read_csv(US_DATA, parse_dates=["date"]).set_index("date").sort_index()
    data = us[["SPY", "QQQ", "BIL"]].dropna()

    rows: list[dict[str, float | int | str]] = []
    for asset, iv_floor in [("SPY", 0.14), ("QQQ", 0.18)]:
        for short_w, long_w in [(10, 40), (10, 60), (20, 60)]:
            for low_ratio, high_ratio in [(0.85, 1.10), (0.90, 1.10), (0.90, 1.15)]:
                for front_dte, back_dte in [(21, 63), (30, 90)]:
                    for front_iv_mult, back_iv_mult in [(1.15, 1.00), (1.20, 1.00), (1.20, 1.05)]:
                        strat_ret, signal = run_term_structure_proxy(
                            close=data[asset],
                            bil_close=data["BIL"],
                            short_w=short_w,
                            long_w=long_w,
                            low_ratio=low_ratio,
                            high_ratio=high_ratio,
                            front_dte=front_dte,
                            back_dte=back_dte,
                            front_iv_mult=front_iv_mult,
                            back_iv_mult=back_iv_mult,
                            iv_floor=iv_floor,
                        )
                        if len(strat_ret) == 0:
                            continue
                        m = calc_daily_metrics(strat_ret)
                        rows.append({
                            "asset": asset,
                            "short_w": short_w,
                            "long_w": long_w,
                            "low_ratio": low_ratio,
                            "high_ratio": high_ratio,
                            "front_dte": front_dte,
                            "back_dte": back_dte,
                            "front_iv_mult": front_iv_mult,
                            "back_iv_mult": back_iv_mult,
                            "annual": m["annual"],
                            "vol": m["vol"],
                            "sharpe": m["sharpe"],
                            "max_dd": m["max_dd"],
                            "total": m["total"],
                            "active_days": int((signal != 0).sum()),
                            "long_days": int((signal > 0).sum()),
                            "short_days": int((signal < 0).sum()),
                            "trades": int((signal != signal.shift(1)).sum()),
                        })

    res = pd.DataFrame(rows).sort_values(["sharpe", "annual"], ascending=[False, False]).reset_index(drop=True)
    res.to_csv(OUT_CSV, index=False)

    top = res.head(15)
    with OUT_MD.open("w", encoding="utf-8") as f:
        f.write("# Vol Term Structure Proxy Scan\n\n")
        f.write("This is a stylized ATM calendar-spread proxy backtest using daily SPY/QQQ/BIL prices.\n")
        f.write("It is not a real option-chain backtest.\n\n")
        f.write("## Top 15\n\n")
        f.write("| rank | asset | short_w | long_w | low_ratio | high_ratio | front_dte | back_dte | front_iv_mult | back_iv_mult | annual | vol | sharpe | max_dd | active_days | long_days | short_days |\n")
        f.write("|---:|:---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n")
        for i, row in top.iterrows():
            f.write(
                f"| {i+1} | {row['asset']} | {int(row['short_w'])} | {int(row['long_w'])} | "
                f"{row['low_ratio']:.2f} | {row['high_ratio']:.2f} | {int(row['front_dte'])} | {int(row['back_dte'])} | "
                f"{row['front_iv_mult']:.2f} | {row['back_iv_mult']:.2f} | {row['annual']:.2%} | {row['vol']:.2%} | "
                f"{row['sharpe']:.2f} | {row['max_dd']:.2%} | {int(row['active_days'])} | {int(row['long_days'])} | {int(row['short_days'])} |\n"
            )

    print(f"saved {OUT_CSV}")
    print(f"saved {OUT_MD}")
    print(res.head(10).to_string(index=False))


if __name__ == "__main__":
    main()
