from __future__ import annotations

import math
from pathlib import Path

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parent
US_DATA = ROOT / "mnt_strategy_data_us.csv"
CACHE_DIR = ROOT / ".cboe_cache"
VIX_CSV = CACHE_DIR / "VIX_History.csv"
VIX3M_CSV = CACHE_DIR / "VIX3M_History.csv"
OUT_CSV = ROOT / "cboe_vix3m_term_structure_scan_results.csv"
OUT_MD = ROOT / "mnt_cboe_vix3m_term_structure_scan_20260328.md"

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


def load_cboe_series(path: Path, name: str) -> pd.Series:
    df = pd.read_csv(path)
    df["DATE"] = pd.to_datetime(df["DATE"], format="%m/%d/%Y")
    df["CLOSE"] = pd.to_numeric(df["CLOSE"], errors="coerce")
    return df.set_index("DATE")["CLOSE"].rename(name).sort_index().dropna()


def run_vix_vix3m_proxy(spy_close: pd.Series,
                        bil_close: pd.Series,
                        vix: pd.Series,
                        vix3m: pd.Series,
                        low_ratio: float,
                        high_ratio: float,
                        dte: int,
                        vol_cap: float | None,
                        slope_filter: bool) -> tuple[pd.Series, pd.Series]:
    df = pd.concat(
        [spy_close.rename("SPY"), bil_close.rename("BIL"), vix.rename("VIX"), vix3m.rename("VIX3M")],
        axis=1,
        join="inner",
    ).dropna()

    ratio = df["VIX"] / df["VIX3M"]
    vix_slope = df["VIX"].diff(3)
    raw_signal = pd.Series(0.0, index=df.index)
    long_mask = ratio >= high_ratio
    short_mask = ratio <= low_ratio
    if vol_cap is not None:
        short_mask &= df["VIX"] <= vol_cap
    if slope_filter:
        long_mask &= vix_slope > 0
        short_mask &= vix_slope < 0
    raw_signal = raw_signal.mask(long_mask, 1.0)
    raw_signal = raw_signal.mask(short_mask, -1.0)
    signal = raw_signal.shift(1).fillna(0.0)

    spy = df["SPY"]
    bil_ret = df["BIL"].pct_change().fillna(0.0)
    sigma = (df["VIX"] / 100.0).clip(lower=0.08)
    init_t = dte / TRADING_DAYS
    step_t = 1.0 / TRADING_DAYS
    out = pd.Series(0.0, index=df.index, dtype=float)

    for i in range(len(df) - 1):
        sig = float(signal.iloc[i])
        if sig == 0.0:
            out.iloc[i + 1] = bil_ret.iloc[i + 1]
            continue
        s0 = float(spy.iloc[i])
        s1 = float(spy.iloc[i + 1])
        if not np.isfinite(s0) or not np.isfinite(s1) or s0 <= 0 or s1 <= 0:
            out.iloc[i + 1] = bil_ret.iloc[i + 1]
            continue
        vol0 = float(sigma.iloc[i])
        vol1 = float(sigma.iloc[i + 1])
        price0 = bs_straddle_price(s0, s0, init_t, vol0, RF_ANNUAL)
        price1 = bs_straddle_price(s1, s0, max(init_t - step_t, step_t), vol1, RF_ANNUAL)
        pnl = sig * (price1 - price0) / s0
        trade_fee = FEE_BPS / 10000.0 if raw_signal.iloc[i] != raw_signal.iloc[i - 1] else 0.0
        out.iloc[i + 1] = bil_ret.iloc[i + 1] + pnl - trade_fee

    return out.dropna(), raw_signal


def main() -> None:
    if not VIX_CSV.exists() or not VIX3M_CSV.exists():
        raise FileNotFoundError("Missing Cboe cache. Need VIX and VIX3M CSV.")

    us = pd.read_csv(US_DATA, parse_dates=["date"]).set_index("date").sort_index()
    spy = us["SPY"].dropna()
    bil = us["BIL"].dropna()
    vix = load_cboe_series(VIX_CSV, "VIX")
    vix3m = load_cboe_series(VIX3M_CSV, "VIX3M")

    rows: list[dict[str, float | int | str]] = []
    for low_ratio, high_ratio in [(0.88, 1.00), (0.90, 1.00), (0.92, 1.02), (0.95, 1.05)]:
        for dte in [21, 30]:
            for vol_cap in [25.0, 30.0, None]:
                for slope_filter in [False, True]:
                    strat_ret, signal = run_vix_vix3m_proxy(
                        spy_close=spy,
                        bil_close=bil,
                        vix=vix,
                        vix3m=vix3m,
                        low_ratio=low_ratio,
                        high_ratio=high_ratio,
                        dte=dte,
                        vol_cap=vol_cap,
                        slope_filter=slope_filter,
                    )
                    if len(strat_ret) == 0:
                        continue
                    m = calc_daily_metrics(strat_ret)
                    rows.append({
                        "low_ratio": low_ratio,
                        "high_ratio": high_ratio,
                        "dte": dte,
                        "vol_cap": "None" if vol_cap is None else vol_cap,
                        "slope_filter": slope_filter,
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
        f.write("# Cboe VIX/VIX3M Term Structure Proxy Scan\n\n")
        f.write("Signal source: official Cboe VIX and VIX3M daily history.\n")
        f.write("Execution proxy: SPY ATM straddle repriced daily using VIX as implied vol.\n")
        f.write("This is a real term-structure signal source with proxy execution.\n\n")
        f.write("## Top 15\n\n")
        f.write("| rank | low_ratio | high_ratio | dte | vol_cap | slope_filter | annual | vol | sharpe | max_dd | active_days | long_days | short_days |\n")
        f.write("|---:|---:|---:|---:|:---:|:---:|---:|---:|---:|---:|---:|---:|---:|\n")
        for i, row in top.iterrows():
            f.write(
                f"| {i+1} | {row['low_ratio']:.2f} | {row['high_ratio']:.2f} | {int(row['dte'])} | "
                f"{row['vol_cap']} | {bool(row['slope_filter'])} | {row['annual']:.2%} | {row['vol']:.2%} | "
                f"{row['sharpe']:.2f} | {row['max_dd']:.2%} | {int(row['active_days'])} | "
                f"{int(row['long_days'])} | {int(row['short_days'])} |\n"
            )

    print(f"saved {OUT_CSV}")
    print(f"saved {OUT_MD}")
    print(res.head(10).to_string(index=False))


if __name__ == "__main__":
    main()
