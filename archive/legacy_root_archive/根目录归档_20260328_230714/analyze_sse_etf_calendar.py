from __future__ import annotations

import math
from pathlib import Path

import numpy as np
import pandas as pd
import akshare as ak


ROOT = Path(__file__).resolve().parent
OUT_CSV = ROOT / "sse_etf_calendar_scan_results.csv"
OUT_MD = ROOT / "mnt_sse_etf_calendar_scan_20260328.md"

TRADING_DAYS = 244
RF_ANNUAL = 0.02
FEE_BPS = 2.0
RF_DAILY = (1 + RF_ANNUAL) ** (1 / TRADING_DAYS) - 1

ETF_CONFIG = {
    "510300": {"label": "沪深300ETF", "qvix_fn": "index_option_300etf_qvix", "start": "2019-12-23"},
    "510500": {"label": "中证500ETF", "qvix_fn": "index_option_500etf_qvix", "start": "2022-09-19"},
}


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


def calc_metrics(ret: pd.Series) -> dict[str, float]:
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


def load_etf_data(symbol: str, qvix_fn: str, start: str) -> pd.DataFrame:
    etf = ak.fund_etf_hist_em(symbol=symbol, period="daily", adjust="qfq")
    etf = etf.rename(columns={"日期": "date", "收盘": "close"})
    etf["date"] = pd.to_datetime(etf["date"])
    etf["close"] = pd.to_numeric(etf["close"], errors="coerce")
    etf = etf.set_index("date")[["close"]].sort_index()

    qvix = getattr(ak, qvix_fn)().copy()
    qvix["date"] = pd.to_datetime(qvix["date"])
    qvix["close"] = pd.to_numeric(qvix["close"], errors="coerce")
    qvix = qvix.set_index("date")["close"].sort_index().rename("qvix")

    df = etf.join(qvix, how="left").ffill()
    df = df.loc[start:].dropna()
    return df


def rolling_percentile(series: pd.Series, window: int) -> pd.Series:
    def _rank(x: np.ndarray) -> float:
        arr = pd.Series(x)
        return arr.rank(pct=True).iloc[-1]
    return series.rolling(window).apply(_rank, raw=False)


def run_calendar_proxy(
    close: pd.Series,
    qvix: pd.Series,
    low_pct: float,
    high_pct: float,
    signal_mode: str,
    front_dte: int,
    back_dte: int,
    strike_mult: float,
    front_iv_mult: float,
    back_iv_mult: float,
) -> tuple[pd.Series, pd.Series]:
    rv20 = close.pct_change().rolling(20).std() * math.sqrt(TRADING_DAYS)
    rv60 = close.pct_change().rolling(60).std() * math.sqrt(TRADING_DAYS)
    qpct = rolling_percentile(qvix, 126)

    long_mask = (qpct <= low_pct) & (rv20 <= rv60)
    short_mask = qpct >= high_pct

    raw_signal = pd.Series(0.0, index=close.index)
    if signal_mode in ("long_only", "both"):
        raw_signal = raw_signal.mask(long_mask, 1.0)
    if signal_mode in ("short_only", "both"):
        raw_signal = raw_signal.mask(short_mask, -1.0)
    signal = raw_signal.shift(1).fillna(0.0)

    out = pd.Series(0.0, index=close.index, dtype=float)
    step_t = 1.0 / TRADING_DAYS
    init_front_t = front_dte / TRADING_DAYS
    init_back_t = back_dte / TRADING_DAYS

    for i in range(len(close) - 1):
        sig = float(signal.iloc[i])
        if sig == 0.0:
            out.iloc[i + 1] = RF_DAILY
            continue
        s0 = float(close.iloc[i])
        s1 = float(close.iloc[i + 1])
        if not np.isfinite(s0) or not np.isfinite(s1) or s0 <= 0 or s1 <= 0:
            out.iloc[i + 1] = RF_DAILY
            continue
        base_iv0 = max(float(qvix.iloc[i]) / 100.0, 0.10)
        base_iv1 = max(float(qvix.iloc[i + 1]) / 100.0, 0.10)
        f0 = base_iv0 * front_iv_mult
        b0 = base_iv0 * back_iv_mult
        f1 = base_iv1 * front_iv_mult
        b1 = base_iv1 * back_iv_mult

        k = s0 * strike_mult
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
        out.iloc[i + 1] = RF_DAILY + pnl - trade_fee

    return out.dropna(), raw_signal


def main() -> None:
    rows: list[dict[str, float | int | str]] = []
    for symbol, cfg in ETF_CONFIG.items():
        df = load_etf_data(symbol, cfg["qvix_fn"], cfg["start"])
        for low_pct, high_pct in [(0.2, 0.8), (0.3, 0.8), (0.3, 0.7)]:
            for signal_mode in ["long_only", "short_only", "both"]:
                for front_dte, back_dte in [(21, 63), (30, 90)]:
                    for strike_mult in [1.00, 1.02]:
                        for front_iv_mult, back_iv_mult in [(1.10, 1.00), (1.15, 1.00), (1.10, 1.05)]:
                            ret, signal = run_calendar_proxy(
                                close=df["close"],
                                qvix=df["qvix"],
                                low_pct=low_pct,
                                high_pct=high_pct,
                                signal_mode=signal_mode,
                                front_dte=front_dte,
                                back_dte=back_dte,
                                strike_mult=strike_mult,
                                front_iv_mult=front_iv_mult,
                                back_iv_mult=back_iv_mult,
                            )
                            if len(ret) == 0:
                                continue
                            m = calc_metrics(ret)
                            rows.append({
                                "symbol": symbol,
                                "low_pct": low_pct,
                                "high_pct": high_pct,
                                "signal_mode": signal_mode,
                                "front_dte": front_dte,
                                "back_dte": back_dte,
                                "strike_mult": strike_mult,
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

    res = pd.DataFrame(rows).sort_values(["symbol", "sharpe", "annual"], ascending=[True, False, False]).reset_index(drop=True)
    res.to_csv(OUT_CSV, index=False)

    with OUT_MD.open("w", encoding="utf-8") as f:
        f.write("# SSE ETF Calendar Proxy Scan\n\n")
        f.write("Stylized A-share ETF option calendar proxy using ETF daily prices and single-line QVIX.\n")
        f.write("Execution is not a real option-chain backtest.\n\n")
        for symbol in ETF_CONFIG:
            top = res[res["symbol"] == symbol].head(10)
            f.write(f"## {symbol}\n\n")
            f.write("| rank | mode | low_pct | high_pct | front_dte | back_dte | strike | front_iv | back_iv | annual | vol | sharpe | max_dd | active |\n")
            f.write("|---:|:---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n")
            for i, row in top.iterrows():
                f.write(
                    f"| {i+1} | {row['signal_mode']} | {row['low_pct']:.1f} | {row['high_pct']:.1f} | "
                    f"{int(row['front_dte'])} | {int(row['back_dte'])} | {row['strike_mult']:.2f} | "
                    f"{row['front_iv_mult']:.2f} | {row['back_iv_mult']:.2f} | {row['annual']:.2%} | "
                    f"{row['vol']:.2%} | {row['sharpe']:.2f} | {row['max_dd']:.2%} | {int(row['active_days'])} |\n"
                )

    print(f"saved {OUT_CSV}")
    print(f"saved {OUT_MD}")
    for symbol in ETF_CONFIG:
        print(f"\n=== {symbol} ===")
        print(res[res["symbol"] == symbol].head(10).to_string(index=False))


if __name__ == "__main__":
    main()
