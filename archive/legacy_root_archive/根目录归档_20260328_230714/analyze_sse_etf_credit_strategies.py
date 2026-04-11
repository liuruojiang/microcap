from __future__ import annotations

import json
import math
from dataclasses import dataclass
from pathlib import Path

import akshare as ak
import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parent
RF_ANNUAL = 0.02
TRADING_DAYS = 244
ROLL_DAYS = 21
PREMIUM_HAIRCUT = 0.90
TRADE_FEE_BPS = 5.0

ETF_CONFIG = {
    "510300": {"label": "沪深300ETF", "qvix_fn": "index_option_300etf_qvix", "start": "2019-12-23"},
    "510500": {"label": "中证500ETF", "qvix_fn": "index_option_500etf_qvix", "start": "2022-09-19"},
}


def norm_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def bs_put_price(spot: float, strike: float, t: float, sigma: float, r: float) -> float:
    if t <= 0:
        return max(strike - spot, 0.0)
    sigma = max(float(sigma), 1e-6)
    sqrt_t = math.sqrt(t)
    d1 = (math.log(max(spot, 1e-12) / max(strike, 1e-12)) + (r + 0.5 * sigma * sigma) * t) / (sigma * sqrt_t)
    d2 = d1 - sigma * sqrt_t
    return strike * math.exp(-r * t) * norm_cdf(-d2) - spot * norm_cdf(-d1)


def load_etf_hist(symbol: str) -> pd.DataFrame:
    df = ak.fund_etf_hist_em(symbol=symbol, period="daily", adjust="qfq")
    df = df.rename(columns={"日期": "date", "开盘": "open", "收盘": "close"})
    df["date"] = pd.to_datetime(df["date"])
    df = df.set_index("date").sort_index()
    return df[["open", "close"]].apply(pd.to_numeric, errors="coerce").dropna()


def load_qvix(fn_name: str) -> pd.Series:
    df = getattr(ak, fn_name)().copy()
    df["date"] = pd.to_datetime(df["date"])
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    ser = df.set_index("date")["close"].sort_index().replace(0, np.nan).dropna() / 100.0
    return ser


@dataclass
class PositionSpec:
    kind: str
    short_m: float
    long_m: float | None = None

    @property
    def label(self) -> str:
        if self.kind == "csp":
            return f"CSP_{int(self.short_m*100)}"
        return f"BPS_{int(self.short_m*100)}/{int(self.long_m*100)}"


def build_trade_dates(index: pd.DatetimeIndex) -> list[int]:
    locs = list(range(0, len(index), ROLL_DAYS))
    if locs[-1] != len(index) - 1:
        locs.append(len(index) - 1)
    return locs


def simulate_strategy(df: pd.DataFrame, spec: PositionSpec, use_filter: bool) -> pd.Series:
    close = df["close"]
    qvix = df["qvix"]
    ma200 = close.rolling(200).mean()

    trade_locs = build_trade_dates(df.index)
    equity = pd.Series(index=df.index, dtype=float)
    account = 1.0

    for i in range(len(trade_locs) - 1):
        start_loc = trade_locs[i]
        end_loc = trade_locs[i + 1]
        start_date = df.index[start_loc]
        end_date = df.index[end_loc]

        s0 = float(close.iloc[start_loc])
        iv0 = float(qvix.iloc[start_loc]) if pd.notna(qvix.iloc[start_loc]) else np.nan
        if not np.isfinite(iv0):
            iv0 = float(qvix.iloc[: start_loc + 1].dropna().iloc[-1]) if len(qvix.iloc[: start_loc + 1].dropna()) else 0.20
        iv0 = max(iv0, 0.12)

        allow = True
        if use_filter:
            ma_ok = pd.notna(ma200.iloc[start_loc]) and s0 > float(ma200.iloc[start_loc])
            vol_ok = iv0 < 0.35
            allow = ma_ok and vol_ok

        path_idx = df.index[start_loc : end_loc + 1]
        if not allow:
            equity.loc[path_idx] = account
            continue

        k_short = s0 * spec.short_m
        p_short_0 = bs_put_price(s0, k_short, ROLL_DAYS / TRADING_DAYS, iv0, RF_ANNUAL)

        if spec.kind == "csp":
            credit0 = p_short_0 * PREMIUM_HAIRCUT
            capital = k_short

            for j, dt in enumerate(path_idx):
                s1 = float(close.loc[dt])
                iv1 = float(qvix.loc[dt]) if pd.notna(qvix.loc[dt]) else iv0
                iv1 = max(iv1, 0.12)
                rem = max((end_loc - (start_loc + j)) / TRADING_DAYS, 0.0)
                liability = bs_put_price(s1, k_short, rem, iv1, RF_ANNUAL) if rem > 0 else max(k_short - s1, 0.0)
                equity.loc[dt] = account * (1.0 + (credit0 - liability) / capital)
        else:
            assert spec.long_m is not None
            k_long = s0 * spec.long_m
            p_long_0 = bs_put_price(s0, k_long, ROLL_DAYS / TRADING_DAYS, iv0, RF_ANNUAL)
            credit0 = (p_short_0 - p_long_0) * PREMIUM_HAIRCUT
            width = max(k_short - k_long, 1e-9)
            capital = width

            for j, dt in enumerate(path_idx):
                s1 = float(close.loc[dt])
                iv1 = float(qvix.loc[dt]) if pd.notna(qvix.loc[dt]) else iv0
                iv1 = max(iv1, 0.12)
                rem = max((end_loc - (start_loc + j)) / TRADING_DAYS, 0.0)
                if rem > 0:
                    short_liab = bs_put_price(s1, k_short, rem, iv1, RF_ANNUAL)
                    long_asset = bs_put_price(s1, k_long, rem, iv1, RF_ANNUAL)
                    liability = short_liab - long_asset
                else:
                    liability = max(k_short - s1, 0.0) - max(k_long - s1, 0.0)
                equity.loc[dt] = account * (1.0 + (credit0 - liability) / capital)

        turnover_cost = TRADE_FEE_BPS / 10000.0
        account = float(equity.loc[end_date]) * (1.0 - turnover_cost)
        equity.loc[end_date] = account

    equity = equity.ffill().dropna()
    return equity.pct_change().fillna(0.0)


def calc_metrics(ret: pd.Series) -> dict:
    ret = ret.dropna()
    nav = (1.0 + ret).cumprod()
    years = (ret.index[-1] - ret.index[0]).days / 365.25
    annual = nav.iloc[-1] ** (1.0 / years) - 1.0 if years > 0 else np.nan
    vol = ret.std(ddof=1) * np.sqrt(TRADING_DAYS)
    sharpe = annual / vol if pd.notna(vol) and vol > 0 else np.nan
    max_dd = ((nav - nav.cummax()) / nav.cummax()).min()
    total = nav.iloc[-1] - 1.0
    return {
        "annual": float(annual),
        "vol": float(vol),
        "sharpe": float(sharpe),
        "max_dd": float(max_dd),
        "total": float(total),
    }


def run_all() -> dict:
    specs = [
        PositionSpec("csp", 0.95),
        PositionSpec("csp", 0.97),
        PositionSpec("csp", 0.99),
        PositionSpec("bps", 0.97, 0.92),
        PositionSpec("bps", 0.98, 0.93),
        PositionSpec("bps", 0.99, 0.94),
    ]
    summary: dict[str, dict] = {}

    for symbol, cfg in ETF_CONFIG.items():
        price = load_etf_hist(symbol)
        qvix = load_qvix(cfg["qvix_fn"])
        df = price.join(qvix.rename("qvix"), how="left").ffill()
        df = df.loc[cfg["start"] :].copy()

        per_symbol = {
            "sample_start": df.index.min().strftime("%Y-%m-%d"),
            "sample_end": df.index.max().strftime("%Y-%m-%d"),
            "buy_hold": calc_metrics(df["close"].pct_change().fillna(0.0)),
            "strategies": {},
        }

        for spec in specs:
            for filt in [False, True]:
                key = spec.label + ("_filter" if filt else "_always")
                ret = simulate_strategy(df, spec, use_filter=filt)
                per_symbol["strategies"][key] = calc_metrics(ret)

        summary[symbol] = per_symbol

    return summary


def main():
    summary = run_all()
    out = ROOT / "sse_etf_credit_strategy_summary.json"
    out.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
