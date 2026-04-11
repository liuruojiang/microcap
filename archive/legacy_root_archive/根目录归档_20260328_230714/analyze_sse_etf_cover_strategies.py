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
CALL_CREDIT_HAIRCUT = 0.90
PUT_COST_MARKUP = 1.03

ETF_CONFIG = {
    "510300": {"label": "沪深300ETF", "qvix_fn": "index_option_300etf_qvix", "start": "2019-12-23"},
    "510500": {"label": "中证500ETF", "qvix_fn": "index_option_500etf_qvix", "start": "2022-09-19"},
}


def norm_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def bs_call_price(spot: float, strike: float, t: float, sigma: float, r: float) -> float:
    if t <= 0:
        return max(spot - strike, 0.0)
    sigma = max(float(sigma), 1e-6)
    sqrt_t = math.sqrt(t)
    d1 = (math.log(max(spot, 1e-12) / max(strike, 1e-12)) + (r + 0.5 * sigma * sigma) * t) / (sigma * sqrt_t)
    d2 = d1 - sigma * sqrt_t
    return spot * norm_cdf(d1) - strike * math.exp(-r * t) * norm_cdf(d2)


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
    return df.set_index("date")["close"].sort_index().replace(0, np.nan).dropna() / 100.0


def build_roll_locs(index: pd.DatetimeIndex) -> list[int]:
    locs = list(range(0, len(index), ROLL_DAYS))
    if locs[-1] != len(index) - 1:
        locs.append(len(index) - 1)
    return locs


@dataclass
class StrategySpec:
    kind: str
    call_m: float
    put_m: float | None = None

    @property
    def label(self) -> str:
        if self.kind == "covered_call":
            return f"CC_{int(self.call_m*100)}"
        return f"Collar_{int(self.put_m*100)}/{int(self.call_m*100)}"


def simulate(df: pd.DataFrame, spec: StrategySpec) -> pd.Series:
    close = df["close"]
    qvix = df["qvix"].ffill()
    roll_locs = build_roll_locs(df.index)
    equity = pd.Series(index=df.index, dtype=float)
    account = 1.0

    for i in range(len(roll_locs) - 1):
        start_loc = roll_locs[i]
        end_loc = roll_locs[i + 1]
        start_date = df.index[start_loc]
        s0 = float(close.iloc[start_loc])
        iv0 = float(qvix.iloc[start_loc]) if pd.notna(qvix.iloc[start_loc]) else 0.25
        iv0 = max(iv0, 0.12)
        k_call = s0 * spec.call_m
        call0 = bs_call_price(s0, k_call, ROLL_DAYS / TRADING_DAYS, iv0, RF_ANNUAL)
        call_credit = call0 * CALL_CREDIT_HAIRCUT

        if spec.kind == "collar":
            assert spec.put_m is not None
            k_put = s0 * spec.put_m
            put0 = bs_put_price(s0, k_put, ROLL_DAYS / TRADING_DAYS, iv0, RF_ANNUAL)
            put_cost = put0 * PUT_COST_MARKUP
        else:
            k_put = None
            put_cost = 0.0

        path_idx = df.index[start_loc : end_loc + 1]
        for j, dt in enumerate(path_idx):
            s1 = float(close.loc[dt])
            iv1 = float(qvix.loc[dt]) if pd.notna(qvix.loc[dt]) else iv0
            iv1 = max(iv1, 0.12)
            rem = max((end_loc - (start_loc + j)) / TRADING_DAYS, 0.0)
            call_t = bs_call_price(s1, k_call, rem, iv1, RF_ANNUAL) if rem > 0 else max(s1 - k_call, 0.0)
            if spec.kind == "collar":
                put_t = bs_put_price(s1, k_put, rem, iv1, RF_ANNUAL) if rem > 0 else max(k_put - s1, 0.0)
            else:
                put_t = 0.0
            value = (s1 / s0) + (call_credit - call_t) / s0 + (put_t - put_cost) / s0
            equity.loc[dt] = account * value

        account = float(equity.loc[path_idx[-1]])

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
        StrategySpec("covered_call", 1.03),
        StrategySpec("covered_call", 1.05),
        StrategySpec("covered_call", 1.07),
        StrategySpec("collar", 1.03, 0.95),
        StrategySpec("collar", 1.05, 0.95),
        StrategySpec("collar", 1.05, 0.93),
    ]
    summary = {}

    for symbol, cfg in ETF_CONFIG.items():
        price = load_etf_hist(symbol)
        qvix = load_qvix(cfg["qvix_fn"])
        df = price.join(qvix.rename("qvix"), how="left").ffill()
        df = df.loc[cfg["start"] :].copy()
        out = {
            "sample_start": df.index.min().strftime("%Y-%m-%d"),
            "sample_end": df.index.max().strftime("%Y-%m-%d"),
            "buy_hold": calc_metrics(df["close"].pct_change().fillna(0.0)),
            "strategies": {},
        }
        for spec in specs:
            ret = simulate(df, spec)
            out["strategies"][spec.label] = calc_metrics(ret)
        summary[symbol] = out

    return summary


def main():
    summary = run_all()
    out = ROOT / "sse_etf_cover_strategy_summary.json"
    out.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
