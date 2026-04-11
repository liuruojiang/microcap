from __future__ import annotations

import json
import time
from pathlib import Path

import akshare as ak
import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parent
CACHE_DIR = ROOT / ".sse_option_stats_cache"
CACHE_DIR.mkdir(exist_ok=True)

SYMBOLS = {
    "510300": "沪深300ETF华泰柏瑞",
    "510500": "中证500ETF南方",
}
START_DATE = "2019-12-23"
END_DATE = "2026-03-27"


def load_etf_hist(symbol: str) -> pd.DataFrame:
    df = ak.fund_etf_hist_em(symbol=symbol, period="daily", adjust="qfq")
    df = df.rename(
        columns={
            "日期": "date",
            "开盘": "open",
            "收盘": "close",
            "最高": "high",
            "最低": "low",
            "成交量": "volume",
        }
    )
    df["date"] = pd.to_datetime(df["date"])
    df = df.set_index("date").sort_index()
    return df[["open", "close", "high", "low", "volume"]].apply(pd.to_numeric, errors="coerce")


def fetch_sse_stats_by_date(date_str: str) -> pd.DataFrame:
    cache_path = CACHE_DIR / f"{date_str}.csv"
    if cache_path.exists():
        return pd.read_csv(cache_path)
    df = ak.option_daily_stats_sse(date=date_str)
    df.to_csv(cache_path, index=False, encoding="utf-8-sig")
    time.sleep(0.15)
    return df


def build_option_stats(trading_days: pd.DatetimeIndex) -> pd.DataFrame:
    rows = []
    for i, dt in enumerate(trading_days):
        date_str = dt.strftime("%Y%m%d")
        try:
            daily = fetch_sse_stats_by_date(date_str)
        except Exception:
            continue
        daily["合约标的代码"] = daily["合约标的代码"].astype(str)
        for code, name in SYMBOLS.items():
            hit = daily[daily["合约标的代码"] == code]
            if hit.empty:
                continue
            r = hit.iloc[0]
            rows.append(
                {
                    "date": dt,
                    "symbol": code,
                    "name": name,
                    "call_volume": float(r["认购成交量"]),
                    "put_volume": float(r["认沽成交量"]),
                    "total_volume": float(r["总成交量"]),
                    "oi_total": float(r["未平仓合约总数"]),
                    "oi_call": float(r["未平仓认购合约数"]),
                    "oi_put": float(r["未平仓认沽合约数"]),
                    "pcr_raw": float(r["认沽/认购"]) / 100.0,
                }
            )
        if (i + 1) % 100 == 0:
            print(f"fetched {i+1}/{len(trading_days)} trading days")
    out = pd.DataFrame(rows)
    if out.empty:
        raise RuntimeError("No option stats were collected from SSE")
    return out.sort_values(["symbol", "date"]).reset_index(drop=True)


def calc_metrics(ret: pd.Series) -> dict:
    ret = ret.dropna()
    if ret.empty:
        return {"annual": np.nan, "vol": np.nan, "sharpe": np.nan, "max_dd": np.nan, "total": np.nan}
    nav = (1.0 + ret).cumprod()
    total = nav.iloc[-1] - 1.0
    years = (ret.index[-1] - ret.index[0]).days / 365.25
    annual = nav.iloc[-1] ** (1.0 / years) - 1.0 if years > 0 else np.nan
    vol = ret.std(ddof=1) * np.sqrt(244)
    sharpe = annual / vol if pd.notna(vol) and vol > 0 else np.nan
    max_dd = ((nav - nav.cummax()) / nav.cummax()).min()
    return {
        "annual": float(annual),
        "vol": float(vol),
        "sharpe": float(sharpe),
        "max_dd": float(max_dd),
        "total": float(total),
    }


def run_symbol_backtest(price_df: pd.DataFrame, option_df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    df = price_df.join(option_df.set_index("date"), how="inner").copy()
    df = df.sort_index()
    df["put5"] = df["put_volume"].rolling(5).sum()
    df["call5"] = df["call_volume"].rolling(5).sum()
    df["pcr5"] = df["put5"] / df["call5"].replace(0, np.nan)
    df["q70"] = df["pcr5"].rolling(60).quantile(0.7)
    df["q30"] = df["pcr5"].rolling(60).quantile(0.3)
    df["signal_long_only"] = np.where(df["pcr5"] >= df["q70"], 1.0, 0.0)
    df["signal_long_short"] = np.where(df["pcr5"] >= df["q70"], 1.0, np.where(df["pcr5"] <= df["q30"], -1.0, 0.0))
    df["oo_ret"] = df["open"].pct_change().shift(-1)
    df["strat_ret_long_only"] = df["signal_long_only"].shift(1) * df["oo_ret"]
    df["strat_ret_long_short"] = df["signal_long_short"].shift(1) * df["oo_ret"]
    df["buy_hold_ret"] = df["oo_ret"]
    metrics = {
        "sample_start": df.index.min().strftime("%Y-%m-%d"),
        "sample_end": df.index.max().strftime("%Y-%m-%d"),
        "obs": int(df["pcr5"].notna().sum()),
        "long_only": calc_metrics(df["strat_ret_long_only"]),
        "long_short": calc_metrics(df["strat_ret_long_short"]),
        "buy_hold": calc_metrics(df["buy_hold_ret"]),
        "long_only_exposure": float(df["signal_long_only"].mean()),
        "long_short_long_exposure": float((df["signal_long_short"] > 0).mean()),
        "long_short_short_exposure": float((df["signal_long_short"] < 0).mean()),
    }
    return df, metrics


def main():
    price_map = {symbol: load_etf_hist(symbol) for symbol in SYMBOLS}
    all_days = sorted(set().union(*(df.loc[START_DATE:END_DATE].index for df in price_map.values())))
    option_stats = build_option_stats(pd.DatetimeIndex(all_days))
    option_stats.to_csv(ROOT / "sse_300_500_option_stats.csv", index=False, encoding="utf-8-sig")

    summary = {}
    for symbol in SYMBOLS:
        price_df = price_map[symbol].loc[START_DATE:END_DATE]
        option_df = option_stats[option_stats["symbol"] == symbol].copy()
        detail_df, metrics = run_symbol_backtest(price_df, option_df)
        detail_df.to_csv(ROOT / f"sse_{symbol}_pcr_backtest_detail.csv", encoding="utf-8-sig")
        summary[symbol] = metrics

    out_path = ROOT / "sse_300_500_pcr_summary.json"
    out_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
