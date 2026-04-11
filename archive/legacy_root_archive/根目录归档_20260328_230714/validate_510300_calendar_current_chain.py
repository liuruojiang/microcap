from __future__ import annotations

import json
from pathlib import Path

import akshare as ak
import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parent
CACHE_DIR = ROOT / ".sse_510300_chain_cache"
DAILY_DIR = CACHE_DIR / "daily"
OUT_JSON = ROOT / "validate_510300_calendar_current_chain.json"

UNDERLYING = "510300"


def load_etf_close() -> pd.Series:
    df = ak.fund_etf_hist_em(symbol=UNDERLYING, period="daily", adjust="qfq")
    df = df.rename(columns={"日期": "date", "收盘": "close"})
    df["date"] = pd.to_datetime(df["date"])
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    return df.set_index("date")["close"].sort_index().dropna()


def load_catalog() -> pd.DataFrame:
    cat = pd.read_csv(CACHE_DIR / "catalog_510300_current.csv")
    cat["option_code"] = cat["合约编码"].astype(str)
    cat["strike"] = pd.to_numeric(cat["行权价"], errors="coerce")
    cat["expiry"] = pd.to_datetime(cat["到期日"], errors="coerce")
    cat["start"] = pd.to_datetime(cat["开始日期"], errors="coerce")
    cat["side_en"] = cat["side"].map({"call": "call", "put": "put"})
    return cat[["option_code", "trade_month", "side_en", "strike", "expiry", "start"]].copy()


def load_daily_closes(catalog: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for code in catalog["option_code"]:
        p = DAILY_DIR / f"{code}.csv"
        if not p.exists():
            continue
        df = pd.read_csv(p)
        df["日期"] = pd.to_datetime(df["日期"], errors="coerce")
        df["收盘"] = pd.to_numeric(df["收盘"], errors="coerce")
        part = df[["日期", "收盘"]].copy()
        part["option_code"] = str(code)
        rows.append(part.rename(columns={"日期": "date", "收盘": "close"}))
    all_df = pd.concat(rows, ignore_index=True)
    all_df["date"] = pd.to_datetime(all_df["date"])
    all_df["option_code"] = all_df["option_code"].astype(str)
    return all_df


def calc_metrics(ret: pd.Series) -> dict:
    ret = ret.dropna()
    nav = (1.0 + ret).cumprod()
    years = (ret.index[-1] - ret.index[0]).days / 365.25 if len(ret) > 1 else np.nan
    annual = nav.iloc[-1] ** (1.0 / years) - 1.0 if years and years > 0 else np.nan
    vol = ret.std(ddof=0) * np.sqrt(244)
    sharpe = annual / vol if pd.notna(vol) and vol > 0 else np.nan
    max_dd = ((nav - nav.cummax()) / nav.cummax()).min()
    return {
        "annual": None if pd.isna(annual) else float(annual),
        "vol": None if pd.isna(vol) else float(vol),
        "sharpe": None if pd.isna(sharpe) else float(sharpe),
        "max_dd": None if pd.isna(max_dd) else float(max_dd),
        "total": float(nav.iloc[-1] - 1.0),
    }


def build_calendar_series() -> tuple[pd.Series, pd.DataFrame]:
    etf_close = load_etf_close()
    cat = load_catalog()
    daily = load_daily_closes(cat)
    merged = daily.merge(cat, on="option_code", how="left")

    calendar_rows = []
    for dt, grp in merged.groupby("date"):
        etf_px = etf_close.get(dt)
        if pd.isna(etf_px):
            continue
        months = sorted(grp["trade_month"].dropna().unique().tolist())
        if len(months) < 2:
            continue
        front, back = months[0], months[1]
        g_front = grp[grp["trade_month"] == front]
        g_back = grp[grp["trade_month"] == back]
        common_strikes = sorted(set(g_front["strike"].dropna()) & set(g_back["strike"].dropna()))
        usable = []
        for strike in common_strikes:
            f_call = g_front[(g_front["side_en"] == "call") & (g_front["strike"] == strike)]
            f_put = g_front[(g_front["side_en"] == "put") & (g_front["strike"] == strike)]
            b_call = g_back[(g_back["side_en"] == "call") & (g_back["strike"] == strike)]
            b_put = g_back[(g_back["side_en"] == "put") & (g_back["strike"] == strike)]
            if min(len(f_call), len(f_put), len(b_call), len(b_put)) < 1:
                continue
            usable.append(
                {
                    "strike": strike,
                    "dist": abs(strike - etf_px),
                    "front_call": float(f_call.iloc[0]["close"]),
                    "front_put": float(f_put.iloc[0]["close"]),
                    "back_call": float(b_call.iloc[0]["close"]),
                    "back_put": float(b_put.iloc[0]["close"]),
                }
            )
        if not usable:
            continue
        best = sorted(usable, key=lambda x: (x["dist"], x["strike"]))[0]
        calendar_value = (best["back_call"] + best["back_put"]) - (best["front_call"] + best["front_put"])
        calendar_rows.append(
            {
                "date": dt,
                "etf_close": float(etf_px),
                "front_month": front,
                "back_month": back,
                "strike": float(best["strike"]),
                "calendar_value": float(calendar_value),
            }
        )

    cal = pd.DataFrame(calendar_rows).sort_values("date").reset_index(drop=True)
    cal["ret"] = cal["calendar_value"].diff() / cal["etf_close"].shift(1)
    cal["ret"] = cal["ret"].fillna(0.0)
    return cal.set_index("date")["ret"], cal


def main() -> None:
    ret, detail = build_calendar_series()
    summary = {
        "sample_start": str(detail["date"].min().date()) if not detail.empty else None,
        "sample_end": str(detail["date"].max().date()) if not detail.empty else None,
        "obs": int(len(detail)),
        "front_months": sorted(detail["front_month"].unique().tolist()) if not detail.empty else [],
        "back_months": sorted(detail["back_month"].unique().tolist()) if not detail.empty else [],
        "metrics": calc_metrics(ret),
    }
    detail.to_csv(ROOT / "validate_510300_calendar_current_chain_detail.csv", index=False, encoding="utf-8-sig")
    OUT_JSON.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
