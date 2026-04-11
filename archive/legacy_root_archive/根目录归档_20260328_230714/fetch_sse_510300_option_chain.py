from __future__ import annotations

import json
import time
from pathlib import Path

import akshare as ak
import pandas as pd


ROOT = Path(__file__).resolve().parent
OUT_DIR = ROOT / ".sse_510300_chain_cache"
DAILY_DIR = OUT_DIR / "daily"
OUT_DIR.mkdir(exist_ok=True)
DAILY_DIR.mkdir(exist_ok=True)

UNDERLYING = "510300"
SSE_SYMBOL = "300ETF"


def fetch_current_catalog() -> pd.DataFrame:
    df = ak.option_current_day_sse().copy()
    df["标的券名称及代码"] = df["标的券名称及代码"].astype(str)
    df = df[df["标的券名称及代码"].str.contains(f"({UNDERLYING})", regex=False)].copy()
    if df.empty:
        raise RuntimeError("No current 510300 option contracts found from SSE current-day catalog.")
    df["合约编码"] = df["合约编码"].astype(str)
    df["期权行权日"] = pd.to_datetime(df["期权行权日"], errors="coerce")
    df["到期日"] = pd.to_datetime(df["到期日"], errors="coerce")
    df["开始日期"] = pd.to_datetime(df["开始日期"], errors="coerce")
    df["行权价"] = pd.to_numeric(df["行权价"], errors="coerce")
    return df.sort_values(["到期日", "类型", "行权价"]).reset_index(drop=True)


def fetch_sina_month_code_lists() -> tuple[pd.DataFrame, pd.DataFrame]:
    months = ak.option_sse_list_sina(symbol=SSE_SYMBOL)
    rows = []
    for month in months:
        for side in ["看涨期权", "看跌期权"]:
            codes = ak.option_sse_codes_sina(symbol=side, trade_date=month, underlying=UNDERLYING).copy()
            codes["trade_month"] = month
            codes["side"] = "call" if side == "看涨期权" else "put"
            rows.append(codes)
            time.sleep(0.1)
    all_codes = pd.concat(rows, ignore_index=True)
    all_codes["期权代码"] = all_codes["期权代码"].astype(str)
    return pd.DataFrame({"trade_month": months}), all_codes


def merge_catalog(catalog: pd.DataFrame, sina_codes: pd.DataFrame) -> pd.DataFrame:
    merged = catalog.merge(
        sina_codes[["期权代码", "trade_month", "side"]],
        left_on="合约编码",
        right_on="期权代码",
        how="left",
    )
    merged = merged.drop(columns=["期权代码"])
    return merged


def fetch_contract_daily(option_code: str) -> pd.DataFrame:
    path = DAILY_DIR / f"{option_code}.csv"
    if path.exists():
        return pd.read_csv(path)
    df = ak.option_sse_daily_sina(symbol=option_code).copy()
    df.to_csv(path, index=False, encoding="utf-8-sig")
    time.sleep(0.1)
    return df


def main() -> None:
    catalog = fetch_current_catalog()
    months_df, sina_codes = fetch_sina_month_code_lists()
    merged = merge_catalog(catalog, sina_codes)

    merged.to_csv(OUT_DIR / "catalog_510300_current.csv", index=False, encoding="utf-8-sig")
    months_df.to_csv(OUT_DIR / "months_510300_current.csv", index=False, encoding="utf-8-sig")

    fetched = []
    for code in merged["合约编码"].astype(str).tolist():
        try:
            daily = fetch_contract_daily(code)
            fetched.append(
                {
                    "option_code": code,
                    "rows": len(daily),
                    "start": str(daily.iloc[0]["日期"]) if not daily.empty else None,
                    "end": str(daily.iloc[-1]["日期"]) if not daily.empty else None,
                }
            )
        except Exception as e:
            fetched.append({"option_code": code, "error": f"{type(e).__name__}: {e}"})

    pd.DataFrame(fetched).to_csv(OUT_DIR / "daily_fetch_status_510300.csv", index=False, encoding="utf-8-sig")

    summary = {
        "underlying": UNDERLYING,
        "months": months_df["trade_month"].tolist(),
        "contract_count": int(len(merged)),
        "call_count": int((merged["类型"] == "认购").sum()),
        "put_count": int((merged["类型"] == "认沽").sum()),
        "min_expiry": str(merged["到期日"].min().date()),
        "max_expiry": str(merged["到期日"].max().date()),
        "daily_dir": str(DAILY_DIR),
    }
    (OUT_DIR / "summary_510300_current.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
