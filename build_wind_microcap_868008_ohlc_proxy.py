from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import akshare as ak
import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parent
OUTPUT_DIR = ROOT / "outputs"
LOCAL_OHLC_CACHE_DIR = ROOT / ".microcap_ohlc_cache"

BASE_INDEX_CSV = OUTPUT_DIR / "wind_microcap_868008_monthly_16y.csv"
MEMBERS_CSV = OUTPUT_DIR / "wind_microcap_868008_monthly_16y_members.csv"

OUT_OHLC_CSV = OUTPUT_DIR / "wind_microcap_868008_monthly_16y_ohlc.csv"
OUT_OHLC_ANCHORED_CSV = OUTPUT_DIR / "wind_microcap_868008_monthly_16y_ohlc_anchored.csv"
OUT_COMPARE_CSV = OUTPUT_DIR / "wind_microcap_868008_monthly_16y_ohlc_compare.csv"
OUT_COMPARE_ANCHORED_CSV = OUTPUT_DIR / "wind_microcap_868008_monthly_16y_ohlc_anchored_compare.csv"
OUT_META_JSON = OUTPUT_DIR / "wind_microcap_868008_monthly_16y_ohlc_meta.json"

MAX_WORKERS = 6


def find_shared_ohlc_cache_dir() -> Path | None:
    for child in ROOT.parent.iterdir():
        candidate = child / ".microcap_index_cache" / "prices_ohlc"
        if candidate.exists():
            return candidate
    return None


def load_members() -> pd.DataFrame:
    df = pd.read_csv(MEMBERS_CSV, dtype={"symbol": str, "rank": int})
    df["symbol"] = df["symbol"].str.zfill(6)
    df["rebalance_date"] = pd.to_datetime(df["rebalance_date"])
    return df.sort_values(["rebalance_date", "rank"]).reset_index(drop=True)


def load_base_index() -> pd.DataFrame:
    df = pd.read_csv(BASE_INDEX_CSV)
    df["date"] = pd.to_datetime(df["date"])
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    return df.dropna(subset=["date", "close"]).sort_values("date").reset_index(drop=True)


def build_daily_members(
    trading_dates: pd.DatetimeIndex,
    members: pd.DataFrame,
) -> tuple[dict[pd.Timestamp, list[str]], dict[pd.Timestamp, list[str]]]:
    by_rebalance = {
        pd.Timestamp(dt): grp["symbol"].tolist()
        for dt, grp in members.groupby("rebalance_date", observed=False)
    }
    rebalance_set = set(by_rebalance)
    current_members: list[str] = []
    daily_members: dict[pd.Timestamp, list[str]] = {}

    for i, dt in enumerate(trading_dates):
        if i > 0 and trading_dates[i - 1] in rebalance_set:
            current_members = by_rebalance[pd.Timestamp(trading_dates[i - 1])]
        daily_members[pd.Timestamp(dt)] = current_members.copy()
    return by_rebalance, daily_members


def read_ohlc_csv(path: Path, start_ts: pd.Timestamp, end_ts: pd.Timestamp) -> pd.DataFrame | None:
    try:
        df = pd.read_csv(path)
    except Exception:
        return None
    required = {"date", "open", "close", "high", "low"}
    if not required.issubset(df.columns):
        return None
    df["date"] = pd.to_datetime(df["date"])
    for col in ["open", "close", "high", "low"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna(subset=["date", "open", "close", "high", "low"]).sort_values("date")
    return df[(df["date"] >= start_ts) & (df["date"] <= end_ts)].copy()


def fetch_ohlc_from_akshare_daily(symbol: str, start_date: str, end_date: str) -> pd.DataFrame | None:
    symbol_prefixed = ("sh" if symbol.startswith("6") else "sz") + symbol
    for _ in range(3):
        try:
            temp_df = ak.stock_zh_a_daily(symbol=symbol_prefixed, adjust="")
            if temp_df is None or temp_df.empty:
                continue
            required = ["date", "open", "close", "high", "low"]
            if not set(required).issubset(temp_df.columns):
                continue
            temp_df = temp_df[required].copy()
            temp_df["date"] = pd.to_datetime(temp_df["date"], errors="coerce")
            for col in ["open", "close", "high", "low"]:
                temp_df[col] = pd.to_numeric(temp_df[col], errors="coerce")
            temp_df = temp_df.dropna(subset=["date", "open", "close", "high", "low"]).sort_values("date")
            temp_df = temp_df[
                (temp_df["date"] >= pd.Timestamp(start_date)) & (temp_df["date"] <= pd.Timestamp(end_date))
            ]
            if not temp_df.empty:
                return temp_df
        except Exception:
            continue
    return None


def load_or_fetch_symbol_ohlc(
    symbol: str,
    start_date: str,
    end_date: str,
    shared_cache_dir: Path | None,
) -> tuple[str, pd.DataFrame | None, str]:
    start_ts = pd.Timestamp(start_date)
    end_ts = pd.Timestamp(end_date)
    LOCAL_OHLC_CACHE_DIR.mkdir(parents=True, exist_ok=True)

    local_path = LOCAL_OHLC_CACHE_DIR / f"{symbol}.csv"
    local_df = read_ohlc_csv(local_path, start_ts, end_ts)
    if local_df is not None and not local_df.empty:
        return symbol, local_df, "local_cache"

    if shared_cache_dir is not None:
        shared_path = shared_cache_dir / f"{symbol}.csv"
        shared_df = read_ohlc_csv(shared_path, start_ts, end_ts)
        if shared_df is not None and not shared_df.empty:
            return symbol, shared_df, "shared_cache"

    fetched = fetch_ohlc_from_akshare_daily(symbol, start_date, end_date)
    if fetched is None or fetched.empty:
        return symbol, None, "missing"
    fetched.to_csv(local_path, index=False, encoding="utf-8")
    return symbol, fetched[(fetched["date"] >= start_ts) & (fetched["date"] <= end_ts)].copy(), "fetched"


def load_ohlc_batch(
    symbols: list[str],
    start_date: str,
    end_date: str,
    shared_cache_dir: Path | None,
) -> tuple[dict[str, pd.DataFrame], dict[str, int]]:
    out: dict[str, pd.DataFrame] = {}
    stats = {"local_cache": 0, "shared_cache": 0, "fetched": 0, "missing": 0}

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {
            pool.submit(load_or_fetch_symbol_ohlc, symbol, start_date, end_date, shared_cache_dir): symbol
            for symbol in symbols
        }
        for fut in as_completed(futures):
            symbol, df, source = fut.result()
            stats[source] += 1
            if df is not None and not df.empty:
                out[symbol] = df
    return out, stats


def prepare_ratio_cache(stock_data: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    out: dict[str, pd.DataFrame] = {}
    for symbol, df in stock_data.items():
        work = df.copy().sort_values("date")
        work["prev_close"] = work["close"].shift(1)
        work["ret_open_pc"] = work["open"] / work["prev_close"] - 1.0
        work["ret_high_pc"] = work["high"] / work["prev_close"] - 1.0
        work["ret_low_pc"] = work["low"] / work["prev_close"] - 1.0
        work["ret_close_pc"] = work["close"] / work["prev_close"] - 1.0
        out[symbol] = work.set_index("date")
    return out


def build_proxy_ohlc(
    trading_dates: pd.DatetimeIndex,
    daily_members: dict[pd.Timestamp, list[str]],
    ratio_cache: dict[str, pd.DataFrame],
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    current_level = 1000.0

    for i, dt in enumerate(trading_dates):
        members = daily_members.get(pd.Timestamp(dt), [])
        if i == 0 or not members:
            rows.append(
                {
                    "date": dt,
                    "open": current_level,
                    "high": current_level,
                    "low": current_level,
                    "close": current_level,
                    "daily_return": np.nan if i == 0 else 0.0,
                    "holding_count": len(members),
                    "valid_member_count": 0,
                    "valid_member_ratio": 0.0,
                }
            )
            current_level = current_level
            continue

        open_vals: list[float] = []
        high_vals: list[float] = []
        low_vals: list[float] = []
        close_vals: list[float] = []

        for symbol in members:
            df = ratio_cache.get(symbol)
            if df is None or pd.Timestamp(dt) not in df.index:
                continue
            row = df.loc[pd.Timestamp(dt)]
            if pd.isna(row["prev_close"]):
                continue
            if pd.notna(row["ret_open_pc"]):
                open_vals.append(float(row["ret_open_pc"]))
            if pd.notna(row["ret_high_pc"]):
                high_vals.append(float(row["ret_high_pc"]))
            if pd.notna(row["ret_low_pc"]):
                low_vals.append(float(row["ret_low_pc"]))
            if pd.notna(row["ret_close_pc"]):
                close_vals.append(float(row["ret_close_pc"]))

        valid_member_count = len(close_vals)
        if valid_member_count == 0:
            rows.append(
                {
                    "date": dt,
                    "open": current_level,
                    "high": current_level,
                    "low": current_level,
                    "close": current_level,
                    "daily_return": 0.0,
                    "holding_count": len(members),
                    "valid_member_count": 0,
                    "valid_member_ratio": 0.0,
                }
            )
            continue

        prev_level = current_level
        open_level = prev_level * (1.0 + float(np.mean(open_vals))) if open_vals else prev_level
        high_level = prev_level * (1.0 + float(np.mean(high_vals))) if high_vals else max(prev_level, open_level)
        low_level = prev_level * (1.0 + float(np.mean(low_vals))) if low_vals else min(prev_level, open_level)
        close_level = prev_level * (1.0 + float(np.mean(close_vals)))

        high_level = max(high_level, open_level, close_level, low_level)
        low_level = min(low_level, open_level, close_level, high_level)

        rows.append(
            {
                "date": dt,
                "open": open_level,
                "high": high_level,
                "low": low_level,
                "close": close_level,
                "daily_return": close_level / prev_level - 1.0,
                "holding_count": len(members),
                "valid_member_count": valid_member_count,
                "valid_member_ratio": valid_member_count / len(members) if members else 0.0,
            }
        )
        current_level = close_level

    return pd.DataFrame(rows)


def build_compare_df(base_df: pd.DataFrame, ohlc_df: pd.DataFrame) -> pd.DataFrame:
    compare = base_df[["date", "close"]].rename(columns={"close": "base_close"}).merge(
        ohlc_df[["date", "close", "valid_member_ratio"]],
        on="date",
        how="left",
    )
    compare = compare.rename(columns={"close": "ohlc_close"})
    compare["close_diff"] = compare["ohlc_close"] - compare["base_close"]
    compare["close_diff_pct"] = compare["ohlc_close"] / compare["base_close"] - 1.0
    return compare


def anchor_ohlc_to_base_close(ohlc_df: pd.DataFrame, base_df: pd.DataFrame) -> pd.DataFrame:
    anchored = ohlc_df.merge(
        base_df[["date", "close"]].rename(columns={"close": "base_close"}),
        on="date",
        how="left",
    ).copy()
    anchored = anchored.rename(columns={"close": "close_proxy_raw"})
    scale = anchored["base_close"] / anchored["close_proxy_raw"]
    scale = scale.replace([np.inf, -np.inf], np.nan).fillna(1.0)

    for col in ["open", "high", "low"]:
        anchored[col] = anchored[col] * scale

    anchored["close"] = anchored["base_close"]
    anchored["high"] = anchored[["high", "open", "close", "low"]].max(axis=1)
    anchored["low"] = anchored[["low", "open", "close", "high"]].min(axis=1)
    anchored["daily_return"] = anchored["close"].pct_change(fill_method=None)
    if not anchored.empty:
        anchored.loc[anchored.index[0], "daily_return"] = np.nan

    return anchored[
        [
            "date",
            "open",
            "high",
            "low",
            "close",
            "close_proxy_raw",
            "daily_return",
            "holding_count",
            "valid_member_count",
            "valid_member_ratio",
        ]
    ].copy()


def build_anchored_compare_df(base_df: pd.DataFrame, anchored_df: pd.DataFrame) -> pd.DataFrame:
    compare = base_df[["date", "close"]].rename(columns={"close": "base_close"}).merge(
        anchored_df[["date", "close", "close_proxy_raw", "valid_member_ratio"]],
        on="date",
        how="left",
    )
    compare = compare.rename(columns={"close": "anchored_close", "close_proxy_raw": "raw_proxy_close"})
    compare["close_diff"] = compare["anchored_close"] - compare["base_close"]
    compare["close_diff_pct"] = compare["anchored_close"] / compare["base_close"] - 1.0
    return compare


def main() -> None:
    members = load_members()
    base_df = load_base_index()
    trading_dates = pd.DatetimeIndex(base_df["date"])
    start_date = str(trading_dates[0].date())
    end_date = str(trading_dates[-1].date())

    _, daily_members = build_daily_members(trading_dates, members)
    unique_symbols = sorted(set(members["symbol"]))

    shared_cache_dir = find_shared_ohlc_cache_dir()
    stock_ohlc, load_stats = load_ohlc_batch(
        symbols=unique_symbols,
        start_date=start_date,
        end_date=end_date,
        shared_cache_dir=shared_cache_dir,
    )
    ratio_cache = prepare_ratio_cache(stock_ohlc)
    ohlc_df = build_proxy_ohlc(trading_dates, daily_members, ratio_cache)
    anchored_ohlc_df = anchor_ohlc_to_base_close(ohlc_df, base_df)
    compare_df = build_compare_df(base_df, ohlc_df)
    compare_anchored_df = build_anchored_compare_df(base_df, anchored_ohlc_df)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    ohlc_df.to_csv(OUT_OHLC_CSV, index=False, encoding="utf-8")
    anchored_ohlc_df.to_csv(OUT_OHLC_ANCHORED_CSV, index=False, encoding="utf-8")
    compare_df.to_csv(OUT_COMPARE_CSV, index=False, encoding="utf-8")
    compare_anchored_df.to_csv(OUT_COMPARE_ANCHORED_CSV, index=False, encoding="utf-8")

    meta = {
        "index_name": "wind_microcap_868008_monthly_16y_ohlc_proxy",
        "source_close_csv": str(BASE_INDEX_CSV),
        "source_members_csv": str(MEMBERS_CSV),
        "shared_ohlc_cache_dir": None if shared_cache_dir is None else str(shared_cache_dir),
        "local_ohlc_cache_dir": str(LOCAL_OHLC_CACHE_DIR),
        "sample_start": start_date,
        "sample_end": end_date,
        "trading_days": int(len(trading_dates)),
        "unique_member_symbols": int(len(unique_symbols)),
        "loaded_ohlc_symbols": int(len(stock_ohlc)),
        "load_stats": load_stats,
        "mean_valid_member_ratio": float(ohlc_df["valid_member_ratio"].mean()),
        "median_valid_member_ratio": float(ohlc_df["valid_member_ratio"].median()),
        "min_valid_member_ratio": float(ohlc_df["valid_member_ratio"].min()),
        "construction_note": (
            "Raw proxy OHLC is built by equal-weighting constituent open/high/low/close moves versus previous close. "
            "Anchored OHLC rescales each day's proxy OHLC so the close matches the existing Wind proxy close exactly."
        ),
        "max_abs_close_diff_pct": float(compare_df["close_diff_pct"].abs().max()),
        "mean_abs_close_diff_pct": float(compare_df["close_diff_pct"].abs().mean()),
        "anchored_max_abs_close_diff_pct": float(compare_anchored_df["close_diff_pct"].abs().max()),
        "anchored_mean_abs_close_diff_pct": float(compare_anchored_df["close_diff_pct"].abs().mean()),
    }
    OUT_META_JSON.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")

    print(json.dumps(meta, ensure_ascii=False, indent=2))
    print(f"saved ohlc: {OUT_OHLC_CSV}")
    print(f"saved anchored ohlc: {OUT_OHLC_ANCHORED_CSV}")
    print(f"saved compare: {OUT_COMPARE_CSV}")
    print(f"saved anchored compare: {OUT_COMPARE_ANCHORED_CSV}")
    print(f"saved meta: {OUT_META_JSON}")


if __name__ == "__main__":
    main()
