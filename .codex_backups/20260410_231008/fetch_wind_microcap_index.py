from __future__ import annotations

import argparse
import json
import time
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from pathlib import Path

import numpy as np
import pandas as pd
import requests


ROOT = Path(__file__).resolve().parent
OUTPUT_DIR = ROOT / "outputs"
CACHE_DIR = ROOT / ".microcap_index_cache"
PRICE_CACHE_DIR = CACHE_DIR / "prices_raw"
ADJ_PRICE_CACHE_DIR = CACHE_DIR / "prices_qfq"
SHARE_CACHE_DIR = CACHE_DIR / "share_change"
UNIVERSE_CACHE = CACHE_DIR / "active_universe.csv"
CURRENT_ST_CACHE = CACHE_DIR / "current_st.csv"

DEFAULT_INDEX_CODE = "868008.WI"
DEFAULT_START = "2025-01-02"
DEFAULT_END = pd.Timestamp.today().strftime("%Y-%m-%d")
DEFAULT_COUNT = 400
DEFAULT_SWITCH_DATE = "2025-01-02"
DEFAULT_OUT_CSV = OUTPUT_DIR / "wind_microcap_868008_data.csv"
DEFAULT_OUT_META = OUTPUT_DIR / "wind_microcap_868008_meta.json"
DEFAULT_OUT_MEMBERS = OUTPUT_DIR / "wind_microcap_868008_constituents.csv"

MAIN_PANEL = ROOT / "mnt_strategy_data_cn.csv"

COL_CODE = "\u4ee3\u7801"
COL_NAME = "\u540d\u79f0"
COL_DATE = "\u65e5\u671f"
COL_CLOSE = "\u6536\u76d8"
COL_CHANGE_DATE = "\u53d8\u52a8\u65e5\u671f"
COL_TOTAL_SHARES = "\u603b\u80a1\u672c"
COL_REASON = "\u53d8\u52a8\u539f\u56e0"


@dataclass
class BuildStats:
    symbols_total: int
    symbols_success: int
    symbols_failed: int
    current_st_excluded: int
    rebalance_dates: int
    active_days: int


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Fetch Wind microcap index data directly from WindPy when available, "
            "or rebuild a public proxy using A-share raw prices + share change data."
        )
    )
    parser.add_argument("--source", choices=["auto", "windpy", "public"], default="auto")
    parser.add_argument("--index-code", default=DEFAULT_INDEX_CODE)
    parser.add_argument("--start-date", default=DEFAULT_START)
    parser.add_argument("--end-date", default=DEFAULT_END)
    parser.add_argument("--constituents", type=int, default=DEFAULT_COUNT)
    parser.add_argument("--switch-date", default=DEFAULT_SWITCH_DATE)
    parser.add_argument(
        "--post-switch-schedule",
        choices=["month_start", "month_end", "week_start", "week_end", "biweek_start", "biweek_end"],
        default="month_start",
        help="Public proxy only. Inference for 868008.WI monthly rebalance after 2025-01-02.",
    )
    parser.add_argument(
        "--pre-switch-schedule",
        choices=["daily", "month_start", "month_end", "week_start", "week_end", "biweek_start", "biweek_end"],
        default="daily",
        help="Public proxy only. Use daily to approximate the legacy daily-equal-weight regime.",
    )
    parser.add_argument("--max-workers", type=int, default=8)
    parser.add_argument("--executor", choices=["auto", "thread", "process"], default="auto")
    parser.add_argument("--force-refresh", action="store_true")
    parser.add_argument(
        "--exclude-current-st",
        action="store_true",
        default=True,
        help="Public proxy only. Exclude stocks on the current ST board from the whole sample.",
    )
    parser.add_argument(
        "--limit-symbols",
        type=int,
        default=None,
        help="For smoke tests only. Restrict the number of symbols fetched.",
    )
    parser.add_argument("--output-csv", type=Path, default=DEFAULT_OUT_CSV)
    parser.add_argument("--output-meta", type=Path, default=DEFAULT_OUT_META)
    parser.add_argument("--output-members", type=Path, default=DEFAULT_OUT_MEMBERS)
    return parser.parse_args()


def ensure_dirs() -> None:
    for path in (CACHE_DIR, PRICE_CACHE_DIR, ADJ_PRICE_CACHE_DIR, SHARE_CACHE_DIR):
        path.mkdir(parents=True, exist_ok=True)


def find_shared_cache_root() -> Path | None:
    for child in ROOT.parent.iterdir():
        candidate = child / ".microcap_index_cache"
        if candidate.exists() and candidate != CACHE_DIR:
            return candidate
    return None


SHARED_CACHE_DIR = find_shared_cache_root()
SHARED_PRICE_CACHE_DIR = SHARED_CACHE_DIR / "prices_raw" if SHARED_CACHE_DIR else None
SHARED_ADJ_PRICE_CACHE_DIR = SHARED_CACHE_DIR / "prices_qfq" if SHARED_CACHE_DIR else None
SHARED_SHARE_CACHE_DIR = SHARED_CACHE_DIR / "share_change" if SHARED_CACHE_DIR else None


def load_calendar(start_date: str, end_date: str) -> pd.DatetimeIndex:
    if MAIN_PANEL.exists():
        panel = pd.read_csv(MAIN_PANEL, usecols=["date"])
        panel["date"] = pd.to_datetime(panel["date"])
        dates = panel["date"].drop_duplicates().sort_values()
        dates = dates[(dates >= pd.Timestamp(start_date)) & (dates <= pd.Timestamp(end_date))]
        if len(dates):
            return pd.DatetimeIndex(dates)
    return pd.bdate_range(start=start_date, end=end_date)


def try_fetch_windpy(index_code: str, start_date: str, end_date: str) -> pd.DataFrame:
    try:
        from WindPy import w  # type: ignore
    except Exception as exc:
        raise RuntimeError(f"WindPy unavailable: {exc}") from exc

    result = w.start()
    if getattr(result, "ErrorCode", 0) != 0:
        raise RuntimeError(f"WindPy start failed: {result.ErrorCode}")

    data = w.wsd(index_code, "close", start_date, end_date, "Fill=Previous")
    if getattr(data, "ErrorCode", 0) != 0:
        raise RuntimeError(f"WindPy wsd failed: {data.ErrorCode}")
    frame = pd.DataFrame(
        {
            "date": pd.to_datetime(data.Times),
            "close": pd.to_numeric(data.Data[0], errors="coerce"),
        }
    ).dropna()
    if frame.empty:
        raise RuntimeError(f"WindPy returned no data for {index_code}")
    return frame


def get_akshare():
    import akshare as ak

    return ak


def fetch_active_universe(force_refresh: bool = False) -> pd.DataFrame:
    if UNIVERSE_CACHE.exists() and not force_refresh:
        return pd.read_csv(UNIVERSE_CACHE, dtype=str)

    ak = get_akshare()
    spot = ak.stock_zh_a_spot()
    frame = spot[[COL_CODE, COL_NAME]].copy()
    frame.columns = ["symbol", "name"]
    frame = frame.drop_duplicates(subset="symbol")
    frame = frame[frame["symbol"].str.startswith(("sh", "sz"))].copy()
    frame["code"] = frame["symbol"].str[-6:]
    frame.to_csv(UNIVERSE_CACHE, index=False, encoding="utf-8")
    return frame


def fetch_current_st_codes(force_refresh: bool = False) -> set[str]:
    if CURRENT_ST_CACHE.exists() and not force_refresh:
        frame = pd.read_csv(CURRENT_ST_CACHE, dtype=str)
        return set(frame["code"].dropna())

    ak = get_akshare()
    st = ak.stock_zh_a_st_em()
    frame = st[[COL_CODE, COL_NAME]].copy()
    frame.columns = ["code", "name"]
    frame.to_csv(CURRENT_ST_CACHE, index=False, encoding="utf-8")
    return set(frame["code"].dropna())


def _read_csv_cached(path: Path, date_col: str) -> pd.DataFrame:
    frame = pd.read_csv(path)
    frame[date_col] = pd.to_datetime(frame[date_col])
    return frame


def _read_local_or_shared_cache(path: Path, date_col: str, shared_dir: Path | None) -> pd.DataFrame | None:
    candidates = [path]
    if shared_dir is not None:
        candidates.append(shared_dir / path.name)
    for candidate in candidates:
        if not candidate.exists():
            continue
        try:
            return _read_csv_cached(candidate, date_col)
        except Exception:
            continue
    return None


def _eastmoney_secid(symbol: str) -> str:
    code = str(symbol).strip()[-6:]
    if code.startswith(("5", "6", "9")):
        return f"1.{code}"
    return f"0.{code}"


def _sina_symbol(symbol: str) -> str:
    code = str(symbol).strip()[-6:]
    return ("sh" if code.startswith(("5", "6", "9")) else "sz") + code


def _fetch_price_history_eastmoney(symbol: str, start_ts: pd.Timestamp, end_ts: pd.Timestamp) -> pd.DataFrame:
    secid = _eastmoney_secid(symbol)
    url = (
        "https://push2his.eastmoney.com/api/qt/stock/kline/get"
        f"?secid={secid}"
        "&fields1=f1,f2,f3,f4,f5,f6"
        "&fields2=f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61"
        "&klt=101&fqt=0"
        f"&beg={start_ts.strftime('%Y%m%d')}"
        f"&end={end_ts.strftime('%Y%m%d')}"
        "&lmt=10000"
    )
    last_err: Exception | None = None
    for attempt in range(3):
        try:
            resp = requests.get(
                url,
                timeout=20,
                headers={"Referer": "https://quote.eastmoney.com/", "User-Agent": "Mozilla/5.0"},
            )
            resp.raise_for_status()
            data = resp.json().get("data") or {}
            klines = data.get("klines") or []
            if not klines:
                raise ValueError(f"empty price history for {symbol}")
            rows: list[tuple[pd.Timestamp, float]] = []
            for line in klines:
                parts = line.split(",")
                if len(parts) < 3:
                    continue
                rows.append((pd.to_datetime(parts[0]), float(parts[2])))
            frame = pd.DataFrame(rows, columns=["date", "close_raw"])
            frame = frame.dropna(subset=["date", "close_raw"]).sort_values("date")
            if frame.empty:
                raise ValueError(f"parsed empty price history for {symbol}")
            return frame
        except Exception as exc:
            last_err = exc
            time.sleep(1.0 * (attempt + 1))
    raise RuntimeError(f"eastmoney price history failed for {symbol}: {last_err}")


def _fetch_price_history_sina(symbol: str, start_ts: pd.Timestamp, end_ts: pd.Timestamp) -> pd.DataFrame:
    url = (
        "https://money.finance.sina.com.cn/quotes_service/api/json_v2.php/CN_MarketData.getKLineData"
        f"?symbol={_sina_symbol(symbol)}&scale=240&ma=no&datalen=6000"
    )
    resp = requests.get(url, timeout=20, headers={"User-Agent": "Mozilla/5.0"})
    resp.raise_for_status()
    data = resp.json()
    if not isinstance(data, list) or not data:
        raise ValueError(f"empty sina price history for {symbol}")
    rows: list[tuple[pd.Timestamp, float]] = []
    for item in data:
        day = item.get("day")
        close = item.get("close")
        if day is None or close is None:
            continue
        rows.append((pd.to_datetime(day), float(close)))
    frame = pd.DataFrame(rows, columns=["date", "close_raw"])
    frame = frame.dropna(subset=["date", "close_raw"]).sort_values("date")
    frame = frame[(frame["date"] >= start_ts) & (frame["date"] <= end_ts)]
    if frame.empty:
        raise ValueError(f"sina price history outside range for {symbol}")
    return frame


def _merge_cache_frames(
    old_frame: pd.DataFrame | None,
    new_frame: pd.DataFrame,
    date_col: str,
) -> pd.DataFrame:
    frames = []
    if old_frame is not None and not old_frame.empty:
        frames.append(old_frame.copy())
    if new_frame is not None and not new_frame.empty:
        frames.append(new_frame.copy())
    if not frames:
        return pd.DataFrame()
    merged = pd.concat(frames, ignore_index=True)
    merged[date_col] = pd.to_datetime(merged[date_col], errors="coerce")
    merged = merged.dropna(subset=[date_col]).sort_values(date_col).drop_duplicates(subset=date_col, keep="last")
    return merged.reset_index(drop=True)


def fetch_price_history(symbol: str, start_date: str, end_date: str, force_refresh: bool = False) -> pd.DataFrame:
    cache_path = PRICE_CACHE_DIR / f"{symbol}.csv"
    start_ts = pd.Timestamp(start_date)
    end_ts = pd.Timestamp(end_date)
    cached = _read_local_or_shared_cache(cache_path, "date", SHARED_PRICE_CACHE_DIR)
    if not force_refresh and cached is not None and not cached.empty and cached["date"].min() <= start_ts and cached["date"].max() >= end_ts:
        return cached[(cached["date"] >= start_ts) & (cached["date"] <= end_ts)].copy()

    fetch_start_ts = start_ts
    if cached is not None and not cached.empty and not force_refresh:
        overlap_start = pd.Timestamp(cached["date"].max()) - pd.Timedelta(days=10)
        fetch_start_ts = max(start_ts, overlap_start)

    try:
        frame = _fetch_price_history_sina(symbol, fetch_start_ts, end_ts)
    except Exception:
        try:
            frame = _fetch_price_history_eastmoney(symbol, fetch_start_ts, end_ts)
        except Exception:
            if cached is not None and not cached.empty:
                return cached[(cached["date"] >= start_ts) & (cached["date"] <= end_ts)].copy()
            ak = get_akshare()
            frame = ak.stock_zh_a_hist(
                symbol=symbol,
                period="daily",
                start_date=fetch_start_ts.strftime("%Y%m%d"),
                end_date=end_date.replace("-", ""),
                adjust="",
            )
            if frame.empty and cached is not None and not cached.empty:
                return cached[(cached["date"] >= start_ts) & (cached["date"] <= end_ts)].copy()
            if frame.empty:
                raise ValueError(f"empty price history for {symbol}")
            frame = frame[[COL_DATE, COL_CLOSE]].copy()
            frame.columns = ["date", "close_raw"]
            frame["date"] = pd.to_datetime(frame["date"])
            frame["close_raw"] = pd.to_numeric(frame["close_raw"], errors="coerce")
            frame = frame.dropna(subset=["close_raw"]).sort_values("date")
    merged = _merge_cache_frames(cached, frame, "date")
    merged.to_csv(cache_path, index=False, encoding="utf-8")
    return merged[(merged["date"] >= start_ts) & (merged["date"] <= end_ts)].copy()


def fetch_adjusted_price_history(symbol: str, start_date: str, end_date: str, force_refresh: bool = False) -> pd.DataFrame:
    cache_path = ADJ_PRICE_CACHE_DIR / f"{symbol}.csv"
    start_ts = pd.Timestamp(start_date)
    end_ts = pd.Timestamp(end_date)
    cached = _read_local_or_shared_cache(cache_path, "date", SHARED_ADJ_PRICE_CACHE_DIR)
    if (
        not force_refresh
        and cached is not None
        and not cached.empty
        and cached["date"].min() <= start_ts
        and cached["date"].max() >= end_ts
    ):
        return cached[(cached["date"] >= start_ts) & (cached["date"] <= end_ts)].copy()

    fetch_start_ts = start_ts
    if cached is not None and not cached.empty and not force_refresh:
        overlap_start = pd.Timestamp(cached["date"].max()) - pd.Timedelta(days=10)
        fetch_start_ts = max(start_ts, overlap_start)

    ak = get_akshare()
    frame = ak.stock_zh_a_hist(
        symbol=symbol,
        period="daily",
        start_date=fetch_start_ts.strftime("%Y%m%d"),
        end_date=end_date.replace("-", ""),
        adjust="qfq",
    )
    if frame.empty and cached is not None and not cached.empty:
        return cached[(cached["date"] >= start_ts) & (cached["date"] <= end_ts)].copy()
    if frame.empty:
        raise ValueError(f"empty adjusted price history for {symbol}")
    frame = frame[[COL_DATE, COL_CLOSE]].copy()
    frame.columns = ["date", "close_qfq"]
    frame["date"] = pd.to_datetime(frame["date"])
    frame["close_qfq"] = pd.to_numeric(frame["close_qfq"], errors="coerce")
    frame = frame.dropna(subset=["close_qfq"]).sort_values("date")
    merged = _merge_cache_frames(cached, frame, "date")
    merged.to_csv(cache_path, index=False, encoding="utf-8")
    return merged[(merged["date"] >= start_ts) & (merged["date"] <= end_ts)].copy()


def fetch_share_change(symbol: str, start_date: str, end_date: str, force_refresh: bool = False) -> pd.DataFrame:
    cache_path = SHARE_CACHE_DIR / f"{symbol}.csv"
    start_ts = pd.Timestamp(start_date)
    end_ts = pd.Timestamp(end_date)
    cached = _read_local_or_shared_cache(cache_path, "change_date", SHARED_SHARE_CACHE_DIR)
    if not force_refresh and cached is not None and not cached.empty and cached["change_date"].min() <= end_ts:
        return cached[cached["change_date"] <= end_ts].copy()

    fetch_start_ts = pd.Timestamp("1990-01-01")
    if cached is not None and not cached.empty and not force_refresh:
        overlap_start = pd.Timestamp(cached["change_date"].max()) - pd.Timedelta(days=30)
        fetch_start_ts = max(fetch_start_ts, overlap_start)

    ak = get_akshare()
    frame = ak.stock_share_change_cninfo(
        symbol=symbol,
        start_date=fetch_start_ts.strftime("%Y%m%d"),
        end_date=end_date.replace("-", ""),
    )
    if frame.empty and cached is not None and not cached.empty:
        return cached[cached["change_date"] <= end_ts].copy()
    if frame.empty:
        raise ValueError(f"empty share change for {symbol}")
    frame = frame[[COL_CHANGE_DATE, COL_TOTAL_SHARES, COL_REASON]].copy()
    frame.columns = ["change_date", "total_shares_10k", "reason"]
    frame["change_date"] = pd.to_datetime(frame["change_date"])
    frame["total_shares_10k"] = pd.to_numeric(frame["total_shares_10k"], errors="coerce")
    frame = frame.dropna(subset=["total_shares_10k"]).sort_values("change_date")
    merged = _merge_cache_frames(cached, frame, "change_date")
    merged.to_csv(cache_path, index=False, encoding="utf-8")
    return merged[merged["change_date"] <= end_ts].copy()


def build_symbol_panel(symbol: str, start_date: str, end_date: str, force_refresh: bool = False) -> pd.DataFrame:
    price = fetch_price_history(symbol=symbol, start_date=start_date, end_date=end_date, force_refresh=force_refresh)
    try:
        adjusted_price = fetch_adjusted_price_history(
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
            force_refresh=force_refresh,
        )
    except Exception:
        adjusted_price = pd.DataFrame(columns=["date", "close_qfq"])
    shares = fetch_share_change(symbol=symbol, start_date=start_date, end_date=end_date, force_refresh=force_refresh)
    merged = pd.merge_asof(
        price.sort_values("date"),
        shares[["change_date", "total_shares_10k"]].sort_values("change_date"),
        left_on="date",
        right_on="change_date",
        direction="backward",
    )
    if not adjusted_price.empty:
        merged = pd.merge_asof(
            merged.sort_values("date"),
            adjusted_price[["date", "close_qfq"]].sort_values("date"),
            on="date",
            direction="backward",
        )
    else:
        merged["close_qfq"] = np.nan
    merged["total_shares"] = merged["total_shares_10k"] * 10000.0
    merged["market_cap"] = merged["close_raw"] * merged["total_shares"]
    return_close = merged["close_qfq"].where(merged["close_qfq"].notna(), merged["close_raw"])
    merged["return"] = return_close.pct_change(fill_method=None)
    merged["symbol"] = symbol
    return merged[["date", "symbol", "close_raw", "market_cap", "return"]].dropna(subset=["market_cap"])


def build_rebalance_dates(
    trading_dates: pd.DatetimeIndex,
    switch_date: str,
    pre_switch_schedule: str,
    post_switch_schedule: str,
) -> pd.DatetimeIndex:
    switch_ts = pd.Timestamp(switch_date)
    pre_dates = trading_dates[trading_dates < switch_ts]
    post_dates = trading_dates[trading_dates >= switch_ts]

    def schedule_dates(dates: pd.DatetimeIndex, mode: str) -> pd.DatetimeIndex:
        if len(dates) == 0:
            return pd.DatetimeIndex([])
        if mode == "daily":
            return dates
        if mode in {"week_start", "week_end"}:
            periods = dates.to_period("W-MON")
            grouped = dates.to_series().groupby(periods)
            picker = grouped.min if mode == "week_start" else grouped.max
            return pd.DatetimeIndex(picker().tolist())
        if mode in {"biweek_start", "biweek_end"}:
            week_periods = dates.to_period("W-MON")
            unique_weeks = sorted(pd.Index(week_periods.unique()))
            week_keys = pd.Series([i // 2 for i, _ in enumerate(unique_weeks)], index=unique_weeks)
            aligned_keys = pd.Index(week_periods).map(lambda p: week_keys[p])
            grouped = dates.to_series().groupby(aligned_keys)
            picker = grouped.min if mode == "biweek_start" else grouped.max
            return pd.DatetimeIndex(picker().tolist())
        periods = dates.to_period("M")
        if mode == "month_start":
            return pd.DatetimeIndex(dates.to_series().groupby(periods).min().tolist())
        if mode == "month_end":
            return pd.DatetimeIndex(dates.to_series().groupby(periods).max().tolist())
        raise ValueError(f"unsupported schedule: {mode}")

    pre = schedule_dates(pre_dates, pre_switch_schedule)
    post = schedule_dates(post_dates, post_switch_schedule)
    return pd.DatetimeIndex(sorted(set(pre.tolist() + post.tolist())))


def build_public_proxy(args: argparse.Namespace) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, object]]:
    ensure_dirs()
    trading_dates = load_calendar(start_date=args.start_date, end_date=args.end_date)
    if len(trading_dates) < 5:
        raise ValueError("Not enough trading dates in selected range.")

    universe = fetch_active_universe(force_refresh=args.force_refresh)
    current_st_codes: set[str] = set()
    if args.exclude_current_st:
        current_st_codes = fetch_current_st_codes(force_refresh=args.force_refresh)
        universe = universe[~universe["code"].isin(current_st_codes)].copy()

    if args.limit_symbols:
        universe = universe.head(args.limit_symbols).copy()

    symbol_panels: dict[str, pd.DataFrame] = {}
    failures: dict[str, str] = {}

    worker_args = [
        (symbol, args.start_date, args.end_date, args.force_refresh)
        for symbol in universe["code"].tolist()
    ]
    executor_name = args.executor
    if executor_name == "auto":
        executor_name = "process" if args.max_workers and args.max_workers > 1 else "thread"

    if args.max_workers <= 1:
        for symbol, start_date, end_date, force_refresh in worker_args:
            try:
                key, panel = _build_symbol_panel_worker(symbol, start_date, end_date, force_refresh)
                if not panel.empty:
                    symbol_panels[key] = panel
            except Exception as exc:
                failures[symbol] = str(exc)
    else:
        executor_cls = ProcessPoolExecutor if executor_name == "process" else ThreadPoolExecutor
        with executor_cls(max_workers=max(1, args.max_workers)) as pool:
            futures = {
                pool.submit(_build_symbol_panel_worker, symbol, start_date, end_date, force_refresh): symbol
                for symbol, start_date, end_date, force_refresh in worker_args
            }
            for fut in as_completed(futures):
                symbol = futures[fut]
                try:
                    key, panel = fut.result()
                    if not panel.empty:
                        symbol_panels[key] = panel
                except Exception as exc:
                    failures[symbol] = str(exc)

    if not symbol_panels:
        sample = dict(list(failures.items())[:10])
        raise RuntimeError(f"No symbol panels were built successfully. Failure sample: {sample}")

    returns_df = pd.DataFrame(index=trading_dates)
    caps_by_date: dict[pd.Timestamp, dict[str, float]] = {}

    rebalance_dates = build_rebalance_dates(
        trading_dates=trading_dates,
        switch_date=args.switch_date,
        pre_switch_schedule=args.pre_switch_schedule,
        post_switch_schedule=args.post_switch_schedule,
    )

    for symbol, panel in symbol_panels.items():
        panel = panel.sort_values("date").drop_duplicates(subset="date")
        series_ret = panel.set_index("date")["return"].reindex(trading_dates)
        returns_df[symbol] = series_ret
        cap_lookup = pd.merge_asof(
            pd.DataFrame({"date": rebalance_dates}),
            panel[["date", "market_cap"]].sort_values("date"),
            on="date",
            direction="backward",
        )
        for row in cap_lookup.itertuples(index=False):
            if pd.notna(row.market_cap):
                caps_by_date.setdefault(pd.Timestamp(row.date), {})[symbol] = float(row.market_cap)

    index_levels: list[dict[str, object]] = []
    members_rows: list[dict[str, object]] = []
    current_level = 1000.0
    current_members: list[str] = []
    active_days = 0

    rebalance_set = set(rebalance_dates)
    next_members_map: dict[pd.Timestamp, list[str]] = {}
    for dt in rebalance_dates:
        cap_map = caps_by_date.get(pd.Timestamp(dt), {})
        ranked = sorted(cap_map.items(), key=lambda x: x[1])
        selected = [symbol for symbol, _ in ranked[: args.constituents]]
        next_members_map[pd.Timestamp(dt)] = selected
        for rank, symbol in enumerate(selected, start=1):
            members_rows.append(
                {
                    "rebalance_date": dt,
                    "rank": rank,
                    "symbol": symbol,
                    "market_cap": cap_map[symbol],
                }
            )

    for i, dt in enumerate(trading_dates):
        if i == 0:
            index_levels.append(
                {
                    "date": dt,
                    "close": current_level,
                    "daily_return": np.nan,
                    "holding_count": 0,
                    "holding_effective": False,
                }
            )
            if dt in rebalance_set:
                current_members = next_members_map.get(pd.Timestamp(dt), [])
            continue

        if trading_dates[i - 1] in rebalance_set:
            current_members = next_members_map.get(pd.Timestamp(trading_dates[i - 1]), [])

        if current_members:
            day_ret = returns_df.loc[dt, current_members].dropna()
            portfolio_ret = float(day_ret.mean()) if len(day_ret) else 0.0
            active_days += 1
        else:
            portfolio_ret = 0.0

        current_level *= 1.0 + portfolio_ret
        index_levels.append(
            {
                "date": dt,
                "close": current_level,
                "daily_return": portfolio_ret,
                "holding_count": len(current_members),
                "holding_effective": bool(current_members),
            }
        )

        if dt in rebalance_set and i == 0:
            current_members = next_members_map.get(pd.Timestamp(dt), [])

    data_df = pd.DataFrame(index_levels)
    members_df = pd.DataFrame(members_rows)

    meta = {
        "index_code": args.index_code,
        "source_used": "public_proxy",
        "method_note": (
            "Public reconstruction using AKShare raw close data and CNInfo share-change data. "
            "This is not the official Wind time series."
        ),
        "rule_note": (
            "Public sources indicate 8841431.WI was renamed to the daily-equal-weight variant, "
            f"while {args.index_code} switched to monthly rebalance on {args.switch_date}. "
            f"Post-switch schedule is inferred as {args.post_switch_schedule}."
        ),
        "limitations": [
            "Current ST board exclusion is applied to the whole sample when --exclude-current-st is on.",
            "Historical ST status is not reconstructed day by day.",
            "Returns use raw close, so corporate-action handling will differ from the official index divisor methodology.",
            "Active universe is built from current SH/SZ A-shares; delisted historical names are not fully backfilled.",
        ],
        "stats": asdict(
            BuildStats(
                symbols_total=int(len(universe)),
                symbols_success=int(len(symbol_panels)),
                symbols_failed=int(len(failures)),
                current_st_excluded=int(len(current_st_codes)),
                rebalance_dates=int(len(rebalance_dates)),
                active_days=int(active_days),
            )
        ),
        "failures_sample": dict(list(failures.items())[:20]),
        "params": {
            "start_date": args.start_date,
            "end_date": args.end_date,
            "constituents": args.constituents,
            "switch_date": args.switch_date,
            "pre_switch_schedule": args.pre_switch_schedule,
            "post_switch_schedule": args.post_switch_schedule,
            "limit_symbols": args.limit_symbols,
            "executor": executor_name,
        },
    }
    return data_df, members_df, meta


def _build_symbol_panel_worker(
    symbol: str,
    start_date: str,
    end_date: str,
    force_refresh: bool,
) -> tuple[str, pd.DataFrame]:
    panel = build_symbol_panel(
        symbol=symbol,
        start_date=start_date,
        end_date=end_date,
        force_refresh=force_refresh,
    )
    return symbol, panel


def main() -> None:
    args = parse_args()
    ensure_dirs()

    source_used = args.source
    meta: dict[str, object] = {}
    data_df: pd.DataFrame
    members_df = pd.DataFrame()

    if args.source in {"auto", "windpy"}:
        try:
            wind_df = try_fetch_windpy(args.index_code, args.start_date, args.end_date)
            data_df = wind_df.rename(columns={"close": "close"})
            meta = {
                "index_code": args.index_code,
                "source_used": "windpy",
                "params": {"start_date": args.start_date, "end_date": args.end_date},
            }
            source_used = "windpy"
        except Exception as exc:
            if args.source == "windpy":
                raise
            source_used = "public"
            meta = {"windpy_error": str(exc)}

    if source_used == "public":
        data_df, members_df, public_meta = build_public_proxy(args)
        meta = {**meta, **public_meta}

    args.output_csv.parent.mkdir(parents=True, exist_ok=True)
    data_df.to_csv(args.output_csv, index=False, encoding="utf-8")
    if not members_df.empty:
        args.output_members.parent.mkdir(parents=True, exist_ok=True)
        members_df.to_csv(args.output_members, index=False, encoding="utf-8")
    args.output_meta.parent.mkdir(parents=True, exist_ok=True)
    args.output_meta.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")

    print(json.dumps(meta, ensure_ascii=False, indent=2))
    print(f"saved {args.output_csv.name}")
    if not members_df.empty:
        print(f"saved {args.output_members.name}")
    print(f"saved {args.output_meta.name}")


if __name__ == "__main__":
    main()
