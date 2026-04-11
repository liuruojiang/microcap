#!/usr/bin/env python
# -*- coding: utf-8 -*-
# poe: name=Microcap-Top100-Signal-Standalone
# poe: privacy_shield=half
"""Standalone POE bot for Top100 microcap signal and realtime signal."""

import json
import math
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import numpy as np
import pandas as pd
import requests

try:
    from fastapi_poe.types import SettingsResponse
except Exception:
    class SettingsResponse:  # type: ignore[no-redef]
        def __init__(self, introduction_message: str = "") -> None:
            self.introduction_message = introduction_message


try:
    poe  # type: ignore[name-defined]
except NameError:
    try:
        import fastapi_poe as poe  # type: ignore
    except Exception:
        poe = None  # type: ignore


ROOT = Path(__file__).resolve().parent
OUTPUT_DIR = ROOT / "outputs"
CACHE_DIR = ROOT / ".microcap_index_cache"
PRICE_DIR = CACHE_DIR / "prices_raw"
REALTIME_DIR = CACHE_DIR / "realtime"

DEFAULT_PANEL = ROOT / "mnt_strategy_data_cn.csv"
DEFAULT_INDEX_CSV = OUTPUT_DIR / "wind_microcap_top_100_biweekly_thursday_16y_cached.csv"
DEFAULT_PROXY_MEMBERS_CSV = OUTPUT_DIR / "microcap_top100_mom16_biweekly_live_proxy_members.csv"

DEFAULT_OUTPUT_PREFIX = "microcap_top100_mom16_biweekly_live"
HEDGE_COLUMN = "1.000852"
LOOKBACK = 16
REBALANCE_WEEKDAY = "Thursday"
FIXED_HEDGE_RATIO = 1.0
FUTURES_DRAG = 3.0 / 10000.0
TAIL_JITTER_WARNING_GAP = 0.001
TAIL_JITTER_CAUTION_GAP = 0.002
DEFAULT_MAX_STALE_ANCHOR_DAYS = 5
HEDGE_HISTORY_LOOKBACK_BUFFER_DAYS = 40
DEFAULT_REALTIME_CACHE_SECONDS = 3600
CN_TRADING_DAYS = 244

WEEK_FREQ_BY_START = {
    "Monday": "W-SUN",
    "Tuesday": "W-MON",
    "Wednesday": "W-TUE",
    "Thursday": "W-WED",
    "Friday": "W-THU",
}

SESSION = requests.Session()
SESSION.headers.update(
    {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/123.0.0.0 Safari/537.36"
        )
    }
)


def _update_settings() -> None:
    if poe is None or not hasattr(poe, "update_settings"):
        return
    poe.update_settings(
        SettingsResponse(
            introduction_message=(
                "📊 **Top100 微盘股对冲策略机器人（单文件版）**\n\n"
                "支持命令：\n"
                '- `信号`：最新收盘确认信号\n'
                '- `实时信号`：盘中实时信号，默认优先复用 1 小时内缓存\n'
                '- `强制刷新实时信号`：忽略缓存，重新抓取实时行情\n\n'
                "这是单文件 standalone 版本，不依赖本地其它 `.py`。"
            )
        )
    )


def build_output_paths(output_prefix: str) -> dict[str, Path]:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    REALTIME_DIR.mkdir(parents=True, exist_ok=True)
    return {
        "panel_shadow": OUTPUT_DIR / f"{output_prefix}_panel_refreshed.csv",
        "signal": OUTPUT_DIR / f"{output_prefix}_latest_signal.csv",
        "realtime_signal": OUTPUT_DIR / f"{output_prefix}_realtime_signal.csv",
        "cache_realtime_meta": REALTIME_DIR / f"{output_prefix}_realtime_cached_meta.json",
        "cache_realtime_signal": REALTIME_DIR / f"{output_prefix}_realtime_cached_signal.csv",
    }


def read_csv_last_date(path: Path, date_col: str = "date") -> pd.Timestamp | None:
    if not path.exists():
        return None
    frame = pd.read_csv(path, usecols=[date_col])
    if frame.empty:
        return None
    dates = pd.to_datetime(frame[date_col], errors="coerce").dropna()
    if dates.empty:
        return None
    return pd.Timestamp(dates.max())


def assess_history_anchor_freshness(
    latest_trade_date: pd.Timestamp,
    max_stale_days: int,
    now: pd.Timestamp | None = None,
) -> dict[str, object]:
    current_ts = pd.Timestamp.now() if now is None else pd.Timestamp(now)
    latest_trade_date = pd.Timestamp(latest_trade_date).normalize()
    current_date = current_ts.normalize()
    stale_days = max(0, int((current_date - latest_trade_date).days))
    is_stale = stale_days > max(0, int(max_stale_days))
    return {
        "latest_trade_date": str(latest_trade_date.date()),
        "current_date": str(current_date.date()),
        "stale_calendar_days": stale_days,
        "max_stale_anchor_days": int(max_stale_days),
        "is_stale": bool(is_stale),
        "status": "stale" if is_stale else "fresh",
    }


def format_anchor_stale_message(anchor_freshness: dict[str, object]) -> str:
    return (
        f"历史锚点已过期：最新历史交易日 {anchor_freshness['latest_trade_date']}，"
        f"当前日期 {anchor_freshness['current_date']}，"
        f"滞后 {anchor_freshness['stale_calendar_days']} 天，"
        f"阈值 {anchor_freshness['max_stale_anchor_days']} 天。"
    )


def fetch_eastmoney_index_history(secid: str, start_date: pd.Timestamp) -> pd.DataFrame:
    end_ts = pd.Timestamp.now().normalize()
    url = (
        "https://push2his.eastmoney.com/api/qt/stock/kline/get"
        f"?secid={secid}"
        "&fields1=f1,f2,f3,f4,f5,f6"
        "&fields2=f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61"
        "&klt=101&fqt=1"
        f"&beg={pd.Timestamp(start_date).strftime('%Y%m%d')}"
        f"&end={end_ts.strftime('%Y%m%d')}"
        "&lmt=10000"
    )
    response = SESSION.get(
        url,
        timeout=20,
        headers={"Referer": "https://quote.eastmoney.com/"},
    )
    response.raise_for_status()
    data = response.json().get("data") or {}
    klines = data.get("klines") or []
    rows: list[dict[str, object]] = []
    for item in klines:
        parts = item.split(",")
        if len(parts) < 3:
            continue
        rows.append({"date": pd.to_datetime(parts[0]), "close": float(parts[2])})
    out = pd.DataFrame(rows)
    if out.empty:
        raise RuntimeError(f"无法获取指数历史数据: {secid}")
    return out.sort_values("date").drop_duplicates(subset="date").reset_index(drop=True)


def build_refreshed_panel_shadow(panel_path: Path, paths: dict[str, Path]) -> tuple[Path, pd.Timestamp]:
    existing_shadow_end = read_csv_last_date(paths["panel_shadow"])
    if existing_shadow_end is not None and existing_shadow_end.normalize() >= pd.Timestamp.now().normalize():
        return paths["panel_shadow"], pd.Timestamp(existing_shadow_end)

    panel = pd.read_csv(panel_path)
    if "date" not in panel.columns or HEDGE_COLUMN not in panel.columns:
        raise ValueError(f"数据文件必须包含 `date` 和 `{HEDGE_COLUMN}` 两列。")
    panel["date"] = pd.to_datetime(panel["date"])
    panel = panel.sort_values("date").drop_duplicates(subset="date", keep="last")
    latest_panel_date = pd.Timestamp(panel["date"].max())
    history_start = latest_panel_date - pd.Timedelta(days=HEDGE_HISTORY_LOOKBACK_BUFFER_DAYS)
    hedge_hist = fetch_eastmoney_index_history("1.000852", history_start)
    latest_hedge_date = pd.Timestamp(hedge_hist["date"].max())

    shadow = panel.set_index("date")
    for row in hedge_hist.itertuples(index=False):
        dt = pd.Timestamp(row.date)
        close = float(row.close)
        if dt in shadow.index:
            shadow.at[dt, HEDGE_COLUMN] = close
        elif dt > latest_panel_date:
            shadow.loc[dt, :] = np.nan
            shadow.at[dt, HEDGE_COLUMN] = close

    shadow = shadow.sort_index().reset_index().rename(columns={"index": "date"})
    shadow.to_csv(paths["panel_shadow"], index=False, encoding="utf-8")
    return paths["panel_shadow"], latest_hedge_date


def load_close_df(panel_path: Path, index_csv: Path) -> pd.DataFrame:
    panel = pd.read_csv(panel_path, usecols=["date", HEDGE_COLUMN])
    panel["date"] = pd.to_datetime(panel["date"])
    hedge = panel.set_index("date")[HEDGE_COLUMN].rename("hedge").astype(float)

    proxy = pd.read_csv(index_csv)
    proxy["date"] = pd.to_datetime(proxy["date"])
    effective_start = infer_proxy_effective_start(proxy)
    microcap = proxy.set_index("date")["close"].rename("microcap").astype(float)

    close_df = pd.concat([microcap, hedge], axis=1).sort_index().dropna()
    if effective_start is not None:
        close_df = close_df.loc[close_df.index >= effective_start].copy()
    if len(close_df) < LOOKBACK + 3:
        raise ValueError(f"对齐后样本不足，至少需要 {LOOKBACK + 3} 行。")
    return close_df


def infer_proxy_effective_start(proxy_df: pd.DataFrame) -> pd.Timestamp | None:
    if proxy_df.empty:
        return None

    proxy = proxy_df.sort_values("date").copy()
    if "holding_effective" in proxy.columns:
        mask = proxy["holding_effective"].fillna(False).astype(bool)
        if mask.any():
            return pd.Timestamp(proxy.loc[mask, "date"].iloc[0])

    if "holding_count" in proxy.columns:
        holding_count = pd.to_numeric(proxy["holding_count"], errors="coerce").fillna(0)
        mask = holding_count.gt(0)
        if mask.any():
            return pd.Timestamp(proxy.loc[mask, "date"].iloc[0])

    if "daily_return" in proxy.columns:
        daily_return = pd.to_numeric(proxy["daily_return"], errors="coerce").fillna(0.0)
        mask = daily_return.abs().gt(1e-12)
        if mask.any():
            return pd.Timestamp(proxy.loc[mask, "date"].iloc[0])

    close = pd.to_numeric(proxy["close"], errors="coerce")
    close0 = close.dropna()
    if not close0.empty:
        first_close = float(close0.iloc[0])
        mask = close.ne(first_close) & close.notna()
        if mask.any():
            return pd.Timestamp(proxy.loc[mask, "date"].iloc[0])
    return None


def calc_momentum(series: pd.Series, lookback: int) -> pd.Series:
    return series.div(series.shift(lookback)).sub(1.0)


def calc_bias_momentum(series: pd.Series, bias_n: int, mom_day: int) -> pd.Series:
    prices = series.values.astype(float)
    n = len(prices)
    result = np.full(n, np.nan)
    ma = series.rolling(bias_n).mean().values
    total_lookback = bias_n + mom_day - 1
    x = np.arange(mom_day, dtype=float)
    for i in range(total_lookback, n):
        bias_window = np.empty(mom_day)
        valid = True
        for j in range(mom_day):
            idx = i - mom_day + 1 + j
            if np.isnan(ma[idx]) or ma[idx] < 1e-10 or np.isnan(prices[idx]):
                valid = False
                break
            bias_window[j] = prices[idx] / ma[idx]
        if not valid or bias_window[0] < 1e-10:
            continue
        bias_norm = bias_window / bias_window[0]
        slope = np.polyfit(x, bias_norm, 1)[0]
        result[i] = slope * 10000
    return pd.Series(result, index=series.index)


def calc_rolling_r2(series: pd.Series, window: int) -> pd.Series:
    values = series.values.astype(float)
    result = np.full(len(values), np.nan)
    x = np.arange(window, dtype=float)
    x_mean = x.mean()
    ss_x = ((x - x_mean) ** 2).sum()
    for i in range(window - 1, len(values)):
        y = values[i - window + 1 : i + 1]
        if np.any(np.isnan(y)):
            continue
        y_mean = y.mean()
        ss_y = ((y - y_mean) ** 2).sum()
        if ss_y < 1e-12:
            result[i] = 0.0
            continue
        ss_xy = ((x - x_mean) * (y - y_mean)).sum()
        result[i] = (ss_xy ** 2) / (ss_x * ss_y)
    return pd.Series(result, index=series.index)


def run_backtest(close_df: pd.DataFrame) -> pd.DataFrame:
    work = close_df.copy()
    work["microcap_ret"] = work["microcap"].pct_change(fill_method=None)
    work["hedge_ret"] = work["hedge"].pct_change(fill_method=None)
    work["microcap_mom"] = calc_momentum(work["microcap"], LOOKBACK)
    work["hedge_mom"] = calc_momentum(work["hedge"], LOOKBACK)
    work["momentum_gap"] = work["microcap_mom"] - work["hedge_mom"]
    work["ratio"] = work["microcap"] / work["hedge"]
    work["ratio_bias_mom"] = calc_bias_momentum(work["ratio"], 60, 20)
    work["ratio_r2"] = calc_rolling_r2(work["ratio"], 5)

    valid_start = work[["microcap_mom", "hedge_mom"]].dropna().index.min()
    if pd.isna(valid_start):
        raise ValueError("动量历史不足，无法生成信号。")

    work = work.loc[valid_start:].copy()
    rows: list[dict[str, object]] = []
    holding = False
    for i in range(1, len(work)):
        active_ret = 0.0
        drag = FUTURES_DRAG if holding else 0.0
        if holding:
            microcap_ret = work["microcap_ret"].iloc[i]
            hedge_ret = work["hedge_ret"].iloc[i]
            if pd.notna(microcap_ret) and pd.notna(hedge_ret):
                active_ret = float(microcap_ret - hedge_ret)
        signal_on = bool(
            pd.notna(work["microcap_mom"].iloc[i])
            and pd.notna(work["hedge_mom"].iloc[i])
            and work["microcap_mom"].iloc[i] > work["hedge_mom"].iloc[i]
        )
        next_holding = "long_microcap_short_zz1000" if signal_on else "cash"
        rows.append(
            {
                "date": work.index[i],
                "return_raw": active_ret - drag,
                "holding": "long_microcap_short_zz1000" if holding else "cash",
                "next_holding": next_holding,
                "signal_on": signal_on,
                "microcap_close": float(work["microcap"].iloc[i]),
                "hedge_close": float(work["hedge"].iloc[i]),
                "microcap_mom": float(work["microcap_mom"].iloc[i]),
                "hedge_mom": float(work["hedge_mom"].iloc[i]),
                "momentum_gap": float(work["momentum_gap"].iloc[i]),
                "ratio_bias_mom": float(work["ratio_bias_mom"].iloc[i]) if pd.notna(work["ratio_bias_mom"].iloc[i]) else np.nan,
                "ratio_r2": float(work["ratio_r2"].iloc[i]) if pd.notna(work["ratio_r2"].iloc[i]) else np.nan,
                "weight": 1.0,
                "futures_drag": drag,
            }
        )
        holding = signal_on
    result = pd.DataFrame(rows).set_index("date")
    result["return"] = result["return_raw"]
    result["nav"] = (1.0 + result["return"]).cumprod()
    return result


def build_latest_signal(result: pd.DataFrame) -> pd.DataFrame:
    last = result.iloc[[-1]].copy().reset_index()
    last["signal_label"] = np.where(last["next_holding"] == "cash", "cash", "long_microcap_short_zz1000")
    return last[
        [
            "date",
            "signal_label",
            "next_holding",
            "microcap_close",
            "hedge_close",
            "microcap_mom",
            "hedge_mom",
            "momentum_gap",
            "ratio_bias_mom",
            "ratio_r2",
            "weight",
            "futures_drag",
        ]
    ]


def compute_trade_state(current_holding: str, next_holding: str) -> str:
    if current_holding == next_holding:
        return "hold"
    if current_holding == "cash" and next_holding != "cash":
        return "open"
    if current_holding != "cash" and next_holding == "cash":
        return "close"
    return "switch"


def enrich_signal_frame(signal_df: pd.DataFrame, result: pd.DataFrame) -> pd.DataFrame:
    out = signal_df.copy()
    last_row = result.iloc[-1]
    current_holding = str(last_row["holding"])
    next_holding = str(last_row["next_holding"])
    out["current_holding"] = current_holding
    out["trade_state"] = compute_trade_state(current_holding, next_holding)
    return out


def build_biweekly_rebalance_dates(trading_dates: pd.DatetimeIndex, week_anchor: str = REBALANCE_WEEKDAY) -> pd.DatetimeIndex:
    freq = WEEK_FREQ_BY_START[week_anchor]
    week_periods = trading_dates.to_period(freq)
    unique_weeks = sorted(pd.Index(week_periods.unique()))
    week_keys = pd.Series([i // 2 for i, _ in enumerate(unique_weeks)], index=unique_weeks)
    aligned_keys = pd.Index(week_periods).map(lambda p: week_keys[p])
    grouped = trading_dates.to_series().groupby(aligned_keys)
    return pd.DatetimeIndex(grouped.min().tolist())


def locate_rebalance_dates(trading_dates: pd.DatetimeIndex) -> tuple[pd.Timestamp, pd.Timestamp | None, pd.Timestamp | None, pd.Timestamp | None]:
    rebalance_dates = build_biweekly_rebalance_dates(trading_dates)
    last_trade_date = pd.Timestamp(trading_dates[-1])
    available = [pd.Timestamp(dt) for dt in rebalance_dates if pd.Timestamp(dt) <= last_trade_date]
    latest_rebalance = available[-1]
    prev_rebalance = available[-2] if len(available) >= 2 else None
    effective_rebalance = prev_rebalance if latest_rebalance == last_trade_date and prev_rebalance is not None else latest_rebalance
    future = [pd.Timestamp(dt) for dt in rebalance_dates if pd.Timestamp(dt) > last_trade_date]
    next_rebalance = future[0] if future else None
    return latest_rebalance, prev_rebalance, next_rebalance, effective_rebalance


def get_next_trade_date(trading_dates: pd.DatetimeIndex, current_date: pd.Timestamp) -> pd.Timestamp | None:
    future_dates = trading_dates[trading_dates > pd.Timestamp(current_date)]
    if len(future_dates) == 0:
        return None
    return pd.Timestamp(future_dates[0])


def load_effective_members(proxy_members_path: Path, effective_rebalance: pd.Timestamp) -> pd.DataFrame:
    members = pd.read_csv(proxy_members_path, dtype={"symbol": str})
    members["rebalance_date"] = pd.to_datetime(members["rebalance_date"], errors="coerce")
    members["symbol"] = members["symbol"].astype(str).str.zfill(6)
    effective_rebalance = pd.Timestamp(effective_rebalance)
    subset = members.loc[members["rebalance_date"] == effective_rebalance].copy()
    if subset.empty:
        available_dates = (
            members.loc[members["rebalance_date"].notna() & (members["rebalance_date"] <= effective_rebalance), "rebalance_date"]
            .drop_duplicates()
            .sort_values()
        )
        if not available_dates.empty:
            fallback_date = pd.Timestamp(available_dates.iloc[-1])
            subset = members.loc[members["rebalance_date"] == fallback_date].copy()
    if subset.empty:
        raise ValueError(f"未找到生效调仓日 {effective_rebalance.date()} 的成分股。")
    return subset.sort_values("rank").reset_index(drop=True)


def load_latest_close_map(symbols: list[str], as_of_date: pd.Timestamp) -> dict[str, float]:
    out: dict[str, float] = {}
    for symbol in symbols:
        path = PRICE_DIR / f"{symbol}.csv"
        if not path.exists():
            continue
        try:
            price = pd.read_csv(path, usecols=["date", "close_raw"])
            price["date"] = pd.to_datetime(price["date"])
            price["close_raw"] = pd.to_numeric(price["close_raw"], errors="coerce")
            price = price.dropna(subset=["close_raw"])
            price = price.loc[price["date"] <= as_of_date].sort_values("date")
            if not price.empty:
                out[symbol] = float(price.iloc[-1]["close_raw"])
        except Exception:
            continue
    return out


def eastmoney_secid(symbol: str, prefer_index: bool = False) -> str:
    code = str(symbol).zfill(6)
    if prefer_index:
        return f"1.{code}"
    if code.startswith(("5", "6", "9")):
        return f"1.{code}"
    return f"0.{code}"


def fetch_eastmoney_spot(symbol: str, prefer_index: bool = False) -> dict[str, object] | None:
    secid = eastmoney_secid(symbol, prefer_index=prefer_index)
    url = (
        "https://push2.eastmoney.com/api/qt/stock/get"
        f"?secid={secid}"
        "&fields=f43,f44,f45,f46,f57,f58,f60"
    )
    try:
        response = SESSION.get(
            url,
            timeout=10,
            headers={"Referer": "https://quote.eastmoney.com/"},
        )
        response.raise_for_status()
        data = response.json().get("data") or {}
        latest = pd.to_numeric(data.get("f43"), errors="coerce")
        high = pd.to_numeric(data.get("f44"), errors="coerce")
        low = pd.to_numeric(data.get("f45"), errors="coerce")
        open_ = pd.to_numeric(data.get("f46"), errors="coerce")
        prev = pd.to_numeric(data.get("f60"), errors="coerce")
        scale = 100.0
        latest = float(latest) / scale if pd.notna(latest) else np.nan
        high = float(high) / scale if pd.notna(high) else np.nan
        low = float(low) / scale if pd.notna(low) else np.nan
        open_ = float(open_) / scale if pd.notna(open_) else np.nan
        prev = float(prev) / scale if pd.notna(prev) else np.nan
        rt_price = latest if pd.notna(latest) and latest > 0 else prev
        if pd.isna(rt_price) or rt_price <= 0:
            return None
        return {
            "code": str(data.get("f57") or symbol).zfill(6),
            "name": str(data.get("f58") or ""),
            "rt_price": float(rt_price),
            "prev_close": float(prev) if pd.notna(prev) else np.nan,
            "open": float(open_) if pd.notna(open_) else np.nan,
            "high": float(high) if pd.notna(high) else np.nan,
            "low": float(low) if pd.notna(low) else np.nan,
        }
    except Exception:
        return None


def fetch_batch_spot(symbols: list[str], max_workers: int = 16) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    with ThreadPoolExecutor(max_workers=max(1, min(max_workers, 16))) as pool:
        futures = {pool.submit(fetch_eastmoney_spot, symbol): symbol for symbol in symbols}
        for fut in as_completed(futures):
            row = fut.result()
            if row is not None:
                rows.append(row)
    return pd.DataFrame(rows)


def classify_tail_jitter_risk(momentum_gap: float) -> tuple[str, str]:
    abs_gap = abs(float(momentum_gap))
    if abs_gap < TAIL_JITTER_WARNING_GAP:
        return "warning", "gap very close to zero; confirm again near the close"
    if abs_gap < TAIL_JITTER_CAUTION_GAP:
        return "caution", "gap is narrow; close-time recheck is recommended"
    return "normal", ""


def load_cached_realtime_signal(paths: dict[str, Path], cache_seconds: int, latest_anchor_trade_date: pd.Timestamp) -> pd.DataFrame | None:
    meta_path = paths["cache_realtime_meta"]
    signal_path = paths["cache_realtime_signal"]
    if not meta_path.exists() or not signal_path.exists() or cache_seconds <= 0:
        return None
    cache_age_seconds = time.time() - meta_path.stat().st_mtime
    if cache_age_seconds > cache_seconds:
        return None
    try:
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
        if meta.get("latest_anchor_trade_date") != str(pd.Timestamp(latest_anchor_trade_date).date()):
            return None
        signal_df = pd.read_csv(signal_path)
        signal_df["cache_age_seconds"] = float(cache_age_seconds)
        signal_df["from_cache"] = True
        return signal_df
    except Exception:
        return None


def save_cached_realtime_signal(paths: dict[str, Path], signal_df: pd.DataFrame, latest_anchor_trade_date: pd.Timestamp) -> None:
    meta = {
        "latest_anchor_trade_date": str(pd.Timestamp(latest_anchor_trade_date).date()),
        "saved_at": str(pd.Timestamp.now()),
    }
    paths["cache_realtime_meta"].write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
    signal_df.to_csv(paths["cache_realtime_signal"], index=False, encoding="utf-8")


def compute_realtime_signal(
    close_df: pd.DataFrame,
    effective_members: pd.DataFrame,
    paths: dict[str, Path],
    cache_seconds: int,
    anchor_freshness: dict[str, object],
) -> pd.DataFrame:
    if bool(anchor_freshness.get("is_stale")):
        raise RuntimeError(format_anchor_stale_message(anchor_freshness))

    latest_trade_date = pd.Timestamp(close_df.index[-1])
    cached = load_cached_realtime_signal(paths, cache_seconds, latest_trade_date)
    if cached is not None:
        return cached

    member_symbols = effective_members["symbol"].astype(str).str.zfill(6).tolist()
    last_close_map = load_latest_close_map(member_symbols, latest_trade_date)
    quotes_df = fetch_batch_spot(member_symbols)
    if quotes_df.empty:
        raise RuntimeError("实时行情抓取失败：无法获取当前成分股报价。")
    quotes_df = quotes_df.set_index("code")

    member_returns: list[float] = []
    available_rows = 0
    for symbol in member_symbols:
        last_close = last_close_map.get(symbol)
        if last_close is None or last_close <= 0 or symbol not in quotes_df.index:
            continue
        rt_price = pd.to_numeric(quotes_df.at[symbol, "rt_price"], errors="coerce")
        if pd.isna(rt_price) or rt_price <= 0:
            continue
        member_returns.append(float(rt_price / last_close - 1.0))
        available_rows += 1
    if not member_returns:
        raise RuntimeError("实时信号计算失败：当前成分股没有可用实时价格。")

    microcap_rt_close = float(close_df["microcap"].iloc[-1]) * (1.0 + float(np.mean(member_returns)))
    hedge_quote = fetch_eastmoney_spot("000852", prefer_index=True)
    hedge_rt_close = (
        float(hedge_quote["rt_price"])
        if hedge_quote is not None and pd.notna(hedge_quote["rt_price"])
        else float(close_df["hedge"].iloc[-1])
    )

    snapshot_ts = pd.Timestamp.now()
    if snapshot_ts <= latest_trade_date:
        snapshot_ts = latest_trade_date + pd.Timedelta(seconds=1)
    rt_close_df = close_df.copy()
    rt_close_df.loc[snapshot_ts, ["microcap", "hedge"]] = [microcap_rt_close, hedge_rt_close]
    rt_close_df = rt_close_df.sort_index()
    rt_result = run_backtest(rt_close_df)
    signal_df = enrich_signal_frame(build_latest_signal(rt_result), rt_result)
    jitter_level, jitter_note = classify_tail_jitter_risk(float(signal_df.iloc[0]["momentum_gap"]))
    signal_df["quote_source"] = "eastmoney_stock_get"
    signal_df["hedge_quote_source"] = "eastmoney_stock_get"
    signal_df["member_price_count"] = available_rows
    signal_df["member_count"] = len(member_symbols)
    signal_df["latest_anchor_trade_date"] = str(latest_trade_date.date())
    signal_df["tail_jitter_risk"] = jitter_level
    signal_df["tail_jitter_note"] = jitter_note
    signal_df["cache_age_seconds"] = 0.0
    signal_df["from_cache"] = False
    save_cached_realtime_signal(paths, signal_df.drop(columns=["cache_age_seconds", "from_cache"]), latest_trade_date)
    return signal_df


def build_context() -> dict[str, object]:
    paths = build_output_paths(DEFAULT_OUTPUT_PREFIX)
    resolved_panel_path, _ = build_refreshed_panel_shadow(DEFAULT_PANEL, paths)
    close_df = load_close_df(resolved_panel_path, DEFAULT_INDEX_CSV)
    result = run_backtest(close_df)
    latest_signal = enrich_signal_frame(build_latest_signal(result), result)
    latest_rebalance, prev_rebalance, next_rebalance, effective_rebalance = locate_rebalance_dates(close_df.index)
    rebalance_effective_date = get_next_trade_date(close_df.index, latest_rebalance)
    effective_members = load_effective_members(DEFAULT_PROXY_MEMBERS_CSV, effective_rebalance)
    anchor_freshness = assess_history_anchor_freshness(pd.Timestamp(result.index[-1]), DEFAULT_MAX_STALE_ANCHOR_DAYS)
    return {
        "paths": paths,
        "close_df": close_df,
        "result": result,
        "latest_signal": latest_signal,
        "latest_rebalance": latest_rebalance,
        "prev_rebalance": prev_rebalance,
        "next_rebalance": next_rebalance,
        "effective_rebalance": effective_rebalance,
        "rebalance_effective_date": rebalance_effective_date,
        "effective_members": effective_members,
        "anchor_freshness": anchor_freshness,
    }


def format_table(df: pd.DataFrame) -> str:
    if df.empty:
        return "(empty)"
    return df.to_string(index=False)


def handle_signal(context: dict[str, object]) -> tuple[str, list[Path]]:
    anchor_freshness = context["anchor_freshness"]
    if bool(anchor_freshness.get("is_stale")):
        raise RuntimeError(format_anchor_stale_message(anchor_freshness))
    signal_df = context["latest_signal"]
    paths = context["paths"]
    signal_df.to_csv(paths["signal"], index=False, encoding="utf-8")
    body = [
        "确认信号",
        format_table(signal_df),
        (
            "history anchor: {status} | latest={latest} | today={today} | lag={lag}d".format(
                status=anchor_freshness["status"],
                latest=anchor_freshness["latest_trade_date"],
                today=anchor_freshness["current_date"],
                lag=anchor_freshness["stale_calendar_days"],
            )
        ),
        f"已保存: {paths['signal'].name}",
    ]
    return "\n".join(body), [paths["signal"]]


def handle_realtime_signal(context: dict[str, object], cache_seconds: int) -> tuple[str, list[Path]]:
    signal_df = compute_realtime_signal(
        close_df=context["close_df"],
        effective_members=context["effective_members"],
        paths=context["paths"],
        cache_seconds=cache_seconds,
        anchor_freshness=context["anchor_freshness"],
    )
    paths = context["paths"]
    signal_df.to_csv(paths["realtime_signal"], index=False, encoding="utf-8")
    row = signal_df.iloc[0]
    body = [
        "实时信号",
        format_table(signal_df.drop(columns=["from_cache"], errors="ignore")),
        f"实时快照时间: {row['date']}",
        f"锚定最新历史交易日: {row['latest_anchor_trade_date']}",
        f"微盘实时价格来源: {row['quote_source']}",
        f"对冲腿实时价格来源: {row['hedge_quote_source']}",
        f"尾盘抖动风险: {row['tail_jitter_risk']} (|gap|={abs(float(row['momentum_gap'])):.4%})",
        f"结果来源: {'cache' if bool(row.get('from_cache', False)) else 'fresh'}",
        f"实时结果年龄: {float(row.get('cache_age_seconds', 0.0)):.1f} 秒",
        f"已保存: {paths['realtime_signal'].name}",
    ]
    return "\n".join(body), [paths["realtime_signal"]]


def normalize_command(query_text: str) -> tuple[str, int]:
    text = (query_text or "").strip()
    if not text:
        return "信号", DEFAULT_REALTIME_CACHE_SECONDS
    if "实时" in text and "信号" in text:
        if "强制" in text or "刷新" in text:
            return "实时信号", 0
        return "实时信号", DEFAULT_REALTIME_CACHE_SECONDS
    if "信号" in text:
        return "信号", DEFAULT_REALTIME_CACHE_SECONDS
    return "帮助", DEFAULT_REALTIME_CACHE_SECONDS


def help_text() -> str:
    return (
        "支持命令：\n"
        "1. `信号`\n"
        "2. `实时信号`\n"
        "3. `强制刷新实时信号`\n\n"
        "依赖文件：\n"
        f"- `{DEFAULT_PANEL.name}`\n"
        f"- `{DEFAULT_INDEX_CSV.name}`\n"
        f"- `{DEFAULT_PROXY_MEMBERS_CSV.name}`\n"
        f"- `.microcap_index_cache/prices_raw/*.csv`\n"
    )


def content_type(path: Path) -> str:
    if path.suffix.lower() == ".csv":
        return "text/csv"
    return "application/octet-stream"


def send_message(text: str, attachments: list[Path] | None = None) -> None:
    if poe is None or not hasattr(poe, "start_message"):
        print(text)
        if attachments:
            for path in attachments:
                print(f"[file] {path}")
        return

    with poe.start_message() as msg:
        msg.write(text)
        if attachments:
            for path in attachments:
                if not path.exists():
                    continue
                msg.attach_file(
                    name=path.name,
                    contents=path.read_bytes(),
                    content_type=content_type(path),
                )


class MicrocapTop100StandaloneBot:
    def run(self) -> None:
        if poe is not None and hasattr(poe, "query") and getattr(poe, "query", None) is not None:
            query_text = (poe.query.text or "").strip()
        else:
            query_text = " ".join(sys.argv[1:]).strip()

        command, cache_seconds = normalize_command(query_text)
        if command == "帮助":
            send_message(help_text())
            return

        try:
            context = build_context()
            if command == "实时信号":
                body, attachments = handle_realtime_signal(context, cache_seconds)
                header = "## 实时信号\n\n"
            else:
                body, attachments = handle_signal(context)
                header = "## 收盘确认信号\n\n"
        except Exception as exc:
            send_message(f"计算失败：{exc}")
            return

        send_message(f"{header}```text\n{body}\n```", attachments)


_update_settings()


if __name__ == "__main__":
    MicrocapTop100StandaloneBot().run()
