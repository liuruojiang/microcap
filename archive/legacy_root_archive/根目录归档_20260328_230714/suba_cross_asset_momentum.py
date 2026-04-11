import io
import json
import time
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


COMMISSION = 0.001
TRADING_DAYS = 252
BIAS_N = 60
MOM_DAY = 40
R2_WINDOW = 20
R2_THRESHOLD = 0.20
TARGET_VOL = 0.30
VOL_WINDOW = 40
MAX_LEV = 1.5
MIN_LEV = 0.1
SCALE_THRESHOLD = 0.00

DEFAULT_PARAMS = {
    "commission": COMMISSION,
    "trading_days": TRADING_DAYS,
    "bias_n": BIAS_N,
    "mom_day": MOM_DAY,
    "r2_window": R2_WINDOW,
    "r2_threshold": R2_THRESHOLD,
    "target_vol": TARGET_VOL,
    "vol_window": VOL_WINDOW,
    "max_lev": MAX_LEV,
    "min_lev": MIN_LEV,
    "scale_threshold": SCALE_THRESHOLD,
}

US_TICKERS = ["QQQ", "GLD", "TLT", "BIL"]
RISKY_CODES = ["QQQ", "GLD", "TLT", "CN_CHINEXT_TR", "CN_DIVLOWVOL_TR"]
CASH_CODE = "BIL"

CN_INDEX_CONFIG = {
    "CN_CHINEXT_TR": {
        "label": "Chinext Total Return",
        "provider": "cnindex",
        "candidates": ["399606"],
    },
    "CN_DIVLOWVOL_TR": {
        "label": "CSI Dividend Low Vol Total Return",
        "provider": "csindex",
        "candidates": ["H30269", "H20955"],
    },
}


def get_session():
    session = requests.Session()
    retries = Retry(
        total=5,
        connect=3,
        read=3,
        backoff_factor=1.5,
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    session.mount("https://", HTTPAdapter(max_retries=retries))
    session.mount("http://", HTTPAdapter(max_retries=retries))
    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/131.0.0.0 Safari/537.36"
            )
        }
    )
    return session


SESSION = get_session()
DATA_FETCH_ERRORS = (
    requests.exceptions.RequestException,
    json.JSONDecodeError,
    KeyError,
    ValueError,
    TypeError,
    IndexError,
)


def fetch_us_yahoo(ticker, start_date="2003-01-01"):
    start_ts = int(pd.Timestamp(start_date).timestamp())
    end_ts = int((datetime.now() + timedelta(days=30)).timestamp())
    url = (
        f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
        f"?period1={start_ts}&period2={end_ts}&interval=1d&includeAdjustedClose=true"
    )
    resp = SESSION.get(url, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    result = data["chart"]["result"][0]
    timestamps = result.get("timestamp", [])
    if not timestamps:
        raise ValueError(f"No Yahoo timestamps for {ticker}")
    quote = result["indicators"]["quote"][0]
    adj = result["indicators"].get("adjclose", [{}])[0]
    rows = []
    for i, ts in enumerate(timestamps):
        close_raw = quote["close"][i]
        if close_raw is None:
            continue
        close_adj = adj.get("adjclose", [None] * len(timestamps))[i]
        if close_adj is None:
            close_adj = close_raw
        rows.append(
            {
                "date": pd.Timestamp.fromtimestamp(ts),
                "close": float(close_adj),
            }
        )
    df = pd.DataFrame(rows)
    if df.empty:
        raise ValueError(f"Yahoo returned empty data for {ticker}")
    return df.drop_duplicates(subset="date").set_index("date").sort_index()


def fetch_us_stooq(ticker, start_date="2003-01-01"):
    stooq_sym = f"{ticker.lower()}.us"
    d1 = pd.Timestamp(start_date).strftime("%Y%m%d")
    d2 = (datetime.now() + timedelta(days=30)).strftime("%Y%m%d")
    url = f"https://stooq.com/q/d/l/?s={stooq_sym}&d1={d1}&d2={d2}&i=d"
    resp = SESSION.get(url, timeout=30)
    resp.raise_for_status()
    text = resp.text.strip()
    if not text or "No data" in text:
        raise ValueError(f"Stooq returned no data for {ticker}")
    df = pd.read_csv(io.StringIO(text))
    if df.empty or "Close" not in df.columns:
        raise ValueError(f"Invalid Stooq CSV for {ticker}")
    df = df.rename(columns={"Date": "date", "Close": "close"})
    df["date"] = pd.to_datetime(df["date"])
    return df[["date", "close"]].dropna().set_index("date").sort_index()


def fetch_us_series(ticker, start_date="2003-01-01"):
    last_err = None
    for source_name, fetcher in (
        ("Yahoo", lambda: fetch_us_yahoo(ticker, start_date)),
        ("Stooq", lambda: fetch_us_stooq(ticker, start_date)),
    ):
        try:
            df = fetcher()
            if len(df) > 50:
                return df, source_name
        except DATA_FETCH_ERRORS as exc:
            last_err = exc
            time.sleep(1)
    raise ValueError(f"US data failed for {ticker}: {last_err}")


def fetch_csindex_series(index_code, start_date="20050101"):
    end_date = (datetime.now() + timedelta(days=30)).strftime("%Y%m%d")
    url = (
        "https://www.csindex.com.cn/csindex-home/perf/index-perf"
        f"?indexCode={index_code}&startDate={start_date}&endDate={end_date}"
    )
    headers = {
        "Referer": "https://www.csindex.com.cn/",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
    }
    resp = SESSION.get(url, timeout=30, headers=headers)
    resp.raise_for_status()
    data = resp.json()
    rows = data.get("data") or []
    if not rows:
        raise ValueError(f"CSIndex returned no data for {index_code}")
    frame = pd.DataFrame(
        {
            "date": pd.to_datetime([row["tradeDate"] for row in rows if row]),
            "close": [float(row["close"]) for row in rows if row],
        }
    )
    if frame.empty:
        raise ValueError(f"CSIndex returned empty rows for {index_code}")
    return frame.drop_duplicates(subset="date").set_index("date").sort_index()


def fetch_cnindex_series(index_code, start_date="2005-01-01"):
    url = "https://hq.cnindex.com.cn/market/market/getIndexDailyDataWithDataFormat"
    params = {
        "indexCode": index_code,
        "startDate": start_date,
        "endDate": (datetime.now() + timedelta(days=30)).strftime("%Y-%m-%d"),
        "frequency": "day",
    }
    headers = {
        "Referer": "https://www.cnindex.com.cn/",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
    }
    resp = SESSION.get(url, params=params, timeout=30, headers=headers)
    resp.raise_for_status()
    payload = resp.json()
    rows = (payload.get("data") or {}).get("data") or []
    if not rows:
        raise ValueError(f"CNIndex returned no data for {index_code}")
    frame = pd.DataFrame(
        {
            "date": pd.to_datetime([row[0] for row in rows]),
            "close": [float(row[5]) for row in rows],
        }
    )
    if frame.empty:
        raise ValueError(f"CNIndex returned empty rows for {index_code}")
    return frame.drop_duplicates(subset="date").set_index("date").sort_index()


def fetch_cn_series_with_candidates(symbol):
    config = CN_INDEX_CONFIG[symbol]
    fetcher = fetch_csindex_series if config.get("provider") == "csindex" else fetch_cnindex_series
    last_err = None
    for candidate in config["candidates"]:
        try:
            df = fetcher(candidate)
            if len(df) > 50:
                return df, f"{config.get('provider', 'unknown').upper()}:{candidate}"
        except DATA_FETCH_ERRORS as exc:
            last_err = exc
            time.sleep(1)
    raise ValueError(f"CN data failed for {symbol}: {last_err}")


def normalize_daily_close(series):
    normalized = series.copy()
    normalized.index = pd.to_datetime(normalized.index).normalize()
    normalized = normalized.groupby(level=0).last()
    return normalized.sort_index()


def calc_bias_momentum(close_series, bias_n=BIAS_N, mom_day=MOM_DAY):
    prices = close_series.values.astype(float)
    ma = close_series.rolling(bias_n).mean().values
    result = np.full(len(prices), np.nan)
    x = np.arange(mom_day, dtype=float)
    total_lookback = bias_n + mom_day - 1
    for i in range(total_lookback, len(prices)):
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
    return pd.Series(result, index=close_series.index)


def calc_rolling_r2(close_series, window=R2_WINDOW):
    values = close_series.values.astype(float)
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
    return pd.Series(result, index=close_series.index)


def run_suba_style_strategy(close_df, risky_codes, cash_code, params=None):
    cfg = DEFAULT_PARAMS.copy()
    if params:
        cfg.update(params)
    bias = {
        code: calc_bias_momentum(close_df[code], cfg["bias_n"], cfg["mom_day"])
        for code in risky_codes
    }
    r2 = {code: calc_rolling_r2(close_df[code], cfg["r2_window"]) for code in risky_codes}
    start_idx = cfg["bias_n"] + cfg["mom_day"]
    holding = cash_code
    rows = []
    for i in range(start_idx, len(close_df)):
        date = close_df.index[i]
        scores = {}
        for code in risky_codes:
            value = bias[code].iloc[i]
            if not np.isnan(value):
                scores[code] = value
        ideal = cash_code
        if scores:
            best = max(scores, key=scores.get)
            if scores[best] > 0:
                r2_value = r2[best].iloc[i]
                if not np.isnan(r2_value) and r2_value >= cfg["r2_threshold"]:
                    ideal = best
        target = ideal if ideal != holding else None
        held_asset = holding
        held_ret = close_df.iloc[i][held_asset] / close_df.iloc[i - 1][held_asset] - 1
        if target is not None:
            legs = 1 if held_asset == cash_code or target == cash_code else 2
            day_ret = (1 + held_ret) * ((1 - cfg["commission"]) ** legs) - 1
            holding = target
        else:
            day_ret = held_ret
        rows.append(
            {
                "date": date,
                "return_raw": day_ret,
                "holding": holding,
                "target": target,
                "is_signal": target is not None,
            }
        )
    result = pd.DataFrame(rows).set_index("date")
    realized_vol = result["return_raw"].rolling(cfg["vol_window"]).std() * np.sqrt(cfg["trading_days"])
    scale_raw = (cfg["target_vol"] / realized_vol).clip(cfg["min_lev"], cfg["max_lev"]).shift(1)
    if cfg["scale_threshold"] > 0:
        scale_arr = scale_raw.to_numpy(copy=True)
        last_scale = np.nan
        for i in range(len(scale_arr)):
            if np.isnan(scale_arr[i]):
                continue
            if np.isnan(last_scale):
                last_scale = scale_arr[i]
            elif abs(scale_arr[i] - last_scale) >= cfg["scale_threshold"] - 1e-9:
                last_scale = scale_arr[i]
            else:
                scale_arr[i] = last_scale
        scale_raw = pd.Series(scale_arr, index=result.index)
    weights = scale_raw.fillna(1.0)
    weights[result["holding"] == cash_code] = 1.0
    prev_weights = weights.shift(1).fillna(weights.iloc[0])
    scale_turnover = (weights - prev_weights).abs()
    scale_tc = np.where(
        (~result["is_signal"]) & (result["holding"] != cash_code),
        cfg["commission"] * scale_turnover,
        0.0,
    )
    result["realized_vol"] = realized_vol
    result["scale_raw"] = scale_raw
    result["weight"] = weights
    result["scale_tc"] = scale_tc
    result["return"] = (1 + result["return_raw"] * weights) * (1 - scale_tc) - 1
    result["nav"] = (1 + result["return"]).cumprod()
    return result, bias, r2


def calc_summary(result, params=None):
    cfg = DEFAULT_PARAMS.copy()
    if params:
        cfg.update(params)
    daily = result["return"].dropna()
    nav = result["nav"].dropna()
    if daily.empty or nav.empty:
        return {}
    total_return = nav.iloc[-1] - 1
    periods = len(daily)
    annual_return = (1 + total_return) ** (cfg["trading_days"] / periods) - 1 if periods > 0 else np.nan
    annual_vol = daily.std(ddof=1) * np.sqrt(cfg["trading_days"]) if periods > 1 else np.nan
    sharpe = annual_return / annual_vol if annual_vol and annual_vol > 0 else np.nan
    max_dd = ((nav - nav.cummax()) / nav.cummax()).min()
    monthly = daily.groupby(daily.index.to_period("M")).apply(lambda x: (1 + x).prod() - 1)
    return {
        "start": str(result.index[0].date()),
        "end": str(result.index[-1].date()),
        "holding": result["holding"].iloc[-1],
        "nav": float(nav.iloc[-1]),
        "total_return": float(total_return),
        "annual_return": float(annual_return) if pd.notna(annual_return) else None,
        "annual_vol": float(annual_vol) if pd.notna(annual_vol) else None,
        "sharpe": float(sharpe) if pd.notna(sharpe) else None,
        "max_drawdown": float(max_dd) if pd.notna(max_dd) else None,
        "calmar": float(annual_return / abs(max_dd)) if pd.notna(max_dd) and max_dd < 0 else None,
        "monthly_win_rate": float((monthly > 0).mean()) if len(monthly) else None,
        "months": int(len(monthly)),
    }


def build_close_matrix():
    series_map = {}
    source_map = {}
    for ticker in US_TICKERS:
        df, source = fetch_us_series(ticker)
        series_map[ticker] = normalize_daily_close(df["close"]).rename(ticker)
        source_map[ticker] = source
    for symbol in CN_INDEX_CONFIG:
        df, source = fetch_cn_series_with_candidates(symbol)
        series_map[symbol] = normalize_daily_close(df["close"]).rename(symbol)
        source_map[symbol] = source
    close_df = pd.concat(series_map.values(), axis=1).sort_index().ffill().dropna()
    return close_df, source_map


def _safe_output_path(filename):
    try:
        with open(filename, "a", encoding="utf-8"):
            pass
        return filename
    except PermissionError:
        stem, ext = filename.rsplit(".", 1)
        return f"{stem}_fixed.{ext}"


def write_outputs(close_df, result, bias, r2, summary, source_map):
    base_dir = "."
    close_path = _safe_output_path(f"{base_dir}/suba_cross_asset_close.csv")
    nav_path = _safe_output_path(f"{base_dir}/suba_cross_asset_nav.csv")
    signal_path = _safe_output_path(f"{base_dir}/suba_cross_asset_latest_signal.csv")
    summary_path = _safe_output_path(f"{base_dir}/suba_cross_asset_summary.json")
    close_df.to_csv(close_path, encoding="utf-8-sig")
    result.to_csv(nav_path, encoding="utf-8-sig")
    latest = pd.DataFrame(
        [
            {
                "asset": code,
                "bias_momentum": bias[code].iloc[-1] if code in bias else np.nan,
                "r2": r2[code].iloc[-1] if code in r2 else np.nan,
                "is_current_holding": result["holding"].iloc[-1] == code,
                "source": source_map.get(code, ""),
            }
            for code in RISKY_CODES
        ]
    )
    latest.to_csv(signal_path, index=False, encoding="utf-8-sig")
    with open(summary_path, "w", encoding="utf-8") as fh:
        json.dump(
            {
                "summary": summary,
                "sources": source_map,
                "assumptions": {
                    "chinext_candidates": CN_INDEX_CONFIG["CN_CHINEXT_TR"]["candidates"],
                    "divlowvol_candidates": CN_INDEX_CONFIG["CN_DIVLOWVOL_TR"]["candidates"],
                    "cash_asset": CASH_CODE,
                },
            },
            fh,
            ensure_ascii=False,
            indent=2,
        )
    return {
        "close": close_path,
        "nav": nav_path,
        "signal": signal_path,
        "summary": summary_path,
    }


def main():
    close_df, source_map = build_close_matrix()
    result, bias, r2 = run_suba_style_strategy(close_df, RISKY_CODES, CASH_CODE, DEFAULT_PARAMS)
    summary = calc_summary(result, DEFAULT_PARAMS)
    output_paths = write_outputs(close_df, result, bias, r2, summary, source_map)
    latest_holding = summary.get("holding")
    latest_nav = summary.get("nav")
    print("Sub-A style cross-asset momentum strategy finished.")
    print(f"Date range: {summary.get('start')} -> {summary.get('end')}")
    print(f"Latest holding: {latest_holding}")
    print(f"Latest NAV: {latest_nav:.4f}" if latest_nav is not None else "Latest NAV: n/a")
    print("Files:")
    print(f"  {output_paths['close']}")
    print(f"  {output_paths['nav']}")
    print(f"  {output_paths['signal']}")
    print(f"  {output_paths['summary']}")


if __name__ == "__main__":
    main()
