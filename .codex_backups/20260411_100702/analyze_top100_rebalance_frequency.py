from __future__ import annotations

import json
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
from decimal import ROUND_HALF_UP, Decimal
from io import BytesIO
from pathlib import Path

import numpy as np
import pandas as pd
import requests
from pandas.errors import PerformanceWarning

import analyze_microcap_zz1000_hedge as hedge_mod
import fetch_wind_microcap_index as index_mod
import scan_top100_momentum_costs as cost_mod


ROOT = Path(__file__).resolve().parent
OUTPUT_DIR = ROOT / "outputs"
CACHE_DIR = ROOT / ".microcap_index_cache"
PRICE_DIR = CACHE_DIR / "prices_raw"
ADJ_PRICE_DIR = CACHE_DIR / "prices_qfq"
OHLC_DIR = CACHE_DIR / "prices_ohlc"
SHARE_DIR = CACHE_DIR / "share_change"
ACTIVE_UNIVERSE = CACHE_DIR / "active_universe.csv"
CURRENT_ST = CACHE_DIR / "current_st.csv"
SECURITY_META_DIR = CACHE_DIR / "security_meta"
SZ_NAME_CHANGE_CACHE = CACHE_DIR / "sz_name_change_short.csv"
CNINFO_ORG_MAP_CACHE = CACHE_DIR / "cninfo_a_org_map.csv"
FALLBACK_OHLC_DIR = ROOT / ".microcap_ohlc_cache"

START_DATE = "2010-01-04"
END_DATE = pd.Timestamp.today().strftime("%Y-%m-%d")
LOOKBACK = 16
TOP_N = 100
CHINEXT_LIMIT_SWITCH = pd.Timestamp("2020-08-24")
LIMIT_PRICE_EPS = 0.011
SCHEDULES = {
    "monthly": "month_start",
    "biweekly": "biweek_start",
    "weekly": "week_start",
}
EXECUTION_TIMING_NEXT_OPEN = "next_open"
EXECUTION_TIMING_CLOSE = "close"
TRADE_CONSTRAINT_MODE_NEXT_OPEN = "next_open"
TRADE_CONSTRAINT_MODE_CLOSE = "close"

warnings.filterwarnings("ignore", category=PerformanceWarning)


def find_shared_cache_root() -> Path | None:
    for child in ROOT.parent.iterdir():
        candidate = child / ".microcap_index_cache"
        if candidate.exists() and candidate != CACHE_DIR:
            return candidate
    return None


SHARED_CACHE_DIR = find_shared_cache_root()
SHARED_PRICE_DIR = SHARED_CACHE_DIR / "prices_raw" if SHARED_CACHE_DIR else None
SHARED_ADJ_PRICE_DIR = SHARED_CACHE_DIR / "prices_qfq" if SHARED_CACHE_DIR else None
SHARED_SHARE_DIR = SHARED_CACHE_DIR / "share_change" if SHARED_CACHE_DIR else None
SHARED_OHLC_DIR = SHARED_CACHE_DIR / "prices_ohlc" if SHARED_CACHE_DIR else None
SHARED_SECURITY_META_DIR = SHARED_CACHE_DIR / "security_meta" if SHARED_CACHE_DIR else None


def resolve_cache_path(local_dir: Path, shared_dir: Path | None, symbol: str) -> Path | None:
    local_path = local_dir / f"{symbol}.csv"
    if local_path.exists():
        return local_path
    if shared_dir is not None:
        shared_path = shared_dir / f"{symbol}.csv"
        if shared_path.exists():
            return shared_path
    return None


def _existing_symbols(local_dir: Path, shared_dir: Path | None) -> set[str]:
    symbols: set[str] = set()
    for base_dir in [local_dir, shared_dir]:
        if base_dir is None or not base_dir.exists():
            continue
        for path in base_dir.glob("*.csv"):
            symbols.add(path.stem.zfill(6))
    return symbols


def list_backtest_universe_symbols() -> list[str]:
    price_symbols = _existing_symbols(PRICE_DIR, SHARED_PRICE_DIR)
    share_symbols = _existing_symbols(SHARE_DIR, SHARED_SHARE_DIR)
    return sorted(price_symbols & share_symbols)


def load_trading_dates() -> pd.DatetimeIndex:
    panel = pd.read_csv(hedge_mod.DEFAULT_PANEL, usecols=["date"])
    panel["date"] = pd.to_datetime(panel["date"])
    dates = panel["date"].drop_duplicates().sort_values()
    dates = dates[(dates >= pd.Timestamp(START_DATE)) & (dates <= pd.Timestamp(END_DATE))]
    return pd.DatetimeIndex(dates)


def load_current_universe() -> list[str]:
    universe = pd.read_csv(ACTIVE_UNIVERSE, dtype=str)
    st_codes = set(pd.read_csv(CURRENT_ST, dtype=str)["code"].dropna())
    universe = universe[~universe["code"].isin(st_codes)].copy()
    codes = []
    for code in universe["code"].tolist():
        if resolve_cache_path(PRICE_DIR, SHARED_PRICE_DIR, code) and resolve_cache_path(SHARE_DIR, SHARED_SHARE_DIR, code):
            codes.append(code)
    return codes


def load_universe() -> list[str]:
    return list_backtest_universe_symbols()


def is_st_name(name: str | None) -> bool:
    text = str(name or "").strip().upper().replace(" ", "")
    return text.startswith(("*ST", "ST", "PT"))


def build_st_intervals_from_name_changes(
    first_trade_date: pd.Timestamp,
    last_trade_date: pd.Timestamp,
    changes: pd.DataFrame,
) -> list[dict[str, str | None]]:
    if changes.empty:
        return []
    work = changes.copy()
    work["change_date"] = pd.to_datetime(work["change_date"], errors="coerce")
    work = work.dropna(subset=["change_date"]).sort_values("change_date")
    if work.empty:
        return []

    first_trade = pd.Timestamp(first_trade_date).normalize()
    last_trade = pd.Timestamp(last_trade_date).normalize()
    intervals: list[dict[str, str | None]] = []
    active_at_start = False

    for row in work.loc[work["change_date"] < first_trade].itertuples(index=False):
        old_is_st = is_st_name(getattr(row, "old_name", ""))
        new_is_st = is_st_name(getattr(row, "new_name", ""))
        if old_is_st and not new_is_st:
            active_at_start = False
        elif (not old_is_st) and new_is_st:
            active_at_start = True

    active_start: pd.Timestamp | None = first_trade if active_at_start else None

    for row in work.itertuples(index=False):
        change_date = pd.Timestamp(row.change_date).normalize()
        if change_date < first_trade:
            continue
        if change_date > last_trade:
            continue
        old_name = getattr(row, "old_name", "")
        new_name = getattr(row, "new_name", "")
        old_is_st = is_st_name(old_name)
        new_is_st = is_st_name(new_name)

        if old_is_st and not new_is_st:
            if active_start is None:
                active_start = first_trade
            intervals.append(
                {
                    "start": str(active_start.date()),
                    "end": str(change_date.date()),
                    "source": "name_change",
                }
            )
            active_start = None
        elif (not old_is_st) and new_is_st:
            if active_start is None:
                active_start = max(first_trade, change_date)

    if active_start is not None:
        intervals.append({"start": str(active_start.date()), "end": None, "source": "name_change"})
    return intervals


def build_st_intervals_from_notices(
    first_trade_date: pd.Timestamp,
    last_trade_date: pd.Timestamp,
    notices: pd.DataFrame,
) -> list[dict[str, str | None]]:
    if notices.empty:
        return []
    work = notices.copy()
    work["notice_date"] = pd.to_datetime(work["notice_date"], errors="coerce")
    work = work.dropna(subset=["notice_date"]).sort_values("notice_date")
    if work.empty:
        return []

    def infer_action(title: str) -> str | None:
        text = str(title or "").upper().replace(" ", "")
        if not text or "申请" in text or "提示性" in text:
            return None
        if "撤销" in text and any(token in text for token in ["退市风险警示", "其他特别处理", "其他风险警示", "特别处理"]):
            return "exit"
        if (
            any(token in text for token in ["实施", "实行"])
            and any(token in text for token in ["退市风险警示", "其他风险警示", "特别处理"])
            and "可能" not in text
            and "撤销" not in text
        ):
            return "entry"
        return None

    first_trade = pd.Timestamp(first_trade_date).normalize()
    last_trade = pd.Timestamp(last_trade_date).normalize()
    intervals: list[dict[str, str | None]] = []
    active_at_start = False

    for row in work.loc[work["notice_date"] < first_trade].itertuples(index=False):
        action = infer_action(getattr(row, "title", ""))
        if action == "entry":
            active_at_start = True
        elif action == "exit":
            active_at_start = False

    active_start: pd.Timestamp | None = first_trade if active_at_start else None

    for row in work.itertuples(index=False):
        notice_date = pd.Timestamp(row.notice_date).normalize()
        if notice_date < first_trade:
            continue
        if notice_date > last_trade:
            continue
        action = infer_action(getattr(row, "title", ""))
        if action == "exit":
            if active_start is None:
                active_start = first_trade
            intervals.append(
                {
                    "start": str(active_start.date()),
                    "end": str(notice_date.date()),
                    "source": "cninfo_notice",
                }
            )
            active_start = None
        elif action == "entry":
            if active_start is None:
                active_start = max(first_trade, notice_date)

    if active_start is not None:
        intervals.append({"start": str(active_start.date()), "end": None, "source": "cninfo_notice"})
    return intervals


def build_st_status_series(meta: dict[str, object] | None, dates: pd.DatetimeIndex) -> pd.Series:
    series = pd.Series(False, index=dates, dtype=bool)
    if not meta:
        return series
    intervals = meta.get("st_intervals")
    if not isinstance(intervals, list):
        return series
    max_date = pd.Timestamp(dates.max()) if len(dates) else None
    for item in intervals:
        if not isinstance(item, dict):
            continue
        start = pd.to_datetime(item.get("start"), errors="coerce")
        if pd.isna(start):
            continue
        end = pd.to_datetime(item.get("end"), errors="coerce")
        if pd.isna(end):
            end = max_date
        if end is None:
            continue
        mask = (series.index >= pd.Timestamp(start)) & (series.index <= pd.Timestamp(end))
        series.loc[mask] = True
    return series


def fetch_sz_name_change_history(force_refresh: bool = False) -> pd.DataFrame:
    if SZ_NAME_CHANGE_CACHE.exists() and not force_refresh:
        frame = pd.read_csv(SZ_NAME_CHANGE_CACHE, dtype={"symbol": str})
        frame["change_date"] = pd.to_datetime(frame["change_date"], errors="coerce")
        return frame

    url = "https://www.szse.cn/api/report/ShowReport"
    params = {
        "SHOWTYPE": "xlsx",
        "CATALOGID": "SSGSGMXX",
        "TABKEY": "tab2",
        "random": "0.6935816432433362",
    }
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        response = requests.get(url, params=params, timeout=30, verify=False)
    response.raise_for_status()
    frame = pd.read_excel(BytesIO(response.content))
    frame = frame.rename(
        columns={
            "变更日期": "change_date",
            "证券代码": "symbol",
            "变更前简称": "old_name",
            "变更后简称": "new_name",
        }
    )
    frame["symbol"] = frame["symbol"].astype(str).str.zfill(6)
    frame["change_date"] = pd.to_datetime(frame["change_date"], errors="coerce")
    frame = frame.dropna(subset=["change_date", "symbol", "old_name", "new_name"]).sort_values("change_date")
    frame = frame[["change_date", "symbol", "old_name", "new_name"]].reset_index(drop=True)
    frame.to_csv(SZ_NAME_CHANGE_CACHE, index=False, encoding="utf-8")
    return frame


def fetch_cninfo_org_map(force_refresh: bool = False) -> dict[str, str]:
    if CNINFO_ORG_MAP_CACHE.exists() and not force_refresh:
        frame = pd.read_csv(CNINFO_ORG_MAP_CACHE, dtype=str)
        return dict(zip(frame["code"], frame["org_id"]))

    response = requests.get("http://www.cninfo.com.cn/new/data/szse_stock.json", timeout=30)
    response.raise_for_status()
    data = response.json()
    frame = pd.DataFrame(data.get("stockList") or [])
    frame = frame.rename(columns={"code": "code", "orgId": "org_id"})
    frame = frame[["code", "org_id"]].dropna().drop_duplicates(subset=["code"])
    frame["code"] = frame["code"].astype(str).str.zfill(6)
    frame.to_csv(CNINFO_ORG_MAP_CACHE, index=False, encoding="utf-8")
    return dict(zip(frame["code"], frame["org_id"]))


def fetch_cninfo_st_notices(symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
    org_map = fetch_cninfo_org_map()
    org_id = org_map.get(str(symbol).zfill(6))
    if not org_id:
        return pd.DataFrame(columns=["notice_date", "title"])

    payload = {
        "pageNum": "1",
        "pageSize": "30",
        "column": "szse",
        "tabName": "fulltext",
        "plate": "",
        "stock": f"{str(symbol).zfill(6)},{org_id}",
        "searchkey": "",
        "secid": "",
        "category": "category_tbclts_szsh",
        "trade": "",
        "seDate": f"{start_date[:4]}-{start_date[4:6]}-{start_date[6:]}~{end_date[:4]}-{end_date[4:6]}-{end_date[6:]}",
        "sortName": "",
        "sortType": "",
        "isHLtitle": "true",
    }
    response = requests.post("http://www.cninfo.com.cn/new/hisAnnouncement/query", data=payload, timeout=30)
    response.raise_for_status()
    data = response.json()
    total = int(data.get("totalAnnouncement") or 0)
    if total <= 0:
        return pd.DataFrame(columns=["notice_date", "title"])

    rows: list[dict[str, object]] = []
    total_pages = max(1, (total + 29) // 30)
    for page in range(1, total_pages + 1):
        payload["pageNum"] = str(page)
        page_resp = requests.post("http://www.cninfo.com.cn/new/hisAnnouncement/query", data=payload, timeout=30)
        page_resp.raise_for_status()
        page_data = page_resp.json()
        for item in page_data.get("announcements") or []:
            rows.append(
                {
                    "notice_date": pd.to_datetime(item.get("announcementTime"), unit="ms", utc=True, errors="coerce")
                    .tz_convert("Asia/Shanghai")
                    .tz_localize(None),
                    "title": item.get("announcementTitle") or "",
                }
            )
    frame = pd.DataFrame(rows)
    if frame.empty:
        return pd.DataFrame(columns=["notice_date", "title"])
    frame = frame.dropna(subset=["notice_date"]).sort_values("notice_date").reset_index(drop=True)
    return frame


def resolve_security_meta_path(symbol: str) -> Path | None:
    return resolve_cache_path(SECURITY_META_DIR, SHARED_SECURITY_META_DIR, symbol)


def build_security_meta(symbol: str) -> dict[str, object] | None:
    price_path = resolve_cache_path(PRICE_DIR, SHARED_PRICE_DIR, symbol)
    if price_path is None:
        return None
    price = pd.read_csv(price_path, usecols=["date"])
    price["date"] = pd.to_datetime(price["date"], errors="coerce")
    price = price.dropna(subset=["date"]).sort_values("date")
    if price.empty:
        return None

    first_trade = pd.Timestamp(price["date"].min()).normalize()
    last_trade = pd.Timestamp(price["date"].max()).normalize()
    st_intervals: list[dict[str, str | None]] = []

    if str(symbol).zfill(6).startswith(("000", "001", "002", "003", "300", "301")):
        try:
            changes = fetch_sz_name_change_history()
            symbol_changes = changes.loc[changes["symbol"] == str(symbol).zfill(6), ["change_date", "old_name", "new_name"]]
            st_intervals = build_st_intervals_from_name_changes(first_trade, last_trade, symbol_changes)
        except Exception:
            st_intervals = []

    if not st_intervals:
        try:
            notices = fetch_cninfo_st_notices(
                symbol=str(symbol).zfill(6),
                start_date=first_trade.strftime("%Y%m%d"),
                end_date=last_trade.strftime("%Y%m%d"),
            )
            st_intervals = build_st_intervals_from_notices(first_trade, last_trade, notices)
        except Exception:
            st_intervals = []

    meta = {
        "symbol": str(symbol).zfill(6),
        "first_trade_date": str(first_trade.date()),
        "last_trade_date": str(last_trade.date()),
        "st_intervals": st_intervals,
    }
    SECURITY_META_DIR.mkdir(parents=True, exist_ok=True)
    (SECURITY_META_DIR / f"{str(symbol).zfill(6)}.json").write_text(
        json.dumps(meta, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return meta


def load_security_meta(symbol: str) -> dict[str, object] | None:
    meta_path = resolve_security_meta_path(symbol)
    if meta_path is not None:
        try:
            return json.loads(meta_path.read_text(encoding="utf-8"))
        except Exception:
            pass
    try:
        return build_security_meta(symbol)
    except Exception:
        return None


def read_ohlc_cache(symbol: str, start_ts: pd.Timestamp, end_ts: pd.Timestamp) -> pd.DataFrame:
    required = {"date", "open", "close", "high", "low"}
    for base_dir in (OHLC_DIR, FALLBACK_OHLC_DIR, SHARED_OHLC_DIR):
        if base_dir is None:
            continue
        path = base_dir / f"{symbol}.csv"
        if not path.exists():
            continue
        try:
            frame = pd.read_csv(path)
        except Exception:
            continue
        if not required.issubset(frame.columns):
            continue
        frame["date"] = pd.to_datetime(frame["date"], errors="coerce")
        for col in ["open", "close", "high", "low"]:
            frame[col] = pd.to_numeric(frame[col], errors="coerce")
        frame = frame.dropna(subset=["date", "open", "close", "high", "low"]).sort_values("date")
        frame = frame[(frame["date"] >= start_ts) & (frame["date"] <= end_ts)].copy()
        if not frame.empty:
            return frame
    return pd.DataFrame(columns=["date", "open", "close", "high", "low"])


def round_limit_price(value: float) -> float:
    return float(Decimal(str(value)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


def get_price_limit_ratio(symbol: str, trade_date: pd.Timestamp, is_st: bool = False) -> float:
    code = str(symbol).zfill(6)
    if is_st and code.startswith(("300", "301")) and pd.Timestamp(trade_date) < CHINEXT_LIMIT_SWITCH:
        return 0.05
    if code.startswith(("300", "301")):
        return 0.2 if pd.Timestamp(trade_date) >= CHINEXT_LIMIT_SWITCH else 0.1
    if code.startswith("688"):
        return 0.2
    if is_st:
        return 0.05
    return 0.1


def detect_limit_locks(
    symbol: str,
    trade_date: pd.Timestamp,
    prev_close: float,
    row: pd.Series,
    is_st: bool = False,
) -> tuple[bool, bool]:
    if pd.isna(prev_close):
        return False, False
    prices = [row.get("open"), row.get("high"), row.get("low"), row.get("close")]
    if any(pd.isna(price) for price in prices):
        return False, False

    ratio = get_price_limit_ratio(symbol, trade_date, is_st=is_st)
    up_limit = round_limit_price(prev_close * (1.0 + ratio))
    down_limit = round_limit_price(prev_close * (1.0 - ratio))
    up_locked = all(abs(float(price) - up_limit) <= LIMIT_PRICE_EPS for price in prices)
    down_locked = all(abs(float(price) - down_limit) <= LIMIT_PRICE_EPS for price in prices)
    return up_locked, down_locked


def detect_close_limit_blocks(
    symbol: str,
    trade_date: pd.Timestamp,
    prev_close: float,
    close_price: float,
    is_st: bool = False,
) -> tuple[bool, bool]:
    if pd.isna(prev_close) or pd.isna(close_price):
        return False, False
    ratio = get_price_limit_ratio(symbol, trade_date, is_st=is_st)
    up_limit = round_limit_price(prev_close * (1.0 + ratio))
    down_limit = round_limit_price(prev_close * (1.0 - ratio))
    up_blocked = abs(float(close_price) - up_limit) <= LIMIT_PRICE_EPS
    down_blocked = abs(float(close_price) - down_limit) <= LIMIT_PRICE_EPS
    return up_blocked, down_blocked


def build_tradeability_series(
    symbol: str,
    price: pd.DataFrame,
    trading_dates: pd.DatetimeIndex,
    return_price: pd.DataFrame | None = None,
    st_series: pd.Series | None = None,
    trade_constraint_mode: str = TRADE_CONSTRAINT_MODE_NEXT_OPEN,
) -> tuple[pd.Series, pd.Series, pd.Series, pd.Series]:
    if trade_constraint_mode not in {TRADE_CONSTRAINT_MODE_NEXT_OPEN, TRADE_CONSTRAINT_MODE_CLOSE}:
        raise ValueError(f"unsupported trade_constraint_mode: {trade_constraint_mode}")

    close_series = price.set_index("date")["close_raw"].sort_index()
    if close_series.empty:
        empty_float = pd.Series(np.nan, index=trading_dates, dtype=float)
        empty_bool = pd.Series(False, index=trading_dates, dtype=bool)
        return empty_float, empty_bool, empty_bool.copy(), empty_bool.copy()

    listed_mask = pd.Series(
        (trading_dates >= close_series.index.min()) & (trading_dates <= close_series.index.max()),
        index=trading_dates,
        dtype=bool,
    )
    actual_price_dates = pd.DatetimeIndex(close_series.index)
    close_calendar = close_series.reindex(trading_dates)
    close_calendar = close_calendar.where(listed_mask)
    close_calendar = close_calendar.ffill().where(listed_mask)
    return_close_series = close_series
    if return_price is not None and not return_price.empty:
        return_col = next((col for col in ["close_qfq", "close_adj", "close_raw"] if col in return_price.columns), None)
        if return_col is not None:
            return_close_series = return_price.set_index("date")[return_col].sort_index()
    return_close_calendar = return_close_series.reindex(trading_dates)
    return_close_calendar = return_close_calendar.where(listed_mask)
    return_close_calendar = return_close_calendar.ffill().where(listed_mask)
    ret_series = return_close_calendar.pct_change(fill_method=None).astype(float)

    ohlc = read_ohlc_cache(symbol, pd.Timestamp(trading_dates.min()), pd.Timestamp(trading_dates.max()))
    ohlc_lookup = ohlc.set_index("date").reindex(trading_dates) if not ohlc.empty else pd.DataFrame(index=trading_dates)

    tradeable = pd.Series(trading_dates.isin(actual_price_dates), index=trading_dates, dtype=bool) & listed_mask
    buyable = tradeable.copy()
    sellable = tradeable.copy()
    if trade_constraint_mode == TRADE_CONSTRAINT_MODE_CLOSE:
        prev_close = close_calendar.shift(1)
        for dt in trading_dates[tradeable.to_numpy()]:
            is_st = False if st_series is None else bool(st_series.reindex(trading_dates).fillna(False).loc[dt])
            up_blocked, down_blocked = detect_close_limit_blocks(
                symbol=symbol,
                trade_date=pd.Timestamp(dt),
                prev_close=prev_close.loc[dt],
                close_price=close_calendar.loc[dt],
                is_st=is_st,
            )
            if up_blocked:
                buyable.loc[dt] = False
            if down_blocked:
                sellable.loc[dt] = False
    elif not ohlc_lookup.empty:
        prev_close = close_calendar.shift(1)
        for dt in trading_dates[tradeable.to_numpy()]:
            if pd.isna(ohlc_lookup.loc[dt].get("open")):
                continue
            is_st = False if st_series is None else bool(st_series.reindex(trading_dates).fillna(False).loc[dt])
            up_locked, down_locked = detect_limit_locks(
                symbol,
                pd.Timestamp(dt),
                prev_close.loc[dt],
                ohlc_lookup.loc[dt],
                is_st=is_st,
            )
            if up_locked:
                buyable.loc[dt] = False
            if down_locked:
                sellable.loc[dt] = False
    return ret_series, tradeable, buyable, sellable


def build_all_rebalance_dates(trading_dates: pd.DatetimeIndex) -> dict[str, pd.DatetimeIndex]:
    out = {}
    for label, mode in SCHEDULES.items():
        out[label] = index_mod.build_rebalance_dates(
            trading_dates=trading_dates,
            switch_date=START_DATE,
            pre_switch_schedule=mode,
            post_switch_schedule=mode,
        )
    return out


def load_symbol_cache(
    symbol: str,
    trading_dates: pd.DatetimeIndex,
    cap_dates: pd.DatetimeIndex,
    trade_constraint_mode: str = TRADE_CONSTRAINT_MODE_NEXT_OPEN,
) -> tuple[str, pd.Series, pd.Series, pd.Series, pd.Series] | None:
    try:
        price_path = resolve_cache_path(PRICE_DIR, SHARED_PRICE_DIR, symbol)
        share_path = resolve_cache_path(SHARE_DIR, SHARED_SHARE_DIR, symbol)
        if price_path is None or share_path is None:
            return None

        start_ts = pd.Timestamp(trading_dates.min())
        end_ts = pd.Timestamp(trading_dates.max())

        price = pd.read_csv(price_path)
        price["date"] = pd.to_datetime(price["date"])
        price = price[(price["date"] >= start_ts) & (price["date"] <= end_ts)]
        if price.empty:
            return None
        price = price.sort_values("date")
        price["close_raw"] = pd.to_numeric(price["close_raw"], errors="coerce")
        price = price.dropna(subset=["date", "close_raw"])

        adjusted_price: pd.DataFrame | None = None
        adjusted_path = resolve_cache_path(ADJ_PRICE_DIR, SHARED_ADJ_PRICE_DIR, symbol)
        if adjusted_path is not None:
            adjusted_price = pd.read_csv(adjusted_path)
            adjusted_price["date"] = pd.to_datetime(adjusted_price["date"])
            adjusted_price = adjusted_price[(adjusted_price["date"] >= start_ts) & (adjusted_price["date"] <= end_ts)]
            if not adjusted_price.empty:
                return_col = next(
                    (col for col in ["close_qfq", "close_adj", "close_raw"] if col in adjusted_price.columns),
                    None,
                )
                if return_col is not None:
                    adjusted_price[return_col] = pd.to_numeric(adjusted_price[return_col], errors="coerce")
                    adjusted_price = adjusted_price.dropna(subset=["date", return_col]).sort_values("date")
                else:
                    adjusted_price = None

        shares = pd.read_csv(share_path)
        shares["change_date"] = pd.to_datetime(shares["change_date"])
        shares["total_shares_10k"] = pd.to_numeric(shares["total_shares_10k"], errors="coerce")
        shares = shares.dropna(subset=["total_shares_10k"]).sort_values("change_date")
        if shares.empty:
            return None

        meta = load_security_meta(symbol)
        st_series = build_st_status_series(meta, trading_dates)
        ret_series, tradeable_series, buyable_series, sellable_series = build_tradeability_series(
            symbol=symbol,
            price=price[["date", "close_raw"]].copy(),
            trading_dates=trading_dates,
            return_price=None if adjusted_price is None or adjusted_price.empty else adjusted_price,
            st_series=st_series,
            trade_constraint_mode=trade_constraint_mode,
        )

        cap_lookup = pd.merge_asof(
            pd.DataFrame({"date": cap_dates}),
            price[["date", "close_raw"]].sort_values("date"),
            on="date",
            direction="backward",
        )
        cap_lookup = pd.merge_asof(
            cap_lookup.sort_values("date"),
            shares[["change_date", "total_shares_10k"]].sort_values("change_date"),
            left_on="date",
            right_on="change_date",
            direction="backward",
        )
        cap_lookup["market_cap"] = cap_lookup["close_raw"] * cap_lookup["total_shares_10k"] * 10000.0
        cap_series = cap_lookup.set_index("date")["market_cap"].astype(float)
        cap_tradeable = tradeable_series.reindex(cap_dates).fillna(False).astype(bool)
        cap_series = cap_series.where(cap_tradeable, np.nan)
        cap_st = st_series.reindex(cap_dates).fillna(False).astype(bool)
        cap_series = cap_series.where(~cap_st, np.nan)

        return symbol, ret_series, cap_series, buyable_series, sellable_series
    except Exception:
        return None


def load_cache_panels(
    symbols: list[str],
    trading_dates: pd.DatetimeIndex,
    cap_dates: pd.DatetimeIndex,
    max_workers: int = 8,
    trade_constraint_mode: str = TRADE_CONSTRAINT_MODE_NEXT_OPEN,
) -> tuple[pd.DataFrame, dict[pd.Timestamp, dict[str, float]], pd.DataFrame, pd.DataFrame]:
    returns_df = pd.DataFrame(index=trading_dates)
    buyable_df = pd.DataFrame(index=trading_dates)
    sellable_df = pd.DataFrame(index=trading_dates)
    caps_by_date: dict[pd.Timestamp, dict[str, float]] = {pd.Timestamp(dt): {} for dt in cap_dates}
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {
            pool.submit(load_symbol_cache, symbol, trading_dates, cap_dates, trade_constraint_mode): symbol
            for symbol in symbols
        }
        for fut in as_completed(futures):
            result = fut.result()
            if result is None:
                continue
            symbol, ret_series, cap_series, buyable_series, sellable_series = result
            returns_df[symbol] = ret_series
            buyable_df[symbol] = buyable_series
            sellable_df[symbol] = sellable_series
            for dt, value in cap_series.items():
                if pd.notna(value):
                    caps_by_date[pd.Timestamp(dt)][symbol] = float(value)
    return returns_df, caps_by_date, buyable_df, sellable_df


def build_target_members_map(
    caps_by_date: dict[pd.Timestamp, dict[str, float]],
    rebalance_dates: pd.DatetimeIndex,
    top_n: int = TOP_N,
) -> dict[pd.Timestamp, list[str]]:
    target_members_map: dict[pd.Timestamp, list[str]] = {}
    for dt in rebalance_dates:
        cap_map = caps_by_date.get(pd.Timestamp(dt), {})
        ranked = sorted(cap_map.items(), key=lambda x: x[1])
        target_members_map[pd.Timestamp(dt)] = [symbol for symbol, _ in ranked[:top_n]]
    return target_members_map


def build_target_members_frame(
    target_members_map: dict[pd.Timestamp, list[str]],
    caps_by_date: dict[pd.Timestamp, dict[str, float]],
    name_map: dict[str, str] | None = None,
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    name_map = name_map or {}
    for dt in sorted(target_members_map):
        cap_map = caps_by_date.get(pd.Timestamp(dt), {})
        for rank, symbol in enumerate(target_members_map[pd.Timestamp(dt)], start=1):
            rows.append(
                {
                    "rebalance_date": pd.Timestamp(dt),
                    "rank": rank,
                    "symbol": symbol,
                    "name": name_map.get(symbol.zfill(6), ""),
                    "market_cap": float(cap_map.get(symbol, np.nan)),
                }
            )
    return pd.DataFrame(rows)


def can_trade_on_day(tradeability_df: pd.DataFrame, trade_date: pd.Timestamp, symbol: str) -> bool:
    if symbol not in tradeability_df.columns or trade_date not in tradeability_df.index:
        return False
    value = tradeability_df.at[trade_date, symbol]
    return bool(pd.notna(value) and value)


def apply_trade_constraints(
    current_members: list[str],
    target_members: list[str],
    trade_date: pd.Timestamp,
    buyable_df: pd.DataFrame,
    sellable_df: pd.DataFrame,
    top_n: int = TOP_N,
) -> dict[str, list[str]]:
    current_set = set(current_members)
    target_set = set(target_members)

    holdovers = [symbol for symbol in target_members if symbol in current_set]
    exited = []
    blocked_exits = []
    for symbol in current_members:
        if symbol in target_set:
            continue
        if can_trade_on_day(sellable_df, trade_date, symbol):
            exited.append(symbol)
        else:
            blocked_exits.append(symbol)

    buy_candidates = [symbol for symbol in target_members if symbol not in current_set]
    available_slots = max(top_n - len(holdovers) - len(blocked_exits), 0)
    entered = []
    blocked_entries = []
    for symbol in buy_candidates:
        if len(entered) >= available_slots:
            blocked_entries.append(symbol)
            continue
        if can_trade_on_day(buyable_df, trade_date, symbol):
            entered.append(symbol)
        else:
            blocked_entries.append(symbol)

    members_after = holdovers + entered + blocked_exits
    return {
        "members_after": members_after,
        "entered": entered,
        "exited": exited,
        "blocked_entries": blocked_entries,
        "blocked_exits": blocked_exits,
    }


def simulate_rebalance_path(
    trading_dates: pd.DatetimeIndex,
    returns_df: pd.DataFrame,
    target_members_map: dict[pd.Timestamp, list[str]],
    rebalance_dates: pd.DatetimeIndex,
    buyable_df: pd.DataFrame,
    sellable_df: pd.DataFrame,
    one_side_cost_rate: float,
    top_n: int = TOP_N,
    execution_timing: str = EXECUTION_TIMING_NEXT_OPEN,
) -> tuple[pd.DataFrame, pd.DataFrame, dict[pd.Timestamp, list[str]]]:
    if execution_timing not in {EXECUTION_TIMING_NEXT_OPEN, EXECUTION_TIMING_CLOSE}:
        raise ValueError(f"unsupported execution_timing: {execution_timing}")

    rebalance_set = set(rebalance_dates)
    turnover_rows: list[dict[str, object]] = []
    effective_members_map: dict[pd.Timestamp, list[str]] = {}

    index_rows: list[dict[str, object]] = []
    current_members: list[str] = []
    current_level = 1000.0
    for i, dt in enumerate(trading_dates):
        if i == 0:
            index_rows.append({"date": dt, "close": current_level, "daily_return": np.nan, "holding_count": 0})
            continue

        prev_dt = pd.Timestamp(trading_dates[i - 1])
        if prev_dt in rebalance_set:
            constraint_trade_date = prev_dt if execution_timing == EXECUTION_TIMING_CLOSE else pd.Timestamp(dt)
            execution_date = prev_dt if execution_timing == EXECUTION_TIMING_CLOSE else pd.Timestamp(dt)
            target_members = target_members_map.get(prev_dt, [])
            trade_result = apply_trade_constraints(
                current_members=current_members,
                target_members=target_members,
                trade_date=constraint_trade_date,
                buyable_df=buyable_df,
                sellable_df=sellable_df,
                top_n=top_n,
            )
            current_members = trade_result["members_after"]
            effective_members_map[prev_dt] = current_members.copy()
            buys = len(trade_result["entered"])
            sells = len(trade_result["exited"])
            turnover_rows.append(
                {
                    "rebalance_date": prev_dt,
                    "execution_timing": execution_timing,
                    "constraint_trade_date": constraint_trade_date,
                    "execution_date": execution_date,
                    "effective_date": execution_date,
                    "return_start_date": pd.Timestamp(dt),
                    "exit_count": sells,
                    "entry_count": buys,
                    "blocked_entry_count": len(trade_result["blocked_entries"]),
                    "blocked_exit_count": len(trade_result["blocked_exits"]),
                    "buy_turnover_frac": buys / top_n,
                    "sell_turnover_frac": sells / top_n,
                    "turnover_frac_one_side": (buys + sells) / (2 * top_n),
                    "two_side_cost_rate": one_side_cost_rate * ((buys + sells) / top_n),
                    "holding_count_after": len(current_members),
                }
            )

        if current_members:
            day_ret = returns_df.loc[dt, current_members].dropna()
            portfolio_ret = float(day_ret.mean()) if len(day_ret) else 0.0
        else:
            portfolio_ret = 0.0
        current_level *= 1.0 + portfolio_ret
        index_rows.append(
            {
                "date": dt,
                "close": current_level,
                "daily_return": portfolio_ret,
                "holding_count": len(current_members),
            }
        )

    return pd.DataFrame(index_rows), pd.DataFrame(turnover_rows), effective_members_map


def build_index_and_turnover(
    trading_dates: pd.DatetimeIndex,
    returns_df: pd.DataFrame,
    caps_by_date: dict[pd.Timestamp, dict[str, float]],
    buyable_df: pd.DataFrame,
    sellable_df: pd.DataFrame,
    rebalance_dates: pd.DatetimeIndex,
    execution_timing: str = EXECUTION_TIMING_NEXT_OPEN,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    target_members_map = build_target_members_map(caps_by_date, rebalance_dates, top_n=TOP_N)
    index_df, turnover_df, _ = simulate_rebalance_path(
        trading_dates=trading_dates,
        returns_df=returns_df,
        target_members_map=target_members_map,
        rebalance_dates=rebalance_dates,
        buyable_df=buyable_df,
        sellable_df=sellable_df,
        one_side_cost_rate=cost_mod.MONTHLY_REBALANCE_ONE_SIDE,
        top_n=TOP_N,
        execution_timing=execution_timing,
    )
    return index_df, turnover_df


def run_strategy(index_df: pd.DataFrame, turnover_df: pd.DataFrame) -> pd.DataFrame:
    panel = pd.read_csv(hedge_mod.DEFAULT_PANEL, usecols=["date", hedge_mod.DEFAULT_HEDGE_COLUMN])
    panel["date"] = pd.to_datetime(panel["date"])
    panel = panel[(panel["date"] >= pd.Timestamp(START_DATE)) & (panel["date"] <= pd.Timestamp(END_DATE))]
    hedge = panel.set_index("date")[hedge_mod.DEFAULT_HEDGE_COLUMN].rename("hedge").astype(float)
    microcap = index_df.set_index("date")["close"].rename("microcap").astype(float)
    close_df = pd.concat([microcap, hedge], axis=1).dropna()

    gross = hedge_mod.run_backtest(
        close_df=close_df,
        signal_model="momentum",
        lookback=LOOKBACK,
        bias_n=hedge_mod.DEFAULT_BIAS_N,
        bias_mom_day=hedge_mod.DEFAULT_BIAS_MOM_DAY,
        futures_drag=hedge_mod.DEFAULT_FUTURES_DRAG,
        require_positive_microcap_mom=False,
        r2_window=hedge_mod.DEFAULT_R2_WINDOW,
        r2_threshold=0.0,
        vol_scale_enabled=False,
        target_vol=hedge_mod.DEFAULT_TARGET_VOL,
        vol_window=hedge_mod.DEFAULT_VOL_WINDOW,
        max_lev=hedge_mod.DEFAULT_MAX_LEV,
        min_lev=hedge_mod.DEFAULT_MIN_LEV,
        scale_threshold=hedge_mod.DEFAULT_SCALE_THRESHOLD,
    )
    turnover_df = turnover_df.copy()
    turnover_df["rebalance_date"] = pd.to_datetime(turnover_df["rebalance_date"])
    net = cost_mod.apply_cost_model(gross, turnover_df)
    return net


def summarize(label: str, net: pd.DataFrame, turnover_df: pd.DataFrame) -> dict[str, object]:
    metrics = hedge_mod.calc_metrics(net["return_net"])
    recent_rows = []
    last_date = net.index[-1]
    for yrs in [1, 2, 3, 4, 5]:
        part = net.loc[net.index >= last_date - pd.DateOffset(years=yrs), "return_net"]
        if len(part) > 30:
            m = hedge_mod.calc_metrics(part)
            recent_rows.append(
                {
                    "window_years": yrs,
                    "annual": m.annual,
                    "max_dd": m.max_dd,
                    "sharpe": m.sharpe,
                }
            )
    return {
        "schedule": label,
        "net_annual": metrics.annual,
        "net_max_dd": metrics.max_dd,
        "net_sharpe": metrics.sharpe,
        "net_vol": metrics.vol,
        "net_total_return": metrics.total_return,
        "entry_exit_cost_sum": float(net["entry_exit_cost"].sum()),
        "rebalance_cost_sum": float(net["rebalance_cost"].sum()),
        "total_cost_sum": float(net["total_cost"].sum()),
        "entry_days": int(net["entry_exit_cost"].gt(0).sum()),
        "rebalance_cost_days": int(net["rebalance_cost"].gt(0).sum()),
        "avg_monthly_equiv_turnover_frac": float(turnover_df["turnover_frac_one_side"].mean()) if len(turnover_df) else 0.0,
        "rebalance_events": int(len(turnover_df)),
        "recent_windows": recent_rows,
    }


def main() -> None:
    trading_dates = load_trading_dates()
    rebalance_map = build_all_rebalance_dates(trading_dates)
    all_cap_dates = pd.DatetimeIndex(sorted(set().union(*[set(v) for v in rebalance_map.values()])))
    symbols = load_universe()
    returns_df, caps_by_date, buyable_df, sellable_df = load_cache_panels(
        symbols,
        trading_dates,
        all_cap_dates,
        max_workers=8,
    )

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    summary_rows: list[dict[str, object]] = []
    recent_rows: list[dict[str, object]] = []
    for label, rebalance_dates in rebalance_map.items():
        index_df, turnover_df = build_index_and_turnover(
            trading_dates,
            returns_df,
            caps_by_date,
            buyable_df,
            sellable_df,
            rebalance_dates,
        )
        net = run_strategy(index_df, turnover_df)
        index_df.to_csv(OUTPUT_DIR / f"wind_microcap_top_100_{label}_16y_cached.csv", index=False, encoding="utf-8")
        turnover_df.to_csv(OUTPUT_DIR / f"microcap_top100_{label}_turnover_stats.csv", index=False, encoding="utf-8")
        net.to_csv(OUTPUT_DIR / f"microcap_top100_mom16_hedge_zz1000_{label}_16y_costed_nav.csv", index_label="date", encoding="utf-8")
        summary = summarize(label, net, turnover_df)
        summary_rows.append({k: v for k, v in summary.items() if k != "recent_windows"})
        for row in summary["recent_windows"]:
            recent_rows.append(
                {
                    "schedule": label,
                    "window_years": row["window_years"],
                    "annual": row["annual"],
                    "max_dd": row["max_dd"],
                    "sharpe": row["sharpe"],
                }
            )

    summary_df = pd.DataFrame(summary_rows).sort_values("net_sharpe", ascending=False)
    recent_df = pd.DataFrame(recent_rows).sort_values(["window_years", "sharpe"], ascending=[True, False])
    summary_df.to_csv(OUTPUT_DIR / "microcap_top100_rebalance_frequency_compare.csv", index=False, encoding="utf-8")
    recent_df.to_csv(OUTPUT_DIR / "microcap_top100_rebalance_frequency_recent_windows.csv", index=False, encoding="utf-8")

    payload = {
        "strategy": "top100_mom16_hedge_zz1000_rebalance_frequency_compare",
        "lookback": LOOKBACK,
        "schedules": SCHEDULES,
        "summary": summary_df.to_dict(orient="records"),
        "recent_windows": recent_df.to_dict(orient="records"),
    }
    (OUTPUT_DIR / "microcap_top100_rebalance_frequency_compare.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    print(summary_df.to_string(index=False))
    print("saved microcap_top100_rebalance_frequency_compare.csv")
    print("saved microcap_top100_rebalance_frequency_recent_windows.csv")


if __name__ == "__main__":
    main()
