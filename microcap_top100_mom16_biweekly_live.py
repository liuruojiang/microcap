from __future__ import annotations

import argparse
from contextlib import contextmanager
import importlib
import json
import os
import re
import sys
import time
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from types import SimpleNamespace

import microcap_runtime_bootstrap as runtime_bootstrap


ROOT = Path(__file__).resolve().parent
OUTPUT_DIR = ROOT / "outputs"
CACHE_DIR = ROOT / ".microcap_index_cache"
REALTIME_DIR = CACHE_DIR / "realtime"
QVERIS_API_BASE = "https://qveris.ai/api/v1"
QVERIS_REALTIME_TOOL_ID = "cn_financial_pro.real_time_quotation.v1"
QVERIS_INDEX_HISTORY_TOOL_QUERY = "A-share index daily historical close price by security code"
QVERIS_STOCK_PRICE_HISTORY_TOOL_QUERY = "A-share stock daily historical raw close price by stock code"
QVERIS_DISABLED_MESSAGE = "QVeris is disabled for future runs; use free sources or local cache."

TOP_N = 100
LOOKBACK = 16
REBALANCE_WEEKDAY = "Thursday"
CN_TIMEZONE = "Asia/Shanghai"
REBALANCE_ANCHOR_DATE = "2016-01-07"
CN_CLOSE_CONFIRM_TIME = "20:00"
DEFAULT_PANEL_PATH = ROOT / "mnt_strategy_data_cn.csv"
HEDGE_COLUMN = "1.000852"
FIXED_HEDGE_RATIO = 1.0
FUTURES_DRAG = 3.0 / 10000.0
REQUIRE_POSITIVE_MICROCAP_MOM = False
MOMENTUM_GAP_EXIT_BUFFER = 0.0
TAIL_JITTER_WARNING_GAP = 0.001
TAIL_JITTER_CAUTION_GAP = 0.002
DEFAULT_MAX_STALE_ANCHOR_DAYS = 5
HEDGE_HISTORY_LOOKBACK_BUFFER_DAYS = 40
EXECUTION_TIMING = "close"
TRADE_CONSTRAINT_MODE = "close"
RESEARCH_STACK_VERSION = "2026-05-12-p0-p1-history-meta-master-stv3"
COMPATIBLE_PROXY_RESEARCH_STACK_VERSIONS = {
    RESEARCH_STACK_VERSION,
    "2026-04-11-p0-p1-history-meta-master-stv2",
}
STATIC_CONTEXT_CACHE_VERSION = "2026-05-12-live-current-st-members-v2"
MEMBER_FILTER_POLICY_VERSION = "empty-name-reject-v1"
REALTIME_QUOTE_POLICY_VERSION = "strict-per-symbol-date-v1"
PROXY_REBALANCE_POLICY_VERSION = "fixed-biweekly-anchor-20160107-v1"
REALTIME_QUOTE_FETCH_ATTEMPTS = 3
REALTIME_QUOTE_RETRY_SECONDS = 5
REALTIME_CLOSE_REFRESH_MAX_WORKERS = 8
REALTIME_LAST_CLOSE_FLAT_FALLBACK_MIN_QUOTED_FRACTION = 0.95
REALTIME_CACHE_LOCK_TIMEOUT_SECONDS = 30
REALTIME_CACHE_STALE_LOCK_SECONDS = 300
TOP100_REALTIME_REQUIRE_STATE_ENV = "TOP100_REALTIME_REQUIRE_STATE"
ALLOWED_ACTIONABLE_HEDGE_QUOTE_SOURCES = {
    "eastmoney_stock_get",
    "tencent_batch_free",
}
_QVERIS_TOOL_ID_CACHE: dict[str, str] = {}

DEFAULT_INDEX_CSV = OUTPUT_DIR / "wind_microcap_top_100_biweekly_thursday_16y_cached.csv"
DEFAULT_OUTPUT_PREFIX = "microcap_top100_mom16_biweekly_live"
DEFAULT_COSTED_NAV_CSV = OUTPUT_DIR / "microcap_top100_mom16_hedge_zz1000_biweekly_thursday_16y_costed_nav.csv"
DEFAULT_FORCED_STOP_LOSS_SCAN_THRESHOLDS = (0.02, 0.03, 0.04, 0.05)
UNIVERSE_LABEL = "Top100"
STRATEGY_TITLE = "Top100 Microcap Mom16 Biweekly"
INDEX_CODE = "TOP100_BIWEEKLY_THURSDAY_PROXY"
WEEK_FREQ_BY_START = {
    "Monday": "W-SUN",
    "Tuesday": "W-MON",
    "Wednesday": "W-TUE",
    "Thursday": "W-WED",
    "Friday": "W-THU",
}

ak = None
np = None
pd = None
plt = None
requests = None
hedge_mod = None
freq_mod = None
fetch_mod = None
PerformanceWarning = None
_RUNTIME_MODULES_READY = False

CN_NUM = {
    "一": 1,
    "二": 2,
    "两": 2,
    "三": 3,
    "四": 4,
    "五": 5,
    "六": 6,
    "七": 7,
    "八": 8,
    "九": 9,
    "十": 10,
    "半": 0.5,
}
DAY_SUFFIX = r"[日号]?"
PERFORMANCE_PATTERN = re.compile(r"表现|收益(?!曲线)|回撤|年化|夏普|回报|净值曲线")
NON_TRADABLE_NAME_PATTERN = re.compile(r"(退$|退市|摘牌)")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            f"{UNIVERSE_LABEL} microcap practical script. Fixed params: exclude current ST, {UNIVERSE_LABEL} "
            "smallest-cap A-shares, biweekly Thursday-signal rebalance, 16-day relative "
            "momentum versus CSI 1000. Supports both batch export and query commands."
        )
    )
    parser.add_argument("query_tokens", nargs="*", help="可选查询，例如：信号 / 实时信号 / 成分股 / 进出名单 / 表现 2024至今")
    parser.add_argument("--panel-path", type=Path, default=DEFAULT_PANEL_PATH)
    parser.add_argument("--index-csv", type=Path, default=DEFAULT_INDEX_CSV)
    parser.add_argument("--costed-nav-csv", type=Path, default=DEFAULT_COSTED_NAV_CSV)
    parser.add_argument("--output-prefix", default=DEFAULT_OUTPUT_PREFIX)
    parser.add_argument("--capital", type=float, default=None, help="Optional gross stock capital used for per-stock target notional.")
    parser.add_argument("--max-workers", type=int, default=8)
    parser.add_argument(
        "--bootstrap-deps",
        action="store_true",
        help="Install missing runtime dependencies from an offline wheelhouse before running.",
    )
    parser.add_argument(
        "--wheelhouse",
        type=Path,
        default=None,
        help=(
            "Offline wheel directory for --bootstrap-deps. "
            "If omitted, auto-detect MICROCAP_WHEELHOUSE, ./wheelhouse, ./.vendor_libs/wheelhouse, or ./.vendor_libs."
        ),
    )
    parser.add_argument(
        "--realtime-cache-seconds",
        type=int,
        default=30,
        help="Only reuse realtime results within this many seconds. Default is 30s for same-decision-window sharing.",
    )
    parser.add_argument(
        "--rebuild-index-if-missing",
        action=argparse.BooleanOptionalAction,
        default=True,
        help=f"If the cached {UNIVERSE_LABEL} biweekly proxy is missing, rebuild it from local/public cache.",
    )
    parser.add_argument(
        "--force-refresh",
        action="store_true",
        help="Only used when rebuilding the proxy. Refresh AKShare cache before rebuilding.",
    )
    parser.add_argument(
        "--max-stale-anchor-days",
        type=int,
        default=DEFAULT_MAX_STALE_ANCHOR_DAYS,
        help=(
            "历史收盘锚点允许的最大自然日滞后。"
            "超过这个阈值时，默认拒绝输出实时查询结果。"
        ),
    )
    parser.add_argument(
        "--allow-stale-realtime",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Allow realtime queries even when the historical anchor is stale. Use with caution.",
    )
    return parser.parse_args()


def _load_runtime_modules() -> None:
    global ak, np, pd, plt, requests
    global hedge_mod, freq_mod, fetch_mod, PerformanceWarning, _RUNTIME_MODULES_READY

    if _RUNTIME_MODULES_READY:
        return

    np = importlib.import_module("numpy")
    pd = importlib.import_module("pandas")
    requests = importlib.import_module("requests")
    ak = importlib.import_module("akshare")
    matplotlib = importlib.import_module("matplotlib")
    matplotlib.use("Agg")
    plt = importlib.import_module("matplotlib.pyplot")
    PerformanceWarning = importlib.import_module("pandas.errors").PerformanceWarning
    hedge_mod = importlib.import_module("analyze_microcap_zz1000_hedge")
    freq_mod = importlib.import_module("analyze_top100_rebalance_frequency")
    fetch_mod = importlib.import_module("fetch_wind_microcap_index")
    warnings.filterwarnings("ignore", category=PerformanceWarning)
    _RUNTIME_MODULES_READY = True


def _ensure_core_deps_or_exit(args: argparse.Namespace) -> None:
    missing = runtime_bootstrap.find_missing_modules()
    if not missing:
        return

    if not args.bootstrap_deps:
        print(runtime_bootstrap.format_missing_dependencies_message(missing, bootstrap_requested=False), file=sys.stderr)
        raise SystemExit(2)

    wheelhouse = runtime_bootstrap.resolve_wheelhouse(ROOT, args.wheelhouse)
    if wheelhouse is None:
        print(runtime_bootstrap.format_missing_dependencies_message(missing, bootstrap_requested=True), file=sys.stderr)
        raise SystemExit(2)

    result = runtime_bootstrap.bootstrap_from_wheelhouse(wheelhouse)
    if result.returncode != 0:
        print(runtime_bootstrap.format_bootstrap_failure_message(wheelhouse, result), file=sys.stderr)
        raise SystemExit(2)

    remaining = runtime_bootstrap.find_missing_modules()
    if remaining:
        print(runtime_bootstrap.format_missing_dependencies_message(remaining, bootstrap_requested=True), file=sys.stderr)
        raise SystemExit(2)


if not runtime_bootstrap.find_missing_modules():
    _load_runtime_modules()


def is_tradable_name(name: str) -> bool:
    text = str(name or "").strip()
    if not text:
        return False
    return NON_TRADABLE_NAME_PATTERN.search(text) is None


def build_output_paths(output_prefix: str) -> dict[str, Path]:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    return {
        "summary": OUTPUT_DIR / f"{output_prefix}_summary.json",
        "signal": OUTPUT_DIR / f"{output_prefix}_latest_signal.csv",
        "members": OUTPUT_DIR / f"{output_prefix}_target_members.csv",
        "changes": OUTPUT_DIR / f"{output_prefix}_rebalance_changes.csv",
        "nav": OUTPUT_DIR / f"{output_prefix}_nav.csv",
        "proxy_meta": OUTPUT_DIR / f"{output_prefix}_proxy_meta.json",
        "proxy_members": OUTPUT_DIR / f"{output_prefix}_proxy_members.csv",
        "proxy_turnover": OUTPUT_DIR / f"{output_prefix}_proxy_turnover.csv",
        "realtime_signal": OUTPUT_DIR / f"{output_prefix}_realtime_signal.csv",
        "realtime_members": OUTPUT_DIR / f"{output_prefix}_realtime_target_members.csv",
        "realtime_changes": OUTPUT_DIR / f"{output_prefix}_realtime_rebalance_changes.csv",
        "performance_summary": OUTPUT_DIR / f"{output_prefix}_performance_summary.csv",
        "performance_yearly": OUTPUT_DIR / f"{output_prefix}_performance_yearly.csv",
        "performance_nav": OUTPUT_DIR / f"{output_prefix}_performance_nav.csv",
        "performance_chart": OUTPUT_DIR / f"{output_prefix}_performance_curve.png",
        "performance_json": OUTPUT_DIR / f"{output_prefix}_performance_summary.json",
        "forced_stop_scan": OUTPUT_DIR / f"{output_prefix}_forced_stop_scan.csv",
        "cache_static_meta": REALTIME_DIR / f"{output_prefix}_static_meta.json",
        "cache_static_target": REALTIME_DIR / f"{output_prefix}_static_target_members.csv",
        "cache_static_effective": REALTIME_DIR / f"{output_prefix}_static_effective_members.csv",
        "cache_static_changes": REALTIME_DIR / f"{output_prefix}_static_rebalance_changes.csv",
        "cache_realtime_meta": REALTIME_DIR / f"{output_prefix}_realtime_meta.json",
        "cache_realtime_members": REALTIME_DIR / f"{output_prefix}_realtime_cached_members.csv",
        "cache_realtime_changes": REALTIME_DIR / f"{output_prefix}_realtime_cached_changes.csv",
        "cache_realtime_signal": REALTIME_DIR / f"{output_prefix}_realtime_cached_signal.csv",
        "cache_fast_realtime_meta": REALTIME_DIR / f"{output_prefix}_realtime_fast_meta.json",
        "cache_fast_realtime_signal": REALTIME_DIR / f"{output_prefix}_realtime_fast_cached_signal.csv",
        "panel_shadow": OUTPUT_DIR / f"{output_prefix}_panel_refreshed.csv",
    }


def _cn_timestamp(now: pd.Timestamp | None = None) -> pd.Timestamp:
    ts = pd.Timestamp.now(tz=CN_TIMEZONE) if now is None else pd.Timestamp(now)
    if ts.tzinfo is None:
        return ts.tz_localize(CN_TIMEZONE)
    return ts.tz_convert(CN_TIMEZONE)


def _cn_local_day(now: pd.Timestamp | None = None) -> pd.Timestamp:
    return pd.Timestamp(_cn_timestamp(now).date())


def _to_jsonable(value: object) -> object:
    if np is not None:
        if isinstance(value, np.integer):
            return int(value)
        if isinstance(value, np.floating):
            return float(value) if np.isfinite(value) else None
        if isinstance(value, np.ndarray):
            return value.tolist()
    if pd is not None:
        if isinstance(value, pd.Timestamp):
            return value.isoformat()
        if value is pd.NaT:
            return None
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def _atomic_temp_path(path: Path) -> Path:
    return path.with_name(f"{path.name}.{os.getpid()}.{time.time_ns()}.tmp")


def _replace_with_retry(tmp: Path, path: Path, attempts: int = 5, delay_seconds: float = 0.05) -> None:
    last_exc: OSError | None = None
    for attempt in range(max(1, int(attempts))):
        try:
            tmp.replace(path)
            return
        except OSError as exc:
            last_exc = exc
            if attempt >= max(1, int(attempts)) - 1:
                break
            time.sleep(delay_seconds * (2**attempt))
    if last_exc is not None:
        raise last_exc


def _atomic_write_text(path: Path, text: str, encoding: str = "utf-8") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = _atomic_temp_path(path)
    try:
        tmp.write_text(text, encoding=encoding)
        _replace_with_retry(tmp, path)
    finally:
        try:
            if tmp.exists():
                tmp.unlink()
        except OSError:
            pass


def _atomic_write_json(path: Path, payload: object, encoding: str = "utf-8") -> None:
    text = json.dumps(payload, ensure_ascii=False, indent=2, default=_to_jsonable)
    _atomic_write_text(path, text, encoding=encoding)


def _atomic_to_csv(frame: pd.DataFrame, path: Path, **kwargs: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = _atomic_temp_path(path)
    try:
        frame.to_csv(tmp, **kwargs)
        _replace_with_retry(tmp, path)
    finally:
        try:
            if tmp.exists():
                tmp.unlink()
        except OSError:
            pass


@contextmanager
def _cache_write_lock(lock_path: Path):
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    start = time.time()
    fd: int | None = None
    while True:
        try:
            fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_RDWR)
            try:
                os.write(fd, f"pid={os.getpid()} ts={time.time()}\n".encode("ascii", errors="ignore"))
            except Exception:
                os.close(fd)
                fd = None
                try:
                    lock_path.unlink()
                except OSError:
                    pass
                raise
            break
        except FileExistsError:
            try:
                age = time.time() - lock_path.stat().st_mtime
                if age > REALTIME_CACHE_STALE_LOCK_SECONDS:
                    lock_path.unlink()
                    continue
            except OSError:
                continue
            if time.time() - start > REALTIME_CACHE_LOCK_TIMEOUT_SECONDS:
                raise TimeoutError(f"Timed out waiting for realtime cache lock: {lock_path}")
            time.sleep(0.1)
    try:
        yield
    finally:
        if fd is not None:
            os.close(fd)
        try:
            lock_path.unlink()
        except OSError:
            pass


def assess_history_anchor_freshness(
    latest_trade_date: pd.Timestamp,
    max_stale_days: int,
    now: pd.Timestamp | None = None,
    trading_dates: pd.DatetimeIndex | None = None,
) -> dict[str, object]:
    latest_trade_date = pd.Timestamp(latest_trade_date).normalize()
    current_date = _cn_local_day(now)
    stale_days = max(0, int((current_date - latest_trade_date).days))
    stale_trading_days: int | None = None
    staleness_unit = "calendar_days"
    stale_value = stale_days
    if trading_dates is not None and len(trading_dates):
        calendar = pd.DatetimeIndex(pd.to_datetime(trading_dates, errors="coerce")).dropna().normalize().unique().sort_values()
        calendar = calendar[calendar <= current_date]
        if len(calendar):
            stale_trading_days = int(((calendar > latest_trade_date) & (calendar <= calendar.max())).sum())
            stale_value = stale_trading_days
            staleness_unit = "trading_days"
    is_stale = stale_value > max(0, int(max_stale_days))
    return {
        "latest_trade_date": str(latest_trade_date.date()),
        "current_date": str(current_date.date()),
        "stale_calendar_days": stale_days,
        "stale_trading_days": stale_trading_days,
        "staleness_unit": staleness_unit,
        "max_stale_anchor_days": int(max_stale_days),
        "is_stale": bool(is_stale),
        "status": "stale" if is_stale else "fresh",
    }


def format_anchor_stale_message(anchor_freshness: dict[str, object]) -> str:
    latest_trade_date = anchor_freshness["latest_trade_date"]
    current_date = anchor_freshness["current_date"]
    stale_days = int(anchor_freshness["stale_calendar_days"])
    max_days = int(anchor_freshness["max_stale_anchor_days"])
    return (
        f"历史锚点已过期：最新锚定交易日为 {latest_trade_date}，"
        f"当前日期为 {current_date}，滞后 {stale_days} 个自然日"
        f"（上限 {max_days} 天）。由于把实时快照接到过期序列后会扭曲 16 日动量窗口，"
        "默认拒绝输出实时结果。请先刷新本地基线文件，或显式传入 --allow-stale-realtime 覆盖。"
    )


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


def fetch_eastmoney_index_history(
    secid: str,
    start_date: pd.Timestamp,
    end_date: pd.Timestamp | None = None,
) -> pd.DataFrame:
    end_ts = _cn_local_day() if end_date is None else pd.Timestamp(end_date)
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
    last_error: Exception | None = None
    klines: list[str] = []
    for attempt in range(3):
        try:
            response = requests.get(
                url,
                timeout=20,
                headers={"Referer": "https://quote.eastmoney.com/", "User-Agent": "Mozilla/5.0"},
            )
            response.raise_for_status()
            data = response.json().get("data") or {}
            klines = data.get("klines") or []
            if klines:
                break
            last_error = RuntimeError(f"Empty EastMoney index history for {secid}")
        except Exception as exc:
            last_error = exc
        time.sleep(1.5 * (attempt + 1))
    if klines:
        rows: list[dict[str, object]] = []
        for item in klines:
            parts = item.split(",")
            if len(parts) < 3:
                continue
            rows.append({"date": pd.to_datetime(parts[0]), "close": float(parts[2])})
        out = pd.DataFrame(rows).dropna().sort_values("date").drop_duplicates(subset="date")
        if not out.empty:
            return out.reset_index(drop=True)

    symbol = "sh" + secid.split(".")[-1]
    sina_url = (
        "https://money.finance.sina.com.cn/quotes_service/api/json_v2.php/CN_MarketData.getKLineData"
        f"?symbol={symbol}&scale=240&ma=no&datalen=6000"
    )
    try:
        response = requests.get(sina_url, timeout=20, headers={"User-Agent": "Mozilla/5.0"})
        response.raise_for_status()
        data = response.json()
        if not isinstance(data, list) or not data:
            raise RuntimeError(f"Failed index history fallback for {secid}: {last_error}")
        rows = []
        start_ts = pd.Timestamp(start_date)
        for item in data:
            day = item.get("day")
            close = item.get("close")
            if day is None or close is None:
                continue
            dt = pd.to_datetime(day)
            if dt < start_ts or dt > end_ts:
                continue
            rows.append({"date": dt, "close": float(close)})
        out = pd.DataFrame(rows).dropna().sort_values("date").drop_duplicates(subset="date")
        if not out.empty:
            return out.reset_index(drop=True)
        last_error = RuntimeError(f"Parsed empty index history fallback for {secid}: {last_error}")
    except Exception as exc:
        last_error = exc

    raise RuntimeError(
        f"Free index history sources returned empty data for {secid}; QVeris fallback is disabled: {last_error}"
    )


def latest_closed_history_date(history_df: pd.DataFrame, now: pd.Timestamp | None = None) -> pd.Timestamp:
    current_ts = _cn_timestamp(now)
    dates = pd.to_datetime(history_df["date"], errors="coerce").dropna().sort_values()
    if dates.empty:
        raise RuntimeError("No valid historical dates available.")
    current_day = pd.Timestamp(current_ts.date())
    hour, minute = (int(part) for part in CN_CLOSE_CONFIRM_TIME.split(":", 1))
    close_confirm_ts = current_ts.normalize() + pd.Timedelta(hours=hour, minutes=minute)
    if current_ts < close_confirm_ts:
        dates = dates[dates.dt.normalize() < current_day]
    if dates.empty:
        raise RuntimeError("No close-confirmed historical dates available.")
    return pd.Timestamp(dates.max()).normalize()


def build_refreshed_panel_shadow(args: argparse.Namespace, paths: dict[str, Path]) -> tuple[Path, pd.Timestamp]:
    existing_shadow_end = read_csv_last_date(paths["panel_shadow"])
    if panel_shadow_cache_is_reusable(paths["panel_shadow"], existing_shadow_end):
        return paths["panel_shadow"], pd.Timestamp(existing_shadow_end)

    panel = pd.read_csv(args.panel_path)
    if "date" not in panel.columns or HEDGE_COLUMN not in panel.columns:
        raise ValueError(f"Panel must contain columns 'date' and '{HEDGE_COLUMN}'")
    panel["date"] = pd.to_datetime(panel["date"])
    panel = panel.sort_values("date").drop_duplicates(subset="date", keep="last")
    latest_panel_date = pd.Timestamp(panel["date"].max())
    history_start = latest_panel_date - pd.Timedelta(days=HEDGE_HISTORY_LOOKBACK_BUFFER_DAYS)
    hedge_hist = fetch_eastmoney_index_history("1.000852", history_start)
    latest_hedge_date = latest_closed_history_date(hedge_hist)
    hedge_hist = hedge_hist.loc[pd.to_datetime(hedge_hist["date"], errors="coerce") <= latest_hedge_date].copy()

    shadow = panel.set_index("date")
    for row in hedge_hist.itertuples(index=False):
        dt = pd.Timestamp(row.date)
        close = float(row.close)
        if dt in shadow.index:
            shadow.at[dt, HEDGE_COLUMN] = close
        elif dt > latest_panel_date:
            shadow.loc[dt, :] = np.nan
            shadow.at[dt, HEDGE_COLUMN] = close

    shadow = shadow.sort_index().reset_index()
    _atomic_to_csv(shadow, paths["panel_shadow"], index=False, encoding="utf-8")
    return paths["panel_shadow"], latest_hedge_date


def panel_shadow_cache_is_reusable(
    panel_shadow: Path,
    existing_shadow_end: pd.Timestamp | None,
    now: pd.Timestamp | None = None,
    same_day_max_age_seconds: int = 600,
) -> bool:
    if existing_shadow_end is None:
        return False
    current_ts = _cn_timestamp(now)
    shadow_day = pd.Timestamp(existing_shadow_end).normalize()
    current_day = pd.Timestamp(current_ts.date())
    if shadow_day > current_day:
        raise RuntimeError(f"panel shadow cache has future date: {shadow_day.date()} > {current_day.date()}")
    if shadow_day < current_day:
        return False
    hour, minute = (int(part) for part in CN_CLOSE_CONFIRM_TIME.split(":", 1))
    close_confirm_ts = current_ts.normalize() + pd.Timedelta(hours=hour, minutes=minute)
    if current_ts < close_confirm_ts:
        return False
    try:
        mtime = pd.Timestamp.fromtimestamp(panel_shadow.stat().st_mtime, tz=CN_TIMEZONE)
    except OSError:
        return False
    return bool(current_ts - mtime <= pd.Timedelta(seconds=max(0, int(same_day_max_age_seconds))))


def refresh_price_cache_tail(
    end_date: pd.Timestamp,
    max_workers: int,
    symbols: list[str] | None = None,
    force_refresh: bool = False,
) -> None:
    if symbols is None:
        symbols = freq_mod.load_current_universe()
    if not symbols:
        raise RuntimeError("No cached-universe symbols available for price-cache refresh.")

    end_text = pd.Timestamp(end_date).strftime("%Y-%m-%d")
    failures: list[str] = []
    workers = max(1, min(int(max_workers), 16))

    def refresh_price_history_with_fallback(symbol: str) -> None:
        try:
            fetch_mod.fetch_price_history(symbol, freq_mod.START_DATE, end_text, force_refresh)
            return
        except Exception as free_exc:
            raise RuntimeError(
                f"free price refresh failed for {symbol}; QVeris fallback is disabled"
            ) from free_exc

    def refresh_symbol(symbol: str) -> None:
        refresh_price_history_with_fallback(symbol)
        fetch_share_change = getattr(fetch_mod, "fetch_share_change", None)
        if fetch_share_change is not None:
            fetch_share_change(symbol, freq_mod.START_DATE, end_text, force_refresh)

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(refresh_symbol, symbol): symbol for symbol in symbols}
        for fut in as_completed(futures):
            symbol = futures[fut]
            try:
                fut.result()
            except Exception:
                failures.append(symbol)
    if failures:
        audit = pd.DataFrame({"symbol": failures})
        audit_path = OUTPUT_DIR / f"price_cache_refresh_failures_{end_text}.csv"
        _atomic_to_csv(audit, audit_path, index=False, encoding="utf-8")
    if len(failures) > max(20, len(symbols) // 100):
        sample = ", ".join(failures[:10])
        raise RuntimeError(
            f"Too many price-cache refresh failures ({len(failures)}/{len(symbols)}). Sample: {sample}"
        )


def _load_cached_market_cap_asof(symbol: str, ref_date: pd.Timestamp) -> tuple[str, float] | None:
    price_path = freq_mod.resolve_cache_path(freq_mod.PRICE_DIR, getattr(freq_mod, "SHARED_PRICE_DIR", None), symbol)
    share_path = freq_mod.resolve_cache_path(freq_mod.SHARE_DIR, getattr(freq_mod, "SHARED_SHARE_DIR", None), symbol)
    if price_path is None or share_path is None:
        return None
    try:
        price = pd.read_csv(price_path, usecols=["date", "close_raw"])
        shares = pd.read_csv(share_path, usecols=["change_date", "total_shares_10k"])
    except Exception:
        return None

    price["date"] = pd.to_datetime(price["date"], errors="coerce")
    price["close_raw"] = pd.to_numeric(price["close_raw"], errors="coerce")
    shares["change_date"] = pd.to_datetime(shares["change_date"], errors="coerce")
    shares["total_shares_10k"] = pd.to_numeric(shares["total_shares_10k"], errors="coerce")
    price = price.dropna(subset=["date", "close_raw"]).sort_values("date")
    shares = shares.dropna(subset=["change_date", "total_shares_10k"]).sort_values("change_date")
    if price.empty or shares.empty:
        return None

    price_part = price.loc[price["date"] <= ref_date]
    shares_part = shares.loc[shares["change_date"] <= ref_date]
    if price_part.empty or shares_part.empty:
        return None

    close_raw = float(price_part.iloc[-1]["close_raw"])
    total_shares = float(shares_part.iloc[-1]["total_shares_10k"]) * 10000.0
    if close_raw <= 0 or total_shares <= 0:
        return None
    return symbol, close_raw * total_shares


def select_recent_candidate_symbols(
    paths: dict[str, Path],
    current_index_end: pd.Timestamp,
    target_end_date: pd.Timestamp,
    max_workers: int,
    top_k: int = 500,
    recent_rebalance_count: int = 6,
    top_n_for_boundary: int = TOP_N,
    dynamic_cap_multiplier: float = 1.5,
    max_dynamic_top_k: int = 1000,
) -> list[str]:
    symbols = set()

    if paths["proxy_members"].exists():
        proxy_members = pd.read_csv(paths["proxy_members"], usecols=["rebalance_date", "symbol"])
        proxy_members["rebalance_date"] = pd.to_datetime(proxy_members["rebalance_date"], errors="coerce")
        proxy_members = proxy_members.dropna(subset=["rebalance_date", "symbol"])
        recent_rebalances = (
            proxy_members["rebalance_date"].drop_duplicates().sort_values().tail(recent_rebalance_count).tolist()
        )
        recent_members = proxy_members.loc[proxy_members["rebalance_date"].isin(recent_rebalances), "symbol"]
        symbols.update(recent_members.astype(str).str.zfill(6).tolist())

    reference_date = min(pd.Timestamp(target_end_date), pd.Timestamp(current_index_end) + pd.Timedelta(days=14))
    universe = freq_mod.load_current_universe()
    workers = max(1, min(int(max_workers), 16))
    caps: list[tuple[str, float]] = []
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(_load_cached_market_cap_asof, symbol, reference_date): symbol for symbol in universe}
        for fut in as_completed(futures):
            result = fut.result()
            if result is not None:
                caps.append(result)

    ranked = sorted(caps, key=lambda item: item[1])
    base_count = max(0, int(top_k))
    selected_ranked = ranked[:base_count]

    boundary_count = min(max(1, int(top_n_for_boundary)), len(ranked))
    if ranked and selected_ranked and dynamic_cap_multiplier > 0 and base_count < len(ranked):
        boundary_cap = float(ranked[boundary_count - 1][1])
        base_tail_cap = float(selected_ranked[-1][1])
        cap_limit = boundary_cap * float(dynamic_cap_multiplier)
        if base_tail_cap <= cap_limit:
            dynamic_count = min(max(base_count, int(max_dynamic_top_k)), len(ranked))
            dynamic_ranked = [item for item in ranked[:dynamic_count] if float(item[1]) <= cap_limit]
            if len(dynamic_ranked) > len(selected_ranked):
                selected_ranked = dynamic_ranked

    symbols.update(symbol for symbol, _ in selected_ranked)
    return sorted(symbols)


def validate_recent_bridge_alignment(
    existing_index_df: pd.DataFrame,
    recent_index_df: pd.DataFrame,
    bridge_date: pd.Timestamp,
    overlap_window: int = 5,
    max_cumulative_return_error: float = 0.05,
    min_return_correlation: float = 0.50,
) -> None:
    bridge_ts = pd.Timestamp(bridge_date).normalize()
    old = existing_index_df[["date", "close"]].copy()
    new = recent_index_df[["date", "close"]].copy()
    old["date"] = pd.to_datetime(old["date"], errors="coerce").dt.normalize()
    new["date"] = pd.to_datetime(new["date"], errors="coerce").dt.normalize()
    old["close"] = pd.to_numeric(old["close"], errors="coerce")
    new["close"] = pd.to_numeric(new["close"], errors="coerce")
    overlap = (
        old.dropna()
        .merge(new.dropna(), on="date", how="inner", suffixes=("_old", "_new"))
        .loc[lambda frame: frame["date"] <= bridge_ts]
        .sort_values("date")
        .tail(max(2, int(overlap_window) + 1))
    )
    if len(overlap) < 3:
        return

    old_close = overlap["close_old"].astype(float)
    new_close = overlap["close_new"].astype(float)
    if old_close.iloc[0] <= 0 or new_close.iloc[0] <= 0:
        raise RuntimeError(f"Invalid bridge overlap close on or before {bridge_ts.date()}.")

    old_cum = old_close.iloc[-1] / old_close.iloc[0] - 1.0
    new_cum = new_close.iloc[-1] / new_close.iloc[0] - 1.0
    cumulative_error = abs(float(new_cum - old_cum))
    if cumulative_error > float(max_cumulative_return_error):
        raise RuntimeError(
            f"Recent proxy bridge cumulative-return drift is too large on {bridge_ts.date()}: "
            f"old={old_cum:.4%}, new={new_cum:.4%}, error={cumulative_error:.4%}."
        )

    returns = pd.DataFrame(
        {
            "old": old_close.pct_change(fill_method=None),
            "new": new_close.pct_change(fill_method=None),
        }
    ).replace([np.inf, -np.inf], np.nan).dropna()
    if len(returns) >= 3 and returns["old"].std() > 0 and returns["new"].std() > 0:
        corr = float(returns["old"].corr(returns["new"]))
        if pd.notna(corr) and corr < float(min_return_correlation):
            raise RuntimeError(
                f"Recent proxy bridge return correlation is too low on {bridge_ts.date()}: corr={corr:.4f}."
            )


def extend_index_recent_window(
    args: argparse.Namespace,
    paths: dict[str, Path],
    panel_path: Path,
    target_end_date: pd.Timestamp,
) -> None:
    if not args.index_csv.exists():
        raise FileNotFoundError(f"Missing proxy index for recent extension: {args.index_csv}")

    index_df = pd.read_csv(args.index_csv)
    index_df["date"] = pd.to_datetime(index_df["date"])
    index_df = index_df.sort_values("date").drop_duplicates(subset="date", keep="last")
    if index_df.empty:
        raise ValueError(f"Existing proxy index is empty: {args.index_csv}")

    current_index_end = pd.Timestamp(index_df["date"].max())
    panel_dates = pd.read_csv(panel_path, usecols=["date"])
    panel_dates["date"] = pd.to_datetime(panel_dates["date"])
    panel_dates = panel_dates.loc[panel_dates["date"] <= pd.Timestamp(target_end_date), "date"].drop_duplicates().sort_values()
    if panel_dates.empty:
        raise ValueError("No trading dates available from panel for recent extension.")

    overlap_needed = max(LOOKBACK + 20, 40)
    recent_dates = pd.DatetimeIndex(panel_dates.tail(overlap_needed))
    if current_index_end not in recent_dates:
        recent_dates = pd.DatetimeIndex(panel_dates.loc[panel_dates >= current_index_end - pd.Timedelta(days=45)])
    if current_index_end not in recent_dates:
        raise RuntimeError(
            f"Recent extension window does not overlap current proxy end {current_index_end.date()}."
        )

    candidate_symbols = select_recent_candidate_symbols(
        paths=paths,
        current_index_end=current_index_end,
        target_end_date=target_end_date,
        max_workers=args.max_workers,
    )
    refresh_price_cache_tail(
        target_end_date,
        args.max_workers,
        candidate_symbols,
        force_refresh=args.force_refresh,
    )

    recent_index_df, recent_members_df, recent_turnover_df, meta = build_local_proxy_bundle(
        args=args,
        trading_dates=recent_dates,
        symbols=candidate_symbols,
    )
    bridge_date = current_index_end
    bridge_old = index_df.loc[index_df["date"] == bridge_date, "close"]
    bridge_new = recent_index_df.loc[recent_index_df["date"] == bridge_date, "close"]
    if bridge_old.empty or bridge_new.empty:
        raise RuntimeError(f"Failed to bridge recent proxy extension on {bridge_date.date()}.")

    validate_recent_bridge_alignment(index_df, recent_index_df, bridge_date)
    scale = float(bridge_old.iloc[0]) / float(bridge_new.iloc[0])
    recent_index_df = recent_index_df.copy()
    recent_index_df["close"] = recent_index_df["close"].astype(float) * scale

    recent_start = pd.Timestamp(recent_dates.min())
    combined_index = pd.concat(
        [index_df.loc[index_df["date"] < recent_start], recent_index_df],
        ignore_index=True,
    ).sort_values("date").drop_duplicates(subset="date", keep="last")

    if paths["proxy_members"].exists():
        existing_members = pd.read_csv(paths["proxy_members"])
        if "rebalance_date" in existing_members.columns:
            existing_members["rebalance_date"] = pd.to_datetime(existing_members["rebalance_date"], errors="coerce")
            existing_members = existing_members.loc[existing_members["rebalance_date"] < recent_start]
        recent_members_out = recent_members_df.copy()
        recent_members_out["rebalance_date"] = pd.to_datetime(recent_members_out["rebalance_date"], errors="coerce")
        combined_members = pd.concat([existing_members, recent_members_out], ignore_index=True)
    else:
        combined_members = recent_members_df

    if paths["proxy_turnover"].exists():
        existing_turnover = pd.read_csv(paths["proxy_turnover"])
        if "rebalance_date" in existing_turnover.columns:
            existing_turnover["rebalance_date"] = pd.to_datetime(existing_turnover["rebalance_date"], errors="coerce")
            existing_turnover = existing_turnover.loc[existing_turnover["rebalance_date"] < recent_start]
        combined_turnover = pd.concat([existing_turnover, recent_turnover_df], ignore_index=True)
    else:
        combined_turnover = recent_turnover_df

    combined_index, combined_members, combined_turnover, effective_start = trim_proxy_history(
        combined_index,
        combined_members,
        combined_turnover,
    )
    _atomic_to_csv(combined_index, args.index_csv, index=False, encoding="utf-8")
    _atomic_to_csv(combined_members, paths["proxy_members"], index=False, encoding="utf-8")
    _atomic_to_csv(combined_turnover, paths["proxy_turnover"], index=False, encoding="utf-8")

    meta["start_date"] = str(pd.Timestamp(combined_index["date"].min()).date())
    meta["end_date"] = str(pd.Timestamp(combined_index["date"].max()).date())
    if effective_start is not None:
        meta["effective_start_date"] = str(effective_start.date())
    if "rebalance_date" in combined_members.columns:
        rebalance_dates = pd.to_datetime(combined_members["rebalance_date"], errors="coerce").dropna().drop_duplicates()
        meta["rebalance_dates_count"] = int(len(rebalance_dates))
    meta["source_used"] = "local_cache_proxy_recent_extension"
    meta["recent_extension_start"] = str(recent_start.date())
    meta["recent_candidate_symbols"] = int(len(candidate_symbols))
    _atomic_write_json(paths["proxy_meta"], meta, encoding="utf-8")


def build_biweekly_rebalance_dates(
    trading_dates: pd.DatetimeIndex,
    week_anchor: str = REBALANCE_WEEKDAY,
) -> pd.DatetimeIndex:
    freq = WEEK_FREQ_BY_START.get(week_anchor)
    if freq is None:
        raise ValueError(f"Unsupported rebalance weekday anchor: {week_anchor}")
    if len(trading_dates) == 0:
        return pd.DatetimeIndex([])
    week_periods = trading_dates.to_period(freq)
    anchor_period = pd.Timestamp(REBALANCE_ANCHOR_DATE).to_period(freq)
    selected_weeks = [period for period in sorted(pd.Index(week_periods.unique())) if (period.ordinal - anchor_period.ordinal) % 2 == 0]
    if not selected_weeks:
        return pd.DatetimeIndex([])
    week_keys = pd.Series(range(len(selected_weeks)), index=selected_weeks)
    aligned_keys = pd.Index(week_periods).map(lambda p: week_keys[p] if p in week_keys.index else np.nan)
    grouped = trading_dates.to_series().groupby(aligned_keys)
    return pd.DatetimeIndex(grouped.min().dropna().tolist())


def build_local_proxy_bundle(
    args: argparse.Namespace,
    trading_dates: pd.DatetimeIndex,
    symbols: list[str] | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, dict[str, object]]:
    rebalance_dates = build_biweekly_rebalance_dates(trading_dates)
    if symbols is None:
        symbols = freq_mod.load_current_universe()
    returns_df, caps_by_date, buyable_df, sellable_df = freq_mod.load_cache_panels(
        symbols=symbols,
        trading_dates=trading_dates,
        cap_dates=rebalance_dates,
        max_workers=args.max_workers,
        trade_constraint_mode=TRADE_CONSTRAINT_MODE,
        exclude_historical_st_from_caps=False,
    )
    name_map = load_name_map()
    target_members_map = build_live_target_members_map(
        caps_by_date=caps_by_date,
        rebalance_dates=rebalance_dates,
        name_map=name_map,
        top_n=TOP_N,
    )
    members_df = freq_mod.build_target_members_frame(target_members_map, caps_by_date, name_map=name_map)
    index_df, turnover_df, _ = freq_mod.simulate_rebalance_path(
        trading_dates=trading_dates,
        returns_df=returns_df,
        target_members_map=target_members_map,
        rebalance_dates=rebalance_dates,
        buyable_df=buyable_df,
        sellable_df=sellable_df,
        one_side_cost_rate=0.003,
        top_n=TOP_N,
        execution_timing=EXECUTION_TIMING,
    )
    index_df["holding_effective"] = index_df["holding_count"].gt(0)
    index_df, members_df, turnover_df, effective_start = trim_proxy_history(index_df, members_df, turnover_df)
    rebalance_count = 0
    if not members_df.empty and "rebalance_date" in members_df.columns:
        rebalance_count = int(pd.to_datetime(members_df["rebalance_date"], errors="coerce").dropna().nunique())
    meta = {
        "index_code": INDEX_CODE,
        "source_used": "local_cache_proxy",
        "method_note": (
            "Local cache reconstruction using raw close data, OHLC tradeability checks, and share-change data. "
            "This practical version anchors biweekly rebalances to Thursday signal dates, excludes suspended names "
            "from signal-date ranking, and applies conservative close execution: if the signal-date close is locked "
            "at the price limit, buys or sells are blocked at the close."
        ),
        "core_params": {
            "top_n": TOP_N,
            "exclude_current_st": True,
            "exclude_bj_exchange": True,
            "exclude_suspended_on_signal_date": True,
            "block_limit_up_entry_at_close": True,
            "block_limit_down_exit_at_close": True,
            "rebalance_frequency": "biweekly",
            "rebalance_weekday_anchor": REBALANCE_WEEKDAY,
            "rebalance_phase_anchor_date": REBALANCE_ANCHOR_DATE,
            "lookback": LOOKBACK,
            "hedge_column": HEDGE_COLUMN,
            "execution_timing": EXECUTION_TIMING,
            "trade_constraint_mode": TRADE_CONSTRAINT_MODE,
            "research_stack_version": RESEARCH_STACK_VERSION,
            "member_filter_policy_version": MEMBER_FILTER_POLICY_VERSION,
            "realtime_quote_policy_version": REALTIME_QUOTE_POLICY_VERSION,
            "proxy_rebalance_policy_version": PROXY_REBALANCE_POLICY_VERSION,
            "security_meta_version": getattr(freq_mod, "SECURITY_META_VERSION", None),
            "security_master_enabled": True,
        },
        "start_date": str(pd.Timestamp(index_df["date"].min()).date()),
        "end_date": str(pd.Timestamp(index_df["date"].max()).date()),
        "rebalance_dates_count": rebalance_count,
    }
    if effective_start is not None:
        meta["effective_start_date"] = str(effective_start.date())
    return index_df, members_df, turnover_df, meta


def proxy_meta_matches_execution_model(meta: dict[str, object]) -> bool:
    core_params = meta.get("core_params") if isinstance(meta, dict) else None
    if not isinstance(core_params, dict):
        return False
    research_stack_version = core_params.get("research_stack_version")
    if research_stack_version not in COMPATIBLE_PROXY_RESEARCH_STACK_VERSIONS:
        return False
    rebalance_phase_anchor_date = core_params.get("rebalance_phase_anchor_date")
    member_filter_policy_version = core_params.get("member_filter_policy_version")
    realtime_quote_policy_version = core_params.get("realtime_quote_policy_version")
    proxy_rebalance_policy_version = core_params.get("proxy_rebalance_policy_version")
    return (
        core_params.get("execution_timing") == EXECUTION_TIMING
        and core_params.get("trade_constraint_mode") == TRADE_CONSTRAINT_MODE
        and rebalance_phase_anchor_date in (None, REBALANCE_ANCHOR_DATE)
        and member_filter_policy_version in (None, MEMBER_FILTER_POLICY_VERSION)
        and realtime_quote_policy_version in (None, REALTIME_QUOTE_POLICY_VERSION)
        and proxy_rebalance_policy_version in (None, PROXY_REBALANCE_POLICY_VERSION)
    )


def proxy_tail_is_suspiciously_flat(index_csv: Path, target_end_date: pd.Timestamp, min_days: int = 5) -> bool:
    if not index_csv.exists():
        return False
    try:
        proxy = pd.read_csv(index_csv)
    except Exception:
        return False
    required = {"date", "close", "daily_return"}
    if not required.issubset(proxy.columns):
        return False
    proxy = proxy.copy()
    proxy["date"] = pd.to_datetime(proxy["date"], errors="coerce")
    proxy["close"] = pd.to_numeric(proxy["close"], errors="coerce")
    proxy["daily_return"] = pd.to_numeric(proxy["daily_return"], errors="coerce")
    proxy = proxy.dropna(subset=["date", "close", "daily_return"]).sort_values("date")
    proxy = proxy.loc[proxy["date"] <= pd.Timestamp(target_end_date)]
    if len(proxy) < min_days:
        return False
    if "holding_effective" in proxy.columns:
        active = proxy["holding_effective"].fillna(False).astype(bool)
    elif "holding_count" in proxy.columns:
        active = pd.to_numeric(proxy["holding_count"], errors="coerce").fillna(0).gt(0)
    else:
        active = pd.Series(True, index=proxy.index)
    tail = proxy.loc[active].tail(min_days)
    if len(tail) < min_days:
        return False
    flat_returns = tail["daily_return"].abs().le(1e-12).all()
    flat_close = tail["close"].nunique(dropna=True) == 1
    return bool(flat_returns and flat_close)


def proxy_latest_row_is_flat_placeholder(index_csv: Path, target_end_date: pd.Timestamp) -> bool:
    if not index_csv.exists():
        return False
    try:
        proxy = pd.read_csv(index_csv)
    except Exception:
        return False
    required = {"date", "close", "daily_return"}
    if not required.issubset(proxy.columns):
        return False
    proxy = proxy.copy()
    proxy["date"] = pd.to_datetime(proxy["date"], errors="coerce")
    proxy["close"] = pd.to_numeric(proxy["close"], errors="coerce")
    proxy["daily_return"] = pd.to_numeric(proxy["daily_return"], errors="coerce")
    proxy = proxy.dropna(subset=["date", "close", "daily_return"]).sort_values("date")
    proxy = proxy.loc[proxy["date"] <= pd.Timestamp(target_end_date)]
    if len(proxy) < 2:
        return False
    latest = proxy.iloc[-1]
    previous = proxy.iloc[-2]
    if pd.Timestamp(latest["date"]).normalize() < pd.Timestamp(target_end_date).normalize():
        return False
    close_tolerance = max(1e-8, abs(float(previous["close"])) * 1e-12)
    return bool(
        abs(float(latest["daily_return"])) <= 1e-12
        and abs(float(latest["close"]) - float(previous["close"])) <= close_tolerance
    )


def assert_proxy_tail_is_actionable(index_csv: Path, target_end_date: pd.Timestamp) -> None:
    if proxy_tail_is_suspiciously_flat(index_csv, target_end_date):
        raise RuntimeError(
            "微盘代理指数尾部连续冻结，拒绝输出实盘信号。请先重建最近窗口并复核价格缓存。"
        )


def ensure_strategy_files(
    args: argparse.Namespace,
    paths: dict[str, Path],
    panel_path: Path,
    target_end_date: pd.Timestamp,
) -> None:
    current_index_end = read_csv_last_date(args.index_csv)
    current_costed_end = read_csv_last_date(args.costed_nav_csv)
    meta_matches_execution_model = False
    if paths["proxy_meta"].exists():
        try:
            meta_matches_execution_model = proxy_meta_matches_execution_model(
                json.loads(paths["proxy_meta"].read_text(encoding="utf-8"))
            )
        except Exception:
            meta_matches_execution_model = False
    can_reuse_index = args.index_csv.exists() and current_index_end is not None and meta_matches_execution_model
    has_proxy_turnover = paths["proxy_turnover"].exists()
    can_reuse_proxy = can_reuse_index and has_proxy_turnover
    frozen_proxy_tail = (
        can_reuse_index
        and current_index_end is not None
        and pd.Timestamp(current_index_end).normalize() >= pd.Timestamp(target_end_date).normalize()
        and (
            proxy_tail_is_suspiciously_flat(args.index_csv, target_end_date)
            or proxy_latest_row_is_flat_placeholder(args.index_csv, target_end_date)
        )
    )
    files_fresh = (
        can_reuse_proxy
        and not frozen_proxy_tail
        and pd.Timestamp(current_index_end).normalize() >= pd.Timestamp(target_end_date).normalize()
        and args.costed_nav_csv.exists()
        and current_costed_end is not None
        and pd.Timestamp(current_costed_end).normalize() >= pd.Timestamp(target_end_date).normalize()
    )
    if files_fresh:
        normalize_existing_proxy_outputs(args, paths)
        return
    # A fresh costed NAV without turnover history cannot be trusted after execution-model changes.
    if (
        can_reuse_proxy
        and not frozen_proxy_tail
        and args.costed_nav_csv.exists()
        and current_costed_end is not None
        and pd.Timestamp(current_index_end).normalize() >= pd.Timestamp(target_end_date).normalize()
        and pd.Timestamp(current_costed_end).normalize() >= pd.Timestamp(target_end_date).normalize()
    ):
        normalize_existing_proxy_outputs(args, paths)
        return
    if not args.rebuild_index_if_missing:
        missing = []
        if not args.index_csv.exists():
            missing.append(str(args.index_csv))
        if not args.costed_nav_csv.exists():
            missing.append(str(args.costed_nav_csv))
        if not paths["proxy_turnover"].exists():
            missing.append(str(paths["proxy_turnover"]))
        raise FileNotFoundError("Missing required strategy files: " + ", ".join(missing))

    if (
        can_reuse_index
        and pd.Timestamp(current_index_end).normalize() >= pd.Timestamp(target_end_date).normalize()
        and args.costed_nav_csv.exists()
        and current_costed_end is not None
        and pd.Timestamp(current_costed_end).normalize() < pd.Timestamp(target_end_date).normalize()
        and try_extend_costed_nav_without_turnover(args, panel_path, target_end_date, paths["proxy_turnover"])
    ):
        return

    if can_reuse_index and pd.Timestamp(current_index_end).normalize() < pd.Timestamp(target_end_date).normalize():
        extend_index_recent_window(args, paths, panel_path, target_end_date)
        if try_extend_costed_nav_without_turnover(args, panel_path, target_end_date, paths["proxy_turnover"]):
            return
        if paths["proxy_turnover"].exists():
            rebuild_costed_nav_from_proxy_turnover(args, paths, panel_path, target_end_date=target_end_date)
            return

    if can_reuse_index and frozen_proxy_tail:
        extend_index_recent_window(args, paths, panel_path, target_end_date)
        if paths["proxy_turnover"].exists():
            rebuild_costed_nav_from_proxy_turnover(args, paths, panel_path, target_end_date=target_end_date)
            assert_proxy_tail_is_actionable(args.index_csv, target_end_date)
            return

    if can_reuse_proxy:
        normalize_existing_proxy_outputs(args, paths)
        rebuild_costed_nav_from_proxy_turnover(args, paths, panel_path, target_end_date=target_end_date)
        assert_proxy_tail_is_actionable(args.index_csv, target_end_date)
        return

    refresh_price_cache_tail(
        target_end_date,
        args.max_workers,
        force_refresh=args.force_refresh,
    )

    panel = pd.read_csv(panel_path, usecols=["date"])
    panel["date"] = pd.to_datetime(panel["date"])
    trading_dates = pd.DatetimeIndex(
        panel.loc[panel["date"] <= pd.Timestamp(target_end_date), "date"].drop_duplicates().sort_values()
    )

    index_df, members_df, turnover_df, meta = build_local_proxy_bundle(args, trading_dates)
    args.index_csv.parent.mkdir(parents=True, exist_ok=True)
    _atomic_to_csv(index_df, args.index_csv, index=False, encoding="utf-8")
    _atomic_to_csv(members_df, paths["proxy_members"], index=False, encoding="utf-8")
    _atomic_to_csv(turnover_df, paths["proxy_turnover"], index=False, encoding="utf-8")
    _atomic_write_json(paths["proxy_meta"], meta, encoding="utf-8")
    rebuild_costed_nav_from_proxy_turnover(args, paths, panel_path, target_end_date=target_end_date)
    assert_proxy_tail_is_actionable(args.index_csv, target_end_date)


def load_close_df(panel_path: Path, index_csv: Path, max_date: pd.Timestamp | None = None) -> pd.DataFrame:
    panel = pd.read_csv(panel_path, usecols=["date", HEDGE_COLUMN])
    panel["date"] = pd.to_datetime(panel["date"])

    proxy = pd.read_csv(index_csv)
    proxy["date"] = pd.to_datetime(proxy["date"])
    if max_date is not None:
        max_ts = pd.Timestamp(max_date).normalize()
        panel = panel.loc[panel["date"].dt.normalize() <= max_ts].copy()
        proxy = proxy.loc[proxy["date"].dt.normalize() <= max_ts].copy()
    hedge = panel.set_index("date")[HEDGE_COLUMN].rename("hedge").astype(float)
    effective_start = infer_proxy_effective_start(proxy)
    microcap = proxy.set_index("date")["close"].rename("microcap").astype(float)

    aligned = pd.concat([microcap, hedge], axis=1).sort_index()
    close_df = aligned.dropna()
    proxy_tail = microcap.dropna().index.max() if not microcap.dropna().empty else None
    aligned_tail = close_df.index.max() if not close_df.empty else None
    if proxy_tail is not None and aligned_tail is not None and pd.Timestamp(aligned_tail) < pd.Timestamp(proxy_tail):
        truncated_dates = microcap.loc[microcap.index > aligned_tail].dropna().index
        warnings.warn(
            "load_close_df truncated proxy tail because hedge data was missing: "
            f"{len(truncated_dates)} rows, last_proxy_date={pd.Timestamp(proxy_tail).date()}, "
            f"last_aligned_date={pd.Timestamp(aligned_tail).date()}",
            RuntimeWarning,
            stacklevel=2,
        )
    if effective_start is not None:
        close_df = close_df.loc[close_df.index >= effective_start].copy()
    if len(close_df) < LOOKBACK + 3:
        raise ValueError(f"Not enough aligned rows for lookback={LOOKBACK}: got {len(close_df)}.")
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


def trim_proxy_history(
    index_df: pd.DataFrame,
    members_df: pd.DataFrame | None = None,
    turnover_df: pd.DataFrame | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame | None, pd.DataFrame | None, pd.Timestamp | None]:
    if index_df.empty:
        return index_df, members_df, turnover_df, None

    index_out = index_df.copy()
    index_out["date"] = pd.to_datetime(index_out["date"], errors="coerce")
    index_out = index_out.dropna(subset=["date"]).sort_values("date").drop_duplicates(subset="date", keep="last")
    effective_start = infer_proxy_effective_start(index_out)
    if effective_start is None:
        return index_out, members_df, turnover_df, None

    index_out = index_out.loc[index_out["date"] >= effective_start].copy()

    members_out = members_df
    if members_df is not None and not members_df.empty and "rebalance_date" in members_df.columns:
        members_out = members_df.copy()
        members_out["rebalance_date"] = pd.to_datetime(members_out["rebalance_date"], errors="coerce")
        members_out = members_out.dropna(subset=["rebalance_date"])
        members_out = members_out.loc[members_out["rebalance_date"] >= effective_start].copy()

    turnover_out = turnover_df
    if turnover_df is not None and not turnover_df.empty and "rebalance_date" in turnover_df.columns:
        turnover_out = turnover_df.copy()
        turnover_out["rebalance_date"] = pd.to_datetime(turnover_out["rebalance_date"], errors="coerce")
        turnover_out = turnover_out.dropna(subset=["rebalance_date"])
        turnover_out = turnover_out.loc[turnover_out["rebalance_date"] >= effective_start].copy()

    return index_out, members_out, turnover_out, effective_start


def normalize_existing_proxy_outputs(args: argparse.Namespace, paths: dict[str, Path]) -> None:
    if not args.index_csv.exists():
        return

    index_df = pd.read_csv(args.index_csv)
    members_df = pd.read_csv(paths["proxy_members"]) if paths["proxy_members"].exists() else None
    trimmed_index, trimmed_members, _, effective_start = trim_proxy_history(index_df, members_df, None)
    if effective_start is None:
        return

    if len(trimmed_index) != len(index_df):
        _atomic_to_csv(trimmed_index, args.index_csv, index=False, encoding="utf-8")

    if members_df is not None and trimmed_members is not None and len(trimmed_members) != len(members_df):
        _atomic_to_csv(trimmed_members, paths["proxy_members"], index=False, encoding="utf-8")

    if args.costed_nav_csv.exists():
        perf = pd.read_csv(args.costed_nav_csv)
        perf["date"] = pd.to_datetime(perf["date"], errors="coerce")
        perf = perf.dropna(subset=["date"]).sort_values("date")
        trimmed_perf = perf.loc[perf["date"] >= effective_start].copy()
        if len(trimmed_perf) != len(perf):
            _atomic_to_csv(trimmed_perf, args.costed_nav_csv, index=False, encoding="utf-8")

    if paths["proxy_turnover"].exists():
        turnover = pd.read_csv(paths["proxy_turnover"])
        if "rebalance_date" in turnover.columns:
            turnover["rebalance_date"] = pd.to_datetime(turnover["rebalance_date"], errors="coerce")
            turnover = turnover.dropna(subset=["rebalance_date"]).sort_values("rebalance_date")
            trimmed_turnover = turnover.loc[turnover["rebalance_date"] >= effective_start].copy()
            if len(trimmed_turnover) != len(turnover):
                _atomic_to_csv(trimmed_turnover, paths["proxy_turnover"], index=False, encoding="utf-8")

    meta = {}
    if paths["proxy_meta"].exists():
        try:
            meta = json.loads(paths["proxy_meta"].read_text(encoding="utf-8"))
        except Exception:
            meta = {}
    meta["start_date"] = str(pd.Timestamp(trimmed_index["date"].min()).date())
    meta["end_date"] = str(pd.Timestamp(trimmed_index["date"].max()).date())
    meta["effective_start_date"] = str(effective_start.date())
    if trimmed_members is not None and not trimmed_members.empty and "rebalance_date" in trimmed_members.columns:
        rebalance_dates = pd.to_datetime(trimmed_members["rebalance_date"], errors="coerce").dropna().drop_duplicates()
        meta["rebalance_dates_count"] = int(len(rebalance_dates))
    _atomic_write_json(paths["proxy_meta"], meta, encoding="utf-8")


def apply_momentum_gap_exit_buffer(gross_result: pd.DataFrame, exit_buffer: float = MOMENTUM_GAP_EXIT_BUFFER) -> pd.DataFrame:
    if exit_buffer < 0:
        raise ValueError("exit_buffer must be non-negative.")
    if gross_result.empty:
        return gross_result.copy()

    out = gross_result.copy().sort_index()
    required = {"microcap_ret", "hedge_ret", "microcap_mom", "momentum_gap"}
    missing = required.difference(out.columns)
    if missing:
        raise KeyError(f"Missing columns for momentum-gap exit buffer: {sorted(missing)}")

    holding = False
    rows = []
    for dt, row in out.iterrows():
        active_ret = 0.0
        drag = FUTURES_DRAG * FIXED_HEDGE_RATIO if holding else 0.0
        if holding and pd.notna(row["microcap_ret"]) and pd.notna(row["hedge_ret"]):
            active_ret = float(row["microcap_ret"] - FIXED_HEDGE_RATIO * row["hedge_ret"])

        gap = float(row["momentum_gap"]) if pd.notna(row["momentum_gap"]) else np.nan
        microcap_mom = float(row["microcap_mom"]) if pd.notna(row["microcap_mom"]) else np.nan
        valid = pd.notna(gap)
        if REQUIRE_POSITIVE_MICROCAP_MOM:
            valid = valid and pd.notna(microcap_mom) and microcap_mom > 0.0
        if not valid:
            signal_on = False
        elif holding:
            signal_on = gap >= -exit_buffer
        else:
            signal_on = gap > 0.0

        rows.append(
            {
                "holding": "long_microcap_short_zz1000" if holding else "cash",
                "next_holding": "long_microcap_short_zz1000" if signal_on else "cash",
                "signal_on": bool(signal_on),
                "return_raw": active_ret - drag,
                "return": active_ret - drag,
                "futures_drag": drag,
                "active_spread_ret": active_ret,
            }
        )
        holding = bool(signal_on)

    adjusted = pd.DataFrame(rows, index=out.index)
    for col in adjusted.columns:
        out[col] = adjusted[col]
    out["momentum_gap_exit_buffer"] = float(exit_buffer)
    return out


def run_signal(close_df: pd.DataFrame) -> pd.DataFrame:
    result = hedge_mod.run_backtest(
        close_df=close_df,
        signal_model="momentum",
        lookback=LOOKBACK,
        bias_n=hedge_mod.DEFAULT_BIAS_N,
        bias_mom_day=hedge_mod.DEFAULT_BIAS_MOM_DAY,
        futures_drag=FUTURES_DRAG * FIXED_HEDGE_RATIO,
        require_positive_microcap_mom=REQUIRE_POSITIVE_MICROCAP_MOM,
        r2_window=hedge_mod.DEFAULT_R2_WINDOW,
        r2_threshold=0.0,
        vol_scale_enabled=False,
        target_vol=hedge_mod.DEFAULT_TARGET_VOL,
        vol_window=hedge_mod.DEFAULT_VOL_WINDOW,
        max_lev=hedge_mod.DEFAULT_MAX_LEV,
        min_lev=hedge_mod.DEFAULT_MIN_LEV,
        scale_threshold=hedge_mod.DEFAULT_SCALE_THRESHOLD,
        hedge_ratio=FIXED_HEDGE_RATIO,
    )
    result.index = pd.to_datetime(result.index)
    return apply_momentum_gap_exit_buffer(result, MOMENTUM_GAP_EXIT_BUFFER)


def _costed_return_from_pre_cost(pre_cost_return: float, total_cost: float) -> float:
    return (1.0 + float(pre_cost_return)) * (1.0 - float(total_cost)) - 1.0


def _pre_cost_return_from_costed(return_net: pd.Series, total_cost: pd.Series) -> pd.Series:
    net = pd.to_numeric(return_net, errors="coerce").fillna(0.0)
    cost = pd.to_numeric(total_cost, errors="coerce").fillna(0.0).clip(lower=0.0, upper=0.99)
    return (1.0 + net).div(1.0 - cost).sub(1.0)


def ensure_overlay_pre_cost_return(out: pd.DataFrame) -> pd.DataFrame:
    if "return_net" not in out.columns or "total_cost" not in out.columns:
        out["overlay_pre_cost_return"] = pd.Series(dtype=float)
        return out
    out["overlay_pre_cost_return"] = _pre_cost_return_from_costed(out["return_net"], out["total_cost"])
    return out


def apply_single_trade_forced_stop_loss(
    gross_result: pd.DataFrame,
    turnover_df: pd.DataFrame,
    stop_loss_threshold: float,
    reentry_momentum_strength_days: int | None = None,
) -> pd.DataFrame:
    if stop_loss_threshold <= 0:
        raise ValueError("stop_loss_threshold must be positive.")
    if reentry_momentum_strength_days is not None and reentry_momentum_strength_days < 2:
        raise ValueError("reentry_momentum_strength_days must be at least 2 when provided.")

    out = gross_result.copy().sort_index()
    if out.empty:
        out["forced_stop_triggered"] = pd.Series(dtype=bool)
        out["signal_reset_seen"] = pd.Series(dtype=bool)
        out["trade_id"] = pd.Series(dtype="Int64")
        out["trade_return_net"] = pd.Series(dtype=float)
        out["entry_exit_cost"] = pd.Series(dtype=float)
        out["rebalance_cost"] = pd.Series(dtype=float)
        out["total_cost"] = pd.Series(dtype=float)
        out["return_net"] = pd.Series(dtype=float)
        out["nav_net"] = pd.Series(dtype=float)
        out["momentum_reentry_streak"] = pd.Series(dtype="Int64")
        out["momentum_reentry_triggered"] = pd.Series(dtype=bool)
        ensure_overlay_pre_cost_return(out)
        return out

    required = {"holding", "next_holding", "return"}
    missing = required.difference(out.columns)
    if missing:
        raise KeyError(f"Missing columns for forced stop loss execution: {sorted(missing)}")
    if reentry_momentum_strength_days is not None and "momentum_gap" not in out.columns:
        raise KeyError("Column 'momentum_gap' is required for momentum-strength reentry.")

    out["base_holding"] = out["holding"].astype(str)
    out["base_next_holding"] = out["next_holding"].astype(str)
    out["base_signal_on"] = out["base_next_holding"].ne("cash")

    rebalance_base = freq_mod.cost_mod.map_rebalance_apply_costs(out.index, turnover_df)
    returns = pd.to_numeric(out["return"], errors="coerce").fillna(0.0)

    executed_holding: list[str] = []
    executed_next_holding: list[str] = []
    executed_signal_on: list[bool] = []
    forced_stop_flags: list[bool] = []
    signal_reset_seen_flags: list[bool] = []
    blocked_flags: list[bool] = []
    entry_exit_costs: list[float] = []
    rebalance_costs: list[float] = []
    total_costs: list[float] = []
    return_nets: list[float] = []
    nav_nets: list[float] = []
    trade_ids: list[int | None] = []
    trade_return_nets: list[float | None] = []
    momentum_reentry_streaks: list[int] = []
    momentum_reentry_flags: list[bool] = []

    current_active = str(out["base_holding"].iloc[0]) != "cash"
    current_trade_id: int | None = 1 if current_active else None
    next_trade_id = 1 if current_active else 0
    blocked_until_reset = False
    signal_reset_seen = False
    nav_net = 1.0
    trade_nav = 1.0 if current_active else 1.0
    blocked_strength_streak = 0
    last_blocked_momentum_gap: float | None = None

    for dt in out.index:
        base_next_active = bool(out.at[dt, "base_next_holding"] != "cash")
        gross_daily_return = float(returns.loc[dt])
        realized_daily_return = gross_daily_return if current_active else 0.0
        momentum_gap = float(out.at[dt, "momentum_gap"]) if "momentum_gap" in out.columns else None
        momentum_reentry_triggered = False

        if blocked_until_reset and reentry_momentum_strength_days is not None:
            if base_next_active and momentum_gap is not None:
                if last_blocked_momentum_gap is None:
                    blocked_strength_streak = 1
                elif momentum_gap > last_blocked_momentum_gap:
                    blocked_strength_streak += 1
                else:
                    blocked_strength_streak = 1
                last_blocked_momentum_gap = momentum_gap
                if blocked_strength_streak >= reentry_momentum_strength_days:
                    blocked_until_reset = False
                    momentum_reentry_triggered = True
                    signal_reset_seen = False
            else:
                blocked_strength_streak = 0
                last_blocked_momentum_gap = None

        desired_next_active = current_active if blocked_until_reset else base_next_active
        if not current_active and desired_next_active:
            next_trade_id += 1
            current_trade_id = next_trade_id
            trade_nav = 1.0

        trade_participates = bool(current_active or desired_next_active)
        if trade_participates:
            trade_id_for_row = current_trade_id
        else:
            trade_id_for_row = None

        entry_cost = freq_mod.cost_mod.ENTRY_COST if (not current_active and desired_next_active) else 0.0
        exit_cost = freq_mod.cost_mod.EXIT_COST if (current_active and not desired_next_active) else 0.0
        rebalance_cost = float(rebalance_base.loc[dt]) if (current_active and desired_next_active) else 0.0

        total_cost = entry_cost + exit_cost + rebalance_cost
        return_net = (1.0 + realized_daily_return) * (1.0 - total_cost) - 1.0
        forced_stop_triggered = False

        stop_decision_return_net = (1.0 + realized_daily_return) * (1.0 - freq_mod.cost_mod.EXIT_COST) - 1.0
        if (
            current_active
            and desired_next_active
            and trade_nav * (1.0 + stop_decision_return_net) - 1.0 <= -float(stop_loss_threshold)
        ):
            desired_next_active = False
            forced_stop_triggered = True
            entry_cost = 0.0
            exit_cost = freq_mod.cost_mod.EXIT_COST
            rebalance_cost = 0.0
            total_cost = exit_cost
            return_net = stop_decision_return_net

        if trade_participates:
            trade_nav *= 1.0 + return_net
            trade_return_net = trade_nav - 1.0
        else:
            trade_return_net = None

        nav_net *= 1.0 + return_net

        executed_holding.append("long_microcap_short_zz1000" if current_active else "cash")
        executed_next_holding.append("long_microcap_short_zz1000" if desired_next_active else "cash")
        executed_signal_on.append(bool(desired_next_active))
        forced_stop_flags.append(bool(forced_stop_triggered))
        blocked_flags.append(bool(blocked_until_reset))
        entry_exit_costs.append(float(entry_cost + exit_cost))
        rebalance_costs.append(float(rebalance_cost))
        total_costs.append(float(total_cost))
        return_nets.append(float(return_net))
        nav_nets.append(float(nav_net))
        trade_ids.append(trade_id_for_row)
        trade_return_nets.append(None if trade_return_net is None else float(trade_return_net))
        momentum_reentry_streaks.append(int(blocked_strength_streak))
        momentum_reentry_flags.append(bool(momentum_reentry_triggered))

        if forced_stop_triggered:
            blocked_until_reset = True
            signal_reset_seen = False
            blocked_strength_streak = 0
            last_blocked_momentum_gap = None
        elif blocked_until_reset and not base_next_active:
            blocked_until_reset = False
            signal_reset_seen = True
            blocked_strength_streak = 0
            last_blocked_momentum_gap = None
        else:
            signal_reset_seen = False
        signal_reset_seen_flags.append(bool(signal_reset_seen))

        current_active = desired_next_active
        if not current_active:
            current_trade_id = None
            trade_nav = 1.0

    out["holding"] = executed_holding
    out["next_holding"] = executed_next_holding
    out["signal_on"] = executed_signal_on
    out["forced_stop_triggered"] = forced_stop_flags
    out["blocked_until_signal_reset"] = blocked_flags
    out["signal_reset_seen"] = signal_reset_seen_flags
    out["trade_id"] = pd.Series(trade_ids, index=out.index, dtype="Int64")
    out["trade_return_net"] = pd.Series(trade_return_nets, index=out.index, dtype=float)
    out["entry_exit_cost"] = pd.Series(entry_exit_costs, index=out.index, dtype=float)
    out["rebalance_cost"] = pd.Series(rebalance_costs, index=out.index, dtype=float)
    out["total_cost"] = pd.Series(total_costs, index=out.index, dtype=float)
    out["return_net"] = pd.Series(return_nets, index=out.index, dtype=float)
    out["nav_net"] = pd.Series(nav_nets, index=out.index, dtype=float)
    out["momentum_reentry_streak"] = pd.Series(momentum_reentry_streaks, index=out.index, dtype="Int64")
    out["momentum_reentry_triggered"] = pd.Series(momentum_reentry_flags, index=out.index, dtype=bool)
    ensure_overlay_pre_cost_return(out)
    return out


def apply_peak_drawdown_forced_stop_loss(
    gross_result: pd.DataFrame,
    turnover_df: pd.DataFrame,
    stop_loss_threshold: float,
    reentry_momentum_strength_days: int | None = None,
    require_reentry_gap_above_stop_level: bool = False,
    stop_trigger_window_days: int | None = None,
    stop_trigger_event_count: int | None = None,
) -> pd.DataFrame:
    if stop_loss_threshold <= 0:
        raise ValueError("stop_loss_threshold must be positive.")
    if reentry_momentum_strength_days is not None and reentry_momentum_strength_days < 2:
        raise ValueError("reentry_momentum_strength_days must be at least 2 when provided.")
    if (stop_trigger_window_days is None) != (stop_trigger_event_count is None):
        raise ValueError("stop_trigger_window_days and stop_trigger_event_count must be provided together.")
    if stop_trigger_window_days is not None and stop_trigger_window_days < 1:
        raise ValueError("stop_trigger_window_days must be at least 1 when provided.")
    if stop_trigger_event_count is not None and stop_trigger_event_count < 1:
        raise ValueError("stop_trigger_event_count must be at least 1 when provided.")

    out = gross_result.copy().sort_index()
    if out.empty:
        out["forced_stop_triggered"] = pd.Series(dtype=bool)
        out["signal_reset_seen"] = pd.Series(dtype=bool)
        out["trade_id"] = pd.Series(dtype="Int64")
        out["trade_return_net"] = pd.Series(dtype=float)
        out["trade_peak_nav"] = pd.Series(dtype=float)
        out["trade_drawdown_from_peak"] = pd.Series(dtype=float)
        out["drawdown_event_triggered"] = pd.Series(dtype=bool)
        out["drawdown_event_count_in_window"] = pd.Series(dtype="Int64")
        out["entry_exit_cost"] = pd.Series(dtype=float)
        out["rebalance_cost"] = pd.Series(dtype=float)
        out["total_cost"] = pd.Series(dtype=float)
        out["return_net"] = pd.Series(dtype=float)
        out["nav_net"] = pd.Series(dtype=float)
        out["momentum_reentry_streak"] = pd.Series(dtype="Int64")
        out["momentum_reentry_triggered"] = pd.Series(dtype=bool)
        ensure_overlay_pre_cost_return(out)
        return out

    required = {"holding", "next_holding", "return"}
    missing = required.difference(out.columns)
    if missing:
        raise KeyError(f"Missing columns for forced stop loss execution: {sorted(missing)}")
    if reentry_momentum_strength_days is not None and "momentum_gap" not in out.columns:
        raise KeyError("Column 'momentum_gap' is required for momentum-strength reentry.")

    out["base_holding"] = out["holding"].astype(str)
    out["base_next_holding"] = out["next_holding"].astype(str)
    out["base_signal_on"] = out["base_next_holding"].ne("cash")

    rebalance_base = freq_mod.cost_mod.map_rebalance_apply_costs(out.index, turnover_df)
    returns = pd.to_numeric(out["return"], errors="coerce").fillna(0.0)

    executed_holding: list[str] = []
    executed_next_holding: list[str] = []
    executed_signal_on: list[bool] = []
    forced_stop_flags: list[bool] = []
    signal_reset_seen_flags: list[bool] = []
    blocked_flags: list[bool] = []
    entry_exit_costs: list[float] = []
    rebalance_costs: list[float] = []
    total_costs: list[float] = []
    return_nets: list[float] = []
    nav_nets: list[float] = []
    trade_ids: list[int | None] = []
    trade_return_nets: list[float | None] = []
    trade_peak_navs: list[float | None] = []
    trade_drawdowns: list[float | None] = []
    drawdown_event_flags: list[bool] = []
    drawdown_event_counts_in_window: list[int] = []
    momentum_reentry_streaks: list[int] = []
    momentum_reentry_flags: list[bool] = []

    current_active = str(out["base_holding"].iloc[0]) != "cash"
    current_trade_id: int | None = 1 if current_active else None
    next_trade_id = 1 if current_active else 0
    blocked_until_reset = False
    signal_reset_seen = False
    nav_net = 1.0
    trade_nav = 1.0
    trade_peak_nav = 1.0
    event_reference_peak_nav = 1.0
    blocked_strength_streak = 0
    last_blocked_momentum_gap: float | None = None
    blocked_stop_momentum_gap: float | None = None
    drawdown_event_dates: list[pd.Timestamp] = []

    for dt in out.index:
        base_next_active = bool(out.at[dt, "base_next_holding"] != "cash")
        gross_daily_return = float(returns.loc[dt])
        realized_daily_return = gross_daily_return if current_active else 0.0
        momentum_gap = float(out.at[dt, "momentum_gap"]) if "momentum_gap" in out.columns else None
        momentum_reentry_triggered = False

        if blocked_until_reset and reentry_momentum_strength_days is not None:
            if base_next_active and momentum_gap is not None:
                if last_blocked_momentum_gap is None:
                    blocked_strength_streak = 1
                elif momentum_gap > last_blocked_momentum_gap:
                    blocked_strength_streak += 1
                else:
                    blocked_strength_streak = 1
                last_blocked_momentum_gap = momentum_gap
                gap_reclaimed = (
                    not require_reentry_gap_above_stop_level
                    or blocked_stop_momentum_gap is None
                    or momentum_gap > blocked_stop_momentum_gap
                )
                if blocked_strength_streak >= reentry_momentum_strength_days and gap_reclaimed:
                    blocked_until_reset = False
                    momentum_reentry_triggered = True
                    signal_reset_seen = False
            else:
                blocked_strength_streak = 0
                last_blocked_momentum_gap = None

        desired_next_active = current_active if blocked_until_reset else base_next_active
        if not current_active and desired_next_active:
            next_trade_id += 1
            current_trade_id = next_trade_id
            trade_nav = 1.0
            trade_peak_nav = 1.0
            event_reference_peak_nav = 1.0
            drawdown_event_dates = []

        trade_participates = bool(current_active or desired_next_active)
        trade_id_for_row = current_trade_id if trade_participates else None

        entry_cost = freq_mod.cost_mod.ENTRY_COST if (not current_active and desired_next_active) else 0.0
        exit_cost = freq_mod.cost_mod.EXIT_COST if (current_active and not desired_next_active) else 0.0
        rebalance_cost = float(rebalance_base.loc[dt]) if (current_active and desired_next_active) else 0.0

        total_cost = entry_cost + exit_cost + rebalance_cost
        return_net = (1.0 + realized_daily_return) * (1.0 - total_cost) - 1.0
        forced_stop_triggered = False
        drawdown_event_triggered = False
        drawdown_event_count_in_window = 0

        if current_active and desired_next_active:
            stop_decision_return_net = (1.0 + realized_daily_return) * (1.0 - freq_mod.cost_mod.EXIT_COST) - 1.0
            trial_trade_nav = trade_nav * (1.0 + stop_decision_return_net)
            event_drawdown = (trial_trade_nav / event_reference_peak_nav - 1.0) if event_reference_peak_nav > 0 else 0.0
            if event_drawdown <= -float(stop_loss_threshold):
                drawdown_event_triggered = True
                event_reference_peak_nav = trial_trade_nav
                drawdown_event_dates.append(pd.Timestamp(dt))
            else:
                event_reference_peak_nav = max(event_reference_peak_nav, trial_trade_nav)

            if stop_trigger_window_days is None:
                drawdown_event_count_in_window = 1 if drawdown_event_triggered else 0
                stop_now = bool(drawdown_event_triggered)
            else:
                window_start = pd.Timestamp(dt) - pd.Timedelta(days=int(stop_trigger_window_days) - 1)
                drawdown_event_dates = [event_dt for event_dt in drawdown_event_dates if event_dt >= window_start]
                drawdown_event_count_in_window = int(len(drawdown_event_dates))
                stop_now = drawdown_event_count_in_window >= int(stop_trigger_event_count)

            if stop_now:
                desired_next_active = False
                forced_stop_triggered = True
                entry_cost = 0.0
                exit_cost = freq_mod.cost_mod.EXIT_COST
                rebalance_cost = 0.0
                total_cost = exit_cost
                return_net = stop_decision_return_net

        if trade_participates:
            trade_nav *= 1.0 + return_net
            trade_peak_nav = max(trade_peak_nav, trade_nav)
            trade_return_net = trade_nav - 1.0
            trade_drawdown_from_peak = (trade_nav / trade_peak_nav - 1.0) if trade_peak_nav > 0 else 0.0
        else:
            trade_return_net = None
            trade_drawdown_from_peak = None

        nav_net *= 1.0 + return_net

        executed_holding.append("long_microcap_short_zz1000" if current_active else "cash")
        executed_next_holding.append("long_microcap_short_zz1000" if desired_next_active else "cash")
        executed_signal_on.append(bool(desired_next_active))
        forced_stop_flags.append(bool(forced_stop_triggered))
        blocked_flags.append(bool(blocked_until_reset))
        entry_exit_costs.append(float(entry_cost + exit_cost))
        rebalance_costs.append(float(rebalance_cost))
        total_costs.append(float(total_cost))
        return_nets.append(float(return_net))
        nav_nets.append(float(nav_net))
        trade_ids.append(trade_id_for_row)
        trade_return_nets.append(None if trade_return_net is None else float(trade_return_net))
        trade_peak_navs.append(None if trade_return_net is None else float(trade_peak_nav))
        trade_drawdowns.append(None if trade_drawdown_from_peak is None else float(trade_drawdown_from_peak))
        drawdown_event_flags.append(bool(drawdown_event_triggered))
        drawdown_event_counts_in_window.append(int(drawdown_event_count_in_window))
        momentum_reentry_streaks.append(int(blocked_strength_streak))
        momentum_reentry_flags.append(bool(momentum_reentry_triggered))

        if forced_stop_triggered:
            blocked_until_reset = True
            signal_reset_seen = False
            blocked_strength_streak = 0
            last_blocked_momentum_gap = None
            blocked_stop_momentum_gap = momentum_gap
        elif blocked_until_reset and not base_next_active:
            blocked_until_reset = False
            signal_reset_seen = True
            blocked_strength_streak = 0
            last_blocked_momentum_gap = None
            blocked_stop_momentum_gap = None
        else:
            signal_reset_seen = False
        signal_reset_seen_flags.append(bool(signal_reset_seen))

        current_active = desired_next_active
        if not current_active:
            current_trade_id = None
            trade_nav = 1.0
            trade_peak_nav = 1.0
            event_reference_peak_nav = 1.0
            drawdown_event_dates = []

    out["holding"] = executed_holding
    out["next_holding"] = executed_next_holding
    out["signal_on"] = executed_signal_on
    out["forced_stop_triggered"] = forced_stop_flags
    out["blocked_until_signal_reset"] = blocked_flags
    out["signal_reset_seen"] = signal_reset_seen_flags
    out["trade_id"] = pd.Series(trade_ids, index=out.index, dtype="Int64")
    out["trade_return_net"] = pd.Series(trade_return_nets, index=out.index, dtype=float)
    out["trade_peak_nav"] = pd.Series(trade_peak_navs, index=out.index, dtype=float)
    out["trade_drawdown_from_peak"] = pd.Series(trade_drawdowns, index=out.index, dtype=float)
    out["drawdown_event_triggered"] = pd.Series(drawdown_event_flags, index=out.index, dtype=bool)
    out["drawdown_event_count_in_window"] = pd.Series(drawdown_event_counts_in_window, index=out.index, dtype="Int64")
    out["entry_exit_cost"] = pd.Series(entry_exit_costs, index=out.index, dtype=float)
    out["rebalance_cost"] = pd.Series(rebalance_costs, index=out.index, dtype=float)
    out["total_cost"] = pd.Series(total_costs, index=out.index, dtype=float)
    out["return_net"] = pd.Series(return_nets, index=out.index, dtype=float)
    out["nav_net"] = pd.Series(nav_nets, index=out.index, dtype=float)
    out["momentum_reentry_streak"] = pd.Series(momentum_reentry_streaks, index=out.index, dtype="Int64")
    out["momentum_reentry_triggered"] = pd.Series(momentum_reentry_flags, index=out.index, dtype=bool)
    ensure_overlay_pre_cost_return(out)
    return out


def apply_ratio_bias_take_profit(
    gross_result: pd.DataFrame,
    turnover_df: pd.DataFrame,
    bias_window: int,
    take_profit_threshold: float,
    require_positive_trade_return: bool = True,
    require_signal_reset_after_take_profit: bool = True,
) -> pd.DataFrame:
    if bias_window < 1:
        raise ValueError("bias_window must be at least 1.")
    if take_profit_threshold <= 0:
        raise ValueError("take_profit_threshold must be positive.")

    out = gross_result.copy().sort_index()
    if out.empty:
        out["ratio_bias"] = pd.Series(dtype=float)
        out["take_profit_triggered"] = pd.Series(dtype=bool)
        out["blocked_until_signal_reset"] = pd.Series(dtype=bool)
        out["signal_reset_seen"] = pd.Series(dtype=bool)
        out["trade_id"] = pd.Series(dtype="Int64")
        out["trade_return_net"] = pd.Series(dtype=float)
        out["entry_exit_cost"] = pd.Series(dtype=float)
        out["rebalance_cost"] = pd.Series(dtype=float)
        out["total_cost"] = pd.Series(dtype=float)
        out["return_net"] = pd.Series(dtype=float)
        out["nav_net"] = pd.Series(dtype=float)
        ensure_overlay_pre_cost_return(out)
        return out

    required = {"holding", "next_holding", "return", "microcap_close", "hedge_close"}
    missing = required.difference(out.columns)
    if missing:
        raise KeyError(f"Missing columns for ratio bias take profit execution: {sorted(missing)}")

    out["base_holding"] = out["holding"].astype(str)
    out["base_next_holding"] = out["next_holding"].astype(str)
    out["base_signal_on"] = out["base_next_holding"].ne("cash")

    ratio = pd.to_numeric(out["microcap_close"], errors="coerce") / pd.to_numeric(out["hedge_close"], errors="coerce")
    out["ratio_bias"] = ratio.div(ratio.rolling(int(bias_window)).mean()).sub(1.0)

    rebalance_base = freq_mod.cost_mod.map_rebalance_apply_costs(out.index, turnover_df)
    returns = pd.to_numeric(out["return"], errors="coerce").fillna(0.0)

    executed_holding: list[str] = []
    executed_next_holding: list[str] = []
    executed_signal_on: list[bool] = []
    take_profit_flags: list[bool] = []
    blocked_flags: list[bool] = []
    signal_reset_seen_flags: list[bool] = []
    entry_exit_costs: list[float] = []
    rebalance_costs: list[float] = []
    total_costs: list[float] = []
    return_nets: list[float] = []
    nav_nets: list[float] = []
    trade_ids: list[int | None] = []
    trade_return_nets: list[float | None] = []

    current_active = str(out["base_holding"].iloc[0]) != "cash"
    current_trade_id: int | None = 1 if current_active else None
    next_trade_id = 1 if current_active else 0
    blocked_until_reset = False
    signal_reset_seen = False
    nav_net = 1.0
    trade_nav = 1.0

    for dt in out.index:
        base_next_active = bool(out.at[dt, "base_next_holding"] != "cash")
        desired_next_active = current_active if blocked_until_reset else base_next_active

        if not current_active and desired_next_active:
            next_trade_id += 1
            current_trade_id = next_trade_id
            trade_nav = 1.0

        trade_participates = bool(current_active or desired_next_active)
        trade_id_for_row = current_trade_id if trade_participates else None

        gross_daily_return = float(returns.loc[dt])
        realized_daily_return = gross_daily_return if current_active else 0.0
        ratio_bias = out.at[dt, "ratio_bias"]

        entry_cost = freq_mod.cost_mod.ENTRY_COST if (not current_active and desired_next_active) else 0.0
        exit_cost = freq_mod.cost_mod.EXIT_COST if (current_active and not desired_next_active) else 0.0
        rebalance_cost = float(rebalance_base.loc[dt]) if (current_active and desired_next_active) else 0.0
        total_cost = entry_cost + exit_cost + rebalance_cost
        return_net = (1.0 + realized_daily_return) * (1.0 - total_cost) - 1.0

        trade_return_before_exit: float | None = None
        if current_active and desired_next_active:
            trade_nav_before_exit = trade_nav * (1.0 + return_net)
            trade_return_before_exit = trade_nav_before_exit - 1.0
        take_profit_triggered = False
        if (
            current_active
            and desired_next_active
            and pd.notna(ratio_bias)
            and float(ratio_bias) >= float(take_profit_threshold)
            and (
                not require_positive_trade_return
                or (trade_return_before_exit is not None and trade_return_before_exit > 0.0)
            )
        ):
            desired_next_active = False
            take_profit_triggered = True
            entry_cost = 0.0
            exit_cost = freq_mod.cost_mod.EXIT_COST
            rebalance_cost = 0.0
            total_cost = exit_cost
            return_net = (1.0 + realized_daily_return) * (1.0 - total_cost) - 1.0

        if trade_participates:
            trade_nav *= 1.0 + return_net
            trade_return_net = trade_nav - 1.0
        else:
            trade_return_net = None

        nav_net *= 1.0 + return_net

        executed_holding.append("long_microcap_short_zz1000" if current_active else "cash")
        executed_next_holding.append("long_microcap_short_zz1000" if desired_next_active else "cash")
        executed_signal_on.append(bool(desired_next_active))
        take_profit_flags.append(bool(take_profit_triggered))
        blocked_flags.append(bool(blocked_until_reset))
        entry_exit_costs.append(float(entry_cost + exit_cost))
        rebalance_costs.append(float(rebalance_cost))
        total_costs.append(float(total_cost))
        return_nets.append(float(return_net))
        nav_nets.append(float(nav_net))
        trade_ids.append(trade_id_for_row)
        trade_return_nets.append(None if trade_return_net is None else float(trade_return_net))

        if take_profit_triggered and require_signal_reset_after_take_profit:
            blocked_until_reset = True
            signal_reset_seen = False
        elif blocked_until_reset and not base_next_active:
            blocked_until_reset = False
            signal_reset_seen = True
        else:
            signal_reset_seen = False
        signal_reset_seen_flags.append(bool(signal_reset_seen))

        current_active = desired_next_active
        if not current_active:
            current_trade_id = None
            trade_nav = 1.0

    out["holding"] = executed_holding
    out["next_holding"] = executed_next_holding
    out["signal_on"] = executed_signal_on
    out["take_profit_triggered"] = pd.Series(take_profit_flags, index=out.index, dtype=bool)
    out["blocked_until_signal_reset"] = pd.Series(blocked_flags, index=out.index, dtype=bool)
    out["signal_reset_seen"] = pd.Series(signal_reset_seen_flags, index=out.index, dtype=bool)
    out["trade_id"] = pd.Series(trade_ids, index=out.index, dtype="Int64")
    out["trade_return_net"] = pd.Series(trade_return_nets, index=out.index, dtype=float)
    out["entry_exit_cost"] = pd.Series(entry_exit_costs, index=out.index, dtype=float)
    out["rebalance_cost"] = pd.Series(rebalance_costs, index=out.index, dtype=float)
    out["total_cost"] = pd.Series(total_costs, index=out.index, dtype=float)
    out["return_net"] = pd.Series(return_nets, index=out.index, dtype=float)
    out["nav_net"] = pd.Series(nav_nets, index=out.index, dtype=float)
    ensure_overlay_pre_cost_return(out)
    return out


def apply_momentum_gap_peak_decay_exit(
    gross_result: pd.DataFrame,
    turnover_df: pd.DataFrame,
    decay_ratio_threshold: float,
    require_signal_reset_after_exit: bool = True,
) -> pd.DataFrame:
    if decay_ratio_threshold < 0:
        raise ValueError("decay_ratio_threshold must be non-negative.")

    out = gross_result.copy().sort_index()
    if out.empty:
        out["gap_peak"] = pd.Series(dtype=float)
        out["gap_decay_ratio"] = pd.Series(dtype=float)
        out["signal_quality_exit_triggered"] = pd.Series(dtype=bool)
        out["blocked_until_signal_reset"] = pd.Series(dtype=bool)
        out["signal_reset_seen"] = pd.Series(dtype=bool)
        out["trade_id"] = pd.Series(dtype="Int64")
        out["trade_return_net"] = pd.Series(dtype=float)
        out["entry_exit_cost"] = pd.Series(dtype=float)
        out["rebalance_cost"] = pd.Series(dtype=float)
        out["total_cost"] = pd.Series(dtype=float)
        out["return_net"] = pd.Series(dtype=float)
        out["nav_net"] = pd.Series(dtype=float)
        ensure_overlay_pre_cost_return(out)
        return out

    required = {"holding", "next_holding", "return", "momentum_gap"}
    missing = required.difference(out.columns)
    if missing:
        raise KeyError(f"Missing columns for momentum-gap peak-decay exit execution: {sorted(missing)}")

    out["base_holding"] = out["holding"].astype(str)
    out["base_next_holding"] = out["next_holding"].astype(str)
    out["base_signal_on"] = out["base_next_holding"].ne("cash")

    rebalance_base = freq_mod.cost_mod.map_rebalance_apply_costs(out.index, turnover_df)
    returns = pd.to_numeric(out["return"], errors="coerce").fillna(0.0)
    momentum_gap_series = pd.to_numeric(out["momentum_gap"], errors="coerce")

    executed_holding: list[str] = []
    executed_next_holding: list[str] = []
    executed_signal_on: list[bool] = []
    exit_flags: list[bool] = []
    blocked_flags: list[bool] = []
    signal_reset_seen_flags: list[bool] = []
    trade_ids: list[int | None] = []
    trade_return_nets: list[float | None] = []
    gap_peaks: list[float | None] = []
    gap_decay_ratios: list[float | None] = []
    entry_exit_costs: list[float] = []
    rebalance_costs: list[float] = []
    total_costs: list[float] = []
    return_nets: list[float] = []
    nav_nets: list[float] = []

    current_active = str(out["base_holding"].iloc[0]) != "cash"
    current_trade_id: int | None = 1 if current_active else None
    next_trade_id = 1 if current_active else 0
    blocked_until_reset = False
    signal_reset_seen = False
    nav_net = 1.0
    trade_nav = 1.0
    gap_peak: float | None = None

    for dt in out.index:
        base_next_active = bool(out.at[dt, "base_next_holding"] != "cash")
        desired_next_active = current_active if blocked_until_reset else base_next_active
        current_gap = float(momentum_gap_series.loc[dt]) if pd.notna(momentum_gap_series.loc[dt]) else None

        if not current_active and desired_next_active:
            next_trade_id += 1
            current_trade_id = next_trade_id
            trade_nav = 1.0
            gap_peak = current_gap

        trade_participates = bool(current_active or desired_next_active)
        trade_id_for_row = current_trade_id if trade_participates else None

        gross_daily_return = float(returns.loc[dt])
        realized_daily_return = gross_daily_return if current_active else 0.0

        if current_active and current_gap is not None:
            gap_peak = current_gap if gap_peak is None else max(float(gap_peak), current_gap)
        gap_decay_ratio = None
        if current_active and current_gap is not None and gap_peak is not None and gap_peak > 0:
            gap_decay_ratio = current_gap / gap_peak

        entry_cost = freq_mod.cost_mod.ENTRY_COST if (not current_active and desired_next_active) else 0.0
        exit_cost = freq_mod.cost_mod.EXIT_COST if (current_active and not desired_next_active) else 0.0
        rebalance_cost = float(rebalance_base.loc[dt]) if (current_active and desired_next_active) else 0.0
        total_cost = entry_cost + exit_cost + rebalance_cost
        return_net = (1.0 + realized_daily_return) * (1.0 - total_cost) - 1.0

        signal_quality_exit_triggered = False
        if (
            current_active
            and desired_next_active
            and gap_decay_ratio is not None
            and gap_decay_ratio <= float(decay_ratio_threshold)
        ):
            desired_next_active = False
            signal_quality_exit_triggered = True
            entry_cost = 0.0
            exit_cost = freq_mod.cost_mod.EXIT_COST
            rebalance_cost = 0.0
            total_cost = exit_cost
            return_net = (1.0 + realized_daily_return) * (1.0 - total_cost) - 1.0

        if trade_participates:
            trade_nav *= 1.0 + return_net
            trade_return_net = trade_nav - 1.0
        else:
            trade_return_net = None

        nav_net *= 1.0 + return_net

        executed_holding.append("long_microcap_short_zz1000" if current_active else "cash")
        executed_next_holding.append("long_microcap_short_zz1000" if desired_next_active else "cash")
        executed_signal_on.append(bool(desired_next_active))
        exit_flags.append(bool(signal_quality_exit_triggered))
        blocked_flags.append(bool(blocked_until_reset))
        trade_ids.append(trade_id_for_row)
        trade_return_nets.append(None if trade_return_net is None else float(trade_return_net))
        gap_peaks.append(None if gap_peak is None else float(gap_peak))
        gap_decay_ratios.append(None if gap_decay_ratio is None else float(gap_decay_ratio))
        entry_exit_costs.append(float(entry_cost + exit_cost))
        rebalance_costs.append(float(rebalance_cost))
        total_costs.append(float(total_cost))
        return_nets.append(float(return_net))
        nav_nets.append(float(nav_net))

        if signal_quality_exit_triggered and require_signal_reset_after_exit:
            blocked_until_reset = True
            signal_reset_seen = False
        elif blocked_until_reset and not base_next_active:
            blocked_until_reset = False
            signal_reset_seen = True
        else:
            signal_reset_seen = False
        signal_reset_seen_flags.append(bool(signal_reset_seen))

        current_active = desired_next_active
        if not current_active:
            current_trade_id = None
            trade_nav = 1.0
            if not blocked_until_reset:
                gap_peak = None

    out["holding"] = executed_holding
    out["next_holding"] = executed_next_holding
    out["signal_on"] = executed_signal_on
    out["gap_peak"] = pd.Series(gap_peaks, index=out.index, dtype=float)
    out["gap_decay_ratio"] = pd.Series(gap_decay_ratios, index=out.index, dtype=float)
    out["signal_quality_exit_triggered"] = pd.Series(exit_flags, index=out.index, dtype=bool)
    out["blocked_until_signal_reset"] = pd.Series(blocked_flags, index=out.index, dtype=bool)
    out["signal_reset_seen"] = pd.Series(signal_reset_seen_flags, index=out.index, dtype=bool)
    out["trade_id"] = pd.Series(trade_ids, index=out.index, dtype="Int64")
    out["trade_return_net"] = pd.Series(trade_return_nets, index=out.index, dtype=float)
    out["entry_exit_cost"] = pd.Series(entry_exit_costs, index=out.index, dtype=float)
    out["rebalance_cost"] = pd.Series(rebalance_costs, index=out.index, dtype=float)
    out["total_cost"] = pd.Series(total_costs, index=out.index, dtype=float)
    out["return_net"] = pd.Series(return_nets, index=out.index, dtype=float)
    out["nav_net"] = pd.Series(nav_nets, index=out.index, dtype=float)
    ensure_overlay_pre_cost_return(out)
    return out


def run_momentum_gap_peak_decay_scan(
    gross_result: pd.DataFrame,
    turnover_df: pd.DataFrame,
    decay_thresholds: tuple[float, ...],
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for threshold in tuple(float(v) for v in decay_thresholds):
        result = apply_momentum_gap_peak_decay_exit(
            gross_result=gross_result,
            turnover_df=turnover_df,
            decay_ratio_threshold=threshold,
        )
        metrics = hedge_mod.calc_metrics(result["return_net"].fillna(0.0))
        rows.append(
            {
                "threshold": threshold,
                "threshold_pct": threshold * 100.0,
                "signal_quality_exit_count": int(result["signal_quality_exit_triggered"].fillna(False).sum()),
                "annual_return": float(metrics.annual),
                "max_drawdown": float(metrics.max_dd),
                "total_return": float(metrics.total_return),
                "final_nav": float(result["nav_net"].iloc[-1]),
            }
        )
    return pd.DataFrame(rows).sort_values("threshold").reset_index(drop=True)


def apply_momentum_gap_peak_decay_derisk(
    gross_result: pd.DataFrame,
    turnover_df: pd.DataFrame,
    decay_ratio_threshold: float,
    derisk_scale: float,
    recovery_ratio_threshold: float | None = None,
) -> pd.DataFrame:
    if decay_ratio_threshold < 0:
        raise ValueError("decay_ratio_threshold must be non-negative.")
    if not (0.0 <= derisk_scale <= 1.0):
        raise ValueError("derisk_scale must be between 0 and 1.")
    if recovery_ratio_threshold is not None:
        recovery_ratio_threshold = float(recovery_ratio_threshold)
        if recovery_ratio_threshold < 0:
            raise ValueError("recovery_ratio_threshold must be non-negative.")

    out = gross_result.copy().sort_index()
    if out.empty:
        out["gap_peak"] = pd.Series(dtype=float)
        out["gap_decay_ratio"] = pd.Series(dtype=float)
        out["signal_quality_derisk_triggered"] = pd.Series(dtype=bool)
        out["execution_scale"] = pd.Series(dtype=float)
        out["signal_quality_scale_turnover"] = pd.Series(dtype=float)
        out["signal_quality_scale_cost"] = pd.Series(dtype=float)
        out["trade_id"] = pd.Series(dtype="Int64")
        out["trade_return_net"] = pd.Series(dtype=float)
        out["entry_exit_cost"] = pd.Series(dtype=float)
        out["rebalance_cost"] = pd.Series(dtype=float)
        out["total_cost"] = pd.Series(dtype=float)
        out["return_net"] = pd.Series(dtype=float)
        out["nav_net"] = pd.Series(dtype=float)
        ensure_overlay_pre_cost_return(out)
        return out

    required = {"holding", "next_holding", "return", "momentum_gap"}
    missing = required.difference(out.columns)
    if missing:
        raise KeyError(f"Missing columns for momentum-gap peak-decay derisk execution: {sorted(missing)}")

    out["base_holding"] = out["holding"].astype(str)
    out["base_next_holding"] = out["next_holding"].astype(str)
    out["base_signal_on"] = out["base_next_holding"].ne("cash")

    rebalance_base = freq_mod.cost_mod.map_rebalance_apply_costs(out.index, turnover_df)
    returns = pd.to_numeric(out["return"], errors="coerce").fillna(0.0)
    momentum_gap_series = pd.to_numeric(out["momentum_gap"], errors="coerce")

    executed_holding: list[str] = []
    executed_next_holding: list[str] = []
    executed_signal_on: list[bool] = []
    derisk_flags: list[bool] = []
    execution_scales: list[float] = []
    signal_quality_scale_turnovers: list[float] = []
    signal_quality_scale_costs: list[float] = []
    trade_ids: list[int | None] = []
    trade_return_nets: list[float | None] = []
    gap_peaks: list[float | None] = []
    gap_decay_ratios: list[float | None] = []
    entry_exit_costs: list[float] = []
    rebalance_costs: list[float] = []
    total_costs: list[float] = []
    return_nets: list[float] = []
    nav_nets: list[float] = []

    current_active = str(out["base_holding"].iloc[0]) != "cash"
    current_trade_id: int | None = 1 if current_active else None
    next_trade_id = 1 if current_active else 0
    nav_net = 1.0
    trade_nav = 1.0
    gap_peak: float | None = None
    active_scale = 1.0
    derisked_in_trade = False
    waiting_for_new_peak_after_recovery = False
    rearm_peak_level: float | None = None

    for dt in out.index:
        desired_next_active = bool(out.at[dt, "base_next_holding"] != "cash")
        current_gap = float(momentum_gap_series.loc[dt]) if pd.notna(momentum_gap_series.loc[dt]) else None

        if not current_active and desired_next_active:
            next_trade_id += 1
            current_trade_id = next_trade_id
            trade_nav = 1.0
            gap_peak = current_gap
            active_scale = 1.0
            derisked_in_trade = False
            waiting_for_new_peak_after_recovery = False
            rearm_peak_level = None

        trade_participates = bool(current_active or desired_next_active)
        trade_id_for_row = current_trade_id if trade_participates else None

        gross_daily_return = float(returns.loc[dt])
        if current_active and current_gap is not None:
            gap_peak = current_gap if gap_peak is None else max(float(gap_peak), current_gap)
        if (
            current_active
            and waiting_for_new_peak_after_recovery
            and rearm_peak_level is not None
            and gap_peak is not None
            and float(gap_peak) > float(rearm_peak_level)
        ):
            waiting_for_new_peak_after_recovery = False
            rearm_peak_level = None
        gap_decay_ratio = None
        if current_active and current_gap is not None and gap_peak is not None and gap_peak > 0:
            gap_decay_ratio = current_gap / gap_peak

        signal_quality_derisk_triggered = False
        previous_active_scale = active_scale if current_active else 0.0
        applied_scale = active_scale if current_active else 0.0
        if (
            current_active
            and desired_next_active
            and derisked_in_trade
            and recovery_ratio_threshold is not None
            and gap_decay_ratio is not None
            and gap_decay_ratio >= recovery_ratio_threshold
        ):
            active_scale = 1.0
            applied_scale = active_scale
            derisked_in_trade = False
            waiting_for_new_peak_after_recovery = True
            rearm_peak_level = gap_peak
        if (
            current_active
            and desired_next_active
            and not derisked_in_trade
            and not waiting_for_new_peak_after_recovery
            and gap_decay_ratio is not None
            and gap_decay_ratio <= float(decay_ratio_threshold)
        ):
            active_scale = float(derisk_scale)
            applied_scale = active_scale
            derisked_in_trade = True
            signal_quality_derisk_triggered = True

        realized_daily_return = gross_daily_return * applied_scale if current_active else 0.0

        entry_cost = freq_mod.cost_mod.ENTRY_COST if (not current_active and desired_next_active) else 0.0
        exit_cost = freq_mod.cost_mod.EXIT_COST if (current_active and not desired_next_active) else 0.0
        scale_delta = float(applied_scale - previous_active_scale) if (current_active and desired_next_active) else 0.0
        rebalance_exposure_scale = max(float(previous_active_scale), float(applied_scale)) if (current_active and desired_next_active) else 0.0
        rebalance_cost = float(rebalance_base.loc[dt]) * rebalance_exposure_scale
        signal_quality_scale_turnover = abs(scale_delta)
        if scale_delta < 0:
            signal_quality_scale_cost = abs(scale_delta) * freq_mod.cost_mod.EXIT_COST
        elif scale_delta > 0:
            signal_quality_scale_cost = abs(scale_delta) * freq_mod.cost_mod.ENTRY_COST
        else:
            signal_quality_scale_cost = 0.0
        total_cost = entry_cost + exit_cost + rebalance_cost + signal_quality_scale_cost
        return_net = _costed_return_from_pre_cost(realized_daily_return, total_cost)

        if trade_participates:
            trade_nav *= 1.0 + return_net
            trade_return_net = trade_nav - 1.0
        else:
            trade_return_net = None

        nav_net *= 1.0 + return_net

        executed_holding.append("long_microcap_short_zz1000" if current_active else "cash")
        executed_next_holding.append("long_microcap_short_zz1000" if desired_next_active else "cash")
        executed_signal_on.append(bool(desired_next_active))
        derisk_flags.append(bool(signal_quality_derisk_triggered))
        execution_scales.append(float(applied_scale))
        signal_quality_scale_turnovers.append(float(signal_quality_scale_turnover))
        signal_quality_scale_costs.append(float(signal_quality_scale_cost))
        trade_ids.append(trade_id_for_row)
        trade_return_nets.append(None if trade_return_net is None else float(trade_return_net))
        gap_peaks.append(None if gap_peak is None else float(gap_peak))
        gap_decay_ratios.append(None if gap_decay_ratio is None else float(gap_decay_ratio))
        entry_exit_costs.append(float(entry_cost + exit_cost))
        rebalance_costs.append(float(rebalance_cost))
        total_costs.append(float(total_cost))
        return_nets.append(float(return_net))
        nav_nets.append(float(nav_net))

        current_active = desired_next_active
        if not current_active:
            current_trade_id = None
            trade_nav = 1.0
            gap_peak = None
            active_scale = 1.0
            derisked_in_trade = False
            waiting_for_new_peak_after_recovery = False
            rearm_peak_level = None

    out["holding"] = executed_holding
    out["next_holding"] = executed_next_holding
    out["signal_on"] = executed_signal_on
    out["gap_peak"] = pd.Series(gap_peaks, index=out.index, dtype=float)
    out["gap_decay_ratio"] = pd.Series(gap_decay_ratios, index=out.index, dtype=float)
    out["signal_quality_derisk_triggered"] = pd.Series(derisk_flags, index=out.index, dtype=bool)
    out["execution_scale"] = pd.Series(execution_scales, index=out.index, dtype=float)
    out["signal_quality_scale_turnover"] = pd.Series(signal_quality_scale_turnovers, index=out.index, dtype=float)
    out["signal_quality_scale_cost"] = pd.Series(signal_quality_scale_costs, index=out.index, dtype=float)
    out["trade_id"] = pd.Series(trade_ids, index=out.index, dtype="Int64")
    out["trade_return_net"] = pd.Series(trade_return_nets, index=out.index, dtype=float)
    out["entry_exit_cost"] = pd.Series(entry_exit_costs, index=out.index, dtype=float)
    out["rebalance_cost"] = pd.Series(rebalance_costs, index=out.index, dtype=float)
    out["total_cost"] = pd.Series(total_costs, index=out.index, dtype=float)
    out["return_net"] = pd.Series(return_nets, index=out.index, dtype=float)
    out["nav_net"] = pd.Series(nav_nets, index=out.index, dtype=float)
    ensure_overlay_pre_cost_return(out)
    return out


def run_momentum_gap_peak_decay_derisk_scan(
    gross_result: pd.DataFrame,
    turnover_df: pd.DataFrame,
    decay_thresholds: tuple[float, ...],
    derisk_scales: tuple[float, ...],
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for threshold in tuple(float(v) for v in decay_thresholds):
        for derisk_scale in tuple(float(v) for v in derisk_scales):
            result = apply_momentum_gap_peak_decay_derisk(
                gross_result=gross_result,
                turnover_df=turnover_df,
                decay_ratio_threshold=threshold,
                derisk_scale=derisk_scale,
            )
            metrics = hedge_mod.calc_metrics(result["return_net"].fillna(0.0))
            rows.append(
                {
                    "threshold": threshold,
                    "threshold_pct": threshold * 100.0,
                    "derisk_scale": derisk_scale,
                    "signal_quality_derisk_count": int(result["signal_quality_derisk_triggered"].fillna(False).sum()),
                    "annual_return": float(metrics.annual),
                    "max_drawdown": float(metrics.max_dd),
                    "total_return": float(metrics.total_return),
                    "final_nav": float(result["nav_net"].iloc[-1]),
                }
            )
    return pd.DataFrame(rows).sort_values(["threshold", "derisk_scale"]).reset_index(drop=True)


def run_forced_stop_loss_scan(
    gross_result: pd.DataFrame,
    turnover_df: pd.DataFrame,
    thresholds: tuple[float, ...] = DEFAULT_FORCED_STOP_LOSS_SCAN_THRESHOLDS,
    reentry_momentum_strength_days: int | None = None,
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    ordered_thresholds = tuple(float(value) for value in thresholds)
    for threshold in ordered_thresholds:
        result = apply_single_trade_forced_stop_loss(
            gross_result=gross_result,
            turnover_df=turnover_df,
            stop_loss_threshold=threshold,
            reentry_momentum_strength_days=reentry_momentum_strength_days,
        )
        metrics = hedge_mod.calc_metrics(result["return_net"].fillna(0.0))
        rows.append(
            {
                "threshold": threshold,
                "threshold_pct": int(round(threshold * 100)),
                "reentry_momentum_strength_days": reentry_momentum_strength_days,
                "forced_stop_count": int(result["forced_stop_triggered"].fillna(False).sum()),
                "momentum_reentry_count": int(result["momentum_reentry_triggered"].fillna(False).sum()),
                "entry_days": int(result["holding"].eq("cash").mul(result["next_holding"].ne("cash")).sum()),
                "annual_return": float(metrics.annual),
                "max_drawdown": float(metrics.max_dd),
                "sharpe": float(metrics.sharpe),
                "total_return": float(metrics.total_return),
                "final_nav": float(result["nav_net"].iloc[-1]),
            }
        )
    return pd.DataFrame(rows).sort_values("threshold").reset_index(drop=True)


def run_forced_stop_loss_scan_from_proxy_turnover(
    args: argparse.Namespace,
    paths: dict[str, Path],
    panel_path: Path,
    thresholds: tuple[float, ...] = DEFAULT_FORCED_STOP_LOSS_SCAN_THRESHOLDS,
    reentry_momentum_strength_days: int | None = None,
) -> pd.DataFrame:
    turnover_path = paths["proxy_turnover"]
    if not turnover_path.exists():
        raise FileNotFoundError(f"Missing proxy turnover history required for forced stop loss scan: {turnover_path}")

    turnover_df = pd.read_csv(turnover_path)
    if "rebalance_date" not in turnover_df.columns:
        raise KeyError(f"Column 'rebalance_date' not found in {turnover_path}.")
    turnover_df["rebalance_date"] = pd.to_datetime(turnover_df["rebalance_date"], errors="coerce")
    turnover_df = turnover_df.dropna(subset=["rebalance_date"]).sort_values("rebalance_date")

    close_df = load_close_df(panel_path, args.index_csv)
    gross = run_signal(close_df)
    summary = run_forced_stop_loss_scan(
        gross_result=gross,
        turnover_df=turnover_df,
        thresholds=thresholds,
        reentry_momentum_strength_days=reentry_momentum_strength_days,
    )
    _atomic_to_csv(summary, paths["forced_stop_scan"], index=False, encoding="utf-8")
    return summary


def rebuild_costed_nav_from_proxy_turnover(
    args: argparse.Namespace,
    paths: dict[str, Path],
    panel_path: Path,
    target_end_date: pd.Timestamp | None = None,
    stop_loss_threshold: float | None = None,
) -> None:
    turnover_path = paths["proxy_turnover"]
    if not turnover_path.exists():
        raise FileNotFoundError(f"Missing proxy turnover history required for costed NAV rebuild: {turnover_path}")

    turnover_df = pd.read_csv(turnover_path)
    if "rebalance_date" not in turnover_df.columns:
        raise KeyError(f"Column 'rebalance_date' not found in {turnover_path}.")
    turnover_df["rebalance_date"] = pd.to_datetime(turnover_df["rebalance_date"], errors="coerce")
    turnover_df = turnover_df.dropna(subset=["rebalance_date"]).sort_values("rebalance_date")

    close_df = load_close_df(panel_path, args.index_csv, max_date=target_end_date)
    gross = run_signal(close_df)
    if stop_loss_threshold is None:
        net = freq_mod.cost_mod.apply_cost_model(gross, turnover_df)
    else:
        net = apply_single_trade_forced_stop_loss(gross, turnover_df, stop_loss_threshold)
    ensure_overlay_pre_cost_return(net)
    _atomic_to_csv(net, args.costed_nav_csv, index_label="date", encoding="utf-8")


def try_extend_costed_nav_without_turnover(
    args: argparse.Namespace,
    panel_path: Path,
    target_end_date: pd.Timestamp,
    proxy_turnover_path: Path | None = None,
) -> bool:
    if not args.index_csv.exists() or not args.costed_nav_csv.exists():
        return False

    costed = pd.read_csv(args.costed_nav_csv)
    if costed.empty or "date" not in costed.columns or "nav_net" not in costed.columns:
        return False
    costed["date"] = pd.to_datetime(costed["date"], errors="coerce")
    costed = costed.dropna(subset=["date"]).sort_values("date")
    if costed.empty:
        return False

    current_costed_end = pd.Timestamp(costed["date"].max())
    close_df = load_close_df(panel_path, args.index_csv, max_date=target_end_date)
    gross = run_signal(close_df).sort_index()
    if gross.empty or current_costed_end not in gross.index:
        return False

    target_end = pd.Timestamp(target_end_date).normalize()
    missing = gross.loc[(gross.index > current_costed_end) & (gross.index <= target_end)].copy()
    if missing.empty:
        return False
    required_cols = {"return", "holding", "next_holding"}
    if required_cols.difference(missing.columns):
        return False

    panel_dates = pd.read_csv(panel_path, usecols=["date"])
    panel_dates["date"] = pd.to_datetime(panel_dates["date"], errors="coerce")
    panel_trading_dates = pd.DatetimeIndex(
        panel_dates.loc[panel_dates["date"] <= target_end, "date"].dropna().drop_duplicates().sort_values()
    )
    missing_rebalances = find_missing_cost_rebalances(
        gross_index=pd.DatetimeIndex(gross.index),
        current_costed_end=current_costed_end,
        target_end_date=target_end,
        proxy_turnover_path=proxy_turnover_path,
        trading_dates=panel_trading_dates,
    )
    if len(missing_rebalances):
        return False

    if EXECUTION_TIMING != freq_mod.EXECUTION_TIMING_CLOSE:
        raise RuntimeError(f"Unsupported EXECUTION_TIMING for tail cost extension: {EXECUTION_TIMING}")
    active = missing["next_holding"].ne("cash")
    prev_active = missing["holding"].ne("cash")

    entry_cost = pd.Series(0.0, index=missing.index, dtype=float)
    entry_cost.loc[active & ~prev_active] = freq_mod.cost_mod.ENTRY_COST
    exit_cost = pd.Series(0.0, index=missing.index, dtype=float)
    exit_cost.loc[~active & prev_active] = freq_mod.cost_mod.EXIT_COST

    missing["entry_exit_cost"] = entry_cost + exit_cost
    missing["rebalance_cost"] = 0.0
    missing["total_cost"] = missing["entry_exit_cost"]
    missing["return_net"] = (1.0 + missing["return"]) * (1.0 - missing["total_cost"]) - 1.0
    missing["overlay_pre_cost_return"] = missing["return"]
    prior_nav = float(costed.loc[costed["date"] == current_costed_end, "nav_net"].iloc[-1])
    missing["nav_net"] = prior_nav * (1.0 + missing["return_net"]).cumprod()

    combined = pd.concat(
        [
            costed.loc[costed["date"] <= current_costed_end].copy(),
            missing.reset_index().rename(columns={"index": "date"}),
        ],
        ignore_index=True,
        sort=False,
    ).sort_values("date")
    combined["date"] = pd.to_datetime(combined["date"], errors="coerce")
    combined = combined.dropna(subset=["date"]).drop_duplicates(subset="date", keep="last")
    _atomic_to_csv(combined, args.costed_nav_csv, index=False, encoding="utf-8")
    return True


def load_proxy_turnover_rebalance_dates(proxy_turnover_path: Path | None) -> pd.DatetimeIndex:
    if proxy_turnover_path is None or not proxy_turnover_path.exists():
        return pd.DatetimeIndex([])
    try:
        turnover = pd.read_csv(proxy_turnover_path, usecols=["rebalance_date"])
    except Exception:
        return pd.DatetimeIndex([])
    dates = pd.to_datetime(turnover["rebalance_date"], errors="coerce").dropna().drop_duplicates().sort_values()
    return pd.DatetimeIndex(dates)


def find_missing_cost_rebalances(
    gross_index: pd.DatetimeIndex,
    current_costed_end: pd.Timestamp,
    target_end_date: pd.Timestamp,
    proxy_turnover_path: Path | None = None,
    trading_dates: pd.DatetimeIndex | None = None,
) -> pd.DatetimeIndex:
    current_end = pd.Timestamp(current_costed_end).normalize()
    target_end = pd.Timestamp(target_end_date).normalize()
    turnover_rebalances = load_proxy_turnover_rebalance_dates(proxy_turnover_path)
    fallback_dates = trading_dates if trading_dates is not None and len(trading_dates) else gross_index
    rebalance_dates = turnover_rebalances if len(turnover_rebalances) else build_biweekly_rebalance_dates(fallback_dates)
    return rebalance_dates[(rebalance_dates > current_end) & (rebalance_dates <= target_end)]


def load_name_map() -> dict[str, str]:
    frame = pd.read_csv(freq_mod.ACTIVE_UNIVERSE, dtype=str)
    return dict(zip(frame["code"].str.zfill(6), frame["name"]))


def build_live_target_members_map(
    caps_by_date: dict[pd.Timestamp, dict[str, float]],
    rebalance_dates: pd.DatetimeIndex,
    name_map: dict[str, str],
    top_n: int = TOP_N,
) -> dict[pd.Timestamp, list[str]]:
    out: dict[pd.Timestamp, list[str]] = {}
    for dt in rebalance_dates:
        cap_map = caps_by_date.get(pd.Timestamp(dt), {})
        ranked = sorted(cap_map.items(), key=lambda x: x[1])
        tradable_members = [
            symbol
            for symbol, _ in ranked
            if is_tradable_name(name_map.get(str(symbol).zfill(6), ""))
        ][:top_n]
        out[pd.Timestamp(dt)] = tradable_members
    return out


def load_member_snapshot(
    snapshot_dates: list[pd.Timestamp],
    max_workers: int,
) -> dict[pd.Timestamp, pd.DataFrame]:
    if not snapshot_dates:
        return {}
    symbols = freq_mod.load_current_universe()
    snapshot_index = pd.DatetimeIndex(sorted(set(pd.Timestamp(dt) for dt in snapshot_dates)))
    _, caps_by_date, _, _ = freq_mod.load_cache_panels(
        symbols=symbols,
        trading_dates=snapshot_index,
        cap_dates=snapshot_index,
        max_workers=max_workers,
        exclude_historical_st_from_caps=False,
    )
    name_map = load_name_map()
    target_members_map = build_live_target_members_map(
        caps_by_date=caps_by_date,
        rebalance_dates=snapshot_index,
        name_map=name_map,
        top_n=TOP_N,
    )

    snapshots: dict[pd.Timestamp, pd.DataFrame] = {}
    for dt in snapshot_index:
        rows = []
        cap_map = caps_by_date.get(pd.Timestamp(dt), {})
        for rank, symbol in enumerate(target_members_map.get(pd.Timestamp(dt), []), start=1):
            rows.append(
                {
                    "rebalance_date": pd.Timestamp(dt),
                    "rank": rank,
                    "symbol": symbol,
                    "name": name_map.get(symbol.zfill(6), ""),
                    "market_cap": float(cap_map.get(symbol, np.nan)),
                    "target_weight": 1.0 / TOP_N,
                }
            )
        snapshots[pd.Timestamp(dt)] = pd.DataFrame(rows)
    return snapshots


def load_member_snapshot_from_proxy_members(
    paths: dict[str, Path],
    snapshot_dates: list[pd.Timestamp],
) -> dict[pd.Timestamp, pd.DataFrame]:
    if not snapshot_dates or not paths["proxy_members"].exists():
        return {}
    try:
        members = pd.read_csv(paths["proxy_members"], dtype={"symbol": str})
    except Exception:
        return {}
    required = {"rebalance_date", "symbol"}
    if not required.issubset(members.columns):
        return {}
    members = members.copy()
    members["rebalance_date"] = pd.to_datetime(members["rebalance_date"], errors="coerce")
    members["symbol"] = members["symbol"].astype(str).str.zfill(6)
    members = members.dropna(subset=["rebalance_date", "symbol"]).sort_values(["rebalance_date", "symbol"])
    if members.empty:
        return {}

    snapshots: dict[pd.Timestamp, pd.DataFrame] = {}
    for dt in sorted(set(pd.Timestamp(dt).normalize() for dt in snapshot_dates)):
        frame = members.loc[members["rebalance_date"].dt.normalize() == dt].copy()
        if frame.empty:
            continue
        if "rank" not in frame.columns:
            frame["rank"] = np.arange(1, len(frame) + 1)
        if "name" not in frame.columns:
            frame["name"] = ""
        if "market_cap" not in frame.columns:
            frame["market_cap"] = np.nan
        if "target_weight" not in frame.columns:
            frame["target_weight"] = 1.0 / TOP_N
        snapshots[dt] = frame[["rebalance_date", "rank", "symbol", "name", "market_cap", "target_weight"]].reset_index(drop=True)
    return snapshots


def fill_member_snapshots_from_proxy_members(
    snapshots: dict[pd.Timestamp, pd.DataFrame],
    paths: dict[str, Path],
    snapshot_dates: list[pd.Timestamp],
) -> dict[pd.Timestamp, pd.DataFrame]:
    missing_dates = [
        pd.Timestamp(dt)
        for dt in snapshot_dates
        if dt is not None
        and (
            pd.Timestamp(dt) not in snapshots
            or snapshots[pd.Timestamp(dt)].empty
            or "symbol" not in snapshots[pd.Timestamp(dt)].columns
        )
    ]
    if not missing_dates:
        return snapshots
    proxy_snapshots = load_member_snapshot_from_proxy_members(paths, missing_dates)
    if not proxy_snapshots:
        return snapshots
    out = dict(snapshots)
    for dt in missing_dates:
        key = pd.Timestamp(dt).normalize()
        if key in proxy_snapshots:
            out[pd.Timestamp(dt)] = proxy_snapshots[key]
    return out


def build_change_table(prev_df: pd.DataFrame | None, curr_df: pd.DataFrame) -> pd.DataFrame:
    prev_df = prev_df.copy() if prev_df is not None else pd.DataFrame(columns=["symbol", "rank", "name"])
    curr_df = curr_df.copy()
    if prev_df.empty and "symbol" not in prev_df.columns:
        prev_df = pd.DataFrame(columns=["symbol", "rank", "name"])
    for label, frame in (("previous", prev_df), ("current", curr_df)):
        if "symbol" not in frame.columns:
            raise KeyError(f"{label} member frame is missing column 'symbol'.")
        duplicated = frame["symbol"].astype(str)[frame["symbol"].astype(str).duplicated()].unique().tolist()
        if duplicated:
            raise ValueError(f"{label} member frame has duplicate symbols: {duplicated[:10]}")

    prev_rank = dict(zip(prev_df["symbol"], prev_df["rank"]))
    curr_rank = dict(zip(curr_df["symbol"], curr_df["rank"]))
    name_map = dict(zip(curr_df["symbol"], curr_df.get("name", "")))
    name_map.update(dict(zip(prev_df["symbol"], prev_df.get("name", ""))))

    rows: list[dict[str, object]] = []
    all_symbols = sorted(set(prev_rank) | set(curr_rank))
    for symbol in all_symbols:
        in_prev = symbol in prev_rank
        in_curr = symbol in curr_rank
        if in_prev and not in_curr:
            action = "exit"
        elif in_curr and not in_prev:
            action = "enter"
        else:
            continue
        rows.append(
            {
                "action": action,
                "symbol": symbol,
                "name": name_map.get(symbol, ""),
                "prev_rank": prev_rank.get(symbol),
                "new_rank": curr_rank.get(symbol),
            }
        )
    if not rows:
        return pd.DataFrame(columns=["action", "symbol", "name", "prev_rank", "new_rank"])
    out = pd.DataFrame(rows)
    action_order = {"enter": 0, "exit": 1}
    out["action_order"] = out["action"].map(action_order)
    out = out.sort_values(["action_order", "new_rank", "prev_rank", "symbol"]).drop(columns="action_order")
    return out.reset_index(drop=True)


def locate_rebalance_dates(
    trading_dates: pd.DatetimeIndex,
) -> tuple[pd.Timestamp, pd.Timestamp | None, pd.Timestamp | None, pd.Timestamp | None]:
    rebalance_dates = build_biweekly_rebalance_dates(trading_dates)
    last_trade_date = pd.Timestamp(trading_dates[-1])
    available = [pd.Timestamp(dt) for dt in rebalance_dates if pd.Timestamp(dt) <= last_trade_date]
    if not available:
        raise ValueError("No rebalance date found up to the latest trade date.")
    latest_rebalance = available[-1]
    prev_rebalance = available[-2] if len(available) >= 2 else None

    effective_rebalance = latest_rebalance

    next_rebalance = None
    future = [pd.Timestamp(dt) for dt in rebalance_dates if pd.Timestamp(dt) > last_trade_date]
    if future:
        next_rebalance = future[0]
    return latest_rebalance, prev_rebalance, next_rebalance, effective_rebalance

def add_capital_columns(members_df: pd.DataFrame, capital: float | None) -> pd.DataFrame:
    out = members_df.copy()
    if capital is not None and not out.empty:
        out["target_notional"] = capital * out["target_weight"]
    return out


def load_cached_static_context(
    paths: dict[str, Path],
    latest_rebalance: pd.Timestamp,
    prev_rebalance: pd.Timestamp | None,
    effective_rebalance: pd.Timestamp | None,
    rebalance_effective_date: pd.Timestamp | None,
    capital: float | None,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame] | None:
    meta_path = paths["cache_static_meta"]
    target_path = paths["cache_static_target"]
    effective_path = paths["cache_static_effective"]
    changes_path = paths["cache_static_changes"]
    if not (meta_path.exists() and target_path.exists() and effective_path.exists() and changes_path.exists()):
        return None
    try:
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
        expected = {
            "cache_version": STATIC_CONTEXT_CACHE_VERSION,
            "member_filter_policy_version": MEMBER_FILTER_POLICY_VERSION,
            "proxy_rebalance_policy_version": PROXY_REBALANCE_POLICY_VERSION,
            "rebalance_phase_anchor_date": REBALANCE_ANCHOR_DATE,
            "latest_rebalance": str(pd.Timestamp(latest_rebalance).date()),
            "prev_rebalance": None if prev_rebalance is None else str(pd.Timestamp(prev_rebalance).date()),
            "effective_rebalance": None if effective_rebalance is None else str(pd.Timestamp(effective_rebalance).date()),
            "rebalance_effective_date": None if rebalance_effective_date is None else str(pd.Timestamp(rebalance_effective_date).date()),
        }
        if any(meta.get(key) != value for key, value in expected.items()):
            return None
        target_members = pd.read_csv(target_path, dtype={"symbol": str})
        effective_members = pd.read_csv(effective_path, dtype={"symbol": str})
        changes_df = pd.read_csv(changes_path, dtype={"symbol": str})
        target_members = add_capital_columns(target_members, capital)
        return target_members, effective_members, changes_df
    except Exception:
        return None


def save_static_context_cache(
    paths: dict[str, Path],
    latest_rebalance: pd.Timestamp,
    prev_rebalance: pd.Timestamp | None,
    effective_rebalance: pd.Timestamp | None,
    rebalance_effective_date: pd.Timestamp | None,
    target_members: pd.DataFrame,
    effective_members: pd.DataFrame,
    changes_df: pd.DataFrame,
) -> None:
    REALTIME_DIR.mkdir(parents=True, exist_ok=True)
    meta = {
        "cache_version": STATIC_CONTEXT_CACHE_VERSION,
        "member_filter_policy_version": MEMBER_FILTER_POLICY_VERSION,
        "proxy_rebalance_policy_version": PROXY_REBALANCE_POLICY_VERSION,
        "rebalance_phase_anchor_date": REBALANCE_ANCHOR_DATE,
        "latest_rebalance": str(pd.Timestamp(latest_rebalance).date()),
        "prev_rebalance": None if prev_rebalance is None else str(pd.Timestamp(prev_rebalance).date()),
        "effective_rebalance": None if effective_rebalance is None else str(pd.Timestamp(effective_rebalance).date()),
        "rebalance_effective_date": None if rebalance_effective_date is None else str(pd.Timestamp(rebalance_effective_date).date()),
    }
    with _cache_write_lock(REALTIME_DIR / f"{paths['cache_static_meta'].stem}.lock"):
        _atomic_to_csv(target_members, paths["cache_static_target"], index=False, encoding="utf-8")
        _atomic_to_csv(effective_members, paths["cache_static_effective"], index=False, encoding="utf-8")
        _atomic_to_csv(changes_df, paths["cache_static_changes"], index=False, encoding="utf-8")
        _atomic_write_json(paths["cache_static_meta"], meta, encoding="utf-8")


def compute_trade_state(current_holding: str, next_holding: str) -> str:
    if current_holding == next_holding:
        return "hold"
    if current_holding == "cash" and next_holding != "cash":
        return "open"
    if current_holding != "cash" and next_holding == "cash":
        return "close"
    return "switch"


def classify_tail_jitter_risk(momentum_gap: float) -> tuple[str, str]:
    abs_gap = abs(float(momentum_gap))
    if abs_gap < TAIL_JITTER_WARNING_GAP:
        return "warning", "gap very close to zero; confirm again near the close"
    if abs_gap < TAIL_JITTER_CAUTION_GAP:
        return "caution", "gap is narrow; close-time recheck is recommended"
    return "normal", ""


def summarize_member_rebalance(changes_df: pd.DataFrame | None) -> dict[str, object]:
    frame = pd.DataFrame() if changes_df is None else pd.DataFrame(changes_df).copy()
    if frame.empty or "action" not in frame.columns:
        return {
            "member_rebalance_state": "none",
            "member_rebalance_required": False,
            "member_enter_count": 0,
            "member_exit_count": 0,
            "member_rebalance_label": "名单不变",
        }
    actions = frame["action"].astype(str)
    enter_count = int(actions.eq("enter").sum())
    exit_count = int(actions.eq("exit").sum())
    has_changes = (enter_count > 0) or (exit_count > 0)
    return {
        "member_rebalance_state": "rebalance" if has_changes else "none",
        "member_rebalance_required": has_changes,
        "member_enter_count": enter_count,
        "member_exit_count": exit_count,
        "member_rebalance_label": (
            f"名单调仓（调入 {enter_count}，调出 {exit_count}）" if has_changes else "名单不变"
        ),
    }


def augment_signal_with_member_rebalance(signal_df: pd.DataFrame, changes_df: pd.DataFrame | None) -> pd.DataFrame:
    out = signal_df.copy()
    member_meta = summarize_member_rebalance(changes_df)
    out["momentum_trade_state"] = out["trade_state"]
    out["member_rebalance_state"] = member_meta["member_rebalance_state"]
    out["member_rebalance_required"] = bool(member_meta["member_rebalance_required"])
    out["member_enter_count"] = int(member_meta["member_enter_count"])
    out["member_exit_count"] = int(member_meta["member_exit_count"])
    out["member_rebalance_label"] = member_meta["member_rebalance_label"]
    return out


def assert_signal_matches_result(signal_df: pd.DataFrame, result: pd.DataFrame) -> None:
    if signal_df.empty or result.empty:
        raise RuntimeError("信号结果为空，拒绝输出实盘信号。")
    signal_row = signal_df.iloc[0]
    result_row = result.iloc[-1]
    for col in ["microcap_mom", "hedge_mom", "momentum_gap"]:
        signal_value = pd.to_numeric(signal_row.get(col), errors="coerce")
        result_value = pd.to_numeric(result_row.get(col), errors="coerce")
        if pd.isna(signal_value) or pd.isna(result_value) or not np.isclose(
            float(signal_value),
            float(result_value),
            rtol=1e-9,
            atol=1e-12,
        ):
            raise RuntimeError(f"信号字段 {col} 与策略结果不一致，拒绝输出实盘信号。")
    if str(signal_row.get("next_holding")) != str(result_row.get("next_holding")):
        raise RuntimeError("信号 next_holding 与策略结果不一致，拒绝输出实盘信号。")
    if "current_holding" in signal_row and str(signal_row.get("current_holding")) != str(result_row.get("holding")):
        raise RuntimeError("信号 current_holding 与策略结果不一致，拒绝输出实盘信号。")


def assert_realtime_meta_is_actionable(meta: dict[str, object]) -> None:
    member_count = int(meta.get("member_count") or 0)
    member_price_count = int(meta.get("member_price_count") or 0)
    if member_count <= 0 or member_price_count != member_count:
        raise RuntimeError(f"实时信号报价覆盖不足: {member_price_count}/{member_count}，拒绝输出实盘信号。")
    bad_symbols = meta.get("member_quote_bad_symbols") or []
    if bad_symbols:
        raise RuntimeError(f"成员股实时报价日期不可验证或早于历史锚点，拒绝输出实盘信号: {bad_symbols[:10]}")
    min_date = str(meta.get("member_quote_trade_date_min") or "").strip()
    max_date = str(meta.get("member_quote_trade_date_max") or "").strip()
    if not min_date or not max_date:
        raise RuntimeError("成员股实时报价缺少逐股票报价交易日，拒绝输出实盘信号。")
    if min_date != max_date:
        raise RuntimeError(f"成员股实时报价交易日不一致: min={min_date}, max={max_date}，拒绝输出实盘信号。")
    member_trade_date_count = int(meta.get("member_quote_trade_date_count") or 0)
    if member_trade_date_count != member_count:
        raise RuntimeError(
            f"Realtime member quotes missing per-symbol trade_date: {member_trade_date_count}/{member_count}; "
            "downgrade to intraday_preview."
        )
    hedge_source = str(meta.get("hedge_quote_source") or "")
    if hedge_source not in ALLOWED_ACTIONABLE_HEDGE_QUOTE_SOURCES:
        raise RuntimeError(f"Realtime hedge quote source is not actionable: {hedge_source}")
    hedge_quote_trade_date = str(meta.get("hedge_quote_trade_date") or "").strip()
    if not hedge_quote_trade_date:
        raise RuntimeError("Realtime hedge quote missing trade_date; downgrade to intraday_preview.")
    quote_trade_date = str(meta.get("quote_trade_date") or "").strip()
    if not quote_trade_date:
        raise RuntimeError("实时信号缺少报价交易日，拒绝输出实盘信号。")
    member_quote_date = pd.Timestamp(quote_trade_date).date()
    hedge_quote_date = pd.Timestamp(hedge_quote_trade_date).date()
    anchor_date = pd.Timestamp(meta["latest_anchor_trade_date"]).date()
    if member_quote_date < anchor_date:
        raise RuntimeError("实时报价日期早于历史锚点，拒绝输出实盘信号。")
    if hedge_quote_date < anchor_date:
        raise RuntimeError("Realtime hedge quote date is earlier than the historical anchor.")
    if hedge_quote_date != member_quote_date:
        raise RuntimeError(
            f"成员股报价交易日与对冲腿报价交易日不一致: "
            f"member={member_quote_date}, hedge={hedge_quote_date}，拒绝输出实盘信号。"
        )


def realtime_meta_is_actionable(meta: dict[str, object]) -> bool:
    try:
        assert_realtime_meta_is_actionable(meta)
        return True
    except Exception:
        return False


def rebuild_realtime_result_from_meta(context: dict[str, object], meta: dict[str, object]) -> pd.DataFrame:
    rt_close_df = apply_realtime_close_to_signal_frame(
        close_df=context["close_df"].copy(),
        latest_trade_date=pd.Timestamp(meta["latest_anchor_trade_date"]),
        snapshot_ts=pd.Timestamp(meta["snapshot_time"]),
        microcap_rt_close=float(meta["microcap_rt_close"]),
        hedge_rt_close=float(meta["hedge_rt_close"]),
        quote_trade_date=meta.get("quote_trade_date", ""),
    )
    return run_signal(rt_close_df)


def enrich_signal_frame(signal_df: pd.DataFrame, result: pd.DataFrame) -> pd.DataFrame:
    out = signal_df.copy()
    last_row = result.iloc[-1]
    current_holding = str(last_row["holding"])
    next_holding = str(last_row["next_holding"])
    out["current_holding"] = current_holding
    out["trade_state"] = compute_trade_state(current_holding, next_holding)
    out["signal_timing"] = "close_confirmed"
    out["official_close_confirmed_signal"] = True
    return out


def build_summary(
    result: pd.DataFrame,
    latest_signal: pd.DataFrame,
    latest_rebalance: pd.Timestamp,
    prev_rebalance: pd.Timestamp | None,
    next_rebalance: pd.Timestamp | None,
    members_df: pd.DataFrame,
    changes_df: pd.DataFrame,
    capital: float | None,
    anchor_freshness: dict[str, object],
) -> dict[str, object]:
    latest_row = latest_signal.iloc[0]
    last_result_row = result.iloc[-1]
    current_holding = last_result_row["holding"]
    next_holding = last_result_row["next_holding"]
    active_next = next_holding != "cash"
    trade_state = compute_trade_state(str(current_holding), str(next_holding))
    member_meta = summarize_member_rebalance(changes_df)
    hedge_notional = capital * FIXED_HEDGE_RATIO if (capital is not None and active_next) else 0.0
    return {
        "strategy": DEFAULT_OUTPUT_PREFIX,
        "version": "1.0",
        "version_note": "Baseline live framework with fixed 1.0x hedge ratio.",
        "core_params": {
            "top_n": TOP_N,
            "exclude_current_st": True,
            "rebalance_schedule": "biweekly",
            "rebalance_weekday_anchor": REBALANCE_WEEKDAY,
            "rebalance_phase_anchor_date": REBALANCE_ANCHOR_DATE,
            "lookback": LOOKBACK,
            "signal_model": "relative_momentum",
            "momentum_gap_entry_threshold": 0.0,
            "momentum_gap_exit_buffer": MOMENTUM_GAP_EXIT_BUFFER,
            "hedge_column": HEDGE_COLUMN,
            "fixed_hedge_ratio": FIXED_HEDGE_RATIO,
            "futures_drag_per_day": FUTURES_DRAG,
            "execution_timing": EXECUTION_TIMING,
            "trade_constraint_mode": TRADE_CONSTRAINT_MODE,
            "research_stack_version": RESEARCH_STACK_VERSION,
            "member_filter_policy_version": MEMBER_FILTER_POLICY_VERSION,
            "realtime_quote_policy_version": REALTIME_QUOTE_POLICY_VERSION,
            "proxy_rebalance_policy_version": PROXY_REBALANCE_POLICY_VERSION,
            "security_meta_version": getattr(freq_mod, "SECURITY_META_VERSION", None),
            "security_master_enabled": True,
        },
        "latest_trade_date": str(result.index[-1].date()),
        "latest_rebalance_date": str(latest_rebalance.date()),
        "previous_rebalance_date": None if prev_rebalance is None else str(prev_rebalance.date()),
        "next_rebalance_date": None if next_rebalance is None else str(next_rebalance.date()),
        "history_anchor": anchor_freshness,
        "latest_signal": {
            "signal_label": latest_row["signal_label"],
            "current_holding": current_holding,
            "next_holding": next_holding,
            "trade_state": trade_state,
            "momentum_trade_state": trade_state,
            "member_rebalance_state": member_meta["member_rebalance_state"],
            "member_rebalance_required": bool(member_meta["member_rebalance_required"]),
            "member_enter_count": int(member_meta["member_enter_count"]),
            "member_exit_count": int(member_meta["member_exit_count"]),
            "member_rebalance_label": member_meta["member_rebalance_label"],
            "microcap_mom": float(latest_row["microcap_mom"]),
            "hedge_mom": float(latest_row["hedge_mom"]),
            "momentum_gap": float(latest_row["momentum_gap"]),
            "microcap_close": float(latest_row["microcap_close"]),
            "hedge_close": float(latest_row["hedge_close"]),
        },
        "target_members": {
            "count": int(len(members_df)),
            "enter_count": int((changes_df["action"] == "enter").sum()) if len(changes_df) else 0,
            "exit_count": int((changes_df["action"] == "exit").sum()) if len(changes_df) else 0,
            "equal_weight": 1.0 / TOP_N,
        },
        "capital_plan": {
            "gross_stock_capital": capital,
            "per_stock_target_notional": None if capital is None else capital / TOP_N,
            "hedge_notional": hedge_notional,
        },
    }


def build_base_context(args: argparse.Namespace, include_members: bool = True) -> dict[str, object]:
    paths = build_output_paths(args.output_prefix)
    resolved_panel_path, target_end_date = build_refreshed_panel_shadow(args, paths)
    ensure_strategy_files(args, paths, resolved_panel_path, target_end_date)

    close_df = load_close_df(resolved_panel_path, args.index_csv, max_date=target_end_date)
    result = run_signal(close_df)
    latest_signal = enrich_signal_frame(hedge_mod.build_latest_signal(result), result)
    assert_proxy_tail_is_actionable(args.index_csv, target_end_date)
    assert_signal_matches_result(latest_signal, result)

    latest_rebalance, prev_rebalance, next_rebalance, effective_rebalance = locate_rebalance_dates(close_df.index)
    rebalance_effective_date = latest_rebalance
    target_members = pd.DataFrame()
    effective_members = pd.DataFrame()
    changes_df = pd.DataFrame(columns=["action", "symbol", "name", "prev_rank", "new_rank"])

    if include_members:
        cached_static = load_cached_static_context(
            paths=paths,
            latest_rebalance=latest_rebalance,
            prev_rebalance=prev_rebalance,
            effective_rebalance=effective_rebalance,
            rebalance_effective_date=rebalance_effective_date,
            capital=args.capital,
        )
        if cached_static is None:
            snapshot_dates = [dt for dt in [latest_rebalance, prev_rebalance, effective_rebalance] if dt is not None]
            snapshots = load_member_snapshot(snapshot_dates=snapshot_dates, max_workers=args.max_workers)
            snapshots = fill_member_snapshots_from_proxy_members(snapshots, paths, snapshot_dates)
            target_members = snapshots[pd.Timestamp(latest_rebalance)].copy()
            prev_members = snapshots.get(pd.Timestamp(prev_rebalance)) if prev_rebalance is not None else None
            effective_members = snapshots.get(pd.Timestamp(effective_rebalance)) if effective_rebalance is not None else target_members.copy()
            target_members = add_capital_columns(target_members, capital=args.capital)
            if not target_members.empty:
                target_members["signal_date"] = pd.Timestamp(latest_rebalance).date()
                target_members["effective_date"] = None if rebalance_effective_date is None else pd.Timestamp(rebalance_effective_date).date()
            changes_df = build_change_table(prev_members, target_members)
            if not changes_df.empty:
                changes_df["signal_date"] = pd.Timestamp(latest_rebalance).date()
                changes_df["effective_date"] = None if rebalance_effective_date is None else pd.Timestamp(rebalance_effective_date).date()
            save_static_context_cache(
                paths=paths,
                latest_rebalance=latest_rebalance,
                prev_rebalance=prev_rebalance,
                effective_rebalance=effective_rebalance,
                rebalance_effective_date=rebalance_effective_date,
                target_members=target_members.drop(columns=["target_notional"], errors="ignore"),
                effective_members=effective_members,
                changes_df=changes_df,
            )
        else:
            target_members, effective_members, changes_df = cached_static

    if not include_members and changes_df.empty:
        snapshot_dates = [dt for dt in [latest_rebalance, prev_rebalance, effective_rebalance] if dt is not None]
        snapshots = load_member_snapshot(snapshot_dates=snapshot_dates, max_workers=args.max_workers)
        snapshots = fill_member_snapshots_from_proxy_members(snapshots, paths, snapshot_dates)
        target_members = snapshots[pd.Timestamp(latest_rebalance)].copy()
        prev_members = snapshots.get(pd.Timestamp(prev_rebalance)) if prev_rebalance is not None else None
        effective_members = snapshots.get(pd.Timestamp(effective_rebalance)) if effective_rebalance is not None else target_members.copy()
        if not target_members.empty:
            target_members["signal_date"] = pd.Timestamp(latest_rebalance).date()
            target_members["effective_date"] = None if rebalance_effective_date is None else pd.Timestamp(rebalance_effective_date).date()
        changes_df = build_change_table(prev_members, target_members)
        if not changes_df.empty:
            changes_df["signal_date"] = pd.Timestamp(latest_rebalance).date()
            changes_df["effective_date"] = None if rebalance_effective_date is None else pd.Timestamp(rebalance_effective_date).date()

    latest_signal = augment_signal_with_member_rebalance(latest_signal, changes_df)
    summary = build_summary(
        result=result,
        latest_signal=latest_signal,
        latest_rebalance=latest_rebalance,
        prev_rebalance=prev_rebalance,
        next_rebalance=next_rebalance,
        members_df=target_members,
        changes_df=changes_df,
        capital=args.capital,
        anchor_freshness=assess_history_anchor_freshness(
            latest_trade_date=pd.Timestamp(result.index[-1]),
            max_stale_days=args.max_stale_anchor_days,
            trading_dates=pd.DatetimeIndex(close_df.index),
        ),
    )
    return {
        "include_members": include_members,
        "paths": paths,
        "resolved_panel_path": resolved_panel_path,
        "target_end_date": pd.Timestamp(target_end_date),
        "close_df": close_df,
        "result": result,
        "latest_signal": latest_signal,
        "latest_rebalance": latest_rebalance,
        "rebalance_effective_date": rebalance_effective_date,
        "prev_rebalance": prev_rebalance,
        "next_rebalance": next_rebalance,
        "effective_rebalance": effective_rebalance,
        "target_members": target_members,
        "effective_members": effective_members,
        "changes_df": changes_df,
        "summary": summary,
        "anchor_freshness": summary["history_anchor"],
    }


def save_base_outputs(context: dict[str, object]) -> None:
    paths = context["paths"]
    result = context["result"]
    latest_signal = context["latest_signal"]
    target_members = context["target_members"]
    changes_df = context["changes_df"]
    summary = context["summary"]
    include_members = bool(context.get("include_members", True))

    _atomic_to_csv(result, paths["nav"], index_label="date", encoding="utf-8")
    _atomic_to_csv(latest_signal, paths["signal"], index=False, encoding="utf-8")
    if include_members:
        _atomic_to_csv(target_members, paths["members"], index=False, encoding="utf-8")
        _atomic_to_csv(changes_df, paths["changes"], index=False, encoding="utf-8")
        _atomic_write_json(paths["summary"], summary, encoding="utf-8")


def print_console_summary(summary: dict[str, object]) -> None:
    latest_signal = summary["latest_signal"]
    capital_plan = summary["capital_plan"]
    target_members = summary["target_members"]
    print(f"最新交易日: {summary['latest_trade_date']}")
    print(f"最新调仓日: {summary['latest_rebalance_date']}")
    print(f"下一调仓日: {summary['next_rebalance_date']}")
    print(f"当前信号: {latest_signal['signal_label']} -> 下期持仓 {latest_signal['next_holding']}")
    print(f"交易动作: {latest_signal['trade_state']}")
    print(
        "16日动量: microcap={:.4%}, hedge={:.4%}, gap={:.4%}".format(
            latest_signal["microcap_mom"],
            latest_signal["hedge_mom"],
            latest_signal["momentum_gap"],
        )
    )
    print(
        f"目标成分股: {target_members['count']} 只, 本次进入 {target_members['enter_count']} 只, "
        f"剔除 {target_members['exit_count']} 只"
    )
    if capital_plan["gross_stock_capital"] is not None:
        print(
            f"股票资金: {capital_plan['gross_stock_capital']:.2f}, "
            f"单票目标资金: {capital_plan['per_stock_target_notional']:.2f}, "
            f"对冲名义: {capital_plan['hedge_notional']:.2f}"
        )


def ensure_realtime_anchor_is_fresh(context: dict[str, object], args: argparse.Namespace) -> None:
    anchor_freshness = context.get("anchor_freshness", {})
    if not anchor_freshness:
        return
    if bool(anchor_freshness.get("is_stale")) and not bool(args.allow_stale_realtime):
        raise RuntimeError(format_anchor_stale_message(anchor_freshness))


def ensure_closed_signal_anchor_is_fresh(context: dict[str, object]) -> None:
    anchor_freshness = context.get("anchor_freshness", {})
    if not anchor_freshness:
        return
    if bool(anchor_freshness.get("is_stale")):
        latest_trade_date = anchor_freshness.get("latest_trade_date")
        current_date = anchor_freshness.get("current_date")
        stale_days = anchor_freshness.get("stale_calendar_days")
        max_days = anchor_freshness.get("max_stale_anchor_days")
        raise RuntimeError(
            "Closed signal is stale: latest anchored trade date is "
            f"{latest_trade_date}, current date is {current_date}, lag={stale_days} calendar days "
            f"(limit={max_days}). Refresh the local baseline files before using the `信号` command."
        )


def _parse_cn_num(text: str) -> int | float | None:
    text = text.strip()
    if not text:
        return None
    if text.isdigit():
        return int(text)
    if text in CN_NUM:
        return CN_NUM[text]
    if "十" in text:
        parts = text.split("十")
        tens = CN_NUM.get(parts[0], 1) if parts[0] else 1
        ones = CN_NUM.get(parts[1], 0) if len(parts) > 1 and parts[1] else 0
        return tens * 10 + ones
    return None


def _strip_query_prefix(text: str) -> str:
    out = re.sub(r"^(查询|看看|看下|看一下|给我看一下|给我看看)", "", text.strip())
    out = re.sub(r"^(表现|净值曲线|收益|回撤|年化|夏普)", "", out)
    out = re.sub(r"^[:：\s]+", "", out)
    return out.strip()


def parse_date_range(text: str, now: pd.Timestamp | None = None) -> tuple[pd.Timestamp | None, pd.Timestamp | None, str]:
    now = _cn_local_day(now)
    raw = text.strip()
    text = re.sub(r"\s+", "", raw)
    text = text.replace("从", "")
    text = _strip_query_prefix(text)
    if not text or text in {"全部", "全样本", "历史全部", "历史", "全周期"}:
        return None, None, "全样本"

    m = re.search(
        r"(\d{4})[-年/.](\d{1,2})[-月/.](\d{1,2})" + DAY_SUFFIX +
        r"[到至—\-~]+" +
        r"(\d{4})[-年/.](\d{1,2})[-月/.](\d{1,2})" + DAY_SUFFIX,
        text,
    )
    if m:
        start = pd.Timestamp(f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}")
        end = pd.Timestamp(f"{m.group(4)}-{int(m.group(5)):02d}-{int(m.group(6)):02d}")
        return start, end, f"{start:%Y-%m-%d} to {end:%Y-%m-%d}"

    m = re.search(r"(\d{4})[-年/.](\d{1,2})[-月/.](\d{1,2})" + DAY_SUFFIX + r"至今", text)
    if m:
        start = pd.Timestamp(f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}")
        return start, now, f"{start:%Y-%m-%d} to now"

    m = re.search(r"(\d{1,2})[-月/.](\d{1,2})" + DAY_SUFFIX + r"至今", text)
    if m:
        year = now.year
        start = pd.Timestamp(f"{year}-{int(m.group(1)):02d}-{int(m.group(2)):02d}")
        if start > now:
            start = start.replace(year=year - 1)
        return start, now, f"{start:%Y-%m-%d} to now"

    m = re.search(r"(\d{4})[-年/.]?(\d{1,2})[-月]?\s*至今", text)
    if m:
        start = pd.Timestamp(f"{m.group(1)}-{int(m.group(2)):02d}-01")
        return start, now, f"{start:%Y-%m} to now"

    m = re.search(r"(\d{4})\s*年?\s*至今", text)
    if m:
        start = pd.Timestamp(f"{m.group(1)}-01-01")
        return start, now, f"{start:%Y} to now"

    m = re.search(r"(\d{4})[-年/.](\d{1,2})[-月]?[到至—\-~]+(\d{4})[-年/.](\d{1,2})", text)
    if m:
        start = pd.Timestamp(f"{m.group(1)}-{int(m.group(2)):02d}-01")
        end = pd.Timestamp(f"{m.group(3)}-{int(m.group(4)):02d}-01") + pd.offsets.MonthEnd(0)
        return start, end, f"{start:%Y-%m} to {end:%Y-%m}"

    m = re.search(r"(\d{4})\s*年?\s*[到至—\-~]+\s*(\d{4})\s*年?", text)
    if m:
        start = pd.Timestamp(f"{m.group(1)}-01-01")
        end = pd.Timestamp(f"{m.group(2)}-12-31")
        return start, end, f"{m.group(1)} to {m.group(2)}"

    m = re.search(r"(?:最近|过去|近)\s*([一二两三四五六七八九十\d半]+)\s*个?\s*年", text)
    if m:
        n = _parse_cn_num(m.group(1))
        if n is not None:
            if isinstance(n, float):
                start = now - pd.DateOffset(months=int(n * 12))
            else:
                start = now - pd.DateOffset(years=int(n))
            return start, now, f"last_{m.group(1)}_years"

    m = re.fullmatch(r"([一二两三四五六七八九十\d半]+)\s*个?\s*年", text)
    if m:
        n = _parse_cn_num(m.group(1))
        if n is not None:
            if isinstance(n, float):
                start = now - pd.DateOffset(months=int(n * 12))
            else:
                start = now - pd.DateOffset(years=int(n))
            return start, now, f"last_{m.group(1)}_years"

    m = re.search(r"(?:最近|过去|近)\s*([一二两三四五六七八九十\d半]+)\s*个?\s*月", text)
    if m:
        n = _parse_cn_num(m.group(1))
        if n is not None:
            months = int(n if n >= 1 else 1)
            start = now - pd.DateOffset(months=months)
            return start, now, f"last_{m.group(1)}_months"

    m = re.fullmatch(r"([一二两三四五六七八九十\d半]+)\s*个?\s*月", text)
    if m:
        n = _parse_cn_num(m.group(1))
        if n is not None:
            months = int(n if n >= 1 else 1)
            start = now - pd.DateOffset(months=months)
            return start, now, f"last_{m.group(1)}_months"

    if "最近几年" in text or "近几年" in text or "过去几年" in text:
        start = now - pd.DateOffset(years=3)
        return start, now, "last_3_years_default"

    if "今年" in text:
        start = pd.Timestamp(f"{now.year}-01-01")
        return start, now, f"{now.year}"

    if "去年" in text:
        year = now.year - 1
        start = pd.Timestamp(f"{year}-01-01")
        end = pd.Timestamp(f"{year}-12-31")
        return start, end, f"{year}"

    if "前年" in text:
        year = now.year - 2
        start = pd.Timestamp(f"{year}-01-01")
        end = pd.Timestamp(f"{year}-12-31")
        return start, end, f"{year}"

    m = re.search(r"(\d{4})[-年/.](\d{1,2})\s*月?份?", text)
    if m:
        year = int(m.group(1))
        month = int(m.group(2))
        if 1 <= month <= 12:
            start = pd.Timestamp(f"{year}-{month:02d}-01")
            end = start + pd.offsets.MonthEnd(0)
            return start, end, f"{year}-{month:02d}"

    m = re.search(r"(\d{4})\s*年?\s*全?年?", text)
    if m:
        year = int(m.group(1))
        if 2000 <= year <= 2099:
            start = pd.Timestamp(f"{year}-01-01")
            end = pd.Timestamp(f"{year}-12-31")
            return start, end, f"{year}"

    return None, None, "全样本"


def normalize_query_text(query: str) -> str:
    text = str(query or "").strip()
    if not text:
        return ""
    if text.startswith("成分股名单"):
        return "成分股" + text[len("成分股名单") :]
    for prefix in ("净值表现", "净值图"):
        if text.startswith(prefix):
            return "表现" + text[len(prefix) :]
    return text


def classify_query_kind(query: str) -> str:
    text = normalize_query_text(query)
    if text == "信号":
        return "signal"
    if text == "实时信号":
        return "realtime_signal"
    if text == "成分股":
        return "members"
    if text == "进出名单":
        return "changes"
    if text == "实时进出名单":
        return "realtime_changes"
    if PERFORMANCE_PATTERN.search(text):
        return "performance"
    return "default"


def load_performance_source(
    costed_nav_csv: Path,
    fallback_result: pd.DataFrame,
    index_csv: Path,
) -> tuple[pd.DataFrame, str, str, str]:
    effective_start = None
    if index_csv.exists():
        proxy = pd.read_csv(index_csv)
        proxy["date"] = pd.to_datetime(proxy["date"])
        effective_start = infer_proxy_effective_start(proxy)

    if costed_nav_csv.exists():
        perf = pd.read_csv(costed_nav_csv)
        perf["date"] = pd.to_datetime(perf["date"])
        perf = perf.set_index("date").sort_index()
        if effective_start is not None:
            perf = perf.loc[perf.index >= effective_start].copy()
        if "return_net" in perf.columns and "nav_net" in perf.columns:
            return perf, "return_net", "nav_net", "costed"
        return perf, "return", "nav", "gross"

    perf = fallback_result.copy()
    if effective_start is not None:
        perf = perf.loc[perf.index >= effective_start].copy()
    return perf, "return", "nav", "gross_fallback"


def calc_max_drawdown_from_returns(returns: pd.Series) -> float:
    nav = (1.0 + returns.fillna(0.0)).cumprod()
    drawdown = nav / nav.cummax() - 1.0
    return float(drawdown.min())


def _normalise_dated_frame(frame: pd.DataFrame, label: str) -> pd.DataFrame:
    out = frame.copy()
    if "date" in out.columns:
        out["date"] = pd.to_datetime(out["date"], errors="coerce")
        out = out.dropna(subset=["date"]).set_index("date")
    elif isinstance(out.index, pd.DatetimeIndex):
        out.index = pd.to_datetime(out.index)
    else:
        raise ValueError(f"{label} requires a date column or DatetimeIndex.")
    out = out.sort_index()
    if out.index.duplicated().any():
        dupes = out.index[out.index.duplicated()].strftime("%Y-%m-%d").unique().tolist()
        raise ValueError(f"{label} has duplicate dates: {dupes[:5]}")
    return out


def assert_no_historical_rewrite(
    previous: pd.DataFrame,
    candidate: pd.DataFrame,
    key_columns: list[str],
    allowed_tail_rows: int,
    label: str,
    audit_path: Path | None = None,
    numeric_tolerance: float = 1e-10,
) -> None:
    prev = _normalise_dated_frame(previous, f"{label} previous")
    cand = _normalise_dated_frame(candidate, f"{label} candidate")
    common = prev.index.intersection(cand.index).sort_values()
    if len(common) <= int(allowed_tail_rows):
        return
    frozen_common = common[:-int(allowed_tail_rows)] if allowed_tail_rows > 0 else common
    changes: list[dict[str, object]] = []
    for col in key_columns:
        if col not in prev.columns or col not in cand.columns:
            continue
        left = prev.loc[frozen_common, col]
        right = cand.loc[frozen_common, col]
        left_num = pd.to_numeric(left, errors="coerce")
        right_num = pd.to_numeric(right, errors="coerce")
        numeric_like = left_num.notna().any() or right_num.notna().any()
        if numeric_like:
            changed = (left_num - right_num).abs().gt(float(numeric_tolerance))
            changed = changed | (left_num.isna() ^ right_num.isna())
        else:
            changed = left.astype(str).ne(right.astype(str))
        for dt in frozen_common[changed.fillna(False)]:
            changes.append(
                {
                    "date": str(pd.Timestamp(dt).date()),
                    "column": col,
                    "previous": prev.at[dt, col],
                    "candidate": cand.at[dt, col],
                }
            )
    if not changes:
        return
    diff_df = pd.DataFrame(changes)
    if audit_path is not None:
        audit_path.parent.mkdir(parents=True, exist_ok=True)
        _atomic_to_csv(diff_df, audit_path, index=False, encoding="utf-8-sig")
    examples = ", ".join(f"{row['date']}:{row['column']}" for row in changes[:5])
    raise RuntimeError(
        f"{label} historical rewrite detected on frozen dates; examples: {examples}. "
        "Refusing to publish query/chart output until the data lineage is audited."
    )


def validate_performance_frame(perf_df: pd.DataFrame, ret_col: str, nav_col: str, source_label: str) -> pd.DataFrame:
    data = _normalise_dated_frame(perf_df, f"{source_label} performance input")
    missing = [col for col in [ret_col, nav_col] if col not in data.columns]
    if missing:
        raise ValueError(f"{source_label} performance input missing columns: {missing}")
    data[ret_col] = pd.to_numeric(data[ret_col], errors="coerce")
    data[nav_col] = pd.to_numeric(data[nav_col], errors="coerce")
    if data[ret_col].isna().all():
        raise ValueError(f"{source_label} performance input has no numeric {ret_col}.")
    if data[nav_col].isna().all():
        raise ValueError(f"{source_label} performance input has no numeric {nav_col}.")
    return data


def build_performance_outputs(
    perf_df: pd.DataFrame,
    ret_col: str,
    nav_col: str,
    source_label: str,
    query_text: str,
    paths: dict[str, Path],
) -> dict[str, object]:
    data = validate_performance_frame(perf_df, ret_col=ret_col, nav_col=nav_col, source_label=source_label)
    source_start = pd.Timestamp(data.index.min())
    source_end = pd.Timestamp(data.index.max())
    start_date, end_date, period_label = parse_date_range(query_text, now=source_end)
    if start_date is None:
        start_date = source_start
    if end_date is None:
        end_date = source_end

    data = data.loc[(data.index >= start_date) & (data.index <= end_date)].copy()
    if data.empty:
        raise ValueError(f"在 {start_date:%Y-%m-%d} 到 {end_date:%Y-%m-%d} 之间没有表现数据。")

    returns = data[ret_col].fillna(0.0)
    metrics = hedge_mod.calc_metrics(returns)
    rebased_nav = (1.0 + returns).cumprod()
    data["nav_rebased"] = rebased_nav

    yearly_rows: list[dict[str, object]] = []
    for year, part in data.groupby(data.index.year):
        part_returns = part[ret_col].fillna(0.0)
        part_metrics = hedge_mod.calc_metrics(part_returns)
        yearly_rows.append(
            {
                "year": str(year),
                "start_date": str(part.index.min().date()),
                "end_date": str(part.index.max().date()),
                "days": int(len(part)),
                "return_pct": float((1.0 + part_returns).prod() - 1.0) * 100.0,
                "max_drawdown_pct": calc_max_drawdown_from_returns(part_returns) * 100.0,
                "sharpe": float(part_metrics.sharpe),
                "annual_pct": float(part_metrics.annual) * 100.0,
            }
        )
    yearly_df = pd.DataFrame(yearly_rows)

    summary_df = pd.DataFrame(
        [
            {
                "period_label": period_label,
                "source": source_label,
                "start_date": str(data.index.min().date()),
                "end_date": str(data.index.max().date()),
                "days": int(len(data)),
                "final_nav": float(rebased_nav.iloc[-1]),
                "total_return_pct": float(rebased_nav.iloc[-1] - 1.0) * 100.0,
                "annual_pct": float(metrics.annual) * 100.0,
                "max_drawdown_pct": float(metrics.max_dd) * 100.0,
                "sharpe": float(metrics.sharpe),
                "vol_pct": float(metrics.vol) * 100.0,
            }
        ]
    )

    _atomic_to_csv(data.reset_index(), paths["performance_nav"], index=False, encoding="utf-8")
    _atomic_to_csv(summary_df, paths["performance_summary"], index=False, encoding="utf-8")
    _atomic_to_csv(yearly_df, paths["performance_yearly"], index=False, encoding="utf-8")

    fig, ax = plt.subplots(figsize=(12, 6))
    ax.plot(data.index, data["nav_rebased"], linewidth=2.0, color="#1f4e79")
    title_label = period_label if str(period_label).isascii() else f"{data.index.min():%Y-%m-%d} to {data.index.max():%Y-%m-%d}"
    ax.set_title(f"{STRATEGY_TITLE} ({title_label})")
    ax.set_ylabel("Rebased NAV")
    ax.grid(True, alpha=0.25)
    fig.tight_layout()
    fig.savefig(paths["performance_chart"], dpi=160)
    plt.close(fig)

    payload = {
        "period_label": period_label,
        "source": source_label,
        "query_text": query_text,
        "source_manifest": {
            "source_start_date": str(source_start.date()),
            "source_end_date": str(source_end.date()),
            "source_rows": int(len(perf_df)),
            "return_column": ret_col,
            "nav_column": nav_col,
            "duplicate_date_count": 0,
            "window_start_date": str(data.index.min().date()),
            "window_end_date": str(data.index.max().date()),
            "window_rows": int(len(data)),
        },
        "start_date": str(data.index.min().date()),
        "end_date": str(data.index.max().date()),
        "summary": summary_df.iloc[0].to_dict(),
        "yearly": yearly_rows,
        "files": {
            "summary_csv": str(paths["performance_summary"]),
            "yearly_csv": str(paths["performance_yearly"]),
            "nav_csv": str(paths["performance_nav"]),
            "chart_png": str(paths["performance_chart"]),
        },
    }
    _atomic_write_json(paths["performance_json"], payload, encoding="utf-8")
    manifest_path = paths.get(
        "performance_manifest",
        paths["performance_json"].with_name(paths["performance_json"].stem + "_manifest.json"),
    )
    _atomic_write_json(manifest_path, payload["source_manifest"], encoding="utf-8")
    return payload


def refresh_history_anchor(args: argparse.Namespace, paths: dict[str, Path]) -> tuple[Path, pd.Timestamp]:
    return build_refreshed_panel_shadow(args, paths)


def ensure_strategy_nav_fresh(
    args: argparse.Namespace,
    paths: dict[str, Path],
    panel_path: Path,
    target_end_date: pd.Timestamp,
) -> None:
    ensure_strategy_files(args, paths, panel_path, target_end_date)


def reusable_cached_proxy_end_for_realtime(
    args: argparse.Namespace,
    paths: dict[str, Path],
    target_end_date: pd.Timestamp,
) -> pd.Timestamp | None:
    current_index_end = read_csv_last_date(args.index_csv)
    current_costed_end = read_csv_last_date(args.costed_nav_csv)
    if current_index_end is None or current_costed_end is None:
        return None
    if not args.index_csv.exists() or not args.costed_nav_csv.exists() or not paths["proxy_turnover"].exists():
        return None
    if paths["proxy_meta"].exists():
        try:
            meta_matches = proxy_meta_matches_execution_model(json.loads(paths["proxy_meta"].read_text(encoding="utf-8")))
        except Exception:
            meta_matches = False
        if not meta_matches:
            return None
    cache_end = min(pd.Timestamp(current_index_end).normalize(), pd.Timestamp(current_costed_end).normalize())
    cache_end = min(cache_end, pd.Timestamp(target_end_date).normalize())
    freshness = assess_history_anchor_freshness(cache_end, args.max_stale_anchor_days)
    if bool(freshness.get("is_stale")) and not bool(args.allow_stale_realtime):
        return None
    return cache_end


def cached_universe_symbols_available() -> bool:
    try:
        return bool(freq_mod.load_current_universe())
    except Exception:
        return False


def build_realtime_context_from_cached_proxy(
    args: argparse.Namespace,
    paths: dict[str, Path],
    panel_path: Path,
    target_end_date: pd.Timestamp,
    reason: str,
) -> dict[str, object] | None:
    cache_end = reusable_cached_proxy_end_for_realtime(args, paths, target_end_date)
    if cache_end is None:
        return None
    close_df = load_close_df(panel_path, args.index_csv, max_date=cache_end)
    context = build_base_signal_context(args, paths, panel_path, cache_end, close_df)
    context["fallback_warning"] = (
        f"realtime base used cached proxy through {cache_end.date()} because {reason}"
    )
    return context


def ensure_base_signal_fresh(
    args: argparse.Namespace,
    paths: dict[str, Path],
    panel_path: Path,
    target_end_date: pd.Timestamp,
) -> dict[str, object]:
    ensure_strategy_nav_fresh(args, paths, panel_path, target_end_date)
    close_df = load_close_df(panel_path, args.index_csv, max_date=target_end_date)
    return build_base_signal_context(args, paths, panel_path, target_end_date, close_df)


def build_base_signal_context(
    args: argparse.Namespace,
    paths: dict[str, Path],
    panel_path: Path,
    target_end_date: pd.Timestamp,
    close_df: pd.DataFrame,
) -> dict[str, object]:
    result = run_signal(close_df)
    latest_signal = enrich_signal_frame(hedge_mod.build_latest_signal(result), result)
    latest_rebalance, prev_rebalance, next_rebalance, effective_rebalance = locate_rebalance_dates(close_df.index)
    rebalance_effective_date = latest_rebalance
    anchor_freshness = assess_history_anchor_freshness(
        latest_trade_date=pd.Timestamp(result.index[-1]),
        max_stale_days=args.max_stale_anchor_days,
        trading_dates=pd.DatetimeIndex(close_df.index),
    )
    return {
        "paths": paths,
        "resolved_panel_path": panel_path,
        "target_end_date": pd.Timestamp(target_end_date),
        "close_df": close_df,
        "result": result,
        "latest_signal": latest_signal,
        "latest_rebalance": latest_rebalance,
        "rebalance_effective_date": rebalance_effective_date,
        "prev_rebalance": prev_rebalance,
        "next_rebalance": next_rebalance,
        "effective_rebalance": effective_rebalance,
        "anchor_freshness": anchor_freshness,
    }


def ensure_realtime_query_base_context(
    args: argparse.Namespace,
    paths: dict[str, Path],
    panel_path: Path,
    target_end_date: pd.Timestamp,
) -> dict[str, object]:
    current_index_end = read_csv_last_date(args.index_csv)
    if (
        current_index_end is not None
        and pd.Timestamp(current_index_end).normalize() < pd.Timestamp(target_end_date).normalize()
        and not cached_universe_symbols_available()
    ):
        cached_context = build_realtime_context_from_cached_proxy(
            args,
            paths,
            panel_path,
            target_end_date,
            "cached price/share universe is unavailable for recent proxy extension",
        )
        if cached_context is not None:
            return cached_context
    try:
        ensure_strategy_nav_fresh(args, paths, panel_path, target_end_date)
    except RuntimeError as exc:
        cached_context = build_realtime_context_from_cached_proxy(
            args,
            paths,
            panel_path,
            target_end_date,
            f"recent proxy refresh failed: {exc}",
        )
        if cached_context is not None:
            return cached_context
        raise
    if not args.index_csv.exists():
        raise FileNotFoundError(f"Missing proxy index required for realtime query: {args.index_csv}")
    close_df = load_close_df(panel_path, args.index_csv, max_date=target_end_date)
    return build_base_signal_context(args, paths, panel_path, target_end_date, close_df)


def ensure_static_members_fresh(
    args: argparse.Namespace,
    paths: dict[str, Path],
    panel_path: Path,
    target_end_date: pd.Timestamp,
    base_context: dict[str, object],
) -> dict[str, object]:
    context = dict(base_context)
    latest_rebalance = pd.Timestamp(context["latest_rebalance"])
    prev_rebalance = context.get("prev_rebalance")
    effective_rebalance = context.get("effective_rebalance")
    rebalance_effective_date = context.get("rebalance_effective_date")
    cached_static = load_cached_static_context(
        paths=paths,
        latest_rebalance=latest_rebalance,
        prev_rebalance=prev_rebalance,
        effective_rebalance=effective_rebalance,
        rebalance_effective_date=rebalance_effective_date,
        capital=args.capital,
    )
    if cached_static is None:
        snapshot_dates = [dt for dt in [latest_rebalance, prev_rebalance, effective_rebalance] if dt is not None]
        snapshots = load_member_snapshot(snapshot_dates=snapshot_dates, max_workers=args.max_workers)
        snapshots = fill_member_snapshots_from_proxy_members(snapshots, paths, snapshot_dates)
        target_members = snapshots[pd.Timestamp(latest_rebalance)].copy()
        prev_members = snapshots.get(pd.Timestamp(prev_rebalance)) if prev_rebalance is not None else None
        effective_members = snapshots.get(pd.Timestamp(effective_rebalance)) if effective_rebalance is not None else target_members.copy()
        target_members = add_capital_columns(target_members, capital=args.capital)
        if not target_members.empty:
            target_members["signal_date"] = pd.Timestamp(latest_rebalance).date()
            target_members["effective_date"] = None if rebalance_effective_date is None else pd.Timestamp(rebalance_effective_date).date()
        changes_df = build_change_table(prev_members, target_members)
        if not changes_df.empty:
            changes_df["signal_date"] = pd.Timestamp(latest_rebalance).date()
            changes_df["effective_date"] = None if rebalance_effective_date is None else pd.Timestamp(rebalance_effective_date).date()
        save_static_context_cache(
            paths=paths,
            latest_rebalance=latest_rebalance,
            prev_rebalance=prev_rebalance,
            effective_rebalance=effective_rebalance,
            rebalance_effective_date=rebalance_effective_date,
            target_members=target_members.drop(columns=["target_notional"], errors="ignore"),
            effective_members=effective_members,
            changes_df=changes_df,
        )
    else:
        target_members, effective_members, changes_df = cached_static
    context["target_members"] = target_members
    context["effective_members"] = effective_members
    context["changes_df"] = changes_df
    context["latest_signal"] = augment_signal_with_member_rebalance(context["latest_signal"], changes_df)
    return context


def handle_performance_query_fast(
    args: argparse.Namespace,
    paths: dict[str, Path],
    panel_path: Path,
    target_end_date: pd.Timestamp,
    query_text: str,
) -> None:
    current_costed_end = read_csv_last_date(args.costed_nav_csv)
    if current_costed_end is None or pd.Timestamp(current_costed_end).normalize() < pd.Timestamp(target_end_date).normalize():
        try_extend_costed_nav_without_turnover(args, panel_path, target_end_date, paths["proxy_turnover"])
    current_costed_end = read_csv_last_date(args.costed_nav_csv)
    if current_costed_end is None or pd.Timestamp(current_costed_end).normalize() < pd.Timestamp(target_end_date).normalize():
        ensure_strategy_nav_fresh(args, paths, panel_path, target_end_date)
    perf_df, ret_col, nav_col, source_label = load_performance_source(
        args.costed_nav_csv,
        pd.DataFrame(),
        args.index_csv,
    )
    build_performance_outputs(
        perf_df=perf_df,
        ret_col=ret_col,
        nav_col=nav_col,
        source_label=source_label,
        query_text=query_text,
        paths=paths,
    )
    summary = pd.read_csv(paths["performance_summary"])
    yearly = pd.read_csv(paths["performance_yearly"])
    print("表现汇总")
    print(format_table(summary))
    print("年度分解")
    print(format_table(yearly, max_rows=30))
    print(f"已保存: {paths['performance_chart'].name}")
    print(f"已保存: {paths['performance_summary'].name}")
    print(f"已保存: {paths['performance_yearly'].name}")
    print(f"已保存: {paths['performance_nav'].name}")
    print(f"已保存: {paths['performance_json'].name}")


def normalize_symbol_code(series: pd.Series) -> pd.Series:
    return series.astype(str).str.extract(r"(\d{6})", expand=False).fillna("")


def get_realtime_cache_file(name: str) -> Path:
    REALTIME_DIR.mkdir(parents=True, exist_ok=True)
    return REALTIME_DIR / name


def load_or_refresh_stock_spot(cache_seconds: int, allow_stale_cache: bool = False) -> pd.DataFrame:
    cache_file = get_realtime_cache_file("stock_spot_latest.csv")
    now = time.time()
    if cache_file.exists() and now - cache_file.stat().st_mtime <= cache_seconds:
        return pd.read_csv(cache_file, dtype={"代码": str})

    last_error: Exception | None = None
    for fetcher in (ak.stock_zh_a_spot_em, ak.stock_zh_a_spot):
        try:
            spot = fetcher()
            _atomic_to_csv(spot, cache_file, index=False, encoding="utf-8")
            return spot
        except Exception as exc:
            last_error = exc

    if cache_file.exists() and allow_stale_cache:
        return pd.read_csv(cache_file, dtype={"代码": str})
    raise RuntimeError(f"实时股票行情抓取失败，且缓存已过期: {last_error}") from last_error


def load_or_refresh_index_spot(cache_seconds: int, allow_stale_cache: bool = False) -> pd.DataFrame:
    cache_file = get_realtime_cache_file("index_spot_latest.csv")
    now = time.time()
    if cache_file.exists() and now - cache_file.stat().st_mtime <= cache_seconds:
        return pd.read_csv(cache_file, dtype={"代码": str})

    try:
        spot = ak.stock_zh_index_spot_em()
        _atomic_to_csv(spot, cache_file, index=False, encoding="utf-8")
        return spot
    except Exception as exc:
        if cache_file.exists() and allow_stale_cache:
            return pd.read_csv(cache_file, dtype={"代码": str})
        raise RuntimeError(f"实时指数行情抓取失败，且缓存已过期: {exc}") from exc


def _first_existing_column(frame: pd.DataFrame, candidates: list[str]) -> str:
    for column in candidates:
        if column in frame.columns:
            return column
    lowered_candidates = {str(item).strip().lower().replace("_", "").replace(" ", "") for item in candidates}
    semantic_tokens: list[str] = []
    if lowered_candidates & {"rtprice", "latestprice"} or any("最新" in str(item) for item in candidates):
        semantic_tokens.extend(["latest", "lastprice", "最新"])
    if lowered_candidates & {"preclose", "prevclose", "previousclose"} or any("昨收" in str(item) for item in candidates):
        semantic_tokens.extend(["previousclose", "prevclose", "preclose", "昨收"])
    if lowered_candidates & {"quotedate", "tradedate"} or any("交易" in str(item) for item in candidates):
        semantic_tokens.extend(["tradedate", "quotedate", "交易日"])
    for column in frame.columns:
        normalized = str(column).strip().lower().replace("_", "").replace(" ", "")
        if any(token in normalized for token in semantic_tokens):
            return column
    raise KeyError(f"Missing expected columns, tried: {candidates}")


def normalize_index_spot_columns(index_spot: pd.DataFrame) -> pd.DataFrame:
    out = index_spot.copy()
    code_col = _first_existing_column(out, ["代码", "浠ｇ爜"])
    latest_col = _first_existing_column(out, ["最新价", "鏈€鏂颁环"])
    prev_col = _first_existing_column(out, ["昨收", "鏄ㄦ敹"])
    out["代码"] = out[code_col]
    out["最新价"] = out[latest_col]
    out["昨收"] = out[prev_col]
    return out


def _optional_existing_column(frame: pd.DataFrame, candidates: list[str]) -> str | None:
    try:
        return _first_existing_column(frame, candidates)
    except KeyError:
        return None


def normalize_stock_spot_realtime_quotes(stock_spot: pd.DataFrame, source: str) -> pd.DataFrame:
    if stock_spot.empty:
        out = pd.DataFrame(columns=["code", "name", "rt_price", "pre_close", "trade_date", "quote_time"])
        out.attrs["quote_source"] = source
        return out
    raw = stock_spot.copy()
    code_col = _first_existing_column(raw, ["code", "代码", "浠ｇ爜"])
    name_col = _optional_existing_column(raw, ["name", "名称", "鍚嶇О"])
    latest_col = _first_existing_column(raw, ["rt_price", "最新价", "鏈€鏂颁环"])
    prev_col = _optional_existing_column(raw, ["pre_close", "昨收", "鏄ㄦ敹"])
    time_col = _optional_existing_column(raw, ["quote_time", "时间戳", "trade_time", "time"])
    trade_date_col = _optional_existing_column(raw, ["trade_date", "交易日"])

    out = pd.DataFrame()
    out["code"] = normalize_symbol_code(raw[code_col])
    out["name"] = raw[name_col].astype(str) if name_col else ""
    out["rt_price"] = pd.to_numeric(raw[latest_col], errors="coerce")
    if prev_col:
        out["pre_close"] = pd.to_numeric(raw[prev_col], errors="coerce")
        out.loc[out["rt_price"].isna() | (out["rt_price"] <= 0), "rt_price"] = out["pre_close"]
    else:
        out["pre_close"] = pd.NA
    if trade_date_col:
        out["trade_date"] = raw[trade_date_col].astype(str)
    else:
        out["trade_date"] = str(_cn_timestamp().date())
    out["quote_time"] = raw[time_col].astype(str) if time_col else ""
    out = out[out["code"].ne("")]
    out = out[pd.to_numeric(out["rt_price"], errors="coerce").gt(0)]
    out = out.drop_duplicates(subset="code", keep="last").reset_index(drop=True)
    out.attrs["quote_source"] = source
    return out


def load_or_refresh_latest_shares(cache_seconds: int = 86400) -> pd.DataFrame:
    cache_file = get_realtime_cache_file("latest_total_shares.csv")
    now = time.time()
    if cache_file.exists() and now - cache_file.stat().st_mtime <= cache_seconds:
        return pd.read_csv(cache_file, dtype={"code": str, "symbol": str})

    universe = pd.read_csv(freq_mod.ACTIVE_UNIVERSE, dtype=str)
    st_codes = set(pd.read_csv(freq_mod.CURRENT_ST, dtype=str)["code"].dropna().astype(str))
    universe = universe[~universe["code"].isin(st_codes)].copy()
    universe = universe[universe["name"].map(is_tradable_name)].copy()

    rows: list[dict[str, object]] = []
    for row in universe.itertuples(index=False):
        code = str(row.code).zfill(6)
        share_path = freq_mod.SHARE_DIR / f"{code}.csv"
        if not share_path.exists():
            continue
        try:
            share_df = pd.read_csv(share_path, usecols=["change_date", "total_shares_10k"])
            share_df = share_df.dropna(subset=["total_shares_10k"])
            if share_df.empty:
                continue
            share_df["change_date"] = pd.to_datetime(share_df["change_date"])
            share_df["total_shares_10k"] = pd.to_numeric(share_df["total_shares_10k"], errors="coerce")
            share_df = share_df.dropna(subset=["total_shares_10k"]).sort_values("change_date")
            last_row = share_df.iloc[-1]
            rows.append(
                {
                    "symbol": str(row.symbol),
                    "code": code,
                    "name": str(row.name),
                    "change_date": str(pd.Timestamp(last_row["change_date"]).date()),
                    "total_shares": float(last_row["total_shares_10k"]) * 10000.0,
                }
            )
        except Exception:
            continue

    latest_shares = pd.DataFrame(rows)
    _atomic_to_csv(latest_shares, cache_file, index=False, encoding="utf-8")
    return latest_shares


def parse_eastmoney_trade_date(value: object) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    digits = "".join(ch for ch in text if ch.isdigit())
    if len(digits) < 8:
        return ""
    try:
        return str(pd.Timestamp(digits[:8]).date())
    except Exception:
        return ""


def parse_quote_epoch_trade_date(value: object) -> str:
    numeric = pd.to_numeric(value, errors="coerce")
    if pd.isna(numeric) or float(numeric) <= 0:
        return ""
    try:
        return str(pd.to_datetime(float(numeric), unit="s", utc=True).tz_convert(CN_TIMEZONE).date())
    except Exception:
        return ""


def eastmoney_secid(symbol: str) -> str:
    code = str(symbol).strip().upper()
    if "." in code:
        raw_code, suffix = code.split(".", 1)
        market = "1" if suffix == "SH" else "0"
        return f"{market}.{raw_code.zfill(6)}"
    code = re.sub(r"\D", "", code).zfill(6)
    market = "1" if code.startswith(("5", "6", "9")) else "0"
    return f"{market}.{code}"


def _empty_realtime_quote_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=["code", "name", "rt_price", "pre_close", "trade_date", "quote_time"])


def _normalize_realtime_quote_frame(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return _empty_realtime_quote_frame()
    out = frame.copy()
    if "code" not in out.columns:
        return _empty_realtime_quote_frame()
    out["code"] = out["code"].astype(str).str.extract(r"(\d{6})", expand=False).fillna("").str.zfill(6)
    out = out.loc[out["code"].ne("")].copy()
    for column in ("rt_price", "pre_close"):
        if column in out.columns:
            out[column] = pd.to_numeric(out[column], errors="coerce")
    for column in ("name", "trade_date", "quote_time"):
        if column not in out.columns:
            out[column] = ""
    return out[["code", "name", "rt_price", "pre_close", "trade_date", "quote_time"]]


def _valid_quote_code_set(frame: pd.DataFrame) -> set[str]:
    if frame.empty:
        return set()
    out = _normalize_realtime_quote_frame(frame)
    valid_price = pd.to_numeric(out["rt_price"], errors="coerce").gt(0)
    valid_date = out["trade_date"].fillna("").astype(str).str.strip().ne("")
    return set(out.loc[valid_price & valid_date, "code"])


def add_last_close_flat_fallback_quotes(
    quotes_df: pd.DataFrame,
    member_symbols: list[str],
    last_close_map: dict[str, float],
    latest_trade_date: pd.Timestamp,
    *,
    max_missing_count: int = 5,
    min_quoted_fraction: float = 0.95,
) -> tuple[pd.DataFrame, int]:
    out = _normalize_realtime_quote_frame(quotes_df)
    member_codes = [str(symbol).zfill(6) for symbol in member_symbols if str(symbol).strip()]
    if not member_codes:
        return out, 0
    quoted_codes = _valid_quote_code_set(out)
    missing_codes = [code for code in member_codes if code not in quoted_codes]
    if not missing_codes:
        return out, 0
    quoted_fraction = len(quoted_codes.intersection(member_codes)) / len(member_codes)
    if len(missing_codes) > int(max_missing_count) or quoted_fraction < float(min_quoted_fraction):
        return out, 0

    valid_dates = pd.to_datetime(out["trade_date"], errors="coerce").dropna()
    if not valid_dates.empty:
        trade_date = str(valid_dates.max().date())
    else:
        trade_date = str(pd.Timestamp(latest_trade_date).date())
    rows: list[dict[str, object]] = []
    for code in missing_codes:
        close = pd.to_numeric(last_close_map.get(code), errors="coerce")
        if pd.isna(close) or float(close) <= 0:
            continue
        rows.append(
            {
                "code": code,
                "name": "",
                "rt_price": float(close),
                "pre_close": float(close),
                "trade_date": trade_date,
                "quote_time": "",
                "quote_source": "latest_close_flat_fallback",
            }
        )
    if not rows:
        return out, 0
    supplemented = pd.concat([out, pd.DataFrame(rows)], ignore_index=True).drop_duplicates(subset="code", keep="first")
    return supplemented, len(rows)


def fetch_eastmoney_batch_realtime_quotes(symbols: list[str], batch_size: int = 80) -> pd.DataFrame:
    clean_secids = []
    seen = set()
    for symbol in symbols:
        secid = eastmoney_secid(str(symbol))
        if secid not in seen:
            seen.add(secid)
            clean_secids.append(secid)
    rows: list[dict[str, object]] = []
    for start in range(0, len(clean_secids), max(1, int(batch_size))):
        batch = clean_secids[start : start + max(1, int(batch_size))]
        if not batch:
            continue
        url = (
            "https://push2.eastmoney.com/api/qt/ulist.np/get"
            f"?fltt=2&secids={','.join(batch)}"
            "&fields=f12,f14,f2,f18,f124,f297"
        )
        try:
            response = requests.get(
                url,
                timeout=10,
                headers={"Referer": "https://quote.eastmoney.com/", "User-Agent": "Mozilla/5.0"},
            )
            response.raise_for_status()
            diff = (response.json().get("data") or {}).get("diff") or []
        except Exception:
            continue
        for item in diff:
            code = str(item.get("f12") or "").zfill(6)
            latest = pd.to_numeric(item.get("f2"), errors="coerce")
            prev = pd.to_numeric(item.get("f18"), errors="coerce")
            if pd.isna(latest) or float(latest) <= 0:
                latest = prev
            if not code or pd.isna(latest) or float(latest) <= 0:
                continue
            trade_date = parse_eastmoney_trade_date(item.get("f297")) or parse_quote_epoch_trade_date(item.get("f124"))
            rows.append(
                {
                    "code": code,
                    "name": str(item.get("f14") or ""),
                    "rt_price": float(latest),
                    "pre_close": prev,
                    "trade_date": trade_date,
                    "quote_time": str(item.get("f124") or ""),
                }
            )
    out = _normalize_realtime_quote_frame(pd.DataFrame(rows).drop_duplicates(subset="code") if rows else pd.DataFrame())
    out.attrs["quote_source"] = "eastmoney_ulist_free"
    return out


def tencent_quote_symbol(symbol: str) -> str:
    raw = str(symbol).strip().lower()
    if raw.startswith(("sh", "sz")) and len(raw) >= 8:
        return raw[:8]
    upper = str(symbol).strip().upper()
    if "." in upper:
        code, suffix = upper.split(".", 1)
        prefix = "sh" if suffix == "SH" else "sz"
        return f"{prefix}{code.zfill(6)}"
    code = re.sub(r"\D", "", upper).zfill(6)
    prefix = "sh" if code.startswith(("5", "6", "9")) else "sz"
    return f"{prefix}{code}"


def fetch_tencent_realtime_quotes(symbols: list[str], batch_size: int = 80) -> pd.DataFrame:
    quote_symbols = []
    seen = set()
    for symbol in symbols:
        quote_symbol = tencent_quote_symbol(str(symbol))
        if quote_symbol not in seen:
            seen.add(quote_symbol)
            quote_symbols.append(quote_symbol)
    rows: list[dict[str, object]] = []
    for start in range(0, len(quote_symbols), max(1, int(batch_size))):
        batch = quote_symbols[start : start + max(1, int(batch_size))]
        if not batch:
            continue
        url = f"https://qt.gtimg.cn/q={','.join(batch)}"
        try:
            response = requests.get(url, timeout=10, headers={"User-Agent": "Mozilla/5.0"})
            response.raise_for_status()
            response.encoding = response.encoding or "gbk"
            text = response.text
        except Exception:
            continue
        for line in text.split(";"):
            if "~" not in line:
                continue
            payload = line.split("=", 1)[-1].strip().strip('"')
            parts = payload.split("~")
            if len(parts) < 5:
                continue
            code = str(parts[2] or "").zfill(6)
            latest = pd.to_numeric(parts[3], errors="coerce")
            prev = pd.to_numeric(parts[4], errors="coerce")
            if pd.isna(latest) or float(latest) <= 0:
                latest = prev
            if not code or pd.isna(latest) or float(latest) <= 0:
                continue
            quote_time = parts[30] if len(parts) > 30 else ""
            trade_date = parse_eastmoney_trade_date(quote_time)
            rows.append(
                {
                    "code": code,
                    "name": str(parts[1] or ""),
                    "rt_price": float(latest),
                    "pre_close": prev,
                    "trade_date": trade_date,
                    "quote_time": quote_time,
                }
            )
    out = _normalize_realtime_quote_frame(pd.DataFrame(rows).drop_duplicates(subset="code") if rows else pd.DataFrame())
    out.attrs["quote_source"] = "tencent_batch_free"
    return out


def fetch_eastmoney_stock_spot(symbol: str) -> dict[str, object] | None:
    code = str(symbol).zfill(6)
    market = "1" if code.startswith(("5", "6", "9")) else "0"
    url = (
        "https://push2.eastmoney.com/api/qt/stock/get"
        f"?secid={market}.{code}"
        "&fields=f43,f44,f45,f46,f57,f58,f60,f86"
    )
    try:
        response = requests.get(
            url,
            timeout=10,
            headers={"Referer": "https://quote.eastmoney.com/", "User-Agent": "Mozilla/5.0"},
        )
        response.raise_for_status()
        data = response.json().get("data") or {}
        latest = pd.to_numeric(data.get("f43"), errors="coerce")
        prev = pd.to_numeric(data.get("f60"), errors="coerce")
        if pd.notna(latest) and latest > 0:
            rt_price = float(latest) / 100.0
        elif pd.notna(prev) and prev > 0:
            rt_price = float(prev) / 100.0
        else:
            return None
        row = {
            "code": str(data.get("f57") or code).zfill(6),
            "name": str(data.get("f58") or ""),
            "rt_price": rt_price,
        }
        if pd.notna(prev) and prev > 0:
            row["pre_close"] = float(prev) / 100.0
        trade_date = parse_eastmoney_trade_date(data.get("f86"))
        if trade_date:
            row["trade_date"] = trade_date
        return row
    except Exception:
        return None


def qveris_security_code(symbol: str) -> str:
    raw = str(symbol).strip().upper()
    if re.fullmatch(r"[01]\.\d{6}", raw):
        market, code = raw.split(".", 1)
        suffix = "SH" if market == "1" else "SZ"
        return f"{code}.{suffix}"
    if "." in raw:
        return raw
    code = raw.zfill(6)
    suffix = "SH" if code.startswith(("5", "6", "9")) else "SZ"
    return f"{code}.{suffix}"


def _iter_qveris_records(data: object):
    if isinstance(data, dict):
        yield data
        return
    if isinstance(data, list):
        for item in data:
            yield from _iter_qveris_records(item)


def _qveris_api_key() -> str:
    api_key = os.environ.get("QVERIS_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("QVERIS_API_KEY is not set")
    return api_key


def _qveris_post(path: str, body: dict[str, object], timeout: int = 30) -> dict[str, object]:
    requests_mod = requests if requests is not None else importlib.import_module("requests")
    response = requests_mod.post(
        f"{QVERIS_API_BASE}{path}",
        headers={"Authorization": f"Bearer {_qveris_api_key()}", "Content-Type": "application/json"},
        json=body,
        timeout=timeout,
    )
    response.raise_for_status()
    payload = response.json()
    return payload if isinstance(payload, dict) else {"payload": payload}


def _extract_qveris_tool_ids(payload: object) -> list[str]:
    out: list[str] = []
    if isinstance(payload, dict):
        for key in ("id", "tool_id", "toolId"):
            value = payload.get(key)
            if isinstance(value, str) and value not in out:
                out.append(value)
        for value in payload.values():
            for tool_id in _extract_qveris_tool_ids(value):
                if tool_id not in out:
                    out.append(tool_id)
    elif isinstance(payload, list):
        for item in payload:
            for tool_id in _extract_qveris_tool_ids(item):
                if tool_id not in out:
                    out.append(tool_id)
    return out


def _qveris_discover_tool_id(query: str, cache_key: str, session_id: str) -> str:
    cached = _QVERIS_TOOL_ID_CACHE.get(cache_key)
    if cached:
        return cached
    search_payload = _qveris_post(
        "/search",
        {"query": query, "limit": 5, "session_id": session_id},
        timeout=20,
    )
    tool_ids = _extract_qveris_tool_ids(search_payload)
    if not tool_ids:
        raise RuntimeError(f"QVeris search found no tool for query: {query}")
    inspect_ids = tool_ids[:3]
    _qveris_post("/tools/by-ids", {"tool_ids": inspect_ids, "ids": inspect_ids}, timeout=20)
    _QVERIS_TOOL_ID_CACHE[cache_key] = tool_ids[0]
    return tool_ids[0]


def _qveris_execute_discovered_tool(
    query: str,
    cache_key: str,
    parameters: dict[str, object],
    session_id: str,
    max_response_size: int = 200000,
) -> dict[str, object]:
    tool_id = _qveris_discover_tool_id(query, cache_key, session_id)
    return _qveris_post(
        f"/tools/execute?tool_id={tool_id}",
        {
            "parameters": parameters,
            "session_id": session_id,
            "max_response_size": int(max_response_size),
        },
        timeout=45,
    )


def _iter_qveris_data_records(data: object):
    if isinstance(data, dict):
        if any(key in data for key in ("date", "trade_date", "tradeDate", "day", "close", "close_raw", "收盘价")):
            yield data
        for value in data.values():
            yield from _iter_qveris_data_records(value)
    elif isinstance(data, list):
        for item in data:
            yield from _iter_qveris_data_records(item)


def _first_record_value(record: dict[str, object], keys: tuple[str, ...]) -> object | None:
    for key in keys:
        if key in record and record.get(key) not in (None, ""):
            return record.get(key)
    lower_map = {str(key).lower(): value for key, value in record.items()}
    for key in keys:
        value = lower_map.get(key.lower())
        if value not in (None, ""):
            return value
    return None


def _qveris_history_frame(payload: dict[str, object], close_column: str) -> pd.DataFrame:
    result = payload.get("result", payload)
    rows: list[dict[str, object]] = []
    for record in _iter_qveris_data_records(result):
        date_value = _first_record_value(record, ("date", "trade_date", "tradeDate", "day", "datetime", "time", "日期"))
        close_value = _first_record_value(
            record,
            ("close", "close_raw", "closePrice", "收盘价", "收盘", "price", "latest", "最新价"),
        )
        if date_value is None or close_value is None:
            continue
        dt = pd.to_datetime(date_value, errors="coerce")
        close = pd.to_numeric(close_value, errors="coerce")
        if pd.isna(dt) or pd.isna(close) or float(close) <= 0:
            continue
        rows.append({"date": pd.Timestamp(dt).normalize(), close_column: float(close)})
    out = pd.DataFrame(rows)
    if out.empty:
        return pd.DataFrame(columns=["date", close_column])
    return out.drop_duplicates(subset="date", keep="last").sort_values("date").reset_index(drop=True)


def fetch_qveris_index_history(
    secid: str,
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
) -> pd.DataFrame:
    raise RuntimeError(QVERIS_DISABLED_MESSAGE)
    payload = _qveris_execute_discovered_tool(
        query=QVERIS_INDEX_HISTORY_TOOL_QUERY,
        cache_key="index_history",
        parameters={
            "code": qveris_security_code(secid),
            "symbol": qveris_security_code(secid),
            "start_date": pd.Timestamp(start_date).strftime("%Y-%m-%d"),
            "end_date": pd.Timestamp(end_date).strftime("%Y-%m-%d"),
            "period": "daily",
            "fields": "date,close",
        },
        session_id="microcap_index_history_fallback",
    )
    out = _qveris_history_frame(payload, close_column="close")
    return out.loc[
        (out["date"] >= pd.Timestamp(start_date).normalize())
        & (out["date"] <= pd.Timestamp(end_date).normalize())
    ].reset_index(drop=True)


def fetch_qveris_price_history(symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
    raise RuntimeError(QVERIS_DISABLED_MESSAGE)
    payload = _qveris_execute_discovered_tool(
        query=QVERIS_STOCK_PRICE_HISTORY_TOOL_QUERY,
        cache_key="stock_price_history",
        parameters={
            "code": qveris_security_code(symbol),
            "symbol": qveris_security_code(symbol),
            "start_date": str(start_date),
            "end_date": str(end_date),
            "period": "daily",
            "adjust": "none",
            "fields": "date,close",
        },
        session_id="microcap_price_history_fallback",
    )
    out = _qveris_history_frame(payload, close_column="close_raw")
    if out.empty:
        raise RuntimeError(f"QVeris returned empty price history for {symbol}")
    return out.loc[
        (out["date"] >= pd.Timestamp(start_date).normalize())
        & (out["date"] <= pd.Timestamp(end_date).normalize())
    ].reset_index(drop=True)


def _price_cache_dir() -> Path:
    path = getattr(fetch_mod, "PRICE_CACHE_DIR", None)
    if path is None:
        path = getattr(freq_mod, "PRICE_DIR", None)
    if path is None:
        raise RuntimeError("No local raw-price cache directory is configured.")
    return Path(path)


def write_qveris_price_history_cache(symbol: str, frame: pd.DataFrame) -> None:
    cache_dir = _price_cache_dir()
    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_path = cache_dir / f"{str(symbol).zfill(6)}.csv"
    incoming = frame[["date", "close_raw"]].copy()
    incoming["date"] = pd.to_datetime(incoming["date"], errors="coerce")
    incoming["close_raw"] = pd.to_numeric(incoming["close_raw"], errors="coerce")
    incoming = incoming.dropna(subset=["date", "close_raw"])
    existing = pd.read_csv(cache_path) if cache_path.exists() else pd.DataFrame(columns=["date", "close_raw"])
    if not existing.empty:
        existing["date"] = pd.to_datetime(existing["date"], errors="coerce")
        existing["close_raw"] = pd.to_numeric(existing["close_raw"], errors="coerce")
        existing = existing.dropna(subset=["date", "close_raw"])
    frames = [frame for frame in (existing[["date", "close_raw"]], incoming) if not frame.empty]
    merged = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=["date", "close_raw"])
    merged = merged.drop_duplicates(subset="date", keep="last").sort_values("date")
    _atomic_to_csv(merged, cache_path, index=False, encoding="utf-8")


def fetch_qveris_realtime_quotes(
    symbols: list[str],
    batch_size: int = 50,
) -> tuple[pd.DataFrame, str]:
    raise RuntimeError(QVERIS_DISABLED_MESSAGE)
    api_key = os.environ.get("QVERIS_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("QVERIS_API_KEY is not set")
    requests_mod = requests if requests is not None else importlib.import_module("requests")
    clean_codes = []
    seen = set()
    for symbol in symbols:
        code = qveris_security_code(str(symbol))
        if code not in seen:
            seen.add(code)
            clean_codes.append(code)
    rows: list[dict[str, object]] = []
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    for start in range(0, len(clean_codes), max(1, int(batch_size))):
        batch = clean_codes[start : start + max(1, int(batch_size))]
        body = {
            "parameters": {"codes": ",".join(batch), "indicators": "common"},
            "session_id": "microcap_realtime_signal",
            "max_response_size": 200000,
        }
        response = requests_mod.post(
            f"{QVERIS_API_BASE}/tools/execute?tool_id={QVERIS_REALTIME_TOOL_ID}",
            headers=headers,
            json=body,
            timeout=30,
        )
        response.raise_for_status()
        payload = response.json()
        result = payload.get("result", {}) if isinstance(payload, dict) else {}
        for rec in _iter_qveris_records(result.get("data")):
            thscode = str(rec.get("thscode") or rec.get("code") or "").upper()
            code = thscode.split(".", 1)[0].zfill(6)
            latest = pd.to_numeric(rec.get("latest"), errors="coerce")
            if not code or pd.isna(latest) or latest <= 0:
                continue
            rows.append(
                {
                    "code": code,
                    "name": str(rec.get("name") or ""),
                    "rt_price": float(latest),
                    "pre_close": pd.to_numeric(rec.get("preClose"), errors="coerce"),
                    "trade_date": str(rec.get("tradeDate") or ""),
                    "quote_time": str(rec.get("time") or rec.get("tradeTime") or ""),
                }
            )
    if not rows:
        return pd.DataFrame(columns=["code", "name", "rt_price", "pre_close", "trade_date", "quote_time"]), "qveris_cn_financial_pro_realtime"
    out = pd.DataFrame(rows).drop_duplicates(subset="code", keep="last")
    out.attrs["quote_source"] = "qveris_cn_financial_pro_realtime"
    return out, "qveris_cn_financial_pro_realtime"


def fetch_member_realtime_quotes(symbols: list[str], max_workers: int = 24) -> pd.DataFrame:
    clean_symbols = [str(symbol).zfill(6) for symbol in symbols if str(symbol).strip()]
    if not clean_symbols:
        return _empty_realtime_quote_frame()
    source_parts: list[str] = []
    frames: list[pd.DataFrame] = []

    eastmoney_batch_df = fetch_eastmoney_batch_realtime_quotes(clean_symbols)
    if not eastmoney_batch_df.empty:
        frames.append(eastmoney_batch_df)
        source_parts.append(str(eastmoney_batch_df.attrs.get("quote_source") or "eastmoney_ulist_free"))

    valid_free_codes = _valid_quote_code_set(pd.concat(frames, ignore_index=True) if frames else pd.DataFrame())
    tencent_symbols = [symbol for symbol in clean_symbols if symbol not in valid_free_codes]
    tencent_df = fetch_tencent_realtime_quotes(tencent_symbols) if tencent_symbols else _empty_realtime_quote_frame()
    if not tencent_df.empty:
        frames.append(tencent_df)
        source_parts.append(str(tencent_df.attrs.get("quote_source") or "tencent_batch_free"))

    valid_free_codes = _valid_quote_code_set(pd.concat(frames, ignore_index=True) if frames else pd.DataFrame())
    per_symbol_symbols = [symbol for symbol in clean_symbols if symbol not in valid_free_codes]
    rows: list[dict[str, object]] = []
    if per_symbol_symbols:
        with ThreadPoolExecutor(max_workers=max(1, min(int(max_workers), 32))) as pool:
            futures = {pool.submit(fetch_eastmoney_stock_spot, symbol): symbol for symbol in per_symbol_symbols}
            for fut in as_completed(futures):
                row = fut.result()
                if row is not None:
                    rows.append(row)
    eastmoney_df = _normalize_realtime_quote_frame(
        pd.DataFrame(rows).drop_duplicates(subset="code") if rows else pd.DataFrame()
    )
    if not eastmoney_df.empty:
        frames.append(eastmoney_df)
        source_parts.append("eastmoney_stock_get_member_only")

    out = (
        pd.concat(frames, ignore_index=True).drop_duplicates(subset="code", keep="first")
        if frames
        else _empty_realtime_quote_frame()
    )
    out.attrs["quote_source"] = "+".join(source_parts or ["free_realtime_quotes_empty"])
    return out


def fetch_hedge_realtime_quote_fast() -> tuple[float, str, str]:
    eastmoney_price: float | None = None
    eastmoney_source = "eastmoney_stock_get"
    url = (
        "https://push2.eastmoney.com/api/qt/stock/get"
        "?secid=1.000852"
        "&fields=f43,f60,f86"
    )
    try:
        response = requests.get(
            url,
            timeout=10,
            headers={"Referer": "https://quote.eastmoney.com/", "User-Agent": "Mozilla/5.0"},
        )
        response.raise_for_status()
        data = response.json().get("data") or {}
        latest = pd.to_numeric(data.get("f43"), errors="coerce")
        prev = pd.to_numeric(data.get("f60"), errors="coerce")
        if pd.notna(latest) and latest > 0:
            eastmoney_price = float(latest) / 100.0
            trade_date = parse_eastmoney_trade_date(data.get("f86"))
            if trade_date:
                return eastmoney_price, eastmoney_source, trade_date
        elif pd.notna(prev) and prev > 0:
            eastmoney_price = float(prev) / 100.0
            eastmoney_source = "eastmoney_prev_close_fallback"
    except Exception:
        pass
    try:
        quotes = fetch_tencent_realtime_quotes(["sh000852"])
        if not quotes.empty:
            row = quotes.iloc[0]
            price = pd.to_numeric(row.get("rt_price"), errors="coerce")
            trade_date = str(row.get("trade_date") or "").strip()
            if pd.notna(price) and price > 0 and trade_date:
                return float(price), str(quotes.attrs.get("quote_source") or "tencent_batch_free"), trade_date
    except Exception:
        pass
    if eastmoney_price is not None:
        return eastmoney_price, f"{eastmoney_source}_missing_trade_date", ""
    index_spot = normalize_index_spot_columns(load_or_refresh_index_spot(cache_seconds=86400))
    index_spot["代码"] = index_spot["代码"].astype(str).str.zfill(6)
    hedge_row = index_spot.loc[index_spot["代码"] == "000852"]
    if hedge_row.empty:
        raise RuntimeError("无法获取中证1000实时价格")
    hedge_row = hedge_row.iloc[0]
    hedge_rt_close = pd.to_numeric(hedge_row.get("最新价"), errors="coerce")
    hedge_prev = pd.to_numeric(hedge_row.get("昨收"), errors="coerce")
    if pd.notna(hedge_rt_close) and hedge_rt_close > 0:
        return float(hedge_rt_close), "index_spot_latest_cached_fallback", ""
    if pd.notna(hedge_prev) and hedge_prev > 0:
        return float(hedge_prev), "index_prev_close_cached_fallback", ""
    raise RuntimeError("无法获取中证1000实时价格")


def format_missing_realtime_symbols(missing_symbols: list[dict[str, object]], limit: int = 8) -> str:
    parts: list[str] = []
    for item in missing_symbols[:limit]:
        symbol = str(item.get("symbol") or "").zfill(6)
        name = str(item.get("name") or "").strip()
        rank = item.get("rank")
        rank_text = "" if rank in (None, "") else f" rank={rank}"
        label = symbol if not name else f"{symbol} {name}"
        parts.append(f"{label}{rank_text}")
    if len(missing_symbols) > limit:
        parts.append(f"... +{len(missing_symbols) - limit} more")
    return "; ".join(parts)


def ensure_realtime_quote_coverage(
    available_rows: int,
    member_count: int,
    missing_symbols: list[dict[str, object]] | None = None,
    quote_source: str | None = None,
) -> None:
    if member_count > 0 and int(available_rows) < int(member_count):
        message = f"实时信号报价覆盖不足: {available_rows}/{member_count}，拒绝输出实盘信号。"
        if missing_symbols:
            message += f" missing_symbols={format_missing_realtime_symbols(missing_symbols)}."
        if quote_source:
            message += f" quote_source={quote_source}."
        raise ValueError(message)


def quote_trade_date_matches_anchor(quote_trade_date: object, latest_trade_date: pd.Timestamp) -> bool:
    try:
        if quote_trade_date is None or str(quote_trade_date).strip() == "":
            return False
        return pd.Timestamp(quote_trade_date).date() == pd.Timestamp(latest_trade_date).date()
    except Exception:
        return False


def quote_trade_date_on_or_after_anchor(quote_trade_date: object, latest_trade_date: pd.Timestamp) -> bool:
    try:
        if quote_trade_date is None or str(quote_trade_date).strip() == "":
            return False
        return pd.Timestamp(quote_trade_date).date() >= pd.Timestamp(latest_trade_date).date()
    except Exception:
        return False


def normalize_hedge_realtime_quote_result(result: object) -> tuple[float, str, str]:
    if isinstance(result, tuple):
        if len(result) >= 3:
            return float(result[0]), str(result[1]), str(result[2] or "")
        if len(result) == 2:
            return float(result[0]), str(result[1]), ""
    raise ValueError(f"Unexpected hedge realtime quote result: {result!r}")


def extract_member_quote_trade_date_stats(
    quotes_df: pd.DataFrame,
    member_symbols: list[str],
    latest_anchor_trade_date: pd.Timestamp,
) -> dict[str, object]:
    if "trade_date" not in quotes_df.columns:
        return {
            "member_quote_trade_date_min": "",
            "member_quote_trade_date_max": "",
            "member_quote_trade_date_count": 0,
            "member_quote_bad_symbols": [str(symbol).zfill(6) for symbol in member_symbols][:20],
        }
    anchor_date = pd.Timestamp(latest_anchor_trade_date).date()
    dates: list[str] = []
    bad_symbols: list[str] = []
    for symbol in member_symbols:
        code = str(symbol).zfill(6)
        if code not in quotes_df.index:
            bad_symbols.append(code)
            continue
        text = str(quotes_df.at[code, "trade_date"] or "").strip()
        if not text:
            bad_symbols.append(code)
            continue
        try:
            quote_date = pd.Timestamp(text).date()
        except Exception:
            bad_symbols.append(code)
            continue
        if quote_date < anchor_date:
            bad_symbols.append(code)
            continue
        dates.append(str(quote_date))
    return {
        "member_quote_trade_date_min": min(dates) if dates else "",
        "member_quote_trade_date_max": max(dates) if dates else "",
        "member_quote_trade_date_count": len(dates),
        "member_quote_bad_symbols": bad_symbols[:20],
    }


def extract_member_quote_trade_date(quotes_df: pd.DataFrame, member_symbols: list[str]) -> tuple[str, int]:
    stats = extract_member_quote_trade_date_stats(
        quotes_df,
        member_symbols,
        latest_anchor_trade_date=pd.Timestamp.min,
    )
    return str(stats["member_quote_trade_date_max"]), int(stats["member_quote_trade_date_count"])


def compute_member_realtime_return(
    symbol: str,
    last_close_map: dict[str, float],
    quotes_df: pd.DataFrame,
    latest_trade_date: pd.Timestamp,
    allow_quote_pre_close_after_anchor: bool = False,
) -> float | None:
    code = str(symbol).zfill(6)
    if code not in quotes_df.index:
        return None
    rt_price = pd.to_numeric(quotes_df.at[code, "rt_price"], errors="coerce")
    if pd.isna(rt_price) or float(rt_price) <= 0:
        return None

    quote_trade_date = quotes_df.at[code, "trade_date"] if "trade_date" in quotes_df.columns else ""
    if str(quote_trade_date or "").strip():
        quote_day = pd.to_datetime(quote_trade_date, errors="coerce")
        if pd.notna(quote_day):
            quote_day = pd.Timestamp(quote_day).normalize()
            anchor_day = pd.Timestamp(latest_trade_date).normalize()
            if quote_day < anchor_day:
                return None
            if quote_day == anchor_day:
                return 0.0

    last_close = last_close_map.get(code)
    if isinstance(last_close, dict):
        close = pd.to_numeric(last_close.get("close"), errors="coerce")
        close_date = last_close.get("date")
        if (
            pd.notna(close)
            and float(close) > 0
            and close_date is not None
            and pd.Timestamp(close_date).date() == pd.Timestamp(latest_trade_date).date()
        ):
            return float(rt_price) / float(close) - 1.0
    elif last_close is not None:
        close = pd.to_numeric(last_close, errors="coerce")
        if pd.notna(close) and float(close) > 0:
            return float(rt_price) / float(close) - 1.0

    if "pre_close" in quotes_df.columns and (
        quote_trade_date_matches_anchor(quote_trade_date, latest_trade_date)
        or (
            allow_quote_pre_close_after_anchor
            and (
                quote_trade_date_on_or_after_anchor(quote_trade_date, latest_trade_date)
                or not str(quote_trade_date or "").strip()
            )
        )
    ):
        pre_close = pd.to_numeric(quotes_df.at[code, "pre_close"], errors="coerce")
        if pd.notna(pre_close) and float(pre_close) > 0:
            return float(rt_price) / float(pre_close) - 1.0
    return None


def compute_member_realtime_returns(
    member_symbols: list[str],
    effective_members: pd.DataFrame,
    last_close_map: dict[str, float],
    quotes_df: pd.DataFrame,
    latest_trade_date: pd.Timestamp,
    allow_quote_pre_close_after_anchor: bool = False,
) -> tuple[list[float], list[dict[str, object]]]:
    member_lookup = effective_members.copy()
    member_lookup["symbol"] = member_lookup["symbol"].astype(str).str.zfill(6)
    member_lookup = member_lookup.set_index("symbol", drop=False)
    member_returns: list[float] = []
    missing_symbols: list[dict[str, object]] = []
    for symbol in member_symbols:
        code = str(symbol).zfill(6)
        member_return = compute_member_realtime_return(
            code,
            last_close_map,
            quotes_df,
            latest_trade_date,
            allow_quote_pre_close_after_anchor=allow_quote_pre_close_after_anchor,
        )
        if member_return is None:
            if code in member_lookup.index:
                row = member_lookup.loc[code]
                if isinstance(row, pd.DataFrame):
                    row = row.iloc[0]
                name = row.get("name", "")
                rank = row.get("rank", "")
            else:
                name = ""
                rank = ""
            missing_symbols.append({"symbol": code, "name": name, "rank": rank})
            continue
        member_returns.append(float(member_return))
    return member_returns, missing_symbols


def add_last_close_flat_fallback_quotes(
    quotes_df: pd.DataFrame,
    member_symbols: list[str],
    last_close_map: dict[str, float],
    latest_trade_date: pd.Timestamp,
    *,
    max_missing_count: int = 5,
    min_quoted_fraction: float = REALTIME_LAST_CLOSE_FLAT_FALLBACK_MIN_QUOTED_FRACTION,
) -> tuple[pd.DataFrame, int]:
    out = _normalize_realtime_quote_frame(quotes_df)
    if "quote_source" not in out.columns:
        out["quote_source"] = ""
    clean_symbols = [str(symbol).zfill(6) for symbol in member_symbols if str(symbol).strip()]
    if not clean_symbols:
        return out, 0
    quoted_codes = _valid_quote_code_set(out)
    missing_symbols = [symbol for symbol in clean_symbols if symbol not in quoted_codes]
    quoted_fraction = len(quoted_codes.intersection(clean_symbols)) / len(clean_symbols)
    if len(missing_symbols) > int(max_missing_count) or quoted_fraction < float(min_quoted_fraction):
        return out, 0
    existing_dates = out["trade_date"].dropna().astype(str).str.strip()
    existing_dates = existing_dates[existing_dates.ne("")]
    fallback_trade_date = existing_dates.max() if not existing_dates.empty else str(pd.Timestamp(latest_trade_date).date())
    rows: list[dict[str, object]] = []
    for symbol in missing_symbols:
        last_close = last_close_map.get(symbol)
        if isinstance(last_close, dict):
            last_close = last_close.get("close")
        close = pd.to_numeric(last_close, errors="coerce")
        if pd.isna(close) or float(close) <= 0:
            continue
        rows.append(
            {
                "code": symbol,
                "name": "",
                "rt_price": float(close),
                "pre_close": float(close),
                "trade_date": fallback_trade_date,
                "quote_time": "",
                "quote_source": "latest_close_flat_fallback",
            }
        )
    if not rows:
        return out, 0
    merged = pd.concat([out, pd.DataFrame(rows)], ignore_index=True).drop_duplicates(subset="code", keep="first")
    return merged, len(rows)


def apply_realtime_close_to_signal_frame(
    close_df: pd.DataFrame,
    latest_trade_date: pd.Timestamp,
    snapshot_ts: pd.Timestamp,
    microcap_rt_close: float,
    hedge_rt_close: float,
    quote_trade_date: object = "",
) -> pd.DataFrame:
    out = close_df.copy()
    latest_day = pd.Timestamp(latest_trade_date).normalize()
    quote_day = pd.to_datetime(quote_trade_date, errors="coerce")
    if pd.notna(quote_day):
        quote_day = pd.Timestamp(quote_day).normalize()
        snapshot_day = pd.Timestamp(snapshot_ts).tz_localize(None).normalize()
        if not latest_day < quote_day <= snapshot_day:
            return out.sort_index()
        target_ts = quote_day
    else:
        target_ts = latest_day

    index = pd.DatetimeIndex(out.index)
    normalized = index.normalize()
    drop_mask = (normalized == target_ts) & (index != target_ts)
    if bool(drop_mask.any()):
        out = out.loc[~drop_mask].copy()
    out.loc[target_ts, ["microcap", "hedge"]] = [microcap_rt_close, hedge_rt_close]
    return out.sort_index()


def build_realtime_quote_map(cache_seconds: int) -> tuple[pd.DataFrame, str]:
    stock_spot = load_or_refresh_stock_spot(cache_seconds)
    stock_spot["code"] = normalize_symbol_code(stock_spot["代码"])
    stock_spot = stock_spot[stock_spot["code"].ne("")].copy()
    for col in ["最新价", "昨收", "今开", "最高", "最低", "成交额"]:
        if col in stock_spot.columns:
            stock_spot[col] = pd.to_numeric(stock_spot[col], errors="coerce")
    stock_spot["rt_price"] = stock_spot["最新价"]
    stock_spot.loc[stock_spot["rt_price"].isna() | (stock_spot["rt_price"] <= 0), "rt_price"] = stock_spot["昨收"]
    source = "live_or_prev_close_fallback"
    return stock_spot, source


def load_latest_close_snapshot_map(symbols: list[str], as_of_date: pd.Timestamp) -> dict[str, tuple[pd.Timestamp, float]]:
    out: dict[str, tuple[pd.Timestamp, float]] = {}
    for symbol in symbols:
        path = freq_mod.PRICE_DIR / f"{symbol}.csv"
        if not path.exists():
            continue
        try:
            price = pd.read_csv(path, usecols=["date", "close_raw"])
            price["date"] = pd.to_datetime(price["date"])
            price["close_raw"] = pd.to_numeric(price["close_raw"], errors="coerce")
            price = price.dropna(subset=["close_raw"])
            price = price.loc[price["date"] <= as_of_date].sort_values("date")
            if price.empty:
                continue
            last_row = price.iloc[-1]
            out[str(symbol).zfill(6)] = (pd.Timestamp(last_row["date"]).normalize(), float(last_row["close_raw"]))
        except Exception:
            continue
    return out


def load_latest_close_map(symbols: list[str], as_of_date: pd.Timestamp) -> dict[str, float]:
    snapshots = load_latest_close_snapshot_map(symbols, as_of_date=as_of_date)
    return {symbol: close for symbol, (_date, close) in snapshots.items()}


def ensure_realtime_last_close_map(
    symbols: list[str],
    as_of_date: pd.Timestamp,
    max_workers: int = REALTIME_CLOSE_REFRESH_MAX_WORKERS,
) -> dict[str, float]:
    clean_symbols = [str(symbol).zfill(6) for symbol in symbols if str(symbol).strip()]
    target_date = pd.Timestamp(as_of_date).normalize()
    snapshots = load_latest_close_snapshot_map(clean_symbols, as_of_date=as_of_date)
    stale_or_missing = [
        symbol
        for symbol in clean_symbols
        if symbol not in snapshots or pd.Timestamp(snapshots[symbol][0]).normalize() < target_date
    ]
    if stale_or_missing and os.environ.get(TOP100_REALTIME_REQUIRE_STATE_ENV, "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }:
        return {
            symbol: close
            for symbol, (date, close) in snapshots.items()
            if pd.Timestamp(date).normalize() >= target_date
        }
    if stale_or_missing:
        try:
            refresh_price_cache_tail(as_of_date, max_workers=max_workers, symbols=stale_or_missing)
        except Exception:
            pass
        snapshots = load_latest_close_snapshot_map(clean_symbols, as_of_date=as_of_date)
    return {symbol: close for symbol, (date, close) in snapshots.items() if pd.Timestamp(date).normalize() >= target_date}


def maybe_refresh_missing_realtime_last_close_map(
    last_close_map: dict[str, float],
    missing_symbols: list[dict[str, object]],
    as_of_date: pd.Timestamp,
) -> dict[str, float]:
    missing_codes = [
        str(item.get("symbol") or "").zfill(6)
        for item in missing_symbols
        if str(item.get("symbol") or "").strip()
    ]
    missing_codes = [symbol for symbol in dict.fromkeys(missing_codes) if symbol not in last_close_map]
    if not missing_codes:
        return last_close_map
    try:
        refreshed = ensure_realtime_last_close_map(missing_codes, as_of_date=as_of_date)
    except Exception:
        return last_close_map
    out = dict(last_close_map)
    out.update(refreshed)
    return out


def build_realtime_target_members(context: dict[str, object], cache_seconds: int, capital: float | None) -> tuple[pd.DataFrame, str]:
    shares_df = load_or_refresh_latest_shares()
    quotes_df, quote_source = build_realtime_quote_map(cache_seconds)
    merged = shares_df.merge(quotes_df[["code", "名称", "rt_price", "昨收", "今开", "最高", "最低", "成交额"]], on="code", how="inner")
    merged = merged[merged["name"].map(is_tradable_name)].copy()
    merged["market_cap"] = merged["rt_price"] * merged["total_shares"]
    merged = merged.dropna(subset=["market_cap"]).sort_values("market_cap").head(TOP_N).copy()
    merged["rank"] = np.arange(1, len(merged) + 1)
    merged["target_weight"] = 1.0 / TOP_N
    merged["symbol"] = merged["code"]
    merged["name"] = merged["名称"].fillna(merged["name"])
    cols = ["rank", "symbol", "name", "rt_price", "market_cap", "target_weight", "change_date", "今开", "最高", "最低", "成交额"]
    out = merged[cols].reset_index(drop=True)
    if capital is not None and not out.empty:
        out["target_notional"] = capital * out["target_weight"]
    return out, quote_source


def build_realtime_signal(context: dict[str, object], cache_seconds: int) -> tuple[pd.DataFrame, dict[str, object]]:
    close_df = context["close_df"].copy()
    effective_members = context["effective_members"].copy()
    latest_trade_date = pd.Timestamp(close_df.index[-1])
    member_symbols = effective_members["symbol"].astype(str).tolist()
    last_close_map = ensure_realtime_last_close_map(member_symbols, as_of_date=latest_trade_date)

    quotes_df, quote_source = build_realtime_quote_map(cache_seconds)
    quotes_df = quotes_df.set_index("code")

    member_returns: list[float] = []
    available_rows = 0
    for symbol in member_symbols:
        last_close = last_close_map.get(symbol)
        if last_close is None or last_close <= 0:
            continue
        if symbol not in quotes_df.index:
            continue
        rt_price = pd.to_numeric(quotes_df.at[symbol, "rt_price"], errors="coerce")
        if pd.isna(rt_price) or rt_price <= 0:
            continue
        member_returns.append(float(rt_price / last_close - 1.0))
        available_rows += 1

    if not member_returns:
        raise ValueError("无法计算实时信号: 当前成分股没有可用实时价格。")
    ensure_realtime_quote_coverage(available_rows, len(member_symbols))

    last_microcap_close = float(close_df["microcap"].iloc[-1])
    microcap_rt_close = last_microcap_close * (1.0 + float(np.mean(member_returns)))

    index_spot = normalize_index_spot_columns(load_or_refresh_index_spot(cache_seconds))
    index_spot["代码"] = index_spot["代码"].astype(str).str.zfill(6)
    hedge_row = index_spot.loc[index_spot["代码"] == "000852"]
    if hedge_row.empty:
        hedge_rt_close = float(close_df["hedge"].iloc[-1])
        hedge_source = "latest_cached_close_fallback"
    else:
        hedge_row = hedge_row.iloc[0]
        hedge_rt_close = pd.to_numeric(hedge_row.get("最新价"), errors="coerce")
        hedge_prev = pd.to_numeric(hedge_row.get("昨收"), errors="coerce")
        if pd.isna(hedge_rt_close) or hedge_rt_close <= 0:
            hedge_rt_close = hedge_prev if pd.notna(hedge_prev) and hedge_prev > 0 else float(close_df["hedge"].iloc[-1])
            hedge_source = "index_prev_close_fallback"
        else:
            hedge_source = "index_spot_latest"

    snapshot_ts = _cn_timestamp()
    quote_trade_date = ""
    if "trade_date" in quotes_df.columns:
        trade_dates = quotes_df["trade_date"].dropna().astype(str)
        if not trade_dates.empty:
            quote_trade_date = trade_dates.max()
    rt_close_df = apply_realtime_close_to_signal_frame(
        close_df=close_df,
        latest_trade_date=latest_trade_date,
        snapshot_ts=snapshot_ts,
        microcap_rt_close=microcap_rt_close,
        hedge_rt_close=float(hedge_rt_close),
        quote_trade_date=quote_trade_date,
    )
    rt_result = run_signal(rt_close_df)
    latest_rt_signal = enrich_signal_frame(hedge_mod.build_latest_signal(rt_result), rt_result)
    latest_rt_signal["date"] = snapshot_ts
    latest_rt_signal["quote_source"] = quote_source
    latest_rt_signal["hedge_quote_source"] = hedge_source
    latest_rt_signal["member_price_count"] = available_rows
    latest_rt_signal["member_count"] = len(member_symbols)
    latest_rt_signal["latest_anchor_trade_date"] = latest_trade_date
    if quote_trade_date:
        latest_rt_signal["quote_trade_date"] = quote_trade_date
    fallback_warning = str(context.get("fallback_warning") or "")
    if fallback_warning:
        latest_rt_signal["fallback_warning"] = fallback_warning

    meta = {
        "snapshot_time": str(snapshot_ts),
        "latest_anchor_trade_date": str(latest_trade_date.date()),
        "quote_source": quote_source,
        "hedge_quote_source": hedge_source,
        "member_price_count": available_rows,
        "member_count": len(member_symbols),
        "microcap_rt_close": float(microcap_rt_close),
        "hedge_rt_close": float(hedge_rt_close),
        "quote_trade_date": quote_trade_date,
    }
    if fallback_warning:
        meta["fallback_warning"] = fallback_warning
    return latest_rt_signal, meta


def build_realtime_signal_fast(context: dict[str, object]) -> tuple[pd.DataFrame, dict[str, object]]:
    close_df = context["close_df"].copy()
    effective_members = context["effective_members"].copy()
    latest_trade_date = pd.Timestamp(close_df.index[-1])
    member_symbols = effective_members["symbol"].astype(str).str.zfill(6).tolist()
    last_close_map = ensure_realtime_last_close_map(member_symbols, as_of_date=latest_trade_date)
    allow_quote_pre_close_after_anchor = bool(context.get("fallback_warning"))

    member_returns: list[float] = []
    missing_symbols: list[dict[str, object]] = []
    available_rows = 0
    quote_source = "eastmoney_stock_get_member_only"
    quotes_df = pd.DataFrame(index=pd.Index([], dtype=str))
    flat_fallback_count = 0
    for attempt in range(1, REALTIME_QUOTE_FETCH_ATTEMPTS + 1):
        raw_quotes_df = fetch_member_realtime_quotes(member_symbols)
        quote_source = str(raw_quotes_df.attrs.get("quote_source") or "eastmoney_stock_get_member_only")
        raw_quotes_df, flat_fallback_count = add_last_close_flat_fallback_quotes(
            raw_quotes_df,
            member_symbols=member_symbols,
            last_close_map=last_close_map,
            latest_trade_date=latest_trade_date,
        )
        if flat_fallback_count:
            quote_source = f"{quote_source}+latest_close_flat_fallback"
        quotes_df = (
            raw_quotes_df.set_index("code")
            if not raw_quotes_df.empty
            else pd.DataFrame(index=pd.Index([], dtype=str))
        )
        member_returns, missing_symbols = compute_member_realtime_returns(
            member_symbols=member_symbols,
            effective_members=effective_members,
            last_close_map=last_close_map,
            quotes_df=quotes_df,
            latest_trade_date=latest_trade_date,
            allow_quote_pre_close_after_anchor=allow_quote_pre_close_after_anchor,
        )
        if missing_symbols:
            refreshed_last_close_map = maybe_refresh_missing_realtime_last_close_map(
                last_close_map=last_close_map,
                missing_symbols=missing_symbols,
                as_of_date=latest_trade_date,
            )
            if refreshed_last_close_map != last_close_map:
                last_close_map = refreshed_last_close_map
                member_returns, missing_symbols = compute_member_realtime_returns(
                    member_symbols=member_symbols,
                    effective_members=effective_members,
                    last_close_map=last_close_map,
                    quotes_df=quotes_df,
                    latest_trade_date=latest_trade_date,
                    allow_quote_pre_close_after_anchor=allow_quote_pre_close_after_anchor,
                )
        available_rows = len(member_returns)
        if available_rows >= len(member_symbols):
            break
        if attempt < REALTIME_QUOTE_FETCH_ATTEMPTS:
            time.sleep(REALTIME_QUOTE_RETRY_SECONDS)

    if not member_returns:
        raise ValueError("无法计算实时信号: 当前成分股没有可用实时价格。")
    ensure_realtime_quote_coverage(
        available_rows,
        len(member_symbols),
        missing_symbols=missing_symbols,
        quote_source=quote_source,
    )

    last_microcap_close = float(close_df["microcap"].iloc[-1])
    microcap_rt_close = last_microcap_close * (1.0 + float(np.mean(member_returns)))

    try:
        hedge_rt_close, hedge_source, hedge_quote_trade_date = normalize_hedge_realtime_quote_result(
            fetch_hedge_realtime_quote_fast()
        )
    except Exception:
        hedge_rt_close = float(close_df["hedge"].iloc[-1])
        hedge_source = "latest_cached_close_fallback"
        hedge_quote_trade_date = ""

    snapshot_ts = _cn_timestamp()
    quote_stats = extract_member_quote_trade_date_stats(quotes_df, member_symbols, latest_trade_date)
    quote_trade_date = str(quote_stats["member_quote_trade_date_max"])
    rt_close_df = apply_realtime_close_to_signal_frame(
        close_df=close_df,
        latest_trade_date=latest_trade_date,
        snapshot_ts=snapshot_ts,
        microcap_rt_close=microcap_rt_close,
        hedge_rt_close=float(hedge_rt_close),
        quote_trade_date=quote_trade_date,
    )
    rt_result = run_signal(rt_close_df)
    signal_df = enrich_signal_frame(hedge_mod.build_latest_signal(rt_result), rt_result)
    signal_df = augment_signal_with_member_rebalance(signal_df, context.get("changes_df"))
    jitter_level, jitter_note = classify_tail_jitter_risk(float(signal_df.iloc[0]["momentum_gap"]))
    signal_df["date"] = snapshot_ts
    signal_df["signal_timing"] = "intraday_hypothetical_if_now_close"
    signal_df["official_close_confirmed_signal"] = False
    signal_df["quote_source"] = quote_source
    signal_df["hedge_quote_source"] = hedge_source
    signal_df["member_price_count"] = available_rows
    signal_df["member_count"] = len(member_symbols)
    signal_df["latest_anchor_trade_date"] = latest_trade_date
    signal_df["member_quote_trade_date_count"] = quote_stats["member_quote_trade_date_count"]
    signal_df["member_quote_trade_date_min"] = quote_stats["member_quote_trade_date_min"]
    signal_df["member_quote_trade_date_max"] = quote_stats["member_quote_trade_date_max"]
    signal_df["member_quote_bad_symbols"] = json.dumps(quote_stats["member_quote_bad_symbols"], ensure_ascii=False)
    signal_df["member_quote_flat_fallback_count"] = flat_fallback_count
    signal_df["hedge_quote_trade_date"] = hedge_quote_trade_date
    if quote_trade_date:
        signal_df["quote_trade_date"] = quote_trade_date
    signal_df["tail_jitter_risk"] = jitter_level
    signal_df["tail_jitter_note"] = jitter_note
    fallback_warning = str(context.get("fallback_warning") or "")
    if fallback_warning:
        signal_df["fallback_warning"] = fallback_warning

    meta = {
        "snapshot_time": str(snapshot_ts),
        "latest_anchor_trade_date": str(latest_trade_date.date()),
        "quote_source": quote_source,
        "hedge_quote_source": hedge_source,
        "member_price_count": available_rows,
        "member_count": len(member_symbols),
        "microcap_rt_close": float(microcap_rt_close),
        "hedge_rt_close": float(hedge_rt_close),
        "quote_trade_date": quote_trade_date,
        **quote_stats,
        "member_quote_flat_fallback_count": flat_fallback_count,
        "hedge_quote_trade_date": hedge_quote_trade_date,
        "tail_jitter_risk": jitter_level,
        "tail_jitter_note": jitter_note,
    }
    if fallback_warning:
        meta["fallback_warning"] = fallback_warning
    return signal_df, meta


def load_cached_fast_realtime_signal(
    paths: dict[str, Path],
    cache_seconds: int,
    latest_anchor_trade_date: pd.Timestamp,
) -> tuple[pd.DataFrame, dict[str, object], float] | None:
    if cache_seconds <= 0:
        return None
    meta_path = paths["cache_fast_realtime_meta"]
    signal_path = paths["cache_fast_realtime_signal"]
    if not meta_path.exists() or not signal_path.exists():
        return None
    cache_age_seconds = time.time() - meta_path.stat().st_mtime
    if cache_age_seconds > cache_seconds:
        return None
    try:
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
        if meta.get("latest_anchor_trade_date") != str(pd.Timestamp(latest_anchor_trade_date).date()):
            return None
        assert_realtime_meta_is_actionable(meta)
        signal_df = pd.read_csv(signal_path)
        signal_df["signal_timing"] = "intraday_hypothetical_if_now_close"
        signal_df["official_close_confirmed_signal"] = False
        return signal_df, meta, float(cache_age_seconds)
    except Exception:
        return None


def save_cached_fast_realtime_signal(paths: dict[str, Path], signal_df: pd.DataFrame, meta: dict[str, object]) -> None:
    assert_realtime_meta_is_actionable(meta)
    REALTIME_DIR.mkdir(parents=True, exist_ok=True)
    with _cache_write_lock(REALTIME_DIR / f"{paths['cache_fast_realtime_meta'].stem}.lock"):
        _atomic_to_csv(signal_df, paths["cache_fast_realtime_signal"], index=False, encoding="utf-8")
        _atomic_write_json(paths["cache_fast_realtime_meta"], meta, encoding="utf-8")


def load_realtime_eligible_codes() -> set[str]:
    universe = pd.read_csv(freq_mod.ACTIVE_UNIVERSE, dtype=str)
    st_codes = set(pd.read_csv(freq_mod.CURRENT_ST, dtype=str)["code"].dropna().astype(str).str.zfill(6))
    universe["code"] = universe["code"].astype(str).str.zfill(6)
    universe = universe[~universe["code"].isin(st_codes)].copy()
    universe = universe[universe["name"].map(is_tradable_name)].copy()
    return set(universe["code"].tolist())


def fetch_realtime_smallcap_members_fast(
    effective_date: pd.Timestamp | None,
    capital: float | None,
    target_size: int = TOP_N,
) -> tuple[pd.DataFrame, str]:
    eligible_codes = load_realtime_eligible_codes()
    rows: list[dict[str, object]] = []
    required_valid = max(int(target_size) * 2, 240)
    page = 1
    while len(rows) < required_valid and page <= 12:
        url = (
            "https://push2.eastmoney.com/api/qt/clist/get"
            f"?pn={page}&pz=100&po=0&np=1&fltt=2&invt=2&fid=f20"
            "&fs=m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23"
            "&fields=f12,f14,f2,f3,f17,f15,f16,f18,f20,f6"
        )
        response = requests.get(
            url,
            timeout=10,
            headers={"Referer": "https://quote.eastmoney.com/", "User-Agent": "Mozilla/5.0"},
        )
        response.raise_for_status()
        data = response.json().get("data") or {}
        diff = data.get("diff") or []
        if not diff:
            break
        for item in diff:
            code = str(item.get("f12") or "").zfill(6)
            name = str(item.get("f14") or "")
            latest = pd.to_numeric(item.get("f2"), errors="coerce")
            market_cap = pd.to_numeric(item.get("f20"), errors="coerce")
            if (
                not code
                or code not in eligible_codes
                or code.startswith(("8", "4"))
                or (not is_tradable_name(name))
                or pd.isna(latest)
                or float(latest) <= 0
                or pd.isna(market_cap)
                or float(market_cap) <= 0
            ):
                continue
            rows.append(
                {
                    "symbol": code,
                    "name": name,
                    "rt_price": float(latest),
                    "market_cap": float(market_cap),
                    "target_weight": 1.0 / TOP_N,
                    "open_price": pd.to_numeric(item.get("f17"), errors="coerce"),
                    "high_price": pd.to_numeric(item.get("f15"), errors="coerce"),
                    "low_price": pd.to_numeric(item.get("f16"), errors="coerce"),
                    "prev_close": pd.to_numeric(item.get("f18"), errors="coerce"),
                    "amount": pd.to_numeric(item.get("f6"), errors="coerce"),
                    "signal_date": _cn_local_day().date(),
                    "effective_date": None if effective_date is None else pd.Timestamp(effective_date).date(),
                }
            )
        page += 1

    frame = pd.DataFrame(rows).drop_duplicates(subset="symbol")
    if frame.empty:
        raise RuntimeError("实时进出名单快速路径未获取到有效股票。")
    frame = frame.sort_values("market_cap").head(target_size).copy()
    if len(frame) < target_size:
        raise RuntimeError(f"实时进出名单快速路径仅得到 {len(frame)}/{target_size} 只股票。")
    frame["rank"] = np.arange(1, len(frame) + 1)
    frame = frame[
        [
            "rank",
            "symbol",
            "name",
            "rt_price",
            "market_cap",
            "target_weight",
            "open_price",
            "high_price",
            "low_price",
            "prev_close",
            "amount",
            "signal_date",
            "effective_date",
        ]
    ].reset_index(drop=True)
    frame = add_capital_columns(frame, capital)
    return frame, "eastmoney_clist_f20_sorted"


def compute_realtime_state_fast(
    context: dict[str, object],
    cache_seconds: int,
    capital: float | None,
    allow_stale_anchor: bool = False,
) -> dict[str, object]:
    anchor_freshness = context.get("anchor_freshness", {})
    if bool(anchor_freshness.get("is_stale")) and not allow_stale_anchor:
        raise RuntimeError(format_anchor_stale_message(anchor_freshness))

    paths = context["paths"]
    latest_trade_date = pd.Timestamp(context["close_df"].index[-1])
    latest_rebalance = pd.Timestamp(context["latest_rebalance"])
    effective_rebalance = context.get("effective_rebalance")
    rebalance_effective_date = context.get("rebalance_effective_date")

    cached = load_cached_realtime_state(
        paths=paths,
        cache_seconds=cache_seconds,
        latest_anchor_trade_date=latest_trade_date,
        latest_rebalance=latest_rebalance,
        effective_rebalance=effective_rebalance,
        rebalance_effective_date=rebalance_effective_date,
        capital=capital,
    )
    if cached is not None:
        return cached

    members_out, quote_source = fetch_realtime_smallcap_members_fast(rebalance_effective_date, capital, target_size=TOP_N)
    current_members = context["effective_members"][["symbol", "rank", "name"]].copy()
    current_members["symbol"] = current_members["symbol"].astype(str).str.zfill(6)
    members_for_diff = members_out[["symbol", "rank", "name", "market_cap", "rt_price"]].copy()
    members_for_diff["symbol"] = members_for_diff["symbol"].astype(str).str.zfill(6)
    changes_df = build_change_table(current_members, members_for_diff[["symbol", "rank", "name"]])
    if not changes_df.empty:
        rt_cap_map = dict(zip(members_for_diff["symbol"], members_for_diff["market_cap"]))
        rt_price_map = dict(zip(members_for_diff["symbol"], members_for_diff["rt_price"]))
        changes_df["realtime_market_cap"] = changes_df["symbol"].map(rt_cap_map)
        changes_df["realtime_price"] = changes_df["symbol"].map(rt_price_map)
        changes_df["signal_date"] = latest_rebalance.date()
        changes_df["effective_date"] = None if rebalance_effective_date is None else pd.Timestamp(rebalance_effective_date).date()

    signal_df, signal_meta = build_realtime_signal_fast(context)
    signal_df["member_list_quote_source"] = quote_source
    meta = {
        "snapshot_time": signal_meta["snapshot_time"],
        "latest_anchor_trade_date": signal_meta["latest_anchor_trade_date"],
        "latest_rebalance": str(latest_rebalance.date()),
        "effective_rebalance": None if effective_rebalance is None else str(pd.Timestamp(effective_rebalance).date()),
        "rebalance_effective_date": None if rebalance_effective_date is None else str(pd.Timestamp(rebalance_effective_date).date()),
        "quote_source": quote_source,
        "member_list_quote_source": quote_source,
        "signal_member_quote_source": signal_meta["quote_source"],
        "signal_hedge_quote_source": signal_meta["hedge_quote_source"],
        "hedge_quote_source": signal_meta["hedge_quote_source"],
        "member_price_count": signal_meta["member_price_count"],
        "member_count": signal_meta["member_count"],
        "microcap_rt_close": signal_meta["microcap_rt_close"],
        "hedge_rt_close": signal_meta["hedge_rt_close"],
        "quote_trade_date": signal_meta.get("quote_trade_date", ""),
        "member_quote_trade_date_count": signal_meta.get("member_quote_trade_date_count", 0),
        "member_quote_trade_date_min": signal_meta.get("member_quote_trade_date_min", ""),
        "member_quote_trade_date_max": signal_meta.get("member_quote_trade_date_max", ""),
        "member_quote_bad_symbols": signal_meta.get("member_quote_bad_symbols", []),
        "hedge_quote_trade_date": signal_meta.get("hedge_quote_trade_date", ""),
        "tail_jitter_risk": signal_meta.get("tail_jitter_risk"),
        "tail_jitter_note": signal_meta.get("tail_jitter_note"),
    }
    if signal_meta.get("fallback_warning"):
        meta["fallback_warning"] = signal_meta["fallback_warning"]
    members_out, changes_df = mark_realtime_preview_outputs(members_out, changes_df)
    if realtime_meta_is_actionable(meta):
        save_realtime_state_cache(
            paths=paths,
            meta=meta,
            signal_df=signal_df,
            members_df=members_out.drop(columns=["target_notional"], errors="ignore"),
            changes_df=changes_df,
        )
    return {
        "meta": meta,
        "signal": signal_df,
        "members": members_out,
        "changes": changes_df,
        "from_cache": False,
        "cache_age_seconds": 0.0,
    }


def load_cached_realtime_state(
    paths: dict[str, Path],
    cache_seconds: int,
    latest_anchor_trade_date: pd.Timestamp,
    latest_rebalance: pd.Timestamp,
    effective_rebalance: pd.Timestamp | None,
    rebalance_effective_date: pd.Timestamp | None,
    capital: float | None,
) -> dict[str, object] | None:
    meta_path = paths["cache_realtime_meta"]
    signal_path = paths["cache_realtime_signal"]
    members_path = paths["cache_realtime_members"]
    changes_path = paths["cache_realtime_changes"]
    needed = [meta_path, signal_path, members_path, changes_path]
    if not all(path.exists() for path in needed):
        return None
    cache_age_seconds = time.time() - meta_path.stat().st_mtime
    if cache_age_seconds > cache_seconds:
        return None
    try:
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
        expected = {
            "latest_anchor_trade_date": str(pd.Timestamp(latest_anchor_trade_date).date()),
            "latest_rebalance": str(pd.Timestamp(latest_rebalance).date()),
            "effective_rebalance": None if effective_rebalance is None else str(pd.Timestamp(effective_rebalance).date()),
            "rebalance_effective_date": None if rebalance_effective_date is None else str(pd.Timestamp(rebalance_effective_date).date()),
        }
        if any(meta.get(key) != value for key, value in expected.items()):
            return None
        assert_realtime_meta_is_actionable(meta)
        signal_df = pd.read_csv(signal_path)
        members_df = pd.read_csv(members_path, dtype={"symbol": str})
        changes_df = pd.read_csv(changes_path, dtype={"symbol": str})
        signal_df = augment_signal_with_member_rebalance(signal_df, changes_df)
        members_df, changes_df = mark_realtime_preview_outputs(members_df, changes_df)
        members_df = add_capital_columns(members_df, capital)
        return {
            "meta": meta,
            "signal": signal_df,
            "members": members_df,
            "changes": changes_df,
            "from_cache": True,
            "cache_age_seconds": float(cache_age_seconds),
        }
    except Exception:
        return None


def save_realtime_state_cache(
    paths: dict[str, Path],
    meta: dict[str, object],
    signal_df: pd.DataFrame,
    members_df: pd.DataFrame,
    changes_df: pd.DataFrame,
) -> None:
    assert_realtime_meta_is_actionable(meta)
    REALTIME_DIR.mkdir(parents=True, exist_ok=True)
    with _cache_write_lock(REALTIME_DIR / f"{paths['cache_realtime_meta'].stem}.lock"):
        _atomic_to_csv(signal_df, paths["cache_realtime_signal"], index=False, encoding="utf-8")
        _atomic_to_csv(members_df, paths["cache_realtime_members"], index=False, encoding="utf-8")
        _atomic_to_csv(changes_df, paths["cache_realtime_changes"], index=False, encoding="utf-8")
        _atomic_write_json(paths["cache_realtime_meta"], meta, encoding="utf-8")


def mark_realtime_preview_outputs(
    members_df: pd.DataFrame,
    changes_df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    members_out = members_df.copy()
    changes_out = changes_df.copy()
    for frame in (members_out, changes_out):
        frame["member_list_type"] = "intraday_preview"
        frame["official_rebalance"] = False
    return members_out, changes_out


def compute_realtime_state(
    context: dict[str, object],
    cache_seconds: int,
    capital: float | None,
    allow_stale_anchor: bool = False,
) -> dict[str, object]:
    anchor_freshness = context.get("anchor_freshness", {})
    if bool(anchor_freshness.get("is_stale")) and not allow_stale_anchor:
        raise RuntimeError(format_anchor_stale_message(anchor_freshness))

    paths = context["paths"]
    latest_trade_date = pd.Timestamp(context["close_df"].index[-1])
    latest_rebalance = pd.Timestamp(context["latest_rebalance"])
    effective_rebalance = context.get("effective_rebalance")
    rebalance_effective_date = context.get("rebalance_effective_date")

    cached = load_cached_realtime_state(
        paths=paths,
        cache_seconds=cache_seconds,
        latest_anchor_trade_date=latest_trade_date,
        latest_rebalance=latest_rebalance,
        effective_rebalance=effective_rebalance,
        rebalance_effective_date=rebalance_effective_date,
        capital=capital,
    )
    if cached is not None:
        return cached

    shares_df = load_or_refresh_latest_shares()
    quotes_df, quote_source = build_realtime_quote_map(cache_seconds)
    quotes_small = quotes_df[["code", "名称", "rt_price", "昨收", "今开", "最高", "最低", "成交额"]].copy()

    realtime_members = shares_df.merge(quotes_small, on="code", how="inner")
    realtime_members = realtime_members[realtime_members["name"].map(is_tradable_name)].copy()
    realtime_members["market_cap"] = realtime_members["rt_price"] * realtime_members["total_shares"]
    realtime_members = realtime_members.dropna(subset=["market_cap"]).sort_values("market_cap").head(TOP_N).copy()
    realtime_members["rank"] = np.arange(1, len(realtime_members) + 1)
    realtime_members["target_weight"] = 1.0 / TOP_N
    realtime_members["symbol"] = realtime_members["code"]
    realtime_members["name"] = realtime_members["名称"].fillna(realtime_members["name"])
    realtime_members["signal_date"] = latest_rebalance.date()
    realtime_members["effective_date"] = None if rebalance_effective_date is None else pd.Timestamp(rebalance_effective_date).date()
    members_out = realtime_members[
        ["rank", "symbol", "name", "rt_price", "market_cap", "target_weight", "change_date", "今开", "最高", "最低", "成交额", "signal_date", "effective_date"]
    ].reset_index(drop=True)
    members_out = add_capital_columns(members_out, capital)

    current_members = context["effective_members"].copy()
    current_members["symbol"] = current_members["symbol"].astype(str)
    members_for_diff = members_out.copy()
    members_for_diff["symbol"] = members_for_diff["symbol"].astype(str)
    changes_df = build_change_table(current_members[["symbol", "rank", "name"]], members_for_diff[["symbol", "rank", "name"]])
    if not changes_df.empty:
        rt_cap_map = dict(zip(members_for_diff["symbol"], members_for_diff["market_cap"]))
        rt_price_map = dict(zip(members_for_diff["symbol"], members_for_diff["rt_price"]))
        changes_df["realtime_market_cap"] = changes_df["symbol"].map(rt_cap_map)
        changes_df["realtime_price"] = changes_df["symbol"].map(rt_price_map)
        changes_df["signal_date"] = latest_rebalance.date()
        changes_df["effective_date"] = None if rebalance_effective_date is None else pd.Timestamp(rebalance_effective_date).date()

    close_df = context["close_df"].copy()
    effective_members_df = context["effective_members"].copy()
    member_symbols = effective_members_df["symbol"].astype(str).tolist()
    last_close_map = ensure_realtime_last_close_map(member_symbols, as_of_date=latest_trade_date)
    quotes_indexed = quotes_df.set_index("code")
    member_returns: list[float] = []
    available_rows = 0
    for symbol in member_symbols:
        last_close = last_close_map.get(symbol)
        if last_close is None or last_close <= 0 or symbol not in quotes_indexed.index:
            continue
        rt_price = pd.to_numeric(quotes_indexed.at[symbol, "rt_price"], errors="coerce")
        if pd.isna(rt_price) or rt_price <= 0:
            continue
        member_returns.append(float(rt_price / last_close - 1.0))
        available_rows += 1
    if not member_returns:
        raise ValueError("无法计算实时信号: 当前成分股没有可用实时价格。")
    ensure_realtime_quote_coverage(available_rows, len(member_symbols))

    last_microcap_close = float(close_df["microcap"].iloc[-1])
    microcap_rt_close = last_microcap_close * (1.0 + float(np.mean(member_returns)))

    index_spot = normalize_index_spot_columns(load_or_refresh_index_spot(cache_seconds))
    index_spot["代码"] = index_spot["代码"].astype(str).str.zfill(6)
    hedge_row = index_spot.loc[index_spot["代码"] == "000852"]
    if hedge_row.empty:
        hedge_rt_close = float(close_df["hedge"].iloc[-1])
        hedge_source = "latest_cached_close_fallback"
    else:
        hedge_row = hedge_row.iloc[0]
        hedge_rt_close = pd.to_numeric(hedge_row.get("最新价"), errors="coerce")
        hedge_prev = pd.to_numeric(hedge_row.get("昨收"), errors="coerce")
        if pd.isna(hedge_rt_close) or hedge_rt_close <= 0:
            hedge_rt_close = hedge_prev if pd.notna(hedge_prev) and hedge_prev > 0 else float(close_df["hedge"].iloc[-1])
            hedge_source = "index_prev_close_fallback"
        else:
            hedge_source = "index_spot_latest"

    snapshot_ts = _cn_timestamp()
    quote_stats = extract_member_quote_trade_date_stats(quotes_indexed, member_symbols, latest_trade_date)
    quote_trade_date = str(quote_stats["member_quote_trade_date_max"])
    rt_close_df = apply_realtime_close_to_signal_frame(
        close_df=close_df,
        latest_trade_date=latest_trade_date,
        snapshot_ts=snapshot_ts,
        microcap_rt_close=microcap_rt_close,
        hedge_rt_close=float(hedge_rt_close),
        quote_trade_date=quote_trade_date,
    )
    rt_result = run_signal(rt_close_df)
    signal_df = enrich_signal_frame(hedge_mod.build_latest_signal(rt_result), rt_result)
    signal_df = augment_signal_with_member_rebalance(signal_df, context.get("changes_df"))
    jitter_level, jitter_note = classify_tail_jitter_risk(float(signal_df.iloc[0]["momentum_gap"]))
    signal_df["date"] = snapshot_ts
    signal_df["quote_source"] = quote_source
    signal_df["hedge_quote_source"] = hedge_source
    signal_df["member_price_count"] = available_rows
    signal_df["member_count"] = len(member_symbols)
    signal_df["latest_anchor_trade_date"] = latest_trade_date
    signal_df["member_quote_trade_date_count"] = quote_stats["member_quote_trade_date_count"]
    signal_df["member_quote_trade_date_min"] = quote_stats["member_quote_trade_date_min"]
    signal_df["member_quote_trade_date_max"] = quote_stats["member_quote_trade_date_max"]
    signal_df["member_quote_bad_symbols"] = json.dumps(quote_stats["member_quote_bad_symbols"], ensure_ascii=False)
    signal_df["hedge_quote_trade_date"] = ""
    if quote_trade_date:
        signal_df["quote_trade_date"] = quote_trade_date
    signal_df["tail_jitter_risk"] = jitter_level
    signal_df["tail_jitter_note"] = jitter_note
    fallback_warning = str(context.get("fallback_warning") or "")
    if fallback_warning:
        signal_df["fallback_warning"] = fallback_warning

    meta = {
        "snapshot_time": str(snapshot_ts),
        "latest_anchor_trade_date": str(latest_trade_date.date()),
        "latest_rebalance": str(latest_rebalance.date()),
        "effective_rebalance": None if effective_rebalance is None else str(pd.Timestamp(effective_rebalance).date()),
        "rebalance_effective_date": None if rebalance_effective_date is None else str(pd.Timestamp(rebalance_effective_date).date()),
        "quote_source": quote_source,
        "member_list_quote_source": quote_source,
        "signal_member_quote_source": quote_source,
        "signal_hedge_quote_source": hedge_source,
        "hedge_quote_source": hedge_source,
        "member_price_count": available_rows,
        "member_count": len(member_symbols),
        "microcap_rt_close": float(microcap_rt_close),
        "hedge_rt_close": float(hedge_rt_close),
        "quote_trade_date": quote_trade_date,
        **quote_stats,
        "hedge_quote_trade_date": "",
        "tail_jitter_risk": jitter_level,
        "tail_jitter_note": jitter_note,
    }
    if fallback_warning:
        meta["fallback_warning"] = fallback_warning
    members_out, changes_df = mark_realtime_preview_outputs(members_out, changes_df)
    if realtime_meta_is_actionable(meta):
        save_realtime_state_cache(
            paths=paths,
            meta=meta,
            signal_df=signal_df,
            members_df=members_out.drop(columns=["target_notional"], errors="ignore"),
            changes_df=changes_df,
        )
    return {
        "meta": meta,
        "signal": signal_df,
        "members": members_out,
        "changes": changes_df,
        "from_cache": False,
        "cache_age_seconds": 0.0,
    }


def format_table(df: pd.DataFrame, max_rows: int = 20) -> str:
    if df.empty:
        return "(empty)"
    return df.head(max_rows).to_string(index=False)


def handle_query(context: dict[str, object], args: argparse.Namespace, query: str) -> None:
    query = normalize_query_text(query).strip()
    paths = context["paths"]
    latest_rebalance = context.get("latest_rebalance")
    if latest_rebalance is not None:
        latest_rebalance = pd.Timestamp(latest_rebalance)
    rebalance_effective_date = context.get("rebalance_effective_date")
    anchor_freshness = context.get("anchor_freshness", {})
    if {"result", "latest_signal", "summary", "target_members", "changes_df"}.issubset(context):
        save_base_outputs(context)

    if query == "\u5b9e\u65f6\u4fe1\u53f7":
        ensure_realtime_anchor_is_fresh(context, args)
        latest_anchor_trade_date = pd.Timestamp(context["close_df"].index[-1])
        try:
            cached_fast = load_cached_fast_realtime_signal(
                paths=paths,
                cache_seconds=args.realtime_cache_seconds,
                latest_anchor_trade_date=latest_anchor_trade_date,
            )
            if cached_fast is None:
                rt_signal, meta = build_realtime_signal_fast(context)
                fresh_fast = True
                cache_age_seconds = 0.0
                result_source = "fresh_fast"
            else:
                rt_signal, meta, cache_age_seconds = cached_fast
                fresh_fast = False
                result_source = "cache_fast"
        except Exception:
            realtime_state = compute_realtime_state(
                context,
                args.realtime_cache_seconds,
                args.capital,
                allow_stale_anchor=args.allow_stale_realtime,
            )
            rt_signal = realtime_state["signal"]
            meta = realtime_state["meta"]
            cache_age_seconds = float(realtime_state.get("cache_age_seconds", 0.0))
            result_source = "cache" if realtime_state["from_cache"] else "fresh_fallback"
            fresh_fast = False
        assert_realtime_meta_is_actionable(meta)
        assert_signal_matches_result(rt_signal, rebuild_realtime_result_from_meta(context, meta))
        if fresh_fast:
            save_cached_fast_realtime_signal(paths, rt_signal, meta)
        _atomic_to_csv(rt_signal, paths["realtime_signal"], index=False, encoding="utf-8")
        gap_value = float(rt_signal.iloc[0]["momentum_gap"])
        jitter_risk = str(rt_signal.iloc[0].get("tail_jitter_risk", "normal"))
        jitter_note = str(rt_signal.iloc[0].get("tail_jitter_note", "") or "")
        print("\u5b9e\u65f6\u4fe1\u53f7")
        print(format_table(rt_signal))
        print(f"\u5b9e\u65f6\u5feb\u7167\u65f6\u95f4: {meta['snapshot_time']}")
        print(f"\u5386\u53f2\u951a\u70b9\u4ea4\u6613\u65e5: {meta['latest_anchor_trade_date']}")
        print(f"实时信号成员股报价来源: {meta.get('signal_member_quote_source', meta.get('quote_source'))}")
        print(f"实时信号对冲腿报价来源: {meta.get('signal_hedge_quote_source', meta.get('hedge_quote_source'))}")
        print(f"\u5c3e\u76d8\u6296\u52a8\u98ce\u9669: {jitter_risk} (|gap|={abs(gap_value):.4%})")
        if jitter_risk != "normal" and jitter_note:
            print(f"\u63d0\u793a: {jitter_note}")
        print(f"\u7ed3\u679c\u6765\u6e90: {result_source}")
        print(f"\u5b9e\u65f6\u7ed3\u679c\u5e74\u9f84: {cache_age_seconds:.1f} \u79d2")
        print(f"\u5df2\u4fdd\u5b58: {paths['realtime_signal'].name}")
        return

    if query == "信号":
        ensure_closed_signal_anchor_is_fresh(context)
        latest_signal = context["latest_signal"]
        assert_proxy_tail_is_actionable(args.index_csv, pd.Timestamp(context["close_df"].index[-1]))
        assert_signal_matches_result(latest_signal, context["result"])
        _atomic_to_csv(latest_signal, paths["signal"], index=False, encoding="utf-8")
        print("确认信号")
        print(format_table(latest_signal))
        if anchor_freshness:
            print(
                "历史锚点: {status} | latest={latest} | today={today} | lag={lag}d".format(
                    status=anchor_freshness.get("status"),
                    latest=anchor_freshness.get("latest_trade_date"),
                    today=anchor_freshness.get("current_date"),
                    lag=anchor_freshness.get("stale_calendar_days"),
                )
            )
        print(f"已保存: {paths['signal'].name}")
        return

    if query == "成分股":
        members = context["target_members"]
        _atomic_to_csv(members, paths["members"], index=False, encoding="utf-8")
        print("最新成分股")
        print(f"信号日: {latest_rebalance.date()}")
        print(
            "生效日: {}".format(
                "暂无下一交易日" if rebalance_effective_date is None else pd.Timestamp(rebalance_effective_date).date()
            )
        )
        print(format_table(members[["rank", "symbol", "name", "market_cap", "target_weight", "signal_date", "effective_date"]], max_rows=TOP_N))
        print(f"已保存: {paths['members'].name}")
        return

    if query == "进出名单":
        changes = context["changes_df"]
        _atomic_to_csv(changes, paths["changes"], index=False, encoding="utf-8")
        print("最新进出名单")
        print(f"信号日: {latest_rebalance.date()}")
        print(
            "生效日: {}".format(
                "暂无下一交易日" if rebalance_effective_date is None else pd.Timestamp(rebalance_effective_date).date()
            )
        )
        print(format_table(changes))
        print(f"已保存: {paths['changes'].name}")
        return

    if query == "实时进出名单":
        try:
            realtime_state = compute_realtime_state_fast(
                context,
                args.realtime_cache_seconds,
                args.capital,
                allow_stale_anchor=args.allow_stale_realtime,
            )
        except Exception:
            realtime_state = compute_realtime_state(
                context,
                args.realtime_cache_seconds,
                args.capital,
                allow_stale_anchor=args.allow_stale_realtime,
            )
        realtime_members = realtime_state["members"]
        changes = realtime_state["changes"]
        meta = realtime_state["meta"]
        member_list_quote_source = meta.get("member_list_quote_source", meta.get("quote_source"))
        signal_member_quote_source = meta.get("signal_member_quote_source", meta.get("quote_source"))
        signal_hedge_quote_source = meta.get("signal_hedge_quote_source", meta.get("hedge_quote_source"))
        snapshot_time = realtime_state["meta"].get("snapshot_time")
        cache_age_seconds = float(realtime_state.get("cache_age_seconds", 0.0))
        _atomic_to_csv(realtime_members, paths["realtime_members"], index=False, encoding="utf-8")
        _atomic_to_csv(changes, paths["realtime_changes"], index=False, encoding="utf-8")
        print("实时进出名单")
        print(f"基准调仓信号日: {latest_rebalance.date()}")
        print(
            "静态名单生效日: {}".format(
                "暂无下一交易日" if rebalance_effective_date is None else pd.Timestamp(rebalance_effective_date).date()
            )
        )
        if snapshot_time:
            print(f"实时快照时间: {snapshot_time}")
        print(f"实时名单价格来源: {member_list_quote_source}")
        print(f"实时信号成员股报价来源: {signal_member_quote_source}")
        print(f"实时信号对冲腿报价来源: {signal_hedge_quote_source}")
        print(f"结果来源: {'cache' if realtime_state['from_cache'] else 'fresh'}")
        print(f"实时结果年龄: {cache_age_seconds:.1f} 秒")
        print(format_table(changes))
        print(f"已保存: {paths['realtime_changes'].name}")
        return

    if PERFORMANCE_PATTERN.search(query):
        perf_df, ret_col, nav_col, source_label = load_performance_source(
            args.costed_nav_csv,
            context["result"],
            args.index_csv,
        )
        build_performance_outputs(
            perf_df=perf_df,
            ret_col=ret_col,
            nav_col=nav_col,
            source_label=source_label,
            query_text=query,
            paths=paths,
        )
        summary = pd.read_csv(paths["performance_summary"])
        yearly = pd.read_csv(paths["performance_yearly"])
        print("表现汇总")
        print(format_table(summary))
        print("年度分解")
        print(format_table(yearly, max_rows=30))
        print(f"已保存: {paths['performance_chart'].name}")
        print(f"已保存: {paths['performance_summary'].name}")
        print(f"已保存: {paths['performance_yearly'].name}")
        print(f"已保存: {paths['performance_nav'].name}")
        print(f"已保存: {paths['performance_json'].name}")
        return

    raise ValueError(
        "不支持的查询命令。支持: 信号 / 实时信号 / 成分股 / 进出名单 / 实时进出名单 / 表现 <区间>"
    )


def execute_query(args: argparse.Namespace, query: str) -> None:
    query_text = normalize_query_text(query)
    paths = build_output_paths(args.output_prefix)
    panel_path, target_end_date = refresh_history_anchor(args, paths)
    kind = classify_query_kind(query_text)
    if kind == "performance":
        handle_performance_query_fast(args, paths, panel_path, target_end_date, query_text)
        return
    if kind == "signal":
        base_context = ensure_base_signal_fresh(args, paths, panel_path, target_end_date)
        handle_query(base_context, args, query_text)
        return
    if kind in {"realtime_signal", "realtime_changes"}:
        try:
            base_context = ensure_realtime_query_base_context(args, paths, panel_path, target_end_date)
        except (FileNotFoundError, ValueError):
            base_context = ensure_base_signal_fresh(args, paths, panel_path, target_end_date)
        member_context = ensure_static_members_fresh(args, paths, panel_path, target_end_date, base_context)
        handle_query(member_context, args, query_text)
        return
    if kind in {"members", "changes"}:
        base_context = ensure_base_signal_fresh(args, paths, panel_path, target_end_date)
        member_context = ensure_static_members_fresh(args, paths, panel_path, target_end_date, base_context)
        handle_query(member_context, args, query_text)
        return

    include_members = (not query_text) or query_text in {"成分股", "进出名单", "实时进出名单", "实时信号"}
    context = build_base_context(args, include_members=include_members)
    if query_text:
        handle_query(context, args, query_text)
        return
    save_base_outputs(context)
    print_console_summary(context["summary"])
    print(f"已保存: {paths['summary'].name}")
    print(f"已保存: {paths['signal'].name}")
    print(f"已保存: {paths['members'].name}")
    print(f"已保存: {paths['changes'].name}")
    print(f"已保存: {paths['nav'].name}")


def main() -> None:
    args = parse_args()
    _ensure_core_deps_or_exit(args)
    _load_runtime_modules()
    query = " ".join(args.query_tokens).strip()
    if query:
        execute_query(args, query)
        return

    context = build_base_context(args, include_members=True)
    save_base_outputs(context)
    print_console_summary(context["summary"])
    paths = context["paths"]
    print(f"已保存: {paths['summary'].name}")
    print(f"已保存: {paths['signal'].name}")
    print(f"已保存: {paths['members'].name}")
    print(f"已保存: {paths['changes'].name}")
    print(f"已保存: {paths['nav'].name}")


if __name__ == "__main__":
    main()
