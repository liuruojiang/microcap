from __future__ import annotations

import argparse
import importlib
import json
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

TOP_N = 100
LOOKBACK = 16
REBALANCE_WEEKDAY = "Thursday"
DEFAULT_PANEL_PATH = ROOT / "mnt_strategy_data_cn.csv"
HEDGE_COLUMN = "1.000852"
FIXED_HEDGE_RATIO = 1.0
FUTURES_DRAG = 3.0 / 10000.0
REQUIRE_POSITIVE_MICROCAP_MOM = False
TAIL_JITTER_WARNING_GAP = 0.001
TAIL_JITTER_CAUTION_GAP = 0.002
DEFAULT_MAX_STALE_ANCHOR_DAYS = 5
HEDGE_HISTORY_LOOKBACK_BUFFER_DAYS = 40
EXECUTION_TIMING = "close"
TRADE_CONSTRAINT_MODE = "close"
RESEARCH_STACK_VERSION = "2026-04-11-p0-p1-history-meta-master-stv2"
STATIC_CONTEXT_CACHE_VERSION = "2026-04-11-live-current-st-members-v1"

DEFAULT_INDEX_CSV = OUTPUT_DIR / "wind_microcap_top_100_biweekly_thursday_16y_cached.csv"
DEFAULT_OUTPUT_PREFIX = "microcap_top100_mom16_biweekly_live"
DEFAULT_COSTED_NAV_CSV = OUTPUT_DIR / "microcap_top100_mom16_hedge_zz1000_biweekly_thursday_16y_costed_nav.csv"
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
        return True
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
    end_ts = pd.Timestamp.now().normalize() if end_date is None else pd.Timestamp(end_date)
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
    if out.empty:
        raise RuntimeError(f"Parsed empty index history fallback for {secid}: {last_error}")
    return out.reset_index(drop=True)


def build_refreshed_panel_shadow(args: argparse.Namespace, paths: dict[str, Path]) -> tuple[Path, pd.Timestamp]:
    existing_shadow_end = read_csv_last_date(paths["panel_shadow"])
    if existing_shadow_end is not None and existing_shadow_end.normalize() >= pd.Timestamp.now().normalize():
        return paths["panel_shadow"], pd.Timestamp(existing_shadow_end)

    panel = pd.read_csv(args.panel_path)
    if "date" not in panel.columns or HEDGE_COLUMN not in panel.columns:
        raise ValueError(f"Panel must contain columns 'date' and '{HEDGE_COLUMN}'")
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
    paths["panel_shadow"].parent.mkdir(parents=True, exist_ok=True)
    shadow.to_csv(paths["panel_shadow"], index=False, encoding="utf-8")
    return paths["panel_shadow"], latest_hedge_date


def refresh_price_cache_tail(end_date: pd.Timestamp, max_workers: int, symbols: list[str] | None = None) -> None:
    if symbols is None:
        symbols = freq_mod.load_current_universe()
    if not symbols:
        raise RuntimeError("No cached-universe symbols available for price-cache refresh.")

    end_text = pd.Timestamp(end_date).strftime("%Y-%m-%d")
    failures: list[str] = []
    workers = max(1, min(int(max_workers), 16))
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(fetch_mod.fetch_price_history, symbol, freq_mod.START_DATE, end_text, False): symbol
            for symbol in symbols
        }
        for fut in as_completed(futures):
            symbol = futures[fut]
            try:
                fut.result()
            except Exception:
                failures.append(symbol)
    if len(failures) > max(50, len(symbols) // 20):
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

    ranked = sorted(caps, key=lambda item: item[1])[:top_k]
    symbols.update(symbol for symbol, _ in ranked)
    return sorted(symbols)


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
    refresh_price_cache_tail(target_end_date, args.max_workers, candidate_symbols)

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
    combined_index.to_csv(args.index_csv, index=False, encoding="utf-8")
    combined_members.to_csv(paths["proxy_members"], index=False, encoding="utf-8")
    combined_turnover.to_csv(paths["proxy_turnover"], index=False, encoding="utf-8")

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
    paths["proxy_meta"].write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")


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
    unique_weeks = sorted(pd.Index(week_periods.unique()))
    week_keys = pd.Series([i // 2 for i, _ in enumerate(unique_weeks)], index=unique_weeks)
    aligned_keys = pd.Index(week_periods).map(lambda p: week_keys[p])
    grouped = trading_dates.to_series().groupby(aligned_keys)
    return pd.DatetimeIndex(grouped.min().tolist())


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
            "lookback": LOOKBACK,
            "hedge_column": HEDGE_COLUMN,
            "execution_timing": EXECUTION_TIMING,
            "trade_constraint_mode": TRADE_CONSTRAINT_MODE,
            "research_stack_version": RESEARCH_STACK_VERSION,
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
    return (
        core_params.get("execution_timing") == EXECUTION_TIMING
        and core_params.get("trade_constraint_mode") == TRADE_CONSTRAINT_MODE
        and core_params.get("research_stack_version") == RESEARCH_STACK_VERSION
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
    files_fresh = (
        can_reuse_proxy
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
        and try_extend_costed_nav_without_turnover(args, panel_path, target_end_date)
    ):
        return

    if can_reuse_index and pd.Timestamp(current_index_end).normalize() < pd.Timestamp(target_end_date).normalize():
        extend_index_recent_window(args, paths, panel_path, target_end_date)
        if try_extend_costed_nav_without_turnover(args, panel_path, target_end_date):
            return
        if paths["proxy_turnover"].exists():
            rebuild_costed_nav_from_proxy_turnover(args, paths, panel_path)
            return

    if can_reuse_proxy:
        normalize_existing_proxy_outputs(args, paths)
        rebuild_costed_nav_from_proxy_turnover(args, paths, panel_path)
        return

    refresh_price_cache_tail(target_end_date, args.max_workers)

    panel = pd.read_csv(panel_path, usecols=["date"])
    panel["date"] = pd.to_datetime(panel["date"])
    trading_dates = pd.DatetimeIndex(
        panel.loc[panel["date"] <= pd.Timestamp(target_end_date), "date"].drop_duplicates().sort_values()
    )

    index_df, members_df, turnover_df, meta = build_local_proxy_bundle(args, trading_dates)
    args.index_csv.parent.mkdir(parents=True, exist_ok=True)
    index_df.to_csv(args.index_csv, index=False, encoding="utf-8")
    members_df.to_csv(paths["proxy_members"], index=False, encoding="utf-8")
    turnover_df.to_csv(paths["proxy_turnover"], index=False, encoding="utf-8")
    paths["proxy_meta"].write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
    rebuild_costed_nav_from_proxy_turnover(args, paths, panel_path)


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
        trimmed_index.to_csv(args.index_csv, index=False, encoding="utf-8")

    if members_df is not None and trimmed_members is not None and len(trimmed_members) != len(members_df):
        trimmed_members.to_csv(paths["proxy_members"], index=False, encoding="utf-8")

    if args.costed_nav_csv.exists():
        perf = pd.read_csv(args.costed_nav_csv)
        perf["date"] = pd.to_datetime(perf["date"], errors="coerce")
        perf = perf.dropna(subset=["date"]).sort_values("date")
        trimmed_perf = perf.loc[perf["date"] >= effective_start].copy()
        if len(trimmed_perf) != len(perf):
            trimmed_perf.to_csv(args.costed_nav_csv, index=False, encoding="utf-8")

    if paths["proxy_turnover"].exists():
        turnover = pd.read_csv(paths["proxy_turnover"])
        if "rebalance_date" in turnover.columns:
            turnover["rebalance_date"] = pd.to_datetime(turnover["rebalance_date"], errors="coerce")
            turnover = turnover.dropna(subset=["rebalance_date"]).sort_values("rebalance_date")
            trimmed_turnover = turnover.loc[turnover["rebalance_date"] >= effective_start].copy()
            if len(trimmed_turnover) != len(turnover):
                trimmed_turnover.to_csv(paths["proxy_turnover"], index=False, encoding="utf-8")

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
    paths["proxy_meta"].write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")


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
    return result


def rebuild_costed_nav_from_proxy_turnover(
    args: argparse.Namespace,
    paths: dict[str, Path],
    panel_path: Path,
) -> None:
    turnover_path = paths["proxy_turnover"]
    if not turnover_path.exists():
        raise FileNotFoundError(f"Missing proxy turnover history required for costed NAV rebuild: {turnover_path}")

    turnover_df = pd.read_csv(turnover_path)
    if "rebalance_date" not in turnover_df.columns:
        raise KeyError(f"Column 'rebalance_date' not found in {turnover_path}.")
    turnover_df["rebalance_date"] = pd.to_datetime(turnover_df["rebalance_date"], errors="coerce")
    turnover_df = turnover_df.dropna(subset=["rebalance_date"]).sort_values("rebalance_date")

    close_df = load_close_df(panel_path, args.index_csv)
    gross = run_signal(close_df)
    net = freq_mod.cost_mod.apply_cost_model(gross, turnover_df)
    net.to_csv(args.costed_nav_csv, index_label="date", encoding="utf-8")


def try_extend_costed_nav_without_turnover(
    args: argparse.Namespace,
    panel_path: Path,
    target_end_date: pd.Timestamp,
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
    close_df = load_close_df(panel_path, args.index_csv)
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

    rebalance_dates = build_biweekly_rebalance_dates(pd.DatetimeIndex(gross.index))
    missing_rebalances = rebalance_dates[(rebalance_dates > current_costed_end) & (rebalance_dates <= target_end)]
    if len(missing_rebalances):
        return False

    if EXECUTION_TIMING == freq_mod.EXECUTION_TIMING_CLOSE:
        active = missing["next_holding"].ne("cash")
        prev_active = missing["holding"].ne("cash")
    else:
        active = missing["holding"].ne("cash")
        prev_active = active.shift(1, fill_value=False)

    entry_cost = pd.Series(0.0, index=missing.index, dtype=float)
    entry_cost.loc[active & ~prev_active] = freq_mod.cost_mod.ENTRY_COST
    exit_cost = pd.Series(0.0, index=missing.index, dtype=float)
    exit_cost.loc[~active & prev_active] = freq_mod.cost_mod.EXIT_COST

    missing["entry_exit_cost"] = entry_cost + exit_cost
    missing["rebalance_cost"] = 0.0
    missing["total_cost"] = missing["entry_exit_cost"]
    missing["return_net"] = (1.0 + missing["return"]) * (1.0 - missing["total_cost"]) - 1.0
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
    combined.to_csv(args.costed_nav_csv, index=False, encoding="utf-8")
    return True


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


def build_change_table(prev_df: pd.DataFrame | None, curr_df: pd.DataFrame) -> pd.DataFrame:
    prev_df = prev_df.copy() if prev_df is not None else pd.DataFrame(columns=["symbol", "rank", "name"])
    curr_df = curr_df.copy()

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


def get_next_trade_date(trading_dates: pd.DatetimeIndex, current_date: pd.Timestamp) -> pd.Timestamp | None:
    future_dates = trading_dates[trading_dates > pd.Timestamp(current_date)]
    if len(future_dates) == 0:
        return None
    return pd.Timestamp(future_dates[0])


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
        "latest_rebalance": str(pd.Timestamp(latest_rebalance).date()),
        "prev_rebalance": None if prev_rebalance is None else str(pd.Timestamp(prev_rebalance).date()),
        "effective_rebalance": None if effective_rebalance is None else str(pd.Timestamp(effective_rebalance).date()),
        "rebalance_effective_date": None if rebalance_effective_date is None else str(pd.Timestamp(rebalance_effective_date).date()),
    }
    paths["cache_static_meta"].write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
    target_members.to_csv(paths["cache_static_target"], index=False, encoding="utf-8")
    effective_members.to_csv(paths["cache_static_effective"], index=False, encoding="utf-8")
    changes_df.to_csv(paths["cache_static_changes"], index=False, encoding="utf-8")


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


def enrich_signal_frame(signal_df: pd.DataFrame, result: pd.DataFrame) -> pd.DataFrame:
    out = signal_df.copy()
    last_row = result.iloc[-1]
    current_holding = str(last_row["holding"])
    next_holding = str(last_row["next_holding"])
    out["current_holding"] = current_holding
    out["trade_state"] = compute_trade_state(current_holding, next_holding)
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
            "lookback": LOOKBACK,
            "signal_model": "relative_momentum",
            "hedge_column": HEDGE_COLUMN,
            "fixed_hedge_ratio": FIXED_HEDGE_RATIO,
            "futures_drag_per_day": FUTURES_DRAG,
            "execution_timing": EXECUTION_TIMING,
            "trade_constraint_mode": TRADE_CONSTRAINT_MODE,
            "research_stack_version": RESEARCH_STACK_VERSION,
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

    close_df = load_close_df(resolved_panel_path, args.index_csv)
    result = run_signal(close_df)
    latest_signal = enrich_signal_frame(hedge_mod.build_latest_signal(result), result)

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

    result.to_csv(paths["nav"], index_label="date", encoding="utf-8")
    latest_signal.to_csv(paths["signal"], index=False, encoding="utf-8")
    if include_members:
        target_members.to_csv(paths["members"], index=False, encoding="utf-8")
        changes_df.to_csv(paths["changes"], index=False, encoding="utf-8")
        paths["summary"].write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")


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
    now = (now or pd.Timestamp.now()).normalize()
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

    m = re.search(r"(?:最近|过去|近)\s*([一二两三四五六七八九十\d半]+)\s*个?\s*月", text)
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


def build_performance_outputs(
    perf_df: pd.DataFrame,
    ret_col: str,
    nav_col: str,
    source_label: str,
    query_text: str,
    paths: dict[str, Path],
) -> dict[str, object]:
    start_date, end_date, period_label = parse_date_range(query_text)
    data = perf_df.copy()
    if start_date is None:
        start_date = pd.Timestamp(data.index.min())
    if end_date is None:
        end_date = pd.Timestamp(data.index.max())

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

    data.reset_index().to_csv(paths["performance_nav"], index=False, encoding="utf-8")
    summary_df.to_csv(paths["performance_summary"], index=False, encoding="utf-8")
    yearly_df.to_csv(paths["performance_yearly"], index=False, encoding="utf-8")

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
    paths["performance_json"].write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
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


def ensure_base_signal_fresh(
    args: argparse.Namespace,
    paths: dict[str, Path],
    panel_path: Path,
    target_end_date: pd.Timestamp,
) -> dict[str, object]:
    ensure_strategy_nav_fresh(args, paths, panel_path, target_end_date)
    close_df = load_close_df(panel_path, args.index_csv)
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
    if not args.index_csv.exists():
        raise FileNotFoundError(f"Missing proxy index required for realtime query: {args.index_csv}")
    close_df = load_close_df(panel_path, args.index_csv)
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


def load_or_refresh_stock_spot(cache_seconds: int) -> pd.DataFrame:
    cache_file = get_realtime_cache_file("stock_spot_latest.csv")
    now = time.time()
    if cache_file.exists() and now - cache_file.stat().st_mtime <= cache_seconds:
        return pd.read_csv(cache_file, dtype={"代码": str})

    last_error: Exception | None = None
    for fetcher in (ak.stock_zh_a_spot_em, ak.stock_zh_a_spot):
        try:
            spot = fetcher()
            spot.to_csv(cache_file, index=False, encoding="utf-8")
            return spot
        except Exception as exc:
            last_error = exc

    if cache_file.exists():
        return pd.read_csv(cache_file, dtype={"代码": str})
    raise RuntimeError(f"实时股票行情抓取失败: {last_error}") from last_error


def load_or_refresh_index_spot(cache_seconds: int) -> pd.DataFrame:
    cache_file = get_realtime_cache_file("index_spot_latest.csv")
    now = time.time()
    if cache_file.exists() and now - cache_file.stat().st_mtime <= cache_seconds:
        return pd.read_csv(cache_file, dtype={"代码": str})

    try:
        spot = ak.stock_zh_index_spot_em()
        spot.to_csv(cache_file, index=False, encoding="utf-8")
        return spot
    except Exception as exc:
        if cache_file.exists():
            return pd.read_csv(cache_file, dtype={"代码": str})
        raise RuntimeError(f"实时指数行情抓取失败: {exc}") from exc


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
    latest_shares.to_csv(cache_file, index=False, encoding="utf-8")
    return latest_shares


def fetch_eastmoney_stock_spot(symbol: str) -> dict[str, object] | None:
    code = str(symbol).zfill(6)
    market = "1" if code.startswith(("5", "6", "9")) else "0"
    url = (
        "https://push2.eastmoney.com/api/qt/stock/get"
        f"?secid={market}.{code}"
        "&fields=f43,f44,f45,f46,f57,f58,f60"
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
        return {
            "code": str(data.get("f57") or code).zfill(6),
            "name": str(data.get("f58") or ""),
            "rt_price": rt_price,
        }
    except Exception:
        return None


def fetch_member_realtime_quotes(symbols: list[str], max_workers: int = 24) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    clean_symbols = [str(symbol).zfill(6) for symbol in symbols if str(symbol).strip()]
    if not clean_symbols:
        return pd.DataFrame(columns=["code", "name", "rt_price"])
    with ThreadPoolExecutor(max_workers=max(1, min(int(max_workers), 32))) as pool:
        futures = {pool.submit(fetch_eastmoney_stock_spot, symbol): symbol for symbol in clean_symbols}
        for fut in as_completed(futures):
            row = fut.result()
            if row is not None:
                rows.append(row)
    if not rows:
        return pd.DataFrame(columns=["code", "name", "rt_price"])
    return pd.DataFrame(rows).drop_duplicates(subset="code")


def fetch_hedge_realtime_quote_fast() -> tuple[float, str]:
    url = (
        "https://push2.eastmoney.com/api/qt/stock/get"
        "?secid=1.000852"
        "&fields=f43,f60"
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
            return float(latest) / 100.0, "eastmoney_stock_get"
        if pd.notna(prev) and prev > 0:
            return float(prev) / 100.0, "eastmoney_prev_close_fallback"
    except Exception:
        pass
    index_spot = load_or_refresh_index_spot(cache_seconds=86400)
    index_spot["浠ｇ爜"] = index_spot["浠ｇ爜"].astype(str).str.zfill(6)
    hedge_row = index_spot.loc[index_spot["浠ｇ爜"] == "000852"]
    if hedge_row.empty:
        raise RuntimeError("无法获取中证1000实时价格")
    hedge_row = hedge_row.iloc[0]
    hedge_rt_close = pd.to_numeric(hedge_row.get("鏈€鏂颁环"), errors="coerce")
    hedge_prev = pd.to_numeric(hedge_row.get("鏄ㄦ敹"), errors="coerce")
    if pd.notna(hedge_rt_close) and hedge_rt_close > 0:
        return float(hedge_rt_close), "index_spot_latest_cached_fallback"
    if pd.notna(hedge_prev) and hedge_prev > 0:
        return float(hedge_prev), "index_prev_close_cached_fallback"
    raise RuntimeError("无法获取中证1000实时价格")


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


def load_latest_close_map(symbols: list[str], as_of_date: pd.Timestamp) -> dict[str, float]:
    out: dict[str, float] = {}
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
            out[symbol] = float(price.iloc[-1]["close_raw"])
        except Exception:
            continue
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
    last_close_map = load_latest_close_map(member_symbols, as_of_date=latest_trade_date)

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

    last_microcap_close = float(close_df["microcap"].iloc[-1])
    microcap_rt_close = last_microcap_close * (1.0 + float(np.mean(member_returns)))

    index_spot = load_or_refresh_index_spot(cache_seconds)
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

    snapshot_ts = pd.Timestamp.now()
    if snapshot_ts <= latest_trade_date:
        snapshot_ts = latest_trade_date + pd.Timedelta(seconds=1)
    rt_close_df = close_df.copy()
    rt_close_df.loc[snapshot_ts, ["microcap", "hedge"]] = [microcap_rt_close, float(hedge_rt_close)]
    rt_close_df = rt_close_df.sort_index()
    rt_result = run_signal(rt_close_df)
    latest_rt_signal = enrich_signal_frame(hedge_mod.build_latest_signal(rt_result), rt_result)
    latest_rt_signal["date"] = snapshot_ts
    latest_rt_signal["quote_source"] = quote_source
    latest_rt_signal["hedge_quote_source"] = hedge_source
    latest_rt_signal["member_price_count"] = available_rows
    latest_rt_signal["member_count"] = len(member_symbols)
    latest_rt_signal["latest_anchor_trade_date"] = latest_trade_date

    meta = {
        "snapshot_time": str(snapshot_ts),
        "latest_anchor_trade_date": str(latest_trade_date.date()),
        "quote_source": quote_source,
        "hedge_quote_source": hedge_source,
        "member_price_count": available_rows,
        "member_count": len(member_symbols),
        "microcap_rt_close": float(microcap_rt_close),
        "hedge_rt_close": float(hedge_rt_close),
    }
    return latest_rt_signal, meta


def build_realtime_signal_fast(context: dict[str, object]) -> tuple[pd.DataFrame, dict[str, object]]:
    close_df = context["close_df"].copy()
    effective_members = context["effective_members"].copy()
    latest_trade_date = pd.Timestamp(close_df.index[-1])
    member_symbols = effective_members["symbol"].astype(str).str.zfill(6).tolist()
    last_close_map = load_latest_close_map(member_symbols, as_of_date=latest_trade_date)

    quotes_df = fetch_member_realtime_quotes(member_symbols)
    quote_source = "eastmoney_stock_get_member_only"
    quotes_df = quotes_df.set_index("code") if not quotes_df.empty else pd.DataFrame(index=pd.Index([], dtype=str))

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
        raise ValueError("无法计算实时信号: 当前成分股没有可用实时价格。")

    last_microcap_close = float(close_df["microcap"].iloc[-1])
    microcap_rt_close = last_microcap_close * (1.0 + float(np.mean(member_returns)))

    try:
        hedge_rt_close, hedge_source = fetch_hedge_realtime_quote_fast()
    except Exception:
        hedge_rt_close = float(close_df["hedge"].iloc[-1])
        hedge_source = "latest_cached_close_fallback"

    snapshot_ts = pd.Timestamp.now()
    if snapshot_ts <= latest_trade_date:
        snapshot_ts = latest_trade_date + pd.Timedelta(seconds=1)
    rt_close_df = close_df.copy()
    rt_close_df.loc[snapshot_ts, ["microcap", "hedge"]] = [microcap_rt_close, float(hedge_rt_close)]
    rt_close_df = rt_close_df.sort_index()
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
    signal_df["tail_jitter_risk"] = jitter_level
    signal_df["tail_jitter_note"] = jitter_note

    meta = {
        "snapshot_time": str(snapshot_ts),
        "latest_anchor_trade_date": str(latest_trade_date.date()),
        "quote_source": quote_source,
        "hedge_quote_source": hedge_source,
        "member_price_count": available_rows,
        "member_count": len(member_symbols),
        "microcap_rt_close": float(microcap_rt_close),
        "hedge_rt_close": float(hedge_rt_close),
        "tail_jitter_risk": jitter_level,
        "tail_jitter_note": jitter_note,
    }
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
        signal_df = pd.read_csv(signal_path)
        return signal_df, meta, float(cache_age_seconds)
    except Exception:
        return None


def save_cached_fast_realtime_signal(paths: dict[str, Path], signal_df: pd.DataFrame, meta: dict[str, object]) -> None:
    REALTIME_DIR.mkdir(parents=True, exist_ok=True)
    paths["cache_fast_realtime_meta"].write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
    signal_df.to_csv(paths["cache_fast_realtime_signal"], index=False, encoding="utf-8")


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
                    "signal_date": pd.Timestamp.now().normalize().date(),
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
    signal_df["quote_source"] = quote_source
    meta = {
        "snapshot_time": signal_meta["snapshot_time"],
        "latest_anchor_trade_date": signal_meta["latest_anchor_trade_date"],
        "latest_rebalance": str(latest_rebalance.date()),
        "effective_rebalance": None if effective_rebalance is None else str(pd.Timestamp(effective_rebalance).date()),
        "rebalance_effective_date": None if rebalance_effective_date is None else str(pd.Timestamp(rebalance_effective_date).date()),
        "quote_source": quote_source,
        "hedge_quote_source": signal_meta["hedge_quote_source"],
        "member_price_count": signal_meta["member_price_count"],
        "member_count": signal_meta["member_count"],
        "microcap_rt_close": signal_meta["microcap_rt_close"],
        "hedge_rt_close": signal_meta["hedge_rt_close"],
        "tail_jitter_risk": signal_meta.get("tail_jitter_risk"),
        "tail_jitter_note": signal_meta.get("tail_jitter_note"),
    }
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
        signal_df = pd.read_csv(signal_path)
        members_df = pd.read_csv(members_path, dtype={"symbol": str})
        changes_df = pd.read_csv(changes_path, dtype={"symbol": str})
        signal_df = augment_signal_with_member_rebalance(signal_df, changes_df)
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
    REALTIME_DIR.mkdir(parents=True, exist_ok=True)
    paths["cache_realtime_meta"].write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
    signal_df.to_csv(paths["cache_realtime_signal"], index=False, encoding="utf-8")
    members_df.to_csv(paths["cache_realtime_members"], index=False, encoding="utf-8")
    changes_df.to_csv(paths["cache_realtime_changes"], index=False, encoding="utf-8")


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
    last_close_map = load_latest_close_map(member_symbols, as_of_date=latest_trade_date)
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

    last_microcap_close = float(close_df["microcap"].iloc[-1])
    microcap_rt_close = last_microcap_close * (1.0 + float(np.mean(member_returns)))

    index_spot = load_or_refresh_index_spot(cache_seconds)
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

    snapshot_ts = pd.Timestamp.now()
    if snapshot_ts <= latest_trade_date:
        snapshot_ts = latest_trade_date + pd.Timedelta(seconds=1)
    rt_close_df = close_df.copy()
    rt_close_df.loc[snapshot_ts, ["microcap", "hedge"]] = [microcap_rt_close, float(hedge_rt_close)]
    rt_close_df = rt_close_df.sort_index()
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
    signal_df["tail_jitter_risk"] = jitter_level
    signal_df["tail_jitter_note"] = jitter_note

    meta = {
        "snapshot_time": str(snapshot_ts),
        "latest_anchor_trade_date": str(latest_trade_date.date()),
        "latest_rebalance": str(latest_rebalance.date()),
        "effective_rebalance": None if effective_rebalance is None else str(pd.Timestamp(effective_rebalance).date()),
        "rebalance_effective_date": None if rebalance_effective_date is None else str(pd.Timestamp(rebalance_effective_date).date()),
        "quote_source": quote_source,
        "hedge_quote_source": hedge_source,
        "member_price_count": available_rows,
        "member_count": len(member_symbols),
        "microcap_rt_close": float(microcap_rt_close),
        "hedge_rt_close": float(hedge_rt_close),
        "tail_jitter_risk": jitter_level,
        "tail_jitter_note": jitter_note,
    }
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
                save_cached_fast_realtime_signal(paths, rt_signal, meta)
                cache_age_seconds = 0.0
                result_source = "fresh_fast"
            else:
                rt_signal, meta, cache_age_seconds = cached_fast
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
        rt_signal.to_csv(paths["realtime_signal"], index=False, encoding="utf-8")
        gap_value = float(rt_signal.iloc[0]["momentum_gap"])
        jitter_risk = str(rt_signal.iloc[0].get("tail_jitter_risk", "normal"))
        jitter_note = str(rt_signal.iloc[0].get("tail_jitter_note", "") or "")
        print("\u5b9e\u65f6\u4fe1\u53f7")
        print(format_table(rt_signal))
        print(f"\u5b9e\u65f6\u5feb\u7167\u65f6\u95f4: {meta['snapshot_time']}")
        print(f"\u5386\u53f2\u951a\u70b9\u4ea4\u6613\u65e5: {meta['latest_anchor_trade_date']}")
        print(f"\u5fae\u76d8\u5b9e\u65f6\u4ef7\u683c\u6765\u6e90: {meta['quote_source']}")
        print(f"\u5bf9\u51b2\u817f\u5b9e\u65f6\u4ef7\u683c\u6765\u6e90: {meta['hedge_quote_source']}")
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
        latest_signal.to_csv(paths["signal"], index=False, encoding="utf-8")
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
        members.to_csv(paths["members"], index=False, encoding="utf-8")
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
        changes.to_csv(paths["changes"], index=False, encoding="utf-8")
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
        quote_source = realtime_state["meta"]["quote_source"]
        snapshot_time = realtime_state["meta"].get("snapshot_time")
        cache_age_seconds = float(realtime_state.get("cache_age_seconds", 0.0))
        realtime_members.to_csv(paths["realtime_members"], index=False, encoding="utf-8")
        changes.to_csv(paths["realtime_changes"], index=False, encoding="utf-8")
        print("实时进出名单")
        print(f"基准调仓信号日: {latest_rebalance.date()}")
        print(
            "静态名单生效日: {}".format(
                "暂无下一交易日" if rebalance_effective_date is None else pd.Timestamp(rebalance_effective_date).date()
            )
        )
        if snapshot_time:
            print(f"实时快照时间: {snapshot_time}")
        print(f"实时价格来源: {quote_source}")
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
