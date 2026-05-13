from __future__ import annotations

import argparse
from contextlib import contextmanager
import hashlib
import importlib
import importlib.util
import json
import math
import os
import platform
import re
import shutil
import subprocess
import sys
import time
import warnings
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from decimal import ROUND_HALF_UP, Decimal
from io import BytesIO
from pathlib import Path
from types import SimpleNamespace
from typing import Iterable

ROOT = Path(__file__).resolve().parent
OUTPUT_DIR = ROOT / "outputs"
CACHE_DIR = ROOT / ".microcap_index_cache"
REALTIME_DIR = CACHE_DIR / "realtime"

ak = None
matplotlib = None
np = None
pd = None
plt = None
requests = None
urllib3 = None
PerformanceWarning = Warning
_V2_RUNTIME_ARGS: argparse.Namespace | None = None
RUNTIME_PACKAGES = (
    "numpy",
    "pandas",
    "requests",
    "urllib3",
    "akshare",
    "matplotlib",
    "openpyxl",
)


def parse_v2_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Top100 Mom16 Biweekly v2.0 standalone target-vol overlay"
    )
    parser.add_argument("query_tokens", nargs="*", help="信号 / 实时信号 / 表现 <区间>")
    parser.add_argument("--panel-path", type=Path, default=None)
    parser.add_argument("--index-csv", type=Path, default=None)
    parser.add_argument("--capital", type=float, default=None)
    parser.add_argument("--max-workers", type=int, default=8)
    parser.add_argument("--realtime-cache-seconds", type=int, default=30)
    parser.add_argument("--allow-stale-realtime", action=argparse.BooleanOptionalAction, default=False)
    parser.add_argument("--bootstrap-deps", action="store_true")
    parser.add_argument("--wheelhouse", type=Path, default=None)
    return parser.parse_args(argv)


def _find_missing_modules(module_names: Iterable[str] = RUNTIME_PACKAGES) -> list[str]:
    missing: list[str] = []
    for module_name in module_names:
        if importlib.util.find_spec(module_name) is None:
            missing.append(module_name)
    return missing


def _build_runtime_tag() -> str:
    machine_name = platform.machine().lower() or os.environ.get("PROCESSOR_ARCHITECTURE", "").lower() or "unknown"
    machine_name = {
        "amd64": "x86_64",
        "x64": "x86_64",
        "aarch64": "arm64",
    }.get(machine_name, machine_name)
    impl_tag = "cp" if sys.implementation.name.lower() == "cpython" else sys.implementation.name[:2].lower()
    return f"{sys.platform.lower()}_{machine_name}_{impl_tag}{sys.version_info.major}{sys.version_info.minor}"


def _resolve_wheelhouse(cli_path: Path | None = None) -> Path | None:
    runtime_tag = _build_runtime_tag()
    candidates: list[Path] = []
    if cli_path is not None:
        candidates.extend([cli_path, cli_path / runtime_tag])
    env_value = os.environ.get("MICROCAP_WHEELHOUSE")
    if env_value:
        env_path = Path(env_value)
        candidates.extend([env_path, env_path / runtime_tag])
    for relative in ("wheelhouse", ".vendor_libs/wheelhouse", ".vendor_libs"):
        base = ROOT / relative
        candidates.extend([base, base / runtime_tag])
    for candidate in candidates:
        resolved = candidate.expanduser().resolve()
        if resolved.is_dir() and any(resolved.glob("*.whl")):
            return resolved
    return None


def _bootstrap_from_wheelhouse(wheelhouse: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            sys.executable,
            "-m",
            "pip",
            "install",
            "--no-index",
            "--disable-pip-version-check",
            "--find-links",
            str(wheelhouse),
            *RUNTIME_PACKAGES,
        ],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )


def _ensure_runtime_deps_or_exit(args: argparse.Namespace) -> None:
    missing = _find_missing_modules()
    if not missing:
        return
    if not args.bootstrap_deps:
        print(
            f"缺少运行依赖: {', '.join(missing)}. 请先安装依赖，或使用 --bootstrap-deps。",
            file=sys.stderr,
        )
        raise SystemExit(2)
    wheelhouse = _resolve_wheelhouse(args.wheelhouse)
    if wheelhouse is None:
        print("未找到可用 wheelhouse，无法离线安装运行依赖。", file=sys.stderr)
        raise SystemExit(2)
    result = _bootstrap_from_wheelhouse(wheelhouse)
    if result.returncode != 0:
        print(f"离线依赖安装失败: {wheelhouse}\n{result.stdout}\n{result.stderr}", file=sys.stderr)
        raise SystemExit(2)
    remaining = _find_missing_modules()
    if remaining:
        print(f"依赖安装后仍缺少: {', '.join(remaining)}", file=sys.stderr)
        raise SystemExit(2)


def _optional_imports() -> None:
    global ak, matplotlib, np, pd, plt, requests, urllib3, PerformanceWarning
    import akshare as _ak
    import matplotlib as _matplotlib
    import numpy as _np
    import pandas as _pd
    import requests as _requests
    import urllib3 as _urllib3
    from pandas.errors import PerformanceWarning as _PerformanceWarning

    _matplotlib.use("Agg")
    import matplotlib.pyplot as _plt

    ak = _ak
    matplotlib = _matplotlib
    np = _np
    pd = _pd
    plt = _plt
    requests = _requests
    urllib3 = _urllib3
    PerformanceWarning = _PerformanceWarning
    warnings.filterwarnings("ignore", category=PerformanceWarning)


def _exec_embedded_module(name: str, source: str, extra: dict[str, object] | None = None) -> tuple[SimpleNamespace, dict[str, object]]:
    ns: dict[str, object] = {
        "__name__": name,
        "__file__": str(ROOT / f"{name}.py"),
        "__package__": "",
    }
    ns.update(globals())
    if extra:
        ns.update(extra)
    exec(compile(source, f"<{name}>", "exec"), ns)
    public = {key: value for key, value in ns.items() if not key.startswith("__")}
    return SimpleNamespace(**public), ns


HEDGE_SOURCE = r'''
from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parent
OUTPUT_DIR = ROOT / "outputs"
DEFAULT_PANEL = ROOT / "mnt_strategy_data_cn.csv"
DEFAULT_MICROCAP_CSV = OUTPUT_DIR / "wind_microcap_868008_data.csv"

DEFAULT_OUTPUT_PREFIX = "microcap_zz1000_hedge"

DEFAULT_MICROCAP_COLUMN = "868008.WI"
DEFAULT_MICROCAP_LABEL = "Wind Microcap Index"
DEFAULT_HEDGE_COLUMN = "1.000852"
DEFAULT_LOOKBACK = 20
DEFAULT_FUTURES_DRAG = 3.0 / 10000.0
DEFAULT_R2_WINDOW = 5
DEFAULT_R2_THRESHOLD = 0.0
DEFAULT_SIGNAL_MODEL = "momentum"
DEFAULT_BIAS_N = 60
DEFAULT_BIAS_MOM_DAY = 20
DEFAULT_TARGET_VOL = 0.20
DEFAULT_VOL_WINDOW = 30
DEFAULT_MAX_LEV = 1.5
DEFAULT_MIN_LEV = 0.1
DEFAULT_SCALE_THRESHOLD = 0.10
CN_TRADING_DAYS = 244

# The workspace does not currently include the Wind microcap index.
# Keep the default candidates generic so the script can be used as soon as
# the user adds a microcap column into the main panel.
DEFAULT_MICROCAP_CANDIDATES = [
    "868008.WI",
    "868008_WI",
    "万得微盘股指数",
    "WIND_MICROCAP",
    "wind_microcap",
    "microcap",
]


@dataclass
class Metrics:
    annual: float
    vol: float
    sharpe: float
    max_dd: float
    calmar: float
    total_return: float
    win_rate: float


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Backtest a hedged China microcap strategy: long microcap, short CSI 1000 "
            "proxy when microcap momentum is stronger than CSI 1000 momentum and "
            "microcap momentum is positive."
        )
    )
    parser.add_argument("--panel-path", type=Path, default=DEFAULT_PANEL)
    parser.add_argument(
        "--microcap-column",
        default=DEFAULT_MICROCAP_COLUMN,
        help="Column name in the main panel for the Wind microcap index. Default: 868008.WI",
    )
    parser.add_argument(
        "--hedge-column",
        default=DEFAULT_HEDGE_COLUMN,
        help="Column name used as the CSI 1000 futures proxy.",
    )
    parser.add_argument(
        "--lookback",
        type=int,
        default=DEFAULT_LOOKBACK,
        help="Momentum lookback window in trading days.",
    )
    parser.add_argument(
        "--signal-model",
        choices=("momentum", "bias_momentum"),
        default=DEFAULT_SIGNAL_MODEL,
        help="Signal model. 'momentum' uses relative momentum; 'bias_momentum' uses ratio/MA slope.",
    )
    parser.add_argument(
        "--bias-n",
        type=int,
        default=DEFAULT_BIAS_N,
        help="Moving-average window used by bias_momentum.",
    )
    parser.add_argument(
        "--bias-mom-day",
        type=int,
        default=DEFAULT_BIAS_MOM_DAY,
        help="Slope-fit window used by bias_momentum.",
    )
    parser.add_argument(
        "--futures-drag",
        type=float,
        default=DEFAULT_FUTURES_DRAG,
        help="Daily basis drag charged on active hedge days. Default = 3/10000.",
    )
    parser.add_argument(
        "--r2-window",
        type=int,
        default=DEFAULT_R2_WINDOW,
        help="Rolling window for R-squared filter on microcap/hedge ratio.",
    )
    parser.add_argument(
        "--r2-threshold",
        type=float,
        default=DEFAULT_R2_THRESHOLD,
        help="Minimum R-squared required to enter a position. Default 0 disables the filter.",
    )
    parser.add_argument(
        "--vol-scale-enabled",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Enable target volatility scaling on strategy daily returns.",
    )
    parser.add_argument("--target-vol", type=float, default=DEFAULT_TARGET_VOL)
    parser.add_argument("--vol-window", type=int, default=DEFAULT_VOL_WINDOW)
    parser.add_argument("--max-lev", type=float, default=DEFAULT_MAX_LEV)
    parser.add_argument("--min-lev", type=float, default=DEFAULT_MIN_LEV)
    parser.add_argument("--scale-threshold", type=float, default=DEFAULT_SCALE_THRESHOLD)
    parser.add_argument(
        "--microcap-csv",
        type=Path,
        default=None,
        help=(
            "Optional external CSV for the microcap index. Must contain a date column "
            "and a close column."
        ),
    )
    parser.add_argument("--microcap-date-col", default="date")
    parser.add_argument("--microcap-close-col", default="close")
    parser.add_argument("--output-prefix", default=DEFAULT_OUTPUT_PREFIX)
    parser.add_argument(
        "--require-positive-microcap-mom",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Require microcap momentum > 0 to enter the hedge trade. Default: true.",
    )
    return parser.parse_args()


def load_main_panel(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Main panel not found: {path}")
    panel = pd.read_csv(path)
    if "date" not in panel.columns:
        raise ValueError(f"'date' column missing in {path}")
    panel["date"] = pd.to_datetime(panel["date"])
    panel = panel.sort_values("date").drop_duplicates(subset="date")
    return panel.set_index("date")


def resolve_microcap_column(panel: pd.DataFrame, explicit_name: str | None) -> str:
    if explicit_name:
        if explicit_name not in panel.columns:
            available = ", ".join(panel.columns)
            raise ValueError(
                f"Microcap column '{explicit_name}' not found in panel. "
                f"Expected Wind microcap code is '{DEFAULT_MICROCAP_COLUMN}'. "
                f"Available columns: {available}"
            )
        return explicit_name

    for candidate in DEFAULT_MICROCAP_CANDIDATES:
        if candidate in panel.columns:
            return candidate

    available = ", ".join(panel.columns)
    raise ValueError(
        f"{DEFAULT_MICROCAP_LABEL} column not found. Add '{DEFAULT_MICROCAP_COLUMN}' into "
        "mnt_strategy_data_cn.csv or pass "
        f"--microcap-column / --microcap-csv. Available columns: {available}"
    )


def load_external_microcap_series(
    csv_path: Path,
    date_col: str,
    close_col: str,
) -> pd.Series:
    if not csv_path.exists():
        raise FileNotFoundError(f"Microcap CSV not found: {csv_path}")
    frame = pd.read_csv(csv_path)
    missing = [col for col in (date_col, close_col) if col not in frame.columns]
    if missing:
        raise ValueError(f"Microcap CSV missing columns: {missing}")
    frame = frame[[date_col, close_col]].copy()
    frame[date_col] = pd.to_datetime(frame[date_col])
    frame = frame.dropna(subset=[close_col]).sort_values(date_col).drop_duplicates(subset=date_col)
    return frame.set_index(date_col)[close_col].rename("microcap")


def build_close_df(args: argparse.Namespace) -> pd.DataFrame:
    panel = load_main_panel(args.panel_path)

    if args.hedge_column not in panel.columns:
        available = ", ".join(panel.columns)
        raise ValueError(
            f"Hedge column '{args.hedge_column}' not found in panel. Available columns: {available}"
        )

    hedge = panel[args.hedge_column].rename("hedge").astype(float)

    external_microcap = args.microcap_csv
    if external_microcap is None and DEFAULT_MICROCAP_CSV.exists() and args.microcap_column not in panel.columns:
        external_microcap = DEFAULT_MICROCAP_CSV
    args.resolved_microcap_csv = str(external_microcap) if external_microcap else None

    if external_microcap:
        microcap = load_external_microcap_series(
            csv_path=external_microcap,
            date_col=args.microcap_date_col,
            close_col=args.microcap_close_col,
        )
    else:
        microcap_col = resolve_microcap_column(panel, args.microcap_column)
        microcap = panel[microcap_col].rename("microcap").astype(float)

    close_df = pd.concat([microcap, hedge], axis=1).sort_index()
    close_df = close_df.dropna(how="all").ffill()
    close_df = close_df.dropna(subset=["microcap", "hedge"])
    if len(close_df) < args.lookback + 3:
        raise ValueError(
            f"Not enough data after alignment. Need at least {args.lookback + 3} rows, got {len(close_df)}."
        )
    return close_df


def build_output_paths(output_prefix: str) -> dict[str, Path]:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    return {
        "nav": OUTPUT_DIR / f"{output_prefix}_nav.csv",
        "signal": OUTPUT_DIR / f"{output_prefix}_latest_signal.csv",
        "summary": OUTPUT_DIR / f"{output_prefix}_summary.json",
        "curve": OUTPUT_DIR / f"{output_prefix}_curve.png",
    }


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


def calc_metrics(ret: pd.Series) -> Metrics:
    ret = ret.dropna()
    if ret.empty:
        raise ValueError("Return series is empty.")

    nav = (1.0 + ret).cumprod()
    years = (ret.index[-1] - ret.index[0]).days / 365.25
    annual = nav.iloc[-1] ** (1.0 / years) - 1.0 if years > 0 else np.nan
    vol = ret.std(ddof=1) * np.sqrt(CN_TRADING_DAYS)
    sharpe = annual / vol if pd.notna(vol) and vol > 0 else np.nan
    max_dd = ((nav - nav.cummax()) / nav.cummax()).min()
    calmar = annual / abs(max_dd) if pd.notna(max_dd) and max_dd != 0 else np.nan
    total_return = nav.iloc[-1] - 1.0
    monthly = ret.groupby(ret.index.to_period("M")).apply(lambda x: (1.0 + x).prod() - 1.0)
    win_rate = float((monthly > 0).mean()) if len(monthly) else np.nan
    return Metrics(
        annual=float(annual),
        vol=float(vol),
        sharpe=float(sharpe),
        max_dd=float(max_dd),
        calmar=float(calmar),
        total_return=float(total_return),
        win_rate=float(win_rate),
    )


def apply_vol_scaling(
    result: pd.DataFrame,
    trading_days: int,
    vol_window: int,
    target_vol: float,
    min_lev: float,
    max_lev: float,
    scale_threshold: float,
) -> pd.DataFrame:
    out = result.copy()
    realized_vol = out["return_raw"].rolling(vol_window).std(ddof=1) * np.sqrt(trading_days)
    scale_raw = (target_vol / realized_vol).clip(lower=min_lev, upper=max_lev).shift(1)
    if scale_threshold > 0:
        scale_arr = scale_raw.to_numpy(copy=True)
        last_scale = np.nan
        for i in range(len(scale_arr)):
            if np.isnan(scale_arr[i]):
                continue
            if np.isnan(last_scale):
                last_scale = scale_arr[i]
            elif abs(scale_arr[i] - last_scale) >= scale_threshold - 1e-9:
                last_scale = scale_arr[i]
            else:
                scale_arr[i] = last_scale
        scale_raw = pd.Series(scale_arr, index=out.index)
    weight = scale_raw.fillna(1.0)
    weight[out["holding"] == "cash"] = 1.0
    out["realized_vol"] = realized_vol
    out["scale_raw"] = scale_raw
    out["weight"] = weight
    out["return"] = out["return_raw"] * out["weight"]
    out["nav"] = (1.0 + out["return"]).cumprod()
    return out


def run_backtest(
    close_df: pd.DataFrame,
    signal_model: str,
    lookback: int,
    bias_n: int,
    bias_mom_day: int,
    futures_drag: float,
    require_positive_microcap_mom: bool,
    r2_window: int,
    r2_threshold: float,
    vol_scale_enabled: bool,
    target_vol: float,
    vol_window: int,
    max_lev: float,
    min_lev: float,
    scale_threshold: float,
    hedge_ratio: float = 1.0,
) -> pd.DataFrame:
    work = close_df.copy()
    work["microcap_ret"] = work["microcap"].pct_change(fill_method=None)
    work["hedge_ret"] = work["hedge"].pct_change(fill_method=None)
    work["microcap_mom"] = calc_momentum(work["microcap"], lookback)
    work["hedge_mom"] = calc_momentum(work["hedge"], lookback)
    work["momentum_gap"] = work["microcap_mom"] - work["hedge_mom"]
    work["ratio"] = work["microcap"] / work["hedge"]
    work["ratio_bias_mom"] = calc_bias_momentum(work["ratio"], bias_n, bias_mom_day)
    work["ratio_r2"] = calc_rolling_r2(work["ratio"], r2_window)

    if signal_model == "bias_momentum":
        valid_mask = work["ratio_bias_mom"].notna()
    else:
        valid_mask = work[["microcap_mom", "hedge_mom"]].notna().all(axis=1)
    valid_start = valid_mask[valid_mask].index.min()
    if pd.isna(valid_start):
        raise ValueError("No valid momentum history after alignment.")

    work = work.loc[valid_start:].copy()
    rows: list[dict[str, object]] = []
    holding = False

    for i in range(1, len(work)):
        date = work.index[i]
        active_ret = 0.0
        drag = futures_drag if holding else 0.0
        if holding:
            microcap_ret = work["microcap_ret"].iloc[i]
            hedge_ret = work["hedge_ret"].iloc[i]
            if pd.notna(microcap_ret) and pd.notna(hedge_ret):
                active_ret = float(microcap_ret - hedge_ratio * hedge_ret)
        if signal_model == "bias_momentum":
            signal_on = bool(
                pd.notna(work["ratio_bias_mom"].iloc[i])
                and work["ratio_bias_mom"].iloc[i] > 0.0
                and (
                    (pd.notna(work["ratio_r2"].iloc[i]) and work["ratio_r2"].iloc[i] >= r2_threshold)
                    if r2_threshold > 0
                    else True
                )
            )
        else:
            signal_on = bool(
                pd.notna(work["microcap_mom"].iloc[i])
                and pd.notna(work["hedge_mom"].iloc[i])
                and work["microcap_mom"].iloc[i] > work["hedge_mom"].iloc[i]
                and (
                    (work["microcap_mom"].iloc[i] > 0.0)
                    if require_positive_microcap_mom
                    else True
                )
                and (
                    (pd.notna(work["ratio_r2"].iloc[i]) and work["ratio_r2"].iloc[i] >= r2_threshold)
                    if r2_threshold > 0
                    else True
                )
            )
        day_ret = active_ret - drag
        next_holding = "long_microcap_short_zz1000" if signal_on else "cash"
        rows.append(
            {
                "date": date,
                "return_raw": day_ret,
                "holding": "long_microcap_short_zz1000" if holding else "cash",
                "next_holding": next_holding,
                "signal_on": signal_on,
                "microcap_close": float(work["microcap"].iloc[i]),
                "hedge_close": float(work["hedge"].iloc[i]),
                "microcap_ret": float(work["microcap_ret"].iloc[i]) if pd.notna(work["microcap_ret"].iloc[i]) else np.nan,
                "hedge_ret": float(work["hedge_ret"].iloc[i]) if pd.notna(work["hedge_ret"].iloc[i]) else np.nan,
                "microcap_mom": float(work["microcap_mom"].iloc[i]),
                "hedge_mom": float(work["hedge_mom"].iloc[i]),
                "momentum_gap": float(work["momentum_gap"].iloc[i]),
                "ratio_bias_mom": float(work["ratio_bias_mom"].iloc[i]) if pd.notna(work["ratio_bias_mom"].iloc[i]) else np.nan,
                "ratio_r2": float(work["ratio_r2"].iloc[i]) if pd.notna(work["ratio_r2"].iloc[i]) else np.nan,
                "futures_drag": drag,
                "active_spread_ret": active_ret,
            }
        )
        holding = signal_on

    result = pd.DataFrame(rows).set_index("date")
    if vol_scale_enabled:
        result = apply_vol_scaling(
            result=result,
            trading_days=CN_TRADING_DAYS,
            vol_window=vol_window,
            target_vol=target_vol,
            min_lev=min_lev,
            max_lev=max_lev,
            scale_threshold=scale_threshold,
        )
    else:
        result["weight"] = 1.0
        result["realized_vol"] = np.nan
        result["scale_raw"] = np.nan
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


def build_summary(
    result: pd.DataFrame,
    args: argparse.Namespace,
    close_df: pd.DataFrame,
) -> dict[str, object]:
    metrics = calc_metrics(result["return"])
    holding_series = result["holding"]
    active_series = holding_series != "cash"
    spell_ids = holding_series.ne(holding_series.shift()).cumsum()
    spell_frame = pd.DataFrame({"holding": holding_series, "spell_id": spell_ids})
    spells = spell_frame.loc[spell_frame["holding"] != "cash"].groupby("spell_id").size()

    yearly: dict[str, float] = {}
    for year in sorted(result.index.year.unique()):
        part = result.loc[result.index.year == year, "return"]
        if len(part) > 10:
            yearly[str(year)] = float((1.0 + part).prod() - 1.0)

    latest = result.iloc[-1]
    return {
        "strategy": args.output_prefix,
        "panel_path": str(args.panel_path),
        "microcap_column": args.microcap_column,
        "microcap_label": DEFAULT_MICROCAP_LABEL,
        "microcap_csv": getattr(args, "resolved_microcap_csv", None),
        "hedge_column": args.hedge_column,
        "lookback": args.lookback,
        "signal_model": args.signal_model,
        "bias_n": args.bias_n,
        "bias_mom_day": args.bias_mom_day,
        "r2_window": args.r2_window,
        "r2_threshold": args.r2_threshold,
        "vol_scale_enabled": bool(args.vol_scale_enabled),
        "target_vol": args.target_vol,
        "vol_window": args.vol_window,
        "max_lev": args.max_lev,
        "min_lev": args.min_lev,
        "scale_threshold": args.scale_threshold,
        "futures_drag_per_day": args.futures_drag,
        "require_positive_microcap_mom": bool(args.require_positive_microcap_mom),
        "entry_rule": (
            f"ratio_bias_mom(price/MA{args.bias_n}, slope_window={args.bias_mom_day}) > 0"
            if args.signal_model == "bias_momentum"
            else (
                "microcap_mom > hedge_mom and microcap_mom > 0"
                if args.require_positive_microcap_mom
                else "microcap_mom > hedge_mom"
            )
        ),
        "r2_filter_rule": (
            f"ratio_r2({args.r2_window}) >= {args.r2_threshold:.2f}"
            if args.r2_threshold > 0
            else "disabled"
        ),
        "return_rule_when_active": "microcap_return - hedge_return - futures_drag_per_day",
        "start_date": str(result.index[0].date()),
        "end_date": str(result.index[-1].date()),
        "n_days": int(len(result)),
        "active_days_pct": float(active_series.mean()),
        "cash_days_pct": float((~active_series).mean()),
        "signal_changes": int(result["signal_on"].ne(result["signal_on"].shift()).sum() - 1),
        "median_holding_spell": float(spells.median()) if len(spells) else 0.0,
        "latest_signal": {
            "date": str(result.index[-1].date()),
            "next_holding": str(latest["next_holding"]),
            "microcap_mom": float(latest["microcap_mom"]),
            "hedge_mom": float(latest["hedge_mom"]),
            "momentum_gap": float(latest["momentum_gap"]),
            "ratio_bias_mom": float(latest["ratio_bias_mom"]) if pd.notna(latest["ratio_bias_mom"]) else None,
            "ratio_r2": float(latest["ratio_r2"]) if pd.notna(latest["ratio_r2"]) else None,
            "weight": float(latest["weight"]) if pd.notna(latest["weight"]) else None,
            "microcap_close": float(close_df["microcap"].iloc[-1]),
            "hedge_close": float(close_df["hedge"].iloc[-1]),
        },
        "metrics": asdict(metrics),
        "yearly": yearly,
    }


def plot_nav(nav: pd.Series, output_path: Path) -> None:
    plt.figure(figsize=(12, 6))
    plt.plot(nav.index, nav.values, linewidth=1.8, label="Microcap / CSI1000 Hedge")
    plt.title("Microcap Long + CSI1000 Hedge NAV")
    plt.grid(alpha=0.25)
    plt.legend()
    plt.tight_layout()
    plt.savefig(output_path, dpi=160)
    plt.close()


def main() -> None:
    args = parse_args()
    output_paths = build_output_paths(args.output_prefix)
    close_df = build_close_df(args)
    result = run_backtest(
        close_df=close_df,
        signal_model=args.signal_model,
        lookback=args.lookback,
        bias_n=args.bias_n,
        bias_mom_day=args.bias_mom_day,
        futures_drag=args.futures_drag,
        require_positive_microcap_mom=args.require_positive_microcap_mom,
        r2_window=args.r2_window,
        r2_threshold=args.r2_threshold,
        vol_scale_enabled=args.vol_scale_enabled,
        target_vol=args.target_vol,
        vol_window=args.vol_window,
        max_lev=args.max_lev,
        min_lev=args.min_lev,
        scale_threshold=args.scale_threshold,
    )
    latest_signal = build_latest_signal(result)
    summary = build_summary(result=result, args=args, close_df=close_df)

    result.to_csv(output_paths["nav"], index_label="date", encoding="utf-8")
    latest_signal.to_csv(output_paths["signal"], index=False, encoding="utf-8")
    output_paths["summary"].write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    plot_nav(result["nav"], output_paths["curve"])

    print(json.dumps(summary, ensure_ascii=False, indent=2))
    print(f"saved {output_paths['nav'].name}")
    print(f"saved {output_paths['signal'].name}")
    print(f"saved {output_paths['summary'].name}")
    print(f"saved {output_paths['curve'].name}")


'''
COST_SOURCE = r'''
from __future__ import annotations

import argparse
import json
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pandas as pd



ROOT = Path(__file__).resolve().parent
OUTPUT_DIR = ROOT / "outputs"
DEFAULT_MICROCAP_CSV = OUTPUT_DIR / "wind_microcap_top_100_monthly_16y.csv"
DEFAULT_TURNOVER_CSV = OUTPUT_DIR / "microcap_top100_monthly_turnover_stats.csv"
DEFAULT_OUTPUT_PREFIX = "microcap_top100_momentum_16y_cost_scan"
ENTRY_COST = 0.003
EXIT_COST = 0.003
MONTHLY_REBALANCE_ONE_SIDE = 0.003


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Scan Top 100 relative momentum lookbacks under the microcap trading cost model."
    )
    parser.add_argument("--panel-path", type=Path, default=hedge_mod.DEFAULT_PANEL)
    parser.add_argument("--microcap-csv", type=Path, default=DEFAULT_MICROCAP_CSV)
    parser.add_argument("--turnover-csv", type=Path, default=DEFAULT_TURNOVER_CSV)
    parser.add_argument("--hedge-column", default=hedge_mod.DEFAULT_HEDGE_COLUMN)
    parser.add_argument("--lookback-start", type=int, default=1)
    parser.add_argument("--lookback-end", type=int, default=20)
    parser.add_argument("--futures-drag", type=float, default=hedge_mod.DEFAULT_FUTURES_DRAG)
    parser.add_argument("--output-prefix", default=DEFAULT_OUTPUT_PREFIX)
    return parser.parse_args()


def build_close_df(args: argparse.Namespace) -> pd.DataFrame:
    ns = SimpleNamespace(
        panel_path=args.panel_path,
        microcap_column=hedge_mod.DEFAULT_MICROCAP_COLUMN,
        hedge_column=args.hedge_column,
        lookback=args.lookback_end,
        signal_model="momentum",
        bias_n=hedge_mod.DEFAULT_BIAS_N,
        bias_mom_day=hedge_mod.DEFAULT_BIAS_MOM_DAY,
        futures_drag=args.futures_drag,
        r2_window=hedge_mod.DEFAULT_R2_WINDOW,
        r2_threshold=0.0,
        vol_scale_enabled=False,
        target_vol=hedge_mod.DEFAULT_TARGET_VOL,
        vol_window=hedge_mod.DEFAULT_VOL_WINDOW,
        max_lev=hedge_mod.DEFAULT_MAX_LEV,
        min_lev=hedge_mod.DEFAULT_MIN_LEV,
        scale_threshold=hedge_mod.DEFAULT_SCALE_THRESHOLD,
        microcap_csv=args.microcap_csv,
        microcap_date_col="date",
        microcap_close_col="close",
    )
    return hedge_mod.build_close_df(ns)


def load_turnover_table(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Turnover table not found: {path}")
    turnover = pd.read_csv(path)
    required = {"rebalance_date", "two_side_cost_rate"}
    missing = required.difference(turnover.columns)
    if missing:
        raise ValueError(f"Turnover table missing columns: {sorted(missing)}")
    turnover["rebalance_date"] = pd.to_datetime(turnover["rebalance_date"])
    return turnover.sort_values("rebalance_date").reset_index(drop=True)


def map_rebalance_apply_costs(index: pd.Index, turnover: pd.DataFrame) -> pd.Series:
    cost_series = pd.Series(0.0, index=index, dtype=float)
    date_array = index.to_numpy()
    for row in turnover.itertuples(index=False):
        execution_timing = getattr(row, "execution_timing", "next_open")
        cost_date = getattr(row, "execution_date", row.rebalance_date)
        side = "left" if execution_timing == "close" else "right"
        pos = date_array.searchsorted(np.datetime64(cost_date), side=side)
        if pos < len(date_array):
            cost_series.iloc[pos] += float(row.two_side_cost_rate)
    return cost_series


def apply_cost_model(result: pd.DataFrame, turnover: pd.DataFrame) -> pd.DataFrame:
    out = result.copy()
    execution_timing = "next_open"
    if len(turnover) and "execution_timing" in turnover.columns:
        non_null = turnover["execution_timing"].dropna().astype(str)
        if not non_null.empty:
            execution_timing = non_null.iloc[0]

    if execution_timing == "close":
        active = out["next_holding"].ne("cash")
        prev_active = out["holding"].ne("cash")
    else:
        active = out["holding"].ne("cash")
        prev_active = active.shift(1, fill_value=False)

    entry_cost = pd.Series(0.0, index=out.index, dtype=float)
    entry_cost.loc[active & ~prev_active] = ENTRY_COST

    exit_cost = pd.Series(0.0, index=out.index, dtype=float)
    exit_cost.loc[~active & prev_active] = EXIT_COST

    rebalance_base = map_rebalance_apply_costs(out.index, turnover)
    rebalance_cost = rebalance_base.where(active & prev_active, 0.0)

    out["entry_exit_cost"] = entry_cost + exit_cost
    out["rebalance_cost"] = rebalance_cost
    out["total_cost"] = out["entry_exit_cost"] + out["rebalance_cost"]
    out["return_net"] = (1.0 + out["return"]) * (1.0 - out["total_cost"]) - 1.0
    out["nav_net"] = (1.0 + out["return_net"]).cumprod()
    return out


def calc_drawdown_info(ret: pd.Series) -> dict[str, object]:
    nav = (1.0 + ret).cumprod()
    dd = nav.div(nav.cummax()).sub(1.0)
    trough_date = dd.idxmin()
    peak_date = nav.loc[:trough_date].idxmax()
    post = nav.loc[trough_date:]
    recovery = post[post >= nav.loc[peak_date]]
    recovery_date = recovery.index[0] if len(recovery) else pd.NaT
    return {
        "peak_date": str(peak_date.date()),
        "trough_date": str(trough_date.date()),
        "recovery_date": None if pd.isna(recovery_date) else str(recovery_date.date()),
    }


def summarize_scan_row(lookback: int, gross: pd.DataFrame, net: pd.DataFrame) -> dict[str, object]:
    gross_metrics = hedge_mod.calc_metrics(gross["return"])
    net_metrics = hedge_mod.calc_metrics(net["return_net"])
    active = gross["holding"].ne("cash")
    active_prev = active.shift(1, fill_value=False)
    dd_info = calc_drawdown_info(net["return_net"])
    return {
        "lookback": lookback,
        "gross_annual": gross_metrics.annual,
        "gross_max_dd": gross_metrics.max_dd,
        "gross_sharpe": gross_metrics.sharpe,
        "gross_vol": gross_metrics.vol,
        "gross_total_return": gross_metrics.total_return,
        "net_annual": net_metrics.annual,
        "net_max_dd": net_metrics.max_dd,
        "net_sharpe": net_metrics.sharpe,
        "net_vol": net_metrics.vol,
        "net_total_return": net_metrics.total_return,
        "active_days_pct": float(active.mean()),
        "signal_changes": int(gross["signal_on"].ne(gross["signal_on"].shift()).sum() - 1),
        "entry_days": int((active & ~active_prev).sum()),
        "exit_days": int((~active & active_prev).sum()),
        "rebalance_cost_days": int(net["rebalance_cost"].gt(0).sum()),
        "entry_exit_cost_sum": float(net["entry_exit_cost"].sum()),
        "rebalance_cost_sum": float(net["rebalance_cost"].sum()),
        "total_cost_sum": float(net["total_cost"].sum()),
        "peak_date": dd_info["peak_date"],
        "trough_date": dd_info["trough_date"],
        "recovery_date": dd_info["recovery_date"],
    }


def build_position_payload(scan_df: pd.DataFrame, lookbacks: list[int]) -> dict[str, object]:
    ordered = scan_df.sort_values(["net_sharpe", "net_annual"], ascending=[False, False]).reset_index(drop=True)
    payload: dict[str, object] = {
        "ranking_rule": "sort by net_sharpe desc, then net_annual desc",
        "cost_model": {
            "entry_buy_one_side": ENTRY_COST,
            "exit_sell_one_side": EXIT_COST,
            "monthly_rebalance_one_side": MONTHLY_REBALANCE_ONE_SIDE,
            "rebalance_cost_formula": "2 * 0.003 * replaced_fraction",
            "note": "Only microcap stock basket cost is added. Futures leg keeps daily drag 3/10000.",
        },
    }
    for lb in lookbacks:
        subset = ordered.loc[ordered["lookback"] == lb]
        if subset.empty:
            continue
        target = subset.iloc[0].to_dict()
        payload[f"lookback_{lb}_rank"] = {
            "rank": int(subset.index[0] + 1),
            "total": int(len(ordered)),
            "target_row": target,
            "top10": ordered.head(10).to_dict(orient="records"),
        }
    return payload


def main() -> None:
    args = parse_args()
    close_df = build_close_df(args)
    turnover = load_turnover_table(args.turnover_csv)

    rows: list[dict[str, object]] = []
    costed_nav_8: pd.DataFrame | None = None

    for lookback in range(args.lookback_start, args.lookback_end + 1):
        gross = hedge_mod.run_backtest(
            close_df=close_df,
            signal_model="momentum",
            lookback=lookback,
            bias_n=hedge_mod.DEFAULT_BIAS_N,
            bias_mom_day=hedge_mod.DEFAULT_BIAS_MOM_DAY,
            futures_drag=args.futures_drag,
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
        net = apply_cost_model(gross, turnover)
        if lookback == 8:
            costed_nav_8 = net.copy()
        rows.append(summarize_scan_row(lookback=lookback, gross=gross, net=net))

    scan_df = pd.DataFrame(rows)
    scan_df = scan_df.sort_values(["net_sharpe", "net_annual"], ascending=[False, False]).reset_index(drop=True)
    scan_df["net_rank"] = np.arange(1, len(scan_df) + 1)
    scan_df["gross_rank"] = scan_df["gross_sharpe"].rank(ascending=False, method="min").astype(int)
    scan_df = scan_df[
        [
            "lookback",
            "gross_rank",
            "net_rank",
            "gross_annual",
            "gross_max_dd",
            "gross_sharpe",
            "net_annual",
            "net_max_dd",
            "net_sharpe",
            "entry_days",
            "exit_days",
            "rebalance_cost_days",
            "entry_exit_cost_sum",
            "rebalance_cost_sum",
            "total_cost_sum",
            "active_days_pct",
            "signal_changes",
            "peak_date",
            "trough_date",
            "recovery_date",
        ]
    ]

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    scan_path = OUTPUT_DIR / f"{args.output_prefix}.csv"
    position_path = OUTPUT_DIR / f"{args.output_prefix}_position.json"
    scan_df.to_csv(scan_path, index=False, encoding="utf-8")

    payload = build_position_payload(scan_df=scan_df, lookbacks=[8, 5])
    position_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    if costed_nav_8 is not None:
        nav8_path = OUTPUT_DIR / "microcap_top100_mom8_hedge_zz1000_16y_costed_nav.csv"
        costed_nav_8.to_csv(nav8_path, index_label="date", encoding="utf-8")

    print(scan_df.head(10).to_string(index=False))
    print(f"saved {scan_path.name}")
    print(f"saved {position_path.name}")


'''
FETCH_SOURCE = r'''
from __future__ import annotations

import argparse
import json
import os
import time
import warnings
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
_FREQ_MOD_FOR_ST: object | None = None


def set_freq_module(mod: object | None) -> None:
    global _FREQ_MOD_FOR_ST
    _FREQ_MOD_FOR_ST = mod

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
        help=(
            "Public proxy only. Exclude ST stocks from rebalance selection using historical ST masks "
            "where available; the current ST board is retained as a snapshot diagnostic."
        ),
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
    last_error: Exception | None = None
    for fetcher in (ak.stock_zh_a_spot_em, ak.stock_zh_a_spot):
        try:
            spot = fetcher()
            frame = spot[[COL_CODE, COL_NAME]].copy()
            break
        except Exception as exc:
            last_error = exc
    else:
        raise RuntimeError(f"failed to fetch active A-share universe: {last_error}") from last_error
    frame.columns = ["symbol", "name"]
    frame = frame.drop_duplicates(subset="symbol")
    frame["code"] = frame["symbol"].astype(str).str[-6:].str.zfill(6)
    prefixed = frame["symbol"].astype(str).str.startswith(("sh", "sz"))
    frame = frame.loc[prefixed | frame["code"].str.match(r"^\d{6}$")].copy()
    inferred_prefix = np.where(frame["code"].str.startswith(("5", "6", "9")), "sh", "sz")
    frame["symbol"] = np.where(prefixed.reindex(frame.index).fillna(False), frame["symbol"], inferred_prefix + frame["code"])
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


def build_historical_st_status_series(symbol: str, trading_dates: pd.DatetimeIndex) -> pd.Series:
    mod = _FREQ_MOD_FOR_ST
    if mod is None:
        return pd.Series(False, index=trading_dates, dtype=bool)
    try:
        meta = mod.load_security_meta(str(symbol).zfill(6))
        series = mod.build_st_status_series(meta, trading_dates)
    except Exception:
        series = pd.Series(False, index=trading_dates, dtype=bool)
    return series.reindex(trading_dates).fillna(False).astype(bool)


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


def _tx_symbol(symbol: str) -> str:
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


def _fetch_adjusted_price_history_tx(symbol: str, start_ts: pd.Timestamp, end_ts: pd.Timestamp) -> pd.DataFrame:
    from akshare.stock_feature.stock_hist_tx import stock_zh_a_hist_tx

    frame = stock_zh_a_hist_tx(
        symbol=_tx_symbol(symbol),
        start_date=start_ts.strftime("%Y-%m-%d"),
        end_date=end_ts.strftime("%Y-%m-%d"),
        adjust="qfq",
        timeout=20,
    )
    if frame.empty:
        raise ValueError(f"empty adjusted price history for {symbol}")
    frame = frame[["date", "close"]].copy()
    frame.columns = ["date", "close_qfq"]
    frame["date"] = pd.to_datetime(frame["date"])
    frame["close_qfq"] = pd.to_numeric(frame["close_qfq"], errors="coerce")
    frame = frame.dropna(subset=["close_qfq"]).sort_values("date")
    if frame.empty:
        raise ValueError(f"parsed empty adjusted price history for {symbol}")
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

    try:
        frame = _fetch_adjusted_price_history_tx(symbol, fetch_start_ts, end_ts)
    except Exception:
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

    if args.limit_symbols:
        universe = universe.head(args.limit_symbols).copy()
    current_st_snapshot_count = int(universe["code"].isin(current_st_codes).sum()) if current_st_codes else 0

    symbol_panels: dict[str, pd.DataFrame] = {}
    failures: dict[str, str] = {}

    worker_args = [
        (symbol, args.start_date, args.end_date, args.force_refresh)
        for symbol in universe["code"].tolist()
    ]
    executor_name = args.executor
    if executor_name == "auto":
        executor_name = "thread" if os.name == "nt" else ("process" if args.max_workers and args.max_workers > 1 else "thread")
    elif executor_name == "process" and os.name == "nt":
        executor_name = "thread"

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
    st_status_by_symbol: dict[str, pd.Series] = {}
    historical_st_cap_excluded = 0
    if args.exclude_current_st:
        st_status_by_symbol = {
            symbol: build_historical_st_status_series(symbol, trading_dates)
            for symbol in symbol_panels
        }

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
        st_status = st_status_by_symbol.get(symbol)
        if st_status is not None and not st_status.empty:
            st_on_rebalance = st_status.reindex(pd.DatetimeIndex(cap_lookup["date"])).fillna(False).to_numpy()
            historical_st_cap_excluded += int((st_on_rebalance & cap_lookup["market_cap"].notna().to_numpy()).sum())
            cap_lookup.loc[st_on_rebalance, "market_cap"] = np.nan
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
            "Historical ST exclusion uses CNInfo ST notices, SZSE name-change records, and current-name snapshots where available.",
            "ST gaps can remain when public notices/name evidence is unavailable; unresolved symbols are not excluded from the whole sample solely because they are current ST.",
            "Returns use raw close, so corporate-action handling will differ from the official index divisor methodology.",
            "Active universe is built from current SH/SZ A-shares; delisted historical names are not fully backfilled.",
        ],
        "stats": asdict(
            BuildStats(
                symbols_total=int(len(universe)),
                symbols_success=int(len(symbol_panels)),
                symbols_failed=int(len(failures)),
                current_st_excluded=int(current_st_snapshot_count),
                rebalance_dates=int(len(rebalance_dates)),
                active_days=int(active_days),
            )
        ),
        "historical_st_cap_excluded": int(historical_st_cap_excluded),
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
    if args.exclude_current_st and historical_st_cap_excluded == 0 and len(symbol_panels) > 500:
        warnings.warn(
            "historical ST cap exclusion removed zero rows for a large proxy build; "
            "verify embedded frequency metadata is available.",
            RuntimeWarning,
        )
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


'''
FREQ_SOURCE = r'''
from __future__ import annotations

import json
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
from decimal import ROUND_HALF_UP, Decimal
from io import BytesIO
from pathlib import Path

import akshare as ak
import numpy as np
import pandas as pd
import requests
import urllib3
from pandas.errors import PerformanceWarning



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
SECURITY_MASTER_CACHE = CACHE_DIR / "security_master.csv"
SZ_NAME_CHANGE_CACHE = CACHE_DIR / "sz_name_change_short.csv"
CNINFO_ORG_MAP_CACHE = CACHE_DIR / "cninfo_a_org_map.csv"
FALLBACK_OHLC_DIR = ROOT / ".microcap_ohlc_cache"

START_DATE = "2010-01-04"
END_DATE = pd.Timestamp.today().strftime("%Y-%m-%d")
LOOKBACK = 16
TOP_N = 100
SECURITY_META_VERSION = 2
CHINEXT_LIMIT_SWITCH = pd.Timestamp("2020-08-24")
LIMIT_PRICE_REL_EPS = 1e-4
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
SHARED_SECURITY_MASTER_CACHE = SHARED_CACHE_DIR / "security_master.csv" if SHARED_CACHE_DIR else None

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

_SECURITY_MASTER_MEMO: pd.DataFrame | None = None


def _first_existing_column(frame: pd.DataFrame, candidates: list[str]) -> str:
    for candidate in candidates:
        if candidate in frame.columns:
            return candidate
    raise KeyError(f"missing expected columns {candidates}; got {list(frame.columns)}")


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


def _normalize_security_master_frame(frame: pd.DataFrame) -> pd.DataFrame:
    work = frame.copy()
    if work.empty:
        return pd.DataFrame(columns=["symbol", "exchange", "board", "list_date", "delist_date", "name", "source"])
    work["symbol"] = work["symbol"].astype(str).str.extract(r"(\d{6})", expand=False)
    work = work.dropna(subset=["symbol"]).copy()
    work["symbol"] = work["symbol"].str.zfill(6)
    for col in ["list_date", "delist_date"]:
        work[col] = pd.to_datetime(work.get(col), errors="coerce")
    for col in ["exchange", "board", "name", "source"]:
        if col not in work.columns:
            work[col] = None
    work = work.sort_values(["symbol", "list_date", "delist_date"], na_position="last")

    rows: list[dict[str, object]] = []
    for symbol, group in work.groupby("symbol", sort=True):
        list_dates = group["list_date"].dropna()
        delist_dates = group["delist_date"].dropna()
        rows.append(
            {
                "symbol": symbol,
                "exchange": group["exchange"].dropna().iloc[-1] if group["exchange"].notna().any() else None,
                "board": group["board"].dropna().iloc[-1] if group["board"].notna().any() else None,
                "list_date": list_dates.min() if not list_dates.empty else pd.NaT,
                "delist_date": delist_dates.max() if not delist_dates.empty else pd.NaT,
                "name": group["name"].dropna().iloc[-1] if group["name"].notna().any() else None,
                "source": "|".join(sorted({str(item) for item in group["source"].dropna().tolist()})) or None,
            }
        )
    return pd.DataFrame(rows).sort_values("symbol").reset_index(drop=True)


def _infer_exchange_board(symbol: str) -> tuple[str | None, str | None]:
    code = str(symbol).zfill(6)
    if code.startswith("688"):
        return "SSE", "科创板"
    if code.startswith(("600", "601", "603", "605")):
        return "SSE", "主板"
    if code.startswith(("300", "301")):
        return "SZSE", "创业板"
    if code.startswith(("000", "001", "002", "003")):
        return "SZSE", "主板"
    if code.startswith(("430", "831", "832", "833", "834", "835", "836", "837", "838", "839", "870", "871", "872", "873", "874", "875", "876", "877", "878", "879", "920")):
        return "BSE", "北交所"
    return None, None


def _build_master_rows_from_cache(symbols: set[str]) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for symbol in sorted(symbols):
        price_path = resolve_cache_path(PRICE_DIR, SHARED_PRICE_DIR, symbol)
        if price_path is None:
            continue
        try:
            price = pd.read_csv(price_path, usecols=["date"])
        except Exception:
            continue
        price["date"] = pd.to_datetime(price["date"], errors="coerce")
        price = price.dropna(subset=["date"]).sort_values("date")
        if price.empty:
            continue
        exchange, board = _infer_exchange_board(symbol)
        rows.append(
            {
                "symbol": symbol,
                "exchange": exchange,
                "board": board,
                "list_date": pd.Timestamp(price["date"].min()).normalize(),
                "delist_date": pd.NaT,
                "name": None,
                "source": "cache_fallback",
            }
        )
    return pd.DataFrame(rows)


def _fetch_sz_current_security_master() -> pd.DataFrame:
    try:
        frame = ak.stock_info_sz_name_code()
    except Exception:
        response = requests.get(
            "https://www.szse.cn/api/report/ShowReport",
            params={"SHOWTYPE": "xlsx", "CATALOGID": "1110", "TABKEY": "tab1"},
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=30,
            verify=False,
        )
        response.raise_for_status()
        frame = pd.read_excel(BytesIO(response.content), dtype=str, engine="openpyxl")
    symbol_col = _first_existing_column(frame, ["A股代码", "证券代码"])
    list_date_col = _first_existing_column(frame, ["A股上市日期", "上市日期"])
    name_col = _first_existing_column(frame, ["A股简称", "证券简称"])
    return pd.DataFrame(
        {
            "symbol": frame[symbol_col],
            "exchange": "SZSE",
            "board": frame.get("板块"),
            "list_date": frame[list_date_col],
            "delist_date": None,
            "name": frame[name_col],
            "source": "sz_current",
        }
    )


def build_security_master() -> pd.DataFrame:
    frames: list[pd.DataFrame] = []

    try:
        sh_current = ak.stock_info_sh_name_code()
        frames.append(
            pd.DataFrame(
                {
                    "symbol": sh_current["证券代码"],
                    "exchange": "SSE",
                    "board": sh_current["证券代码"].astype(str).map(
                        lambda value: "科创板" if str(value).zfill(6).startswith("688") else "主板"
                    ),
                    "list_date": sh_current["上市日期"],
                    "delist_date": None,
                    "name": sh_current["证券简称"],
                    "source": "sh_current",
                }
            )
        )
    except Exception:
        pass

    try:
        frames.append(_fetch_sz_current_security_master())
    except Exception:
        pass

    try:
        bj_current = ak.stock_info_bj_name_code()
        symbol_col = _first_existing_column(bj_current, ["证券代码", "A股代码"])
        list_date_col = _first_existing_column(bj_current, ["上市日期", "A股上市日期"])
        name_col = _first_existing_column(bj_current, ["证券简称", "A股简称"])
        frames.append(
            pd.DataFrame(
                {
                    "symbol": bj_current[symbol_col],
                    "exchange": "BSE",
                    "board": "北交所",
                    "list_date": bj_current[list_date_col],
                    "delist_date": None,
                    "name": bj_current[name_col],
                    "source": "bj_current",
                }
            )
        )
    except Exception:
        pass

    try:
        sh_delist = ak.stock_info_sh_delist()
        frames.append(
            pd.DataFrame(
                {
                    "symbol": sh_delist["公司代码"],
                    "exchange": "SSE",
                    "board": sh_delist["公司代码"].astype(str).map(
                        lambda value: "科创板" if str(value).zfill(6).startswith("688") else "主板"
                    ),
                    "list_date": sh_delist["上市日期"],
                    "delist_date": sh_delist["暂停上市日期"],
                    "name": sh_delist["公司简称"],
                    "source": "sh_delist",
                }
            )
        )
    except Exception:
        pass

    try:
        sz_delist = ak.stock_info_sz_delist()
        frames.append(
            pd.DataFrame(
                {
                    "symbol": sz_delist["证券代码"],
                    "exchange": "SZSE",
                    "board": sz_delist["证券代码"].astype(str).map(
                        lambda value: "创业板" if str(value).zfill(6).startswith(("300", "301")) else "主板"
                    ),
                    "list_date": sz_delist["上市日期"],
                    "delist_date": sz_delist["终止上市日期"],
                    "name": sz_delist["证券简称"],
                    "source": "sz_delist",
                }
            )
        )
    except Exception:
        pass

    if frames:
        master = _normalize_security_master_frame(pd.concat(frames, ignore_index=True))
    else:
        master = _normalize_security_master_frame(pd.DataFrame())
    cached_symbols = _existing_symbols(PRICE_DIR, SHARED_PRICE_DIR) & _existing_symbols(SHARE_DIR, SHARED_SHARE_DIR)
    missing_symbols = cached_symbols - set(master["symbol"].dropna().astype(str).str.zfill(6))
    if missing_symbols:
        master = _normalize_security_master_frame(
            pd.concat([master, _build_master_rows_from_cache(missing_symbols)], ignore_index=True)
        )
    SECURITY_MASTER_CACHE.parent.mkdir(parents=True, exist_ok=True)
    master.to_csv(SECURITY_MASTER_CACHE, index=False, encoding="utf-8")
    global _SECURITY_MASTER_MEMO
    _SECURITY_MASTER_MEMO = master
    return master


def load_security_master(force_refresh: bool = False) -> pd.DataFrame:
    global _SECURITY_MASTER_MEMO
    if not force_refresh and _SECURITY_MASTER_MEMO is not None:
        return _SECURITY_MASTER_MEMO
    if not force_refresh and SECURITY_MASTER_CACHE.exists():
        _SECURITY_MASTER_MEMO = pd.read_csv(
            SECURITY_MASTER_CACHE,
            dtype={"symbol": str},
            parse_dates=["list_date", "delist_date"],
        )
        return _SECURITY_MASTER_MEMO
    if not force_refresh and SHARED_SECURITY_MASTER_CACHE is not None and SHARED_SECURITY_MASTER_CACHE.exists():
        _SECURITY_MASTER_MEMO = pd.read_csv(
            SHARED_SECURITY_MASTER_CACHE,
            dtype={"symbol": str},
            parse_dates=["list_date", "delist_date"],
        )
        return _SECURITY_MASTER_MEMO
    try:
        return build_security_master()
    except Exception:
        if SECURITY_MASTER_CACHE.exists():
            _SECURITY_MASTER_MEMO = pd.read_csv(
                SECURITY_MASTER_CACHE,
                dtype={"symbol": str},
                parse_dates=["list_date", "delist_date"],
            )
            return _SECURITY_MASTER_MEMO
        return pd.DataFrame(columns=["symbol", "exchange", "board", "list_date", "delist_date", "name", "source"])


def list_backtest_universe_symbols() -> list[str]:
    price_symbols = _existing_symbols(PRICE_DIR, SHARED_PRICE_DIR)
    share_symbols = _existing_symbols(SHARE_DIR, SHARED_SHARE_DIR)
    cached_symbols = price_symbols & share_symbols
    master = load_security_master()
    if master.empty or "symbol" not in master.columns:
        return sorted(cached_symbols)
    master_symbols = set(master["symbol"].dropna().astype(str).str.zfill(6))
    return sorted(cached_symbols & master_symbols)


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
    return merge_st_intervals(intervals)


def build_st_interval_from_current_name_snapshot(
    first_trade_date: pd.Timestamp,
    last_trade_date: pd.Timestamp,
    current_name: str | None,
) -> list[dict[str, str | None]]:
    if not is_st_name(current_name):
        return []
    last_trade = pd.Timestamp(last_trade_date).normalize()
    if pd.isna(last_trade):
        return []
    first_trade = pd.Timestamp(first_trade_date).normalize()
    start = max(first_trade, last_trade)
    return [{"start": str(start.date()), "end": None, "source": "current_name_snapshot"}]


def merge_st_intervals(intervals: list[dict[str, str | None]]) -> list[dict[str, str | None]]:
    if not intervals:
        return []
    work: list[dict[str, object]] = []
    for item in intervals:
        if not isinstance(item, dict):
            continue
        start = pd.to_datetime(item.get("start"), errors="coerce")
        if pd.isna(start):
            continue
        end = pd.to_datetime(item.get("end"), errors="coerce")
        work.append(
            {
                "start": pd.Timestamp(start).normalize(),
                "end": None if pd.isna(end) else pd.Timestamp(end).normalize(),
                "source": str(item.get("source") or "").strip() or None,
            }
        )
    if not work:
        return []
    work.sort(key=lambda x: (x["start"], pd.Timestamp.max if x["end"] is None else x["end"]))
    merged: list[dict[str, object]] = []
    for item in work:
        if not merged:
            merged.append(item.copy())
            continue
        prev = merged[-1]
        prev_end = prev["end"]
        curr_end = item["end"]
        can_merge = prev_end is None or item["start"] <= prev_end + pd.Timedelta(days=1)
        if can_merge:
            if prev_end is None or curr_end is None:
                prev["end"] = None
            else:
                prev["end"] = max(prev_end, curr_end)
            sources = {s for s in [prev.get("source"), item.get("source")] if s}
            prev["source"] = "|".join(sorted(sources)) if sources else None
            continue
        merged.append(item.copy())
    return [
        {
            "start": str(item["start"].date()),
            "end": None if item["end"] is None else str(item["end"].date()),
            "source": item.get("source"),
        }
        for item in merged
    ]


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
    return merge_st_intervals(intervals)


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


def build_active_status_series(meta: dict[str, object] | None, dates: pd.DatetimeIndex) -> pd.Series:
    series = pd.Series(True, index=dates, dtype=bool)
    if not meta:
        return series
    list_date = pd.to_datetime(meta.get("list_date"), errors="coerce")
    delist_date = pd.to_datetime(meta.get("delist_date"), errors="coerce")
    if pd.notna(list_date):
        series &= dates >= pd.Timestamp(list_date).normalize()
    if pd.notna(delist_date):
        series &= dates <= pd.Timestamp(delist_date).normalize()
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
    frame = pd.read_excel(BytesIO(response.content), engine="openpyxl")
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
    local_path = SECURITY_META_DIR / f"{str(symbol).zfill(6)}.json"
    if local_path.exists():
        return local_path
    if SHARED_SECURITY_META_DIR is not None:
        shared_path = SHARED_SECURITY_META_DIR / f"{str(symbol).zfill(6)}.json"
        if shared_path.exists():
            return shared_path
    return None


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
    master = load_security_master()
    master_row: pd.Series | None = None
    if not master.empty:
        matched = master.loc[master["symbol"].astype(str).str.zfill(6) == str(symbol).zfill(6)]
        if not matched.empty:
            master_row = matched.iloc[-1]
    name_intervals: list[dict[str, str | None]] = []
    notice_intervals: list[dict[str, str | None]] = []
    current_name_intervals: list[dict[str, str | None]] = []

    if str(symbol).zfill(6).startswith(("000", "001", "002", "003", "300", "301")):
        try:
            changes = fetch_sz_name_change_history()
            symbol_changes = changes.loc[changes["symbol"] == str(symbol).zfill(6), ["change_date", "old_name", "new_name"]]
            name_intervals = build_st_intervals_from_name_changes(first_trade, last_trade, symbol_changes)
        except Exception:
            name_intervals = []

    try:
        notices = fetch_cninfo_st_notices(
            symbol=str(symbol).zfill(6),
            start_date=first_trade.strftime("%Y%m%d"),
            end_date=last_trade.strftime("%Y%m%d"),
        )
        notice_intervals = build_st_intervals_from_notices(first_trade, last_trade, notices)
    except Exception:
        notice_intervals = []

    if master_row is not None:
        current_name_intervals = build_st_interval_from_current_name_snapshot(
            first_trade,
            last_trade,
            None if pd.isna(master_row.get("name")) else str(master_row.get("name")),
        )

    st_intervals = merge_st_intervals([*name_intervals, *notice_intervals, *current_name_intervals])

    meta = {
        "meta_version": SECURITY_META_VERSION,
        "symbol": str(symbol).zfill(6),
        "first_trade_date": str(first_trade.date()),
        "last_trade_date": str(last_trade.date()),
        "list_date": (
            None
            if master_row is None or pd.isna(master_row.get("list_date"))
            else str(pd.Timestamp(master_row["list_date"]).date())
        ),
        "delist_date": (
            None
            if master_row is None or pd.isna(master_row.get("delist_date"))
            else str(pd.Timestamp(master_row["delist_date"]).date())
        ),
        "exchange": None if master_row is None else master_row.get("exchange"),
        "board": None if master_row is None else master_row.get("board"),
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
            payload = json.loads(meta_path.read_text(encoding="utf-8"))
            if payload.get("meta_version") == SECURITY_META_VERSION:
                return payload
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
    if code.startswith(("4", "8", "920")):
        return 0.3
    if is_st and code.startswith(("300", "301")) and pd.Timestamp(trade_date) < CHINEXT_LIMIT_SWITCH:
        return 0.05
    if code.startswith(("300", "301")):
        return 0.2 if pd.Timestamp(trade_date) >= CHINEXT_LIMIT_SWITCH else 0.1
    if code.startswith("688"):
        return 0.2
    if is_st:
        return 0.05
    return 0.1


def is_price_at_limit(
    price: float,
    prev_close: float,
    limit_ratio: float,
    direction: int,
) -> bool:
    if pd.isna(price) or pd.isna(prev_close) or float(prev_close) <= 0:
        return False
    if direction not in {-1, 1}:
        raise ValueError(f"unsupported limit direction: {direction}")
    limit_price = round_limit_price(float(prev_close) * (1.0 + direction * float(limit_ratio)))
    expected_return = limit_price / float(prev_close) - 1.0
    actual_return = float(price) / float(prev_close) - 1.0
    return abs(actual_return - expected_return) <= LIMIT_PRICE_REL_EPS


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
    up_locked = all(is_price_at_limit(float(price), float(prev_close), ratio, 1) for price in prices)
    down_locked = all(is_price_at_limit(float(price), float(prev_close), ratio, -1) for price in prices)
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
    up_blocked = is_price_at_limit(float(close_price), float(prev_close), ratio, 1)
    down_blocked = is_price_at_limit(float(close_price), float(prev_close), ratio, -1)
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
    raw_return_close_calendar = close_series.reindex(trading_dates)
    raw_return_close_calendar = raw_return_close_calendar.where(listed_mask)
    raw_return_close_calendar = raw_return_close_calendar.ffill().where(listed_mask)
    return_close_calendar = raw_return_close_calendar.copy()
    if return_price is not None and not return_price.empty:
        return_col = next((col for col in ["close_qfq", "close_adj", "close_raw"] if col in return_price.columns), None)
        if return_col is not None:
            adjusted_close_series = return_price.set_index("date")[return_col].sort_index()
            adjusted_listed_mask = pd.Series(
                (trading_dates >= adjusted_close_series.index.min())
                & (trading_dates <= adjusted_close_series.index.max()),
                index=trading_dates,
                dtype=bool,
            )
            adjusted_close_calendar = adjusted_close_series.reindex(trading_dates)
            adjusted_close_calendar = adjusted_close_calendar.where(adjusted_listed_mask)
            adjusted_close_calendar = adjusted_close_calendar.ffill().where(adjusted_listed_mask)
            return_close_calendar = adjusted_close_calendar.combine_first(raw_return_close_calendar)
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
    exclude_historical_st_from_caps: bool = True,
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
        active_series = build_active_status_series(meta, trading_dates)
        ret_series, tradeable_series, buyable_series, sellable_series = build_tradeability_series(
            symbol=symbol,
            price=price[["date", "close_raw"]].copy(),
            trading_dates=trading_dates,
            return_price=None if adjusted_price is None or adjusted_price.empty else adjusted_price,
            st_series=st_series,
            trade_constraint_mode=trade_constraint_mode,
        )
        ret_series = ret_series.where(active_series, np.nan)
        tradeable_series = tradeable_series.where(active_series, False).astype(bool)
        buyable_series = buyable_series.where(active_series, False).astype(bool)
        sellable_series = sellable_series.where(active_series, False).astype(bool)

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
        cap_active = build_active_status_series(meta, cap_dates).astype(bool)
        cap_series = cap_series.where(cap_active, np.nan)
        if exclude_historical_st_from_caps:
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
    exclude_historical_st_from_caps: bool = True,
) -> tuple[pd.DataFrame, dict[pd.Timestamp, dict[str, float]], pd.DataFrame, pd.DataFrame]:
    returns_df = pd.DataFrame(index=trading_dates)
    buyable_df = pd.DataFrame(index=trading_dates)
    sellable_df = pd.DataFrame(index=trading_dates)
    caps_by_date: dict[pd.Timestamp, dict[str, float]] = {pd.Timestamp(dt): {} for dt in cap_dates}
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {
            pool.submit(
                load_symbol_cache,
                symbol,
                trading_dates,
                cap_dates,
                trade_constraint_mode,
                exclude_historical_st_from_caps,
            ): symbol
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
            day_ret = returns_df.loc[dt, current_members].reindex(current_members)
            portfolio_ret = float(day_ret.fillna(0.0).mean()) if len(day_ret) else 0.0
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


'''
BASE_SOURCE = r'''
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



ROOT = Path(__file__).resolve().parent
OUTPUT_DIR = ROOT / "outputs"
CACHE_DIR = ROOT / ".microcap_index_cache"
REALTIME_DIR = CACHE_DIR / "realtime"

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

    warnings.filterwarnings("ignore", category=PerformanceWarning)
    _RUNTIME_MODULES_READY = True


def _ensure_core_deps_or_exit(args: argparse.Namespace) -> None:
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
            stale_trading_days = int((calendar > latest_trade_date).sum())
    # max_stale_days is a calendar-day safety bound. Trading-day lag remains
    # diagnostic metadata, but it must not silently widen the realtime stale
    # anchor guard across long holidays.
    is_stale = stale_value > max(0, int(max_stale_days))
    return {
        "latest_trade_date": str(latest_trade_date.date()),
        "current_date": str(current_date.date()),
        "stale_calendar_days": stale_days,
        "stale_trading_days": stale_trading_days,
        "staleness_value": stale_value,
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
        warnings.warn(
            f"panel shadow cache has future date: {shadow_day.date()} > {current_day.date()}; rebuilding",
            RuntimeWarning,
        )
        try:
            panel_shadow.unlink(missing_ok=True)
        except OSError:
            pass
        return False
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
    def normalize(value: object) -> str:
        return str(value).strip().lower().replace("_", "").replace(" ", "")

    normalized_candidates = {normalize(item) for item in candidates}
    alias_candidates = set(normalized_candidates)
    if normalized_candidates & {"rtprice", "latestprice"}:
        alias_candidates.update({"rtprice", "latestprice", "lastprice"})
    if normalized_candidates & {"preclose", "prevclose", "previousclose"}:
        alias_candidates.update({"preclose", "prevclose", "previousclose"})
    if normalized_candidates & {"quotedate", "tradedate"}:
        alias_candidates.update({"quotedate", "tradedate"})
    for column in frame.columns:
        if normalize(column) in alias_candidates:
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


def _price_cache_dir() -> Path:
    path = getattr(fetch_mod, "PRICE_CACHE_DIR", None)
    if path is None:
        path = getattr(freq_mod, "PRICE_DIR", None)
    if path is None:
        raise RuntimeError("No local raw-price cache directory is configured.")
    return Path(path)


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
        if quote_day < latest_day:
            raise ValueError(
                f"quote_trade_date {quote_day.date()} earlier than anchor {latest_day.date()}; refusing overlay"
            )
        target_ts = quote_day if latest_day < quote_day <= snapshot_day else latest_day
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


def build_realtime_signal_fast(
    context: dict[str, object],
) -> tuple[pd.DataFrame, dict[str, object], pd.DataFrame, pd.DataFrame]:
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
    return signal_df, meta, rt_close_df, rt_result


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

    signal_df, signal_meta, _, _ = build_realtime_signal_fast(context)
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
                rt_signal, meta, _, _ = build_realtime_signal_fast(context)
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


'''

_V2_RUNTIME_ARGS = parse_v2_args(sys.argv[1:]) if __name__ == "__main__" else parse_v2_args([])
_ensure_runtime_deps_or_exit(_V2_RUNTIME_ARGS)
_optional_imports()

hedge_mod, _hedge_ns = _exec_embedded_module("embedded_hedge", HEDGE_SOURCE)
cost_mod, _cost_ns = _exec_embedded_module("embedded_cost", COST_SOURCE, {"hedge_mod": hedge_mod})
fetch_mod, _fetch_ns = _exec_embedded_module("embedded_fetch", FETCH_SOURCE)
freq_mod, _freq_ns = _exec_embedded_module(
    "embedded_frequency",
    FREQ_SOURCE,
    {"hedge_mod": hedge_mod, "index_mod": fetch_mod, "cost_mod": cost_mod},
)
if hasattr(fetch_mod, "set_freq_module"):
    fetch_mod.set_freq_module(freq_mod)
    _fetch_ns["_FREQ_MOD_FOR_ST"] = freq_mod
base_mod, _base_ns = _exec_embedded_module(
    "embedded_top100_base",
    BASE_SOURCE,
    {
        "hedge_mod": hedge_mod,
        "freq_mod": freq_mod,
        "fetch_mod": fetch_mod,
        "ak": ak,
        "np": np,
        "pd": pd,
        "plt": plt,
        "requests": requests,
        "PerformanceWarning": PerformanceWarning,
    },
)

BASE_HEDGE_RATIO = 0.8
V2_BASE_OUTPUT_PREFIX = "microcap_top100_mom16_biweekly_live_v2_0_base"
V2_BASE_COSTED_NAV_CSV = OUTPUT_DIR / "microcap_top100_mom16_hedge_zz1000_0p8x_biweekly_thursday_16y_v2_0_base_costed_nav.csv"
V2_BASE_SUMMARY_JSON = OUTPUT_DIR / f"{V2_BASE_OUTPUT_PREFIX}_summary.json"
V2_BASE_COST_MODEL = "standalone_v2_embedded_mainline_base"
V2_0_MOMENTUM_GAP_EXIT_BUFFER = 0.0030
DECAY_RATIO_THRESHOLD = 0.25
DERISK_SCALE = 0.0
RECOVERY_RATIO_THRESHOLD = 0.35
TARGET_VOL_TRADING_DAYS = 244


def _base_summary_version_key() -> str:
    return f"hedge_{BASE_HEDGE_RATIO:.12g}"


def _sync_embedded_base_config() -> None:
    updates = {
        "FIXED_HEDGE_RATIO": BASE_HEDGE_RATIO,
        "DEFAULT_OUTPUT_PREFIX": V2_BASE_OUTPUT_PREFIX,
        "DEFAULT_COSTED_NAV_CSV": V2_BASE_COSTED_NAV_CSV,
        "STRATEGY_TITLE": "Top100 Microcap Mom16 Biweekly v2.0 Embedded Base",
        "hedge_mod": hedge_mod,
        "freq_mod": freq_mod,
        "fetch_mod": fetch_mod,
        "ak": ak,
        "np": np,
        "pd": pd,
        "plt": plt,
        "requests": requests,
        "PerformanceWarning": PerformanceWarning,
        "_RUNTIME_MODULES_READY": True,
    }
    for key, value in updates.items():
        _base_ns[key] = value
        setattr(base_mod, key, value)


_sync_embedded_base_config()
_ORIGINAL_BASE_BUILD_SUMMARY = base_mod.build_summary


def _standalone_base_build_summary(
    result,
    latest_signal,
    latest_rebalance,
    prev_rebalance,
    next_rebalance,
    members_df,
    changes_df,
    capital,
    anchor_freshness,
):
    summary = _ORIGINAL_BASE_BUILD_SUMMARY(
        result=result,
        latest_signal=latest_signal,
        latest_rebalance=latest_rebalance,
        prev_rebalance=prev_rebalance,
        next_rebalance=next_rebalance,
        members_df=members_df,
        changes_df=changes_df,
        capital=capital,
        anchor_freshness=anchor_freshness,
    )
    summary["version"] = "2.0_base"
    summary["version_role"] = "standalone_embedded_base"
    summary["version_note"] = "Standalone v2.0 embedded base with 0.8x hedge ratio."
    summary["summary_version_key"] = _base_summary_version_key()
    return summary


_base_ns["build_summary"] = _standalone_base_build_summary
base_mod.build_summary = _standalone_base_build_summary
_sync_embedded_base_config()


def _file_sha1(path: Path) -> str:
    if not path.exists():
        return "MISSING"
    digest = hashlib.sha1()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _build_base_args(
    max_workers: int = 8,
    capital: float | None = None,
    realtime_cache_seconds: int = 30,
    allow_stale_realtime: bool = False,
) -> argparse.Namespace:
    _sync_embedded_base_config()
    runtime_args = globals().get("_V2_RUNTIME_ARGS")
    if runtime_args is not None:
        max_workers = int(getattr(runtime_args, "max_workers", max_workers))
        capital = getattr(runtime_args, "capital", capital)
        realtime_cache_seconds = int(getattr(runtime_args, "realtime_cache_seconds", realtime_cache_seconds))
        allow_stale_realtime = bool(getattr(runtime_args, "allow_stale_realtime", allow_stale_realtime))
        panel_path = getattr(runtime_args, "panel_path", None) or base_mod.hedge_mod.DEFAULT_PANEL
        index_csv = getattr(runtime_args, "index_csv", None) or base_mod.DEFAULT_INDEX_CSV
    else:
        panel_path = base_mod.hedge_mod.DEFAULT_PANEL
        index_csv = base_mod.DEFAULT_INDEX_CSV
    return argparse.Namespace(
        query_tokens=[],
        panel_path=panel_path,
        index_csv=index_csv,
        costed_nav_csv=base_mod.DEFAULT_COSTED_NAV_CSV,
        output_prefix=base_mod.DEFAULT_OUTPUT_PREFIX,
        capital=capital,
        max_workers=max_workers,
        realtime_cache_seconds=realtime_cache_seconds,
        rebuild_index_if_missing=True,
        force_refresh=False,
        max_stale_anchor_days=base_mod.DEFAULT_MAX_STALE_ANCHOR_DAYS,
        allow_stale_realtime=allow_stale_realtime,
    )


def _proxy_meta_matches_execution_model(meta_path: Path) -> bool:
    if not meta_path.exists():
        return False
    try:
        return bool(base_mod.proxy_meta_matches_execution_model(json.loads(meta_path.read_text(encoding="utf-8"))))
    except Exception:
        return False


def _seed_proxy_bundle(paths: dict[str, Path]) -> list[Path]:
    copied: list[Path] = []
    for prefix in ("microcap_top100_mom16_biweekly_live_base_0p8", "microcap_top100_mom16_biweekly_live"):
        source_paths = base_mod.build_output_paths(prefix)
        if not _proxy_meta_matches_execution_model(source_paths["proxy_meta"]):
            continue
        if not all(source_paths[key].exists() for key in ("proxy_meta", "proxy_members", "proxy_turnover")):
            continue
        for key in ("proxy_meta", "proxy_members", "proxy_turnover"):
            src = source_paths[key]
            dst = paths[key]
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(src, dst)
            copied.append(dst)
        return copied
    return copied


def _base_costed_nav_matches_current_hedge_ratio(path: Path, hedge_ratio: float) -> bool:
    if not path.exists():
        return False
    try:
        frame = pd.read_csv(path)
    except Exception:
        return False
    required = {"holding", "microcap_ret", "hedge_ret", "futures_drag", "return_raw"}
    if required.difference(frame.columns):
        return False
    active = frame.loc[frame["holding"].astype(str) != "cash"].copy()
    if active.empty:
        return True
    for col in ("microcap_ret", "hedge_ret", "futures_drag", "return_raw"):
        active[col] = pd.to_numeric(active[col], errors="coerce")
    active = active.dropna(subset=["microcap_ret", "hedge_ret", "futures_drag", "return_raw"])
    if active.empty:
        return False
    expected = active["microcap_ret"] - float(hedge_ratio) * active["hedge_ret"] - active["futures_drag"]
    return bool((active["return_raw"] - expected).abs().le(1e-10).all())


def _ensure_base_outputs_unlocked() -> None:
    _sync_embedded_base_config()
    args = _build_base_args()
    base_paths = base_mod.build_output_paths(base_mod.DEFAULT_OUTPUT_PREFIX)
    missing_proxy = any(not base_paths[key].exists() for key in ("proxy_meta", "proxy_members", "proxy_turnover"))
    if missing_proxy:
        seeded = _seed_proxy_bundle(base_paths)
        if seeded:
            base_mod.normalize_existing_proxy_outputs(args, base_paths)
    if base_mod.DEFAULT_COSTED_NAV_CSV.exists() and not _base_costed_nav_matches_current_hedge_ratio(
        base_mod.DEFAULT_COSTED_NAV_CSV,
        hedge_ratio=BASE_HEDGE_RATIO,
    ):
        base_mod.DEFAULT_COSTED_NAV_CSV.unlink(missing_ok=True)
    missing = [
        path
        for path in (
            base_mod.DEFAULT_INDEX_CSV,
            base_mod.DEFAULT_COSTED_NAV_CSV,
            base_paths["proxy_meta"],
            base_paths["proxy_members"],
            base_paths["proxy_turnover"],
        )
        if not path.exists()
    ]
    if not missing:
        return
    resolved_panel_path, target_end_date = base_mod.build_refreshed_panel_shadow(args, base_paths)
    base_mod.ensure_strategy_files(args, base_paths, resolved_panel_path, target_end_date)


def _ensure_base_outputs() -> None:
    with _v2_base_build_lock():
        _ensure_base_outputs_unlocked()


def _read_current_reference_summary() -> dict[str, object] | None:
    if V2_BASE_SUMMARY_JSON.exists():
        try:
            summary = json.loads(V2_BASE_SUMMARY_JSON.read_text(encoding="utf-8"))
            if summary.get("summary_version_key") == _base_summary_version_key():
                return summary
        except Exception:
            pass
    return None


def _load_reference_summary_unlocked() -> dict[str, object]:
    summary = _read_current_reference_summary()
    if summary is not None:
        return summary
    _ensure_base_outputs_unlocked()
    summary = _read_current_reference_summary()
    if summary is not None:
        return summary
    args = _build_base_args()
    context = base_mod.build_base_context(args, include_members=True)
    base_mod.save_base_outputs(context)
    if not V2_BASE_SUMMARY_JSON.exists():
        raise FileNotFoundError(f"v2.0 embedded base summary was not created: {V2_BASE_SUMMARY_JSON}")
    return json.loads(V2_BASE_SUMMARY_JSON.read_text(encoding="utf-8"))


def _load_reference_summary() -> dict[str, object]:
    summary = _read_current_reference_summary()
    if summary is not None:
        return summary
    with _v2_base_build_lock():
        return _load_reference_summary_unlocked()


def current_base_fingerprint() -> dict[str, object]:
    validate_base_hedge_ratio()
    return {
        "base_version": "embedded_v2_base",
        "base_hedge_ratio": BASE_HEDGE_RATIO,
        "base_costed_nav_csv": str(base_mod.DEFAULT_COSTED_NAV_CSV),
        "base_costed_nav_sha1": _file_sha1(base_mod.DEFAULT_COSTED_NAV_CSV),
        "research_stack_version": base_mod.RESEARCH_STACK_VERSION,
        "embedded_cost_model": V2_BASE_COST_MODEL,
        "overlay_type": "momentum_gap_peak_decay_derisk_new_peak_guard",
        "momentum_gap_exit_buffer": V2_0_MOMENTUM_GAP_EXIT_BUFFER,
        "decay_ratio_threshold": DECAY_RATIO_THRESHOLD,
        "derisk_scale": DERISK_SCALE,
        "recovery_ratio_threshold": RECOVERY_RATIO_THRESHOLD,
        "trading_days": TARGET_VOL_TRADING_DAYS,
        "overlay_pre_cost_return_field": True,
    }


def validate_base_hedge_ratio() -> None:
    _sync_embedded_base_config()
    value = getattr(base_mod, "FIXED_HEDGE_RATIO", None)
    if value is None:
        raise RuntimeError("missing embedded base hedge ratio")
    if abs(float(value) - float(BASE_HEDGE_RATIO)) > 1e-9:
        raise ValueError(f"hedge ratio mismatch: v2.0={BASE_HEDGE_RATIO}, embedded_base={value}")


def _safe_bool(value: object, default: bool = False) -> bool:
    if value is None:
        return bool(default)
    try:
        if pd.isna(value):
            return bool(default)
    except (TypeError, ValueError):
        pass
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"true", "1", "yes", "y", "on"}:
            return True
        if lowered in {"false", "0", "no", "n", "off", ""}:
            return False
    return bool(value)


def _load_embedded_base_context() -> tuple[dict[str, object], pd.DataFrame, pd.DataFrame]:
    with _v2_base_build_lock():
        _ensure_base_outputs_unlocked()
        args = _build_base_args()
        base_paths = base_mod.build_output_paths(base_mod.DEFAULT_OUTPUT_PREFIX)
        panel_path, target_end_date = base_mod.refresh_history_anchor(args, base_paths)
        index_end_date = base_mod.read_csv_last_date(args.index_csv)
        if index_end_date is not None:
            target_end_date = min(pd.Timestamp(target_end_date), pd.Timestamp(index_end_date))
        costed_end_date = base_mod.read_csv_last_date(base_mod.DEFAULT_COSTED_NAV_CSV)
        if costed_end_date is None or pd.Timestamp(costed_end_date).normalize() < pd.Timestamp(target_end_date).normalize():
            base_mod.ensure_strategy_nav_fresh(args, base_paths, panel_path, target_end_date)
        close_df = base_mod.load_close_df(panel_path, args.index_csv, max_date=target_end_date)
        gross = base_mod.run_signal(close_df).sort_index()
        turnover_df = pd.read_csv(base_paths["proxy_turnover"])
        if "rebalance_date" not in turnover_df.columns:
            raise KeyError(f"Column 'rebalance_date' not found in {base_paths['proxy_turnover']}.")
        turnover_df["rebalance_date"] = pd.to_datetime(turnover_df["rebalance_date"], errors="coerce")
        turnover_df = turnover_df.dropna(subset=["rebalance_date"]).sort_values("rebalance_date")
        reference_summary = _load_reference_summary_unlocked()
    return reference_summary, gross, turnover_df


def _load_realtime_embedded_base_context() -> tuple[dict[str, object], pd.DataFrame, dict[str, object]]:
    with _v2_base_build_lock():
        _ensure_base_outputs_unlocked()
        args = _build_base_args()
        base_paths = base_mod.build_output_paths(base_mod.DEFAULT_OUTPUT_PREFIX)
        panel_path, target_end_date = base_mod.refresh_history_anchor(args, base_paths)
        try:
            base_context = base_mod.ensure_realtime_query_base_context(args, base_paths, panel_path, target_end_date)
        except (FileNotFoundError, ValueError):
            base_context = base_mod.ensure_base_signal_fresh(args, base_paths, panel_path, target_end_date)
        member_context = base_mod.ensure_static_members_fresh(args, base_paths, panel_path, target_end_date, base_context)
        turnover_df = pd.read_csv(base_paths["proxy_turnover"])
        turnover_df["rebalance_date"] = pd.to_datetime(turnover_df["rebalance_date"], errors="coerce")
        turnover_df = turnover_df.dropna(subset=["rebalance_date"]).sort_values("rebalance_date")
        reference_summary = _load_reference_summary_unlocked()
    return member_context, turnover_df, reference_summary


@dataclass(frozen=True)
class RealtimeBase:
    context: dict[str, object]
    turnover_df: pd.DataFrame
    reference_summary: dict[str, object]
    meta: dict[str, object]
    realtime_close_df: pd.DataFrame
    base_gross: pd.DataFrame


def csv_safe_meta_value(value: object) -> object:
    if isinstance(value, (dict, list, tuple)):
        return json.dumps(value, ensure_ascii=False, default=str)
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return value


def apply_realtime_meta_to_signal_row(signal_row: pd.DataFrame, meta: dict[str, object]) -> None:
    for key, value in meta.items():
        signal_row[key] = csv_safe_meta_value(value)


def realtime_state_required() -> bool:
    value = os.environ.get("TOP100_REALTIME_REQUIRE_STATE", "")
    return value.strip().lower() in {"1", "true", "yes", "on"}


def load_realtime_context() -> tuple[dict[str, object], pd.DataFrame, dict[str, object]]:
    with _v2_base_build_lock():
        _ensure_base_outputs_unlocked()
        args = _build_base_args()
        base_paths = base_mod.build_output_paths(base_mod.DEFAULT_OUTPUT_PREFIX)
        panel_path, target_end_date = base_mod.refresh_history_anchor(args, base_paths)
        if realtime_state_required():
            base_context = base_mod.build_realtime_context_from_cached_proxy(
                args,
                base_paths,
                panel_path,
                target_end_date,
                "production state-only mode avoids implicit cache rebuilds",
            )
            if base_context is None:
                raise RuntimeError("Validated realtime state is not reusable for production; refusing implicit proxy/cache rebuild.")
        else:
            try:
                base_context = base_mod.ensure_realtime_query_base_context(args, base_paths, panel_path, target_end_date)
            except (FileNotFoundError, ValueError):
                base_context = base_mod.ensure_base_signal_fresh(args, base_paths, panel_path, target_end_date)
        member_context = base_mod.ensure_static_members_fresh(args, base_paths, panel_path, target_end_date, base_context)
        turnover_df = pd.read_csv(base_paths["proxy_turnover"])
        turnover_df["rebalance_date"] = pd.to_datetime(turnover_df["rebalance_date"], errors="coerce")
        turnover_df = turnover_df.dropna(subset=["rebalance_date"]).sort_values("rebalance_date")
        reference_summary = _load_reference_summary_unlocked()
    return member_context, turnover_df, reference_summary


def load_realtime_base() -> RealtimeBase:
    context, turnover_df, reference_summary = load_realtime_context()
    _, meta, realtime_close_df, base_gross = base_mod.build_realtime_signal_fast(context)
    return RealtimeBase(context, turnover_df, reference_summary, meta, realtime_close_df, base_gross)


def build_realtime_overlay_base(realtime_base: RealtimeBase) -> pd.DataFrame:
    gross = base_mod.apply_momentum_gap_exit_buffer(realtime_base.base_gross, V2_0_MOMENTUM_GAP_EXIT_BUFFER)
    out = base_mod.apply_momentum_gap_peak_decay_derisk(
        gross_result=gross,
        turnover_df=realtime_base.turnover_df,
        decay_ratio_threshold=DECAY_RATIO_THRESHOLD,
        derisk_scale=DERISK_SCALE,
        recovery_ratio_threshold=RECOVERY_RATIO_THRESHOLD,
    )
    return base_mod.ensure_overlay_pre_cost_return(out)


embedded_base_adapter = SimpleNamespace(
    BASE_HEDGE_RATIO=BASE_HEDGE_RATIO,
    V2_0_MOMENTUM_GAP_EXIT_BUFFER=V2_0_MOMENTUM_GAP_EXIT_BUFFER,
    DECAY_RATIO_THRESHOLD=DECAY_RATIO_THRESHOLD,
    DERISK_SCALE=DERISK_SCALE,
    RECOVERY_RATIO_THRESHOLD=RECOVERY_RATIO_THRESHOLD,
    TARGET_VOL_TRADING_DAYS=TARGET_VOL_TRADING_DAYS,
    base_mod=base_mod,
    current_base_fingerprint=current_base_fingerprint,
    _load_embedded_base_context=_load_embedded_base_context,
    _load_realtime_embedded_base_context=_load_realtime_embedded_base_context,
    _load_reference_summary=_load_reference_summary,
)
embedded_context = embedded_base_adapter
realtime_core = SimpleNamespace(
    base_mod=base_mod,
    apply_realtime_meta_to_signal_row=apply_realtime_meta_to_signal_row,
    load_realtime_base=load_realtime_base,
    build_realtime_overlay_base=build_realtime_overlay_base,
)



OVERLAY_SOURCE = r'''
ROOT = Path(__file__).resolve().parent
OUTPUT_DIR = ROOT / "outputs"

OUTPUT_PREFIX = "microcap_top100_mom16_biweekly_live_v2_0"
TARGET_VOL = 0.25
TARGET_VOL_WINDOW = 60
TARGET_VOL_MAX_LEVERAGE = 1.5
TARGET_VOL_MIN_LEVERAGE = 0.0
TARGET_VOL_TRADING_DAYS = 244
TARGET_VOL_SCALE_CHANGE_COST = 0.001
TARGET_VOL_SCALE_REBALANCE_THRESHOLD = 0.10
TARGET_VOL_FINANCING_RATE = 0.03
IDLE_CASH_YIELD = 0.02
SCALE_TRADE_REQUIRED_EPSILON = 1e-6
HISTORICAL_TARGET_VOL_VALUES_TO_CLEANUP = (0.15,)
DEFAULT_ALLOWED_TAIL_ROWS = max(int(getattr(embedded_context.base_mod, "LOOKBACK", 16)) + 20, 40)


def _costed_nav_path(target_vol: float) -> Path:
    return OUTPUT_DIR / f"microcap_top100_mom16_targetvol{int(round(float(target_vol) * 100))}_max1p5_v2_0_costed_nav.csv"


SUMMARY_JSON = OUTPUT_DIR / f"{OUTPUT_PREFIX}_summary.json"
LATEST_SIGNAL_CSV = OUTPUT_DIR / f"{OUTPUT_PREFIX}_latest_signal.csv"
REALTIME_SIGNAL_CSV = OUTPUT_DIR / f"{OUTPUT_PREFIX}_realtime_signal.csv"
NAV_CSV = OUTPUT_DIR / f"{OUTPUT_PREFIX}_nav.csv"
COSTED_NAV_CSV = _costed_nav_path(TARGET_VOL)
LEGACY_COSTED_NAV_CSVS = [_costed_nav_path(value) for value in HISTORICAL_TARGET_VOL_VALUES_TO_CLEANUP]


def _first_legacy_costed_nav_alias(paths: list[Path]) -> Path | None:
    return paths[0] if paths else None


LEGACY_COSTED_NAV_CSV = _first_legacy_costed_nav_alias(LEGACY_COSTED_NAV_CSVS)
PERF_SUMMARY_CSV = OUTPUT_DIR / f"{OUTPUT_PREFIX}_performance_summary.csv"
PERF_YEARLY_CSV = OUTPUT_DIR / f"{OUTPUT_PREFIX}_performance_yearly.csv"
PERF_NAV_CSV = OUTPUT_DIR / f"{OUTPUT_PREFIX}_performance_nav.csv"
PERF_JSON = OUTPUT_DIR / f"{OUTPUT_PREFIX}_performance_summary.json"
PERF_PNG = OUTPUT_DIR / f"{OUTPUT_PREFIX}_performance_curve.png"
PERF_QUERY_SUMMARY_CSV = OUTPUT_DIR / f"{OUTPUT_PREFIX}_performance_query_summary.csv"
PERF_QUERY_YEARLY_CSV = OUTPUT_DIR / f"{OUTPUT_PREFIX}_performance_query_yearly.csv"
PERF_QUERY_NAV_CSV = OUTPUT_DIR / f"{OUTPUT_PREFIX}_performance_query_nav.csv"
PERF_QUERY_JSON = OUTPUT_DIR / f"{OUTPUT_PREFIX}_performance_query_summary.json"
PERF_QUERY_PNG = OUTPUT_DIR / f"{OUTPUT_PREFIX}_performance_query_curve.png"

EXPECTED_VERSION_ROLE = "standalone_target_vol_overlay"
EXPECTED_VERSION_NOTE_PREFIX = "Standalone target-volatility overlay matching repaired v1.6 behavior."
BASE_HEDGE_RATIO_SOURCE = embedded_context
V2_0_MOMENTUM_GAP_EXIT_BUFFER_SOURCE = embedded_context
DECAY_RATIO_THRESHOLD_SOURCE = embedded_context
DERISK_SCALE_SOURCE = embedded_context
RECOVERY_RATIO_THRESHOLD_SOURCE = embedded_context
TARGET_VOL_TRADING_DAYS_SOURCE = embedded_context
BASE_HEDGE_RATIO = float(BASE_HEDGE_RATIO_SOURCE.BASE_HEDGE_RATIO)
V2_0_MOMENTUM_GAP_EXIT_BUFFER = float(V2_0_MOMENTUM_GAP_EXIT_BUFFER_SOURCE.V2_0_MOMENTUM_GAP_EXIT_BUFFER)
DECAY_RATIO_THRESHOLD = float(DECAY_RATIO_THRESHOLD_SOURCE.DECAY_RATIO_THRESHOLD)
DERISK_SCALE = float(DERISK_SCALE_SOURCE.DERISK_SCALE)
RECOVERY_RATIO_THRESHOLD = float(RECOVERY_RATIO_THRESHOLD_SOURCE.RECOVERY_RATIO_THRESHOLD)
TARGET_VOL_TRADING_DAYS = int(TARGET_VOL_TRADING_DAYS_SOURCE.TARGET_VOL_TRADING_DAYS)
PNL_RETURN_SOURCE = "embedded_lineage_overlay_pre_cost_return_explicit_or_return_net_cost_reversal_fallback"
ALLOWED_HOLDINGS = {"cash", "long_microcap_short_zz1000"}
VOLATILITY_RETURN_SOURCE_PRIORITY = [
    "constructed_microcap_minus_hedge",
    "return_raw",
    "base_gross_return",
    "return_net_fallback_warning",
]
MEMBER_REBALANCE_META_COLS = {
    "member_rebalance_state",
    "member_rebalance_required",
    "member_enter_count",
    "member_exit_count",
    "member_rebalance_label",
}
REALTIME_META_FORCE_COLS = {
    "quote_source",
    "hedge_quote_source",
    "member_price_count",
    "member_count",
    "latest_anchor_trade_date",
    "quote_trade_date",
    "snapshot_time",
    "tail_jitter_risk",
    "tail_jitter_note",
}


def validate_base_hedge_ratio() -> None:
    embedded_base_mod = getattr(embedded_context, "base_mod", None)
    if embedded_base_mod is None:
        raise RuntimeError("missing embedded base module; cannot validate v2.0 base hedge ratio")
    checks = {
        "embedded_base_adapter.BASE_HEDGE_RATIO": getattr(embedded_context, "BASE_HEDGE_RATIO", None),
        "embedded_base_mod.FIXED_HEDGE_RATIO": getattr(embedded_base_mod, "FIXED_HEDGE_RATIO", None),
    }
    for name, value in checks.items():
        if value is None:
            raise RuntimeError(f"missing {name}; cannot validate v2.0 base hedge ratio")
        if abs(float(value) - float(BASE_HEDGE_RATIO)) > 1e-9:
            raise ValueError(f"hedge ratio mismatch: v2.0={BASE_HEDGE_RATIO}, {name}={value}")


def _to_jsonable(value: object) -> object:
    if isinstance(value, np.bool_):
        return bool(value)
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, np.floating):
        return float(value) if np.isfinite(value) else None
    if isinstance(value, np.ndarray):
        return value.tolist()
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if value is pd.NaT:
        return None
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def _json_sanitize(value: object) -> object:
    if isinstance(value, dict):
        return {key: _json_sanitize(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_sanitize(item) for item in value]
    if isinstance(value, tuple):
        return [_json_sanitize(item) for item in value]
    if isinstance(value, np.ndarray):
        return [_json_sanitize(item) for item in value.tolist()]
    if isinstance(value, np.bool_):
        return bool(value)
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, np.floating):
        return float(value) if np.isfinite(value) else None
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if value is pd.NaT:
        return None
    return value


def _json_dumps(payload: object) -> str:
    return json.dumps(_json_sanitize(payload), ensure_ascii=False, indent=2, allow_nan=False, default=_to_jsonable)


def _safe_float(value: object, default: float = 0.0) -> float:
    if value is None:
        return float(default)
    try:
        if pd.isna(value):
            return float(default)
    except (TypeError, ValueError):
        pass
    return float(value)


def _safe_bool(value: object, default: bool = False) -> bool:
    if value is None:
        return bool(default)
    try:
        if pd.isna(value):
            return bool(default)
    except (TypeError, ValueError):
        pass
    if isinstance(value, str):
        text = value.strip().lower()
        if text in {"true", "1", "yes", "y", "on"}:
            return True
        if text in {"false", "0", "no", "n", "off", ""}:
            return False
    return bool(value)


def _atomic_temp_path(path: Path) -> Path:
    return path.with_suffix(path.suffix + f".tmp.{os.getpid()}.{time.time_ns()}")


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


def _atomic_write_csv(frame: pd.DataFrame, path: Path, **kwargs: object) -> None:
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


def _csv_safe_value(value: object) -> object:
    if isinstance(value, (dict, list, tuple)):
        return _json_dumps(value)
    if isinstance(value, np.bool_):
        return bool(value)
    if isinstance(value, np.datetime64):
        return pd.Timestamp(value).isoformat()
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, np.floating):
        return float(value) if np.isfinite(value) else ""
    return value


def _apply_realtime_meta_to_signal_row(signal_row: pd.DataFrame, meta: dict[str, object]) -> None:
    for key, value in meta.items():
        signal_row[key] = _csv_safe_value(value)


def _apply_selected_realtime_meta_to_signal_row(
    signal_row: pd.DataFrame,
    meta: dict[str, object],
    passthrough_cols: list[str],
) -> None:
    for col in passthrough_cols:
        if col in meta and col not in signal_row.columns:
            signal_row[col] = _csv_safe_value(meta[col])


def _apply_realtime_meta_columns_to_signal_row(signal_row: pd.DataFrame, meta: dict[str, object]) -> None:
    for col in MEMBER_REBALANCE_META_COLS:
        if col in meta and col not in signal_row.columns:
            signal_row[col] = _csv_safe_value(meta[col])
    for col in REALTIME_META_FORCE_COLS:
        if col in meta:
            signal_row[col] = _csv_safe_value(meta[col])


def _normalize_holding_series(
    value: pd.Series | object,
    index: pd.Index,
    *,
    fill_from: pd.Series | None = None,
    default: str = "cash",
) -> pd.Series:
    if isinstance(value, pd.Series):
        series = value.reindex(index)
    else:
        series = pd.Series(value, index=index)
    if fill_from is not None:
        series = series.where(series.notna(), fill_from.reindex(index))
    series = series.fillna(default).astype(str)
    bad = sorted(set(series.dropna().unique()).difference(ALLOWED_HOLDINGS))
    if bad:
        raise ValueError(f"unexpected holding labels: {bad}")
    return series


def _read_costed_nav_csv(path: Path = COSTED_NAV_CSV, **kwargs: object) -> pd.DataFrame:
    return pd.read_csv(path, encoding="utf-8-sig", **kwargs)


def current_base_fingerprint() -> dict[str, object]:
    validate_base_hedge_ratio()
    base = dict(embedded_context.current_base_fingerprint())
    base["momentum_gap_exit_buffer"] = V2_0_MOMENTUM_GAP_EXIT_BUFFER
    return {
        "base_version": "embedded_v2_base",
        "embedded_lineage_base_fingerprint": base,
        "overlay_type": "target_volatility_scaling",
        "base_hedge_ratio": BASE_HEDGE_RATIO,
        "target_vol": TARGET_VOL,
        "vol_window": TARGET_VOL_WINDOW,
        "max_leverage": TARGET_VOL_MAX_LEVERAGE,
        "min_leverage": TARGET_VOL_MIN_LEVERAGE,
        "trading_days": TARGET_VOL_TRADING_DAYS,
        "scale_change_cost": TARGET_VOL_SCALE_CHANGE_COST,
        "scale_change_cost_model": "microcap_long_plus_hedge_leg_net_turnover",
        "scale_rebalance_threshold": TARGET_VOL_SCALE_REBALANCE_THRESHOLD,
        "scale_trade_required_epsilon": SCALE_TRADE_REQUIRED_EPSILON,
        "base_trade_cost_model": "embedded_lineage_total_cost_scaled_by_target_vol_exposure",
        "volatility_return_source_priority": VOLATILITY_RETURN_SOURCE_PRIORITY,
        "target_vol_return_nan_policy": "preserve_nan_before_rolling_vol_fill_output_only",
        "pnl_return_source": PNL_RETURN_SOURCE,
        "financing_rate": TARGET_VOL_FINANCING_RATE,
        "idle_cash_yield": IDLE_CASH_YIELD,
        "momentum_gap_exit_buffer": V2_0_MOMENTUM_GAP_EXIT_BUFFER,
        "decay_ratio_threshold": DECAY_RATIO_THRESHOLD,
        "derisk_scale": DERISK_SCALE,
        "recovery_ratio_threshold": RECOVERY_RATIO_THRESHOLD,
    }


def summary_matches_current_v2_0_base(summary: dict[str, object]) -> bool:
    if not isinstance(summary, dict):
        return False
    if str(summary.get("version")) != "2.0":
        return False
    if str(summary.get("version_role")) != EXPECTED_VERSION_ROLE:
        return False
    if not str(summary.get("version_note", "")).startswith(EXPECTED_VERSION_NOTE_PREFIX):
        return False
    return summary.get("base_fingerprint") == current_base_fingerprint()


def invalidate_incompatible_v2_0_outputs() -> list[Path]:
    stale = incompatible_v2_0_outputs()
    removed: list[Path] = []
    for path in stale:
        if path.exists():
            path.unlink(missing_ok=True)
            removed.append(path)
    return removed


def incompatible_v2_0_outputs() -> list[Path]:
    all_outputs = [
        SUMMARY_JSON,
        LATEST_SIGNAL_CSV,
        REALTIME_SIGNAL_CSV,
        NAV_CSV,
        COSTED_NAV_CSV,
        *LEGACY_COSTED_NAV_CSVS,
        PERF_SUMMARY_CSV,
        PERF_YEARLY_CSV,
        PERF_NAV_CSV,
        PERF_JSON,
        PERF_PNG,
        PERF_QUERY_SUMMARY_CSV,
        PERF_QUERY_YEARLY_CSV,
        PERF_QUERY_NAV_CSV,
        PERF_QUERY_JSON,
        PERF_QUERY_PNG,
    ]
    if not SUMMARY_JSON.exists():
        return [path for path in all_outputs if path.exists()]
    try:
        summary = json.loads(SUMMARY_JSON.read_text(encoding="utf-8"))
    except Exception:
        summary = None
    if summary_matches_current_v2_0_base(summary):
        return []
    return all_outputs


def _stale_outputs_to_remove_after_generate(stale_outputs: list[Path], regenerated_outputs: set[Path]) -> list[Path]:
    preserved = set(regenerated_outputs)
    # Close-confirmed generation does not own the realtime signal artifact; the
    # realtime route refreshes it atomically when queried.
    preserved.add(REALTIME_SIGNAL_CSV)
    return [path for path in stale_outputs if path not in preserved]


def ensure_output_dir() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def target_vol_legs_for_state(
    holding: str,
    scale: float,
    hedge_ratio: float = BASE_HEDGE_RATIO,
) -> dict[str, float]:
    if str(holding) == "cash" or not np.isfinite(scale) or scale <= 1e-12:
        return {}
    return {
        "microcap_top100": float(scale),
        "hedge_zz1000": -float(hedge_ratio) * float(scale),
    }


def calc_target_vol_turnover(
    prev_holding: str,
    prev_scale: float,
    next_holding: str,
    next_scale: float,
    hedge_ratio: float = BASE_HEDGE_RATIO,
) -> float:
    old_legs = target_vol_legs_for_state(prev_holding, prev_scale, hedge_ratio=hedge_ratio)
    new_legs = target_vol_legs_for_state(next_holding, next_scale, hedge_ratio=hedge_ratio)
    return float(sum(abs(new_legs.get(k, 0.0) - old_legs.get(k, 0.0)) for k in set(old_legs) | set(new_legs)))


def _target_vol_turnover_series(holding: pd.Series, execution_scale: pd.Series) -> pd.Series:
    holding = _normalize_holding_series(holding, holding.index)
    prev_holding = holding.shift(1).fillna("cash")
    prev_scale = execution_scale.shift(1).fillna(0.0)
    values = [
        calc_target_vol_turnover(old_holding, old_scale, new_holding, new_scale)
        for old_holding, old_scale, new_holding, new_scale in zip(
            prev_holding,
            prev_scale,
            holding,
            execution_scale.fillna(0.0),
        )
    ]
    return pd.Series(values, index=holding.index, dtype=float)


def calc_scale_change_cost(holding: pd.Series, target_vol_turnover: pd.Series) -> pd.Series:
    """Charge only same-holding scale changes; transition days are zeroed inside calc_target_vol_costed_turnover."""
    return calc_target_vol_costed_turnover(holding, target_vol_turnover) * TARGET_VOL_SCALE_CHANGE_COST


def calc_target_vol_costed_turnover(holding: pd.Series, target_vol_turnover: pd.Series) -> pd.Series:
    holding = _normalize_holding_series(holding, holding.index)
    same_holding = holding.eq(holding.shift(1))
    return pd.to_numeric(target_vol_turnover, errors="coerce").fillna(0.0).where(same_holding, 0.0)


def _scale_from_realized_vol(realized_vol: pd.Series) -> pd.Series:
    scale = TARGET_VOL / realized_vol.replace(0.0, np.nan)
    return scale.replace([np.inf, -np.inf], np.nan).clip(
        lower=TARGET_VOL_MIN_LEVERAGE,
        upper=TARGET_VOL_MAX_LEVERAGE,
    )


def apply_scale_rebalance_threshold(
    desired_scale: pd.Series,
    active: pd.Series,
    threshold: float = TARGET_VOL_SCALE_REBALANCE_THRESHOLD,
) -> pd.Series:
    if not desired_scale.index.is_unique:
        raise ValueError("desired_scale must have a unique index")
    if not active.index.is_unique:
        raise ValueError("active must have a unique index")
    desired = pd.to_numeric(desired_scale, errors="coerce").fillna(1.0)
    active_flags = active.reindex(desired.index).fillna(False).astype(bool)
    values: list[float] = []
    last_scale = 0.0
    for dt, target in desired.items():
        if not bool(active_flags.loc[dt]):
            values.append(0.0)
            last_scale = 0.0
            continue
        target = float(target)
        if last_scale <= 1e-12 or abs(target - last_scale) >= float(threshold):
            last_scale = target
        values.append(float(last_scale))
    return pd.Series(values, index=desired.index, dtype=float)


def calc_next_session_actionable_scale(
    current_execution_scale: pd.Series,
    next_session_target_scale: pd.Series,
    next_holding: pd.Series,
    threshold: float = TARGET_VOL_SCALE_REBALANCE_THRESHOLD,
) -> pd.Series:
    current = pd.to_numeric(current_execution_scale, errors="coerce").fillna(0.0)
    target = pd.to_numeric(next_session_target_scale, errors="coerce").fillna(current)
    next_holding = _normalize_holding_series(next_holding, current.index)
    actionable = current.copy()
    to_cash = next_holding.eq("cash")
    enter_from_cash = current.le(1e-12) & next_holding.ne("cash") & target.gt(1e-12)
    rebalance = target.sub(current).abs().ge(float(threshold))
    actionable.loc[to_cash] = 0.0
    actionable.loc[~to_cash & (enter_from_cash | rebalance)] = target.loc[~to_cash & (enter_from_cash | rebalance)]
    return actionable.astype(float)


def calc_next_session_actionable_scale_value(
    current_execution_scale: float,
    next_session_target_scale: float,
    next_holding: str,
    threshold: float = TARGET_VOL_SCALE_REBALANCE_THRESHOLD,
) -> float:
    current = 0.0 if pd.isna(current_execution_scale) else float(current_execution_scale)
    target = current if pd.isna(next_session_target_scale) else float(next_session_target_scale)
    if str(next_holding) == "cash":
        return 0.0
    if current <= 1e-12 and target > 1e-12:
        return target
    if abs(target - current) >= float(threshold):
        return target
    return current


def calc_base_trade_cost_scale(
    holding: pd.Series,
    next_holding: pd.Series,
    current_execution_scale: pd.Series,
    next_session_actionable_scale: pd.Series,
) -> pd.Series:
    holding = _normalize_holding_series(holding, holding.index)
    next_holding = _normalize_holding_series(next_holding, holding.index, fill_from=holding)
    current = pd.to_numeric(current_execution_scale, errors="coerce").fillna(0.0)
    actionable = pd.to_numeric(next_session_actionable_scale, errors="coerce").fillna(current)
    scale = pd.Series(0.0, index=holding.index, dtype=float)
    current_active = holding.ne("cash")
    next_active = next_holding.ne("cash")
    scale.loc[~current_active & next_active] = actionable.loc[~current_active & next_active]
    scale.loc[current_active] = current.loc[current_active]
    return scale.clip(lower=0.0)


def _select_target_vol_return_source(out: pd.DataFrame, fallback: pd.Series) -> tuple[pd.Series, str]:
    if {"microcap_close", "hedge_close"}.issubset(out.columns):
        micro_close = pd.to_numeric(out["microcap_close"], errors="coerce")
        hedge_close = pd.to_numeric(out["hedge_close"], errors="coerce")
        micro = micro_close.pct_change(fill_method=None)
        hedge = hedge_close.pct_change(fill_method=None)
        spread = (micro - float(BASE_HEDGE_RATIO) * hedge).replace([np.inf, -np.inf], np.nan)
        if spread.notna().sum() >= TARGET_VOL_WINDOW:
            return spread, "constructed_microcap_minus_hedge"
    for col in ["return_raw", "base_gross_return"]:
        if col in out.columns:
            series = pd.to_numeric(out[col], errors="coerce").replace([np.inf, -np.inf], np.nan)
            if series.notna().any():
                return series, col
    return fallback.replace([np.inf, -np.inf], np.nan), "return_net_fallback_warning"


def _select_base_pre_cost_return(out: pd.DataFrame, base_return_net: pd.Series, base_trade_cost: pd.Series) -> tuple[pd.Series, str]:
    if "overlay_pre_cost_return" in out.columns:
        series = pd.to_numeric(out["overlay_pre_cost_return"], errors="coerce")
        if series.notna().any():
            return series.fillna(0.0), "overlay_pre_cost_return"
    safe_cost = base_trade_cost.clip(lower=0.0, upper=0.99)
    return (1.0 + base_return_net).div(1.0 - safe_cost).sub(1.0), "return_net_cost_reversal"


def apply_target_vol_scaling(base_result: pd.DataFrame, treat_last_row_as_snapshot: bool = False) -> pd.DataFrame:
    validate_base_hedge_ratio()
    out = base_result.copy().sort_index()
    base_return_net = pd.to_numeric(out["return_net"], errors="coerce").fillna(0.0)
    target_vol_return, target_vol_return_source = _select_target_vol_return_source(out, base_return_net)
    holding = _normalize_holding_series(out["holding"], out.index)
    next_holding = _normalize_holding_series(out["next_holding"], out.index, fill_from=holding) if "next_holding" in out.columns else holding.copy()
    out["holding"] = holding
    out["next_holding"] = next_holding
    active = holding.ne("cash")
    realized_vol = (
        target_vol_return.rolling(TARGET_VOL_WINDOW, min_periods=TARGET_VOL_WINDOW).std(ddof=1)
        * np.sqrt(TARGET_VOL_TRADING_DAYS)
    )
    realtime_snapshot_vol_frozen = pd.Series(False, index=out.index, dtype=bool)
    realtime_snapshot_vol_frozen_lag_days = pd.Series(0, index=out.index, dtype=int)
    realtime_snapshot_vol_frozen_source_date = pd.Series(pd.NA, index=out.index, dtype="string")
    if treat_last_row_as_snapshot and len(realized_vol) >= 2:
        previous_realized_vol = realized_vol.iloc[:-1].dropna()
        if not previous_realized_vol.empty:
            last_valid_idx = pd.Timestamp(previous_realized_vol.index[-1])
            lag_days = max(0, int((pd.Timestamp(realized_vol.index[-1]) - last_valid_idx).days))
            realized_vol.iloc[-1] = previous_realized_vol.iloc[-1]
            realtime_snapshot_vol_frozen.iloc[-1] = True
            realtime_snapshot_vol_frozen_lag_days.iloc[-1] = lag_days
            realtime_snapshot_vol_frozen_source_date.iloc[-1] = str(last_valid_idx.date())
            if lag_days > 3:
                warnings.warn(
                    f"target_vol snapshot using realized vol from {lag_days} calendar days ago",
                    RuntimeWarning,
                )
    scale_from_realized_vol = _scale_from_realized_vol(realized_vol)
    target_execution_scale = scale_from_realized_vol.shift(1).fillna(1.0)
    execution_scale = apply_scale_rebalance_threshold(
        target_execution_scale,
        active,
        threshold=TARGET_VOL_SCALE_REBALANCE_THRESHOLD,
    )
    next_session_target_scale = scale_from_realized_vol.copy()
    next_session_target_scale.loc[next_holding.eq("cash")] = 0.0
    next_session_target_scale.loc[next_holding.ne("cash")] = next_session_target_scale.loc[next_holding.ne("cash")].fillna(1.0)
    next_session_target_scale = next_session_target_scale.fillna(0.0)
    next_session_actionable_scale = calc_next_session_actionable_scale(
        execution_scale,
        next_session_target_scale,
        next_holding,
        threshold=TARGET_VOL_SCALE_REBALANCE_THRESHOLD,
    )
    target_vol_turnover = _target_vol_turnover_series(holding, execution_scale)
    scale_change_cost = calc_scale_change_cost(holding, target_vol_turnover)
    financing_cost = execution_scale.sub(1.0).clip(lower=0.0) * TARGET_VOL_FINANCING_RATE / TARGET_VOL_TRADING_DAYS
    idle_cash_yield = (
        active.astype(float)
        * execution_scale.rsub(1.0).clip(lower=0.0, upper=1.0)
        * IDLE_CASH_YIELD
        / TARGET_VOL_TRADING_DAYS
    )
    base_trade_cost = pd.to_numeric(out.get("total_cost", pd.Series(0.0, index=out.index)), errors="coerce").fillna(0.0)
    base_pre_cost_return, base_pre_cost_return_source = _select_base_pre_cost_return(out, base_return_net, base_trade_cost)
    base_trade_cost_scale = calc_base_trade_cost_scale(
        holding,
        next_holding,
        execution_scale,
        next_session_actionable_scale,
    )
    base_trade_cost_scaled = (base_trade_cost * base_trade_cost_scale).clip(lower=0.0, upper=0.99)
    ret = (
        (1.0 + base_pre_cost_return * execution_scale + idle_cash_yield)
        * (1.0 - base_trade_cost_scaled)
        * (1.0 - scale_change_cost)
        * (1.0 - financing_cost)
        - 1.0
    )

    out["target_vol"] = TARGET_VOL
    out["target_vol_window"] = TARGET_VOL_WINDOW
    out["target_vol_return"] = target_vol_return.fillna(0.0)
    out["target_vol_return_source"] = target_vol_return_source
    out["target_vol_realized_vol"] = realized_vol
    out["target_vol_realtime_snapshot_vol_frozen"] = realtime_snapshot_vol_frozen
    out["target_vol_frozen_lag_days"] = realtime_snapshot_vol_frozen_lag_days
    out["target_vol_frozen_source_date"] = realtime_snapshot_vol_frozen_source_date
    out["latest_realized_vol"] = realized_vol
    out["target_vol_scale_raw"] = scale_from_realized_vol
    out["target_vol_execution_scale_raw"] = target_execution_scale
    out["current_execution_scale"] = execution_scale
    out["next_session_target_scale"] = next_session_target_scale
    out["raw_next_target_scale"] = next_session_target_scale
    out["next_session_actionable_scale"] = next_session_actionable_scale
    out["target_vol_scale_next_session"] = next_session_actionable_scale
    out["execution_scale"] = execution_scale
    out["target_vol_turnover"] = target_vol_turnover
    out["target_vol_costed_turnover"] = calc_target_vol_costed_turnover(holding, target_vol_turnover)
    out["scale_change_cost"] = scale_change_cost
    out["target_vol_trade_cost"] = scale_change_cost
    out["financing_cost"] = financing_cost
    out["idle_cash_yield"] = idle_cash_yield
    out["base_trade_cost"] = base_trade_cost
    out["base_trade_cost_scale"] = base_trade_cost_scale
    out["base_trade_cost_scaled"] = base_trade_cost_scaled
    out["base_pre_cost_return"] = base_pre_cost_return
    out["base_pre_cost_return_source"] = base_pre_cost_return_source
    out["embedded_lineage_return_net"] = base_return_net
    out["embedded_lineage_nav_net"] = pd.to_numeric(out.get("nav_net", pd.Series(np.nan, index=out.index)), errors="coerce")
    out["return_net"] = ret
    out["nav_net"] = (1.0 + out["return_net"].fillna(0.0)).cumprod()
    out["return"] = out["return_net"]
    out["nav"] = out["nav_net"]
    out["version"] = "2.0"
    out["base_version"] = "embedded_v2_base"
    out["overlay_type"] = "target_volatility_scaling"
    return out


def _build_signal_row(net_df: pd.DataFrame, reference_summary: dict[str, object]) -> pd.DataFrame:
    latest_row = net_df.iloc[-1]
    latest_signal = dict(reference_summary.get("latest_signal", {}))
    current_holding = _normalize_holding_series(
        pd.Series([latest_row.get("holding", latest_signal.get("current_holding", "cash"))]),
        pd.Index([0]),
    ).iloc[0]
    next_holding = _normalize_holding_series(
        pd.Series([latest_row.get("next_holding", latest_signal.get("next_holding", current_holding))]),
        pd.Index([0]),
        fill_from=pd.Series([current_holding], index=pd.Index([0])),
    ).iloc[0]
    holding_trade_state = embedded_context.base_mod.compute_trade_state(current_holding, next_holding)
    current_execution_scale = _safe_float(
        latest_row.get("current_execution_scale", latest_row.get("execution_scale")),
        0.0,
    )
    next_session_target_scale = _safe_float(
        latest_row.get(
            "next_session_target_scale",
            latest_row.get("target_vol_scale_next_session", current_execution_scale),
        ),
        current_execution_scale,
    )
    raw_next_session_actionable_scale = latest_row.get("next_session_actionable_scale", np.nan)
    if pd.notna(raw_next_session_actionable_scale):
        next_session_actionable_scale = float(raw_next_session_actionable_scale)
    else:
        next_session_actionable_scale = calc_next_session_actionable_scale_value(
            current_execution_scale,
            next_session_target_scale,
            next_holding,
            threshold=TARGET_VOL_SCALE_REBALANCE_THRESHOLD,
        )
    raw_scale_delta = next_session_target_scale - current_execution_scale
    actionable_scale_delta = next_session_actionable_scale - current_execution_scale
    scale_delta = actionable_scale_delta
    holding_transition = current_holding != next_holding
    same_active_holding = current_holding == next_holding and current_holding != "cash"
    scale_trade_required = bool(
        same_active_holding
        and abs(actionable_scale_delta) >= SCALE_TRADE_REQUIRED_EPSILON
    )
    scale_trade_state = "rebalance_scale" if scale_trade_required else "hold_scale"
    next_session_leg_turnover = calc_target_vol_turnover(
        current_holding,
        current_execution_scale,
        next_holding,
        next_session_actionable_scale,
    )
    next_session_leg_cost_est_raw = next_session_leg_turnover * TARGET_VOL_SCALE_CHANGE_COST
    same_holding_next = current_holding == next_holding
    next_session_overlay_cost_est = next_session_leg_cost_est_raw if same_holding_next else 0.0
    next_session_trade_cost_est = next_session_overlay_cost_est
    effective_trade_state = "rebalance_scale" if scale_trade_required else holding_trade_state
    latest_signal["current_holding"] = current_holding
    latest_signal["next_holding"] = next_holding
    latest_signal["trade_state"] = effective_trade_state
    latest_signal["effective_trade_state"] = effective_trade_state
    latest_signal["holding_trade_state"] = holding_trade_state
    latest_signal["momentum_trade_state"] = holding_trade_state
    latest_signal["scale_trade_state"] = scale_trade_state
    latest_signal["scale_trade_required"] = scale_trade_required
    latest_signal["position_transition"] = bool(holding_transition)
    latest_signal["raw_scale_delta"] = float(raw_scale_delta)
    latest_signal["actionable_scale_delta"] = float(actionable_scale_delta)
    latest_signal["scale_delta"] = float(scale_delta)
    latest_signal["position_scale_delta"] = float(actionable_scale_delta)
    latest_signal["current_execution_scale"] = float(current_execution_scale)
    latest_signal["target_position_scale"] = float(next_session_actionable_scale)
    latest_signal["next_session_target_scale"] = float(next_session_target_scale)
    latest_signal["raw_next_target_scale"] = float(next_session_target_scale)
    latest_signal["next_session_actionable_scale"] = float(next_session_actionable_scale)
    latest_signal["target_vol_scale_next_session"] = float(next_session_actionable_scale)
    latest_signal["next_session_turnover"] = float(next_session_leg_turnover)
    latest_signal["next_session_leg_turnover"] = float(next_session_leg_turnover)
    latest_signal["next_session_leg_cost_est_raw"] = float(next_session_leg_cost_est_raw)
    latest_signal["next_session_overlay_cost_est"] = float(next_session_overlay_cost_est)
    latest_signal["next_session_trade_cost_est"] = float(next_session_trade_cost_est)
    latest_signal["next_session_overlay_trade_cost_est"] = float(next_session_overlay_cost_est)
    latest_signal["next_session_trade_cost_est_type"] = "overlay_only"
    latest_signal["next_session_total_trade_cost_est_note"] = (
        "entry/exit base cost handled by embedded lineage total_cost; not directly estimable here"
    )
    latest_signal["target_vol_signal_timing"] = "close_confirmed"
    latest_signal["signal_timing"] = "close_confirmed"
    latest_signal["official_close_confirmed_signal"] = True
    latest_signal["target_vol_realtime_snapshot_vol_frozen"] = _safe_bool(
        latest_row.get("target_vol_realtime_snapshot_vol_frozen", False)
    )
    latest_signal["target_vol_frozen_lag_days"] = int(_safe_float(latest_row.get("target_vol_frozen_lag_days"), 0.0))
    latest_signal["target_vol_frozen_source_date"] = latest_row.get("target_vol_frozen_source_date", "")
    # Numeric-only passthrough fields. Derived scale and next-session cost fields
    # above remain authoritative and are intentionally excluded here.
    for src_col in [
        "microcap_close",
        "hedge_close",
        "microcap_mom",
        "hedge_mom",
        "momentum_gap",
        "gap_peak",
        "gap_decay_ratio",
        "execution_scale",
        "target_vol_realized_vol",
        "latest_realized_vol",
        "target_vol_turnover",
        "target_vol_costed_turnover",
        "scale_change_cost",
        "target_vol_trade_cost",
        "financing_cost",
        "idle_cash_yield",
    ]:
        if src_col in latest_row and pd.notna(latest_row[src_col]):
            latest_signal[src_col] = float(latest_row[src_col])
    latest_signal["signal_quality_derisk_triggered"] = _safe_bool(
        latest_row.get("signal_quality_derisk_triggered", False),
        default=False,
    )
    latest_signal["fixed_hedge_ratio"] = BASE_HEDGE_RATIO
    latest_signal["momentum_gap_exit_buffer"] = V2_0_MOMENTUM_GAP_EXIT_BUFFER
    latest_signal["decay_ratio_threshold"] = DECAY_RATIO_THRESHOLD
    latest_signal["derisk_scale"] = DERISK_SCALE
    latest_signal["recovery_ratio_threshold"] = RECOVERY_RATIO_THRESHOLD
    latest_signal["version"] = "2.0"
    latest_signal["base_version"] = "embedded_v2_base"
    latest_signal["overlay_type"] = "target_volatility_scaling"
    latest_signal["target_vol"] = TARGET_VOL
    latest_signal["target_vol_window"] = TARGET_VOL_WINDOW
    latest_signal["max_leverage"] = TARGET_VOL_MAX_LEVERAGE
    latest_signal["signal_label"] = next_holding
    return pd.DataFrame([{**latest_signal, "date": pd.Timestamp(net_df.index.max())}])


def summarize_returns(ret: pd.Series) -> dict[str, float | str | int]:
    ret = ret.dropna().astype(float)
    if ret.empty:
        raise ValueError("empty return series")
    nav = (1.0 + ret).cumprod()
    years = (ret.index[-1] - ret.index[0]).days / 365.25
    annual = nav.iloc[-1] ** (1.0 / years) - 1.0 if years > 0 else 0.0
    vol = ret.std(ddof=1) * (TARGET_VOL_TRADING_DAYS**0.5)
    sharpe = annual / vol if vol > 0 else 0.0
    dd = nav / nav.cummax() - 1.0
    return {
        "start_date": str(pd.Timestamp(ret.index[0]).date()),
        "end_date": str(pd.Timestamp(ret.index[-1]).date()),
        "days": int(len(ret)),
        "final_nav": float(nav.iloc[-1]),
        "total_return_pct": float((nav.iloc[-1] - 1.0) * 100.0),
        "annual_pct": float(annual * 100.0),
        "max_drawdown_pct": float(dd.min() * 100.0),
        "sharpe": float(sharpe),
        "vol_pct": float(vol * 100.0),
    }


def summarize_yearly(ret: pd.Series) -> pd.DataFrame:
    rows = []
    for year, part in ret.groupby(ret.index.year):
        part = part.dropna()
        if part.empty:
            continue
        nav = (1.0 + part).cumprod()
        years = (part.index[-1] - part.index[0]).days / 365.25
        annual = nav.iloc[-1] ** (1.0 / years) - 1.0 if years > 0 and len(part) >= 60 else np.nan
        vol = part.std(ddof=1) * (TARGET_VOL_TRADING_DAYS**0.5)
        sharpe = annual / vol if vol > 0 else 0.0
        dd = nav / nav.cummax() - 1.0
        rows.append(
            {
                "year": str(year),
                "start_date": str(pd.Timestamp(part.index[0]).date()),
                "end_date": str(pd.Timestamp(part.index[-1]).date()),
                "days": int(len(part)),
                "return_pct": float((nav.iloc[-1] - 1.0) * 100.0),
                "max_drawdown_pct": float(dd.min() * 100.0),
                "sharpe": float(sharpe),
                "annual_pct": float(annual * 100.0),
            }
        )
    return pd.DataFrame(rows)


def build_performance_payload(ret: pd.Series, source_label: str = "costed_v2_0") -> dict[str, object]:
    ensure_output_dir()
    summary = summarize_returns(ret)
    yearly_df = summarize_yearly(ret)
    _atomic_write_csv(yearly_df, PERF_YEARLY_CSV, index=False, encoding="utf-8-sig")

    nav_df = pd.DataFrame(
        {
            "date": ret.index,
            "return_net": ret.values,
            "nav_net": (1.0 + ret.fillna(0.0)).cumprod().values,
        }
    )
    _atomic_write_csv(nav_df, PERF_NAV_CSV, index=False, encoding="utf-8-sig")
    _atomic_write_csv(pd.DataFrame([summary]), PERF_SUMMARY_CSV, index=False, encoding="utf-8-sig")

    plt.figure(figsize=(12, 6))
    plt.plot(nav_df["date"], nav_df["nav_net"], linewidth=2.0)
    plt.title("Top100 Microcap Mom16 Biweekly v2.0 Target Volatility")
    plt.ylabel("NAV")
    plt.grid(alpha=0.25)
    plt.tight_layout()
    plt.savefig(PERF_PNG, dpi=160)
    plt.close()

    payload = {
        "period_label": "full_sample",
        "source": source_label,
        "start_date": summary["start_date"],
        "end_date": summary["end_date"],
        "summary": summary,
        "yearly": yearly_df.to_dict(orient="records"),
        "files": {
            "summary_csv": str(PERF_SUMMARY_CSV),
            "yearly_csv": str(PERF_YEARLY_CSV),
            "nav_csv": str(PERF_NAV_CSV),
            "chart_png": str(PERF_PNG),
        },
    }
    _atomic_write_text(PERF_JSON, _json_dumps(payload), encoding="utf-8")
    return payload


def _load_v2_base_proxy_meta() -> dict[str, object]:
    base_paths = embedded_context.base_mod.build_output_paths(embedded_context.base_mod.DEFAULT_OUTPUT_PREFIX)
    meta_path = base_paths.get("proxy_meta")
    if meta_path is None:
        return {}
    try:
        meta = json.loads(Path(meta_path).read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return {"source_used": "unknown", "proxy_meta_path": str(meta_path)}
    if isinstance(meta, dict):
        meta["proxy_meta_path"] = str(meta_path)
        return meta
    return {"source_used": "unknown", "proxy_meta_path": str(meta_path)}


def _build_v2_data_lineage() -> dict[str, object]:
    meta = _load_v2_base_proxy_meta()
    source_used = str(meta.get("source_used", "unknown"))
    source_lower = source_used.lower()
    official_wind_series = source_lower in {"windpy", "official_wind", "official_wind_csv", "wind"}
    core_params = meta.get("core_params") if isinstance(meta.get("core_params"), dict) else {}
    return {
        "microcap_proxy_type": "official_wind" if official_wind_series else "top100_public_proxy_or_official_wind",
        "official_wind_series": bool(official_wind_series),
        "source_used": source_used,
        "source_meta_path": meta.get("proxy_meta_path"),
        "method_note": meta.get("method_note"),
        "public_proxy_note": (
            "When source_used is public/local proxy, the microcap series is reconstructed from local/public "
            "A-share price, share-change, membership, and tradeability data. It is not the official Wind 868008.WI series."
        ),
        "proxy_core_params": core_params,
    }


def _v2_performance_source_label(data_lineage: dict[str, object]) -> str:
    if bool(data_lineage.get("official_wind_series")):
        return "costed_v2_0_official_wind"
    return "public_or_local_proxy_not_official_wind"


def generate_v2_0_outputs() -> tuple[dict[str, object], pd.DataFrame, pd.DataFrame]:
    ensure_output_dir()
    reference_summary, base_gross_cached, turnover_df = embedded_context._load_embedded_base_context()
    stale_outputs = incompatible_v2_0_outputs()
    if COSTED_NAV_CSV in stale_outputs and COSTED_NAV_CSV.exists():
        COSTED_NAV_CSV.unlink(missing_ok=True)
    close_df = base_gross_cached[["microcap_close", "hedge_close"]].rename(
        columns={"microcap_close": "microcap", "hedge_close": "hedge"}
    )
    live_overlay_meta: dict[str, object] = {
        "applied": False,
        "reason": "close_confirmed_signal_uses_official_base_series",
    }
    base_gross = embedded_context.base_mod.run_signal(close_df).sort_index()
    gross = embedded_context.base_mod.apply_momentum_gap_exit_buffer(
        base_gross,
        V2_0_MOMENTUM_GAP_EXIT_BUFFER,
    )
    embedded_lineage_base = embedded_context.base_mod.apply_momentum_gap_peak_decay_derisk(
        gross_result=gross,
        turnover_df=turnover_df,
        decay_ratio_threshold=DECAY_RATIO_THRESHOLD,
        derisk_scale=DERISK_SCALE,
        recovery_ratio_threshold=RECOVERY_RATIO_THRESHOLD,
    )
    out = apply_target_vol_scaling(embedded_lineage_base)
    if COSTED_NAV_CSV.exists() and COSTED_NAV_CSV not in stale_outputs:
        previous = _read_costed_nav_csv(COSTED_NAV_CSV)
        embedded_context.base_mod.assert_no_historical_rewrite(
            previous=previous,
            candidate=out.rename_axis("date").reset_index(),
            key_columns=["return_net", "holding", "next_holding", "base_pre_cost_return"],
            allowed_tail_rows=DEFAULT_ALLOWED_TAIL_ROWS,
            label="v2.0 official costed NAV",
            audit_path=OUTPUT_DIR / f"{OUTPUT_PREFIX}_historical_rewrite_audit.csv",
        )
    _atomic_write_csv(out, COSTED_NAV_CSV, index_label="date", encoding="utf-8-sig")
    _atomic_write_csv(out.rename_axis("date").reset_index(), NAV_CSV, index=False, encoding="utf-8-sig")

    signal_row = _build_signal_row(out, reference_summary)
    _atomic_write_text(LATEST_SIGNAL_CSV, signal_row.to_csv(index=False), encoding="utf-8")

    data_lineage = _build_v2_data_lineage()
    performance_source_label = _v2_performance_source_label(data_lineage)
    perf_payload = build_performance_payload(
        out["return_net"].fillna(0.0),
        source_label=performance_source_label,
    )

    summary = dict(reference_summary)
    summary["strategy"] = OUTPUT_PREFIX
    summary["version"] = "2.0"
    summary["version_role"] = EXPECTED_VERSION_ROLE
    summary["version_note"] = (
        "Standalone target-volatility overlay matching repaired v1.6 behavior. Uses v2.0-specific 0.30% momentum-gap exit buffer, "
        "60-day realized volatility, 25% annual target volatility, max 1.5x leverage, "
        "10bp leg-turnover scale-change cost, scaled embedded-lineage base trading cost, "
        "and 3% annual financing cost on exposure above 1.0x."
    )
    summary.setdefault("core_params", {})
    summary["core_params"]["fixed_hedge_ratio"] = BASE_HEDGE_RATIO
    summary["core_params"]["momentum_gap_entry_threshold"] = 0.0
    summary["core_params"]["momentum_gap_exit_buffer"] = V2_0_MOMENTUM_GAP_EXIT_BUFFER
    summary["core_params"]["signal_quality_derisk"] = {
        "type": "momentum_gap_peak_decay_derisk_new_peak_guard",
        "decay_ratio_threshold": DECAY_RATIO_THRESHOLD,
        "derisk_scale": DERISK_SCALE,
        "recovery_ratio_threshold": RECOVERY_RATIO_THRESHOLD,
        "rearm_rule": "must set a new trade gap peak after recovery before a later derisk can trigger again",
    }
    summary["core_params"]["target_volatility_scaling"] = {
        "target_vol": TARGET_VOL,
        "vol_window": TARGET_VOL_WINDOW,
        "max_leverage": TARGET_VOL_MAX_LEVERAGE,
        "min_leverage": TARGET_VOL_MIN_LEVERAGE,
        "scale_change_cost": TARGET_VOL_SCALE_CHANGE_COST,
        "scale_change_cost_model": "microcap_long_plus_hedge_leg_net_turnover",
        "scale_rebalance_threshold": TARGET_VOL_SCALE_REBALANCE_THRESHOLD,
        "scale_trade_required_epsilon": SCALE_TRADE_REQUIRED_EPSILON,
        "base_trade_cost_model": (
            "embedded_lineage_total_cost scaled by transition timing: entry uses next actionable scale, "
            "exit and active rebalance use current execution scale"
        ),
        "entry_exit_overlay_cost_model": "target-vol scale-change cost is skipped on holding transition days to avoid double-counting v1.4 entry/exit cost",
        "target_vol_scale_next_session_semantics": "actionable scale after rebalance threshold; raw model target is raw_next_target_scale",
        "idle_cash_yield": IDLE_CASH_YIELD,
        "idle_credit_on_cash_day": False,
        "idle_cash_return": "credited only on active holding days when execution_scale < 1.0",
        "volatility_return_source_priority": VOLATILITY_RETURN_SOURCE_PRIORITY,
        "pnl_return_source": PNL_RETURN_SOURCE,
        "financing_rate": TARGET_VOL_FINANCING_RATE,
        "trading_days": TARGET_VOL_TRADING_DAYS,
        "timing": "current execution scale uses T-1 realized volatility; next-session target scale uses T close realized volatility",
    }
    summary["latest_trade_date"] = str(pd.Timestamp(signal_row.iloc[0]["date"]).date())
    summary["latest_nav_date"] = str(pd.Timestamp(out.index.max()).date())
    summary["latest_signal"] = signal_row.iloc[0].drop(labels=["date"], errors="ignore").to_dict()
    summary["data_lineage"] = data_lineage
    summary["performance_source_label"] = performance_source_label
    summary["st_filter_backtest_bias_note"] = (
        "Public/local proxy excludes current ST names across the full sample when exclude_current_st is true; "
        "historical ST interval filtering is not yet implemented."
    )
    summary["proxy_construction"] = {
        "weighting": "equal_weight_available_returns",
        "constituents": 100,
        "missing_return_policy": "drop_missing_and_average_available",
        "liquidity_slippage_model": "not_included_beyond_configured_cost_model",
        "tradeability_note": (
            "The local proxy includes configured OHLC price-limit/suspension checks where available, but does not model "
            "stock-level market impact, queue priority, or capacity limits."
        ),
    }
    summary["performance_snapshot"] = perf_payload["summary"]
    summary["base_fingerprint"] = current_base_fingerprint()
    summary["live_microcap_tail_overlay"] = live_overlay_meta
    _atomic_write_text(SUMMARY_JSON, _json_dumps(summary), encoding="utf-8")
    regenerated_outputs = {
        SUMMARY_JSON,
        LATEST_SIGNAL_CSV,
        NAV_CSV,
        COSTED_NAV_CSV,
        PERF_SUMMARY_CSV,
        PERF_YEARLY_CSV,
        PERF_NAV_CSV,
        PERF_JSON,
        PERF_PNG,
    }
    for stale_path in _stale_outputs_to_remove_after_generate(stale_outputs, regenerated_outputs):
        if stale_path.exists():
            stale_path.unlink(missing_ok=True)
    return summary, signal_row, out


def build_realtime_v2_0_outputs() -> tuple[pd.DataFrame, dict[str, object], pd.DataFrame]:
    ensure_output_dir()
    realtime_base = realtime_core.load_realtime_base()
    embedded_lineage_realtime = realtime_core.build_realtime_overlay_base(realtime_base)
    meta = realtime_base.meta
    out = apply_target_vol_scaling(embedded_lineage_realtime, treat_last_row_as_snapshot=True)
    signal_row = _build_signal_row(out, realtime_base.reference_summary)
    signal_row = realtime_core.base_mod.augment_signal_with_member_rebalance(
        signal_row,
        realtime_base.context.get("changes_df"),
    )
    _apply_realtime_meta_columns_to_signal_row(signal_row, meta)
    signal_row["quote_coverage"] = f"{meta.get('member_price_count', 0)}/{meta.get('member_count', 0)}"
    signal_row["target_vol_signal_timing"] = "intraday_hypothetical_if_now_close"
    signal_row["signal_timing"] = "intraday_hypothetical_if_now_close"
    signal_row["official_close_confirmed_signal"] = False
    _atomic_write_text(REALTIME_SIGNAL_CSV, signal_row.to_csv(index=False), encoding="utf-8")
    return signal_row, meta, out


def _print_scale_fields(row: pd.Series, include_frozen: bool = False) -> None:
    print(f"current_execution_scale: {_safe_float(row.get('current_execution_scale', row.get('execution_scale')), 0.0):.2f}")
    print(f"target_vol_realized_vol: {_safe_float(row.get('target_vol_realized_vol'), 0.0):.4%}")
    if include_frozen:
        print(f"realized_vol_frozen_from_snapshot: {bool(row.get('target_vol_realtime_snapshot_vol_frozen', False))}")
    print(f"raw_next_target_scale: {_safe_float(row.get('raw_next_target_scale', row.get('next_session_target_scale')), 0.0):.2f}")
    print(
        "next_session_actionable_scale: "
        f"{_safe_float(row.get('next_session_actionable_scale', row.get('next_session_target_scale')), 0.0):.2f}"
    )
    print(f"raw_scale_delta: {_safe_float(row.get('raw_scale_delta', row.get('scale_delta')), 0.0):+.2f}")
    print(f"actionable_scale_delta: {_safe_float(row.get('actionable_scale_delta', row.get('scale_delta')), 0.0):+.2f}")
    print(f"scale_delta: {_safe_float(row.get('scale_delta'), 0.0):+.2f}")
    print(f"next_session_turnover: {_safe_float(row.get('next_session_turnover'), 0.0):.4f}")
    print(f"next_session_leg_turnover: {_safe_float(row.get('next_session_leg_turnover', row.get('next_session_turnover')), 0.0):.4f}")
    print(f"next_session_leg_cost_est_raw: {_safe_float(row.get('next_session_leg_cost_est_raw'), 0.0):.4%}")
    print(
        "next_session_overlay_cost_est: "
        f"{_safe_float(row.get('next_session_overlay_cost_est', row.get('next_session_trade_cost_est')), 0.0):.4%}"
    )
    print(f"next_session_trade_cost_est: {_safe_float(row.get('next_session_trade_cost_est'), 0.0):.4%}")
    print(f"next_session_trade_cost_est_type: {row.get('next_session_trade_cost_est_type', 'overlay_only')}")


def _print_signal_query() -> None:
    _, signal_df, _ = generate_v2_0_outputs()
    row = signal_df.iloc[0]
    print("signal")
    print("strategy_version: v2.0")
    print("base_version: embedded_v2_base")
    print(
        "overlay: target volatility "
        f"(target={TARGET_VOL:.0%}, window={TARGET_VOL_WINDOW}, max={TARGET_VOL_MAX_LEVERAGE:.1f}x)"
    )
    print(f"current_holding: {row['current_holding']}")
    print(f"next_holding: {row['next_holding']}")
    print(f"trade_state: {row.get('effective_trade_state', row.get('trade_state', 'hold'))}")
    print(f"holding_trade_state: {row.get('holding_trade_state', row.get('momentum_trade_state', 'hold'))}")
    print(f"scale_trade_state: {row.get('scale_trade_state', 'hold_scale')}")
    print(f"signal_date: {pd.Timestamp(row['date']).strftime('%Y-%m-%d')}")
    print(f"momentum_gap: {float(row.get('momentum_gap', 0.0)):+.4%}")
    _print_scale_fields(row, include_frozen=False)
    print(f"official_close_confirmed_signal: {row.get('official_close_confirmed_signal', True)}")
    print(SUMMARY_JSON)
    print(LATEST_SIGNAL_CSV)


def _print_realtime_signal_query() -> None:
    signal_df, meta, _ = build_realtime_v2_0_outputs()
    row = signal_df.iloc[0]
    print("realtime_signal")
    print("strategy_version: v2.0")
    print("base_version: embedded_v2_base")
    print(
        "overlay: target volatility "
        f"(target={TARGET_VOL:.0%}, window={TARGET_VOL_WINDOW}, max={TARGET_VOL_MAX_LEVERAGE:.1f}x)"
    )
    print(f"snapshot_time: {meta.get('snapshot_time')}")
    print(f"latest_anchor_trade_date: {meta.get('latest_anchor_trade_date')}")
    print(f"quote_trade_date: {meta.get('quote_trade_date', '')}")
    print(f"current_holding: {row['current_holding']}")
    print(f"next_holding: {row['next_holding']}")
    print(f"trade_state: {row.get('effective_trade_state', row.get('trade_state', 'hold'))}")
    print(f"holding_trade_state: {row.get('holding_trade_state', row.get('momentum_trade_state', 'hold'))}")
    print(f"scale_trade_state: {row.get('scale_trade_state', 'hold_scale')}")
    print("target_vol_signal_timing: intraday_hypothetical_if_now_close")
    _print_scale_fields(row, include_frozen=True)
    print("official_close_confirmed_signal: False")
    print(f"microcap_mom: {float(row.get('microcap_mom', 0.0)):+.4%}")
    print(f"hedge_mom: {float(row.get('hedge_mom', 0.0)):+.4%}")
    print(f"momentum_gap: {float(row.get('momentum_gap', 0.0)):+.4%}")
    print(f"quote_source: {meta.get('quote_source')}")
    print(f"hedge_quote_source: {meta.get('hedge_quote_source')}")
    print(f"quote_coverage: {meta.get('member_price_count')}/{meta.get('member_count')}")
    print(REALTIME_SIGNAL_CSV)


def _print_performance_query(query: str) -> None:
    generate_v2_0_outputs()
    perf_df = _read_costed_nav_csv(COSTED_NAV_CSV, parse_dates=["date"]).sort_values("date").set_index("date")
    old_title = embedded_context.base_mod.STRATEGY_TITLE
    embedded_context.base_mod.STRATEGY_TITLE = "Top100 Microcap Mom16 Biweekly v2.0 Target Volatility"
    try:
        embedded_context.base_mod.build_performance_outputs(
            perf_df=perf_df,
            ret_col="return_net",
            nav_col="nav_net",
            source_label=_v2_performance_source_label(_build_v2_data_lineage()),
            query_text=query,
            paths={
                "performance_summary": PERF_QUERY_SUMMARY_CSV,
                "performance_yearly": PERF_QUERY_YEARLY_CSV,
                "performance_nav": PERF_QUERY_NAV_CSV,
                "performance_chart": PERF_QUERY_PNG,
                "performance_json": PERF_QUERY_JSON,
            },
        )
    finally:
        embedded_context.base_mod.STRATEGY_TITLE = old_title
    print(PERF_QUERY_PNG)
    print(PERF_QUERY_SUMMARY_CSV)
    print(PERF_QUERY_YEARLY_CSV)
    print(PERF_QUERY_NAV_CSV)
    print(PERF_QUERY_JSON)


def _handle_query(query: str) -> None:
    # Mojibake aliases preserve compatibility with older Windows scheduled tasks
    # that passed UTF-8 bytes through a legacy code page.
    if query in {"信号", "淇″彿"}:
        _print_signal_query()
        return
    if query in {"实时信号", "瀹炴椂淇″彿"}:
        _print_realtime_signal_query()
        return
    if embedded_context.base_mod.PERFORMANCE_PATTERN.search(query):
        _print_performance_query(query)
        return
    raise ValueError("v2.0 supports: 信号 / 实时信号 / 表现 <区间>")


def main() -> None:
    query = " ".join(sys.argv[1:]).strip()
    if query:
        _handle_query(query)
        return
    generate_v2_0_outputs()
    print(str(SUMMARY_JSON))
    print(str(LATEST_SIGNAL_CSV))
    print(str(COSTED_NAV_CSV))


'''
overlay_mod, _overlay_ns = _exec_embedded_module("embedded_v2_overlay", OVERLAY_SOURCE)
OUTPUT_PREFIX = overlay_mod.OUTPUT_PREFIX
SUMMARY_JSON = overlay_mod.SUMMARY_JSON
LATEST_SIGNAL_CSV = overlay_mod.LATEST_SIGNAL_CSV
COSTED_NAV_CSV = overlay_mod.COSTED_NAV_CSV
_generate_v2_0_outputs_unlocked = overlay_mod.generate_v2_0_outputs
_build_realtime_v2_0_outputs_unlocked = overlay_mod.build_realtime_v2_0_outputs


def _pid_is_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    if os.name == "nt":
        try:
            import ctypes

            process_query_limited_information = 0x1000
            handle = ctypes.windll.kernel32.OpenProcess(
                process_query_limited_information,
                False,
                int(pid),
            )
            if not handle:
                return False
            exit_code = ctypes.c_ulong(0)
            still_active = 259
            try:
                if ctypes.windll.kernel32.GetExitCodeProcess(handle, ctypes.byref(exit_code)):
                    return bool(exit_code.value == still_active)
                return True
            finally:
                ctypes.windll.kernel32.CloseHandle(handle)
        except Exception:
            return False
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def _read_lock_pid(lock_path: Path) -> int | None:
    try:
        text = lock_path.read_text(encoding="ascii", errors="ignore").strip()
        return int(text.split()[0])
    except Exception:
        return None


@contextmanager
def _v2_file_lock(
    lock_name: str,
    wait_timeout_seconds: float = 900.0,
    stale_lock_seconds: float = 7200.0,
):
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    lock_path = OUTPUT_DIR / lock_name
    deadline = time.time() + float(wait_timeout_seconds)
    fd: int | None = None
    while fd is None:
        try:
            fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            try:
                os.write(fd, f"{os.getpid()} {time.time()}\n".encode("ascii"))
            except Exception:
                os.close(fd)
                fd = None
                try:
                    lock_path.unlink(missing_ok=True)
                except OSError:
                    pass
                raise
        except FileExistsError:
            try:
                pid = _read_lock_pid(lock_path)
                age = time.time() - lock_path.stat().st_mtime
                if age > stale_lock_seconds and (pid is None or not _pid_is_alive(pid)):
                    lock_path.unlink(missing_ok=True)
                    continue
            except OSError:
                pass
            if time.time() >= deadline:
                raise TimeoutError(f"timed out waiting for v2.0 output generation lock: {lock_path}")
            time.sleep(0.2)
    try:
        yield
    finally:
        if fd is not None:
            try:
                os.close(fd)
            except OSError:
                pass
        try:
            lock_path.unlink(missing_ok=True)
        except OSError:
            pass


@contextmanager
def _v2_output_generation_lock(
    wait_timeout_seconds: float = 900.0,
    stale_lock_seconds: float = 7200.0,
):
    with _v2_file_lock(
        f"{OUTPUT_PREFIX}_generation.lock",
        wait_timeout_seconds=wait_timeout_seconds,
        stale_lock_seconds=stale_lock_seconds,
    ):
        yield


@contextmanager
def _v2_base_build_lock(
    wait_timeout_seconds: float = 900.0,
    stale_lock_seconds: float = 7200.0,
):
    with _v2_file_lock(
        f"{OUTPUT_PREFIX}_base_build.lock",
        wait_timeout_seconds=wait_timeout_seconds,
        stale_lock_seconds=stale_lock_seconds,
    ):
        yield


@contextmanager
def _v2_realtime_output_lock(
    wait_timeout_seconds: float = 60.0,
    stale_lock_seconds: float = 300.0,
):
    with _v2_file_lock(
        f"{OUTPUT_PREFIX}_realtime.lock",
        wait_timeout_seconds=wait_timeout_seconds,
        stale_lock_seconds=stale_lock_seconds,
    ):
        yield


def generate_v2_0_outputs() -> tuple[dict[str, object], pd.DataFrame, pd.DataFrame]:
    with _v2_output_generation_lock():
        return _generate_v2_0_outputs_unlocked()


_overlay_ns["generate_v2_0_outputs"] = generate_v2_0_outputs
overlay_mod.generate_v2_0_outputs = generate_v2_0_outputs


def build_realtime_v2_0_outputs() -> tuple[pd.DataFrame, dict[str, object], pd.DataFrame]:
    with _v2_realtime_output_lock():
        return _build_realtime_v2_0_outputs_unlocked()


_overlay_ns["build_realtime_v2_0_outputs"] = build_realtime_v2_0_outputs
overlay_mod.build_realtime_v2_0_outputs = build_realtime_v2_0_outputs


def _handle_query(query: str) -> None:
    overlay_mod._handle_query(query)


def main() -> None:
    global _V2_RUNTIME_ARGS
    args = _V2_RUNTIME_ARGS or parse_v2_args(sys.argv[1:])
    _V2_RUNTIME_ARGS = args
    query = " ".join(args.query_tokens).strip()
    if query:
        _handle_query(query)
        return
    generate_v2_0_outputs()
    print(str(SUMMARY_JSON))
    print(str(LATEST_SIGNAL_CSV))
    print(str(COSTED_NAV_CSV))


if __name__ == "__main__":
    main()
