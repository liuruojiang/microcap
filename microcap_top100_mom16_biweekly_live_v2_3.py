from __future__ import annotations

import argparse
import copy
import hashlib
import importlib
import json
import math
import re
import shutil
import sys
import tempfile
import threading
import time
import warnings
from pathlib import Path


RUNTIME_PACKAGES = (
    "numpy",
    "pandas",
    "requests",
    "urllib3",
    "akshare",
    "matplotlib",
    "openpyxl",
)


def _early_parse_bootstrap(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--bootstrap-deps", action="store_true")
    parser.add_argument("--wheelhouse", type=Path, default=None)
    return parser.parse_known_args(argv)[0]


def _run_early_bootstrap_if_requested(argv: list[str]) -> None:
    early = _early_parse_bootstrap(argv)
    if not early.bootstrap_deps:
        return
    from microcap_runtime_bootstrap import (
        bootstrap_from_wheelhouse,
        find_missing_modules,
        format_bootstrap_failure_message,
        format_missing_dependencies_message,
        resolve_wheelhouse,
    )

    missing = find_missing_modules(RUNTIME_PACKAGES)
    if not missing:
        return
    wheelhouse = resolve_wheelhouse(Path(__file__).resolve().parent, early.wheelhouse)
    if wheelhouse is None:
        print(format_missing_dependencies_message(missing, bootstrap_requested=True), file=sys.stderr)
        raise SystemExit(2)
    result = bootstrap_from_wheelhouse(wheelhouse, RUNTIME_PACKAGES)
    if result.returncode != 0:
        print(format_bootstrap_failure_message(wheelhouse, result), file=sys.stderr)
        raise SystemExit(2)
    remaining = find_missing_modules(RUNTIME_PACKAGES)
    if remaining:
        print(
            "Runtime dependencies are still missing after bootstrap: "
            + ", ".join(str(item) for item in remaining),
            file=sys.stderr,
        )
        raise SystemExit(2)


_run_early_bootstrap_if_requested(sys.argv[1:] if __name__ == "__main__" else [])

import matplotlib
import numpy as np
import pandas as pd

matplotlib.use("Agg")
import matplotlib.pyplot as plt


class _LazyV2_0Module:
    def __init__(self) -> None:
        object.__setattr__(self, "_module", None)

    def _load(self) -> object:
        module = object.__getattribute__(self, "_module")
        if module is None:
            module = importlib.import_module("microcap_top100_mom16_biweekly_live_v2_0")
            object.__setattr__(self, "_module", module)
        return module

    def __getattr__(self, name: str) -> object:
        return getattr(self._load(), name)

    def __setattr__(self, name: str, value: object) -> None:
        setattr(self._load(), name, value)


v2_0 = _LazyV2_0Module()


ROOT = Path(__file__).resolve().parent
OUTPUT_DIR = ROOT / "outputs"

OUTPUT_PREFIX = "microcap_top100_mom16_biweekly_live_v2_3"
DEFAULT_OUTPUT_PREFIX = OUTPUT_PREFIX
SUMMARY_JSON = OUTPUT_DIR / f"{OUTPUT_PREFIX}_summary.json"
LATEST_SIGNAL_CSV = OUTPUT_DIR / f"{OUTPUT_PREFIX}_latest_signal.csv"
REALTIME_SIGNAL_CSV = OUTPUT_DIR / f"{OUTPUT_PREFIX}_realtime_signal.csv"
NAV_CSV = OUTPUT_DIR / f"{OUTPUT_PREFIX}_nav.csv"
PREVIOUS_COSTED_NAV_CSV = OUTPUT_DIR / "microcap_top100_mom16_lb25_hl2p5_r2w25_g0p08_eb0p08_vol10_oh_t0p26_rr0p75_exec0p8_v2_3_costed_nav.csv"
COSTED_NAV_CSV = OUTPUT_DIR / "microcap_top100_mom16_lb25_hl2p5_r2off_eb0p08_vol10_oh26_recovery20_exec0p8_v2_3_costed_nav.csv"
DEFAULT_COSTED_NAV_CSV = COSTED_NAV_CSV
LEGACY_COSTED_NAV_CSVS = [
    OUTPUT_DIR / "microcap_top100_mom16_exp_h3_lb17_signal1p0_exec0p8_gap13_nodecay_targetvol25_scale030_v2_3_costed_nav.csv",
    OUTPUT_DIR / "microcap_top100_mom16_exp_h4_lb17_signal1p0_exec0p8_gap13_nodecay_targetvol25_scale030_v2_3_costed_nav.csv",
    OUTPUT_DIR / "microcap_top100_mom16_exp_h4_lb17_signal1p0_exec0p8_gap13_decay35_recovery50_targetvol25_scale030_v2_3_costed_nav.csv"
]
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

VERSION = "2.3"
STRATEGY_REVISION = "plain_lb25_hl2p5_r2off_vol10_26_20_20260904"
EXPECTED_VERSION_ROLE = "spread_nav_log_wls_lb25_r2off_vol10_overheat"
EXPECTED_VERSION_NOTE_PREFIX = "Formal v2.3 spread-NAV log-WLS LB25 vol10 overheat defense."
LOOKBACK = 25
HALFLIFE = 2.5
R2_WINDOW = 25
R2_ENTRY_GATE = 0.0  # OFF; R2_WINDOW is retained for diagnostic compatibility only.
MOMENTUM_GAP_ENTRY_THRESHOLD = 0.0
MOMENTUM_GAP_EXIT_BUFFER = 0.08
OVERHEAT_KIND = "vol"
OVERHEAT_FEATURE_WINDOW = 10
OVERHEAT_TRIGGER_THRESHOLD = 0.26
OVERHEAT_RECOVERY_THRESHOLD = 0.20
OVERHEAT_RECOVERY_RATIO = OVERHEAT_RECOVERY_THRESHOLD / OVERHEAT_TRIGGER_THRESHOLD
TARGET_VOL_ENABLED = False
CASH_DAY_YIELD_ENABLED = False
FINANCING_ENABLED = False
TARGET_VOL = 0.25
TARGET_VOL_SCALE_REBALANCE_THRESHOLD = 0.30
FORMAL_START_DATE = pd.Timestamp("2010-05-05")
CASH_DAY_YIELD = 0.0
MISMATCH_DIAGNOSTIC_ROLLING_WINDOW = 60
MAX_REALTIME_TARGET_VOL_FROZEN_LAG_DAYS = 5
_OFFICIAL_V2_0_OUT_CACHE: tuple[str, pd.DataFrame] | None = None
_OFFICIAL_V2_0_OUT_CACHE_LOCK = threading.Lock()

EXPECTED_V2_0_TARGET_VOL_WINDOW = 75
EXPECTED_V2_0_MAX_LEVERAGE = 1.5
EXPECTED_V2_0_BASE_HEDGE_RATIO = 0.8
EXPECTED_V2_0_TRADING_DAYS = 244
EXPECTED_V2_0_IDLE_CASH_YIELD = 0.02
EXPECTED_V2_0_FUTURES_DRAG = 0.0003
EXPECTED_V2_0_FINANCING_RATE = 0.03
SIGNAL_SPREAD_HEDGE_RATIO = 1.0
EXECUTION_HEDGE_RATIO = float(v2_0.BASE_HEDGE_RATIO)
BASE_HEDGE_RATIO = EXECUTION_HEDGE_RATIO
TRADING_DAYS = int(v2_0.overlay_mod.TARGET_VOL_TRADING_DAYS)
TARGET_VOL_WINDOW = int(v2_0.overlay_mod.TARGET_VOL_WINDOW)
TARGET_VOL_MAX_LEVERAGE = float(v2_0.overlay_mod.TARGET_VOL_MAX_LEVERAGE)
FUTURES_DRAG = float(v2_0.base_mod.FUTURES_DRAG)
REQUIRED_BASE_VERSION = "2.0"
MIN_V2_0_BASE_API_REVISION = 12
MIN_V2_0_HISTORICAL_AUDIT_REVISION = 5
MIN_V2_0_DATA_STATE_FINGERPRINT_REVISION = 2
MIN_V2_0_REALTIME_CALENDAR_GUARD_REVISION = 3


def _require_v2_0_attr(parent: object, attr: str, label: str) -> object:
    if not hasattr(parent, attr):
        raise RuntimeError(f"v2_0 {label} missing")
    return getattr(parent, attr)


def _require_v2_0_callable(parent: object, attr: str, label: str) -> object:
    value = _require_v2_0_attr(parent, attr, label)
    if not callable(value):
        raise RuntimeError(f"v2_0 {label} must be callable")
    return value


def _base_version_is_compatible(actual_version: object) -> bool:
    version = str(actual_version)
    return version == REQUIRED_BASE_VERSION or version.startswith(f"{REQUIRED_BASE_VERSION}.")


def validate_v2_0_contract() -> None:
    module_name = str(getattr(v2_0, "__name__", ""))
    if not module_name.endswith("_v2_0"):
        raise RuntimeError(f"v2_0 module version mismatch: expected suffix _v2_0, got {module_name!r}")
    if not hasattr(v2_0, "VERSION"):
        raise RuntimeError("v2_0 VERSION missing")
    actual_version = str(v2_0.VERSION)
    if not _base_version_is_compatible(actual_version):
        raise RuntimeError(f"v2_0 VERSION mismatch: expected {REQUIRED_BASE_VERSION}, got {actual_version}")

    base_mod = _require_v2_0_attr(v2_0, "base_mod", "base_mod")
    embedded_context = _require_v2_0_attr(v2_0, "embedded_context", "embedded_context")
    realtime_core = _require_v2_0_attr(v2_0, "realtime_core", "realtime_core")
    overlay_mod = _require_v2_0_attr(v2_0, "overlay_mod", "overlay_mod")
    freq_mod = _require_v2_0_attr(base_mod, "freq_mod", "base_mod.freq_mod")
    cost_mod = _require_v2_0_attr(freq_mod, "cost_mod", "base_mod.freq_mod.cost_mod")

    _require_v2_0_attr(v2_0, "BASE_HEDGE_RATIO", "BASE_HEDGE_RATIO")
    _require_v2_0_attr(v2_0, "BASE_API_REVISION", "BASE_API_REVISION")
    _require_v2_0_attr(v2_0, "HISTORICAL_AUDIT_REVISION", "HISTORICAL_AUDIT_REVISION")
    _require_v2_0_attr(v2_0, "DATA_STATE_FINGERPRINT_REVISION", "DATA_STATE_FINGERPRINT_REVISION")
    _require_v2_0_attr(v2_0, "REALTIME_CALENDAR_GUARD_REVISION", "REALTIME_CALENDAR_GUARD_REVISION")
    _require_v2_0_attr(base_mod, "FUTURES_DRAG", "base_mod.FUTURES_DRAG")
    _require_v2_0_callable(v2_0, "current_base_fingerprint", "current_base_fingerprint")
    _require_v2_0_callable(v2_0, "current_strategy_fingerprint", "current_strategy_fingerprint")
    _require_v2_0_callable(v2_0, "current_data_state_fingerprint", "current_data_state_fingerprint")
    _require_v2_0_callable(v2_0, "assert_top100_outputs_fresh", "assert_top100_outputs_fresh")
    _require_v2_0_callable(v2_0, "current_runtime_fingerprint", "current_runtime_fingerprint")
    _require_v2_0_callable(v2_0, "generate_v2_0_outputs", "generate_v2_0_outputs")
    _require_v2_0_callable(v2_0, "run_realtime_query_with_fresh_state", "run_realtime_query_with_fresh_state")
    _require_v2_0_callable(embedded_context, "_load_embedded_base_context", "embedded_context._load_embedded_base_context")
    _require_v2_0_callable(embedded_context, "current_base_fingerprint", "embedded_context.current_base_fingerprint")
    _require_v2_0_callable(realtime_core, "load_realtime_base", "realtime_core.load_realtime_base")
    _require_v2_0_callable(base_mod, "apply_momentum_gap_exit_buffer", "base_mod.apply_momentum_gap_exit_buffer")
    _require_v2_0_callable(
        base_mod,
        "apply_momentum_gap_no_peak_decay_cost_model",
        "base_mod.apply_momentum_gap_no_peak_decay_cost_model",
    )
    _require_v2_0_callable(base_mod, "assert_no_historical_rewrite", "base_mod.assert_no_historical_rewrite")
    _require_v2_0_callable(cost_mod, "apply_cost_model", "base_mod.freq_mod.cost_mod.apply_cost_model")
    _require_v2_0_callable(overlay_mod, "apply_target_vol_scaling", "overlay_mod.apply_target_vol_scaling")

    expected_constants = {
        "BASE_HEDGE_RATIO": (float(v2_0.BASE_HEDGE_RATIO), EXPECTED_V2_0_BASE_HEDGE_RATIO),
        "base_mod.FUTURES_DRAG": (float(base_mod.FUTURES_DRAG), EXPECTED_V2_0_FUTURES_DRAG),
        "overlay_mod.TARGET_VOL_WINDOW": (int(overlay_mod.TARGET_VOL_WINDOW), EXPECTED_V2_0_TARGET_VOL_WINDOW),
        "overlay_mod.TARGET_VOL_MAX_LEVERAGE": (float(overlay_mod.TARGET_VOL_MAX_LEVERAGE), EXPECTED_V2_0_MAX_LEVERAGE),
        "overlay_mod.TARGET_VOL_TRADING_DAYS": (int(overlay_mod.TARGET_VOL_TRADING_DAYS), EXPECTED_V2_0_TRADING_DAYS),
        "overlay_mod.IDLE_CASH_YIELD": (float(overlay_mod.IDLE_CASH_YIELD), EXPECTED_V2_0_IDLE_CASH_YIELD),
        "overlay_mod.TARGET_VOL_FINANCING_RATE": (
            float(overlay_mod.TARGET_VOL_FINANCING_RATE),
            EXPECTED_V2_0_FINANCING_RATE,
        ),
    }
    for label, (actual, expected) in expected_constants.items():
        if isinstance(expected, float):
            if not math.isclose(float(actual), float(expected), rel_tol=0.0, abs_tol=1e-12):
                raise RuntimeError(f"v2_0 {label} mismatch: expected {expected}, got {actual}")
        elif actual != expected:
            raise RuntimeError(f"v2_0 {label} mismatch: expected {expected}, got {actual}")
    minimum_revisions = {
        "BASE_API_REVISION": (int(v2_0.BASE_API_REVISION), MIN_V2_0_BASE_API_REVISION),
        "HISTORICAL_AUDIT_REVISION": (
            int(v2_0.HISTORICAL_AUDIT_REVISION),
            MIN_V2_0_HISTORICAL_AUDIT_REVISION,
        ),
        "DATA_STATE_FINGERPRINT_REVISION": (
            int(v2_0.DATA_STATE_FINGERPRINT_REVISION),
            MIN_V2_0_DATA_STATE_FINGERPRINT_REVISION,
        ),
        "REALTIME_CALENDAR_GUARD_REVISION": (
            int(v2_0.REALTIME_CALENDAR_GUARD_REVISION),
            MIN_V2_0_REALTIME_CALENDAR_GUARD_REVISION,
        ),
    }
    for label, (actual, minimum) in minimum_revisions.items():
        if actual < minimum:
            raise RuntimeError(f"v2_0 {label} too old: expected >= {minimum}, got {actual}")


_V2_0_CONTRACT_VALIDATED = False


def _ensure_v2_0_contract_validated() -> None:
    global _V2_0_CONTRACT_VALIDATED
    if _V2_0_CONTRACT_VALIDATED:
        return
    validate_v2_0_contract()
    _V2_0_CONTRACT_VALIDATED = True


def parse_v2_3_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Top100 Mom16 Biweekly v2.3 spread-NAV log-WLS target-vol overlay"
    )
    parser.add_argument("query_tokens", nargs="*", help="信号 / 实时信号 / 表现 <区间>")
    parser.add_argument("--panel-path", type=Path, default=None)
    parser.add_argument("--index-csv", type=Path, default=None)
    parser.add_argument(
        "--v23-costed-nav-csv",
        "--costed-nav-csv",
        dest="v23_costed_nav_csv",
        type=Path,
        default=None,
        help="Override the v2.3 costed NAV CSV written/read by queries.",
    )
    parser.add_argument("--base-costed-nav-csv", type=Path, default=None)
    parser.add_argument("--capital", type=float, default=None)
    parser.add_argument("--max-workers", type=int, default=8)
    parser.add_argument("--realtime-cache-seconds", type=int, default=v2_0.DEFAULT_REALTIME_CACHE_SECONDS)
    parser.add_argument("--allow-stale-realtime", action=argparse.BooleanOptionalAction, default=False)
    parser.add_argument("--force-refresh", action="store_true")
    parser.add_argument("--bootstrap-deps", action="store_true")
    parser.add_argument("--wheelhouse", type=Path, default=None)
    parser.add_argument(
        "--v23-output-prefix",
        "--output-prefix",
        dest="v23_output_prefix",
        default=None,
        help="Override the v2.3 output prefix for summary, signal, performance, and NAV files.",
    )
    parser.add_argument("--base-output-prefix", default=None)
    parser.add_argument("--audited-history-migration-report", type=Path, default=None)
    parser.add_argument("--audited-strategy-migration-report", type=Path, default=None)
    return parser.parse_args(argv)


def configure_output_paths(output_prefix: str | None = None, costed_nav_csv: Path | None = None) -> None:
    global OUTPUT_PREFIX
    global SUMMARY_JSON, LATEST_SIGNAL_CSV, REALTIME_SIGNAL_CSV, NAV_CSV, COSTED_NAV_CSV
    global PERF_SUMMARY_CSV, PERF_YEARLY_CSV, PERF_NAV_CSV, PERF_JSON, PERF_PNG
    global PERF_QUERY_SUMMARY_CSV, PERF_QUERY_YEARLY_CSV, PERF_QUERY_NAV_CSV, PERF_QUERY_JSON, PERF_QUERY_PNG

    OUTPUT_PREFIX = str(output_prefix or DEFAULT_OUTPUT_PREFIX)
    SUMMARY_JSON = OUTPUT_DIR / f"{OUTPUT_PREFIX}_summary.json"
    LATEST_SIGNAL_CSV = OUTPUT_DIR / f"{OUTPUT_PREFIX}_latest_signal.csv"
    REALTIME_SIGNAL_CSV = OUTPUT_DIR / f"{OUTPUT_PREFIX}_realtime_signal.csv"
    NAV_CSV = OUTPUT_DIR / f"{OUTPUT_PREFIX}_nav.csv"
    if costed_nav_csv is not None:
        COSTED_NAV_CSV = Path(costed_nav_csv)
    elif OUTPUT_PREFIX == DEFAULT_OUTPUT_PREFIX:
        COSTED_NAV_CSV = DEFAULT_COSTED_NAV_CSV
    else:
        COSTED_NAV_CSV = OUTPUT_DIR / f"{OUTPUT_PREFIX}_costed_nav.csv"
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


_ACTIVE_RUNTIME_ARGS: argparse.Namespace | None = None


def configure_runtime(args: argparse.Namespace) -> None:
    global _ACTIVE_RUNTIME_ARGS
    _ACTIVE_RUNTIME_ARGS = args
    _ensure_v2_0_contract_validated()
    configure_output_paths(
        output_prefix=getattr(args, "v23_output_prefix", None),
        costed_nav_csv=getattr(args, "v23_costed_nav_csv", None),
    )
    v2_0._V2_RUNTIME_ARGS = argparse.Namespace(
        query_tokens=[],
        panel_path=getattr(args, "panel_path", None),
        index_csv=getattr(args, "index_csv", None),
        costed_nav_csv=getattr(args, "base_costed_nav_csv", None),
        output_prefix=getattr(args, "base_output_prefix", None),
        capital=getattr(args, "capital", None),
        max_workers=getattr(args, "max_workers", 8),
        realtime_cache_seconds=getattr(args, "realtime_cache_seconds", v2_0.DEFAULT_REALTIME_CACHE_SECONDS),
        allow_stale_realtime=getattr(args, "allow_stale_realtime", False),
        force_refresh=getattr(args, "force_refresh", False),
        bootstrap_deps=getattr(args, "bootstrap_deps", False),
        wheelhouse=getattr(args, "wheelhouse", None),
    )


def v2_3_output_lock(
    wait_timeout_seconds: float = v2_0.DEFAULT_V2_LOCK_WAIT_SECONDS,
    stale_lock_seconds: float = v2_0.DEFAULT_V2_STALE_LOCK_SECONDS,
):
    return v2_0._v2_file_lock(
        f"{OUTPUT_PREFIX}_generation.lock",
        wait_timeout_seconds=wait_timeout_seconds,
        stale_lock_seconds=stale_lock_seconds,
    )


def v2_3_realtime_output_lock(
    wait_timeout_seconds: float = v2_0.DEFAULT_V2_LOCK_WAIT_SECONDS,
    stale_lock_seconds: float = v2_0.DEFAULT_V2_STALE_LOCK_SECONDS,
):
    return v2_0._v2_file_lock(
        f"{OUTPUT_PREFIX}_realtime.lock",
        wait_timeout_seconds=wait_timeout_seconds,
        stale_lock_seconds=stale_lock_seconds,
    )


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
    return json.dumps(_json_sanitize(payload), ensure_ascii=False, indent=2, allow_nan=False, default=str)


def _atomic_temp_path(path: Path) -> Path:
    return path.with_suffix(path.suffix + f".tmp.{time.time_ns()}")


def _replace_with_retry(tmp: Path, path: Path, attempts: int = 5, delay_seconds: float = 0.05) -> None:
    for attempt in range(int(attempts)):
        try:
            tmp.replace(path)
            return
        except OSError:
            if attempt >= int(attempts) - 1:
                raise
            time.sleep(float(delay_seconds) * (2**attempt))


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


def ensure_output_dir() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def _read_costed_nav_csv(path: Path | None = None, **kwargs: object) -> pd.DataFrame:
    return pd.read_csv(COSTED_NAV_CSV if path is None else path, encoding="utf-8-sig", **kwargs)


def exp_weights(lookback: int = LOOKBACK, halflife: float = HALFLIFE) -> tuple[float, ...]:
    age_from_latest = np.arange(int(lookback) - 1, -1, -1, dtype=float)
    raw = 0.5 ** (age_from_latest / float(halflife))
    return tuple((raw / raw.sum()).tolist())


def validate_close_df(close_df: pd.DataFrame) -> pd.DataFrame:
    required = {"microcap", "hedge"}
    missing = required - set(close_df.columns)
    if missing:
        raise ValueError(f"close_df missing columns: {sorted(missing)}")
    if close_df.index.has_duplicates:
        raise ValueError("close_df index has duplicate dates")
    if not close_df.index.is_monotonic_increasing:
        raise ValueError("close_df index must be monotonic increasing")
    prices = close_df[["microcap", "hedge"]].apply(pd.to_numeric, errors="coerce")
    if prices.isna().any().any():
        raise ValueError("close_df contains NaN prices")
    if np.isinf(prices.to_numpy(dtype=float)).any():
        raise ValueError("close_df contains inf prices")
    if (prices <= 0).any().any():
        raise ValueError("close_df contains non-positive prices")
    normalized = close_df.copy()
    normalized["microcap"] = prices["microcap"].astype(float)
    normalized["hedge"] = prices["hedge"].astype(float)
    return normalized


def always_on_spread_nav(close_df: pd.DataFrame) -> tuple[pd.Series, pd.Series, pd.Series, float]:
    close_df = validate_close_df(close_df).sort_index()
    micro_ret = close_df["microcap"].pct_change(fill_method=None)
    hedge_ret = close_df["hedge"].pct_change(fill_method=None)
    daily_drag = float(v2_0.base_mod.FUTURES_DRAG) * SIGNAL_SPREAD_HEDGE_RATIO
    spread_ret = micro_ret.fillna(0.0) - SIGNAL_SPREAD_HEDGE_RATIO * hedge_ret.fillna(0.0) - daily_drag
    spread_nav = (1.0 + spread_ret.fillna(0.0)).cumprod()
    spread_nav.name = "spread_nav"
    return spread_nav, micro_ret, hedge_ret, daily_drag


def log_wls_score_and_r2(
    spread_nav: pd.Series,
    lookback: int = LOOKBACK,
    halflife: float = HALFLIFE,
    r2_window: int | None = None,
) -> pd.DataFrame:
    lookback = int(lookback)
    if lookback <= 0:
        raise ValueError("lookback must be positive")
    r2_window = lookback if r2_window is None else int(r2_window)
    if r2_window <= 0:
        raise ValueError("r2_window must be positive")
    weights = np.asarray(exp_weights(lookback, halflife), dtype=float)
    y = np.log(pd.to_numeric(spread_nav, errors="coerce").replace(0.0, np.nan))
    x = np.arange(lookback, dtype=float)
    w_sum = float(weights.sum())
    x_bar = float((weights * x).sum() / w_sum)
    x_centered = x - x_bar
    denom = float((weights * x_centered**2).sum())
    values = y.to_numpy(dtype=float)
    score = np.full(len(y), np.nan, dtype=float)
    r2 = np.full(len(y), np.nan, dtype=float)
    if len(values) >= lookback and denom > 0:
        windows = np.lib.stride_tricks.sliding_window_view(values, lookback)
        valid = np.isfinite(windows).all(axis=1)
        if valid.any():
            valid_windows = windows[valid]
            y_bar = valid_windows @ weights / w_sum
            y_centered = valid_windows - y_bar[:, None]
            slope = y_centered @ (weights * x_centered) / denom
            fitted = y_bar[:, None] + slope[:, None] * x_centered[None, :]
            ss_tot = (weights * y_centered**2).sum(axis=1)
            ss_res = (weights * (valid_windows - fitted) ** 2).sum(axis=1)
            r2_values = np.zeros_like(ss_tot, dtype=float)
            nonzero_tot = ss_tot > 0
            r2_values[nonzero_tot] = np.clip(1.0 - ss_res[nonzero_tot] / ss_tot[nonzero_tot], 0.0, 1.0)
            target_positions = np.flatnonzero(valid) + lookback - 1
            score[target_positions] = slope * TRADING_DAYS
            r2[target_positions] = r2_values
    result = pd.DataFrame({"annualized_log_wls_score": score, "log_wls_r2": r2}, index=y.index)
    if r2_window != lookback:
        independent_r2 = log_wls_score_and_r2(
            spread_nav,
            lookback=r2_window,
            halflife=halflife,
            r2_window=r2_window,
        )["log_wls_r2"]
        result["log_wls_r2"] = independent_r2
    return result


def _valid_log_wls_index(close_df: pd.DataFrame) -> pd.DatetimeIndex:
    spread_nav, _micro_ret, _hedge_ret, _daily_drag = always_on_spread_nav(close_df)
    log_wls = log_wls_score_and_r2(spread_nav, r2_window=R2_WINDOW)
    valid = np.isfinite(log_wls["annualized_log_wls_score"])
    if R2_ENTRY_GATE > 0:
        valid &= np.isfinite(log_wls["log_wls_r2"])
    return pd.DatetimeIndex(log_wls.index[valid])


def build_v2_3_common_index(
    close_df: pd.DataFrame,
    official_index: pd.DatetimeIndex | pd.Index | None = None,
) -> pd.DatetimeIndex:
    valid_idx = pd.DatetimeIndex(_valid_log_wls_index(close_df))
    valid_idx = pd.DatetimeIndex(valid_idx[valid_idx >= FORMAL_START_DATE]).sort_values()
    idx = valid_idx
    if official_index is not None:
        official_idx = pd.DatetimeIndex(official_index).dropna().sort_values()
        _assert_official_index_covers_valid_signal_index(valid_idx, official_idx, "v2.3")
        idx = pd.DatetimeIndex(idx.intersection(official_idx))
    return pd.DatetimeIndex(idx).sort_values()


def _assert_official_index_covers_valid_signal_index(
    valid_idx: pd.DatetimeIndex,
    official_idx: pd.DatetimeIndex,
    label: str,
) -> None:
    if len(valid_idx) == 0:
        return
    if len(official_idx) == 0:
        raise RuntimeError(f"{label} official v2.0 output index is empty")
    if official_idx.max() < valid_idx.max():
        raise RuntimeError(
            f"{label} official v2.0 output is stale: "
            f"official_last_date={official_idx.max().date()}, valid_signal_last_date={valid_idx.max().date()}"
        )
    in_overlap = valid_idx[(valid_idx >= official_idx.min()) & (valid_idx <= official_idx.max())]
    missing_internal = pd.DatetimeIndex(in_overlap.difference(official_idx)).sort_values()
    if len(missing_internal):
        examples = ", ".join(str(pd.Timestamp(dt).date()) for dt in missing_internal[:5])
        raise RuntimeError(
            f"{label} official v2.0 output missing internal sessions; "
            f"missing_count={len(missing_internal)}, examples={examples}"
        )


def build_spread_log_wls_gross(close_df: pd.DataFrame, index: pd.DatetimeIndex | None = None) -> pd.DataFrame:
    close_df = validate_close_df(close_df).sort_index()
    spread_nav, micro_ret, hedge_ret, _signal_daily_drag = always_on_spread_nav(close_df)
    log_wls = log_wls_score_and_r2(spread_nav, r2_window=R2_WINDOW)
    microcap_component = log_wls_score_and_r2(
        (1.0 + micro_ret.fillna(0.0)).cumprod(),
        r2_window=R2_WINDOW,
    )
    hedge_component = log_wls_score_and_r2(
        (1.0 + hedge_ret.fillna(0.0)).cumprod(),
        r2_window=R2_WINDOW,
    )
    common_index = _valid_log_wls_index(close_df) if index is None else pd.DatetimeIndex(index)
    score = pd.to_numeric(log_wls["annualized_log_wls_score"].loc[common_index], errors="coerce")
    r2 = pd.to_numeric(log_wls["log_wls_r2"].loc[common_index], errors="coerce")
    microcap_component_score = pd.to_numeric(
        microcap_component["annualized_log_wls_score"].loc[common_index], errors="coerce"
    )
    hedge_component_score = pd.to_numeric(
        hedge_component["annualized_log_wls_score"].loc[common_index], errors="coerce"
    )
    next_active_values: list[bool] = []
    active_state = False
    for dt in common_index:
        score_value = score.loc[dt]
        r2_value = r2.loc[dt]
        score_valid = pd.notna(score_value) and np.isfinite(float(score_value))
        r2_pass = R2_ENTRY_GATE <= 0 or (
            pd.notna(r2_value) and np.isfinite(float(r2_value)) and float(r2_value) >= R2_ENTRY_GATE
        )
        if not score_valid:
            next_active = False
        elif active_state:
            next_active = bool(float(score_value) >= -float(MOMENTUM_GAP_EXIT_BUFFER))
        else:
            next_active = bool(float(score_value) > MOMENTUM_GAP_ENTRY_THRESHOLD and r2_pass)
        next_active_values.append(next_active)
        active_state = bool(next_active)
    signal_on = pd.Series(next_active_values, index=common_index, dtype=bool)
    current_active = signal_on.shift(1, fill_value=False).astype(bool)
    microcap_ret = micro_ret.loc[common_index]
    hedge_ret_part = hedge_ret.loc[common_index]
    execution_daily_drag = float(v2_0.base_mod.FUTURES_DRAG) * EXECUTION_HEDGE_RATIO
    active_spread_ret = microcap_ret.fillna(0.0) - EXECUTION_HEDGE_RATIO * hedge_ret_part.fillna(0.0)
    futures_drag = pd.Series(
        np.where(current_active, execution_daily_drag, 0.0),
        index=common_index,
        dtype=float,
    )
    gross_ret = pd.Series(np.where(current_active, active_spread_ret - futures_drag, 0.0), index=common_index, dtype=float)
    return pd.DataFrame(
        {
            "return_raw": gross_ret,
            "return": gross_ret,
            "holding": np.where(current_active, "long_microcap_short_zz1000", "cash"),
            "next_holding": np.where(signal_on, "long_microcap_short_zz1000", "cash"),
            "signal_on": signal_on.astype(bool),
            "microcap_close": close_df["microcap"].loc[common_index],
            "hedge_close": close_df["hedge"].loc[common_index],
            "microcap_ret": microcap_ret,
            "hedge_ret": hedge_ret_part,
            "microcap_mom": microcap_component_score,
            "hedge_mom": hedge_component_score,
            "microcap_log_wls_r2": pd.to_numeric(
                microcap_component["log_wls_r2"].loc[common_index], errors="coerce"
            ),
            "hedge_log_wls_r2": pd.to_numeric(
                hedge_component["log_wls_r2"].loc[common_index], errors="coerce"
            ),
            "momentum_gap": score,
            "momentum_gap_deprecated": True,
            "annualized_log_wls_score": score,
            "schema_version": "log_wls_score_schema_v1",
            "log_wls_r2": r2,
            "spread_nav": spread_nav.loc[common_index],
            "halflife": HALFLIFE,
            "lookback": LOOKBACK,
            "exp_weight_oldest_to_newest": ",".join(f"{w:.8f}" for w in exp_weights()),
            "signal_score_label": "annualized_log_wls_score",
            "momentum_gap_legacy_note": "legacy field contains annualized spread-NAV log-WLS score, not plain microcap-minus-hedge momentum gap",
            "r2_window": R2_WINDOW,
            "r2_entry_gate": R2_ENTRY_GATE,
            "r2_gate_pass": (
                pd.Series(True, index=common_index, dtype=bool)
                if R2_ENTRY_GATE <= 0
                else (np.isfinite(r2) & r2.ge(R2_ENTRY_GATE)).fillna(False).astype(bool)
            ),
            "entry_threshold": MOMENTUM_GAP_ENTRY_THRESHOLD,
            "momentum_gap_exit_buffer": MOMENTUM_GAP_EXIT_BUFFER,
            "signal_spread_hedge_ratio": SIGNAL_SPREAD_HEDGE_RATIO,
            "execution_hedge_ratio": EXECUTION_HEDGE_RATIO,
            "futures_drag": futures_drag,
            "active_spread_ret": pd.Series(np.where(current_active, active_spread_ret, 0.0), index=common_index, dtype=float),
            "weight": 1.0,
            "target_vol_execution_scale": 1.0,
        },
        index=common_index,
    )


def apply_cost(gross: pd.DataFrame, turnover_df: pd.DataFrame) -> pd.DataFrame:
    out = v2_0.base_mod.freq_mod.cost_mod.apply_cost_model(gross, turnover_df)
    out["overlay_pre_cost_return"] = pd.to_numeric(out["return"], errors="coerce").fillna(0.0)
    return out


def _overheat_feature_series(gross: pd.DataFrame) -> pd.Series:
    kind = str(OVERHEAT_KIND)
    if kind == "vol":
        spread_nav = pd.to_numeric(gross["spread_nav"], errors="coerce")
        ret = spread_nav.pct_change(fill_method=None).fillna(0.0)
        return ret.rolling(int(OVERHEAT_FEATURE_WINDOW)).std(ddof=1).mul(math.sqrt(TRADING_DAYS))
    raise ValueError(f"unknown v2.3 overheat kind: {kind}")


def apply_overheat_defense(gross: pd.DataFrame, turnover_df: pd.DataFrame) -> pd.DataFrame:
    trigger_threshold = float(OVERHEAT_TRIGGER_THRESHOLD)
    recovery_threshold = float(OVERHEAT_RECOVERY_THRESHOLD)
    if recovery_threshold >= trigger_threshold:
        raise ValueError("OVERHEAT_RECOVERY_THRESHOLD must be lower than OVERHEAT_TRIGGER_THRESHOLD")

    out = gross.copy().sort_index()
    feature = _overheat_feature_series(out).reindex(out.index)
    base_holding = out["holding"].fillna("cash").astype(str)
    base_next_holding = out["next_holding"].fillna(base_holding).astype(str)
    returns = pd.to_numeric(out["return"], errors="coerce")
    rebalance_base = v2_0.base_mod.freq_mod.cost_mod.map_rebalance_apply_costs(out.index, turnover_df)
    rebalance_base = pd.to_numeric(rebalance_base.reindex(out.index), errors="coerce").fillna(0.0)
    entry_cost_value = float(v2_0.base_mod.freq_mod.cost_mod.ENTRY_COST)
    exit_cost_value = float(v2_0.base_mod.freq_mod.cost_mod.EXIT_COST)

    current_active = bool(base_holding.iloc[0] != "cash") if len(base_holding) else False
    risk_off = False
    nav_net = 1.0

    executed_holding: list[str] = []
    executed_next_holding: list[str] = []
    executed_signal_on: list[bool] = []
    risk_off_flags: list[bool] = []
    exit_flags: list[bool] = []
    reentry_flags: list[bool] = []
    block_entry_flags: list[bool] = []
    execution_scales: list[float] = []
    next_execution_scales: list[float] = []
    entry_exit_costs: list[float] = []
    rebalance_costs: list[float] = []
    total_costs: list[float] = []
    return_nets: list[float] = []
    nav_nets: list[float] = []
    overlay_pre_cost_returns: list[float] = []

    for dt in out.index:
        base_next_active = bool(base_next_holding.loc[dt] != "cash")
        value = feature.loc[dt]
        is_hot = pd.notna(value) and float(value) >= trigger_threshold
        is_cool = pd.notna(value) and float(value) <= recovery_threshold
        exit_trigger = False
        reentry_trigger = False
        block_entry_trigger = False

        if risk_off:
            if is_cool:
                desired_next_active = base_next_active
                risk_off_next = False
                reentry_trigger = bool(base_next_active)
            else:
                desired_next_active = False
                risk_off_next = True
        else:
            desired_next_active = base_next_active
            risk_off_next = False
            if desired_next_active and is_hot:
                if current_active:
                    exit_trigger = True
                else:
                    block_entry_trigger = True
                desired_next_active = False
                risk_off_next = True

        raw_daily_return = returns.loc[dt]
        return_is_finite = pd.notna(raw_daily_return) and np.isfinite(float(raw_daily_return))
        if current_active and not return_is_finite:
            raise ValueError(
                "v2.3 active return is non-finite: "
                f"date={pd.Timestamp(dt).isoformat()}, value={out.at[dt, 'return']!r}"
            )
        gross_daily_return = float(raw_daily_return) if return_is_finite else 0.0
        realized_daily_return = gross_daily_return if current_active else 0.0
        entry_cost = entry_cost_value if (not current_active and desired_next_active) else 0.0
        exit_cost = exit_cost_value if (current_active and not desired_next_active) else 0.0
        rebalance_cost = float(rebalance_base.loc[dt]) if (current_active and desired_next_active) else 0.0
        total_cost = entry_cost + exit_cost + rebalance_cost
        return_net = (1.0 + realized_daily_return) * (1.0 - total_cost) - 1.0
        nav_net *= 1.0 + return_net

        executed_holding.append("long_microcap_short_zz1000" if current_active else "cash")
        executed_next_holding.append("long_microcap_short_zz1000" if desired_next_active else "cash")
        executed_signal_on.append(bool(desired_next_active))
        risk_off_flags.append(bool(risk_off))
        exit_flags.append(bool(exit_trigger))
        reentry_flags.append(bool(reentry_trigger))
        block_entry_flags.append(bool(block_entry_trigger))
        execution_scales.append(1.0 if current_active else 0.0)
        next_execution_scales.append(1.0 if desired_next_active else 0.0)
        entry_exit_costs.append(float(entry_cost + exit_cost))
        rebalance_costs.append(float(rebalance_cost))
        total_costs.append(float(total_cost))
        return_nets.append(float(return_net))
        nav_nets.append(float(nav_net))
        overlay_pre_cost_returns.append(float(realized_daily_return))

        current_active = bool(desired_next_active)
        risk_off = bool(risk_off_next)

    out["base_holding"] = base_holding
    out["base_next_holding"] = base_next_holding
    out["base_signal_on"] = base_next_holding.ne("cash")
    out["holding"] = executed_holding
    out["next_holding"] = executed_next_holding
    out["signal_on"] = executed_signal_on
    out["overheat_kind"] = OVERHEAT_KIND
    out["overheat_feature_window"] = int(OVERHEAT_FEATURE_WINDOW)
    out["overheat_trigger_threshold"] = trigger_threshold
    out["overheat_recovery_threshold"] = recovery_threshold
    out["overheat_recovery_ratio"] = float(OVERHEAT_RECOVERY_RATIO)
    out["overheat_feature_value"] = feature
    out["overheat_risk_off"] = pd.Series(risk_off_flags, index=out.index, dtype=bool)
    out["overheat_exit_triggered"] = pd.Series(exit_flags, index=out.index, dtype=bool)
    out["overheat_reentry_triggered"] = pd.Series(reentry_flags, index=out.index, dtype=bool)
    out["overheat_block_entry_triggered"] = pd.Series(block_entry_flags, index=out.index, dtype=bool)
    out["actual_execution_scale"] = pd.Series(execution_scales, index=out.index, dtype=float)
    out["current_execution_scale"] = out["actual_execution_scale"]
    out["execution_scale"] = out["actual_execution_scale"]
    out["target_vol_execution_scale"] = out["actual_execution_scale"]
    out["next_session_actionable_scale"] = pd.Series(next_execution_scales, index=out.index, dtype=float)
    out["next_session_target_scale"] = out["next_session_actionable_scale"]
    out["target_vol_scale_next_session"] = out["next_session_actionable_scale"]
    out["entry_exit_cost"] = pd.Series(entry_exit_costs, index=out.index, dtype=float)
    out["rebalance_cost"] = pd.Series(rebalance_costs, index=out.index, dtype=float)
    out["base_trade_cost_scaled"] = out["rebalance_cost"]
    out["scale_change_cost"] = 0.0
    out["financing_cost"] = 0.0
    out["total_cost"] = pd.Series(total_costs, index=out.index, dtype=float)
    out["base_pre_cost_return"] = pd.Series(overlay_pre_cost_returns, index=out.index, dtype=float)
    out["overlay_pre_cost_return"] = out["base_pre_cost_return"]
    out["return_net"] = pd.Series(return_nets, index=out.index, dtype=float)
    out["nav_net"] = pd.Series(nav_nets, index=out.index, dtype=float)
    out["return"] = out["return_net"]
    out["nav"] = out["nav_net"]
    out["version"] = VERSION
    out["base_version"] = "embedded_v2_base"
    out["overlay_type"] = STRATEGY_REVISION
    out["strategy_revision"] = STRATEGY_REVISION
    out["r2_gate_enabled"] = R2_ENTRY_GATE > 0
    out["target_vol_enabled"] = TARGET_VOL_ENABLED
    out["cash_day_yield"] = 0.0
    out["cash_day_yield_annual"] = 0.0
    out["cash_day_yield_enabled"] = CASH_DAY_YIELD_ENABLED
    out["financing_enabled"] = FINANCING_ENABLED
    out["return_column_semantics"] = (
        "return equals return_net after v2.3 LB25 R2-OFF signal, vol10 overheat defense, "
        "and base entry/exit/rebalance costs; no target-vol, cash-day yield, or financing overlay"
    )
    return out


def apply_cash_day_yield(out: pd.DataFrame) -> pd.DataFrame:
    adjusted = out.copy()
    holding = adjusted["holding"].astype(str) if "holding" in adjusted.columns else pd.Series("cash", index=adjusted.index)
    cash_day = holding.eq("cash")
    daily_yield = float(CASH_DAY_YIELD) / float(TRADING_DAYS)
    cash_day_yield = pd.Series(0.0, index=adjusted.index, dtype=float)
    cash_day_yield.loc[cash_day] = daily_yield
    ret = pd.to_numeric(adjusted["return_net"], errors="coerce").fillna(0.0)
    adjusted["return_net"] = (1.0 + ret) * (1.0 + cash_day_yield) - 1.0
    adjusted["nav_net"] = (1.0 + adjusted["return_net"].fillna(0.0)).cumprod()
    adjusted["return"] = adjusted["return_net"]
    adjusted["nav"] = adjusted["nav_net"]
    base_gross = pd.to_numeric(
        adjusted.get("return_gross_target_vol", pd.Series(0.0, index=adjusted.index)),
        errors="coerce",
    ).fillna(0.0)
    adjusted["return_gross_target_vol_after_cash_day_yield"] = (1.0 + base_gross) * (1.0 + cash_day_yield) - 1.0
    adjusted["cash_day_yield"] = cash_day_yield
    adjusted["cash_day_yield_annual"] = CASH_DAY_YIELD
    adjusted["cash_day_yield_enabled"] = True
    adjusted["return_column_semantics"] = (
        "return equals return_net after v2.3 target-vol and cash-day-yield overlay; "
        "use base_pre_cost_return, return_gross_base, or "
        "return_gross_target_vol_after_cash_day_yield for pre-cost return"
    )
    return adjusted


def _safe_float(value: object, default: float = 0.0) -> float:
    try:
        if pd.isna(value):
            return float(default)
        return float(value)
    except (TypeError, ValueError):
        return float(default)


def _holding_is_active(value: object) -> bool:
    if pd.isna(value):
        return False
    text = str(value or "cash")
    return text not in {"", "cash", "nan", "None", "<NA>"}


def _apply_fixed_exposure_next_session_cost_fields(
    row: pd.DataFrame,
    latest: pd.Series,
    *,
    hedge_ratio: float,
) -> None:
    row_idx = row.index[0]
    current_holding = str(row.at[row_idx, "current_holding"])
    next_holding = str(row.at[row_idx, "next_holding"])
    current_active = _holding_is_active(current_holding)
    next_active = _holding_is_active(next_holding)
    current_scale = _safe_float(
        latest.get("current_execution_scale", latest.get("execution_scale")),
        1.0 if current_active else 0.0,
    )
    next_scale = _safe_float(
        latest.get("next_session_actionable_scale", latest.get("target_vol_scale_next_session")),
        1.0 if next_active else 0.0,
    )
    if not current_active:
        current_scale = 0.0
    if not next_active:
        next_scale = 0.0
    exposure_delta = abs(float(next_scale) - float(current_scale))
    next_session_leg_turnover = exposure_delta * (1.0 + float(hedge_ratio))
    entry_cost = (
        float(v2_0.base_mod.freq_mod.cost_mod.ENTRY_COST) * max(float(next_scale), 0.0)
        if (not current_active and next_active)
        else 0.0
    )
    exit_cost = (
        float(v2_0.base_mod.freq_mod.cost_mod.EXIT_COST) * max(float(current_scale), 0.0)
        if (current_active and not next_active)
        else 0.0
    )
    trade_cost = float(entry_cost + exit_cost)
    row["next_session_turnover"] = float(next_session_leg_turnover)
    row["next_session_leg_turnover"] = float(next_session_leg_turnover)
    row["next_session_leg_cost_est_raw"] = trade_cost
    row["next_session_overlay_cost_est"] = 0.0
    row["next_session_trade_cost_est"] = trade_cost
    row["next_session_overlay_trade_cost_est"] = 0.0
    row["next_session_trade_cost_est_type"] = "fixed_exposure_entry_exit"
    row["next_session_total_trade_cost_est_note"] = (
        "fixed-exposure entry/exit cost estimate; target-vol scale-change cost is disabled"
    )


def build_signal_execution_mismatch_diagnostics(
    close_df: pd.DataFrame,
    out: pd.DataFrame,
    rolling_window: int = MISMATCH_DIAGNOSTIC_ROLLING_WINDOW,
) -> dict[str, object]:
    close_df = close_df.sort_index()
    micro_ret = close_df["microcap"].pct_change(fill_method=None)
    hedge_ret = close_df["hedge"].pct_change(fill_method=None)
    daily_drag = float(v2_0.base_mod.FUTURES_DRAG)
    signal_ret = micro_ret - SIGNAL_SPREAD_HEDGE_RATIO * hedge_ret - daily_drag * SIGNAL_SPREAD_HEDGE_RATIO
    execution_ret = micro_ret - EXECUTION_HEDGE_RATIO * hedge_ret - daily_drag * EXECUTION_HEDGE_RATIO
    frame = pd.DataFrame(
        {
            "signal_ret": signal_ret,
            "execution_ret": execution_ret,
            "exec_minus_signal_ret": execution_ret - signal_ret,
        }
    ).replace([np.inf, -np.inf], np.nan)
    frame = frame.loc[frame.index.intersection(pd.DatetimeIndex(out.index))].dropna()
    active = out["holding"].astype(str).ne("cash").reindex(frame.index).fillna(False) if "holding" in out.columns else pd.Series(False, index=frame.index)
    active_frame = frame.loc[active]
    rolling_window = max(2, int(rolling_window))
    rolling_corr = frame["signal_ret"].rolling(rolling_window, min_periods=min(rolling_window, len(frame))).corr(frame["execution_ret"])
    active_rolling_corr = active_frame["signal_ret"].rolling(
        rolling_window,
        min_periods=min(rolling_window, len(active_frame)),
    ).corr(active_frame["execution_ret"]) if len(active_frame) else pd.Series(dtype=float)
    return {
        "signal_spread_hedge_ratio": SIGNAL_SPREAD_HEDGE_RATIO,
        "execution_hedge_ratio": EXECUTION_HEDGE_RATIO,
        "net_unhedged_zz1000_ratio": SIGNAL_SPREAD_HEDGE_RATIO - EXECUTION_HEDGE_RATIO,
        "rolling_window": rolling_window,
        "rows": int(len(frame)),
        "active_rows": int(len(active_frame)),
        "rolling_corr_latest": _safe_float(rolling_corr.dropna().iloc[-1], np.nan) if rolling_corr.dropna().size else None,
        f"rolling_corr_{rolling_window}d_latest": _safe_float(rolling_corr.dropna().iloc[-1], np.nan) if rolling_corr.dropna().size else None,
        "active_rolling_corr_latest": _safe_float(active_rolling_corr.dropna().iloc[-1], np.nan) if active_rolling_corr.dropna().size else None,
        f"active_rolling_corr_{rolling_window}d_latest": _safe_float(active_rolling_corr.dropna().iloc[-1], np.nan) if active_rolling_corr.dropna().size else None,
        "cumulative_exec_minus_signal_component": float((1.0 + frame["exec_minus_signal_ret"]).prod() - 1.0) if len(frame) else 0.0,
        "active_cumulative_exec_minus_signal_component": float((1.0 + active_frame["exec_minus_signal_ret"]).prod() - 1.0) if len(active_frame) else 0.0,
        "mean_daily_exec_minus_signal": float(frame["exec_minus_signal_ret"].mean()) if len(frame) else 0.0,
        "active_mean_daily_exec_minus_signal": float(active_frame["exec_minus_signal_ret"].mean()) if len(active_frame) else 0.0,
    }


def apply_signal_execution_mismatch_columns(signal_row: pd.DataFrame, diagnostics: dict[str, object]) -> pd.DataFrame:
    for key, value in diagnostics.items():
        signal_row[f"signal_execution_mismatch_{key}"] = value
    return signal_row


def _realtime_target_vol_trading_lag_from_calendar(
    snapshot_date: object,
    source_date: object,
    official_index: pd.DatetimeIndex | pd.Index,
) -> int | None:
    official = pd.DatetimeIndex(official_index).dropna().sort_values()
    if len(official) == 0:
        warnings.warn(
            "target-vol freshness guard could not compare trading lag: empty official index",
            RuntimeWarning,
            stacklevel=2,
        )
        return None
    snapshot_ts = pd.Timestamp(snapshot_date).normalize()
    source_ts = pd.Timestamp(source_date).normalize()
    official_days = pd.DatetimeIndex(pd.Series(official.normalize()).drop_duplicates().sort_values())
    expected_pos = int(official_days.searchsorted(snapshot_ts, side="right") - 1)
    source_pos = int(official_days.searchsorted(source_ts, side="right") - 1)
    if expected_pos < 0 or source_pos < 0:
        warnings.warn(
            "target-vol freshness guard could not compare trading lag: "
            f"snapshot_date={snapshot_ts.date()}, source_date={source_ts.date()}, "
            f"official_start={official_days[0].date()}, official_end={official_days[-1].date()}",
            RuntimeWarning,
            stacklevel=2,
        )
        return None
    if official_days[source_pos] != source_ts:
        warnings.warn(
            "target-vol freshness guard aligned source_date to previous official trading day: "
            f"source_date={source_ts.date()}, aligned_source_date={official_days[source_pos].date()}",
            RuntimeWarning,
            stacklevel=2,
        )
    if snapshot_ts <= official_days[-1] and official_days[expected_pos] != snapshot_ts:
        warnings.warn(
            "target-vol freshness guard aligned snapshot date to previous official trading day: "
            f"snapshot_date={snapshot_ts.date()}, aligned_snapshot_date={official_days[expected_pos].date()}",
            RuntimeWarning,
            stacklevel=2,
        )
    return max(0, expected_pos - source_pos)


def assert_realtime_target_vol_lag_fresh(
    out: pd.DataFrame,
    official_index: pd.DatetimeIndex | pd.Index | None = None,
    required_calendar_end_date: object | None = None,
) -> None:
    if out.empty or "target_vol_frozen_lag_days" not in out.columns:
        return
    latest = out.iloc[-1]
    trading_lag_days = int(_safe_float(latest.get("target_vol_frozen_lag_trading_days", latest.get("target_vol_frozen_lag_days")), 0.0))
    calendar_lag_days = int(_safe_float(latest.get("target_vol_frozen_lag_calendar_days", latest.get("target_vol_frozen_lag_days")), 0.0))
    source_date = latest.get("target_vol_frozen_source_date", "")
    guard_trading_lag_days = trading_lag_days
    if official_index is not None and source_date != "":
        official_days = pd.DatetimeIndex(official_index).dropna().normalize().drop_duplicates().sort_values()
        source_ts = pd.Timestamp(source_date).normalize()
        required_end_ts = (
            pd.Timestamp(required_calendar_end_date).normalize()
            if required_calendar_end_date not in (None, "")
            else source_ts
        )
        if len(official_days) == 0 and required_calendar_end_date not in (None, ""):
            raise RuntimeError(
                "official trading calendar is empty; refusing realtime target-vol freshness check"
            )
        if len(official_days) and official_days[-1] < required_end_ts:
            raise RuntimeError(
                "official trading calendar is stale relative to confirmed anchor date: "
                f"calendar_last_date={official_days[-1].date()}, confirmed_anchor_date={required_end_ts.date()}"
            )
        latest_ts = pd.Timestamp(out.index[-1]).normalize()
        snapshot_mode = bool(trading_lag_days > 0 or calendar_lag_days > 0 or source_ts != latest_ts)
        expected_date = latest_ts if snapshot_mode or len(official_days) == 0 else official_days[-1]
        calendar_lag = _realtime_target_vol_trading_lag_from_calendar(expected_date, source_date, official_index)
        if calendar_lag is not None:
            guard_trading_lag_days = calendar_lag
    if guard_trading_lag_days <= MAX_REALTIME_TARGET_VOL_FROZEN_LAG_DAYS:
        return
    raise RuntimeError(
        "target-vol frozen lag exceeds realtime limit: "
        f"trading_lag_days={guard_trading_lag_days}, stored_trading_lag_days={trading_lag_days}, "
        f"calendar_lag_days={calendar_lag_days}, "
        f"limit={MAX_REALTIME_TARGET_VOL_FROZEN_LAG_DAYS}, source_date={source_date}"
    )


def apply_target_vol(costed_base: pd.DataFrame, target_vol: float = TARGET_VOL, *, treat_last_row_as_snapshot: bool = False) -> pd.DataFrame:
    out = v2_0.overlay_mod.apply_target_vol_scaling(
        costed_base,
        treat_last_row_as_snapshot=treat_last_row_as_snapshot,
        target_vol=float(target_vol),
        scale_rebalance_threshold=float(TARGET_VOL_SCALE_REBALANCE_THRESHOLD),
    )
    out = apply_cash_day_yield(out)
    out["version"] = VERSION
    out["base_version"] = "embedded_v2_base"
    out["overlay_type"] = "spread_nav_log_wls_gap_target_vol"
    out["scale_rebalance_threshold"] = TARGET_VOL_SCALE_REBALANCE_THRESHOLD
    out["schema_version"] = "log_wls_score_schema_v1"
    out["momentum_gap_deprecated"] = True
    return out


def build_v2_3_result(
    close_df: pd.DataFrame,
    turnover_df: pd.DataFrame,
    common_index: pd.DatetimeIndex | None = None,
) -> pd.DataFrame:
    if common_index is None:
        common_index = build_v2_3_common_index(close_df)
    else:
        common_index = pd.DatetimeIndex(common_index)
        common_index = common_index[common_index >= FORMAL_START_DATE].sort_values()
    gross = build_spread_log_wls_gross(close_df, common_index)
    out = apply_overheat_defense(gross, turnover_df)
    if out.empty:
        raise ValueError(
            "v2.3 output is empty: check close_df, official_v2_0_out.index, "
            "FORMAL_START_DATE, and valid log-WLS window."
        )
    return out


def current_base_fingerprint() -> dict[str, object]:
    _ensure_v2_0_contract_validated()
    base = dict(v2_0.embedded_context.current_base_fingerprint())
    return {
        "base_version": "embedded_v2_base",
        "strategy_version": VERSION,
        "base_fingerprint": base,
        "signal_model": {
            "type": "spread_nav_log_wls_exp",
            "lookback": LOOKBACK,
            "halflife": HALFLIFE,
            "r2_window": R2_WINDOW,
            "r2_entry_gate": R2_ENTRY_GATE,
            "entry_threshold": MOMENTUM_GAP_ENTRY_THRESHOLD,
            "momentum_gap_exit_buffer": MOMENTUM_GAP_EXIT_BUFFER,
            "signal_spread_hedge_ratio": SIGNAL_SPREAD_HEDGE_RATIO,
            "execution_hedge_ratio": EXECUTION_HEDGE_RATIO,
        },
        "lookback": LOOKBACK,
        "halflife": HALFLIFE,
        "exp_weight_oldest_to_newest": list(exp_weights()),
        "common_index_source": "intersection of valid spread-NAV log-WLS signal dates and official v2.0 output index, filtered from 2010-05-05",
        "score_definition": "annualized weighted log slope of always-on 1.0x hedged signal spread NAV",
        "nav_csv_momentum_gap_column_alias_note": "momentum_gap stores annualized_log_wls_score for v2.0 compatibility, not raw microcap minus hedge gap",
        "schema_version": "log_wls_score_schema_v1",
        "momentum_gap_deprecated": True,
        "r2_gate": R2_ENTRY_GATE,
        "r2_window": R2_WINDOW,
        "momentum_gap_entry_threshold": MOMENTUM_GAP_ENTRY_THRESHOLD,
        "signal_spread_hedge_ratio": SIGNAL_SPREAD_HEDGE_RATIO,
        "execution_hedge_ratio": EXECUTION_HEDGE_RATIO,
        "base_hedge_ratio": BASE_HEDGE_RATIO,
        "momentum_gap_exit_buffer": MOMENTUM_GAP_EXIT_BUFFER,
        "signal_quality_derisk_enabled": False,
        "overheat_defense": {
            "enabled": True,
            "kind": OVERHEAT_KIND,
            "feature_window": OVERHEAT_FEATURE_WINDOW,
            "trigger_threshold": OVERHEAT_TRIGGER_THRESHOLD,
            "recovery_ratio": OVERHEAT_RECOVERY_RATIO,
            "recovery_threshold": OVERHEAT_RECOVERY_THRESHOLD,
        },
        "target_volatility_scaling": {"enabled": TARGET_VOL_ENABLED},
        "cash_day_yield": {"enabled": CASH_DAY_YIELD_ENABLED},
        "financing": {"enabled": FINANCING_ENABLED},
        "signal_execution_mismatch_rolling_window": MISMATCH_DIAGNOSTIC_ROLLING_WINDOW,
    }


def summary_matches_current_v2_3_base(summary: dict[str, object]) -> bool:
    if not isinstance(summary, dict):
        return False
    if str(summary.get("version")) != VERSION:
        return False
    if str(summary.get("version_role")) != EXPECTED_VERSION_ROLE:
        return False
    if not str(summary.get("version_note", "")).startswith(EXPECTED_VERSION_NOTE_PREFIX):
        return False
    return summary.get("base_fingerprint") == current_base_fingerprint()


def incompatible_v2_3_outputs() -> list[Path]:
    outputs = [
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
        return [path for path in outputs if path.exists()]
    try:
        summary = json.loads(SUMMARY_JSON.read_text(encoding="utf-8"))
    except Exception:
        summary = None
    if summary_matches_current_v2_3_base(summary):
        return []
    return outputs


def _stale_outputs_to_remove_after_generate(stale_outputs: list[Path], regenerated_outputs: set[Path]) -> list[Path]:
    protected = set(regenerated_outputs)
    # Close-confirmed generation does not own the realtime signal artifact; the
    # realtime route refreshes it atomically when queried.
    protected.add(REALTIME_SIGNAL_CSV)
    return [path for path in stale_outputs if path not in protected]


def summarize_returns(ret: pd.Series) -> dict[str, float | str | int]:
    ret = ret.dropna().astype(float)
    if ret.empty:
        raise ValueError("empty return series")
    nav = (1.0 + ret).cumprod()
    years = (ret.index[-1] - ret.index[0]).days / 365.25
    annual = nav.iloc[-1] ** (1.0 / years) - 1.0 if years > 0 else 0.0
    vol = ret.std(ddof=1) * (TRADING_DAYS**0.5)
    sharpe = annual / vol if vol > 0 else 0.0
    # Initial capital is a high-water mark even when the first return is a loss.
    drawdown = nav.div(nav.cummax().clip(lower=1.0)).sub(1.0)
    return {
        "start_date": str(pd.Timestamp(ret.index[0]).date()),
        "end_date": str(pd.Timestamp(ret.index[-1]).date()),
        "days": int(len(ret)),
        "final_nav": float(nav.iloc[-1]),
        "total_return_pct": float((nav.iloc[-1] - 1.0) * 100.0),
        "annual_pct": float(annual * 100.0),
        "max_drawdown_pct": float(drawdown.min() * 100.0),
        "sharpe": float(sharpe),
        "vol_pct": float(vol * 100.0),
    }


REQUIRED_PERFORMANCE_WINDOWS: tuple[tuple[str, int | None], ...] = (
    ("full", None),
    ("last_10y", 10),
    ("last_5y", 5),
    ("last_3y", 3),
    ("last_1y", 1),
)


def _unavailable_window_summary(window: str, reason: str) -> dict[str, object]:
    return {
        "window": window,
        "start_date": "",
        "end_date": "",
        "days": 0,
        "final_nav": np.nan,
        "total_return_pct": np.nan,
        "annual_pct": np.nan,
        "max_drawdown_pct": np.nan,
        "sharpe": np.nan,
        "vol_pct": np.nan,
        "unavailable_reason": reason,
    }


def summarize_required_windows(ret: pd.Series) -> list[dict[str, object]]:
    clean = ret.dropna().astype(float)
    if clean.empty:
        raise ValueError("empty return series")
    end = pd.Timestamp(clean.index[-1])
    rows: list[dict[str, object]] = []
    for window, years in REQUIRED_PERFORMANCE_WINDOWS:
        if years is None:
            part = clean
            required_start = pd.Timestamp(clean.index[0])
        else:
            required_start = end - pd.DateOffset(years=int(years))
            part = clean.loc[clean.index >= required_start]
            if pd.Timestamp(clean.index[0]) > required_start:
                row = _unavailable_window_summary(
                    window,
                    f"insufficient history: actual_start={pd.Timestamp(clean.index[0]).date()}, "
                    f"required_start={required_start.date()}",
                )
                row["required_start_date"] = str(required_start.date())
                rows.append(row)
                continue
        if part.empty:
            rows.append(_unavailable_window_summary(window, "no data in requested window"))
            continue
        row = dict(summarize_returns(part))
        row["window"] = window
        row["required_start_date"] = str(required_start.date())
        row["unavailable_reason"] = ""
        rows.append(row)
    return rows


def summarize_yearly(ret: pd.Series) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for year, part in ret.groupby(ret.index.year):
        part = part.dropna()
        if part.empty:
            continue
        nav = (1.0 + part).cumprod()
        years = (part.index[-1] - part.index[0]).days / 365.25
        annual = nav.iloc[-1] ** (1.0 / years) - 1.0 if years > 0 and len(part) >= 60 else np.nan
        vol = part.std(ddof=1) * (TRADING_DAYS**0.5)
        drawdown = nav.div(nav.cummax().clip(lower=1.0)).sub(1.0)
        rows.append(
            {
                "year": str(year),
                "start_date": str(pd.Timestamp(part.index[0]).date()),
                "end_date": str(pd.Timestamp(part.index[-1]).date()),
                "days": int(len(part)),
                "return_pct": float((nav.iloc[-1] - 1.0) * 100.0),
                "max_drawdown_pct": float(drawdown.min() * 100.0),
                "sharpe": float(annual / vol) if vol > 0 and pd.notna(annual) else 0.0,
                "annual_pct": float(annual * 100.0) if pd.notna(annual) else np.nan,
            }
        )
    return pd.DataFrame(rows)


def build_performance_payload(
    ret: pd.Series,
    source_label: str = "costed_v2_3",
    output_paths: dict[str, Path] | None = None,
) -> dict[str, object]:
    ensure_output_dir()
    write_paths = output_paths or {
        "summary": PERF_SUMMARY_CSV,
        "yearly": PERF_YEARLY_CSV,
        "nav": PERF_NAV_CSV,
        "json": PERF_JSON,
        "png": PERF_PNG,
    }
    window_summaries = summarize_required_windows(ret)
    summary = dict(window_summaries[0])
    yearly_df = summarize_yearly(ret)
    nav_df = pd.DataFrame(
        {
            "date": ret.index,
            "return_net": ret.values,
            "nav_net": (1.0 + ret.fillna(0.0)).cumprod().values,
        }
    )
    _atomic_write_csv(yearly_df, write_paths["yearly"], index=False, encoding="utf-8-sig")
    _atomic_write_csv(nav_df, write_paths["nav"], index=False, encoding="utf-8-sig")
    _atomic_write_csv(pd.DataFrame(window_summaries), write_paths["summary"], index=False, encoding="utf-8-sig")
    plt.figure(figsize=(12, 6))
    plt.plot(nav_df["date"], nav_df["nav_net"], label="v2.3 nav_net")
    plt.title("Top100 Microcap Mom16 v2.3 Costed NAV")
    plt.xlabel("date")
    plt.ylabel("nav_net")
    plt.grid(True, alpha=0.3)
    plt.legend()
    plt.tight_layout()
    plt.savefig(write_paths["png"], dpi=150)
    plt.close()
    payload = {
        "source_label": source_label,
        "summary": summary,
        "windows": window_summaries,
        "outputs": {
            "summary_csv": str(PERF_SUMMARY_CSV),
            "yearly_csv": str(PERF_YEARLY_CSV),
            "nav_csv": str(PERF_NAV_CSV),
            "chart": str(PERF_PNG),
        },
    }
    _atomic_write_text(write_paths["json"], _json_dumps(payload), encoding="utf-8")
    return payload


def _build_signal_row(net_df: pd.DataFrame, reference_summary: dict[str, object]) -> pd.DataFrame:
    row = v2_0.overlay_mod._build_signal_row(net_df, reference_summary)
    row["version"] = VERSION
    row["strategy_version"] = f"v{VERSION}"
    row["base_version"] = "embedded_v2_base"
    row["overlay_type"] = STRATEGY_REVISION
    row["strategy_revision"] = STRATEGY_REVISION
    row["r2_gate_enabled"] = R2_ENTRY_GATE > 0
    row["signal_model"] = "spread_nav_log_wls_exp_halflife_2p5_lb25_r2off_signal1p0_exec0p8_vol10_overheat26_recovery20"
    row["signal_spread_hedge_ratio"] = SIGNAL_SPREAD_HEDGE_RATIO
    row["execution_hedge_ratio"] = EXECUTION_HEDGE_RATIO
    row["halflife"] = HALFLIFE
    row["lookback"] = LOOKBACK
    row["r2_window"] = R2_WINDOW
    row["r2_entry_gate"] = R2_ENTRY_GATE
    row["momentum_gap_entry_threshold"] = MOMENTUM_GAP_ENTRY_THRESHOLD
    row["momentum_gap_exit_buffer"] = MOMENTUM_GAP_EXIT_BUFFER
    row["signal_quality_derisk_enabled"] = False
    row["signal_score_label"] = "annualized_log_wls_score"
    row["schema_version"] = "log_wls_score_schema_v1"
    row["momentum_gap_deprecated"] = True
    row["momentum_gap_legacy_note"] = (
        "legacy field contains annualized spread-NAV log-WLS score, not plain microcap-minus-hedge momentum gap"
    )
    latest = net_df.iloc[-1]
    for col in [
        "annualized_log_wls_score",
        "log_wls_r2",
        "spread_nav",
        "overheat_feature_value",
        "actual_execution_scale",
    ]:
        if col in latest and pd.notna(latest[col]):
            row[col] = float(latest[col])
    row["overheat_kind"] = OVERHEAT_KIND
    row["overheat_enabled"] = True
    row["overheat_overlay_enabled"] = True
    row["overheat_window"] = OVERHEAT_FEATURE_WINDOW
    row["overheat_threshold"] = OVERHEAT_TRIGGER_THRESHOLD
    row["overheat_metric_name"] = "spread_nav_realized_vol"
    row["overheat_triggered"] = bool(latest.get("overheat_exit_triggered", False))
    row["overheat_require_positive_trade_return"] = False
    row["overheat_require_signal_reset"] = False
    row["overheat_feature_window"] = OVERHEAT_FEATURE_WINDOW
    row["overheat_trigger_threshold"] = OVERHEAT_TRIGGER_THRESHOLD
    row["overheat_recovery_ratio"] = OVERHEAT_RECOVERY_RATIO
    row["overheat_recovery_threshold"] = OVERHEAT_RECOVERY_THRESHOLD
    row["overheat_risk_off"] = bool(latest.get("overheat_risk_off", False))
    row["overheat_exit_triggered"] = bool(latest.get("overheat_exit_triggered", False))
    row["overheat_reentry_triggered"] = bool(latest.get("overheat_reentry_triggered", False))
    row["overheat_block_entry_triggered"] = bool(latest.get("overheat_block_entry_triggered", False))
    row["target_vol_enabled"] = TARGET_VOL_ENABLED
    row["target_vol"] = 0.0
    row["target_vol_window"] = 0
    row["target_vol_signal_timing"] = ""
    row["target_vol_max_leverage"] = 1.0
    row["max_leverage"] = 1.0
    row["target_vol_scale_rebalance_threshold"] = 0.0
    row["cash_day_yield"] = 0.0
    row["cash_day_yield_annual"] = 0.0
    row["cash_day_yield_enabled"] = CASH_DAY_YIELD_ENABLED
    row["financing_enabled"] = FINANCING_ENABLED
    _apply_fixed_exposure_next_session_cost_fields(row, latest, hedge_ratio=EXECUTION_HEDGE_RATIO)
    row["return_column_semantics"] = (
        "return equals return_net after LB25 R2-OFF signal, vol10 overheat defense, "
        "and base entry/exit/rebalance costs; no target-vol, cash-day yield, or financing overlay"
    )
    return row


def _close_df_from_base(base_gross_cached: pd.DataFrame) -> pd.DataFrame:
    return base_gross_cached[["microcap_close", "hedge_close"]].rename(
        columns={"microcap_close": "microcap", "hedge_close": "hedge"}
    ).sort_index()


def _official_v2_0_cache_key() -> str:
    _ensure_v2_0_contract_validated()
    payload = {
        "strategy": v2_0.current_strategy_fingerprint(),
        "data_state": v2_0.current_data_state_fingerprint(),
    }
    return json.dumps(
        _json_sanitize(payload),
        ensure_ascii=False,
        sort_keys=True,
        allow_nan=False,
        default=str,
    )


def _load_official_v2_0_out() -> pd.DataFrame:
    global _OFFICIAL_V2_0_OUT_CACHE
    cache_key = _official_v2_0_cache_key()
    force_refresh = bool(getattr(getattr(v2_0, "_V2_RUNTIME_ARGS", None), "force_refresh", False))
    with _OFFICIAL_V2_0_OUT_CACHE_LOCK:
        if force_refresh:
            _OFFICIAL_V2_0_OUT_CACHE = None
        elif _OFFICIAL_V2_0_OUT_CACHE is not None and _OFFICIAL_V2_0_OUT_CACHE[0] == cache_key:
            return _OFFICIAL_V2_0_OUT_CACHE[1]
        _, _, official_v2_0_out = v2_0.generate_v2_0_outputs()
        # Recompute after generation because panel shadow/base files may refresh
        # inside generate_v2_0_outputs(); cache the state subsequent calls will see.
        post_key = _official_v2_0_cache_key()
        if cache_key != post_key:
            warnings.warn(
                "v2.0 base fingerprint changed during generation; "
                "official v2.0 output cache is keyed to the post-generation state.",
                RuntimeWarning,
                stacklevel=2,
            )
        _OFFICIAL_V2_0_OUT_CACHE = (post_key, official_v2_0_out)
        return official_v2_0_out


def _load_realtime_v2_0_official_index() -> pd.DatetimeIndex:
    global _OFFICIAL_V2_0_OUT_CACHE
    cache_key = _official_v2_0_cache_key()
    force_refresh = bool(getattr(getattr(v2_0, "_V2_RUNTIME_ARGS", None), "force_refresh", False))
    with _OFFICIAL_V2_0_OUT_CACHE_LOCK:
        cached = _OFFICIAL_V2_0_OUT_CACHE
        if force_refresh:
            cached = None
            _OFFICIAL_V2_0_OUT_CACHE = None
    if cached is not None and cached[0] == cache_key:
        return pd.DatetimeIndex(cached[1].index).sort_values()
    costed_nav_csv = Path(getattr(v2_0, "COSTED_NAV_CSV", ""))
    if costed_nav_csv.exists():
        try:
            dates = pd.read_csv(costed_nav_csv, usecols=["date"], parse_dates=["date"], encoding="utf-8-sig")["date"]
            return pd.DatetimeIndex(dates).dropna().sort_values()
        except Exception:
            pass
    return pd.DatetimeIndex(_load_official_v2_0_out().index).sort_values()


def _build_realtime_v2_3_official_index(
    close_df: pd.DataFrame,
    meta: dict[str, object],
    official_index: pd.DatetimeIndex | pd.Index | None = None,
) -> pd.DatetimeIndex:
    del official_index
    anchor_text = str(meta.get("latest_anchor_trade_date") or "").strip()
    if not anchor_text:
        raise RuntimeError("v2.3 realtime metadata is missing latest_anchor_trade_date")
    anchor = pd.Timestamp(anchor_text).normalize()
    close_index = pd.DatetimeIndex(close_df.index).dropna().sort_values()
    history_index = close_index[close_index.normalize() <= anchor]
    if len(history_index) == 0 or pd.Timestamp(history_index[-1]).normalize() != anchor:
        raise RuntimeError(f"v2.3 validated realtime close history does not reach anchor {anchor.date()}")
    return pd.DatetimeIndex(history_index)


V2_3_REWRITE_AUDIT_KEY_COLUMNS = [
    "return_net",
    "holding",
    "next_holding",
    "base_holding",
    "base_next_holding",
    "base_pre_cost_return",
    "actual_execution_scale",
    "current_execution_scale",
    "next_session_actionable_scale",
    "base_trade_cost_scaled",
    "entry_exit_cost",
    "rebalance_cost",
    "total_cost",
    "annualized_log_wls_score",
    "log_wls_r2",
    "overheat_feature_value",
    "overheat_risk_off",
    "overheat_exit_triggered",
    "overheat_reentry_triggered",
    "overheat_block_entry_triggered",
]
V2_3_SIGNAL_AUDIT_ALLOWED_TAIL_ROWS = LOOKBACK + OVERHEAT_FEATURE_WINDOW + 5
V2_3_REWRITE_AUDIT_ALLOWED_TAIL_ROWS_BY_COLUMN = {
    "holding": V2_3_SIGNAL_AUDIT_ALLOWED_TAIL_ROWS,
    "next_holding": V2_3_SIGNAL_AUDIT_ALLOWED_TAIL_ROWS,
    "base_holding": V2_3_SIGNAL_AUDIT_ALLOWED_TAIL_ROWS,
    "base_next_holding": V2_3_SIGNAL_AUDIT_ALLOWED_TAIL_ROWS,
    "base_pre_cost_return": V2_3_SIGNAL_AUDIT_ALLOWED_TAIL_ROWS,
    "annualized_log_wls_score": V2_3_SIGNAL_AUDIT_ALLOWED_TAIL_ROWS,
    "log_wls_r2": V2_3_SIGNAL_AUDIT_ALLOWED_TAIL_ROWS,
    "overheat_feature_value": V2_3_SIGNAL_AUDIT_ALLOWED_TAIL_ROWS,
    "overheat_risk_off": V2_3_SIGNAL_AUDIT_ALLOWED_TAIL_ROWS,
    "overheat_exit_triggered": V2_3_SIGNAL_AUDIT_ALLOWED_TAIL_ROWS,
    "overheat_reentry_triggered": V2_3_SIGNAL_AUDIT_ALLOWED_TAIL_ROWS,
    "overheat_block_entry_triggered": V2_3_SIGNAL_AUDIT_ALLOWED_TAIL_ROWS,
}


def _v2_3_changed_columns(
    previous: pd.DataFrame,
    candidate: pd.DataFrame,
    key_columns: list[str],
    allowed_tail_rows: int,
    atol: float = 1e-9,
    rtol: float = 1e-7,
    column_allowed_tail_rows: dict[str, int] | None = None,
) -> dict[str, int]:
    prev = v2_0.base_mod._normalise_dated_frame(previous, "v2.3 previous diagnostic")
    cand = v2_0.base_mod._normalise_dated_frame(candidate, "v2.3 candidate diagnostic")
    changed: dict[str, int] = {}
    for col in key_columns:
        if col not in prev.columns or col not in cand.columns:
            continue
        col_tail_rows = max(0, int(allowed_tail_rows))
        if column_allowed_tail_rows is not None and col in column_allowed_tail_rows:
            col_tail_rows = max(0, int(column_allowed_tail_rows[col]))
        col_frozen_prev = prev.index[:-col_tail_rows] if col_tail_rows > 0 else prev.index
        if len(col_frozen_prev) == 0:
            continue
        frozen_common = col_frozen_prev.intersection(cand.index).sort_values()
        left = prev.loc[frozen_common, col]
        right = cand.loc[frozen_common, col]
        left_num = pd.to_numeric(left, errors="coerce").astype(float)
        right_num = pd.to_numeric(right, errors="coerce").astype(float)
        numeric_like = left_num.notna().any() or right_num.notna().any()
        if numeric_like:
            diff = (left_num - right_num).abs()
            threshold = float(atol) + float(rtol) * right_num.abs()
            mask = diff.gt(threshold) | (left_num.isna() ^ right_num.isna())
        else:
            mask = left.astype(str).ne(right.astype(str))
        count = int(mask.fillna(False).sum())
        if count:
            changed[col] = count
    return changed


def _v2_3_audit_change_summary(
    previous: pd.DataFrame,
    candidate: pd.DataFrame,
    audit_path: Path,
) -> dict[str, object]:
    counts = {"date_removed": 0, "date_added": 0, "missing_key_column": 0}
    if audit_path.exists():
        try:
            audit_df = pd.read_csv(audit_path)
            if "change_type" in audit_df.columns:
                value_counts = audit_df["change_type"].fillna("").astype(str).value_counts()
                for key in counts:
                    counts[key] = int(value_counts.get(key, 0))
        except Exception:
            pass
    prev = v2_0.base_mod._normalise_dated_frame(previous, "v2.3 previous diagnostic")
    cand = v2_0.base_mod._normalise_dated_frame(candidate, "v2.3 candidate diagnostic")
    prev_latest = prev.index.max() if len(prev.index) else pd.NaT
    cand_latest = cand.index.max() if len(cand.index) else pd.NaT
    latest_regressed = bool(pd.notna(prev_latest) and (pd.isna(cand_latest) or pd.Timestamp(cand_latest) < pd.Timestamp(prev_latest)))
    return {
        **counts,
        "latest_date_regressed": latest_regressed,
        "previous_latest_date": "" if pd.isna(prev_latest) else str(pd.Timestamp(prev_latest).date()),
        "candidate_latest_date": "" if pd.isna(cand_latest) else str(pd.Timestamp(cand_latest).date()),
        "previous_row_count": int(len(prev.index)),
        "candidate_row_count": int(len(cand.index)),
        "row_count_change": int(len(cand.index) - len(prev.index)),
    }


def _write_v2_3_rewrite_diagnostics(
    previous: pd.DataFrame,
    candidate: pd.DataFrame,
    allowed_tail_rows: int,
    audit_path: Path,
    column_allowed_tail_rows: dict[str, int] | None = None,
) -> Path:
    raw_input_cols = {
        "base_pre_cost_return",
        "annualized_log_wls_score",
        "log_wls_r2",
        "overheat_feature_value",
    }
    changed = _v2_3_changed_columns(
        previous,
        candidate,
        V2_3_REWRITE_AUDIT_KEY_COLUMNS,
        allowed_tail_rows,
        column_allowed_tail_rows=column_allowed_tail_rows,
    )
    audit_summary = _v2_3_audit_change_summary(previous, candidate, audit_path)
    changed_cols = set(changed)
    if audit_summary["date_removed"] or audit_summary["date_added"]:
        diagnosis = "date_set_changed"
    elif audit_summary["missing_key_column"]:
        diagnosis = "schema_changed"
    elif changed_cols & raw_input_cols:
        diagnosis = "raw_input_or_signal_changed"
    elif changed_cols:
        diagnosis = "threshold_path_dependent_state_changed"
    else:
        diagnosis = "no_key_column_change_detected"
    diagnostics = {
        "diagnosis": diagnosis,
        "allowed_tail_rows": int(allowed_tail_rows),
        "changed_columns": changed,
        **audit_summary,
        "raw_input_columns": sorted(raw_input_cols),
        "audit_csv": str(audit_path),
        "note": (
            "raw_input_or_signal_changed means upstream returns/costs/signal inputs changed on frozen dates; "
            "threshold_path_dependent_state_changed means frozen-date differences are confined to overheat "
            "path-dependent state, cost application, or derived returns and should be reviewed as path transmission."
        ),
    }
    diagnostics_path = OUTPUT_DIR / f"{OUTPUT_PREFIX}_historical_rewrite_diagnostics.json"
    _atomic_write_text(diagnostics_path, _json_dumps(diagnostics), encoding="utf-8")
    return diagnostics_path


def _v2_3_rewrite_allowed_tail_rows() -> int:
    return max(LOOKBACK + OVERHEAT_FEATURE_WINDOW + 20, 60)


def v2_3_rewrite_audit_matches_approved_lineage_migration(
    report_path: Path | None,
    previous: pd.DataFrame,
    candidate: pd.DataFrame,
    audit_path: Path,
) -> bool:
    if report_path is None or not Path(report_path).exists() or not audit_path.exists():
        return False
    try:
        report = json.loads(Path(report_path).read_text(encoding="utf-8"))
        resolved = v2_0._resolve_base_paths()
        proxy_meta_path = resolved.output_paths["proxy_meta"]
        prev = v2_0.base_mod._normalise_dated_frame(previous, "approved v2.3 migration previous")
        cand = v2_0.base_mod._normalise_dated_frame(candidate, "approved v2.3 migration candidate")
        expected = {
            "schema_version": 1,
            "version": VERSION,
            "approved": True,
            "previous_costed_nav_sha256": v2_0.overlay_mod._sha256_path(COSTED_NAV_CSV),
            "candidate_frame_sha256": v2_0.overlay_mod._candidate_frame_sha256(candidate),
            "v2_0_costed_nav_sha256": v2_0.overlay_mod._sha256_path(v2_0.COSTED_NAV_CSV),
            "base_proxy_meta_sha256": v2_0.overlay_mod._sha256_path(proxy_meta_path),
            "rewrite_audit_sha256": v2_0.overlay_mod._sha256_path(audit_path),
            "previous_row_count": int(len(prev)),
            "candidate_row_count": int(len(cand)),
            "previous_latest_date": str(pd.Timestamp(prev.index.max()).date()),
            "candidate_latest_date": str(pd.Timestamp(cand.index.max()).date()),
            "new_member_st_violations": 0,
            "new_member_bad_policy_count": 0,
            "proxy_meta_matches_current_cache": True,
        }
    except Exception:
        return False
    return all(report.get(key) == value for key, value in expected.items())


def _write_v2_3_lineage_migration_diagnostics(report_path: Path, audit_path: Path) -> Path:
    report = json.loads(Path(report_path).read_text(encoding="utf-8"))
    diagnostics = {
        "diagnosis": "audited_st_metadata_and_historical_universe_lineage_migration",
        "version": VERSION,
        "allowed": True,
        "migration_report": str(Path(report_path)),
        "audit_csv": str(audit_path),
        "previous_costed_nav_sha256": report["previous_costed_nav_sha256"],
        "candidate_frame_sha256": report["candidate_frame_sha256"],
        "v2_0_costed_nav_sha256": report["v2_0_costed_nav_sha256"],
        "base_proxy_meta_sha256": report["base_proxy_meta_sha256"],
        "rewrite_audit_sha256": report["rewrite_audit_sha256"],
        "note": "One-time exact-hash v2.3 migration after the audited v2.0 ST and historical-universe repair.",
    }
    diagnostics_path = OUTPUT_DIR / f"{OUTPUT_PREFIX}_historical_rewrite_diagnostics.json"
    _atomic_write_text(diagnostics_path, _json_dumps(diagnostics), encoding="utf-8")
    return diagnostics_path


def strategy_promotion_evidence(previous_path: Path, candidate: pd.DataFrame, audit_path: Path) -> dict[str, object]:
    """Exact parameter migration, separate from historical security-lineage repair."""
    evidence = dict(v2_0.overlay_mod.strategy_promotion_evidence(previous_path, candidate, audit_path))
    evidence.update(
        authorization="user_replace_existing_v2_3",
        strategy_revision=STRATEGY_REVISION,
        source_sha256_lf=hashlib.sha256(Path(__file__).read_bytes().replace(b"\r\n", b"\n")).hexdigest(),
        v2_0_source_sha256_lf=hashlib.sha256(Path(v2_0.__file__).read_bytes().replace(b"\r\n", b"\n")).hexdigest(),
        v2_0_costed_nav_sha256=v2_0.overlay_mod._sha256_path(v2_0.COSTED_NAV_CSV),
    )
    return evidence


def strategy_promotion_matches(report_path: Path | None, previous_path: Path,
                               candidate: pd.DataFrame, audit_path: Path) -> bool:
    if report_path is None:
        return False
    try:
        report = json.loads(Path(report_path).read_text(encoding="utf-8"))
        return report.get("approved") is True and all(
            report.get(key) == value
            for key, value in strategy_promotion_evidence(previous_path, candidate, audit_path).items()
        )
    except (OSError, ValueError, TypeError, KeyError, AttributeError):
        return False


def _generate_v2_3_outputs_unlocked() -> tuple[dict[str, object], pd.DataFrame, pd.DataFrame]:
    ensure_output_dir()
    official_v2_0_out = _load_official_v2_0_out()
    reference_summary, base_gross_cached, turnover_df = v2_0.embedded_context._load_embedded_base_context()
    stale_outputs = incompatible_v2_3_outputs()
    close_df = _close_df_from_base(base_gross_cached)
    common_index = build_v2_3_common_index(close_df, official_v2_0_out.index)
    out = build_v2_3_result(close_df, turnover_df, common_index)
    mismatch_diagnostics = build_signal_execution_mismatch_diagnostics(close_df, out)
    rewrite_audit_status: dict[str, object] = {"status": "not_checked", "reason": "no_previous_costed_nav"}
    previous_path = COSTED_NAV_CSV
    if not previous_path.exists() and previous_path.name == DEFAULT_COSTED_NAV_CSV.name:
        previous_path = COSTED_NAV_CSV.parent / PREVIOUS_COSTED_NAV_CSV.name
    if previous_path.exists():
        previous = _read_costed_nav_csv(previous_path, parse_dates=["date"])
        audit_path = OUTPUT_DIR / f"{OUTPUT_PREFIX}_historical_rewrite_audit.csv"
        allowed_tail_rows = _v2_3_rewrite_allowed_tail_rows()
        candidate = out.rename_axis("date").reset_index()
        try:
            v2_0.base_mod.assert_no_historical_rewrite(
                previous=previous,
                candidate=candidate,
                key_columns=V2_3_REWRITE_AUDIT_KEY_COLUMNS,
                allowed_tail_rows=allowed_tail_rows,
                label="v2.3 official costed NAV",
                audit_path=audit_path,
                column_allowed_tail_rows=V2_3_REWRITE_AUDIT_ALLOWED_TAIL_ROWS_BY_COLUMN,
            )
            rewrite_audit_status = {"status": "clean", "audit_csv": None}
        except RuntimeError as exc:
            report_path = getattr(_ACTIVE_RUNTIME_ARGS, "audited_history_migration_report", None)
            if strategy_promotion_matches(
                getattr(_ACTIVE_RUNTIME_ARGS, "audited_strategy_migration_report", None),
                previous_path, candidate, audit_path,
            ):
                rewrite_audit_status = {
                    "status": "audited_exact_hash_strategy_promotion",
                    "strategy_revision": STRATEGY_REVISION,
                    "migration_report": str(_ACTIVE_RUNTIME_ARGS.audited_strategy_migration_report),
                    "audit_csv": str(audit_path),
                }
            elif v2_3_rewrite_audit_matches_approved_lineage_migration(
                report_path,
                previous,
                candidate,
                audit_path,
            ):
                diagnostics_path = _write_v2_3_lineage_migration_diagnostics(Path(report_path), audit_path)
                rewrite_audit_status = {
                    "status": "audited_exact_hash_lineage_migration",
                    "diagnostics_json": str(diagnostics_path),
                    "audit_csv": str(audit_path),
                    "migration_report": str(report_path),
                }
            else:
                try:
                    diagnostics_path = _write_v2_3_rewrite_diagnostics(
                        previous=previous,
                        candidate=candidate,
                        allowed_tail_rows=allowed_tail_rows,
                        audit_path=audit_path,
                        column_allowed_tail_rows=V2_3_REWRITE_AUDIT_ALLOWED_TAIL_ROWS_BY_COLUMN,
                    )
                except Exception as diag_exc:
                    raise RuntimeError(
                        f"{exc} v2.3 rewrite diagnostics failed: {type(diag_exc).__name__}: {diag_exc}"
                    ) from exc
                raise RuntimeError(f"{exc} v2.3 rewrite diagnostics written to {diagnostics_path}.") from exc

    freshness_proof = v2_0.assert_top100_candidate_fresh(
        out.index,
        expected_latest_date=out.index.max(),
        label="v2.3 official costed NAV",
    )
    bundle_targets = [
        COSTED_NAV_CSV, NAV_CSV, LATEST_SIGNAL_CSV, PERF_SUMMARY_CSV, PERF_YEARLY_CSV,
        PERF_NAV_CSV, PERF_JSON, PERF_PNG, SUMMARY_JSON,
    ]
    stage_scope = tempfile.TemporaryDirectory(prefix=f".{OUTPUT_PREFIX}.stage.", dir=OUTPUT_DIR)
    stage_root = Path(stage_scope.name)
    staged_files = {
        target: stage_root / f"{position:02d}{target.suffix}"
        for position, target in enumerate(bundle_targets)
    }
    _atomic_write_csv(out, staged_files[COSTED_NAV_CSV], index_label="date", encoding="utf-8-sig")
    _atomic_write_csv(out.rename_axis("date").reset_index(), staged_files[NAV_CSV], index=False, encoding="utf-8-sig")
    data_lineage = v2_0.overlay_mod._build_v2_data_lineage()
    performance_source_label = v2_0.overlay_mod.proxy_aware_performance_source_label(data_lineage, "costed_v2_3")
    signal_row = _build_signal_row(out, reference_summary)
    signal_row = v2_0.overlay_mod.augment_close_confirmed_signal_with_member_contract(
        signal_row,
        turnover_df,
        out.index,
        proxy_members_path=v2_0._resolve_base_paths().output_paths["proxy_members"],
    )
    apply_signal_execution_mismatch_columns(signal_row, mismatch_diagnostics)
    signal_row["microcap_series_source"] = data_lineage.get("source_used")
    signal_row["official_wind_series"] = bool(data_lineage.get("official_wind_series"))
    signal_row["proxy_warning"] = data_lineage.get("public_proxy_note", "")
    _atomic_write_text(staged_files[LATEST_SIGNAL_CSV], signal_row.to_csv(index=False), encoding="utf-8")
    perf_payload = build_performance_payload(
        out["return_net"].fillna(0.0),
        source_label=performance_source_label,
        output_paths={
            "summary": staged_files[PERF_SUMMARY_CSV], "yearly": staged_files[PERF_YEARLY_CSV],
            "nav": staged_files[PERF_NAV_CSV], "json": staged_files[PERF_JSON], "png": staged_files[PERF_PNG],
        },
    )

    summary = copy.deepcopy(reference_summary)
    summary["strategy"] = OUTPUT_PREFIX
    summary["version"] = VERSION
    summary["strategy_revision"] = STRATEGY_REVISION
    summary["version_role"] = EXPECTED_VERSION_ROLE
    summary["version_note"] = (
        "Formal v2.3 spread-NAV log-WLS LB25 vol10 overheat defense. Uses exp half-life 2.5 weighted log slope on "
        "25 trading days of always-on 1.0x hedged signal spread NAV, R2 entry filter OFF, executes with "
        "0.8x CSI1000 hedge, exits when score falls below -8%, applies close-executed vol10 overheat defense at "
        "26% annualized realized volatility with 20% recovery threshold, and has no target-vol, cash-day-yield, "
        "financing, peak-decay, static NAV defense, or CSI2000 volume filter."
    )
    summary.setdefault("core_params", {})
    summary["core_params"]["fixed_hedge_ratio"] = BASE_HEDGE_RATIO
    summary["core_params"]["signal_spread_hedge_ratio"] = SIGNAL_SPREAD_HEDGE_RATIO
    summary["core_params"]["execution_hedge_ratio"] = EXECUTION_HEDGE_RATIO
    summary["core_params"]["signal_model"] = {
        "type": "spread_nav_log_wls_exp",
        "lookback": LOOKBACK,
        "halflife": HALFLIFE,
        "weights_oldest_to_newest": list(exp_weights()),
        "score_definition": "annualized weighted log slope of always-on 1.0x hedged signal spread NAV",
        "nav_csv_momentum_gap_column_alias_note": (
            "momentum_gap stores annualized_log_wls_score for v2.0 compatibility, not raw microcap minus hedge gap"
        ),
        "r2_window": R2_WINDOW,
        "r2_entry_gate": R2_ENTRY_GATE,
        "legacy_momentum_gap_field": "same value as annualized_log_wls_score for v2.0 compatibility",
    }
    summary["core_params"]["momentum_gap_entry_threshold"] = MOMENTUM_GAP_ENTRY_THRESHOLD
    summary["core_params"]["momentum_gap_exit_buffer"] = MOMENTUM_GAP_EXIT_BUFFER
    summary["core_params"]["signal_quality_derisk"] = {"enabled": False, "type": "removed_no_peak_decay"}
    summary["core_params"]["overheat_defense"] = {
        "enabled": True,
        "kind": OVERHEAT_KIND,
        "feature_window": OVERHEAT_FEATURE_WINDOW,
        "feature_definition": "annualized rolling standard deviation of always-on 1.0x hedged spread NAV returns",
        "trigger_threshold": OVERHEAT_TRIGGER_THRESHOLD,
        "recovery_ratio": OVERHEAT_RECOVERY_RATIO,
        "recovery_threshold": OVERHEAT_RECOVERY_THRESHOLD,
        "execution": "close-executed risk-off; cash days have zero return before costs",
    }
    summary["core_params"]["target_volatility_scaling"] = {"enabled": TARGET_VOL_ENABLED}
    summary["core_params"]["cash_day_yield"] = {"enabled": CASH_DAY_YIELD_ENABLED}
    summary["core_params"]["financing"] = {"enabled": FINANCING_ENABLED}
    summary["core_params"]["static_nav_defense"] = {"enabled": False}
    summary["core_params"]["csi2000_volume_filter"] = {"enabled": False}
    summary["core_params"]["signal_execution_mismatch_diagnostics"] = mismatch_diagnostics
    summary["latest_trade_date"] = str(pd.Timestamp(signal_row.iloc[0]["date"]).date())
    summary["latest_nav_date"] = str(pd.Timestamp(out.index.max()).date())
    summary["latest_signal"] = signal_row.iloc[0].drop(labels=["date"], errors="ignore").to_dict()
    summary["data_lineage"] = data_lineage
    summary["data_freshness_proof"] = freshness_proof
    summary["historical_rewrite_audit"] = rewrite_audit_status
    v2_0.overlay_mod.attach_proxy_source_summary_fields(
        summary,
        data_lineage,
        source_label="costed_v2_3",
        parameter_retest_status={
            "required_before_parameter_scan": True,
            "reason": "post-P0 proxy lineage changed from current-universe/current-ST to historical security master/historical-ST",
            "recommended_windows": ["full", "10Y", "5Y", "3Y", "1Y"],
        },
    )
    summary["performance_snapshot"] = perf_payload["summary"]
    summary["base_fingerprint"] = current_base_fingerprint()
    summary["synthetic_basket_execution"] = True
    summary["execution_model"] = {
        "synthetic_basket_execution": True,
        "member_level_fill_engine": False,
        "note": "Strategy-level basket entry/exit and configured aggregate costs; no member-level fill simulation.",
    }
    _atomic_write_text(staged_files[SUMMARY_JSON], _json_dumps(summary), encoding="utf-8")
    with v2_0.staged_output_bundle(
        bundle_targets,
        summary_path=SUMMARY_JSON,
        post_promotion_validator=lambda: v2_0.assert_top100_outputs_fresh(
            expected_latest_date=out.index.max(),
            extra_daily_paths={"v2_3_costed_nav": COSTED_NAV_CSV},
        ),
    ) as promotion_paths:
        for target, source in staged_files.items():
            shutil.copy2(source, promotion_paths[target])
    stage_scope.cleanup()
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
    for path in _stale_outputs_to_remove_after_generate(stale_outputs, regenerated_outputs):
        path.unlink(missing_ok=True)
    return summary, signal_row, out


def generate_v2_3_outputs() -> tuple[dict[str, object], pd.DataFrame, pd.DataFrame]:
    _ensure_v2_0_contract_validated()
    with v2_3_output_lock():
        return _generate_v2_3_outputs_unlocked()


def _build_realtime_v2_3_outputs_unlocked() -> tuple[pd.DataFrame, dict[str, object], pd.DataFrame]:
    ensure_output_dir()
    realtime_base = v2_0.realtime_core.load_realtime_base()
    close_df = realtime_base.realtime_close_df[["microcap", "hedge"]].sort_index()
    freshness_calendar = _build_realtime_v2_3_official_index(close_df, realtime_base.meta)
    signal_official_index = freshness_calendar
    if bool(realtime_base.meta.get("snapshot_row_appended", False)) and len(close_df.index):
        signal_official_index = signal_official_index.union(pd.DatetimeIndex([close_df.index[-1]])).sort_values()
    common_index = build_v2_3_common_index(close_df, signal_official_index)
    gross = build_spread_log_wls_gross(close_df, common_index)
    is_snapshot = bool(realtime_base.meta.get("snapshot_row_appended", False))
    signal_timing = "intraday_hypothetical_if_now_close" if is_snapshot else "close_confirmed_anchor"
    out = apply_overheat_defense(gross, realtime_base.turnover_df)
    mismatch_diagnostics = build_signal_execution_mismatch_diagnostics(close_df, out)
    signal_row = _build_signal_row(out, realtime_base.reference_summary)
    apply_signal_execution_mismatch_columns(signal_row, mismatch_diagnostics)
    signal_row = v2_0.realtime_core.base_mod.augment_realtime_signal_with_member_rebalance(
        signal_row,
        realtime_base.context.get("changes_df"),
        latest_rebalance=realtime_base.context.get("latest_rebalance"),
        latest_anchor_trade_date=realtime_base.meta.get("latest_anchor_trade_date"),
        quote_trade_date=realtime_base.meta.get("quote_trade_date"),
    )
    v2_0.overlay_mod._apply_realtime_meta_columns_to_signal_row(signal_row, realtime_base.meta)
    signal_row["quote_coverage"] = f"{realtime_base.meta.get('member_price_count', 0)}/{realtime_base.meta.get('member_count', 0)}"
    signal_row["target_vol_signal_timing"] = ""
    signal_row["signal_timing"] = signal_timing
    signal_row["official_close_confirmed_signal"] = not is_snapshot
    _atomic_write_text(REALTIME_SIGNAL_CSV, signal_row.to_csv(index=False), encoding="utf-8")
    return signal_row, realtime_base.meta, out


def build_realtime_v2_3_outputs() -> tuple[pd.DataFrame, dict[str, object], pd.DataFrame]:
    _ensure_v2_0_contract_validated()
    with v2_3_realtime_output_lock():
        return _build_realtime_v2_3_outputs_unlocked()


def _print_scale_fields(row: pd.Series, include_frozen: bool = False) -> None:
    print(f"current_execution_scale: {_safe_float(row.get('current_execution_scale', row.get('execution_scale')), 0.0):.2f}")
    print(f"next_session_target_scale: {_safe_float(row.get('next_session_target_scale'), 0.0):.2f}")
    print(f"next_session_actionable_scale: {_safe_float(row.get('next_session_actionable_scale'), 0.0):.2f}")
    print(f"raw_scale_delta: {_safe_float(row.get('raw_scale_delta'), 0.0):+.2f}")
    print(f"actionable_scale_delta: {_safe_float(row.get('actionable_scale_delta'), 0.0):+.2f}")
    print(f"scale_delta: {_safe_float(row.get('scale_delta'), 0.0):+.2f}")
    print(f"next_session_turnover: {_safe_float(row.get('next_session_turnover'), 0.0):.4f}")
    print(f"next_session_leg_turnover: {_safe_float(row.get('next_session_leg_turnover'), 0.0):.4f}")
    print(f"next_session_trade_cost_est: {_safe_float(row.get('next_session_trade_cost_est'), 0.0):.4%}")
    print(f"overheat_feature_value: {_safe_float(row.get('overheat_feature_value'), 0.0):.4%}")
    print(f"overheat_risk_off: {bool(row.get('overheat_risk_off', False))}")


def _print_signal_query() -> None:
    _, signal_df, _ = generate_v2_3_outputs()
    row = signal_df.iloc[0]
    print("signal")
    print("strategy_version: v2.3")
    print("base_version: embedded_v2_base")
    print(
        "signal_model: spread-NAV log-WLS exp half-life 2.5, lookback 25, "
        "R2 entry filter OFF, signal spread 1.0x, execution hedge 0.8x"
    )
    print(
        f"overlay: entry score > {MOMENTUM_GAP_ENTRY_THRESHOLD:.2f}, exit buffer {MOMENTUM_GAP_EXIT_BUFFER:.2f}, "
        f"vol10 overheat trigger {OVERHEAT_TRIGGER_THRESHOLD:.0%}, recovery {OVERHEAT_RECOVERY_THRESHOLD:.1%}, "
        "no target-vol/cash-yield/financing"
    )
    print(f"current_holding: {row['current_holding']}")
    print(f"next_holding: {row['next_holding']}")
    print(f"trade_state: {row.get('effective_trade_state', row.get('trade_state', 'hold'))}")
    print(f"holding_trade_state: {row.get('holding_trade_state', row.get('momentum_trade_state', 'hold'))}")
    print(f"scale_trade_state: {row.get('scale_trade_state', 'hold_scale')}")
    print(f"signal_date: {pd.Timestamp(row['date']).strftime('%Y-%m-%d')}")
    print(f"annualized_log_wls_score: {float(row.get('annualized_log_wls_score', row.get('momentum_gap', 0.0))):+.4%}")
    print(f"log_wls_r2: {float(row.get('log_wls_r2', 0.0)):.4f}")
    print("momentum_gap_legacy_note: legacy field is the annualized log-WLS score, not plain gap")
    _print_scale_fields(row, include_frozen=False)
    print(f"official_close_confirmed_signal: {row.get('official_close_confirmed_signal', True)}")
    print(SUMMARY_JSON)
    print(LATEST_SIGNAL_CSV)


def _print_realtime_signal_query() -> None:
    def emit() -> None:
        signal_df, meta, _ = build_realtime_v2_3_outputs()
        row = signal_df.iloc[0]
        print("realtime_signal")
        print("strategy_version: v2.3")
        print("base_version: embedded_v2_base")
        print(
            "signal_model: spread-NAV log-WLS exp half-life 2.5, lookback 25, "
            "R2 entry filter OFF, signal spread 1.0x, execution hedge 0.8x"
        )
        print(
            f"overlay: entry score > {MOMENTUM_GAP_ENTRY_THRESHOLD:.2f}, exit buffer {MOMENTUM_GAP_EXIT_BUFFER:.2f}, "
            f"vol10 overheat trigger {OVERHEAT_TRIGGER_THRESHOLD:.0%}, recovery {OVERHEAT_RECOVERY_THRESHOLD:.1%}, "
            "no target-vol/cash-yield/financing"
        )
        print(f"snapshot_time: {meta.get('snapshot_time')}")
        print(f"latest_anchor_trade_date: {meta.get('latest_anchor_trade_date')}")
        print(f"expected_latest_completed_trade_date: {meta.get('expected_latest_completed_trade_date', '')}")
        print(f"quote_trade_date: {meta.get('quote_trade_date', '')}")
        print(f"current_holding: {row['current_holding']}")
        print(f"next_holding: {row['next_holding']}")
        print(f"trade_state: {row.get('effective_trade_state', row.get('trade_state', 'hold'))}")
        print(f"holding_trade_state: {row.get('holding_trade_state', row.get('momentum_trade_state', 'hold'))}")
        print(f"scale_trade_state: {row.get('scale_trade_state', 'hold_scale')}")
        print(f"signal_timing: {row.get('signal_timing', '')}")
        _print_scale_fields(row, include_frozen=True)
        print(f"official_close_confirmed_signal: {row.get('official_close_confirmed_signal', False)}")
        print(f"snapshot_row_appended: {bool(meta.get('snapshot_row_appended', False))}")
        print(f"member_quote_flat_fallback_count: {int(meta.get('member_quote_flat_fallback_count') or 0)}")
        print(f"from_cache: {bool(meta.get('from_cache', False))}")
        print(f"cache_age_seconds: {_safe_float(meta.get('cache_age_seconds'), 0.0):.1f}")
        print(f"fallback_warning: {meta.get('fallback_warning', '')}")
        print(f"annualized_log_wls_score: {float(row.get('annualized_log_wls_score', row.get('momentum_gap', 0.0))):+.4%}")
        print(f"log_wls_r2: {float(row.get('log_wls_r2', 0.0)):.4f}")
        print("momentum_gap_legacy_note: legacy field is the annualized log-WLS score, not plain gap")
        print(f"quote_source: {meta.get('quote_source')}")
        print(f"hedge_quote_source: {meta.get('hedge_quote_source')}")
        print(f"quote_coverage: {meta.get('member_price_count')}/{meta.get('member_count')}")
        print(REALTIME_SIGNAL_CSV)

    try:
        v2_0.run_realtime_query_with_fresh_state(emit)
    except Exception as exc:
        if v2_0.is_realtime_actionability_error(exc):
            v2_0.print_realtime_blocked_result("v2.3", exc)
            return
        raise


def _print_performance_query(query: str) -> None:
    with v2_3_output_lock():
        _summary, _signal_row, perf_df = _generate_v2_3_outputs_unlocked()
        perf_df = perf_df.rename_axis("date").sort_index()
        old_title = v2_0.embedded_context.base_mod.STRATEGY_TITLE
        v2_0.embedded_context.base_mod.STRATEGY_TITLE = "Top100 Microcap Mom16 Biweekly v2.3"
        try:
            v2_0.embedded_context.base_mod.build_performance_outputs(
                perf_df=perf_df,
                ret_col="return_net",
                nav_col="nav_net",
                source_label="costed_v2_3",
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
            v2_0.embedded_context.base_mod.STRATEGY_TITLE = old_title
    print(PERF_QUERY_PNG)
    print(PERF_QUERY_SUMMARY_CSV)
    print(PERF_QUERY_YEARLY_CSV)
    print(PERF_QUERY_NAV_CSV)
    print(PERF_QUERY_JSON)


def normalize_v2_3_query_text(query: str) -> str:
    text = str(query or "").strip()
    embedded_context = getattr(v2_0, "embedded_context", None)
    base_mod = getattr(embedded_context, "base_mod", None)
    base_normalizer = getattr(base_mod, "normalize_query_text", None)
    if base_normalizer is None:
        base_normalizer = getattr(v2_0, "normalize_query_text", None)
    if callable(base_normalizer):
        text = base_normalizer(text)
    text = re.sub(r"[\s?？!！。．.]+$", "", text).strip()
    compact = re.sub(r"\s+", "", text)
    ascii_key = re.sub(r"[\s-]+", "_", text.lower())
    if compact in {"信号", "信號"} or ascii_key == "signal":
        return "信号"
    if compact in {"实时信号", "實時信號"} or ascii_key in {"realtime_signal", "live_signal"}:
        return "实时信号"
    return text


def _handle_query(query: str) -> None:
    normalized = normalize_v2_3_query_text(query)
    if normalized == "信号":
        _print_signal_query()
        return
    if normalized == "实时信号":
        _print_realtime_signal_query()
        return
    if v2_0.embedded_context.base_mod.PERFORMANCE_PATTERN.search(query) or (
        normalized != query and v2_0.embedded_context.base_mod.PERFORMANCE_PATTERN.search(normalized)
    ):
        _print_performance_query(query)
        return
    raise ValueError("v2.3 supports: 信号 / 实时信号 / 表现 <区间>")


def main(argv: list[str] | None = None) -> None:
    global _ACTIVE_RUNTIME_ARGS
    args = parse_v2_3_args(sys.argv[1:] if argv is None else argv)
    previous_active_runtime_args = _ACTIVE_RUNTIME_ARGS
    previous_runtime_args = v2_0._V2_RUNTIME_ARGS
    previous_output_prefix = OUTPUT_PREFIX
    previous_costed_nav_csv = COSTED_NAV_CSV
    previous_v2_0_output_prefix = v2_0.OUTPUT_PREFIX
    try:
        configure_runtime(args)
        query = " ".join(args.query_tokens).strip()
        if query:
            _handle_query(query)
            return
        generate_v2_3_outputs()
        print(str(SUMMARY_JSON))
        print(str(LATEST_SIGNAL_CSV))
        print(str(COSTED_NAV_CSV))
    finally:
        _ACTIVE_RUNTIME_ARGS = previous_active_runtime_args
        configure_output_paths(previous_output_prefix, previous_costed_nav_csv)
        v2_0.configure_output_paths(previous_v2_0_output_prefix)
        v2_0._V2_RUNTIME_ARGS = previous_runtime_args

if __name__ == "__main__":
    main()
