from __future__ import annotations

import argparse
import copy
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

REQUIRED_BASE_VERSION = "2.0"
MIN_V2_0_BASE_API_REVISION = 12
MIN_V2_0_HISTORICAL_AUDIT_REVISION = 5
MIN_V2_0_DATA_STATE_FINGERPRINT_REVISION = 2
MIN_V2_0_REALTIME_CALENDAR_GUARD_REVISION = 3
EXPECTED_V2_0_TARGET_VOL_WINDOW = 75


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
    _require_v2_0_attr(v2_0, "_V2_RUNTIME_ARGS", "_V2_RUNTIME_ARGS")
    _require_v2_0_attr(v2_0, "BASE_API_REVISION", "BASE_API_REVISION")
    _require_v2_0_attr(v2_0, "HISTORICAL_AUDIT_REVISION", "HISTORICAL_AUDIT_REVISION")
    _require_v2_0_attr(v2_0, "DATA_STATE_FINGERPRINT_REVISION", "DATA_STATE_FINGERPRINT_REVISION")
    _require_v2_0_attr(v2_0, "REALTIME_CALENDAR_GUARD_REVISION", "REALTIME_CALENDAR_GUARD_REVISION")
    _require_v2_0_attr(v2_0, "DEFAULT_REALTIME_CACHE_SECONDS", "DEFAULT_REALTIME_CACHE_SECONDS")
    _require_v2_0_attr(v2_0, "DEFAULT_V2_LOCK_WAIT_SECONDS", "DEFAULT_V2_LOCK_WAIT_SECONDS")
    _require_v2_0_attr(v2_0, "DEFAULT_V2_STALE_LOCK_SECONDS", "DEFAULT_V2_STALE_LOCK_SECONDS")
    _require_v2_0_callable(v2_0, "_v2_file_lock", "_v2_file_lock")
    _require_v2_0_callable(v2_0, "generate_v2_0_outputs", "generate_v2_0_outputs")
    _require_v2_0_callable(v2_0, "current_base_fingerprint", "current_base_fingerprint")
    _require_v2_0_callable(v2_0, "current_strategy_fingerprint", "current_strategy_fingerprint")
    _require_v2_0_callable(v2_0, "current_data_state_fingerprint", "current_data_state_fingerprint")
    _require_v2_0_callable(v2_0, "assert_top100_outputs_fresh", "assert_top100_outputs_fresh")
    _require_v2_0_callable(v2_0, "current_runtime_fingerprint", "current_runtime_fingerprint")
    _require_v2_0_callable(v2_0, "run_realtime_query_with_fresh_state", "run_realtime_query_with_fresh_state")
    _require_v2_0_attr(embedded_context, "base_mod", "embedded_context.base_mod")
    _require_v2_0_callable(embedded_context, "_load_embedded_base_context", "embedded_context._load_embedded_base_context")
    _require_v2_0_callable(embedded_context, "current_base_fingerprint", "embedded_context.current_base_fingerprint")
    _require_v2_0_callable(realtime_core, "load_realtime_base", "realtime_core.load_realtime_base")
    _require_v2_0_attr(realtime_core, "base_mod", "realtime_core.base_mod")
    freq_mod = _require_v2_0_attr(base_mod, "freq_mod", "base_mod.freq_mod")
    cost_mod = _require_v2_0_attr(freq_mod, "cost_mod", "base_mod.freq_mod.cost_mod")
    _require_v2_0_callable(cost_mod, "apply_cost_model", "base_mod.freq_mod.cost_mod.apply_cost_model")
    _require_v2_0_attr(cost_mod, "ENTRY_COST", "base_mod.freq_mod.cost_mod.ENTRY_COST")
    _require_v2_0_attr(cost_mod, "EXIT_COST", "base_mod.freq_mod.cost_mod.EXIT_COST")
    _require_v2_0_callable(base_mod, "assert_no_historical_rewrite", "base_mod.assert_no_historical_rewrite")
    _require_v2_0_callable(base_mod, "_normalise_dated_frame", "base_mod._normalise_dated_frame")
    _require_v2_0_callable(base_mod, "augment_signal_with_member_rebalance", "base_mod.augment_signal_with_member_rebalance")
    _require_v2_0_callable(base_mod, "build_performance_outputs", "base_mod.build_performance_outputs")
    _require_v2_0_attr(base_mod, "STRATEGY_TITLE", "base_mod.STRATEGY_TITLE")
    _require_v2_0_attr(base_mod, "PERFORMANCE_PATTERN", "base_mod.PERFORMANCE_PATTERN")
    overlay_mod = _require_v2_0_attr(v2_0, "overlay_mod", "overlay_mod")
    _require_v2_0_callable(overlay_mod, "_build_signal_row", "overlay_mod._build_signal_row")
    _require_v2_0_callable(overlay_mod, "_build_v2_data_lineage", "overlay_mod._build_v2_data_lineage")
    _require_v2_0_callable(
        overlay_mod,
        "_apply_realtime_meta_columns_to_signal_row",
        "overlay_mod._apply_realtime_meta_columns_to_signal_row",
    )
    _require_v2_0_attr(overlay_mod, "TARGET_VOL_TRADING_DAYS", "overlay_mod.TARGET_VOL_TRADING_DAYS")
    expected_constants = {
        "overlay_mod.TARGET_VOL_MIN_LEVERAGE": (float(overlay_mod.TARGET_VOL_MIN_LEVERAGE), TARGET_VOL_MIN_LEVERAGE),
        "overlay_mod.TARGET_VOL_WINDOW": (int(overlay_mod.TARGET_VOL_WINDOW), EXPECTED_V2_0_TARGET_VOL_WINDOW),
        "base_mod.freq_mod.cost_mod.ENTRY_COST": (float(cost_mod.ENTRY_COST), TARGET_VOL_SCALE_CHANGE_ENTRY_COST),
        "base_mod.freq_mod.cost_mod.EXIT_COST": (float(cost_mod.EXIT_COST), TARGET_VOL_SCALE_CHANGE_EXIT_COST),
        "overlay_mod.TARGET_VOL_FINANCING_RATE": (float(overlay_mod.TARGET_VOL_FINANCING_RATE), TARGET_VOL_FINANCING_RATE),
        "overlay_mod.IDLE_CASH_YIELD": (float(overlay_mod.IDLE_CASH_YIELD), IDLE_CASH_YIELD),
        "overlay_mod.TARGET_VOL_TRADING_DAYS": (int(overlay_mod.TARGET_VOL_TRADING_DAYS), TRADING_DAYS),
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


ROOT = Path(__file__).resolve().parent
OUTPUT_DIR = ROOT / "outputs"

OUTPUT_PREFIX = "microcap_top100_mom16_biweekly_live_v2_5"
DEFAULT_OUTPUT_PREFIX = OUTPUT_PREFIX
SUMMARY_JSON = OUTPUT_DIR / f"{OUTPUT_PREFIX}_summary.json"
COMPATIBILITY_AUDIT_JSON = OUTPUT_DIR / f"{OUTPUT_PREFIX}_compatibility_audit.json"
LATEST_SIGNAL_CSV = OUTPUT_DIR / f"{OUTPUT_PREFIX}_latest_signal.csv"
REALTIME_SIGNAL_CSV = OUTPUT_DIR / f"{OUTPUT_PREFIX}_realtime_signal.csv"
NAV_CSV = OUTPUT_DIR / f"{OUTPUT_PREFIX}_nav.csv"
COSTED_NAV_CSV = OUTPUT_DIR / "microcap_top100_mom16_lb17_hl3_entry46_exit25_no_targetvol_v2_5_costed_nav.csv"
DEFAULT_COSTED_NAV_CSV = COSTED_NAV_CSV
LEGACY_COSTED_NAV_CSVS: list[Path] = [
    OUTPUT_DIR / "microcap_top100_mom16_microcap_only_exp_h3_lb17_entry40_exit40_targetvol30_max1p3_scale030_v2_5_costed_nav.csv",
]
LEGACY_RETEST_OUTPUT_GLOBS: tuple[str, ...] = (
    "microcap_top100_mom16_biweekly_live_v2_5_retest_20260629*",
)
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

VERSION = "2.5"
EXPECTED_VERSION_ROLE = "microcap_only_log_wls_threshold_no_target_vol"
EXPECTED_VERSION_NOTE_PREFIX = "Formal v2.5 microcap-only log-WLS threshold without target-vol overlay."
LOOKBACK = 17
HALFLIFE = 3.0
ENTRY_THRESHOLD = 0.46
EXIT_THRESHOLD = 0.25
TARGET_VOL_ENABLED = False
CASH_DAY_YIELD_ENABLED = False
FINANCING_ENABLED = False
TARGET_VOL = 0.30
TARGET_VOL_MAX_LEVERAGE = 1.3
TARGET_VOL_MIN_LEVERAGE = 0.0
TARGET_VOL_WINDOW = 60
TARGET_VOL_SCALE_REBALANCE_THRESHOLD = 0.30
TARGET_VOL_SCALE_CHANGE_ENTRY_COST = 0.003
TARGET_VOL_SCALE_CHANGE_EXIT_COST = 0.003
TARGET_VOL_SCALE_CHANGE_COST = TARGET_VOL_SCALE_CHANGE_ENTRY_COST
TARGET_VOL_FINANCING_RATE = 0.03
IDLE_CASH_YIELD = 0.02
FORMAL_START_DATE = pd.Timestamp("2010-05-05")
MAX_REALTIME_TARGET_VOL_FROZEN_LAG_DAYS = 5
_OFFICIAL_V2_0_OUT_CACHE: tuple[str, pd.DataFrame] | None = None
_OFFICIAL_V2_0_OUT_CACHE_LOCK = threading.Lock()

SIGNAL_SPREAD_HEDGE_RATIO = 0.0
EXECUTION_HEDGE_RATIO = 0.0
BASE_HEDGE_RATIO = 0.0
TRADING_DAYS = 244


def parse_v2_5_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Top100 Mom16 Biweekly v2.5 microcap-only log-WLS selected no-target-vol line"
    )
    parser.add_argument("query_tokens", nargs="*", help="信号 / 实时信号 / 表现 <区间>")
    parser.add_argument("--panel-path", type=Path, default=None)
    parser.add_argument("--index-csv", type=Path, default=None)
    parser.add_argument(
        "--v25-costed-nav-csv",
        "--costed-nav-csv",
        dest="v25_costed_nav_csv",
        type=Path,
        default=None,
        help="Override the v2.5 costed NAV CSV written/read by queries.",
    )
    parser.add_argument("--base-costed-nav-csv", type=Path, default=None)
    parser.add_argument("--capital", type=float, default=None)
    parser.add_argument("--max-workers", type=int, default=8)
    parser.add_argument("--realtime-cache-seconds", type=int, default=None)
    parser.add_argument("--allow-stale-realtime", action=argparse.BooleanOptionalAction, default=False)
    parser.add_argument("--force-refresh", action="store_true")
    parser.add_argument("--bootstrap-deps", action="store_true")
    parser.add_argument("--wheelhouse", type=Path, default=None)
    parser.add_argument(
        "--v25-output-prefix",
        "--output-prefix",
        dest="v25_output_prefix",
        default=None,
        help="Override the v2.5 output prefix for summary, signal, performance, and NAV files.",
    )
    parser.add_argument("--base-output-prefix", default=None)
    return parser.parse_args(argv)


def configure_output_paths(output_prefix: str | None = None, costed_nav_csv: Path | None = None) -> None:
    global OUTPUT_PREFIX
    global SUMMARY_JSON, COMPATIBILITY_AUDIT_JSON, LATEST_SIGNAL_CSV, REALTIME_SIGNAL_CSV, NAV_CSV, COSTED_NAV_CSV
    global PERF_SUMMARY_CSV, PERF_YEARLY_CSV, PERF_NAV_CSV, PERF_JSON, PERF_PNG
    global PERF_QUERY_SUMMARY_CSV, PERF_QUERY_YEARLY_CSV, PERF_QUERY_NAV_CSV, PERF_QUERY_JSON, PERF_QUERY_PNG

    OUTPUT_PREFIX = str(output_prefix or DEFAULT_OUTPUT_PREFIX)
    SUMMARY_JSON = OUTPUT_DIR / f"{OUTPUT_PREFIX}_summary.json"
    COMPATIBILITY_AUDIT_JSON = OUTPUT_DIR / f"{OUTPUT_PREFIX}_compatibility_audit.json"
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


def configure_runtime(args: argparse.Namespace) -> None:
    _ensure_v2_0_contract_validated()
    configure_output_paths(
        output_prefix=getattr(args, "v25_output_prefix", None),
        costed_nav_csv=getattr(args, "v25_costed_nav_csv", None),
    )
    v2_0._V2_RUNTIME_ARGS = argparse.Namespace(
        query_tokens=[],
        panel_path=getattr(args, "panel_path", None),
        index_csv=getattr(args, "index_csv", None),
        costed_nav_csv=getattr(args, "base_costed_nav_csv", None),
        output_prefix=getattr(args, "base_output_prefix", None),
        capital=getattr(args, "capital", None),
        max_workers=getattr(args, "max_workers", 8),
        realtime_cache_seconds=(
            v2_0.DEFAULT_REALTIME_CACHE_SECONDS
            if getattr(args, "realtime_cache_seconds", None) is None
            else getattr(args, "realtime_cache_seconds")
        ),
        allow_stale_realtime=getattr(args, "allow_stale_realtime", False),
        force_refresh=getattr(args, "force_refresh", False),
        bootstrap_deps=getattr(args, "bootstrap_deps", False),
        wheelhouse=getattr(args, "wheelhouse", None),
    )


def v2_5_output_lock(
    wait_timeout_seconds: float | None = None,
    stale_lock_seconds: float | None = None,
):
    _ensure_v2_0_contract_validated()
    return v2_0._v2_file_lock(
        f"{OUTPUT_PREFIX}_generation.lock",
        wait_timeout_seconds=v2_0.DEFAULT_V2_LOCK_WAIT_SECONDS if wait_timeout_seconds is None else wait_timeout_seconds,
        stale_lock_seconds=v2_0.DEFAULT_V2_STALE_LOCK_SECONDS if stale_lock_seconds is None else stale_lock_seconds,
    )


def v2_5_realtime_output_lock(
    wait_timeout_seconds: float | None = None,
    stale_lock_seconds: float | None = None,
):
    _ensure_v2_0_contract_validated()
    return v2_0._v2_file_lock(
        f"{OUTPUT_PREFIX}_realtime.lock",
        wait_timeout_seconds=v2_0.DEFAULT_V2_LOCK_WAIT_SECONDS if wait_timeout_seconds is None else wait_timeout_seconds,
        stale_lock_seconds=v2_0.DEFAULT_V2_STALE_LOCK_SECONDS if stale_lock_seconds is None else stale_lock_seconds,
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


def _json_canonical(payload: object) -> object:
    return json.loads(_json_dumps(payload))


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


def microcap_nav(close_df: pd.DataFrame) -> tuple[pd.Series, pd.Series, pd.Series]:
    validate_close_df(close_df)
    close_df = close_df.sort_index()
    micro_ret = close_df["microcap"].pct_change(fill_method=None)
    if "hedge" in close_df.columns:
        hedge_ret = close_df["hedge"].pct_change(fill_method=None)
    else:
        hedge_ret = pd.Series(0.0, index=close_df.index, dtype=float)
    nav = (1.0 + micro_ret.fillna(0.0)).cumprod()
    nav.name = "microcap_nav"
    return nav, micro_ret, hedge_ret


def validate_close_df(close_df: pd.DataFrame) -> None:
    required = {"microcap"}
    missing = required - set(close_df.columns)
    if missing:
        raise ValueError(f"close_df missing columns: {sorted(missing)}")
    if close_df.index.has_duplicates:
        raise ValueError("close_df index has duplicate dates")
    if not close_df.index.is_monotonic_increasing:
        raise ValueError("close_df index must be monotonic increasing")
    price_cols = ["microcap"] + (["hedge"] if "hedge" in close_df.columns else [])
    prices = close_df[price_cols].apply(pd.to_numeric, errors="coerce")
    if prices.isna().any().any():
        raise ValueError("close_df contains NaN prices")
    if np.isinf(prices.to_numpy(dtype=float)).any():
        raise ValueError("close_df contains inf prices")
    if (prices <= 0).any().any():
        raise ValueError("close_df contains non-positive prices")


def log_wls_score_and_r2(
    spread_nav: pd.Series,
    lookback: int = LOOKBACK,
    halflife: float = HALFLIFE,
) -> pd.DataFrame:
    lookback = int(lookback)
    if lookback <= 0:
        raise ValueError("lookback must be positive")
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
    if len(values) < lookback or denom <= 0:
        return pd.DataFrame({"annualized_log_wls_score": score, "log_wls_r2": r2}, index=y.index)

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
    return pd.DataFrame({"annualized_log_wls_score": score, "log_wls_r2": r2}, index=y.index)


def _valid_log_wls_index(close_df: pd.DataFrame) -> pd.DatetimeIndex:
    nav, _micro_ret, _hedge_ret = microcap_nav(close_df)
    log_wls = log_wls_score_and_r2(nav)
    valid = log_wls["annualized_log_wls_score"].notna() & log_wls["log_wls_r2"].notna()
    return pd.DatetimeIndex(log_wls.index[valid])


def build_v2_5_common_index(
    close_df: pd.DataFrame,
    official_index: pd.DatetimeIndex | pd.Index | None = None,
) -> pd.DatetimeIndex:
    valid_idx = pd.DatetimeIndex(_valid_log_wls_index(close_df))
    valid_idx = pd.DatetimeIndex(valid_idx[valid_idx >= FORMAL_START_DATE]).sort_values()
    idx = valid_idx
    if official_index is not None:
        official_idx = pd.DatetimeIndex(official_index).dropna().sort_values()
        _assert_official_index_covers_valid_signal_index(valid_idx, official_idx, "v2.5")
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


def _warn_on_missing_common_index_sessions(common_index: pd.DatetimeIndex, expected_index: pd.DatetimeIndex) -> None:
    gap = _common_index_gap_summary(common_index, expected_index)
    if int(gap["missing_count"]) == 0:
        return
    warnings.warn(
        "v2.5 common index missing trading sessions inside the official overlap; "
        f"first_missing={gap['first_missing']}, missing_count={gap['missing_count']}. "
        "Using the v2.0 official overlap index; performance may omit those base-valid sessions.",
        RuntimeWarning,
        stacklevel=2,
    )


def _date_ranges_for_missing_sessions(missing: pd.DatetimeIndex, expected_span: pd.DatetimeIndex) -> list[dict[str, str | int]]:
    if len(missing) == 0:
        return []
    expected_pos = {pd.Timestamp(dt): i for i, dt in enumerate(expected_span)}
    ranges: list[dict[str, str | int]] = []
    start = pd.Timestamp(missing[0])
    prev = start
    count = 1
    for raw_dt in missing[1:]:
        dt = pd.Timestamp(raw_dt)
        if expected_pos.get(dt, -10) == expected_pos.get(prev, -20) + 1:
            prev = dt
            count += 1
            continue
        ranges.append({"start": str(start.date()), "end": str(prev.date()), "count": count})
        start = prev = dt
        count = 1
    ranges.append({"start": str(start.date()), "end": str(prev.date()), "count": count})
    return ranges


def _common_index_gap_summary(common_index: pd.DatetimeIndex, expected_index: pd.DatetimeIndex) -> dict[str, object]:
    common = pd.DatetimeIndex(common_index).sort_values()
    expected = pd.DatetimeIndex(expected_index).sort_values()
    if len(common) == 0 or len(expected) == 0:
        return {
            "missing_count": 0,
            "first_missing": None,
            "last_missing": None,
            "missing_ranges_head": [],
            "note": "no overlap span available",
        }
    expected_span = expected[(expected >= common.min()) & (expected <= common.max())]
    missing = expected_span.difference(common)
    ranges = _date_ranges_for_missing_sessions(missing, expected_span)
    return {
        "missing_count": int(len(missing)),
        "first_missing": str(pd.Timestamp(missing[0]).date()) if len(missing) else None,
        "last_missing": str(pd.Timestamp(missing[-1]).date()) if len(missing) else None,
        "missing_ranges_head": ranges[:10],
        "note": (
            "Sessions are base-valid log-WLS dates omitted because v2.5 uses the intersection "
            "with the official v2.0 output index."
        ),
    }


def build_microcap_log_wls_gross(close_df: pd.DataFrame, index: pd.DatetimeIndex | None = None) -> pd.DataFrame:
    close_df = close_df.sort_index()
    nav, micro_ret, hedge_ret = microcap_nav(close_df)
    log_wls = log_wls_score_and_r2(nav)
    common_index = _valid_log_wls_index(close_df) if index is None else pd.DatetimeIndex(index)
    score = pd.to_numeric(log_wls["annualized_log_wls_score"].loc[common_index], errors="coerce")
    r2 = pd.to_numeric(log_wls["log_wls_r2"].loc[common_index], errors="coerce")
    microcap_ret = micro_ret.loc[common_index]
    hedge_ret_part = hedge_ret.loc[common_index]
    hedge_close = close_df["hedge"].loc[common_index] if "hedge" in close_df.columns else pd.Series(1.0, index=common_index)

    current_active = False
    holdings: list[str] = []
    next_holdings: list[str] = []
    signal_on_values: list[bool] = []
    returns: list[float] = []
    for dt in common_index:
        active_before_signal = bool(current_active)
        holdings.append("long_microcap_top100" if active_before_signal else "cash")
        returns.append(float(microcap_ret.loc[dt]) if active_before_signal else 0.0)
        current_score = score.loc[dt]
        if pd.isna(current_score):
            next_active = False
        elif active_before_signal:
            next_active = float(current_score) > EXIT_THRESHOLD
        else:
            next_active = float(current_score) > ENTRY_THRESHOLD
        next_holdings.append("long_microcap_top100" if next_active else "cash")
        signal_on_values.append(bool(next_active))
        current_active = bool(next_active)

    gross_ret = pd.Series(returns, index=common_index, dtype=float)
    futures_drag = pd.Series(0.0, index=common_index, dtype=float)
    return pd.DataFrame(
        {
            "return_raw": gross_ret,
            "return": gross_ret,
            "holding": holdings,
            "next_holding": next_holdings,
            "signal_on": signal_on_values,
            "microcap_close": close_df["microcap"].loc[common_index],
            "hedge_close": hedge_close,
            "microcap_ret": microcap_ret,
            "hedge_ret": hedge_ret_part,
            "microcap_mom": score,
            "hedge_mom": 0.0,
            "momentum_gap": score,
            "momentum_gap_deprecated": True,
            "annualized_log_wls_score": score,
            "schema_version": "log_wls_score_schema_v1",
            "log_wls_r2": r2,
            "microcap_nav": nav.loc[common_index],
            "halflife": HALFLIFE,
            "exp_weight_oldest_to_newest": ",".join(f"{w:.8f}" for w in exp_weights()),
            "signal_score_label": "microcap_only_annualized_log_wls_score",
            "entry_threshold": ENTRY_THRESHOLD,
            "exit_threshold": EXIT_THRESHOLD,
            "momentum_gap_legacy_note": "legacy field contains annualized microcap-only log-WLS score, not plain microcap-minus-hedge momentum gap",
            "futures_drag": futures_drag,
            "active_spread_ret": gross_ret,
            "weight": 1.0,
            "target_vol_execution_scale": 1.0,
        },
        index=common_index,
    )


def apply_cost(gross: pd.DataFrame, turnover_df: pd.DataFrame) -> pd.DataFrame:
    _ensure_v2_0_contract_validated()
    out = v2_0.base_mod.freq_mod.cost_mod.apply_cost_model(gross, turnover_df)
    _assert_apply_cost_model_preserves_gross_return(gross, out)
    out["overlay_pre_cost_return"] = pd.to_numeric(out["return"], errors="coerce").fillna(0.0)
    return out


def _assert_apply_cost_model_preserves_gross_return(gross: pd.DataFrame, costed: pd.DataFrame) -> None:
    if "return" not in gross.columns or "return" not in costed.columns:
        raise RuntimeError("v2_0 cost model contract changed: gross and costed outputs must contain return")
    common_index = pd.Index(costed.index).intersection(pd.Index(gross.index))
    if len(common_index) != len(costed.index):
        raise RuntimeError("v2_0 cost model contract changed: costed output index must stay aligned with gross input")
    expected = pd.to_numeric(gross.loc[common_index, "return"], errors="coerce").fillna(0.0)
    actual = pd.to_numeric(costed.loc[common_index, "return"], errors="coerce").fillna(0.0)
    if not np.allclose(actual.to_numpy(dtype=float), expected.to_numpy(dtype=float), rtol=1e-9, atol=1e-9):
        raise RuntimeError(
            "v2_0 cost model contract changed: apply_cost_model must preserve gross return in out['return']; "
            "costed values belong in return_net and total_cost"
        )


def _safe_float(value: object, default: float = 0.0) -> float:
    try:
        if pd.isna(value):
            return float(default)
        return float(value)
    except (TypeError, ValueError):
        return float(default)


def _v2_0_signal_compat_net_df(net_df: pd.DataFrame) -> pd.DataFrame:
    compat = net_df.copy()
    holding_map = {"long_microcap_top100": "long_microcap_short_zz1000"}
    for col in ["holding", "next_holding"]:
        if col in compat.columns:
            compat[col] = compat[col].replace(holding_map)
    return compat


def _apply_v2_5_signal_fields(row: pd.DataFrame, latest: pd.Series) -> None:
    row_idx = row.index[0]
    current_holding = str(latest.get("holding", row.at[row_idx, "current_holding"]))
    next_holding = str(latest.get("next_holding", current_holding))
    current_scale = _safe_float(row.at[row_idx, "current_execution_scale"], 0.0)
    next_scale = _safe_float(row.at[row_idx, "next_session_actionable_scale"], current_scale)
    turnover = v2_0.overlay_mod.calc_target_vol_turnover(
        current_holding,
        current_scale,
        next_holding,
        next_scale,
        hedge_ratio=EXECUTION_HEDGE_RATIO,
    )
    cost_rate = (
        TARGET_VOL_SCALE_CHANGE_ENTRY_COST
        if next_scale >= current_scale
        else TARGET_VOL_SCALE_CHANGE_EXIT_COST
    )
    raw_cost = turnover * cost_rate
    same_holding = current_holding == next_holding
    overlay_cost = raw_cost if same_holding else 0.0
    trade_cost = overlay_cost if same_holding else raw_cost

    row["current_holding"] = current_holding
    row["next_holding"] = next_holding
    row["signal_label"] = next_holding
    row["next_session_turnover"] = turnover
    row["next_session_leg_turnover"] = turnover
    row["next_session_leg_cost_est_raw"] = raw_cost
    row["next_session_overlay_cost_est"] = overlay_cost
    row["next_session_trade_cost_est"] = trade_cost
    row["next_session_overlay_trade_cost_est"] = overlay_cost
    row["next_session_trade_cost_est_type"] = (
        "overlay_only" if same_holding else "fixed_exposure_entry_exit"
    )
    row["fixed_hedge_ratio"] = EXECUTION_HEDGE_RATIO
    row["max_leverage"] = TARGET_VOL_MAX_LEVERAGE


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
    """Guard realtime target-vol freshness against the v2.0 official output index.

    This catches cases where recent sessions exist in the official index but
    the v2.5 common index dropped them. It cannot detect a shared upstream
    panel outage that also truncates the official index itself.
    """
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


def _apply_scale_rebalance_threshold(desired_scale: pd.Series, active: pd.Series) -> pd.Series:
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
        if last_scale <= 1e-12 or abs(target - last_scale) >= TARGET_VOL_SCALE_REBALANCE_THRESHOLD:
            last_scale = target
        values.append(float(last_scale))
    return pd.Series(values, index=desired.index, dtype=float)


def _calc_next_session_actionable_scale(
    current_execution_scale: pd.Series,
    next_session_target_scale: pd.Series,
    next_holding: pd.Series,
) -> pd.Series:
    current = pd.to_numeric(current_execution_scale, errors="coerce").fillna(0.0)
    target = pd.to_numeric(next_session_target_scale, errors="coerce").fillna(current)
    next_holding = next_holding.fillna("cash").astype(str)
    actionable = current.copy()
    to_cash = next_holding.eq("cash")
    enter_from_cash = current.le(1e-12) & next_holding.ne("cash") & target.gt(1e-12)
    rebalance = target.sub(current).abs().ge(TARGET_VOL_SCALE_REBALANCE_THRESHOLD)
    actionable.loc[to_cash] = 0.0
    actionable.loc[~to_cash & (enter_from_cash | rebalance)] = target.loc[~to_cash & (enter_from_cash | rebalance)]
    return actionable.astype(float)


def _microcap_turnover_series(holding: pd.Series, execution_scale: pd.Series) -> pd.Series:
    holding = holding.fillna("cash").astype(str)
    scale = pd.to_numeric(execution_scale, errors="coerce").fillna(0.0)
    leg = scale.where(holding.ne("cash"), 0.0)
    return leg.sub(leg.shift(1).fillna(0.0)).abs().astype(float)


def _target_vol_scale_change_cost(holding: pd.Series, execution_scale: pd.Series) -> pd.Series:
    holding = holding.fillna("cash").astype(str)
    scale = pd.to_numeric(execution_scale, errors="coerce").fillna(0.0)
    scale_delta = scale.sub(scale.shift(1).fillna(0.0))
    same_holding = holding.eq(holding.shift(1))
    cost_rate = pd.Series(TARGET_VOL_SCALE_CHANGE_ENTRY_COST, index=holding.index, dtype=float)
    cost_rate.loc[scale_delta < 0.0] = TARGET_VOL_SCALE_CHANGE_EXIT_COST
    return scale_delta.abs().where(same_holding, 0.0).mul(cost_rate).astype(float)


def _target_vol_scale_change_cost_note() -> str:
    if math.isclose(TARGET_VOL_SCALE_CHANGE_ENTRY_COST, TARGET_VOL_SCALE_CHANGE_EXIT_COST, rel_tol=0.0, abs_tol=1e-12):
        return f"{TARGET_VOL_SCALE_CHANGE_ENTRY_COST:.2%} one-side microcap exposure scale-change cost"
    return (
        f"{TARGET_VOL_SCALE_CHANGE_ENTRY_COST:.2%} entry / "
        f"{TARGET_VOL_SCALE_CHANGE_EXIT_COST:.2%} exit one-side microcap exposure scale-change cost"
    )


def _base_trade_cost_scale(
    holding: pd.Series,
    next_holding: pd.Series,
    current_execution_scale: pd.Series,
    next_session_actionable_scale: pd.Series,
) -> pd.Series:
    holding = holding.fillna("cash").astype(str)
    next_holding = next_holding.fillna(holding).astype(str)
    current = pd.to_numeric(current_execution_scale, errors="coerce").fillna(0.0)
    actionable = pd.to_numeric(next_session_actionable_scale, errors="coerce").fillna(current)
    scale = pd.Series(0.0, index=holding.index, dtype=float)
    current_active = holding.ne("cash")
    next_active = next_holding.ne("cash")
    scale.loc[~current_active & next_active] = actionable.loc[~current_active & next_active]
    scale.loc[current_active] = current.loc[current_active]
    return scale.clip(lower=0.0)


def apply_target_vol(
    costed_base: pd.DataFrame,
    target_vol: float = TARGET_VOL,
    *,
    treat_last_row_as_snapshot: bool = False,
    snapshot_date: object | None = None,
) -> pd.DataFrame:
    out = costed_base.copy().sort_index()
    required_cols = {"return_net", "total_cost", "overlay_pre_cost_return", "microcap_ret", "holding", "next_holding"}
    missing = required_cols - set(out.columns)
    if missing:
        raise RuntimeError(f"apply_target_vol missing required columns: {sorted(missing)}")
    target_vol_value = float(target_vol)
    holding = out["holding"].fillna("cash").astype(str)
    next_holding = out["next_holding"].fillna(holding).astype(str)
    active = holding.ne("cash")
    base_return_net = pd.to_numeric(out["return_net"], errors="coerce").fillna(0.0)
    base_trade_cost = pd.to_numeric(out["total_cost"], errors="coerce").fillna(0.0)
    base_pre_cost_return = pd.to_numeric(out["overlay_pre_cost_return"], errors="coerce").fillna(0.0)
    target_vol_return = pd.to_numeric(out["microcap_ret"], errors="coerce").replace([np.inf, -np.inf], np.nan)
    realized_vol = target_vol_return.rolling(TARGET_VOL_WINDOW, min_periods=TARGET_VOL_WINDOW).std(ddof=1) * math.sqrt(TRADING_DAYS)
    raw_scale = (target_vol_value / realized_vol.replace(0.0, np.nan)).replace([np.inf, -np.inf], np.nan).clip(
        lower=TARGET_VOL_MIN_LEVERAGE,
        upper=TARGET_VOL_MAX_LEVERAGE,
    )
    target_execution_scale = raw_scale.shift(1).fillna(1.0)
    execution_scale = _apply_scale_rebalance_threshold(target_execution_scale, active)
    frozen_source_dates = pd.Series(
        [str(pd.Timestamp(idx).date()) for idx in out.index],
        index=out.index,
        dtype=object,
    )
    frozen_lag_calendar_days = pd.Series(0, index=out.index, dtype=int)
    frozen_lag_trading_days = pd.Series(0, index=out.index, dtype=int)
    if treat_last_row_as_snapshot and len(out.index) >= 2:
        snapshot_idx = out.index[-1]
        if snapshot_date is not None and pd.Timestamp(snapshot_idx).date() != pd.Timestamp(snapshot_date).date():
            raise RuntimeError(
                "target-vol snapshot row mismatch: "
                f"expected_snapshot_date={pd.Timestamp(snapshot_date).date()}, "
                f"actual_last_date={pd.Timestamp(snapshot_idx).date()}"
            )
        source_idx = out.index[-2]
        frozen_source_dates.loc[snapshot_idx] = str(pd.Timestamp(source_idx).date())
        frozen_lag_calendar_days.loc[snapshot_idx] = int(
            (pd.Timestamp(snapshot_idx).date() - pd.Timestamp(source_idx).date()).days
        )
        frozen_lag_trading_days.loc[snapshot_idx] = int(out.index.get_loc(snapshot_idx) - out.index.get_loc(source_idx))
    next_session_target_scale = raw_scale.copy()
    next_session_target_scale.loc[next_holding.eq("cash")] = 0.0
    next_session_target_scale.loc[next_holding.ne("cash")] = next_session_target_scale.loc[next_holding.ne("cash")].fillna(1.0)
    next_session_target_scale = next_session_target_scale.fillna(0.0)
    next_session_actionable_scale = _calc_next_session_actionable_scale(
        execution_scale,
        next_session_target_scale,
        next_holding,
    )
    target_vol_turnover = _microcap_turnover_series(holding, execution_scale)
    same_holding = holding.eq(holding.shift(1))
    target_vol_costed_turnover = target_vol_turnover.where(same_holding, 0.0)
    scale_change_cost = _target_vol_scale_change_cost(holding, execution_scale)
    financing_cost = execution_scale.sub(1.0).clip(lower=0.0) * TARGET_VOL_FINANCING_RATE / TRADING_DAYS
    idle_cash_yield = active.astype(float) * execution_scale.rsub(1.0).clip(lower=0.0, upper=1.0) * IDLE_CASH_YIELD / TRADING_DAYS
    cash_day_yield = active.astype(float).rsub(1.0) * IDLE_CASH_YIELD / TRADING_DAYS
    base_cost_scale = _base_trade_cost_scale(holding, next_holding, execution_scale, next_session_actionable_scale)
    base_trade_cost_scaled = (base_trade_cost * base_cost_scale).clip(lower=0.0, upper=0.99)
    return_gross_target_vol = base_pre_cost_return * execution_scale + idle_cash_yield + cash_day_yield
    ret = (
        (1.0 + return_gross_target_vol)
        * (1.0 - base_trade_cost_scaled)
        * (1.0 - scale_change_cost)
        * (1.0 - financing_cost)
        - 1.0
    )
    out["target_vol_enabled"] = True
    out["target_vol"] = target_vol_value
    out["target_vol_window"] = TARGET_VOL_WINDOW
    out["target_vol_return"] = target_vol_return.fillna(0.0)
    out["target_vol_return_source"] = "microcap_pct_change_unhedged"
    out["target_vol_realized_vol"] = realized_vol
    out["target_vol_scale_raw"] = raw_scale
    out["target_vol_execution_scale_raw"] = target_execution_scale
    out["target_vol_frozen_source_date"] = frozen_source_dates
    out["target_vol_frozen_lag_days"] = frozen_lag_calendar_days
    out["target_vol_frozen_lag_calendar_days"] = frozen_lag_calendar_days
    out["target_vol_frozen_lag_trading_days"] = frozen_lag_trading_days
    out["current_execution_scale"] = execution_scale
    out["execution_scale"] = execution_scale
    out["weight"] = execution_scale
    out["next_session_target_scale"] = next_session_target_scale
    out["next_session_actionable_scale"] = next_session_actionable_scale
    out["target_vol_scale_next_session"] = next_session_actionable_scale
    out["target_vol_turnover"] = target_vol_turnover
    out["target_vol_costed_turnover"] = target_vol_costed_turnover
    out["scale_change_cost"] = scale_change_cost
    out["target_vol_trade_cost"] = scale_change_cost
    out["financing_cost"] = financing_cost
    out["idle_cash_yield"] = idle_cash_yield
    out["cash_day_yield"] = cash_day_yield
    out["cash_day_yield_annual"] = IDLE_CASH_YIELD
    out["cash_day_yield_enabled"] = True
    out["base_trade_cost"] = base_trade_cost
    out["base_trade_cost_scale"] = base_cost_scale
    out["base_trade_cost_scaled"] = base_trade_cost_scaled
    out["base_pre_cost_return"] = base_pre_cost_return
    out["return_gross_target_vol"] = return_gross_target_vol
    out["embedded_lineage_return_net"] = base_return_net
    out["embedded_lineage_nav_net"] = pd.to_numeric(out.get("nav_net", pd.Series(np.nan, index=out.index)), errors="coerce")
    out["return_net"] = ret
    out["nav_net"] = (1.0 + out["return_net"].fillna(0.0)).cumprod()
    out["return"] = out["return_net"]
    out["nav"] = out["nav_net"]
    out["return_column_semantics"] = (
        "return equals return_net after target-vol overlay; use base_pre_cost_return or "
        "return_gross_target_vol for gross return"
    )
    out["version"] = VERSION
    out["base_version"] = "embedded_v2_base"
    out["overlay_type"] = "microcap_only_log_wls_threshold_target_vol"
    out["scale_rebalance_threshold"] = TARGET_VOL_SCALE_REBALANCE_THRESHOLD
    out["target_vol_max_leverage"] = TARGET_VOL_MAX_LEVERAGE
    out["hedge_removed"] = True
    out["schema_version"] = "log_wls_score_schema_v1"
    out["momentum_gap_deprecated"] = True
    return out


def apply_no_target_vol(costed_base: pd.DataFrame) -> pd.DataFrame:
    out = costed_base.copy().sort_index()
    required_cols = {"return_net", "total_cost", "overlay_pre_cost_return", "holding", "next_holding"}
    missing = required_cols - set(out.columns)
    if missing:
        raise RuntimeError(f"apply_no_target_vol missing required columns: {sorted(missing)}")
    holding = out["holding"].fillna("cash").astype(str)
    next_holding = out["next_holding"].fillna(holding).astype(str)
    active_scale = holding.ne("cash").astype(float)
    next_scale = next_holding.ne("cash").astype(float)
    ret = pd.to_numeric(out["return_net"], errors="coerce").fillna(0.0)
    total_cost = pd.to_numeric(out["total_cost"], errors="coerce").fillna(0.0)
    base_pre_cost_return = pd.to_numeric(out["overlay_pre_cost_return"], errors="coerce").fillna(0.0)
    zero = pd.Series(0.0, index=out.index, dtype=float)
    frozen_source_dates = pd.Series(
        [str(pd.Timestamp(idx).date()) for idx in out.index],
        index=out.index,
        dtype=object,
    )

    out["target_vol_enabled"] = False
    out["target_vol"] = 0.0
    out["target_vol_window"] = 0
    out["target_vol_return"] = pd.to_numeric(out.get("microcap_ret", zero), errors="coerce").fillna(0.0)
    out["target_vol_return_source"] = "disabled_no_target_vol"
    out["target_vol_realized_vol"] = zero
    out["target_vol_scale_raw"] = active_scale
    out["target_vol_execution_scale_raw"] = active_scale
    out["target_vol_execution_scale"] = active_scale
    out["target_vol_realtime_snapshot_vol_frozen"] = False
    out["target_vol_frozen_source_date"] = frozen_source_dates
    out["target_vol_frozen_lag_days"] = 0
    out["target_vol_frozen_lag_calendar_days"] = 0
    out["target_vol_frozen_lag_trading_days"] = 0
    out["current_execution_scale"] = active_scale
    out["execution_scale"] = active_scale
    out["weight"] = active_scale
    out["next_session_target_scale"] = next_scale
    out["next_session_actionable_scale"] = next_scale
    out["target_vol_scale_next_session"] = next_scale
    out["target_vol_turnover"] = zero
    out["target_vol_costed_turnover"] = zero
    out["scale_change_cost"] = zero
    out["target_vol_trade_cost"] = zero
    out["financing_cost"] = zero
    out["financing_enabled"] = False
    out["idle_cash_yield"] = zero
    out["cash_day_yield"] = zero
    out["cash_day_yield_annual"] = 0.0
    out["cash_day_yield_enabled"] = False
    out["base_trade_cost"] = total_cost
    out["base_trade_cost_scale"] = active_scale
    out["base_trade_cost_scaled"] = total_cost
    out["base_pre_cost_return"] = base_pre_cost_return
    out["return_gross_target_vol"] = base_pre_cost_return
    out["embedded_lineage_return_net"] = ret
    out["embedded_lineage_nav_net"] = pd.to_numeric(out.get("nav_net", pd.Series(np.nan, index=out.index)), errors="coerce")
    out["return_net"] = ret
    out["nav_net"] = (1.0 + out["return_net"].fillna(0.0)).cumprod()
    out["return"] = out["return_net"]
    out["nav"] = out["nav_net"]
    out["return_column_semantics"] = (
        "return equals return_net after selected v2.5 microcap-only log-WLS threshold "
        "and base entry/exit/rebalance costs; no target-vol, cash-day yield, or financing overlay"
    )
    out["version"] = VERSION
    out["base_version"] = "embedded_v2_base"
    out["overlay_type"] = "microcap_only_log_wls_threshold_no_target_vol"
    out["scale_rebalance_threshold"] = 0.0
    out["target_vol_max_leverage"] = 1.0
    out["hedge_removed"] = True
    out["schema_version"] = "log_wls_score_schema_v1"
    out["momentum_gap_deprecated"] = True
    return out


def build_v2_5_result(
    close_df: pd.DataFrame,
    turnover_df: pd.DataFrame,
    common_index: pd.DatetimeIndex | None = None,
) -> pd.DataFrame:
    if common_index is None:
        common_index = build_v2_5_common_index(close_df)
    else:
        common_index = pd.DatetimeIndex(common_index)
        common_index = common_index[common_index >= FORMAL_START_DATE].sort_values()
    gross = build_microcap_log_wls_gross(close_df, common_index)
    costed = apply_cost(gross, turnover_df)
    out = apply_no_target_vol(costed)
    if out.empty:
        raise ValueError(
            "v2.5 output is empty: check close_df, official_v2_0_out.index, "
            "FORMAL_START_DATE, and valid log-WLS window."
        )
    return out


def current_base_fingerprint() -> dict[str, object]:
    _ensure_v2_0_contract_validated()
    base = dict(v2_0.embedded_context.current_base_fingerprint())
    runtime_args = getattr(v2_0, "_V2_RUNTIME_ARGS", None)

    def runtime_value(name: str) -> str | None:
        if runtime_args is None:
            return None
        value = getattr(runtime_args, name, None)
        return None if value is None else str(value)

    return {
        "base_version": "embedded_v2_base",
        "strategy_version": VERSION,
        "base_fingerprint": base,
        "runtime_overrides": {
            "panel_path": runtime_value("panel_path"),
            "index_csv": runtime_value("index_csv"),
            "base_costed_nav_csv": runtime_value("costed_nav_csv"),
            "base_output_prefix": runtime_value("output_prefix"),
            "v25_costed_nav_csv": str(COSTED_NAV_CSV),
            "v25_output_prefix": OUTPUT_PREFIX,
        },
        "signal_model": "microcap_only_log_wls_exp_halflife_3p0_lb17_entry46_exit25_no_targetvol",
        "lookback": LOOKBACK,
        "halflife": HALFLIFE,
        "exp_weight_oldest_to_newest": list(exp_weights()),
        "common_index_source": "intersection of valid microcap-only log-WLS signal dates and official v2.0 output index, filtered from 2010-05-05",
        "rebalance_timing_note": (
            "The biweekly file/output name is inherited from the embedded Top100 member proxy; "
            "the v2.5 close-confirmed overlay evaluates its microcap-only log-WLS threshold daily."
        ),
        "score_definition": "annualized weighted log slope of unhedged microcap Top100 NAV",
        "nav_csv_momentum_gap_column_alias_note": "momentum_gap stores annualized microcap-only log-WLS score for v2.0 compatibility, not raw microcap-minus-hedge momentum gap",
        "schema_version": "log_wls_score_schema_v1",
        "momentum_gap_deprecated": True,
        "return_column_semantics": (
            "return equals return_net after selected v2.5 microcap-only log-WLS threshold and base costs; "
            "use base_pre_cost_return for pre-cost return"
        ),
        "r2_gate": None,
        "signal_spread_hedge_ratio": SIGNAL_SPREAD_HEDGE_RATIO,
        "execution_hedge_ratio": EXECUTION_HEDGE_RATIO,
        "base_hedge_ratio": BASE_HEDGE_RATIO,
        "entry_threshold": ENTRY_THRESHOLD,
        "exit_threshold": EXIT_THRESHOLD,
        "signal_quality_derisk_enabled": False,
        "single_trade_stop_loss_enabled": False,
        "equity_drawdown_overlay_enabled": False,
        "momentum_decay_overlay_enabled": False,
        "overheat_overlay_enabled": False,
        "target_volatility_scaling": {"enabled": TARGET_VOL_ENABLED},
        "cash_day_yield": {"enabled": CASH_DAY_YIELD_ENABLED},
        "financing": {"enabled": FINANCING_ENABLED},
        "hedge_removed": True,
    }


def current_realtime_fingerprint() -> dict[str, object]:
    runtime_args = getattr(v2_0, "_V2_RUNTIME_ARGS", None)
    return {
        "historical_base_fingerprint": current_base_fingerprint(),
        "realtime_runtime_options": {
            "force_refresh": bool(getattr(runtime_args, "force_refresh", False)) if runtime_args is not None else False,
            "realtime_cache_seconds": getattr(runtime_args, "realtime_cache_seconds", None) if runtime_args is not None else None,
            "allow_stale_realtime": bool(getattr(runtime_args, "allow_stale_realtime", False)) if runtime_args is not None else False,
        },
    }


def summary_matches_current_v2_5_base(summary: dict[str, object]) -> bool:
    audit = build_v2_5_compatibility_audit(summary)
    return bool(
        audit["version_match"]
        and audit["version_role_match"]
        and audit["version_note_prefix_match"]
        and audit["fingerprint_match"]
    )


def build_v2_5_compatibility_audit(summary: dict[str, object] | None) -> dict[str, object]:
    expected_fingerprint = current_base_fingerprint()
    if not isinstance(summary, dict):
        return {
            "summary_is_dict": False,
            "version_match": False,
            "version_role_match": False,
            "version_note_prefix_match": False,
            "fingerprint_match": False,
            "expected_version": VERSION,
            "actual_version": None,
            "expected_version_role": EXPECTED_VERSION_ROLE,
            "actual_version_role": None,
            "expected_version_note_prefix": EXPECTED_VERSION_NOTE_PREFIX,
            "actual_version_note": None,
            "expected_fingerprint": expected_fingerprint,
            "actual_fingerprint": None,
        }
    actual_fingerprint = summary.get("base_fingerprint")
    return {
        "summary_is_dict": True,
        "version_match": str(summary.get("version")) == VERSION,
        "version_role_match": str(summary.get("version_role")) == EXPECTED_VERSION_ROLE,
        "version_note_prefix_match": str(summary.get("version_note", "")).startswith(EXPECTED_VERSION_NOTE_PREFIX),
        "fingerprint_match": _json_canonical(actual_fingerprint) == _json_canonical(expected_fingerprint),
        "expected_version": VERSION,
        "actual_version": summary.get("version"),
        "expected_version_role": EXPECTED_VERSION_ROLE,
        "actual_version_role": summary.get("version_role"),
        "expected_version_note_prefix": EXPECTED_VERSION_NOTE_PREFIX,
        "actual_version_note": summary.get("version_note"),
        "expected_fingerprint": expected_fingerprint,
        "actual_fingerprint": actual_fingerprint,
    }


def write_v2_5_compatibility_audit(summary: dict[str, object] | None, read_error: str | None = None) -> None:
    audit = build_v2_5_compatibility_audit(summary)
    if read_error is not None:
        audit["summary_read_error"] = read_error
    _atomic_write_text(COMPATIBILITY_AUDIT_JSON, _json_dumps(audit), encoding="utf-8")


def stale_v2_5_legacy_retest_outputs() -> list[Path]:
    paths: set[Path] = set()
    for pattern in LEGACY_RETEST_OUTPUT_GLOBS:
        paths.update(path for path in OUTPUT_DIR.glob(pattern) if path.exists())
    return sorted(paths)


def incompatible_v2_5_outputs() -> list[Path]:
    legacy_retest_outputs = stale_v2_5_legacy_retest_outputs()
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
        *legacy_retest_outputs,
    ]
    if not SUMMARY_JSON.exists():
        return [path for path in outputs if path.exists()]
    try:
        summary = json.loads(SUMMARY_JSON.read_text(encoding="utf-8"))
        read_error = None
    except Exception as exc:
        summary = None
        read_error = repr(exc)
    if summary_matches_current_v2_5_base(summary):
        COMPATIBILITY_AUDIT_JSON.unlink(missing_ok=True)
        return legacy_retest_outputs
    write_v2_5_compatibility_audit(summary, read_error=read_error)
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
    sharpe_cagr = annual / vol if vol > 0 else 0.0
    sharpe_mean = ret.mean() * TRADING_DAYS / vol if vol > 0 else 0.0
    drawdown = nav.div(nav.cummax()).sub(1.0)
    return {
        "start_date": str(pd.Timestamp(ret.index[0]).date()),
        "end_date": str(pd.Timestamp(ret.index[-1]).date()),
        "days": int(len(ret)),
        "final_nav": float(nav.iloc[-1]),
        "total_return_pct": float((nav.iloc[-1] - 1.0) * 100.0),
        "annual_pct": float(annual * 100.0),
        "max_drawdown_pct": float(drawdown.min() * 100.0),
        "sharpe": float(sharpe_cagr),
        "sharpe_cagr": float(sharpe_cagr),
        "cagr_to_vol": float(sharpe_cagr),
        "sharpe_mean": float(sharpe_mean),
        "sharpe_note": "sharpe is retained as backward-compatible alias for sharpe_cagr = CAGR / annualized volatility; sharpe_mean = mean daily return * trading days / annualized volatility",
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
        "sharpe_cagr": np.nan,
        "cagr_to_vol": np.nan,
        "sharpe_mean": np.nan,
        "sharpe_note": "",
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
        drawdown = nav.div(nav.cummax()).sub(1.0)
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
    source_label: str = "costed_v2_5",
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
    try:
        plt.plot(nav_df["date"], nav_df["nav_net"], label="v2.5 nav_net")
        plt.title("Top100 Microcap Mom16 v2.5 Costed NAV")
        plt.xlabel("date")
        plt.ylabel("nav_net")
        plt.grid(True, alpha=0.3)
        plt.legend()
        plt.tight_layout()
        plt.savefig(write_paths["png"], dpi=150)
    finally:
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
    _ensure_v2_0_contract_validated()
    row = v2_0.overlay_mod._build_signal_row(_v2_0_signal_compat_net_df(net_df), reference_summary)
    row["version"] = VERSION
    row["strategy_version"] = f"v{VERSION}"
    row["base_version"] = "embedded_v2_base"
    row["overlay_type"] = "microcap_only_log_wls_threshold_no_target_vol"
    row["signal_model"] = "microcap_only_log_wls_exp_halflife_3p0_lb17_entry46_exit25_no_targetvol"
    row["signal_spread_hedge_ratio"] = SIGNAL_SPREAD_HEDGE_RATIO
    row["execution_hedge_ratio"] = EXECUTION_HEDGE_RATIO
    row["fixed_hedge_ratio"] = EXECUTION_HEDGE_RATIO
    row["hedge_removed"] = True
    row["halflife"] = HALFLIFE
    row["lookback"] = LOOKBACK
    row["entry_threshold"] = ENTRY_THRESHOLD
    row["exit_threshold"] = EXIT_THRESHOLD
    row["signal_quality_derisk_enabled"] = False
    row["single_trade_stop_loss_enabled"] = False
    row["equity_drawdown_overlay_enabled"] = False
    row["momentum_decay_overlay_enabled"] = False
    row["overheat_overlay_enabled"] = False
    row["overheat_enabled"] = False
    row["overheat_kind"] = "disabled"
    row["overheat_window"] = 0
    row["overheat_threshold"] = 0.0
    row["overheat_metric_name"] = "disabled"
    row["overheat_triggered"] = False
    row["overheat_require_positive_trade_return"] = False
    row["overheat_require_signal_reset"] = False
    row["signal_score_label"] = "microcap_only_annualized_log_wls_score"
    row["schema_version"] = "log_wls_score_schema_v1"
    row["momentum_gap_deprecated"] = True
    row["return_column_semantics"] = (
        "return equals return_net after selected v2.5 microcap-only log-WLS threshold and base costs; "
        "use base_pre_cost_return for pre-cost return"
    )
    row["momentum_gap_legacy_note"] = (
        "legacy field contains annualized microcap-only log-WLS score, not plain microcap-minus-hedge momentum gap"
    )
    latest = net_df.iloc[-1]
    for col in ["annualized_log_wls_score", "log_wls_r2", "microcap_nav"]:
        if col in latest and pd.notna(latest[col]):
            row[col] = float(latest[col])
    row["target_vol_enabled"] = TARGET_VOL_ENABLED
    row["target_vol"] = 0.0
    row["target_vol_window"] = 0
    row["target_vol_signal_timing"] = ""
    row["target_vol_scale_rebalance_threshold"] = 0.0
    for col in [
        "target_vol_frozen_lag_calendar_days",
        "target_vol_frozen_lag_trading_days",
    ]:
        if col in latest and pd.notna(latest[col]):
            row[col] = latest[col]
    row["target_vol_max_leverage"] = 1.0
    row["max_leverage"] = 1.0
    row["momentum_gap_exit_buffer"] = EXIT_THRESHOLD
    row["cash_day_yield"] = float(latest.get("cash_day_yield", 0.0)) if "cash_day_yield" in latest else 0.0
    row["cash_day_yield_annual"] = 0.0
    row["cash_day_yield_enabled"] = CASH_DAY_YIELD_ENABLED
    row["financing_enabled"] = FINANCING_ENABLED
    _apply_v2_5_signal_fields(row, latest)
    return row


def _close_df_from_base(base_gross_cached: pd.DataFrame) -> pd.DataFrame:
    if "microcap_close" not in base_gross_cached.columns:
        raise RuntimeError("v2.5 base context missing required microcap_close column")
    out = pd.DataFrame({"microcap": base_gross_cached["microcap_close"]}, index=base_gross_cached.index)
    if "hedge_close" in base_gross_cached.columns:
        out["hedge"] = base_gross_cached["hedge_close"]
    return out.sort_index()


def _close_df_from_realtime(realtime_close_df: pd.DataFrame) -> pd.DataFrame:
    if "microcap" not in realtime_close_df.columns:
        raise RuntimeError("v2.5 realtime base missing required microcap column")
    cols = ["microcap"] + (["hedge"] if "hedge" in realtime_close_df.columns else [])
    return realtime_close_df[cols].sort_index()


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
        # Cache the post-generation base state because generation may refresh
        # panel shadow/base files.
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
    _ensure_v2_0_contract_validated()
    cache_key = _official_v2_0_cache_key()
    force_refresh = bool(getattr(getattr(v2_0, "_V2_RUNTIME_ARGS", None), "force_refresh", False))
    with _OFFICIAL_V2_0_OUT_CACHE_LOCK:
        if force_refresh:
            _OFFICIAL_V2_0_OUT_CACHE = None
        cached = _OFFICIAL_V2_0_OUT_CACHE
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


def _build_realtime_v2_5_official_index(
    close_df: pd.DataFrame,
    meta: dict[str, object],
    official_index: pd.DatetimeIndex | pd.Index | None = None,
) -> pd.DatetimeIndex:
    del official_index
    anchor_text = str(meta.get("latest_anchor_trade_date") or "").strip()
    if not anchor_text:
        raise RuntimeError("v2.5 realtime metadata is missing latest_anchor_trade_date")
    anchor = pd.Timestamp(anchor_text).normalize()
    close_index = pd.DatetimeIndex(close_df.index).dropna().sort_values()
    history_index = close_index[close_index.normalize() <= anchor]
    if len(history_index) == 0 or pd.Timestamp(history_index[-1]).normalize() != anchor:
        raise RuntimeError(f"v2.5 validated realtime close history does not reach anchor {anchor.date()}")
    return pd.DatetimeIndex(history_index)


V2_5_REWRITE_AUDIT_KEY_COLUMNS = [
    "return_net",
    "holding",
    "next_holding",
    "base_pre_cost_return",
    "current_execution_scale",
    "next_session_actionable_scale",
    "target_vol_scale_raw",
    "target_vol_realized_vol",
    "base_trade_cost",
    "base_trade_cost_scaled",
    "scale_change_cost",
    "financing_cost",
    "annualized_log_wls_score",
]
V2_5_SIGNAL_AUDIT_ALLOWED_TAIL_ROWS = LOOKBACK + 5
V2_5_REWRITE_AUDIT_ALLOWED_TAIL_ROWS_BY_COLUMN = {
    "holding": V2_5_SIGNAL_AUDIT_ALLOWED_TAIL_ROWS,
    "next_holding": V2_5_SIGNAL_AUDIT_ALLOWED_TAIL_ROWS,
    "base_pre_cost_return": V2_5_SIGNAL_AUDIT_ALLOWED_TAIL_ROWS,
    "annualized_log_wls_score": V2_5_SIGNAL_AUDIT_ALLOWED_TAIL_ROWS,
}


def _v2_5_changed_columns(
    previous: pd.DataFrame,
    candidate: pd.DataFrame,
    key_columns: list[str],
    allowed_tail_rows: int,
    atol: float = 1e-9,
    rtol: float = 1e-7,
    column_allowed_tail_rows: dict[str, int] | None = None,
) -> dict[str, int]:
    prev = v2_0.base_mod._normalise_dated_frame(previous, "v2.5 previous diagnostic")
    cand = v2_0.base_mod._normalise_dated_frame(candidate, "v2.5 candidate diagnostic")
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
        left_num = pd.to_numeric(left, errors="coerce")
        right_num = pd.to_numeric(right, errors="coerce")
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


def _v2_5_audit_change_summary(
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
    prev = v2_0.base_mod._normalise_dated_frame(previous, "v2.5 previous diagnostic")
    cand = v2_0.base_mod._normalise_dated_frame(candidate, "v2.5 candidate diagnostic")
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


def _write_v2_5_rewrite_diagnostics(
    previous: pd.DataFrame,
    candidate: pd.DataFrame,
    allowed_tail_rows: int,
    audit_path: Path,
    column_allowed_tail_rows: dict[str, int] | None = None,
) -> Path:
    raw_input_cols = {
        "base_pre_cost_return",
        "target_vol_scale_raw",
        "target_vol_realized_vol",
        "base_trade_cost",
        "annualized_log_wls_score",
    }
    changed = _v2_5_changed_columns(
        previous,
        candidate,
        V2_5_REWRITE_AUDIT_KEY_COLUMNS,
        allowed_tail_rows,
        column_allowed_tail_rows=column_allowed_tail_rows,
    )
    audit_summary = _v2_5_audit_change_summary(previous, candidate, audit_path)
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
            "threshold_path_dependent_state_changed means frozen-date differences are confined to threshold "
            "state, cost fields, or derived returns and should be reviewed as path transmission."
        ),
    }
    diagnostics_path = OUTPUT_DIR / f"{OUTPUT_PREFIX}_historical_rewrite_diagnostics.json"
    _atomic_write_text(diagnostics_path, _json_dumps(diagnostics), encoding="utf-8")
    return diagnostics_path


def _generate_v2_5_outputs_unlocked() -> tuple[dict[str, object], pd.DataFrame, pd.DataFrame]:
    ensure_output_dir()
    official_v2_0_out = _load_official_v2_0_out()
    reference_summary, base_gross_cached, turnover_df = v2_0.embedded_context._load_embedded_base_context()
    stale_outputs = incompatible_v2_5_outputs()
    close_df = _close_df_from_base(base_gross_cached)
    common_index = build_v2_5_common_index(close_df, official_v2_0_out.index)
    out = build_v2_5_result(close_df, turnover_df, common_index)
    if COSTED_NAV_CSV.exists():
        previous = _read_costed_nav_csv(parse_dates=["date"])
        audit_path = OUTPUT_DIR / f"{OUTPUT_PREFIX}_historical_rewrite_audit.csv"
        allowed_tail_rows = _v2_5_rewrite_allowed_tail_rows()
        candidate = out.rename_axis("date").reset_index()
        try:
            v2_0.base_mod.assert_no_historical_rewrite(
                previous=previous,
                candidate=candidate,
                key_columns=V2_5_REWRITE_AUDIT_KEY_COLUMNS,
                allowed_tail_rows=allowed_tail_rows,
                label="v2.5 official costed NAV",
                audit_path=audit_path,
                column_allowed_tail_rows=V2_5_REWRITE_AUDIT_ALLOWED_TAIL_ROWS_BY_COLUMN,
            )
        except RuntimeError as exc:
            try:
                diagnostics_path = _write_v2_5_rewrite_diagnostics(
                    previous=previous,
                    candidate=candidate,
                    allowed_tail_rows=allowed_tail_rows,
                    audit_path=audit_path,
                    column_allowed_tail_rows=V2_5_REWRITE_AUDIT_ALLOWED_TAIL_ROWS_BY_COLUMN,
                )
            except Exception as diag_exc:
                raise RuntimeError(
                    f"{exc} v2.5 rewrite diagnostics failed: {type(diag_exc).__name__}: {diag_exc}"
                ) from exc
            raise RuntimeError(f"{exc} v2.5 rewrite diagnostics written to {diagnostics_path}.") from exc

    freshness_proof = v2_0.assert_top100_candidate_fresh(
        out.index,
        expected_latest_date=out.index.max(),
        label="v2.5 official costed NAV",
    )
    bundle_targets = [
        COSTED_NAV_CSV, NAV_CSV, LATEST_SIGNAL_CSV, PERF_SUMMARY_CSV, PERF_YEARLY_CSV,
        PERF_NAV_CSV, PERF_JSON, PERF_PNG, SUMMARY_JSON,
    ]
    stage_scope = tempfile.TemporaryDirectory(prefix=f".{OUTPUT_PREFIX}.stage.", dir=OUTPUT_DIR)
    stage_root = Path(stage_scope.name)
    staged_files = {target: stage_root / target.name for target in bundle_targets}
    _atomic_write_csv(out, staged_files[COSTED_NAV_CSV], index_label="date", encoding="utf-8-sig")
    _atomic_write_csv(out.rename_axis("date").reset_index(), staged_files[NAV_CSV], index=False, encoding="utf-8-sig")
    data_lineage = dict(v2_0.overlay_mod._build_v2_data_lineage())
    performance_source_label = v2_0.overlay_mod.proxy_aware_performance_source_label(data_lineage, "costed_v2_5")
    signal_row = _build_signal_row(out, reference_summary)
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

    valid_log_wls_index = pd.DatetimeIndex(_valid_log_wls_index(close_df))
    valid_log_wls_index = pd.DatetimeIndex(valid_log_wls_index[valid_log_wls_index >= FORMAL_START_DATE]).sort_values()
    data_lineage["v2_5_common_index_gap"] = _common_index_gap_summary(common_index, valid_log_wls_index)
    data_lineage["v2_5_rebalance_timing_note"] = (
        "The biweekly file/output name is inherited from the embedded Top100 member proxy; "
        "the v2.5 close-confirmed overlay evaluates its microcap-only log-WLS threshold daily."
    )
    summary = copy.deepcopy(reference_summary)
    summary.pop("summary_version_key", None)
    summary["strategy"] = OUTPUT_PREFIX
    summary["version"] = VERSION
    summary["version_role"] = EXPECTED_VERSION_ROLE
    summary["version_note"] = (
        "Formal v2.5 microcap-only log-WLS threshold without target-vol overlay. Uses exp half-life 3.0 weighted log slope on "
        "17 trading days of unhedged microcap Top100 NAV, enters when score is above 46% and exits when score is at or below 25%, "
        "removes the hedge leg, applies no R2 gate, no single-trade stop-loss, no equity drawdown stop, "
        "no momentum-decay exit, no overheat exit, no cash-day yield, no financing, and no target-volatility scaling."
    )
    summary.setdefault("core_params", {})
    summary["core_params"]["fixed_hedge_ratio"] = BASE_HEDGE_RATIO
    summary["core_params"]["lookback"] = LOOKBACK
    summary["core_params"]["signal_spread_hedge_ratio"] = SIGNAL_SPREAD_HEDGE_RATIO
    summary["core_params"]["execution_hedge_ratio"] = EXECUTION_HEDGE_RATIO
    summary["core_params"]["hedge_removed"] = True
    summary["core_params"]["signal_model"] = {
        "type": "microcap_only_log_wls_exp",
        "lookback": LOOKBACK,
        "halflife": HALFLIFE,
        "weights_oldest_to_newest": list(exp_weights()),
        "score_definition": "annualized weighted log slope of unhedged microcap Top100 NAV",
        "nav_csv_momentum_gap_column_alias_note": (
            "momentum_gap stores annualized microcap-only log-WLS score for v2.0 compatibility, not raw microcap minus hedge gap"
        ),
        "r2_gate": None,
        "legacy_momentum_gap_field": "same value as annualized_log_wls_score for v2.0 compatibility",
    }
    summary["core_params"]["entry_threshold"] = ENTRY_THRESHOLD
    summary["core_params"]["exit_threshold"] = EXIT_THRESHOLD
    summary["core_params"]["signal_quality_derisk"] = {"enabled": False, "type": "removed_no_peak_decay"}
    summary["core_params"]["single_trade_stop_loss"] = {"enabled": False}
    summary["core_params"]["equity_drawdown_overlay"] = {"enabled": False}
    summary["core_params"]["momentum_decay_overlay"] = {"enabled": False}
    summary["core_params"]["overheat_overlay"] = {"enabled": False}
    summary["core_params"]["target_volatility_scaling"] = {"enabled": TARGET_VOL_ENABLED}
    summary["core_params"]["cash_day_yield"] = {"enabled": CASH_DAY_YIELD_ENABLED}
    summary["core_params"]["financing"] = {"enabled": FINANCING_ENABLED}
    summary["core_params"]["parameter_replacement"] = {
        "replaced_original_v2_5": True,
        "selected_from_run": "quant_param_scan_runs/20260629_microcap_top100_v2_5_microcap_only_log_wls_layer5_entry_threshold",
        "comparison_run": "quant_param_scan_runs/20260629_microcap_top100_v2_5_original_vs_selected_params_comparison",
        "selected_params": {
            "lookback": LOOKBACK,
            "halflife": HALFLIFE,
            "entry_threshold": ENTRY_THRESHOLD,
            "exit_threshold": EXIT_THRESHOLD,
            "target_vol_enabled": TARGET_VOL_ENABLED,
        },
    }
    summary["latest_trade_date"] = str(pd.Timestamp(signal_row.iloc[0]["date"]).date())
    summary["latest_nav_date"] = str(pd.Timestamp(out.index.max()).date())
    summary["latest_signal"] = signal_row.iloc[0].drop(labels=["date"], errors="ignore").to_dict()
    summary["data_lineage"] = data_lineage
    summary["data_freshness_proof"] = freshness_proof
    v2_0.overlay_mod.attach_proxy_source_summary_fields(
        summary,
        data_lineage,
        source_label="costed_v2_5",
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
            extra_daily_paths={"v2_5_costed_nav": COSTED_NAV_CSV},
        ),
    ) as promotion_paths:
        for target, source in staged_files.items():
            shutil.copy2(source, promotion_paths[target])
    stage_scope.cleanup()
    COMPATIBILITY_AUDIT_JSON.unlink(missing_ok=True)
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


def _v2_5_rewrite_allowed_tail_rows() -> int:
    # v2.5 now depends on the signal lookback only. Threshold state can still
    # propagate if a revised row flips a holding decision and does not quickly
    # resync, so keep a modest tail for path-dependent transmission review.
    return max(LOOKBACK + 20, 40)


def generate_v2_5_outputs() -> tuple[dict[str, object], pd.DataFrame, pd.DataFrame]:
    with v2_5_output_lock():
        return _generate_v2_5_outputs_unlocked()


def _build_realtime_v2_5_outputs_unlocked() -> tuple[pd.DataFrame, dict[str, object], pd.DataFrame]:
    ensure_output_dir()
    realtime_base = v2_0.realtime_core.load_realtime_base()
    close_df = _close_df_from_realtime(realtime_base.realtime_close_df)
    freshness_calendar = _build_realtime_v2_5_official_index(close_df, realtime_base.meta)
    signal_official_index = freshness_calendar
    if bool(realtime_base.meta.get("snapshot_row_appended", False)) and len(close_df.index):
        signal_official_index = signal_official_index.union(pd.DatetimeIndex([close_df.index[-1]])).sort_values()
    common_index = build_v2_5_common_index(close_df, signal_official_index)
    gross = build_microcap_log_wls_gross(close_df, common_index)
    costed = apply_cost(gross, realtime_base.turnover_df)
    is_snapshot = bool(realtime_base.meta.get("snapshot_row_appended", False))
    signal_timing = "intraday_hypothetical_if_now_close" if is_snapshot else "close_confirmed_anchor"
    out = apply_no_target_vol(costed)
    signal_row = _build_signal_row(out, realtime_base.reference_summary)
    signal_row = v2_0.realtime_core.base_mod.augment_signal_with_member_rebalance(
        signal_row,
        realtime_base.context.get("changes_df"),
    )
    v2_0.overlay_mod._apply_realtime_meta_columns_to_signal_row(signal_row, realtime_base.meta)
    signal_row["quote_coverage"] = f"{realtime_base.meta.get('member_price_count', 0)}/{realtime_base.meta.get('member_count', 0)}"
    signal_row["target_vol_signal_timing"] = ""
    signal_row["signal_timing"] = signal_timing
    signal_row["official_close_confirmed_signal"] = not is_snapshot
    signal_row["base_fingerprint"] = _json_dumps(current_base_fingerprint())
    signal_row["strategy_fingerprint"] = _json_dumps(current_realtime_fingerprint())
    _atomic_write_text(REALTIME_SIGNAL_CSV, signal_row.to_csv(index=False), encoding="utf-8")
    return signal_row, realtime_base.meta, out


def build_realtime_v2_5_outputs() -> tuple[pd.DataFrame, dict[str, object], pd.DataFrame]:
    with v2_5_realtime_output_lock():
        return _build_realtime_v2_5_outputs_unlocked()


def _print_scale_fields(row: pd.Series, include_frozen: bool = False) -> None:
    print(f"current_execution_scale: {_safe_float(row.get('current_execution_scale', row.get('execution_scale')), 0.0):.2f}")
    print(f"next_session_target_scale: {_safe_float(row.get('next_session_target_scale'), 0.0):.2f}")
    print(f"next_session_actionable_scale: {_safe_float(row.get('next_session_actionable_scale'), 0.0):.2f}")
    print(f"raw_scale_delta: {_safe_float(row.get('raw_scale_delta', row.get('scale_delta')), 0.0):+.2f}")
    print(f"actionable_scale_delta: {_safe_float(row.get('actionable_scale_delta', row.get('scale_delta')), 0.0):+.2f}")
    print(f"next_session_turnover: {_safe_float(row.get('next_session_turnover'), 0.0):.4f}")
    print(f"next_session_trade_cost_est: {_safe_float(row.get('next_session_trade_cost_est'), 0.0):.4%}")


def _print_signal_query() -> None:
    _, signal_df, _ = generate_v2_5_outputs()
    row = signal_df.iloc[0]
    print("signal")
    print("strategy_version: v2.5")
    print("base_version: embedded_v2_base")
    print(
        "signal_model: microcap-only log-WLS exp half-life 3.0, lookback 17, "
        "entry threshold 46%, exit threshold 25%, no R2 gate"
    )
    print("overlay: no hedge, no stop-loss/DD/decay/overheat overlay, no target-vol/cash-yield/financing")
    print(f"current_holding: {row['current_holding']}")
    print(f"next_holding: {row['next_holding']}")
    print(f"trade_state: {row.get('effective_trade_state', row.get('trade_state', 'hold'))}")
    print(f"holding_trade_state: {row.get('holding_trade_state', row.get('momentum_trade_state', 'hold'))}")
    print(f"scale_trade_state: {row.get('scale_trade_state', 'hold_scale')}")
    print(f"signal_date: {pd.Timestamp(row['date']).strftime('%Y-%m-%d')}")
    print(f"annualized_log_wls_score: {float(row.get('annualized_log_wls_score', row.get('momentum_gap', 0.0))):+.4%}")
    print(f"log_wls_r2: {float(row.get('log_wls_r2', 0.0)):.4f}")
    print("momentum_gap_legacy_note: legacy field is the annualized microcap-only log-WLS score, not plain gap")
    _print_scale_fields(row, include_frozen=False)
    print(f"official_close_confirmed_signal: {row.get('official_close_confirmed_signal', True)}")
    print(SUMMARY_JSON)
    print(LATEST_SIGNAL_CSV)


def _print_realtime_signal_query() -> None:
    def emit() -> None:
        signal_df, meta, _ = build_realtime_v2_5_outputs()
        row = signal_df.iloc[0]
        print("realtime_signal")
        print("strategy_version: v2.5")
        print("base_version: embedded_v2_base")
        print(
            "signal_model: microcap-only log-WLS exp half-life 3.0, lookback 17, "
            "entry threshold 46%, exit threshold 25%, no R2 gate"
        )
        print("overlay: no hedge, no stop-loss/DD/decay/overheat overlay, no target-vol/cash-yield/financing")
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
        print("momentum_gap_legacy_note: legacy field is the annualized microcap-only log-WLS score, not plain gap")
        print(f"quote_source: {meta.get('quote_source')}")
        print(f"hedge_quote_source: {meta.get('hedge_quote_source')}")
        print(f"quote_coverage: {meta.get('member_price_count')}/{meta.get('member_count')}")
        print(REALTIME_SIGNAL_CSV)

    try:
        v2_0.run_realtime_query_with_fresh_state(emit)
    except Exception as exc:
        if v2_0.is_realtime_actionability_error(exc):
            v2_0.print_realtime_blocked_result("v2.5", exc)
            return
        raise


def _print_performance_query(query: str) -> None:
    with v2_5_output_lock():
        _summary, _signal_row, perf_df = _generate_v2_5_outputs_unlocked()
        perf_df = perf_df.rename_axis("date").sort_index()
        old_title = v2_0.embedded_context.base_mod.STRATEGY_TITLE
        v2_0.embedded_context.base_mod.STRATEGY_TITLE = "Top100 Microcap Mom16 Biweekly v2.5"
        try:
            v2_0.embedded_context.base_mod.build_performance_outputs(
                perf_df=perf_df,
                ret_col="return_net",
                nav_col="nav_net",
                source_label="costed_v2_5",
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


def normalize_v2_5_query_text(query: str) -> str:
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
    normalized = normalize_v2_5_query_text(query)
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
    raise ValueError("v2.5 supports: 信号 / 实时信号 / 表现 <区间>")


def main(argv: list[str] | None = None) -> None:
    args = parse_v2_5_args(sys.argv[1:] if argv is None else argv)
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
        generate_v2_5_outputs()
        print(str(SUMMARY_JSON))
        print(str(LATEST_SIGNAL_CSV))
        print(str(COSTED_NAV_CSV))
    finally:
        configure_output_paths(previous_output_prefix, previous_costed_nav_csv)
        v2_0.configure_output_paths(previous_v2_0_output_prefix)
        v2_0._V2_RUNTIME_ARGS = previous_runtime_args

if __name__ == "__main__":
    main()

