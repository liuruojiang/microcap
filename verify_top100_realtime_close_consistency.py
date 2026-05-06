from __future__ import annotations

import argparse
import importlib
import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence

import pandas as pd

from run_top100_v1_6_v1_8_realtime_signals import ensure_static_realtime_inputs


ROOT = Path(__file__).resolve().parent
OUTPUT_DIR = ROOT / "outputs"
DEFAULT_OUTPUT_JSON = OUTPUT_DIR / "top100_realtime_close_consistency_summary.json"
DEFAULT_OUTPUT_CSV = OUTPUT_DIR / "top100_realtime_close_consistency_details.csv"


@dataclass(frozen=True)
class StrategySpec:
    version: str
    module_name: str
    close_builder_name: str
    realtime_builder_name: str


@dataclass(frozen=True)
class ComparisonResult:
    version: str
    passed: bool
    details: pd.DataFrame
    realtime_meta: dict[str, Any]


DEFAULT_SPECS: tuple[StrategySpec, ...] = (
    StrategySpec(
        version="v1.6",
        module_name="microcap_top100_mom16_biweekly_live_v1_6",
        close_builder_name="generate_v1_6_outputs",
        realtime_builder_name="build_realtime_v1_6_outputs",
    ),
    StrategySpec(
        version="v1.8",
        module_name="microcap_top100_mom16_biweekly_live_v1_8",
        close_builder_name="generate_v1_8_outputs",
        realtime_builder_name="build_realtime_v1_8_outputs",
    ),
)

TEXT_FIELDS: dict[str, tuple[str, ...]] = {
    "current_holding": ("current_holding",),
    "next_holding": ("next_holding",),
    "trade_state": ("effective_trade_state", "trade_state", "momentum_trade_state"),
}

NUMERIC_FIELDS: dict[str, tuple[str, ...]] = {
    "current_execution_scale": ("current_execution_scale", "execution_scale"),
    "next_session_actionable_scale": (
        "next_session_actionable_scale",
        "next_session_target_scale",
        "raw_next_target_scale",
    ),
    "microcap_mom": ("microcap_mom",),
    "hedge_mom": ("hedge_mom",),
    "momentum_gap": ("momentum_gap",),
}


def _jsonable(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    if hasattr(value, "isoformat"):
        return value.isoformat()
    if pd.isna(value) if not isinstance(value, (list, dict, tuple, set)) else False:
        return None
    return value


def _first_present(row: dict[str, Any], meta: dict[str, Any], names: Sequence[str], default: Any = None) -> Any:
    for name in names:
        value = row.get(name)
        if value not in (None, ""):
            return value
        value = meta.get(name)
        if value not in (None, ""):
            return value
    return default


def _date_text(value: Any) -> str:
    if value in (None, ""):
        return ""
    try:
        return str(pd.Timestamp(value).date())
    except Exception:
        return str(value)


def _compare_text(version: str, field: str, realtime_value: Any, close_value: Any) -> dict[str, Any]:
    rt = "" if realtime_value is None else str(realtime_value)
    close = "" if close_value is None else str(close_value)
    return {
        "version": version,
        "field": field,
        "comparison_type": "text",
        "realtime_value": rt,
        "close_value": close,
        "abs_diff": "",
        "tolerance": "",
        "passed": bool(rt == close and rt != ""),
    }


def _compare_numeric(
    version: str,
    field: str,
    realtime_value: Any,
    close_value: Any,
    numeric_tolerance: float,
) -> dict[str, Any]:
    try:
        rt = float(realtime_value)
        close = float(close_value)
        diff = abs(rt - close)
        passed = diff <= float(numeric_tolerance)
    except (TypeError, ValueError):
        rt = realtime_value
        close = close_value
        diff = ""
        passed = False
    return {
        "version": version,
        "field": field,
        "comparison_type": "numeric",
        "realtime_value": rt,
        "close_value": close,
        "abs_diff": diff,
        "tolerance": float(numeric_tolerance),
        "passed": bool(passed),
    }


def compare_signal_rows(
    version: str,
    realtime_row: dict[str, Any],
    close_row: dict[str, Any],
    realtime_meta: dict[str, Any],
    numeric_tolerance: float,
) -> ComparisonResult:
    records: list[dict[str, Any]] = []
    close_signal_date = _date_text(close_row.get("date"))
    latest_anchor_date = _date_text(
        _first_present(realtime_row, realtime_meta, ["latest_anchor_trade_date"], default="")
    )
    quote_trade_date = _date_text(_first_present(realtime_row, realtime_meta, ["quote_trade_date"], default=""))

    records.append(_compare_text(version, "latest_anchor_trade_date", latest_anchor_date, close_signal_date))
    records.append(_compare_text(version, "quote_trade_date", quote_trade_date, close_signal_date))

    for field, aliases in TEXT_FIELDS.items():
        records.append(
            _compare_text(
                version,
                field,
                _first_present(realtime_row, realtime_meta, aliases),
                _first_present(close_row, {}, aliases),
            )
        )

    for field, aliases in NUMERIC_FIELDS.items():
        records.append(
            _compare_numeric(
                version,
                field,
                _first_present(realtime_row, realtime_meta, aliases),
                _first_present(close_row, {}, aliases),
                numeric_tolerance=numeric_tolerance,
            )
        )

    details = pd.DataFrame(records)
    return ComparisonResult(
        version=version,
        passed=bool(details["passed"].all()),
        details=details,
        realtime_meta=dict(realtime_meta or {}),
    )


def run_strategy_audit(spec: StrategySpec, numeric_tolerance: float) -> ComparisonResult:
    module = importlib.import_module(spec.module_name)
    close_builder = getattr(module, spec.close_builder_name)
    realtime_builder = getattr(module, spec.realtime_builder_name)

    _summary, close_signal_df, _close_result = close_builder()
    realtime_signal_df, realtime_meta, _realtime_result = realtime_builder()
    if close_signal_df is None or close_signal_df.empty:
        raise RuntimeError(f"{spec.version} close builder returned no signal rows")
    if realtime_signal_df is None or realtime_signal_df.empty:
        raise RuntimeError(f"{spec.version} realtime builder returned no signal rows")

    return compare_signal_rows(
        version=spec.version,
        realtime_row=realtime_signal_df.iloc[0].to_dict(),
        close_row=close_signal_df.iloc[0].to_dict(),
        realtime_meta=dict(realtime_meta or {}),
        numeric_tolerance=numeric_tolerance,
    )


def build_summary(results: list[ComparisonResult], output_csv: Path) -> dict[str, Any]:
    versions = {}
    for result in results:
        failed = result.details.loc[~result.details["passed"]]
        versions[result.version] = {
            "passed": bool(result.passed),
            "failed_fields": failed["field"].tolist(),
            "realtime_meta": {key: _jsonable(value) for key, value in result.realtime_meta.items()},
        }
    return {
        "status": "ok" if all(result.passed for result in results) else "fail",
        "versions": versions,
        "details_csv": str(output_csv),
    }


def main(argv: Sequence[str] | None = None, specs: Sequence[StrategySpec] = DEFAULT_SPECS) -> int:
    parser = argparse.ArgumentParser(
        description="Compare Top100 v1.6/v1.8 realtime signals against close-confirmed full outputs."
    )
    parser.add_argument("--numeric-tolerance", type=float, default=1e-6)
    parser.add_argument("--output-json", type=Path, default=DEFAULT_OUTPUT_JSON)
    parser.add_argument("--output-csv", type=Path, default=DEFAULT_OUTPUT_CSV)
    parser.add_argument(
        "--skip-static-input-check",
        action="store_true",
        help="skip active universe/current ST seed checks before running the audit",
    )
    args = parser.parse_args(argv)

    if not args.skip_static_input_check:
        ensure_static_realtime_inputs()

    results: list[ComparisonResult] = []
    failures: list[str] = []
    for spec in specs:
        try:
            results.append(run_strategy_audit(spec, numeric_tolerance=args.numeric_tolerance))
        except Exception as exc:
            failures.append(f"{spec.version}: {exc}")

    detail_frames = [result.details for result in results]
    details = pd.concat(detail_frames, ignore_index=True) if detail_frames else pd.DataFrame()
    args.output_csv.parent.mkdir(parents=True, exist_ok=True)
    args.output_json.parent.mkdir(parents=True, exist_ok=True)
    details.to_csv(args.output_csv, index=False, encoding="utf-8")
    summary = build_summary(results, args.output_csv)
    if failures:
        summary["status"] = "fail"
        summary["failures"] = failures
    args.output_json.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"status: {summary['status']}")
    print(f"details_csv: {args.output_csv}")
    print(f"summary_json: {args.output_json}")
    for version, payload in summary["versions"].items():
        print(f"{version}: {'pass' if payload['passed'] else 'fail'}")
        if payload["failed_fields"]:
            print(f"{version}_failed_fields: {', '.join(payload['failed_fields'])}")
    for failure in failures:
        print(f"ERROR: {failure}", file=sys.stderr)
    return 0 if summary["status"] == "ok" else 1


if __name__ == "__main__":
    raise SystemExit(main())
