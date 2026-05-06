from __future__ import annotations

import argparse
import importlib
import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Sequence


@dataclass(frozen=True)
class StrategySpec:
    version: str
    module_name: str
    builder_name: str
    realtime_csv_attr: str


@dataclass(frozen=True)
class StrategyRun:
    version: str
    row: dict[str, Any]
    meta: dict[str, Any]
    csv_path: Path | None


DEFAULT_SPECS: tuple[StrategySpec, ...] = (
    StrategySpec(
        version="v1.6",
        module_name="microcap_top100_mom16_biweekly_live_v1_6",
        builder_name="build_realtime_v1_6_outputs",
        realtime_csv_attr="REALTIME_SIGNAL_CSV",
    ),
    StrategySpec(
        version="v1.8",
        module_name="microcap_top100_mom16_biweekly_live_v1_8",
        builder_name="build_realtime_v1_8_outputs",
        realtime_csv_attr="REALTIME_SIGNAL_CSV",
    ),
)


def _first_present(row: dict[str, Any], meta: dict[str, Any], names: Iterable[str], default: Any = "") -> Any:
    for name in names:
        value = row.get(name)
        if value not in (None, ""):
            return value
        value = meta.get(name)
        if value not in (None, ""):
            return value
    return default


def _format_decimal(value: Any, digits: int = 2, signed: bool = False, percent: bool = False) -> str:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return str(value) if value not in (None, "") else ""
    if percent:
        number *= 100.0
        sign = "+" if signed else ""
        return f"{number:{sign}.{digits}f}%"
    sign = "+" if signed else ""
    return f"{number:{sign}.{digits}f}"


def run_strategy(spec: StrategySpec) -> StrategyRun:
    module = importlib.import_module(spec.module_name)
    builder = getattr(module, spec.builder_name)
    signal_df, meta, _result = builder()
    if signal_df is None or len(signal_df) == 0:
        raise RuntimeError(f"{spec.version} realtime builder returned no signal rows")
    row = signal_df.iloc[0].to_dict()
    csv_path = getattr(module, spec.realtime_csv_attr, None)
    return StrategyRun(
        version=spec.version,
        row=row,
        meta=dict(meta or {}),
        csv_path=Path(csv_path) if csv_path is not None else None,
    )


def format_run(result: StrategyRun) -> str:
    row = result.row
    meta = result.meta
    trade_state = _first_present(
        row,
        meta,
        ["effective_trade_state", "trade_state", "holding_trade_state", "momentum_trade_state"],
        "unknown",
    )
    quote_coverage = _first_present(row, meta, ["quote_coverage"], "")
    if not quote_coverage:
        member_price_count = _first_present(row, meta, ["member_price_count"], "")
        member_count = _first_present(row, meta, ["member_count"], "")
        if member_price_count != "" and member_count != "":
            quote_coverage = f"{member_price_count}/{member_count}"

    lines = [
        f"===== {result.version} realtime_signal =====",
        f"strategy_version: {result.version}",
        f"snapshot_time: {_first_present(row, meta, ['snapshot_time'], '')}",
        f"latest_anchor_trade_date: {_first_present(row, meta, ['latest_anchor_trade_date'], '')}",
        f"quote_trade_date: {_first_present(row, meta, ['quote_trade_date'], '')}",
        f"current_holding: {_first_present(row, meta, ['current_holding'], '')}",
        f"next_holding: {_first_present(row, meta, ['next_holding'], '')}",
        f"trade_state: {trade_state}",
        "current_execution_scale: "
        + _format_decimal(_first_present(row, meta, ["current_execution_scale", "execution_scale"], "")),
        "next_session_actionable_scale: "
        + _format_decimal(
            _first_present(row, meta, ["next_session_actionable_scale", "next_session_target_scale"], "")
        ),
        "microcap_mom: " + _format_decimal(_first_present(row, meta, ["microcap_mom"], ""), digits=4, signed=True, percent=True),
        "hedge_mom: " + _format_decimal(_first_present(row, meta, ["hedge_mom"], ""), digits=4, signed=True, percent=True),
        "momentum_gap: " + _format_decimal(_first_present(row, meta, ["momentum_gap"], ""), digits=4, signed=True, percent=True),
        f"quote_source: {_first_present(row, meta, ['quote_source'], '')}",
        f"hedge_quote_source: {_first_present(row, meta, ['hedge_quote_source'], '')}",
        f"quote_coverage: {quote_coverage}",
        f"official_close_confirmed_signal: {_first_present(row, meta, ['official_close_confirmed_signal'], False)}",
    ]

    warning = _first_present(row, meta, ["fallback_warning", "stale_data_warning", "tail_jitter_note"], "")
    if warning:
        lines.append(f"warning: {warning}")
    if result.csv_path is not None:
        lines.append(f"realtime_signal_csv: {_display_path(result.csv_path)}")
    return "\n".join(lines)


def _jsonable(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return value


def _display_path(path: Path) -> str:
    return path.as_posix()


def main(argv: Sequence[str] | None = None, specs: Sequence[StrategySpec] = DEFAULT_SPECS) -> int:
    parser = argparse.ArgumentParser(
        description="Run Top100 v1.6 and v1.8 realtime signals from the official strategy modules."
    )
    parser.add_argument("--json", action="store_true", help="print machine-readable JSON instead of text")
    args = parser.parse_args(argv)

    runs: list[StrategyRun] = []
    failures: list[str] = []
    for spec in specs:
        try:
            runs.append(run_strategy(spec))
        except Exception as exc:
            failures.append(f"{spec.version}: {exc}")

    if args.json:
        payload = {
            "signals": [
                {
                    "version": run.version,
                    "row": {key: _jsonable(value) for key, value in run.row.items()},
                    "meta": {key: _jsonable(value) for key, value in run.meta.items()},
                    "realtime_signal_csv": _display_path(run.csv_path) if run.csv_path is not None else None,
                }
                for run in runs
            ],
            "failures": failures,
        }
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        for index, run in enumerate(runs):
            if index:
                print()
            print(format_run(run))
        for failure in failures:
            if runs:
                print()
            print(f"ERROR: {failure}", file=sys.stderr)

    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
