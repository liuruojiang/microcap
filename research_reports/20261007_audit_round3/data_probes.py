"""Save observable boundary behavior from real functions and read-back fixture files."""
from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
from types import SimpleNamespace
import sys

import numpy as np
import pandas as pd
from _pytest.monkeypatch import MonkeyPatch


ROOT = Path(__file__).resolve().parents[2]


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def record_call(fn):
    try:
        return {"status": "accepted", **fn()}
    except Exception as exc:
        return {"status": "rejected", "exception_type": type(exc).__name__, "exception": str(exc)}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-root", type=Path)
    parser.add_argument("--out-dir", type=Path, required=True)
    args = parser.parse_args()
    source_root = ROOT if args.source_root is None else args.source_root.resolve()
    if args.source_root is not None:
        os.environ["MICROCAP_ROUND3_SOURCE_ROOT"] = str(source_root)
    else:
        os.environ.pop("MICROCAP_ROUND3_SOURCE_ROOT", None)
    out = args.out_dir.resolve()
    out.mkdir(parents=True, exist_ok=False)
    sys.path.insert(0, str(ROOT))
    spec = importlib.util.spec_from_file_location("_round3_probe_test_helpers", ROOT / "tests/test_round3_data_boundary.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    source_files = ["microcap_top100_mom16_biweekly_live.py", "microcap_top100_mom16_biweekly_live_v2_0.py",
                    "microcap_top100_mom16_biweekly_live_v2_3.py", "microcap_top100_mom16_biweekly_live_v2_5.py"]
    source_before = {name: sha(source_root / name) for name in source_files}
    report = {"scope": "isolated_diagnostic_only", "source_root": str(source_root), "source_sha256": source_before,
              "test_sha256": sha(ROOT / "tests/test_round3_data_boundary.py"), "pandas_version": pd.__version__,
              "probes": {}}
    for label, base in [("shared_v20", module.v20.base_mod), ("mainline", module.mainline)]:
        frame = pd.DataFrame({"date": ["2026-09-01", "2026-09-02 00:00:00"], "value": [1.0, 2.0]})
        def daily():
            parsed = base._normalise_dated_frame(frame, "round3 mixed daily fixture")
            return {"rows": len(parsed), "dates": parsed.index.strftime("%Y-%m-%d").tolist()}
        report["probes"][f"mixed_daily_{label}"] = record_call(daily)
        previous = pd.DataFrame({"date": pd.bdate_range("2026-09-01", periods=2),
                                 "warmup": pd.Series([np.nan, np.nan], dtype=object)})
        candidate = pd.DataFrame({"date": pd.bdate_range("2026-09-01", periods=2),
                                  "warmup": pd.Series([None, None], dtype=object)})
        def warmup():
            base.assert_no_historical_rewrite(previous, candidate, ["warmup"], 0, "round3 all-null fixture")
            return {"previous_isna": previous["warmup"].isna().tolist(), "candidate_isna": candidate["warmup"].isna().tolist()}
        report["probes"][f"same_null_warmup_{label}"] = record_call(warmup)

    for label, second_signal, rate in [("legal_mixed", "2026-09-02 00:00:00", 0.002),
                                       ("missing_date", None, 0.002), ("missing_date_infinite_rate", None, np.inf)]:
        case_dir = out / label
        case_dir.mkdir()
        events = pd.DataFrame({"rebalance_date": ["2026-09-01", second_signal],
                               "execution_date": ["2026-09-01", "2026-09-02 00:00:00"],
                               "execution_timing": "close", "two_side_cost_rate": [0.001, rate]})
        with MonkeyPatch.context() as patch:
            loader, _ = module._embedded_inputs(case_dir, patch, events)
            def embedded():
                _, gross, consumed = loader()
                costs = module.v20.cost_mod.map_rebalance_apply_costs(gross.index, consumed)
                costed = module.v20.cost_mod.apply_cost_model(gross, consumed)
                consumed.to_csv(case_dir / "consumed_turnover.csv", index=False)
                costs.rename("cost").to_csv(case_dir / "mapped_costs.csv", index_label="date")
                costed.to_csv(case_dir / "actual_costed_from_consumed_events.csv", index_label="date")
                result = {"raw_rows_read_back": len(pd.read_csv(case_dir / "turnover.csv")),
                        "consumed_rows": len(consumed), "sum_mapped_cost": float(costs.sum()),
                        "gross_rows": len(gross), "gross_latest_date": str(gross.index[-1].date()),
                        "sep02_executed_holding": str(costed.loc["2026-09-02", "holding"]),
                        "sep02_charged_rebalance_cost": float(costed.loc["2026-09-02", "rebalance_cost"]),
                        "sep02_return_net": float(costed.loc["2026-09-02", "return_net"])}
                if label == "legal_mixed":
                    control = module.v20.cost_mod.apply_cost_model(gross, events)
                    control.to_csv(case_dir / "control_costed_from_all_raw_events.csv", index_label="date")
                    result["sep02_control_charged_rebalance_cost"] = float(control.loc["2026-09-02", "rebalance_cost"])
                    result["sep02_control_return_net"] = float(control.loc["2026-09-02", "return_net"])
                return result
            report["probes"][f"actual_embedded_{label}"] = record_call(embedded)

    dates = pd.bdate_range("2026-09-01", periods=3)
    duplicate = pd.DataFrame({"rebalance_date": ["2026-09-01"] * 2, "execution_date": ["2026-09-01"] * 2,
                              "execution_timing": "close", "two_side_cost_rate": [0.006] * 2})
    duplicate.to_csv(out / "duplicate_events.csv", index=False)
    report["probes"]["duplicate_mapper"] = record_call(lambda: {
        "mapped_costs": module.v20.cost_mod.map_rebalance_apply_costs(dates, duplicate).tolist()})
    weekend = pd.DataFrame({"rebalance_date": ["2026-09-05"], "execution_date": ["2026-09-07"],
                            "execution_timing": ["next_open"], "two_side_cost_rate": [0.006]})
    weekend.to_csv(out / "weekend_signal.csv", index=False)
    report["probes"]["weekend_signal"] = record_call(lambda: {
        "mapped_costs": module.v20.cost_mod.map_rebalance_apply_costs(
            pd.to_datetime(["2026-09-04", "2026-09-07", "2026-09-08"]), weekend).tolist()})
    gross = pd.DataFrame({"holding": ["cash", "microcap", "microcap"],
                          "next_holding": ["microcap", "microcap", "cash"], "return": 0.0}, index=dates)
    mixed = pd.DataFrame({"rebalance_date": [dates[0], dates[1]], "execution_date": [dates[0], dates[2]],
                          "execution_timing": ["close", "next_open"], "two_side_cost_rate": [0.006, 0.006]})
    mixed.to_csv(out / "mixed_timing_events.csv", index=False)
    for label, rows in [("original_order", mixed), ("reverse_order", mixed.iloc[::-1])]:
        report["probes"][f"mixed_timing_{label}"] = record_call(lambda: {
            "entry_exit_costs": module.v20.cost_mod.apply_cost_model(gross, rows)["entry_exit_cost"].tolist()})

    for version in ["2.0", "2.3", "2.5"]:
        case_dir = out / f"migration_v{version}"
        case_dir.mkdir()
        with MonkeyPatch.context() as patch:
            verifier, report_path, payload, previous, candidate, audit = module.migration_case.__wrapped__(
                SimpleNamespace(param=version), case_dir, patch)
            payload["approved"] = 1
            report_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
            report["probes"][f"migration_integer_approval_v{version}"] = {
                "report_approved_raw_type": type(json.loads(report_path.read_text(encoding="utf-8"))["approved"]).__name__,
                "accepted": verifier(report_path, previous, candidate, audit)}

    report["source_sha256_after"] = {name: sha(source_root / name) for name in source_files}
    report["source_unchanged_during_probe"] = source_before == report["source_sha256_after"]
    report["fixture_files_read_back"] = {str(path.relative_to(out)): {"bytes": path.stat().st_size, "sha256": sha(path)}
                                         for path in sorted(out.rglob("*")) if path.is_file()}
    destination = out / "data_probes_result.json"
    destination.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"output": str(destination), "source_unchanged_during_probe": report["source_unchanged_during_probe"],
                      "probe_count": len(report["probes"]), "fixture_readback_count": len(report["fixture_files_read_back"])}, ensure_ascii=False))


if __name__ == "__main__":
    main()
