"""Isolated v2.3 overheat scan on the exact L4 frozen old-input snapshot.

No official cache, output, or strategy source is modified.
"""
from __future__ import annotations

import hashlib
import itertools
import json
import math
import sys
from pathlib import Path

import numpy as np
import pandas as pd

RUN = Path(__file__).resolve().parent
ROOT = RUN.parents[3]
L4_META = ROOT / "research_reports/20261003_l4_diagnostic/executor/meta.json"
sys.path.insert(0, str(ROOT))
import microcap_top100_mom16_biweekly_live_v2_3 as v  # noqa: E402


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def freeze_check(meta: dict) -> None:
    for path_string, expected in meta["source_sha256"].items():
        path = ROOT / path_string
        assert sha256(path) == expected, f"source changed from L4: {path}"
    for item in meta["files"].values():
        path = Path(item["path"])
        assert sha256(path) == item["sha256"], f"input changed from L4: {path}"


def parameters() -> list[tuple[str, int, float, float, bool]]:
    result = [("formal_baseline", 10, .26, .20, True)]
    for window, trigger, recovery in itertools.product(
        (9, 10, 11), (.255, .26, .265), (.195, .20, .205)
    ):
        if (window, trigger, recovery) == (10, .26, .20):
            continue
        label = f"w{window}_t{round(trigger * 1000):03d}_r{round(recovery * 1000):03d}"
        result.append((label, window, trigger, recovery, True))
    result.extend((
        ("legacy_w10_t270_r210", 10, .27, .21, True),
        ("overheat_off", 10, math.inf, .20, False),
    ))
    assert len(result) == 29
    return result


def main() -> None:
    RUN.mkdir(parents=True, exist_ok=True)
    (RUN / "daily").mkdir(exist_ok=True)
    meta_path = RUN / "scan_meta.json"
    meta = json.loads(meta_path.read_text(encoding="utf-8"))
    l4 = json.loads(L4_META.read_text(encoding="utf-8"))
    freeze_check(l4)

    _, cached, turnover = v.v2_0.embedded_context._load_embedded_base_context()
    freeze_check(l4)
    close = v._close_df_from_base(cached)
    official20 = pd.read_csv(v.v2_0.COSTED_NAV_CSV, parse_dates=["date"]).set_index("date")
    official = pd.read_csv(v.COSTED_NAV_CSV, parse_dates=["date"]).set_index("date")
    index = v.build_v2_3_common_index(close, official20.index)
    gross = v.build_spread_log_wls_gross(close, index)
    assert index.equals(official.index)
    assert len(index) == 3988 and index[-1] == pd.Timestamp("2026-09-30")

    original = {
        "OVERHEAT_FEATURE_WINDOW": v.OVERHEAT_FEATURE_WINDOW,
        "OVERHEAT_TRIGGER_THRESHOLD": v.OVERHEAT_TRIGGER_THRESHOLD,
        "OVERHEAT_RECOVERY_THRESHOLD": v.OVERHEAT_RECOVERY_THRESHOLD,
        "OVERHEAT_RECOVERY_RATIO": v.OVERHEAT_RECOVERY_RATIO,
    }
    assert original == {
        "OVERHEAT_FEATURE_WINDOW": 10,
        "OVERHEAT_TRIGGER_THRESHOLD": .26,
        "OVERHEAT_RECOVERY_THRESHOLD": .20,
        "OVERHEAT_RECOVERY_RATIO": .20 / .26,
    }
    parity_cols_numeric = [
        "return_net", "nav_net", "total_cost", "rebalance_cost", "entry_exit_cost",
        "annualized_log_wls_score", "overheat_feature_value",
    ]
    parity_cols_state = [
        "holding", "next_holding", "base_holding", "base_next_holding",
        "overheat_risk_off", "overheat_exit_triggered",
        "overheat_block_entry_triggered", "overheat_reentry_triggered",
    ]
    summary_rows: list[dict] = []
    wide_rows: list[dict] = []
    daily_map: dict[str, dict] = {}
    candidates = parameters()
    try:
        for candidate, window, trigger, recovery, enabled in candidates:
            v.OVERHEAT_FEATURE_WINDOW = window
            v.OVERHEAT_TRIGGER_THRESHOLD = trigger
            v.OVERHEAT_RECOVERY_THRESHOLD = recovery
            v.OVERHEAT_RECOVERY_RATIO = recovery / trigger if enabled else 0.0
            out = v.apply_overheat_defense(gross, turnover)
            assert out.index.equals(index)
            assert np.isfinite(out.return_net.to_numpy()).all()
            assert out.holding.iloc[1:].reset_index(drop=True).equals(
                out.next_holding.iloc[:-1].reset_index(drop=True)
            )
            assert (out.loc[out.holding.eq("cash") & out.next_holding.eq("cash"), "return_net"] == 0).all()
            no_cost_nav = (1 + out.base_pre_cost_return).cumprod()
            assert (out.nav_net <= no_cost_nav + 1e-10).all()
            if candidate == "formal_baseline":
                for col in parity_cols_numeric:
                    a = pd.to_numeric(out[col], errors="coerce")
                    b = pd.to_numeric(official[col], errors="coerce")
                    assert np.allclose(a, b, atol=1e-12, rtol=0, equal_nan=True), f"official parity: {col}"
                for col in parity_cols_state:
                    assert out[col].equals(official[col]), f"official parity: {col}"
            if candidate == "overheat_off":
                assert not out[[
                    "overheat_exit_triggered", "overheat_reentry_triggered",
                    "overheat_block_entry_triggered", "overheat_risk_off",
                ]].any().any()
                plain = v.apply_cost(gross, turnover)
                assert np.allclose(out.return_net, plain.return_net, atol=1e-12, rtol=0)
                assert np.allclose(out.nav_net, plain.nav_net, atol=1e-12, rtol=0)

            daily_path = RUN / "daily" / f"{candidate}.csv.gz"
            out.to_csv(daily_path, index_label="date", compression={"method": "gzip", "mtime": 0})
            daily_map[candidate] = {
                "path": str(daily_path), "sha256": sha256(daily_path),
                "rows": len(out), "first_date": str(out.index[0].date()),
                "last_date": str(out.index[-1].date()),
                "feature_window": window,
                "trigger": trigger if enabled else None,
                "recovery": recovery if enabled else None,
                "enabled": enabled,
            }
            wide: dict = {
                "candidate": candidate, "feature_window": window,
                "trigger": trigger if enabled else "OFF",
                "recovery": recovery if enabled else "OFF", "enabled": enabled,
                "risk_off_days": int(out.overheat_risk_off.sum()),
                "exit_events": int(out.overheat_exit_triggered.sum()),
                "blocked_entry_events": int(out.overheat_block_entry_triggered.sum()),
                "reentry_events": int(out.overheat_reentry_triggered.sum()),
            }
            for window_item in v.summarize_required_windows(out.return_net):
                segment = window_item["window"]
                assert not window_item["unavailable_reason"]
                part = out.loc[window_item["start_date"]:window_item["end_date"]]
                ann_return = float(window_item["annual_pct"]) / 100
                ann_vol = float(window_item["vol_pct"]) / 100
                max_dd = float(window_item["max_drawdown_pct"]) / 100
                sharpe = float(window_item["sharpe"])
                summary_rows.append({
                    "candidate": candidate, "segment": segment,
                    "start": window_item["start_date"], "end": window_item["end_date"],
                    "rows": int(window_item["days"]),
                    "ann_return": ann_return, "ann_vol": ann_vol,
                    "sharpe_repo": sharpe, "max_dd": max_dd,
                    "feature_window": window, "trigger": trigger if enabled else "OFF",
                    "recovery": recovery if enabled else "OFF", "enabled": enabled,
                    "holding_days": int(part.holding.ne("cash").sum()),
                    "holding_day_ratio": float(part.holding.ne("cash").mean()),
                    "sum_daily_cost_rates": float(part.total_cost.sum()),
                    "final_nav": float(window_item["final_nav"]),
                })
                wide[f"ann_return_{segment}"] = ann_return
                wide[f"max_dd_{segment}"] = max_dd
                wide[f"ann_vol_{segment}"] = ann_vol
                wide[f"sharpe_repo_{segment}"] = sharpe
            wide_rows.append(wide)
            print(candidate, flush=True)
    finally:
        for key, value in original.items():
            setattr(v, key, value)
    freeze_check(l4)
    pd.DataFrame(summary_rows).to_csv(RUN / "scan_summary.csv", index=False, encoding="utf-8-sig")
    pd.DataFrame(wide_rows).to_csv(RUN / "window_metrics.csv", index=False, encoding="utf-8-sig")
    meta.update({
        "scan_type": "frozen_old_input_overheat_parameter_scan",
        "parameter_group": "feature_window_trigger_recovery",
        "baseline": {"candidate": "formal_baseline", "feature_window": 10, "trigger": .26, "recovery": .20},
        "candidate_grid": [
            {"candidate": x[0], "feature_window": x[1],
             "trigger": x[2] if x[4] else None,
             "recovery": x[3] if x[4] else None,
             "enabled": x[4]} for x in candidates
        ],
        "data_snapshot": l4["files"],
        "source_hashes": l4["source_sha256"],
        "cost_model": {
            "mode": "formal_old_paper_model",
            "entry_one_side": float(v.v2_0.base_mod.freq_mod.cost_mod.ENTRY_COST),
            "exit_one_side": float(v.v2_0.base_mod.freq_mod.cost_mod.EXIT_COST),
            "rebalance": "formal turnover map",
            "hedge_ratio": float(v.EXECUTION_HEDGE_RATIO),
            "financing_enabled": bool(v.FINANCING_ENABLED),
            "target_vol_enabled": bool(v.TARGET_VOL_ENABLED),
            "cash_yield_enabled": bool(v.CASH_DAY_YIELD_ENABLED),
        },
        "call_chain": [
            "v2_0.embedded_context._load_embedded_base_context",
            "v2_3._close_df_from_base", "v2_3.build_v2_3_common_index",
            "v2_3.build_spread_log_wls_gross", "v2_3.apply_overheat_defense",
            "v2_3.summarize_required_windows",
        ],
        "parity_check": {
            "formal_baseline_official_3988_rows": True,
            "numeric_tolerance": 1e-12,
            "numeric_columns": parity_cols_numeric,
            "state_columns": parity_cols_state,
            "overheat_off_pure_cost_parity": True,
        },
        "daily_files": daily_map,
        "output_hashes": {
            name: sha256(RUN / name) for name in ("scan_summary.csv", "window_metrics.csv")
        },
        "formal_usable": False,
        "formal_blockers": [
            "L1/L2 PIT membership and rights lineage unresolved",
            "L5 actual financing and executable futures hedge unresolved",
            "local/remote delivery authority mismatch",
        ],
    })
    meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2, allow_nan=False), encoding="utf-8")
    with (RUN / "command_log.txt").open("a", encoding="utf-8") as log:
        log.write("\npython -X utf8 research_reports/20261003_l6_diagnostic/executor/20261003_l6_frozen_old_inputs/run_scan.py\n")
    print(json.dumps({"candidates": len(candidates), "rows_each": len(index), "last_date": str(index[-1].date())}))


if __name__ == "__main__":
    main()
