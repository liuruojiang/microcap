"""Read-only diagnostic recompute; this does not generate or promote official outputs."""

from __future__ import annotations

import hashlib
import importlib.util
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

import microcap_top100_mom16_biweekly_live_v2_0 as v0
import microcap_top100_mom16_biweekly_live_v2_3 as v23
import microcap_top100_mom16_biweekly_live_v2_5 as v25


REPORT_DIR = Path(__file__).resolve().parent
BACKUP = ROOT / ".codex_backups" / "20261007_143135"
TARGET = pd.Timestamp("2026-09-30")


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def backup_module(version: str):
    source = BACKUP / f"microcap_top100_mom16_biweekly_live_v2_{version}.py"
    spec = importlib.util.spec_from_file_location(f"audit_pre_edit_v2_{version}", source)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def independent_wls(nav: pd.Series, module) -> pd.DataFrame:
    lookback = module.LOOKBACK
    x = np.arange(lookback, dtype=float)
    weights = 0.5 ** ((lookback - 1 - x) / module.HALFLIFE)
    design = np.column_stack([np.ones(lookback), x])
    weighted_design = design * np.sqrt(weights[:, None])
    y = np.log(nav.to_numpy(dtype=float))
    rows = []
    for end in range(lookback - 1, len(y)):
        part = y[end - lookback + 1 : end + 1]
        coefficients = np.linalg.lstsq(weighted_design, part * np.sqrt(weights), rcond=None)[0]
        total = np.sum(weights * (part - np.average(part, weights=weights)) ** 2)
        residual = np.sum(weights * (part - design @ coefficients) ** 2)
        r2 = 0.0 if total == 0.0 else float(np.clip(1.0 - residual / total, 0.0, 1.0))
        rows.append([coefficients[1] * module.TRADING_DAYS, r2])
    return pd.DataFrame(rows, index=nav.index[lookback - 1 :], columns=["annualized_log_wls_score", "log_wls_r2"])


def read_stream(path: Path) -> pd.DataFrame:
    return pd.read_csv(path, encoding="utf-8-sig", parse_dates=["date"]).set_index("date")


def compare(left: pd.DataFrame, right: pd.DataFrame, fields: list[str]) -> dict:
    assert left.index.equals(right.index), "daily date index differs"
    result = {}
    for field in fields:
        if pd.api.types.is_numeric_dtype(left[field]) and pd.api.types.is_numeric_dtype(right[field]):
            delta = (left[field] - right[field]).abs()
            result[field] = {"max_abs_error": float(delta.max()), "outside_1e12": int(delta.gt(1e-12).sum())}
        else:
            result[field] = {"different_rows": int(left[field].ne(right[field]).sum())}
    return result


def main() -> None:
    resolved = v0._resolve_base_paths()
    source_paths = {
        "panel": resolved.output_paths["panel_shadow"],
        "proxy_index": resolved.index_csv,
        "turnover": resolved.output_paths["proxy_turnover"],
        "base_costed_nav": resolved.costed_nav_csv,
        "v20_nav": v0.overlay_mod.NAV_CSV,
        "v23_costed_nav": v23.COSTED_NAV_CSV,
        "v25_costed_nav": v25.COSTED_NAV_CSV,
    }
    hashes_before = {label: sha256(path) for label, path in source_paths.items()}
    freshness = {}
    for label, path in source_paths.items():
        frame = pd.read_csv(path, encoding="utf-8-sig")
        date_column = "rebalance_date" if label == "turnover" else "date"
        dates = pd.to_datetime(frame[date_column])
        freshness[label] = {"path": str(path), "rows": len(frame), "start": str(dates.min().date()), "end": str(dates.max().date()), "sha256": hashes_before[label]}
        if label != "turnover":
            assert dates.max().normalize() == TARGET, f"{label} stale"
    # Mirror the actual context's read/signal call chain without refresh or writes.
    close_all = v0.base_mod.load_close_df(source_paths["panel"], source_paths["proxy_index"], max_date=TARGET)
    base_gross = v0.base_mod.run_signal(close_all).sort_index()
    close_df = v25._close_df_from_base(base_gross)
    turnover = pd.read_csv(source_paths["turnover"], encoding="utf-8-sig")
    for column in ["rebalance_date", "execution_date", "effective_date", "return_start_date"]:
        turnover[column] = pd.to_datetime(turnover[column], errors="coerce")
    official_index = read_stream(source_paths["v20_nav"]).index
    result = {
        "caliber": "read_only_diagnostic_recompute_with_official_baseline_parity",
        "target_latest_completed_session": str(TARGET.date()),
        "freshness": freshness,
        "close_input_rows": len(close_df),
        "close_input_start": str(close_df.index.min().date()),
        "close_input_end": str(close_df.index.max().date()),
        "versions": {},
    }
    fields = ["holding", "next_holding", "return_net", "nav_net", "current_execution_scale", "total_cost", "overlay_pre_cost_return"]
    for version, module in [("3", v23), ("5", v25)]:
        old_module = backup_module(version)
        common_index = getattr(module, f"build_v2_{version}_common_index")(close_df, official_index)
        new_out = getattr(module, f"build_v2_{version}_result")(close_df, turnover, common_index)
        old_out = getattr(old_module, f"build_v2_{version}_result")(close_df, turnover, common_index)
        official = read_stream(source_paths[f"v2{version}_costed_nav"])
        assert new_out.index.max() == TARGET
        old_new = compare(new_out, old_out, fields)
        official_parity = compare(new_out, official, fields)
        new_score = new_out.annualized_log_wls_score
        old_score = old_out.annualized_log_wls_score
        score_diff = (new_score - old_score).abs()
        zero_sign_changes = np.sign(new_score).ne(np.sign(old_score))
        nav = module.always_on_spread_nav(close_df)[0] if version == "3" else module.microcap_nav(close_df)[0]
        independent = independent_wls(nav, module).loc[new_out.index]
        independent_error = np.abs(new_out[["annualized_log_wls_score", "log_wls_r2"]].to_numpy() - independent.to_numpy())
        near_zero = pd.DataFrame({"old_score": old_score, "new_score": new_score, "score_delta": new_score-old_score, "holding": new_out.holding, "return_net": new_out.return_net})
        near_zero = near_zero.loc[new_score.abs().sort_values().index[:20]]
        near_zero.to_csv(REPORT_DIR / f"v2{version}_near_zero_scores.csv", index_label="date", encoding="utf-8-sig")
        result["versions"][f"v2.{version}"] = {
            "source_before_sha256": sha256(BACKUP / f"microcap_top100_mom16_biweekly_live_v2_{version}.py"),
            "source_after_sha256": sha256(ROOT / f"microcap_top100_mom16_biweekly_live_v2_{version}.py"),
            "rows": len(new_out), "start": str(new_out.index.min().date()), "end": str(new_out.index.max().date()),
            "old_vs_new": old_new, "official_vs_new": official_parity,
            "wls_max_abs_score_change": float(score_diff.max()),
            "zero_threshold_sign_changes": int(zero_sign_changes.sum()),
            "scores_within_1e10_of_zero": int(new_score.abs().lt(1e-10).sum()),
            "minimum_abs_score": float(new_score.abs().min()),
            "independent_weighted_lstsq_max_score_error": float(independent_error[:,0].max()),
            "independent_weighted_lstsq_max_r2_error": float(independent_error[:,1].max()),
        }
        for comparison in [old_new, official_parity]:
            for row in comparison.values():
                assert row.get("different_rows", 0) == 0
                assert row.get("outside_1e12", 0) == 0
        assert not zero_sign_changes.any(), "real-data zero-threshold decision changed"
        assert float(independent_error.max()) < 1e-10
    # Normal float dtype is exact; the textual representation also reaches the
    # same executed path and costs after normalization.
    numeric = v25.build_v2_5_result(close_df, turnover)
    textual = v25.build_v2_5_result(close_df.astype(str), turnover)
    result["v25_real_numeric_text_parity"] = compare(numeric, textual, fields)
    for row in result["v25_real_numeric_text_parity"].values():
        assert row.get("different_rows", 0) == 0
        assert row.get("outside_1e12", 0) == 0
    hashes_after = {label: sha256(path) for label, path in source_paths.items()}
    assert hashes_before == hashes_after, "official input/output artifact was modified"
    result["official_artifacts_unchanged"] = True
    result["result"] = "PASS"
    (REPORT_DIR / "math_real_parity.json").write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"result": result["result"], "official_artifacts_unchanged": True, "versions": result["versions"]}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
