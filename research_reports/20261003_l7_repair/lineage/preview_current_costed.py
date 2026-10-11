"""Read-only frozen-input replay of the three formal costed NAV calculation chains."""
from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))

import microcap_top100_mom16_biweekly_live_v2_0 as v20
import microcap_top100_mom16_biweekly_live_v2_3 as v23
import microcap_top100_mom16_biweekly_live_v2_5 as v25


OUTPUT = ROOT / "outputs"


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def compare(label: str, candidate: pd.DataFrame, path: Path,
            audit_columns: list[str], allowed_tail_rows: int,
            column_allowed_tail_rows: dict[str, int] | None = None) -> dict:
    saved = pd.read_csv(path, encoding="utf-8-sig", low_memory=False)
    saved["date"] = pd.to_datetime(saved["date"])
    saved = saved.set_index("date").sort_index()
    candidate = candidate.sort_index()
    columns = ["holding", "next_holding", "return_net", "nav_net", "total_cost"]
    result = {"version": label, "candidate_rows": len(candidate), "saved_rows": len(saved),
              "candidate_latest": str(candidate.index[-1].date()),
              "saved_latest": str(saved.index[-1].date()), "first_diff": None, "max_numeric_abs_diff": {}}
    v20.embedded_context.base_mod.assert_no_historical_rewrite(
        previous=saved, candidate=candidate,
        key_columns=audit_columns, allowed_tail_rows=allowed_tail_rows,
        column_allowed_tail_rows=column_allowed_tail_rows,
        label=f"{label} read-only frozen-input preview", audit_path=None)
    result["official_rewrite_audit_function"] = "clean"
    for col in columns:
        a = candidate[col].reindex(saved.index)
        b = saved[col]
        if col in {"holding", "next_holding"}:
            different = a.astype(str).ne(b.astype(str))
        else:
            a = pd.to_numeric(a, errors="coerce")
            b = pd.to_numeric(b, errors="coerce")
            delta = (a-b).abs()
            result["max_numeric_abs_diff"][col] = float(delta.max())
            different = delta.gt(1e-12) | a.isna().ne(b.isna())
        if different.any():
            day = different[different].index[0]
            value = {"date": str(day.date()), "column": col,
                     "candidate": str(a.loc[day]), "saved": str(b.loc[day])}
            if result["first_diff"] is None or day < pd.Timestamp(result["first_diff"]["date"]):
                result["first_diff"] = value
    return result


def main() -> None:
    paths = {
        "0": v20.COSTED_NAV_CSV,
        "3": v23.COSTED_NAV_CSV,
        "5": v25.COSTED_NAV_CSV,
    }
    before = {key: digest(path) for key, path in paths.items()}
    base_paths = v20._resolve_base_paths().output_paths
    panel_path = base_paths["panel_shadow"]
    index_path = v20._build_base_args().index_csv
    close_df = v20.base_mod.load_close_df(panel_path, index_path, max_date=pd.Timestamp("2026-09-30"))
    gross = v20.base_mod.run_signal(close_df).sort_index()
    turnover = pd.read_csv(base_paths["proxy_turnover"])
    turnover["rebalance_date"] = pd.to_datetime(turnover["rebalance_date"])
    v20_close = gross[["microcap_close", "hedge_close"]].rename(columns={"microcap_close":"microcap", "hedge_close":"hedge"})
    out20 = v20.overlay_mod.apply_v2_0_execution(
        v20.embedded_context.base_mod.apply_momentum_gap_exit_buffer(
            v20.embedded_context.base_mod.run_signal(v20_close).sort_index(),
            v20.overlay_mod.V2_0_MOMENTUM_GAP_EXIT_BUFFER), turnover)
    close23 = v23._close_df_from_base(gross)
    out23 = v23.build_v2_3_result(close23, turnover, v23.build_v2_3_common_index(close23, out20.index))
    close25 = v25._close_df_from_base(gross)
    out25 = v25.build_v2_5_result(close25, turnover, v25.build_v2_5_common_index(close25, out20.index))
    results = [
        compare("v2.0", out20, paths["0"], v20.V2_0_REWRITE_AUDIT_KEY_COLUMNS,
                v20._v2_0_rewrite_allowed_tail_rows(),
                v20.overlay_mod.V2_0_REWRITE_AUDIT_ALLOWED_TAIL_ROWS_BY_COLUMN),
        compare("v2.3", out23, paths["3"], v23.V2_3_REWRITE_AUDIT_KEY_COLUMNS,
                v23._v2_3_rewrite_allowed_tail_rows(),
                v23.V2_3_REWRITE_AUDIT_ALLOWED_TAIL_ROWS_BY_COLUMN),
        compare("v2.5", out25, paths["5"], v25.V2_5_REWRITE_AUDIT_KEY_COLUMNS,
                v25._v2_5_rewrite_allowed_tail_rows(),
                v25.V2_5_REWRITE_AUDIT_ALLOWED_TAIL_ROWS_BY_COLUMN),
    ]
    after = {key: digest(path) for key, path in paths.items()}
    report = {"read_only_costed_hashes_unchanged": before == after,
              "costed_sha256": before, "results": results}
    destination = Path(__file__).with_name("PREVIEW.json")
    destination.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
