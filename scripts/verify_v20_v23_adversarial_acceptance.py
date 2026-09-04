# /// script
# requires-python = ">=3.11"
# dependencies = ["numpy", "pandas", "requests", "urllib3", "akshare", "matplotlib", "openpyxl"]
# ///
"""Read back refreshed formal artifacts and prove bug fixes preserve daily trading PnL."""
import argparse
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import microcap_top100_mom16_biweekly_live_v2_0 as v20
import microcap_top100_mom16_biweekly_live_v2_3 as v23
from scripts import top100_delivery as delivery


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--backup", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    target = delivery.independent_target(ROOT)
    result = delivery.inspect_outputs(ROOT, target)
    assert result["ok"], result["errors"]
    result["strategy_sha"] = delivery.verify_release(ROOT)
    manifest = json.loads((ROOT / delivery.MANIFEST).read_text(encoding="utf-8"))
    assert manifest["status"] == "complete" and manifest["release_sha"] == result["strategy_sha"]
    assert manifest["expected_date"] == target
    assert manifest["inputs"] == delivery.input_hashes(ROOT)
    result["daily_parity"] = {}
    result["windows"] = {}
    result["annual_drawdown_corrections"] = {}
    for suffix in ("0", "3", "5"):
        name = f"outputs/microcap_top100_mom16_biweekly_live_v2_{suffix}_nav.csv"
        current = pd.read_csv(ROOT / name, index_col="date", parse_dates=True)
        previous = pd.read_csv(args.backup / name, index_col="date", parse_dates=True)
        assert current.index.equals(previous.index)
        assert str(current.index[-1].date()) == target
        deltas = {}
        for field in ("return_net", "nav_net", "total_cost", "current_execution_scale", "next_session_actionable_scale"):
            np.testing.assert_allclose(current[field], previous[field], atol=1e-12, rtol=0, err_msg=f"v2.{suffix}/{field}")
            deltas[field] = float((current[field] - previous[field]).abs().max())
        for field in ("holding", "next_holding"):
            assert current[field].equals(previous[field]), f"v2.{suffix}/{field}"
        result["daily_parity"][f"v2.{suffix}"] = dict(rows=len(current), max_absolute_deltas=deltas, state_equal=True)
        if suffix == "5":
            continue
        module = v20 if suffix == "0" else v23
        result["windows"][f"v2.{suffix}"] = module.summarize_required_windows(current.return_net)
        yearly_path = ROOT / f"outputs/microcap_top100_mom16_biweekly_live_v2_{suffix}_performance_yearly.csv"
        yearly = pd.read_csv(yearly_path).set_index("year")
        changes = {}
        for year, returns in current.return_net.groupby(current.index.year):
            nav = (1 + returns).cumprod()
            corrected = float((nav / nav.cummax().clip(lower=1) - 1).min() * 100)
            original = float((nav / nav.cummax() - 1).min() * 100)
            np.testing.assert_allclose(yearly.loc[year, "max_drawdown_pct"], corrected, atol=1e-10, rtol=0)
            if abs(original - corrected) > 1e-10:
                changes[str(year)] = dict(previous_pct=original, corrected_pct=corrected)
        result["annual_drawdown_corrections"][f"v2.{suffix}"] = changes
    result["scope"] = "local formal artifact acceptance; cloud delivery verified separately"
    result["backup"] = str(args.backup.resolve())
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(result, ensure_ascii=False, indent=2, allow_nan=False), encoding="utf-8")
    print(json.dumps(result, ensure_ascii=False, allow_nan=False))


if __name__ == "__main__":
    main()
