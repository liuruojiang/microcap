"""Read-only real-data parity for the script audit; creates diagnostic evidence only."""
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
import microcap_top100_mom16_biweekly_live_v2_3 as new

OLD_PATH = ROOT / ".codex_backups/20261007_143135/microcap_top100_mom16_biweekly_live_v2_3.py"
spec = importlib.util.spec_from_file_location("v23_before_script_audit", OLD_PATH)
old = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = old
spec.loader.exec_module(old)

OUTPUT = ROOT / "research_reports/20261007_script_adversarial"
proof_path = OUTPUT / "pre_delivery_check.json"
proof = json.loads(proof_path.read_text(encoding="utf-8"))
assert proof["ok"] and proof["expected_date"] == "2026-09-30"

paths = {
    "base_costed": ROOT / "outputs/microcap_top100_mom16_hedge_zz1000_0p8x_biweekly_thursday_16y_v2_0_base_costed_nav.csv",
    "turnover": ROOT / "outputs/microcap_top100_mom16_biweekly_live_v2_0_base_proxy_turnover.csv",
    "v20": ROOT / "outputs/microcap_top100_mom16_biweekly_live_v2_0_nav.csv",
    "v23": ROOT / "outputs/microcap_top100_mom16_biweekly_live_v2_3_nav.csv",
}
for path in paths.values():
    # The delivery contract canonicalizes CRLF to LF before hashing text.
    assert hashlib.sha256(path.read_bytes().replace(b"\r\n", b"\n")).hexdigest() == proof["streams"][path.name]["sha256"]

base = pd.read_csv(paths["base_costed"], parse_dates=["date"]).set_index("date")
turnover = pd.read_csv(paths["turnover"], parse_dates=["rebalance_date"])
official_v20 = pd.read_csv(paths["v20"], parse_dates=["date"]).set_index("date")
official_v23 = pd.read_csv(paths["v23"], parse_dates=["date"]).set_index("date")
prices = base[["microcap_close", "hedge_close"]].rename(columns={"microcap_close": "microcap", "hedge_close": "hedge"})
old_common = old.build_v2_3_common_index(prices, official_v20.index)
new_common = new.build_v2_3_common_index(prices, official_v20.index)
pd.testing.assert_index_equal(old_common, new_common)
old_out = old.build_v2_3_result(prices, turnover, old_common)
new_out = new.build_v2_3_result(prices, turnover, new_common)
pd.testing.assert_index_equal(new_out.index, official_v23.index)

all_columns = ["holding", "next_holding", "signal_on", "overheat_risk_off", "overheat_exit_triggered",
               "overheat_reentry_triggered", "overheat_block_entry_triggered", "return_net", "nav_net", "total_cost"]
for column in all_columns:
    pd.testing.assert_series_equal(old_out[column], new_out[column], check_exact=True)
for column in ["holding", "next_holding"]:
    assert new_out[column].equals(official_v23[column])
for column in ["return_net", "nav_net", "total_cost"]:
    np.testing.assert_allclose(old_out[column], official_v23[column], atol=1e-10, rtol=1e-12)

ret = new_out["return_net"]
old_wls = old.log_wls_score_and_r2(old.always_on_spread_nav(prices)[0])
new_wls = new.log_wls_score_and_r2(new.always_on_spread_nav(prices)[0])
score_diff = np.abs(old_wls["annualized_log_wls_score"] - new_wls["annualized_log_wls_score"])
valid = np.isfinite(old_wls["annualized_log_wls_score"]) & np.isfinite(new_wls["annualized_log_wls_score"])
score_sign_changed = int((np.sign(old_wls.loc[valid, "annualized_log_wls_score"]) !=
                          np.sign(new_wls.loc[valid, "annualized_log_wls_score"])).sum())
reference = json.loads((ROOT / "outputs/microcap_top100_mom16_biweekly_live_v2_0_summary.json").read_text(encoding="utf-8"))
signal = new._build_signal_row(new_out, reference)
expected_feature = new_out.iloc[-1]["overheat_feature_value"]
assert signal.iloc[0]["overheat_metric"] == expected_feature
assert signal.iloc[0]["overheat_feature_value"] == expected_feature

bundle_hashes_before = {path: hashlib.sha256(path.read_bytes()).hexdigest() for path in paths.values()}
prefix = {}
for end in [100, 500, 1500, 3000]:
    cutoff = old_common[end - 1]
    prefix_prices = prices.loc[:cutoff]
    prefix_out = new.build_v2_3_result(prefix_prices, turnover, new_common[new_common <= cutoff])
    for column in all_columns:
        pd.testing.assert_series_equal(prefix_out[column], new_out.loc[prefix_out.index, column], check_exact=True)
    prefix[str(cutoff.date())] = {"rows": len(prefix_out), "causal_parity": True}
assert bundle_hashes_before == {path: hashlib.sha256(path.read_bytes()).hexdigest() for path in paths.values()}

result = {
    "status": "PASS",
    "caliber": "diagnostic_read_only_actual_code_and_existing_costed_proxy_data",
    "freshness_proof": str(proof_path.relative_to(ROOT)),
    "latest_completed_date": proof["expected_date"],
    "base_price_rows": len(prices),
    "v20_rows": len(official_v20),
    "v23_rows": len(new_out),
    "old_new_executed_holdings_costs_returns_nav_exact": True,
    "old_recompute_matches_formal_file": True,
    "score_max_absolute_change": float(score_diff.max()),
    "real_score_sign_changes": score_sign_changed,
    "prefix_tests": prefix,
    "official_file_writes": False,
    "inputs": {str(path.relative_to(ROOT)): hashlib.sha256(path.read_bytes()).hexdigest() for path in paths.values()},
    "sources": {str(path.relative_to(ROOT)): hashlib.sha256(path.read_bytes()).hexdigest()
                for path in [OLD_PATH, Path(new.__file__)]},
    "signal": {"overheat_metric_matches_native_feature": True,
               "latest_native_overheat_feature": float(expected_feature)},
    "limitations": ["PIT universe and corporate-action history were not recertified by this script audit",
                    "No live quotes, production deployment, actual fills or SMTP delivery were tested"],
}
(OUTPUT / "v23_signal_real_parity.json").write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
print(json.dumps({key: result[key] for key in ["status", "v23_rows", "old_new_executed_holdings_costs_returns_nav_exact",
                                           "old_recompute_matches_formal_file", "score_max_absolute_change",
                                           "real_score_sign_changes", "official_file_writes"]}, indent=2))
