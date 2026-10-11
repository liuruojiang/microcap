"""Run four research-only L5 cases with daily swap variation in common cash."""

from __future__ import annotations

import json
import shutil
import sys
from pathlib import Path

import baostock as bs

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
L5 = ROOT / "research_reports/20261003_l5_diagnostic/executor"
SIGNAL = ROOT / "research_reports/20261003_l5_repair/signal_audit"
sys.path.insert(0, str(L5))

import replay_overheat_account as old_driver  # noqa: E402
import common_cash_replay as replay  # noqa: E402


def main() -> None:
    binding = json.loads((SIGNAL / "binding_result.json").read_text(encoding="utf-8"))
    login = bs.login()
    if login.error_code != "0":
        raise RuntimeError(f"baostock_login_failed:{login.error_code}:{login.error_msg}")
    result = {"label": "research_only_frozen_old_pool_common_cash_variation_no_credit_not_broker_executable",
              "source_L5_result_sha256": old_driver.sha(L5 / "result.json"),
              "official_binding_sha256": old_driver.sha(SIGNAL / "binding_result.json"),
              "copied_L2_account_sha256": old_driver.sha(ROOT / "research_reports/20261002_l2_full_repair/version_integration/strategy_replay.py"),
              "replay_source_sha256": old_driver.sha(HERE / "common_cash_replay.py"),
              "cases": {}}
    try:
        for branch in ("prefunded", "netted"):
            for mode in ("on", "off"):
                key = f"{branch}_{mode}"
                signal_branch = branch if mode == "on" else f"{branch}_off"
                old_signal = SIGNAL / signal_branch / f"recent_{branch}_rebuilt_signals.csv"
                expected_signal_sha = (binding["branches"][branch]["output_sha256"] if mode == "on"
                                       else binding["branches"][branch]["off_output_sha256"])
                if old_driver.sha(old_signal) != expected_signal_sha:
                    raise ValueError(f"input_signal_hash_mismatch:{key}")
                out = HERE / key
                out.mkdir(exist_ok=True)
                new_signal = out / old_signal.name
                shutil.copyfile(old_signal, new_signal)
                if old_driver.sha(new_signal) != old_driver.sha(old_signal):
                    raise AssertionError(f"copy_mismatch:{key}")
                replay.HERE = out
                src = old_driver.source()
                case = replay.run_account(branch, "v2.3", src)
                case["source_queries"] = src.queries
                queried = [row for (symbol, date), row in sorted(src.bars.items())
                           if any(item["symbol"] == symbol and item["start_date"] <= date <= item["end_date"]
                                  for item in src.queries)]
                if queried:
                    response = out / "new_queried_baostock_bars.csv"
                    replay.write_csv(response, queried)
                    case["new_query_response"] = {"path": str(response.relative_to(ROOT)),
                                                  "rows": len(queried), "sha256": old_driver.sha(response)}
                case["input_signal_sha256"] = old_driver.sha(new_signal)
                case["signal_binding"] = "frozen_official_v2_3_ON_or_base_before_overheat_OFF"
                result["cases"][key] = case
    finally:
        bs.logout()
    (HERE / "common_cash_result.json").write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({key: {field: val.get(field) for field in ("last_settled", "settled_days", "first_block")}
                      for key, val in result["cases"].items()}, ensure_ascii=False))


if __name__ == "__main__":
    main()
