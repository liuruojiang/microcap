import json
from pathlib import Path


ROOT = Path(__file__).resolve().parent
OUTPUT_DIR = ROOT / "outputs"
OUT_JSON = OUTPUT_DIR / "microcap_top100_nav_throttle_practical_current_candidate.json"


def main() -> None:
    payload = {
        "strategy_family": "top100_mom16_biweekly_live_practical_nav_throttle",
        "as_of_date": "2026-04-10",
        "selected_version": "1.2",
        "selected_candidate": {
            "name": "practical_nav_throttle_4_8",
            "dd_moderate": 0.04,
            "dd_severe": 0.08,
            "scale_moderate": 0.85,
            "scale_severe": 0.70,
            "recover_dd": 0.03,
            "rebal_cost_bps": 2.0,
            "timing_rule": "T close observes NAV drawdown, T+1 session applies new scale",
            "rerisk_rule": "restore directly to full once drawdown recovers inside 3%",
            "version_alias": "v1.2",
        },
        "selection_note": "Chosen as the current practical candidate for live-style follow-up tests because it is close to best recent-window efficiency while requiring materially fewer scale changes than 3/8.",
        "source_artifacts": {
            "practical_scan_summary": str(
                OUTPUT_DIR / "microcap_top100_mom16_v1_1_nav_throttle_practical_summary.json"
            ),
            "focus_windows_summary": str(
                OUTPUT_DIR / "microcap_top100_mom16_v1_1_nav_throttle_practical_focus_windows_summary.csv"
            ),
        },
    }
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(str(OUT_JSON))


if __name__ == "__main__":
    main()
