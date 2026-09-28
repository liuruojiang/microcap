# /// script
# requires-python = ">=3.11"
# dependencies = []
# ///
"""Run real state-only local delivery routes after close, never pretend this is a 14:30 run."""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time
from datetime import datetime
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / "research_reports/20260904_index_preflight_failover"
sys.path.insert(0, str(ROOT))
from scripts import top100_delivery as delivery


def main():
    now = datetime.now(ZoneInfo("Asia/Shanghai"))
    assert (now.hour, now.minute) >= (15, 30), "After-close validation only"
    target = delivery.independent_target(ROOT)
    baseline = delivery.input_hashes(ROOT)
    nav_paths = [ROOT / "outputs" / f"microcap_top100_mom16_biweekly_live_v2_{v}_nav.csv" for v in ("0", "3", "5")]
    hashes = {str(p): hashlib.sha256(p.read_bytes()).hexdigest() for p in nav_paths}
    report = {"scope": "actual after-close CLI checks, not a scheduled 14:30 intraday publication",
              "started_at": now.isoformat(), "expected_date": target, "hard_timeout_seconds_each": 60, "steps": []}
    env = os.environ.copy()
    env["TOP100_REALTIME_REQUIRE_STATE"] = "1"
    commands = [
        ("sync", ["scripts/top100_cloud_delivery.py", "sync", "--expected-date", target]),
        ("preflight", ["scripts/realtime_state_bundle.py", "preflight", "--max-anchor-age-days", "5", "--expected-date", target]),
        ("validate", ["scripts/realtime_state_bundle.py", "validate", "--max-anchor-age-days", "5"]),
        ("check", ["scripts/top100_delivery.py", "check"]),
    ] + [(f"v2.{v}_realtime", [f"microcap_top100_mom16_biweekly_live_v2_{v}.py", "实时信号"]) for v in ("0", "3", "5")]
    passed = True
    for name, argv in commands:
        started = time.monotonic()
        try:
            run = subprocess.run([sys.executable, "-X", "utf8", *argv], cwd=ROOT, env=env,
                                 capture_output=True, text=True, encoding="utf-8", timeout=60)
            output = run.stdout + run.stderr
            if name.endswith("_realtime"):
                ok = run.returncode != 0 and (
                    ("realtime_signal_blocked" in output and "status: BLOCKED" in output
                     and "latest_anchor_trade_date is not the previous completed trading day" in output)
                    or "RuntimeError: latest_anchor_trade_date does not equal the official previous completed trading day before snapshot:" in output)
            else:
                ok = run.returncode == 0 and json.loads(run.stdout)["ok"] is True
            step = dict(name=name, command=argv, elapsed_seconds=round(time.monotonic()-started, 3),
                        exit_code=run.returncode, expected_result_verified=ok)
        except subprocess.TimeoutExpired as exc:
            output = f"Hard timeout: {exc}"
            step = dict(name=name, elapsed_seconds=round(time.monotonic()-started, 3), timeout=True, expected_result_verified=False)
            ok = False
        (OUT / f"local_route_{name}.txt").write_text(output, encoding="utf-8")
        report["steps"].append(step)
        print(json.dumps(step, ensure_ascii=False), flush=True)
        if not ok:
            passed = False
            break
    report["base_inputs_unchanged"] = delivery.input_hashes(ROOT) == baseline
    report["all_nav_bytes_unchanged"] = all(hashlib.sha256(p.read_bytes()).hexdigest() == hashes[str(p)] for p in nav_paths)
    report["ok"] = passed and report["base_inputs_unchanged"] and report["all_nav_bytes_unchanged"]
    report["finished_at"] = datetime.now(ZoneInfo("Asia/Shanghai")).isoformat()
    (OUT / "local_routes_acceptance.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    raise SystemExit(0 if report["ok"] else 1)


if __name__ == "__main__":
    main()
