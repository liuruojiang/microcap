"""Offline fault-injection contracts, NOT backtests or live-signal claims.

Cross-repository digest checks use MICROCAP_DIGEST_SOURCE when provided; they
skip on machines without that separately owned source tree.
"""
import csv
import importlib.util
import json
import os
import sys
from pathlib import Path

import pytest

from scripts import top100_delivery as delivery
from test_top100_delivery import workspace  # noqa: F401


def rewrite_csv(path, transform):
    with path.open(encoding="utf-8-sig", newline="") as stream:
        rows = list(csv.DictReader(stream))
    rows = transform(rows)
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


@pytest.mark.parametrize("version", ["0", "3"])
@pytest.mark.parametrize("value", ["nan", "inf", "-inf", ""])
def test_final_nav_nonfinite_return_cannot_be_certified(workspace, version, value):
    """An upstream numerical failure must not become a new complete manifest."""
    for name in (delivery.COSTED[version],
                 f"microcap_top100_mom16_biweekly_live_v2_{version}_nav.csv"):
        def mutate(rows):
            rows[-1]["return_net"] = value
            return rows
        rewrite_csv(workspace / "outputs" / name, mutate)
    report = delivery.inspect_outputs(workspace, "2026-09-03")
    assert not report["ok"], "nonfinite final return was accepted for certification"


@pytest.mark.parametrize("version", ["0", "3"])
def test_latest_signal_holding_must_match_final_nav(workspace, version):
    """Same-date files can come from different generation branches."""
    prefix = f"microcap_top100_mom16_biweekly_live_v2_{version}"
    for name in (delivery.COSTED[version], f"{prefix}_nav.csv"):
        rewrite_csv(workspace / "outputs" / name,
                    lambda rows: [{**row, "holding": "cash", "next_holding": "cash"} for row in rows])
    rewrite_csv(workspace / "outputs" / f"{prefix}_latest_signal.csv",
                lambda rows: [{**row, "current_holding": "microcap_hedged", "next_holding": "cash"} for row in rows])
    report = delivery.inspect_outputs(workspace, "2026-09-03")
    assert not report["ok"], "conflicting NAV/signal holdings passed the publication gate"


@pytest.mark.parametrize("value", ["nan", "inf", "-1", "999"])
def test_final_nav_must_match_cumulative_returns(workspace, value):
    for name in (delivery.COSTED["3"], "microcap_top100_mom16_biweekly_live_v2_3_nav.csv"):
        def mutate(rows):
            rows[-1]["nav_net"] = value
            return rows
        rewrite_csv(workspace / "outputs" / name, mutate)
    assert not delivery.inspect_outputs(workspace, "2026-09-03")["ok"]


def test_performance_cannot_certify_different_self_consistent_curve(workspace):
    def mutate(rows):
        rows[-1].update(return_net="0.03", nav_net="1.0403")
        return rows
    rewrite_csv(workspace / "outputs/microcap_top100_mom16_biweekly_live_v2_3_performance_nav.csv", mutate)
    assert not delivery.inspect_outputs(workspace, "2026-09-03")["ok"]


def test_warmup_diagnostic_nan_is_not_a_realized_return_failure(workspace):
    for version in ("0", "3"):
        for name in (delivery.COSTED[version], f"microcap_top100_mom16_biweekly_live_v2_{version}_nav.csv"):
            rewrite_csv(workspace / "outputs" / name,
                        lambda rows: [{**row, "log_wls_r2": "nan"} for row in rows])
    report = delivery.inspect_outputs(workspace, "2026-09-03")
    assert report["ok"], report["errors"]


@pytest.mark.parametrize("value", ["0", "inf", "nan", "-1", ""])
def test_signal_scale_must_match_nav(workspace, value):
    rewrite_csv(workspace / "outputs/microcap_top100_mom16_biweekly_live_v2_3_latest_signal.csv",
                lambda rows: [{**row, "current_execution_scale": value} for row in rows])
    assert not delivery.inspect_outputs(workspace, "2026-09-03")["ok"]


@pytest.mark.parametrize("key", ["member_rebalance_required", "member_rebalance_official"])
def test_inactive_member_flags_still_require_explicit_bool(workspace, key):
    rewrite_csv(workspace / "outputs/microcap_top100_mom16_biweekly_live_v2_3_latest_signal.csv",
                lambda rows: [{**row, key: "nan"} for row in rows])
    assert not delivery.inspect_outputs(workspace, "2026-09-03")["ok"]


@pytest.fixture
def digest_module():
    path = Path(os.environ.get("MICROCAP_DIGEST_SOURCE",
        "D:/Codex/worktrees/v23-plain-digest-20260904/scripts/build_microcap_realtime_digest.py"))
    if not path.is_file():
        pytest.skip("Separate digest source unavailable; set MICROCAP_DIGEST_SOURCE")
    spec = importlib.util.spec_from_file_location("adversarial_digest", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def run_digest(module, tmp_path, monkeypatch, version, mutation=None, stdout_extra=""):
    identity = module.STRATEGY_IDENTITIES[version]
    row = {key: str(value) for group in identity.values() for key, value in group.items()}
    row.update(date="2026-09-03", signal_timing="close_confirmed", official_close_confirmed_signal="True",
               current_holding="cash", next_holding="cash", current_execution_scale="0",
               next_session_actionable_scale="0", trade_state="hold", holding_trade_state="hold",
               member_rebalance_required="False", member_rebalance_actionable="False",
               member_rebalance_official="False")
    if mutation:
        mutation(row)
    csv_path = tmp_path / "signal.csv"
    with csv_path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(row))
        writer.writeheader()
        writer.writerow(row)
    stdout = tmp_path / "stdout.txt"
    stdout.write_text("signal\n" + stdout_extra, encoding="utf-8")
    out = tmp_path / "digest"
    monkeypatch.setattr(sys, "argv", ["digest", "--result", f"{version}={stdout}", "--signal-csv",
        f"{version}={csv_path}", "--publication-mode", "close_confirmed", "--expected-signal-date",
        "2026-09-03", "--exit-code", "0", "--out-dir", str(out)])
    assert module.main() == 0
    return json.loads((out / "metadata.json").read_text(encoding="utf-8"))


@pytest.mark.parametrize("version", ["v2.0", "v2.3"])
def test_digest_control_valid_final_signal_passes(digest_module, tmp_path, monkeypatch, version):
    assert run_digest(digest_module, tmp_path, monkeypatch, version)["status"] == "OK"


@pytest.mark.parametrize("version", ["v2.0", "v2.3"])
def test_digest_cannot_backfill_final_csv_holdings_from_stdout(digest_module, tmp_path, monkeypatch, version):
    def mutation(row):
        row.pop("current_holding")
        row.pop("next_holding")
    result = run_digest(digest_module, tmp_path, monkeypatch, version, mutation,
                        "current_holding: cash\nnext_holding: microcap_hedged\n")
    assert result["status"] == "FAILED", result["body"]


@pytest.mark.parametrize("version", ["v2.0", "v2.3"])
def test_digest_rejects_nonfinite_actionable_scale(digest_module, tmp_path, monkeypatch, version):
    result = run_digest(digest_module, tmp_path, monkeypatch, version,
                        lambda row: row.update(next_session_actionable_scale="inf"))
    assert result["status"] == "FAILED", result["body"]


def test_digest_close_members_cannot_republish_historical_action(digest_module, tmp_path, monkeypatch):
    result = run_digest(digest_module, tmp_path, monkeypatch, "v2.3", lambda row: row.update(
        member_rebalance_required="True", member_rebalance_actionable="True", member_rebalance_official="True",
        member_rebalance_signal_date="2026-08-20", member_rebalance_execution_date="2026-08-21"))
    assert result["status"] == "FAILED", result["body"]
