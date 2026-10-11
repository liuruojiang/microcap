"""Independent real-artifact delivery content fault injection. Never sends mail."""
from __future__ import annotations

import csv
import hashlib
import importlib.util
import json
import os
import sys
from datetime import datetime
from email.message import EmailMessage
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[3]
HERE = Path(__file__).resolve().parent
AUTO = Path(r"D:\动量策略\codex-daily-automation-probe-l7-sync")
BUILDER = AUTO / "scripts/build_microcap_realtime_digest.py"
sys.path.insert(0, str(ROOT))
from scripts.exchange_calendar import BJ, is_trading_day, latest_completed_session

spec = importlib.util.spec_from_file_location("content_probe_digest", BUILDER)
digest = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(digest)
VERSIONS = ("v2.0", "v2.3", "v2.5")
VERSION_FOLDERS = {"v2.0": "microcap", "v2.3": "microcap-v23", "v2.5": "microcap-v25"}
CLOUD = ROOT / "research_reports/20261007_remote_daily_sync/cloud_artifacts_readback/microcap-realtime-digest"
SHA = "501b269b9f51a83f05f3b71488143b4da285cd0b"


def read_csv(path: Path):
    with path.open(encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def hash_file(path: Path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def load_row(version: str, source: str = "local"):
    filename = f"microcap_top100_mom16_biweekly_live_{version.replace('.', '_')}_latest_signal.csv"
    path = ROOT / "outputs" / filename if source == "local" else CLOUD / VERSION_FOLDERS[version] / "outputs" / filename
    rows = read_csv(path)
    assert len(rows) == 1, (path, len(rows))
    assert rows[-1]["date"] == "2026-09-30"
    return dict(rows[-1])


def build(case: str, rows: dict, mode="close_confirmed", stdout=None, expected="2026-09-30", now=None, prefix="验收不发送", test_id=""):
    case_dir = HERE / "cases" / case
    case_dir.mkdir(parents=True, exist_ok=True)
    argv = [str(BUILDER), "--out-dir", str(case_dir / "artifacts"), "--publication-mode", mode,
            "--strategy-sha", SHA, "--subject-prefix", prefix]
    if expected:
        argv += ["--expected-signal-date", expected]
    if test_id:
        argv += ["--delivery-test-id", test_id]
    for version, row in rows.items():
        path = case_dir / f"{version}.csv"
        with path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(row))
            writer.writeheader()
            writer.writerow(row)
        output = "signal\n" if mode == "close_confirmed" else "realtime_signal\n"
        if mode == "realtime":
            output += "quote_trade_date: 2026-10-08\nlatest_anchor_trade_date: 2026-09-30\n"
        output += (stdout or {}).get(version, "")
        log = case_dir / f"{version}.txt"
        log.write_text(output, encoding="utf-8")
        argv += ["--result", f"{version}={log}", "--signal-csv", f"{version}={path}", "--exit-code", f"{version}=0"]
    with patch.object(sys, "argv", argv), patch.object(digest, "now_bj", return_value=now or datetime(2026,10,7,22,30,tzinfo=BJ)), patch.dict(os.environ, {"GITHUB_RUN_URL": ""}):
        assert digest.main() == 0
    return json.loads((case_dir / "artifacts/metadata.json").read_text(encoding="utf-8"))


def realtime(row):
    return {**row, "date": "2026-10-08", "quote_trade_date": "2026-10-08",
            "latest_anchor_trade_date": "2026-09-30", "expected_latest_completed_trade_date": "2026-09-30",
            "expected_latest_completed_trade_date_source": "independent_close_history_refresh",
            "snapshot_time": "2026-10-08T14:15:00+08:00", "signal_timing": "intraday_hypothetical_if_now_close",
            "official_close_confirmed_signal": "False", "quote_coverage": "100/100"}


def main():
    current = datetime.now(BJ)
    next_open = datetime(2026, 10, 8, 14, 15, tzinfo=BJ)
    calendar = {"verified_at": current.isoformat(), "trading_day": is_trading_day(current.date()),
                "latest_completed_session": latest_completed_session(current).isoformat(),
                "next_open_probe": next_open.isoformat(), "next_open_is_trading_day": is_trading_day(next_open.date()),
                "next_open_completed_session": latest_completed_session(next_open).isoformat()}
    assert calendar["latest_completed_session"] == "2026-09-30"
    assert calendar["next_open_completed_session"] == "2026-09-30"
    streams = {
        "panel": "microcap_top100_mom16_biweekly_live_v2_0_base_panel_refreshed.csv",
        "proxy": "wind_microcap_top_100_biweekly_thursday_16y_cached.csv",
        "turnover_events": "microcap_top100_mom16_biweekly_live_v2_0_base_proxy_turnover.csv",
        "base_costed": "microcap_top100_mom16_hedge_zz1000_0p8x_biweekly_thursday_16y_v2_0_base_costed_nav.csv",
        "v2.0_costed": "microcap_top100_mom16_plain_fixed1_v2_0_costed_nav.csv",
        "v2.3_costed": "microcap_top100_mom16_lb25_hl2p5_r2off_eb0p08_vol10_oh26_recovery20_exec0p8_v2_3_costed_nav.csv",
        "v2.5_costed": "microcap_top100_mom16_lb20_hl3_entry0_exit0_no_targetvol_v2_5_costed_nav.csv",
    }
    freshness = {}
    for name, filename in streams.items():
        path = ROOT / "outputs" / filename
        rows = read_csv(path)
        date_key = next(key for key in rows[0] if key in {"date", "trade_date", "rebalance_date", "Date", "", "Unnamed: 0"})
        dates = [row[date_key][:10] for row in rows]
        freshness[name] = {"path": str(path), "rows": len(rows), "first_date": min(dates),
                           "latest_date": max(dates), "sha256": hash_file(path), "date_column": date_key}
        if name != "turnover_events":
            assert max(dates) == calendar["latest_completed_session"]
    original_source_hash = hash_file(BUILDER)
    cases = []
    def add(name, rows, status, **kwargs):
        meta = build(name, rows, **kwargs)
        cases.append({"name": name, "expected_status": status, "actual_status": meta["status"],
                      "passed": meta["status"] == status, "subject": meta["subject"],
                      "metadata_path": str(HERE / "cases" / name / "artifacts/metadata.json")})
        return meta
    local = {v: load_row(v) for v in VERSIONS}
    cloud = {v: load_row(v,"cloud") for v in VERSIONS}
    a = add("real_local_close", local, "OK")
    b = add("real_cloud_close", cloud, "OK")
    assert a == b
    for version in VERSIONS:
        base = local[version]
        for field in ("version", "strategy_revision", "current_holding", "next_holding", "next_session_actionable_scale"):
            row = dict(base)
            row.pop(field)
            add(f"missing_{version}_{field}", {version: row}, "FAILED", stdout={version: f"{field}: {base[field]}\n"})
        add(f"wrong_revision_{version}", {version: {**base, "strategy_revision": "retired_other_version"}}, "FAILED")
        add(f"invalid_scale_{version}", {version: {**base, "next_session_actionable_scale": "inf"}}, "FAILED")
        add(f"html_holding_{version}", {version: {**base, "current_holding": "<script>attack()</script>\n## INJECT"}}, "FAILED")
        historical = add(f"historical_member_false_{version}", {version: base}, "OK")
        assert "需要名单调仓" not in historical["body"]
        add(f"historical_member_true_{version}", {version: {**base, "member_rebalance_actionable": "True"}}, "FAILED")
        add(f"member_yes_{version}", {version: {**base, "member_rebalance_actionable": "yes"}}, "FAILED")
        add(f"stale_close_{version}", {version: {**base, "date": "2026-09-29"}}, "FAILED")
        rt = realtime(base)
        add(f"holiday_first_session_{version}", {version: rt}, "OK", mode="realtime", expected="", now=next_open)
        for name, changes in {
            "old_anchor": {"latest_anchor_trade_date": "2026-09-29"},
            "missing_expected": {"expected_latest_completed_trade_date": ""},
            "future_anchor": {"latest_anchor_trade_date": "2026-10-08", "expected_latest_completed_trade_date": "2026-10-08"},
            "naive_snapshot": {"snapshot_time": "2026-10-08T14:15:00"},
            "stale_snapshot": {"snapshot_time": "2026-09-30T14:15:00+08:00"},
            "close_as_realtime": {"official_close_confirmed_signal": "True", "signal_timing": "close_confirmed"},
        }.items():
            add(f"{name}_{version}", {version: {**rt, **changes}}, "FAILED", mode="realtime", expected="", now=next_open)
        add(f"utc_before_deadline_{version}", {version: {**rt, "snapshot_time": "2026-10-08T06:59:59+00:00"}},
            "OK", mode="realtime", expected="", now=datetime(2026,10,8,14,59,59,tzinfo=BJ))
        add(f"snapshot_deadline_{version}", {version: {**rt, "snapshot_time": "2026-10-08T07:00:00+00:00"}},
            "FAILED", mode="realtime", expected="", now=datetime(2026,10,8,14,59,59,tzinfo=BJ))
        add(f"delayed_publication_deadline_{version}", {version: rt}, "FAILED", mode="realtime", expected="",
            now=datetime(2026,10,8,15,0,tzinfo=BJ))
    add("mixed_close_dates", {**local, "v2.3": {**local["v2.3"], "date": "2026-09-29"}}, "FAILED")
    # Header injection must be refused before transport construction or login.
    injection = build("subject_header_injection", {"v2.0": local["v2.0"]}, prefix="test\nBcc: attacker@example.invalid")
    message = EmailMessage()
    header_refused = False
    try:
        message["Subject"] = injection["subject"]
    except ValueError:
        header_refused = True
    assert header_refused
    mail_spec = importlib.util.spec_from_file_location("content_probe_mail", AUTO / "scripts/mail_utils.py")
    mail = importlib.util.module_from_spec(mail_spec)
    assert mail_spec and mail_spec.loader
    mail_spec.loader.exec_module(mail)
    env = {"MAIL_SERVER": "smtp.example.invalid", "MAIL_PORT": "465", "MAIL_USERNAME": "diagnostic",
           "MAIL_PASSWORD": "fake-not-a-secret", "MAIL_FROM": "sender@example.invalid", "MAIL_TO": "recipient@example.invalid"}
    actual_transport_refused = False
    with patch.dict(os.environ, env, clear=True), patch.object(mail.smtplib, "SMTP") as smtp, patch.object(mail.smtplib, "SMTP_SSL") as smtp_ssl:
        try:
            mail.send_mail(injection["subject"], injection["body"])
        except ValueError:
            actual_transport_refused = True
        smtp.assert_not_called()
        smtp_ssl.assert_not_called()
    assert actual_transport_refused
    # Full production MIME construction rejects the header before SMTP exists.
    report = {"scope": "independent_real_artifact_content_fault_injection_no_smtp",
              "calendar": calendar, "freshness": freshness, "builder_path": str(BUILDER),
              "builder_sha256": original_source_hash, "local_cloud_metadata_exact_equal": a == b,
              "cases": cases, "passed": sum(case["passed"] for case in cases),
              "failed": sum(not case["passed"] for case in cases), "header_injection_refused_before_transport": header_refused,
              "production_mail_utils_header_refused_without_smtp_construction": actual_transport_refused,
              "sent_mail": False, "notes": "Realtime cases are explicit mutated diagnostic inputs, not published quote snapshots or performance."}
    filename = sys.argv[1] if len(sys.argv) > 1 else "results.json"
    (HERE / filename).write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"cases": len(cases), "passed": report["passed"], "failed": report["failed"],
                      "bad_cases": [case["name"] for case in cases if not case["passed"]], "report": str(HERE / filename)}, ensure_ascii=False))


if __name__ == "__main__":
    main()
