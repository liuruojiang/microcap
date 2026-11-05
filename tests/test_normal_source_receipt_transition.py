"""Actual normal refresh_state -> three outputs -> shared pack receipt lifecycle.

Market calculations use the existing synthetic delivery fixture. Native refresh
control flow, complete final-stream inspection, ZIP writing/verification and
source-provenance validation remain real; this is not fresh market acceptance.
"""
import csv
import hashlib
import io
import json
import sys
from types import SimpleNamespace
import zipfile

import pandas as pd
import pytest

from scripts import realtime_state_bundle as state
from scripts import top100_cloud_delivery as cloud
from scripts import top100_delivery as delivery
from test_top100_delivery import workspace


@pytest.fixture
def certified(workspace, monkeypatch):
    root, day = workspace, "2026-09-03"
    original = delivery.inspect_outputs(root, day)
    assert original["ok"], original["errors"]
    original.update(status="complete", release_sha="b" * 40)
    raw = json.dumps(original, indent=2).encode()
    (root / delivery.MANIFEST).write_bytes(raw)
    changes = {}
    for name in state.VALIDATION_SOURCE_REVIEW["changes"]:
        before = delivery.sha(root / name)
        (root / name).write_bytes((root / name).read_bytes() + b"\n# diagnostic reviewed validator upgrade\n")
        changes[name] = (before, delivery.sha(root / name))
    review = {"revision": "diagnostic_normal_next_session", "expected_date": day,
              "manifest_sha256": hashlib.sha256(raw).hexdigest(),
              "stock_fingerprint_sha256": "c" * 64, "changes": changes}
    monkeypatch.setattr(state, "VALIDATION_SOURCE_REVIEW", review)
    monkeypatch.setattr(delivery, "RELEASE_FILES", list(original["inputs"]))
    monkeypatch.setattr(delivery, "verify_release", lambda root: "a" * 40)
    monkeypatch.setattr(delivery, "independent_target", lambda root: day)
    def validated(root, *args, **kwargs):
        target = str(kwargs.get("close_confirmed_session") or day)
        return {"ok": True, "errors": [], "anchor_dates": {k: target for k in delivery.DAILY_STATE_ANCHORS}}
    monkeypatch.setattr(state, "validate_state", validated)
    delivery.recertify_validation_source(root, day)
    prior = (root / delivery.MANIFEST).read_bytes()
    monkeypatch.setattr(cloud.subprocess, "check_output", lambda *a, **kw: "a" * 40)
    return root, prior


def normal_refresh_and_generate(root, target, monkeypatch):
    """Use normal workflow ordering, including deleting old latest CSVs first."""
    current = "2026-09-03"
    finals = {v: root / f"outputs/microcap_top100_mom16_biweekly_live_v2_{v}_latest_signal.csv" for v in delivery.COSTED}
    old_final = {v: path.read_text(encoding="utf-8") for v, path in finals.items()}
    for path in finals.values():
        path.unlink()
    calls = []
    def advance_panel(*args):
        calls.append("refresh_market_panel")
        if target != current:
            daily_names = {delivery.BASE_PANEL, delivery.BASE_FILES["proxy_index"], delivery.BASE_FILES["costed_nav"]}
            for version, costed in delivery.COSTED.items():
                daily_names.update((costed, f"microcap_top100_mom16_biweekly_live_v2_{version}_nav.csv",
                                    f"microcap_top100_mom16_biweekly_live_v2_{version}_performance_nav.csv"))
            for name in daily_names:
                path = root / "outputs" / name
                text = path.read_text()
                tail = [line.replace(current + ",0.02,", target + ",0.0,") for line in text.splitlines() if line.startswith(current + ",")]
                path.write_text(text.rstrip("\n") + "\n" + "\n".join(tail) + "\n", encoding="utf-8")
            for path in (root / state.PRICE_CACHE_DIR).glob("*.csv"):
                text = path.read_text()
                tail = [line.replace(current, target) for line in text.splitlines() if line.startswith(current + ",")]
                path.write_text(text.rstrip("\n") + "\n" + "\n".join(tail) + "\n", encoding="utf-8")
            for version in delivery.COSTED:
                path = root / f"outputs/microcap_top100_mom16_biweekly_live_v2_{version}_summary.json"
                summary = json.loads(path.read_bytes())
                summary.update(latest_nav_date=target, latest_trade_date=target)
                summary["data_freshness_proof"]["expected_latest_date"] = target
                path.write_text(json.dumps(summary), encoding="utf-8")
            reference = root / state.REQUIRED_FILES[1]
            summary = json.loads(reference.read_bytes()); summary["latest_trade_date"] = target
            reference.write_text(json.dumps(summary), encoding="utf-8")
        return root / "outputs" / delivery.BASE_PANEL, pd.Timestamp(target)
    base = SimpleNamespace(DEFAULT_OUTPUT_PREFIX="diagnostic", build_output_paths=lambda *a: {},
        build_refreshed_panel_shadow=advance_panel, ensure_strategy_files=lambda *a: None,
        ensure_realtime_query_base_context=lambda *a: {"close_df": pd.DataFrame(index=[pd.Timestamp(target)])},
        ensure_static_members_fresh=lambda *a: {}, freq_mod=SimpleNamespace())
    facade = SimpleNamespace(_sync_embedded_base_config=lambda: None, _build_base_args=lambda **kw: SimpleNamespace(), base_mod=base)
    monkeypatch.setitem(sys.modules, "microcap_top100_mom16_biweekly_live_v2_0", facade)
    monkeypatch.setitem(sys.modules, "run_top100_v1_6_v1_8_realtime_signals",
                        SimpleNamespace(ensure_static_realtime_inputs=lambda **kw: None))
    monkeypatch.setattr(state, "persist_reference_summary", lambda *a: None)
    monkeypatch.setattr(state, "materialize_member_cache_inputs", lambda *a: [])
    monkeypatch.setattr(state, "_write_refresh_proof", lambda *a: None)
    monkeypatch.setattr(state, "validate_refreshed_state", lambda *a: {
        "ok": True, "errors": [], "anchor_dates": {k: target for k in delivery.DAILY_STATE_ANCHORS}})
    report = state.refresh_state(root)
    assert report["ok"] and report["target_end_date"] == target
    assert calls == ["refresh_market_panel"]
    for version, path in finals.items():
        text = old_final[version]
        # Non-economic generation annotation also exercises a same-day rerender.
        rows = list(csv.DictReader(io.StringIO(text))); rows[0]["generation_note"] = "diagnostic fresh final"
        rows[0]["date"] = target
        with path.open("w", encoding="utf-8", newline="") as stream:
            writer = csv.DictWriter(stream, fieldnames=list(rows[0])); writer.writeheader(); writer.writerows(rows)


@pytest.mark.parametrize("target", ["2026-09-03", "2026-09-04"])
def test_normal_same_and_next_day_pack_preserves_authenticated_previous_receipt(certified, monkeypatch, target):
    root, prior = certified
    normal_refresh_and_generate(root, target, monkeypatch)
    # Ordinary check remains strict until the official new-generation writer.
    assert not delivery.inspect_outputs(root, target)["ok"]
    bundle = root.parent / (root.name + "-normal.zip")
    result = cloud.pack({v: root for v in delivery.COSTED}, bundle, target)
    assert result["ok"]
    with zipfile.ZipFile(bundle) as archive:
        state._verify_bundle_manifest(archive)
        manifest = json.loads(archive.read(delivery.MANIFEST))
    assert "source_validation_compatibility" not in manifest
    previous = manifest["previous_source_validation"]
    assert previous == {"manifest_sha256": hashlib.sha256(prior).hexdigest(), "manifest_utf8": prior.decode()}
    assert manifest["expected_date"] == target
    # Read back the packaged manifest through the ordinary strict inspector.
    (root / delivery.MANIFEST).write_text(json.dumps(manifest), encoding="utf-8")
    assert delivery.validate_manifest(root, delivery.inspect_outputs(root, target))["ok"]


@pytest.mark.parametrize("fault", ["bad_old_receipt", "malformed_manifest", "missing_final", "history_rewrite", "wrong_day", "base_failure"])
def test_normal_pack_does_not_waive_old_provenance_or_new_validation(certified, monkeypatch, fault):
    root, prior = certified
    target = "2026-09-04"
    normal_refresh_and_generate(root, target, monkeypatch)
    if fault == "bad_old_receipt":
        old = json.loads(prior); old["source_validation_compatibility"]["original_manifest_utf8"] += " "
        (root / delivery.MANIFEST).write_text(json.dumps(old), encoding="utf-8")
    elif fault == "malformed_manifest":
        (root / delivery.MANIFEST).write_text("{", encoding="utf-8")
    elif fault == "missing_final":
        (root / "outputs/microcap_top100_mom16_biweekly_live_v2_5_latest_signal.csv").unlink()
    elif fault == "history_rewrite":
        path = root / "outputs/microcap_top100_mom16_biweekly_live_v2_3_summary.json"
        summary = json.loads(path.read_bytes()); summary["historical_rewrite_audit"]["status"] = "changed"
        path.write_text(json.dumps(summary), encoding="utf-8")
    elif fault == "wrong_day":
        target = "2026-09-03"
    else:
        monkeypatch.setattr(state, "validate_state", lambda *a, **kw: {"ok": False, "errors": ["diagnostic source/history gate failure"]})
    bundle = root.parent / (root.name + "-rejected.zip")
    with pytest.raises((RuntimeError, ValueError)):
        cloud.pack({v: root for v in delivery.COSTED}, bundle, target)
    assert not bundle.exists()


def test_strict_receipt_still_rejects_advanced_normal_outputs(certified, monkeypatch):
    """Keep the legacy exact-receipt boundary without requiring a Git checkout.

    The original e41 modules were separately executed in lifecycle a798bd30;
    daily materialized source intentionally contains neither .git nor old refs.
    """
    root, prior = certified
    normal_refresh_and_generate(root, "2026-09-04", monkeypatch)
    report = delivery.inspect_outputs(root, "2026-09-04")
    assert report["ok"] is False
    assert any("Certified source manifest/current mismatch: expected_date" in error for error in report["errors"])
    assert (root / delivery.MANIFEST).read_bytes() == prior
