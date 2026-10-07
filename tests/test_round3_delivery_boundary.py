"""Real final-file attacks; isolated semantics, never production acceptance."""
from __future__ import annotations

import csv
import hashlib
import json
import shutil
from pathlib import Path

import pandas as pd
import pytest

from scripts import top100_delivery as delivery


ROOT = Path(__file__).resolve().parents[1]
AUDIT = ROOT / "research_reports" / "20261007_audit_round3"


@pytest.fixture(scope="module")
def frozen_delivery(tmp_path_factory):
    proof = json.loads((AUDIT / "baseline_and_freshness.json").read_text(encoding="utf-8"))
    for name, item in proof["artifacts"].items():
        assert hashlib.sha256(Path(item["path"]).read_bytes()).hexdigest() == item["raw_sha256"], name
    baseline = delivery.inspect_outputs(ROOT, proof["latest_completed_session"])
    assert baseline["ok"], baseline["errors"]
    root = tmp_path_factory.mktemp("round3_delivery")
    for name in set(baseline["inputs"]) | set(baseline["artifacts"]):
        dest = root / name
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(ROOT / name, dest)
    originals = {}
    for v in ("0", "3", "5"):
        for name in (delivery.COSTED[v], f"microcap_top100_mom16_biweekly_live_v2_{v}_nav.csv",
                     f"microcap_top100_mom16_biweekly_live_v2_{v}_latest_signal.csv"):
            p = root / "outputs" / name
            originals[p] = p.read_bytes()
    yield root, proof["latest_completed_session"], originals
    for name, item in proof["artifacts"].items():
        assert hashlib.sha256(Path(item["path"]).read_bytes()).hexdigest() == item["raw_sha256"], name


@pytest.fixture
def workspace(frozen_delivery):
    root, date, originals = frozen_delivery
    for p, payload in originals.items():
        p.write_bytes(payload)
    yield root, date
    for p, payload in originals.items():
        p.write_bytes(payload)


def _save_rows(path, rows):
    with path.open("w", encoding="utf-8", newline="") as handle:
        csv.writer(handle).writerows(rows)


def _csv_rows(path):
    with path.open(encoding="utf-8-sig", newline="") as handle:
        return [row for row in csv.reader(handle) if row]


def _probe(case, report, **fields):
    AUDIT.mkdir(exist_ok=True)
    with (AUDIT / "root_delivery_probes.jsonl").open("a", encoding="utf-8") as handle:
        handle.write(json.dumps({"case": case, "ok": report["ok"], "errors": report["errors"],
                                 "delivery_source_sha256": hashlib.sha256(Path(delivery.__file__).read_bytes()).hexdigest(),
                                 **fields}, ensure_ascii=False) + "\n")


def test_authentic_three_version_final_delivery_semantics_remain_valid(workspace):
    root, date = workspace
    report = delivery.inspect_outputs(root, date)
    assert report["ok"], report["errors"]


@pytest.mark.parametrize("v", ("0", "3", "5"))
@pytest.mark.parametrize("mode", ("wrong_version", "missing_version"))
def test_final_nav_version_identity_cannot_disagree_with_named_stream(workspace, v, mode):
    root, date = workspace
    for name in (delivery.COSTED[v], f"microcap_top100_mom16_biweekly_live_v2_{v}_nav.csv"):
        path = root / "outputs" / name
        rows = _csv_rows(path)
        position = rows[0].index("version")
        if mode == "wrong_version":
            rows[len(rows) // 2][position] = "2.5" if v != "5" else "2.0"
        else:
            for row in rows:
                del row[position]
        _save_rows(path, rows)
    report = delivery.inspect_outputs(root, date)
    _probe(f"nav_identity_{v}_{mode}", report)
    assert not report["ok"], "A contradictory final NAV version was certified"


@pytest.mark.parametrize("v", ("0", "3", "5"))
@pytest.mark.parametrize("mode", ("extra_cell", "short_row"))
def test_native_signal_ragged_rows_cannot_be_certified(workspace, v, mode):
    root, date = workspace
    path = root / "outputs" / f"microcap_top100_mom16_biweekly_live_v2_{v}_latest_signal.csv"
    rows = _csv_rows(path)
    assert len(rows) == 2 and len(rows[0]) == len(rows[1])
    if mode == "extra_cell":
        rows[1].append("unheaded_payload")
    else:
        rows[1].pop()
    _save_rows(path, rows)
    pandas_version = str(pd.read_csv(path).iloc[0]["version"])
    report = delivery.inspect_outputs(root, date)
    _probe(f"ragged_signal_{v}_{mode}", report, pandas_version=pandas_version,
           actual_header_columns=len(rows[0]), actual_row_columns=len(rows[1]))
    assert not report["ok"], "Final CSV with incompatible row/header width was certified"
