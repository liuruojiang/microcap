import hashlib
import json
import shutil
from pathlib import Path

import pytest

from scripts.st_notice_evidence import StNoticeEvidenceError, evidence_fingerprint

ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize("mutation", [None, "title", "prefix"])
def test_real_provider_whitespace_is_exactly_bound(tmp_path, mutation):
    source = ROOT / "data/st_notice_evidence"
    dest = tmp_path / "data/st_notice_evidence"
    dest.mkdir(parents=True)
    registry = json.loads((source / "registry.json").read_text(encoding="utf-8"))
    event = next(e for e in registry["events"] if e["announcement_id"] == "AN201304120003163874")
    registry["events"] = [event]
    for name in [event["relative_pdf_path"]] + [s["relative_path"] for s in event["native_evidence"].values()]:
        shutil.copy2(source / name, dest / name)
    assert event["title"].startswith(" ")
    if mutation:
        field = "title" if mutation == "title" else "removed_issuer_prefix"
        event[field] = event[field].strip() if mutation == "title" else event[field] + " "
        # Rehashing the human-review artifact cannot override the raw API title.
        spec = event["native_evidence"]["review"]
        path = dest / spec["relative_path"]
        review = json.loads(path.read_text(encoding="utf-8"))
        review[field] = event[field]
        path.write_text(json.dumps(review), encoding="utf-8")
        spec["sha256"] = hashlib.sha256(path.read_bytes()).hexdigest()
    (dest / "registry.json").write_text(json.dumps(registry), encoding="utf-8")
    if mutation:
        with pytest.raises(StNoticeEvidenceError):
            evidence_fingerprint(tmp_path)
    else:
        assert evidence_fingerprint(tmp_path)
