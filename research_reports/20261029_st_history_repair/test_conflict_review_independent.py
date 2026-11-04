"""Independent counterexamples: self-consistent hashes do not override review identity."""
import hashlib
import json
from pathlib import Path
import shutil
import pytest
from scripts.st_notice_evidence import evidence_fingerprint, evidence_source_files, StNoticeEvidenceError

ROOT = Path(__file__).resolve().parents[2]


@pytest.mark.parametrize("damage", ["missing_event", "changed_action", "wrong_provider_title", "missing_review"])
def test_rehashed_counterexamples_still_block(tmp_path, damage):
    folder = tmp_path / "data/st_notice_evidence"
    shutil.copytree(ROOT / "data/st_notice_evidence", folder)
    path = folder / "registry.json"
    registry = json.loads(path.read_text(encoding="utf-8"))
    event = next(e for e in registry["events"] if e["announcement_id"] == "1220378276")
    if damage == "missing_review":
        del event["conflict_evidence"]["review"]
    else:
        kind = "provider_response" if damage == "wrong_provider_title" else "review"
        spec = event["conflict_evidence"][kind]
        artifact = folder / spec["relative_path"]
        obj = json.loads(artifact.read_text(encoding="utf-8"))
        if damage == "missing_event":
            del obj["event"]
        elif damage == "changed_action":
            obj["event"]["risk_actions"] = {"delisting": True, "other": False}
        else:
            next(r for r in obj["announcements"] if r["announcementId"] == "1220378276")["announcementTitle"] += "伪造"
        artifact.write_text(json.dumps(obj, ensure_ascii=False), encoding="utf-8")
        spec["sha256"] = hashlib.sha256(artifact.read_bytes()).hexdigest()
    path.write_text(json.dumps(registry, ensure_ascii=False), encoding="utf-8")
    with pytest.raises(StNoticeEvidenceError):
        evidence_fingerprint(tmp_path)


def test_delivery_transports_complete_evidence_closure():
    from scripts import top100_delivery as delivery
    from scripts import top100_cloud_delivery as cloud
    required = set(evidence_source_files(ROOT))
    assert required <= set(delivery.RELEASE_FILES)
    assert required <= set(cloud.SOURCE_FILES)
    assert all((ROOT / path).is_file() for path in required)
