"""Real immutable delivery baseline controls; never current-market freshness."""
import json
import shutil
from pathlib import Path

import pytest

from frozen_formal_baseline import (
    assert_frozen_baseline_unchanged,
    data_root,
    load_frozen_formal_baseline,
)


def test_real_recorded_baseline_preserves_original_hash_contract():
    root = data_root()
    manifest = json.loads((root / "outputs/top100_delivery_manifest.json").read_bytes())
    baseline = load_frozen_formal_baseline(root)
    assert baseline["target"] == manifest["expected_date"]
    assert baseline["report"]["ok"] is True
    assert baseline["snapshot"]["recorded_release_sha"] == manifest["release_sha"]
    assert baseline["snapshot"]["same_day_freshness_certified_by_this_fixture"] is False
    assert_frozen_baseline_unchanged(baseline)


@pytest.mark.parametrize("change", ["normalize_crlf", "one_byte"])
def test_original_evidence_bytes_cannot_be_normalized_or_changed(tmp_path, change):
    """An incomplete negative shadow must reject evidence before later paths."""
    root = data_root()
    manifest_path = Path("outputs/top100_delivery_manifest.json")
    manifest = json.loads((root / manifest_path).read_bytes())
    evidence = "data/st_notice_evidence/registry.json"
    shadow = tmp_path / "invalid_evidence_shadow"
    (shadow / manifest_path).parent.mkdir(parents=True)
    shutil.copyfile(root / manifest_path, shadow / manifest_path)
    copied = False
    for name in {**manifest["inputs"], **manifest["artifacts"]}:
        destination = shadow / name
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(root / name, destination)
        if name == evidence:
            copied = True
            break
    assert copied, "The real recorded manifest must authenticate original ST evidence"
    path = shadow / evidence
    original = path.read_bytes()
    changed = original.replace(b"\r\n", b"\n") if change == "normalize_crlf" else original + b"\n"
    assert changed != original, "The negative control must actually change source bytes"
    path.write_bytes(changed)
    with pytest.raises(AssertionError, match="Frozen delivery baseline hash mismatch: " + evidence):
        load_frozen_formal_baseline(shadow)
