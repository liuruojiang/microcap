from __future__ import annotations

import json
import zipfile
from pathlib import Path

import pandas as pd
import pytest

from scripts import full_rebalance_cache_bundle as bundle


def _seed(root: Path) -> None:
    cache = root / ".microcap_index_cache"
    for directory, suffix in bundle.CACHE_DIRS.items():
        target = cache / directory
        target.mkdir(parents=True, exist_ok=True)
        for symbol in ("000001", "000002"):
            path = target / f"{symbol}{suffix}"
            if suffix == ".json":
                path.write_text(json.dumps({"symbol": symbol}), encoding="utf-8")
            else:
                pd.DataFrame({"date": ["2026-09-03"], "value": [1.0]}).to_csv(path, index=False)
    pd.DataFrame(
        {"code": ["000001", "000002"], "name": ["one", "two"]}
    ).to_csv(cache / "active_universe.csv", index=False)
    pd.DataFrame({"code": ["000999"], "name": ["ST sample"]}).to_csv(
        cache / "current_st.csv", index=False
    )


def test_full_cache_bundle_round_trip(tmp_path: Path) -> None:
    source = tmp_path / "source"
    target = tmp_path / "target"
    archive = tmp_path / "full-cache.zip"
    _seed(source)

    packed = bundle.pack_cache(source, archive, min_symbols=2)
    restored = bundle.restore_cache(target, archive, min_symbols=2)

    assert packed["ok"] is True
    assert restored["ok"] is True
    assert restored["counts"] == {"prices_raw": 2, "security_meta": 2, "share_change": 2}
    assert (target / ".microcap_index_cache/prices_raw/000001.csv").is_file()


def test_full_cache_bundle_rejects_tampering_before_extracting(tmp_path: Path) -> None:
    source = tmp_path / "source"
    target = tmp_path / "target"
    archive = tmp_path / "full-cache.zip"
    _seed(source)
    assert bundle.pack_cache(source, archive, min_symbols=2)["ok"] is True
    with zipfile.ZipFile(archive, "a") as handle:
        handle.writestr(".microcap_index_cache/prices_raw/000001.csv", "tampered")

    with pytest.raises(ValueError, match="duplicate archive members"):
        bundle.restore_cache(target, archive, min_symbols=2)
    assert not (target / ".microcap_index_cache").exists()


def test_full_cache_validation_fails_closed_on_narrow_runner_cache(tmp_path: Path) -> None:
    _seed(tmp_path)

    report = bundle.validate_cache(tmp_path, min_symbols=3)

    assert report["ok"] is False
    assert any("prices_raw count=2 minimum=3" in error for error in report["errors"])
