"""Exact approved seed evidence, separate from a current delivery snapshot.

No fixture authorizes new history or regenerates a manifest. Git-less audit
copies must explicitly provide MICROCAP_TEST_SEED_ROOT exported by their runner.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import shutil
import subprocess

import pandas as pd
import pytest

from scripts import top100_delivery as delivery
from frozen_formal_baseline import assert_frozen_baseline_unchanged, digest


def validate_immutable_seed(root: Path, approved_authority: Path) -> dict:
    authority_path = root / delivery.AUTHORITY
    assert delivery.sha(authority_path) == delivery.sha(approved_authority), (
        "Seed authority differs from the authenticated delivery authority")
    authority = json.loads(authority_path.read_text(encoding="utf-8"))
    assert set(authority["seed_file_sha256"]) == set(delivery.BASE_FILES)
    for label, name in delivery.BASE_FILES.items():
        assert delivery.sha(root / "outputs" / name) == authority["seed_file_sha256"][label], (
            f"Immutable seed hash mismatch: {label}")
    for label in ("proxy_index", "costed_nav"):
        frame = pd.read_csv(root / "outputs" / delivery.BASE_FILES[label])
        assert len(frame) == authority["seed_file_rows"][label]
        assert frame.iloc[-1]["date"] == authority["seed_end_date"]
    meta = json.loads((root / "outputs" / delivery.BASE_FILES["proxy_meta"]).read_text(encoding="utf-8"))
    assert meta["core_params"]["security_meta_cache_fingerprint"] == authority["security_meta_cache_fingerprint"]
    return authority


def export_immutable_seed(destination: Path, approved_authority: Path, *,
                          source_root: Path | None = None, repository: Path | None = None) -> dict:
    """Export exact files, then authenticate all six against a trusted authority."""
    names = [delivery.AUTHORITY, *["outputs/" + name for name in delivery.BASE_FILES.values()]]
    destination.mkdir(parents=True, exist_ok=True)
    assert not any(destination.iterdir()), "Seed destination must be a new empty directory"
    revision = None
    if source_root is None:
        repository = repository or Path(__file__).resolve().parents[1]
        try:
            revision = subprocess.check_output(
                ["git", "-C", str(repository), "rev-parse", "HEAD"], stderr=subprocess.PIPE, text=True).strip()
        except (OSError, subprocess.CalledProcessError) as exc:
            raise RuntimeError("No immutable seed source: set MICROCAP_TEST_SEED_ROOT for a Git-less test copy") from exc
    for name in names:
        target = destination / name
        target.parent.mkdir(parents=True, exist_ok=True)
        if source_root is not None:
            shutil.copy2(source_root / name, target)
        else:
            target.write_bytes(subprocess.check_output(
                ["git", "-C", str(repository), "show", f"{revision}:{name}"], stderr=subprocess.PIPE))
    authority = validate_immutable_seed(destination, approved_authority)
    return {"root": destination, "authority": authority, "source_revision": revision,
            "hashes": {name: digest(destination / name) for name in names}}


@pytest.fixture(scope="session")
def immutable_seed_baseline(tmp_path_factory, frozen_formal_baseline):
    explicit = os.environ.get("MICROCAP_TEST_SEED_ROOT")
    seed = export_immutable_seed(
        tmp_path_factory.mktemp("approved_seed"),
        frozen_formal_baseline["root"] / delivery.AUTHORITY,
        source_root=Path(explicit).resolve() if explicit else None)
    yield seed
    assert_frozen_baseline_unchanged(seed)


def assert_continuation_preserves_seed_economics(seed_root: Path, current_root: Path) -> None:
    """Use the existing production rewrite contract with zero mutable tail rows.

    No new tolerances, migrated hashes, or success monkeypatches are introduced.
    The delivery fixture separately authenticates every current artifact byte.
    """
    import microcap_top100_mom16_biweekly_live_v2_0 as v20

    for label in ("proxy_index", "costed_nav", "proxy_turnover"):
        name = delivery.BASE_FILES[label]
        previous = pd.read_csv(seed_root / "outputs" / name)
        candidate = pd.read_csv(current_root / "outputs" / name)
        if label == "proxy_turnover":
            previous = previous.rename(columns={"rebalance_date": "date"})
            candidate = candidate.rename(columns={"rebalance_date": "date"})
        v20.base_mod.assert_no_historical_rewrite(
            previous, candidate, key_columns=[key for key in previous if key != "date"],
            allowed_tail_rows=0, label=f"approved seed to current {label}")

    name = delivery.BASE_FILES["proxy_members"]
    previous = pd.read_csv(seed_root / "outputs" / name, dtype={"symbol": str})
    candidate = pd.read_csv(current_root / "outputs" / name, dtype={"symbol": str})
    historical = candidate.loc[candidate.rebalance_date <= previous.rebalance_date.max()]
    identity = [key for key in previous if key != "market_cap"]
    pd.testing.assert_frame_equal(previous[identity].reset_index(drop=True),
                                  historical[identity].reset_index(drop=True), check_exact=True)
    # Market-cap serialization follows the SAME native numerical contract; the
    # selected identities and ranks above remain strictly exact.
    if "market_cap" in previous:
        left = previous.pivot(index="rebalance_date", columns="rank", values="market_cap")
        right = candidate.pivot(index="rebalance_date", columns="rank", values="market_cap")
        left.index, right.index = pd.to_datetime(left.index), pd.to_datetime(right.index)
        v20.base_mod.assert_no_historical_rewrite(
            left, right, key_columns=list(left.columns), allowed_tail_rows=0,
            label="approved historical ranked member capitalization")
