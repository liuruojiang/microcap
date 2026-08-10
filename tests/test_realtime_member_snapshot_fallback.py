from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

import microcap_top100_mom16_biweekly_live_v2_0 as v2_0
from scripts import realtime_state_bundle


def _proxy_member_rows(rebalance_date: str) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "rebalance_date": [rebalance_date] * 100,
            "rank": range(1, 101),
            "symbol": [f"{value:06d}" for value in range(1, 101)],
            "name": [f"member-{value}" for value in range(1, 101)],
            "market_cap": [float(value) for value in range(1, 101)],
            "target_weight": [0.01] * 100,
        }
    )


def test_member_snapshots_use_proxy_members_when_raw_cache_pool_is_empty(
    tmp_path: Path,
    monkeypatch,
) -> None:
    snapshot_dates = [pd.Timestamp("2026-07-23"), pd.Timestamp("2026-08-06")]
    proxy_members = tmp_path / "proxy_members.csv"
    pd.concat(
        [_proxy_member_rows("2026-07-23"), _proxy_member_rows("2026-08-06")],
        ignore_index=True,
    ).to_csv(proxy_members, index=False)

    def empty_raw_cache(*_args, **_kwargs):
        raise RuntimeError(
            "live rebalance candidate pool below top_n on 2026-07-23: "
            "available=0 required=100"
        )

    helper = v2_0.base_mod.load_member_snapshots_with_proxy_fallback
    monkeypatch.setitem(helper.__globals__, "load_member_snapshot", empty_raw_cache)

    snapshots = helper(
        snapshot_dates=snapshot_dates,
        max_workers=1,
        paths={"proxy_members": proxy_members},
    )

    assert set(snapshots) == set(snapshot_dates)
    assert all(len(frame) == 100 for frame in snapshots.values())
    assert all(frame["symbol"].nunique() == 100 for frame in snapshots.values())


def _write_csv(path: Path, frame: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(path, index=False)


def _write_minimal_required_state(root: Path) -> None:
    daily = pd.DataFrame({"date": ["2026-08-07"], "value": [1.0]})
    _write_csv(root / realtime_state_bundle.REQUIRED_FILES[0], daily)
    summary = root / realtime_state_bundle.REQUIRED_FILES[1]
    summary.parent.mkdir(parents=True, exist_ok=True)
    summary.write_text("{}", encoding="utf-8")
    _write_csv(root / realtime_state_bundle.REQUIRED_FILES[2], daily)
    proxy_meta = root / realtime_state_bundle.REQUIRED_FILES[3]
    proxy_meta.write_text("{}", encoding="utf-8")
    _write_csv(root / realtime_state_bundle.REQUIRED_FILES[4], _proxy_member_rows("2026-08-06"))
    _write_csv(
        root / realtime_state_bundle.REQUIRED_FILES[5],
        pd.DataFrame({"rebalance_date": ["2026-08-06"], "turnover": [0.1]}),
    )
    _write_csv(root / realtime_state_bundle.REQUIRED_FILES[6], daily)
    _write_csv(
        root / realtime_state_bundle.REQUIRED_FILES[7],
        pd.DataFrame({"code": [f"{value:06d}" for value in range(1, 101)], "name": ["member"] * 100}),
    )
    _write_csv(
        root / realtime_state_bundle.REQUIRED_FILES[8],
        pd.DataFrame({"code": ["000999"], "name": ["st"]}),
    )


def test_validate_state_rejects_obsolete_static_context_without_price_cache(tmp_path: Path) -> None:
    _write_minimal_required_state(tmp_path)
    prefix = (
        tmp_path
        / ".microcap_index_cache/realtime/"
        / "microcap_top100_mom16_biweekly_live_v2_0_base"
    )
    prefix.parent.mkdir(parents=True, exist_ok=True)
    Path(f"{prefix}_static_meta.json").write_text(
        json.dumps({"latest_rebalance": "2026-07-23"}),
        encoding="utf-8",
    )
    old_members = _proxy_member_rows("2026-07-23")
    old_members.to_csv(Path(f"{prefix}_static_target_members.csv"), index=False)
    old_members.to_csv(Path(f"{prefix}_static_effective_members.csv"), index=False)
    pd.DataFrame(columns=["symbol", "action"]).to_csv(
        Path(f"{prefix}_static_rebalance_changes.csv"),
        index=False,
    )

    report = realtime_state_bundle.validate_state(
        tmp_path,
        max_anchor_age_days=3,
        today=pd.Timestamp("2026-08-10").date(),
    )

    assert report["ok"] is False
    assert any("current v2.0 static member context" in error for error in report["errors"])
