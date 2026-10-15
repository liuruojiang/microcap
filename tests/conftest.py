"""Explicit frozen-data fixtures; no implicit refresh or production writes."""
import pytest

from frozen_formal_baseline import (
    assert_frozen_baseline_unchanged,
    load_frozen_formal_baseline,
    write_synthetic_realtime,
)


@pytest.fixture(scope="session")
def frozen_formal_baseline():
    baseline = load_frozen_formal_baseline()
    yield baseline
    assert_frozen_baseline_unchanged(baseline)


@pytest.fixture(scope="session")
def synthetic_realtime_artifacts(tmp_path_factory):
    root = tmp_path_factory.mktemp("synthetic_realtime_contract")
    paths = {v: root / f"v2_{v}_realtime.csv" for v in ("0", "3", "5")}
    for version, path in paths.items():
        write_synthetic_realtime(path, version)
    return paths
