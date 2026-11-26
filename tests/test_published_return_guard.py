"""Ordinary reruns must preserve every already published net return."""
import pandas as pd
import pytest

import microcap_top100_mom16_biweekly_live_v2_0 as v20
import microcap_top100_mom16_biweekly_live_v2_3 as v23
import microcap_top100_mom16_biweekly_live_v2_5 as v25
from scripts.top100_delivery import validate_final_nav


@pytest.mark.parametrize("strategy,version", [
    (v20.overlay_mod, "2_0"), (v23, "2_3"), (v25, "2_5"),
])
def test_published_return_cannot_change_inside_audit_tail(tmp_path, strategy, version):
    keys = getattr(strategy, f"V{version}_REWRITE_AUDIT_KEY_COLUMNS")
    allowances = getattr(strategy, f"V{version}_REWRITE_AUDIT_ALLOWED_TAIL_ROWS_BY_COLUMN")
    tail = getattr(strategy, f"_v{version}_rewrite_allowed_tail_rows")()
    days = pd.bdate_range("2026-01-05", periods=tail + 2)
    full = pd.DataFrame(0.0, index=days, columns=keys)
    full["nav_net"] = 1.0
    full.index.name = "date"
    previous = full.iloc[:-1].reset_index()
    appended_day = days[-1]
    changed_day = days[-2]
    full.loc[appended_day, "return_net"] = 0.01
    full["nav_net"] = (1 + full["return_net"]).cumprod()
    audit_path = tmp_path / "rewrite.csv"

    def audit(frame):
        v20.base_mod.assert_no_historical_rewrite(
            previous, frame.reset_index(), keys, tail, f"v{version}", audit_path,
            column_allowed_tail_rows=allowances,
        )

    audit(full)
    assert not audit_path.exists()
    full.loc[changed_day, "return_net"] += 0.02
    full["nav_net"] = (1 + full["return_net"]).cumprod()
    validate_final_nav(full.reset_index().to_dict("records"), f"v{version}")
    with pytest.raises(RuntimeError, match="historical rewrite detected"):
        audit(full)
    differences = pd.read_csv(audit_path)
    assert differences[["date", "column"]].values.tolist() == [[changed_day.date().isoformat(), "return_net"]]
