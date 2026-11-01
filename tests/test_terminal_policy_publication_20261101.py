import pandas as pd
import pytest
import microcap_top100_mom16_biweekly_live_v2_0 as v

POLICY = "delist_zero_writeoff_equal_survivors_v1"

@pytest.mark.parametrize("meta", [{}, {"delisting_valuation_policy": POLICY}, {"delisting_valuation_policy": POLICY, "core_params": {"delisting_valuation_policy": "old"}}])
def test_old_or_contradictory_authority_cannot_be_relabelled(monkeypatch, meta):
    monkeypatch.setitem(v._apply_terminal_loss_policy.__globals__, "_load_v2_base_proxy_meta", lambda: meta)
    signal = pd.DataFrame({"date": ["2026-10-30"]})
    with pytest.raises(RuntimeError, match="valuation policy"):
        v._apply_terminal_loss_policy(signal)
    assert "terminal_loss_policy" not in signal


def test_actual_authority_provides_policy_and_nontrade_note(monkeypatch):
    meta = {"delisting_valuation_policy": POLICY, "core_params": {"delisting_valuation_policy": POLICY}}
    monkeypatch.setitem(v._apply_terminal_loss_policy.__globals__, "_load_v2_base_proxy_meta", lambda: meta)
    signal = pd.DataFrame({"date": ["2026-10-30"], "terminal_loss_policy": ["old"]})
    v._apply_terminal_loss_policy(signal)
    assert signal.iloc[0].terminal_loss_policy == POLICY
    assert "非真实成交" in signal.iloc[0].terminal_loss_policy_note


def test_three_versions_use_common_publication_helpers():
    from pathlib import Path
    for name in ("microcap_top100_mom16_biweekly_live_v2_3.py", "microcap_top100_mom16_biweekly_live_v2_5.py"):
        text = Path(name).read_text(encoding="utf-8")
        assert "overlay_mod.augment_close_confirmed_signal_with_member_contract(" in text
        assert "overlay_mod._apply_realtime_meta_columns_to_signal_row(" in text
