import pandas as pd
import pytest
import microcap_top100_mom16_biweekly_live_v2_0 as v

@pytest.fixture
def base(): return v.base_mod


def test_old_st_display_label_is_replaced_without_mutating_history(base):
    old = pd.DataFrame({"symbol": ["000001"], "name": ["*ST旧名"], "rank": [1]})
    result = base._with_current_publication_names(old, {"000001": "正常新名"})
    assert result.name.iloc[0] == "正常新名"
    assert old.name.iloc[0] == "*ST旧名"
    assert result.symbol.equals(old.symbol) and result['rank'].equals(old['rank'])

@pytest.mark.parametrize("mapping", [{"000001": "*ST真实"}, {}, {"000001": ""}])
def test_current_risk_missing_or_empty_name_is_blocked(base, mapping):
    frame = pd.DataFrame({"symbol": ["000001"], "name": ["旧正常名"]})
    with pytest.raises(RuntimeError): base._with_current_publication_names(frame, mapping)

@pytest.mark.parametrize("rows,st", [
    ([['000001','正常'],['000001','另名']], {'000002':'ST真实'}),
    ([['000001','正常'],['000002','正常']], {'000002':'ST真实'}),
    ([['000001','正常'],['000002','ST真实']], {}),
])
def test_snapshot_duplicate_or_st_disagreement_is_blocked(base, tmp_path, monkeypatch, rows, st):
    path=tmp_path/'active.csv';pd.DataFrame(rows,columns=['code','name']).to_csv(path,index=False)
    monkeypatch.setattr(base.freq_mod, 'ACTIVE_UNIVERSE', path)
    monkeypatch.setattr(base.freq_mod, 'load_current_st_name_map', lambda: st)
    with pytest.raises(RuntimeError):base._current_publication_name_map()


def test_current_snapshot_coherent_identity_passes(base, tmp_path, monkeypatch):
    path=tmp_path/'active.csv';pd.DataFrame([['000001','正常'],['000002','*ST真实']],columns=['code','name']).to_csv(path,index=False)
    monkeypatch.setattr(base.freq_mod, 'ACTIVE_UNIVERSE', path)
    monkeypatch.setattr(base.freq_mod, 'load_current_st_name_map', lambda: {'000002':'*ST真实'})
    assert base._current_publication_name_map()=={'000001':'正常','000002':'*ST真实'}
