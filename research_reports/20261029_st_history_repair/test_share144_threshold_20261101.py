import sys
from pathlib import Path
import pytest
sys.path.insert(0,str(Path(__file__).resolve().parent))
from prepare_share144_threshold_review_20261101 import threshold_check

def test_set_proof_matches_full_cross_section_with_order_change():
 selected=[(f'{i:06}',float(i+1)) for i in range(100)]
 original=dict(selected);original.update({'000100':101.,'000101':102.})
 baseline={'000000':1.,'000001':2.,'000100':101.}
 alternate={'000000':2.5,'000001':1.5,'000100':100.5}
 result=threshold_check(selected,baseline,alternate)
 changed={**original,**alternate};actual=sorted(changed,key=lambda s:(changed[s],s))[:100]
 assert result['target_set_proven_unchanged'] and set(actual)==set(dict(selected))
 assert result['selected_rank_changed'] and actual==result['selected_reordered_symbols']
 assert result['execution_nav_certified'] is False

def test_rank100_move_is_not_cleared_and_actual_entry_detected():
 selected=[(f'{i:06}',float(i+1)) for i in range(100)]
 result=threshold_check(selected,{'000099':100.},{'000099':103.})
 assert result['requires_full_cross_section'] and not result['target_set_proven_unchanged']
 assert threshold_check(selected,{'000100':101.},{'000100':99.5})['requires_full_cross_section']
 # Equal cap must use symbol tie-break, not cap-only <= logic.
 assert threshold_check(selected,{'000100':101.},{'000100':100.})['target_set_proven_unchanged']

def test_source_cap_or_eligibility_mismatch_rejected():
 selected=[(f'{i:06}',float(i+1)) for i in range(100)]
 for before,after in [({'000000':3.},{'000000':4.}),({'000000':None},{'000000':None}),({'000100':50.},{'000100':200.})]:
  with pytest.raises(ValueError):threshold_check(selected,before,after)

def test_refinement_exact_subset_bound_and_unsafe_selected_rejection():
 from refine_share144_threshold_20261101 import refine
 selected=[(f'{i:06}',float(i+1)) for i in range(100)]
 result=refine(selected,{'000100':101.,'000101':102.},{'000100':98.5,'000101':97.5})
 full={**dict(selected),'000100':98.5,'000101':97.5,'000102':103.}
 assert result['alternative']==sorted(full,key=lambda s:(full[s],s))[:100]
 assert result['entered']==['000100','000101'] and result['exited']==['000098','000099']
 with pytest.raises(ValueError,match='Selected issuer exceeds'):refine(selected,{'000099':100.},{'000099':104.})
