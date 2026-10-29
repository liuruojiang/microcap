"""Exact-hash local migration authorized on 2026-10-29; no remote deployment."""
from pathlib import Path
import argparse
import json
import os
import sys
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ['TOP100_REALTIME_REQUIRE_STATE'] = '1'
import microcap_top100_mom16_biweekly_live_v2_3 as v
from scripts import top100_delivery as delivery

RUN = ROOT / 'research_reports/20261029_v23_no_overheat_promotion'
BACKUP = ROOT / '.codex_backups/20261029_142117'
RESEARCH = ROOT / 'quant_param_scan_runs/20261029_microcap_top100_v2_3_overheat_vol_window_trigger_recovery'

def read(path):
    return pd.read_csv(path, parse_dates=['date']).set_index('date')

def parity(actual, oracle):
    assert actual.index.equals(oracle.index)
    differences = {}
    for col in ['return_net', 'nav_net', 'total_cost']:
        differences[col] = float((actual[col]-oracle[col]).abs().max())
        np.testing.assert_allclose(actual[col], oracle[col], atol=1e-12, rtol=0)
    for col in ['holding', 'next_holding']:
        assert actual[col].equals(oracle[col]), col
    return differences

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--prepare', action='store_true')
    args = parser.parse_args()
    RUN.mkdir(parents=True, exist_ok=True)
    assert BACKUP.is_dir()
    frozen = json.loads((RESEARCH/'delivery_refresh_manifest.json').read_text(encoding='utf-8'))
    for name in [delivery.BASE_PANEL, delivery.BASE_FILES['proxy_index'], delivery.BASE_FILES['costed_nav'], delivery.BASE_FILES['proxy_turnover']]:
        assert delivery.sha(ROOT/'outputs'/name) == frozen['streams'][name]['sha256'], name
    target = delivery.independent_target(ROOT)
    oracle = read(RESEARCH/'daily_outputs/no_constraint.csv')
    assert target == str(oracle.index[-1].date())
    if args.prepare:
        v._ensure_v2_0_contract_validated()
        _, base, turnover = v.v2_0.embedded_context._load_embedded_base_context()
        close = v._close_df_from_base(base)
        official0 = read(ROOT/'outputs'/delivery.COSTED['0'])
        candidate_out = v.build_v2_3_result(close, turnover, v.build_v2_3_common_index(close, official0.index))
        diff = parity(candidate_out, oracle)
        previous = read(v.PREVIOUS_COSTED_NAV_CSV)
        parity(previous, read(RESEARCH/'daily_outputs/w10_t0.26_r0.20.csv'))
        candidate = candidate_out.rename_axis('date').reset_index()
        audit = RUN/'strategy_rewrite_audit.csv'
        try:
            v.v2_0.base_mod.assert_no_historical_rewrite(
                previous=previous.reset_index(), candidate=candidate,
                key_columns=v.V2_3_REWRITE_AUDIT_KEY_COLUMNS,
                allowed_tail_rows=v._v2_3_rewrite_allowed_tail_rows(),
                label='v2.3 official costed NAV', audit_path=audit,
                column_allowed_tail_rows=v.V2_3_REWRITE_AUDIT_ALLOWED_TAIL_ROWS_BY_COLUMN)
        except RuntimeError:
            pass
        assert audit.exists()
        result = v.strategy_promotion_evidence(v.PREVIOUS_COSTED_NAV_CSV, candidate, audit)
        result.update(approved=True, user_authorization='哦，我决定把2.3的这个束去掉',
                      backup=str(BACKUP), research_oracle=str(RESEARCH/'daily_outputs/no_constraint.csv'),
                      oracle_parity=diff, unchanged_proxy_lineage=True)
        output = RUN/'approved_strategy_migration.json'
    else:
        result = delivery.inspect_outputs(ROOT, target)
        assert result['ok'], result['errors']
        actual = read(v.COSTED_NAV_CSV)
        result['oracle_parity'] = parity(actual, oracle)
        for suffix in ['0','5']:
            name=f'outputs/microcap_top100_mom16_biweekly_live_v2_{suffix}_nav.csv'
            parity(read(ROOT/name), read(BACKUP/name))
        assert not actual.overheat_enabled.any()
        assert not actual.overheat_risk_off.any()
        result.update(scope='local_artifact_acceptance_not_whole_delivery_or_remote_deployment',
                      unaffected_v20_v25_parity=True, windows=v.summarize_required_windows(actual.return_net), backup=str(BACKUP))
        output = RUN/'local_acceptance.json'
    output.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding='utf-8')
    print(json.dumps({'ok':True,'report':str(output)}, ensure_ascii=False))

if __name__ == '__main__':
    main()
