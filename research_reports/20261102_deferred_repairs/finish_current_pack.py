"""Package only after the original local delivery gates and historical parity pass."""
from pathlib import Path
import hashlib
import io
import json
import sys
import tempfile
import zipfile
import pandas as pd

root = Path.cwd()
sys.path.insert(0, str(root))
from scripts import top100_delivery as delivery, top100_cloud_delivery as cloud

evidence = root / 'research_reports/20261102_deferred_repairs'
receipt = json.loads((evidence / 'native_primary_delivery_receipt.json').read_bytes())
assert receipt['local_delivery_pass'] is True and receipt['expected_date'] == '2026-11-02'
manifest = json.loads((root / delivery.MANIFEST).read_bytes())
assert manifest['status'] == 'complete' and manifest['expected_date'] == '2026-11-02'
base = evidence / 'microcap-native-whole-delivery-20261030.zip'
assert hashlib.sha256(base.read_bytes()).hexdigest() == '2c673040c8d919476f8896e5596b7a693db6443304d4ad9e3f50925383c8a140'
results = []
with zipfile.ZipFile(base) as archive:
    for version, costed in delivery.COSTED.items():
        names = ['outputs/' + costed] + [f'outputs/microcap_top100_mom16_biweekly_live_v2_{version}_{suffix}' for suffix in ('nav.csv', 'performance_nav.csv')]
        for name in names:
            before = pd.read_csv(io.BytesIO(archive.read(name)))
            after = pd.read_csv(root / name)
            assert len(after) > len(before), name
            prefix = after.iloc[:len(before)].reset_index(drop=True)
            # The original refresh-all/check rewrite gates already passed. Permit
            # only the observed two-ULP diagnostic score difference, never a NAV,
            # return, cost, position, date, or other historical economic change.
            rounded = []
            exact_before, exact_after = before.copy(), prefix.copy()
            if version == '5':
                import numpy as np
                for col in ('microcap_mom', 'momentum_gap', 'annualized_log_wls_score'):
                    if col not in before:
                        continue
                    mismatch = ~((before[col] == prefix[col]) | (before[col].isna() & prefix[col].isna()))
                    for index in before.index[mismatch]:
                        assert str(before.loc[index, 'date']) == '2026-10-29'
                        old, new = float(before.loc[index, col]), float(prefix.loc[index, col])
                        assert abs(old-new) <= 2 * abs(np.spacing(old))
                        rounded.append(dict(date='2026-10-29', column=col, before=old, after=new))
                        exact_after.loc[index, col] = old
            pd.testing.assert_frame_equal(exact_before, exact_after, check_exact=True, check_dtype=False)
            results.append(dict(path=name, unchanged_economic_history_rows=len(before), new_rows=len(after)-len(before), diagnostic_score_rounding=rounded))
(evidence / 'current_historical_prefix_parity.json').write_text(json.dumps(results, indent=2), encoding='utf-8')
print('HISTORICAL_PREFIX_PARITY_PASS', len(results), flush=True)
temporary = evidence / 'current_pack_tmp'
temporary.mkdir(exist_ok=True)
tempfile.tempdir = str(temporary)
bundle = evidence / 'microcap-tencent-whole-delivery-20261102.zip'
assert not bundle.exists(), 'Inspect an existing bundle before attempting another publication'
packed = cloud.pack({version: root for version in delivery.COSTED}, bundle, '2026-11-02')
assert packed['ok'] is True
packed.update(sha256=hashlib.sha256(bundle.read_bytes()).hexdigest(), bytes=bundle.stat().st_size)
(evidence / 'current_whole_pack_receipt.json').write_text(json.dumps(packed, indent=2), encoding='utf-8')
print(json.dumps(packed, indent=2), flush=True)
