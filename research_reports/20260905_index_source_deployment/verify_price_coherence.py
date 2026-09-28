# /// script
# requires-python = ">=3.11"
# dependencies = ["numpy", "pandas"]
# ///
"""Verify every base/final hedge price agrees with its own canonical panel."""
import argparse
import io
import json
from pathlib import Path
import sys
import zipfile

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
import microcap_top100_mom16_biweekly_live_v2_0 as v20
from scripts import top100_delivery as delivery

parser = argparse.ArgumentParser()
parser.add_argument("--bundle", type=Path)
parser.add_argument("--out", type=Path, required=True)
args = parser.parse_args()
archive = zipfile.ZipFile(args.bundle) if args.bundle else None
def read(name):
    return pd.read_csv(io.BytesIO(archive.read("outputs/"+name))) if archive else pd.read_csv(ROOT / "outputs" / name)
panel = read(delivery.BASE_PANEL).set_index("date")[v20.base_mod.HEDGE_COLUMN]
result = {}
for name in [delivery.BASE_FILES["costed_nav"], *delivery.COSTED.values()]:
    frame = read(name)
    expected = frame.date.map(panel)
    assert np.isfinite(frame.hedge_close.to_numpy(dtype=float)).all()
    assert np.isfinite(expected.to_numpy(dtype=float)).all()
    np.testing.assert_allclose(frame.hedge_close, expected, rtol=0, atol=1e-9)
    result[name] = dict(rows=len(frame), last_date=frame.date.iloc[-1], max_price_difference=float((frame.hedge_close-expected).abs().max()))
report = dict(ok=True, streams=result, scope="internal price coherence, not cross-vendor display precision")
args.out.write_text(json.dumps(report, indent=2), encoding="utf-8")
print(json.dumps(report))
