"""Independent unadjusted price and cap audit for both case reports."""

import importlib.util
from pathlib import Path


HERE = Path(__file__).resolve().parent
SOURCE = HERE.parent / "initial_early_pair" / "check_prices.py"
spec = importlib.util.spec_from_file_location("pair_price_audit", SOURCE)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
module.HERE = HERE
module.SHARES_FROM_ISSUER_PDFS = {"600634": 87_207_283, "600687": 126_888_563}
module.main()
