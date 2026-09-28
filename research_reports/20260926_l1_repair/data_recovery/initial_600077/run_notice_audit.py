"""Reuse the frozen PDF retrieval/indexing logic for the 600077 case."""

import importlib.util
from pathlib import Path


HERE = Path(__file__).resolve().parent
SOURCE = HERE.parent / "initial_early_pair" / "audit_notices.py"
spec = importlib.util.spec_from_file_location("pair_notice_audit", SOURCE)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
module.HERE = HERE
module.SYMBOLS = {"600077"}
module.main()
