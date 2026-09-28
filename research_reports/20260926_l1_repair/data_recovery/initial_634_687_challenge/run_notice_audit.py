"""Independent fetch and hash audit for 600634 and 600687 notice bodies."""

import importlib.util
from pathlib import Path


HERE = Path(__file__).resolve().parent
SOURCE = HERE.parent / "initial_early_pair" / "audit_notices.py"
spec = importlib.util.spec_from_file_location("pair_notice_audit", SOURCE)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
module.HERE = HERE
module.SYMBOLS = {"600634", "600687"}
module.main()
