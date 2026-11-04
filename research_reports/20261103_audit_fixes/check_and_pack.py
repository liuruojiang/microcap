"""Run the unchanged independent check and pack, sharing only the existing hash-bound process memo."""
from pathlib import Path
import contextlib
from datetime import datetime, timezone
import hashlib
import json
import sys

root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(root))
from scripts import top100_delivery as delivery, top100_cloud_delivery as cloud

evidence = Path(__file__).resolve().parent
receipt_path = evidence / "fix_receipt.json"

def record(**values):
    receipt = json.loads(receipt_path.read_bytes())
    receipt.update(values)
    receipt_path.write_text(json.dumps(receipt, indent=2), encoding="utf-8")

def now():
    return datetime.now(timezone.utc).isoformat()

assert (evidence / "local-refresh-all.exit").read_text().strip() == "0"
record(phase="independent_check", local_refresh_all="passed", local_check_started_at=now())
with (evidence / "local-check.log").open("w", encoding="utf-8") as log, contextlib.redirect_stdout(log), contextlib.redirect_stderr(log):
    code = delivery.main(["check"])
if code:
    record(local_check="failed", phase="blocked_independent_check")
    raise SystemExit(code)
record(local_check="passed", local_check_completed_at=now(), phase="pack", pack_started_at=now())
manifest = json.loads((root / delivery.MANIFEST).read_bytes())
target = manifest["expected_date"]
bundle = evidence / f"microcap-audit-fixed-whole-delivery-{target.replace('-', '')}.zip"
with (evidence / "pack.log").open("w", encoding="utf-8") as log, contextlib.redirect_stdout(log), contextlib.redirect_stderr(log):
    packed = cloud.pack({v: root for v in delivery.COSTED}, bundle, target)
assert packed["ok"]
packed.update(sha256=hashlib.sha256(bundle.read_bytes()).hexdigest(), bytes=bundle.stat().st_size, completed_at=now())
(evidence / "pack_receipt.json").write_text(json.dumps(packed, indent=2), encoding="utf-8")
original = json.loads(receipt_path.read_bytes())
record(phase="package_ready", target_session=target, pack_completed_at=now(), pack=packed,
       refresh_started_strategy_sha=original["strategy_sha"], strategy_sha=packed["release_sha"])
print(json.dumps(packed), flush=True)
