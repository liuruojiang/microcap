"""Validate candidate patch without applying it to the workspace."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
import subprocess
import sys

from build_patch import FILES, ROOT, OUT


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def run(argv: list[str]) -> dict:
    proc = subprocess.run(argv, cwd=ROOT, capture_output=True, text=True, encoding="utf-8", errors="replace")
    return {"argv": argv, "exit_code": proc.returncode, "stdout": proc.stdout, "stderr": proc.stderr}


def main() -> None:
    before = {path: sha(ROOT / path) for path in FILES}
    steps = [
        run([sys.executable, "-X", "utf8", str(OUT / "build_patch.py")]),
        run(["git", "apply", "--check", str(OUT / "candidate_code_fix.patch")]),
        run([sys.executable, "-X", "utf8", str(OUT / "test_candidate.py")]),
    ]
    after = {path: sha(ROOT / path) for path in FILES}
    report = {
        "formal_source_unchanged": before == after,
        "source_sha256": before,
        "candidate_patch_sha256": sha(OUT / "candidate_code_fix.patch"),
        "steps": steps,
        "pass": before == after and all(step["exit_code"] == 0 for step in steps),
    }
    (OUT / "validation.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"pass": report["pass"], "source_unchanged": report["formal_source_unchanged"], "step_exit_codes": [step["exit_code"] for step in steps], "patch_sha256": report["candidate_patch_sha256"]}, ensure_ascii=False))
    if not report["pass"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
