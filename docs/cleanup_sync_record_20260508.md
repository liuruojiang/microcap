# 2026-05-08 cleanup and cloud sync record

## Scope

- Repository: `微盘股对冲策略`
- Branch: `main`
- Goal: include the microcap strategy repository in the test/cache cleanup and cloud sync pass.

## Candidate scan

Commands used before deletion:

- `git status -sb`
- `git fetch origin`
- `git rev-list --left-right --count origin/main...HEAD`
- `git ls-files | Select-String -Pattern '(^|/)(tests?/|test_.*\.py$|.*_test\.py$|.*test.*\.py$|__pycache__/|\.pyc$)'`
- `git status --ignored --short | Select-String -Pattern '(^|/)(tests?($|/)|test_|_test|__pycache__|\.pyc$|tmp|temp|scratch|测试|临时)'`
- PowerShell recursive scans excluding `.git`, `.codex_backups`, `docs`, `archive`, `归档`, `outputs`, `wheelhouse`, and `.github`.

Result:

- No tracked, untracked, or ignored first-party test/cache deletion candidates were found.
- Existing `analyze_`, `scan_`, `validate_`, `verify_`, and formal versioned strategy scripts were treated as research or production artifacts and preserved.

## Deleted

- None.

## Preserved

- Formal strategy scripts, including `microcap_top100_mom16_biweekly_live.py` and versioned `v1_1` through `v1_8` wrappers.
- `poe_bots/`, `scripts/`, `docs/`, `outputs/`, archives, backups, local cache directories, and runtime wheelhouse artifacts.
- Existing research helpers and durable analysis scripts.

## Backup note

- Because there were no delete targets, no file copy backup was required for this cleanup pass.

## Verification

- `python -m py_compile` passed on the formal live entrypoints, version wrappers, realtime runner, Poe bot entrypoints, and adapter verification script.
- Regenerated first-party `__pycache__/` directories were removed from the repo root, `poe_bots/`, and `scripts/`.
- `git diff --check` passed.

## Sync result

- Push target: `origin/main`
- Post-push target check: `origin/main...HEAD = 0 0`
