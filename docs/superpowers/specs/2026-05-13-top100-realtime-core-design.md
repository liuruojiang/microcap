# Top100 Realtime Core Design

## Scope

This change only untangles the production realtime signal path for Top100 v1.4, v1.6, and v1.8. Historical generation, performance reports, backtests, parameter scans, and existing overlay math are out of scope unless a small compatibility shim is required for realtime.

## Problem

The current realtime path has version-to-version dependencies:

```text
v1.6 -> top100_v14_base_context -> v1.4 -> v1.1/base_mod
v1.8 -> top100_v14_base_context -> v1.4 -> v1.1/base_mod
```

For v1.6, realtime generation calls `build_realtime_v1_4_outputs()` before applying v1.6 logic. This means one cache, metadata, member snapshot, or CSV-writing edge case in v1.4 can break later versions and obscure the real failure.

## Design

Create a shared realtime adapter that builds the base realtime context exactly once and exposes stable functions for the version scripts:

- `load_realtime_base()`: returns the v1.1/base realtime context, turnover frame, reference summary, and realtime metadata.
- `build_realtime_overlay_base()`: applies the shared v1.4 overlay to that base context without writing the v1.4 realtime CSV.
- `csv_safe_meta_value()` and `apply_realtime_meta_to_signal_row()`: serialize metadata consistently before CSV writes.

Then update realtime builders:

- v1.4 consumes the shared adapter and writes only v1.4 output.
- v1.6 consumes the shared adapter and applies v1.6 target-vol logic directly, without calling `build_realtime_v1_4_outputs()`.
- v1.8 consumes the shared adapter and applies v1.8 logic directly.

The existing `top100_v14_base_context` module can remain as a compatibility facade for historical code, but realtime code should not call another version's realtime builder.

## Tests

Add focused tests that monkeypatch the real modules and verify:

- v1.6 realtime does not call `v14_context.build_realtime_v1_4_outputs()`.
- v1.8 realtime uses the shared realtime base adapter instead of a version builder.
- metadata values such as empty lists are safe to write into one-row CSV outputs.

The tests intentionally avoid live network calls and do not assert trading performance. GitHub Actions remains the production integration check for real realtime quote behavior.

## Success Criteria

- The dedicated tests fail before the refactor and pass after it.
- `python -m py_compile` passes for the touched realtime modules.
- `python -m pytest` passes for the new tests.
- GitHub `Top100 Realtime Signals` passes on the refactor branch and on `main` after merge.
