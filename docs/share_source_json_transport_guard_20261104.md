# Native share-source JSON transport guard

This repair adds delivery validation after the existing native
`verify_share_cache` parser, merge, issuer/date, hash, and complete-chain checks.
It does not edit those functions or any of the 18 stock economic source files.

The native parser accepts `False` as an empty query count because Python treats
`False == 0`; it also converts `F003N=True` to a positive floating quantity.
Three isolated source-chain regressions first reproduced both the original
native verifier and bundle inventory accepting those values.

`realtime_state_bundle._verify_share_source_json_protocol` consumes the native
verifier's actual `source_files` closure. It follows the declared current and
previous states and checks only native schema1 CNInfo response bodies:

- `resultcode`, `total`, and `count` must be JSON integers, excluding booleans;
  resultcode remains 200 and both counts must equal the records length.
- `records` remains an array of objects; a total-share `F003N` value cannot be
  a JSON boolean. Existing valid numerical strings and provider extension keys
  retain their existing native interpretation.
- Every inspected body/state must match the original native verified closure
  and declared hash. Readback detects a change during the additional check.

The new guard runs immediately after the original native verification in both
bundle enumeration and `validate_state`. Existing whole-delivery checks,
state restoration, packing, and preflight therefore retain their native
validation and fail closed on this additional data-protocol error. Schema2
EM/Sina evidence continues through its unchanged native verifier; its earlier
schema1 source states are not exempted.

No global monkeypatch, replacement manifest, new raw input, altered historical
CSV, model parameter, provider routing, or economic source fingerprint is used.

Compatibility was checked read-only against the already certified isolated
`D:\CodexDaily\sharefb3` candidate, not a concurrently refreshing production
root: 102 native source chains (97 schema2 and five schema1) and 209 declared
CNInfo frames passed the unchanged native verifier plus this guard. All 2,336
candidate file hashes remained identical before and after. This is source-only
compatibility qualification, not whole-state or current-session readiness.
Evidence is retained in automation task artifact
`audit_delivery/share_json_guard_102_qualification.json`.

The original source/transport, core state-guard, preflight, cloud, and delivery
test files are retained. The red logs and original code remain auditable;
production activation and matching final whole-state/cloud acceptance are
separate steps after the current production writer exits.
