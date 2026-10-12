# 2026-10-12 historical session/ST/share lineage repair

The user explicitly approved the frozen review SHA `d112e541b7a2000289cf89aa61b3ecda32b4e53e1865970413fca4ed3a3d9fe6` and completing production/cloud acceptance. See `microcap_lineage_repair_20261012_review.md` for the exact history, economic impact, raw hashes and migration contract.

This release changes only the frozen continuation authority version and its exact six-leg 2026-10-12 seed. v2.0/v2.3/v2.5 strategy revisions, parameters, costs and signal clocks stay unchanged. The execution fixes are inherited from PR #69/#70.

The reviewed 2026-10-09 history was independently extended with the default strict incremental path to 2026-10-12. All three final streams passed preflight, validate and whole delivery check, with 100 unique current members, 100 same-day name quotes and zero ST intersections. All corrected historical economic prefix fields stayed unchanged. Runtime seed hashes use the existing CRLF-to-LF transport convention; migration reports and portable archive evidence bind raw bytes separately.

The code release alone does not certify local adoption, cloud state, email send or inbox delivery. These are recorded separately after actual acceptance. Intraday results for 2026-10-12 are not backfilled. Automatic normal email resends are not authorized.

## Final primary seed binding

The primary default refresh passed and used the officially validated `sina_legacy` fallback after an Eastmoney connection failure. Its 2026-10-12 CSI1000 close was 7021.472, compared with 7021.470 in the earlier isolated Eastmoney probe. Only the new 10/12 row differs: v2.0/v2.3 daily net return differs by 2.23591377746e-7; holdings/scales and all corrected history through 10/09 are unchanged, and v2.5 is unchanged. The final authority binds the actual primary six-leg seed, including its producer metadata. It does not replace the failed Eastmoney attempt or mix source prices.

The effective-member seed was independently reconstructed with the official saved-target/tradeability model through 2026-10-12. It has 100 unique members, exactly the same ordered members as the prior 10/09 record, and the same set as the successful 10/12 current-name/ST preflight. The new as-of record is reconstructed evidence, not a relabelled old proof. No NAV or price file was replaced.
