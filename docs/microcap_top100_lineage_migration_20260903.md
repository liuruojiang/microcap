# Top100 2026-09-03 lineage migration report

## Decision

Approved as the exact post-rebalance production seed for 2026-09-03. This is a
lineage migration caused by the security-metadata content fingerprint changing;
it is not a parameter change and does not authorize approximate history or a
narrow-cache rebuild.

## Scope and freshness

- Panel: 8,720 rows, latest date `2026-09-03`.
- Proxy index: 4,049 rows, latest date `2026-09-03`.
- Proxy turnover: 431 rebalance rows, latest rebalance `2026-09-03`.
- Base costed NAV: 4,032 rows, latest date `2026-09-03`.
- Proxy members: 43,100 rows; latest rebalance contains exactly 100 unique symbols.
- Current-member ST/code intersection: 0.
- Historical point-in-time ST interval violations across all 43,100 member rows: 0.
- Security metadata cache: 4,975 present, 0 missing.
- Metadata content fingerprint changed from
  `f5b3b2e47994f81c5904d565c34a2ad51ec807aaa53ea00f669e7d9fe05fd867`
  to `1f09de33716deb9742593765a1121f223ed0cee86b29c4e343ce9b63f6e4f94e`.

## Exact canonical hashes

Hashes below use the repository's canonical newline normalization (CRLF to LF
only). All other bytes remain authoritative.

| Artifact | Previous SHA-256 | Candidate SHA-256 |
|---|---|---|
| proxy index | `614627532d16b116c3fd63aad41303fd25827f8e1cc68c1ccbdef1bfbe5a1302` | `25311cba62cf0a74041a4b83dbf4f395565bb092880e15a767023107a2ee9ad3` |
| base costed NAV | `9a7bcd3ecb490bb2c808dc1e0809e0a8a1966212ac9600f7badea1ad9da159d3` | `66c5c63d08090bda5e25d6dda6846955e533fa4399d50805d53138e4638270ab` |
| proxy metadata | `866690cf8447910fb79f5fdfada05a95781aa430d8b047f4c61bdbecf712961b` | `5216305f5dc18b249822457fc72345e6d1a9c957840719f1171810d9f16d9a73` |
| proxy members | `0eb6f3a076f1f72d6e20fce7b365fd902d465dd8d6350113b8b7b71a8e034cef` | `ac6396a16cdd5b493b5a6bede3e7ead2820913e5f92983da166467144392e07e` |
| proxy turnover | `e6c556e82235c902e6d46baa92e1090ab1a166475125849ff7bb47e4484e9685` | `e3dfb5dc4e5b07eb19ecd6b98443723dde1873f5b10393a1ce587d544cc18ce9` |
| effective members | `ecec6660a8cb704b12056bca02f987db6af1ff4edcfdef9d74ab5d030386a4b5` | `9558082806163587d9813100bd19cafe92337a2c086e3fc8ac6aa8a71e5825ee` |

## Rewrite audit

The migration candidate was regenerated from the full historical security
master and complete price, share-change, and security-metadata caches. The
previous 43,000 member rows and candidate 43,100 member rows were compared by
rebalance date and rank; 11,989 historical rank-symbol cells changed, so this is
explicitly recorded as a lineage rewrite rather than a tail-only update.

After promotion, the formal refresh was run again without a migration option.
The tracked seed hashes remained equal to the candidate hashes above. The v2.0,
v2.3, and v2.5 summaries each reported `historical_rewrite_audit.status=clean`,
and the second refresh validated all anchors at `2026-09-03`.
