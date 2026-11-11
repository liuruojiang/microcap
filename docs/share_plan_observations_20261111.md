# 11-Nov share catalog and unexecuted-plan observations

Candidate from `efbfc346`; this is a source classification repair, not a production activation or approval of historical share changes.

## Actual observations

- `301429`: the future 12-Nov EM row changed only `FREELIQCI_SHARES` from 35,884,736 to 35,884,739. Total remains 118,220,000, and the original PDF remains exact. Retain both exact full-row hashes; do not consume the future row or infer legal execution. All other row fields, the two-row inventory, independent dated totals, Sina and quote checks remain required.
- `300535`: PDF announcement `AN202611101831596291` (2026-062), signed 11-Nov, schedules 1,157,480 shares on 12-Nov, from 107,019,863 to 108,177,343. Register only `verified_future_plan_not_executed`. The pending cancellation discussed in the document is not applied.
- `300644`: PDF announcement `AN202611101831590688` (2026-063), signed 10-Nov, schedules 40,000 shares on 12-Nov, from 110,933,506 to 110,973,506. Register only `verified_future_plan_not_executed`.

The actual dated totals on both 10 and 11 Nov for the latter two remain their before totals. These registrations cannot certify a 12-Nov market observation or add a 12-Nov share-curve row on 11 Nov.

| Evidence | SHA256 |
|---|---|
| 300535 [original PDF](https://pdf.dfcfw.com/pdf/H2_AN202611101831596291_1.pdf), 164416 bytes | `57402fedb9a03fe919e43b4261c95d7a35c4fc2f8481d6a36a94cb9d6f4970ba` |
| 300644 [original PDF](https://pdf.dfcfw.com/pdf/H2_AN202611101831590688_1.pdf), 157324 bytes | `00c72869319cb8e8ef844fb7a367ab20c0a4de5259bcaf5eb20181057c0d8b57` |
| 301429 original 12-Nov EM row | `1eeabcec4ff5f278a00cd2e4b3e702d5858881c1edec4fc2eb11f761bf959843` |
| 301429 additional exact 12-Nov row | `e3650714e308713862a751c71a551e786a508cae45d3232bdd3aa51ca6c53c8e` |
| Captured actual raw/receipt/PDF/old10 closure fixture | `4b0abf60785bff19624199cad30b89897350617cdb150fba93b83642c7267420` |

## Validation and explicit limits

`tests/test_share_catalog_and_plans_20261111.py` replays preserved source objects, with no network:

- Old10 native `verify_share_cache` passes for all three symbols using the candidate reader and registry; original candidate CSV bytes remain exact.
- Actual complete 301429 input passes to 11-Nov, with zero share changes and unchanged CSV. A separate source-only private `refresh_share_change` also completed using already-captured raw objects with no network.
- Actual 300535/300644 PDF/EM future-plan matching passes. Both actual private refreshes remain incomplete because quote requests exhausted with HTTP502; successful current quotes are absent from the real fixture. Full actual source verification continues to fail for the missing quote.
- Explicitly labeled in-memory software quote cases test that the future plan is excluded and CSV stays unchanged. They are not market acceptance, producer output, or formal evidence.
- Wrong document bytes, quantities, dates, announcement identifiers and unreviewed 301429 catalog variants are rejected.

216 related tests passed in lifecycle record `7d70efac328a401fb035414d47206722`. The prior failed test record `39369f7257f6420abc9df439f4da6d19` reflects a new fixture-shape mistake (PDF references nested under files instead of at manifest top level); it was corrected without changing source code.

## Source identity and migration boundary

Relative to efbfc346, only two `stock_input_contract.SOURCE_FILES` entries change (LF-normalized SHA256):

| File | Old | New |
|---|---|---|
| `scripts/share_change_source.py` | `3dfb3ad0facbda0b8994af3767272fd35e22b16554993a60f6942d3e8bd74b7b` | `2d1e33c6dbfd78e1b14fa6400bd9386b28811f167de600a84e4e589edeea2f1e` |
| `data/share_source_future_plan_reviews.json` | `972825f752eb59d274c14e6a009f1b92cbed2e4a3f45ad5adce027f3b174e2e1` | `86c353677ec35023bc451c00ca1fd13966f117e3cc7352958d0d5b28bd1b7aa8` |

Old10 per-stock share proofs can natively revalidate. Whole-input source fingerprints and exact release/consumer contracts still reject the new source identity until separately reviewed and freshly computed. Do not substitute new hashes inside old proofs. The formal bundle requires a strict source-successor/code-only revalidation migration owned by the production cutover work; this candidate does not authorize that activation. Relative to the running be89 producer, the inherited M4 changes to `stock_return_pipeline.py` and `st_notice_refresh.py` must also be accounted for.

All old registry records and nested approvals remain unchanged. No running runtime, protected primary, strategy parameters, historical CSV or original raw receipt was edited.
