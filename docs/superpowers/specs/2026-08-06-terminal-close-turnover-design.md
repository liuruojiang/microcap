# Terminal Close Rebalance Turnover Design

## Context

The Top100 refresh can end on a close-confirmed biweekly rebalance date. The proxy member table already contains that date, but `simulate_rebalance_path()` currently records a close-execution rebalance only when the following trading-day loop iteration begins. When no following date is present yet, the terminal rebalance is omitted from the turnover table, the same-day transaction cost is omitted from the costed NAV, and the freshness guard blocks publication.

## Approved behavior

When the final available trading date is a close-execution rebalance date, the simulator must execute the existing trade-constraint logic at that close and append exactly one turnover record for the final date. The record uses the final date as the constraint, execution, and effective date. Because the next return date is not yet present, `return_start_date` is left missing. The existing cost mapper then charges the resulting turnover cost on the close-execution date.

When a following trading day is available, the existing path remains authoritative: the prior close rebalance is recorded during the following-day iteration and must not be duplicated.

## Scope

- Preserve all v2.0, v2.3, and v2.5 signal, exposure, and cost parameters.
- Reuse `apply_trade_constraints()` and the existing turnover schema.
- Do not synthesize a future price or return row.
- Do not weaken the freshness guard.
- Keep the change limited to terminal close-execution turnover registration and focused regression tests.

## Verification

1. A failing regression test proves that a terminal close rebalance is absent before the fix.
2. The fixed simulator emits one terminal turnover row with the expected entries, exits, execution date, missing future return date, and cost rate.
3. A following-day regression test proves the same rebalance is not duplicated.
4. The relevant test module passes.
5. A formal refresh through 2026-08-06 passes the existing freshness proof, after which official v2.0, v2.3, and v2.5 costed streams are generated and read back.
