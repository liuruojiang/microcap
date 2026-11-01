def simulate_rebalance_path(
    trading_dates: pd.DatetimeIndex,
    returns_df: pd.DataFrame,
    target_members_map: dict[pd.Timestamp, list[str]],
    rebalance_dates: pd.DatetimeIndex,
    buyable_df: pd.DataFrame,
    sellable_df: pd.DataFrame,
    one_side_cost_rate: float,
    top_n: int = TOP_N,
    execution_timing: str = EXECUTION_TIMING_NEXT_OPEN,
    initial_members: list[str] | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, dict[pd.Timestamp, list[str]]]:
    if execution_timing not in {EXECUTION_TIMING_NEXT_OPEN, EXECUTION_TIMING_CLOSE}:
        raise ValueError(f"unsupported execution_timing: {execution_timing}")

    rebalance_set = set(rebalance_dates)
    if initial_members is not None:
        if len(initial_members) != top_n or len(set(initial_members)) != top_n:
            raise ValueError("Continuation requires a complete unique executed member seed")
        if not set(initial_members).issubset(returns_df.columns):
            raise ValueError("Continuation returns are missing executed members")
        if len(trading_dates):
            # The seed is the settled state after this close, not an instruction
            # to execute its rebalance a second time.
            rebalance_set.discard(pd.Timestamp(trading_dates[0]))
    turnover_rows: list[dict[str, object]] = []
    effective_members_map: dict[pd.Timestamp, list[str]] = {}

    index_rows: list[dict[str, object]] = []
    current_members: list[str] = list(initial_members or [])
    current_level = 1000.0
    for i, dt in enumerate(trading_dates):
        if i == 0:
            index_rows.append({"date": dt, "close": current_level, "daily_return": np.nan, "holding_count": len(current_members)})
            continue

        prev_dt = pd.Timestamp(trading_dates[i - 1])
        if prev_dt in rebalance_set:
            constraint_trade_date = prev_dt if execution_timing == EXECUTION_TIMING_CLOSE else pd.Timestamp(dt)
            execution_date = prev_dt if execution_timing == EXECUTION_TIMING_CLOSE else pd.Timestamp(dt)
            target_members = target_members_map.get(prev_dt, [])
            trade_result = apply_trade_constraints(
                current_members=current_members,
                target_members=target_members,
                trade_date=constraint_trade_date,
                buyable_df=buyable_df,
                sellable_df=sellable_df,
                top_n=top_n,
            )
            current_members = trade_result["members_after"]
            effective_members_map[prev_dt] = current_members.copy()
            buys = len(trade_result["entered"])
            sells = len(trade_result["exited"])
            turnover_rows.append(
                {
                    "rebalance_date": prev_dt,
                    "execution_timing": execution_timing,
                    "constraint_trade_date": constraint_trade_date,
                    "execution_date": execution_date,
                    "effective_date": execution_date,
                    "return_start_date": pd.Timestamp(dt),
                    "exit_count": sells,
                    "entry_count": buys,
                    "blocked_entry_count": len(trade_result["blocked_entries"]),
                    "blocked_exit_count": len(trade_result["blocked_exits"]),
                    "buy_turnover_frac": buys / top_n,
                    "sell_turnover_frac": sells / top_n,
                    "turnover_frac_one_side": (buys + sells) / (2 * top_n),
                    "two_side_cost_rate": one_side_cost_rate * ((buys + sells) / top_n),
                    "holding_count_after": len(current_members),
                }
            )

        if current_members:
            day_ret = returns_df.loc[dt, current_members].reindex(current_members)
            invalid = ~np.isfinite(day_ret.to_numpy(dtype=float))
            if invalid.any():
                symbols = ",".join(day_ret.index[invalid].astype(str))
                raise RuntimeError(
                    f"Unvalued held securities on {pd.Timestamp(dt).date()}: {symbols}; "
                    "missing or terminal returns require explicit source-backed treatment"
                )
            portfolio_ret = float(day_ret.mean())
        else:
            portfolio_ret = 0.0
        current_level *= 1.0 + portfolio_ret
        index_rows.append(
            {
                "date": dt,
                "close": current_level,
                "daily_return": portfolio_ret,
                "holding_count": len(current_members),
            }
        )

    if len(trading_dates) and execution_timing == EXECUTION_TIMING_CLOSE:
        final_dt = pd.Timestamp(trading_dates[-1])
        if final_dt in rebalance_set and final_dt not in effective_members_map:
            target_members = target_members_map.get(final_dt, [])
            trade_result = apply_trade_constraints(
                current_members=current_members,
                target_members=target_members,
                trade_date=final_dt,
                buyable_df=buyable_df,
                sellable_df=sellable_df,
                top_n=top_n,
            )
            current_members = trade_result["members_after"]
            effective_members_map[final_dt] = current_members.copy()
            buys = len(trade_result["entered"])
            sells = len(trade_result["exited"])
            turnover_rows.append(
                {
                    "rebalance_date": final_dt,
                    "execution_timing": execution_timing,
                    "constraint_trade_date": final_dt,
                    "execution_date": final_dt,
                    "effective_date": final_dt,
                    "return_start_date": pd.NaT,
                    "exit_count": sells,
                    "entry_count": buys,
                    "blocked_entry_count": len(trade_result["blocked_entries"]),
                    "blocked_exit_count": len(trade_result["blocked_exits"]),
                    "buy_turnover_frac": buys / top_n,
                    "sell_turnover_frac": sells / top_n,
                    "turnover_frac_one_side": (buys + sells) / (2 * top_n),
                    "two_side_cost_rate": one_side_cost_rate * ((buys + sells) / top_n),
                    "holding_count_after": len(current_members),
                }
            )

    return pd.DataFrame(index_rows), pd.DataFrame(turnover_rows), effective_members_map
