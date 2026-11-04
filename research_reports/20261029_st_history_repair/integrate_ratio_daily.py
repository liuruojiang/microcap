from pathlib import Path
p=Path('microcap_top100_mom16_biweekly_live_v2_0.py');t=p.read_text(encoding='utf-8')
a=t.index('def refresh_price_cache_tail(')
helper='''def refresh_return_basis_sources(symbols: list[str], end_text: str) -> None:
    """Revalidate all selected legs, including stocks whose raw tail is current."""
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = {pool.submit(fetch_mod.fetch_adjusted_price_history, symbol,
                               freq_mod.START_DATE, end_text, False): symbol for symbol in symbols}
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as exc:
                for pending in futures:
                    pending.cancel()
                raise RuntimeError(f"Return-basis refresh failed for {futures[future]}: {exc}") from exc


'''
t=t[:a]+helper+t[a:]
t=t.replace('        _log_price_cache_refresh("price-cache refresh complete 0/0 failures=0")\n        return','        refresh_return_basis_sources(symbols, end_text)\n        _log_price_cache_refresh("price-cache refresh complete 0/0 failures=0")\n        return',1)
needle='    _log_price_cache_refresh(f"price-cache refresh complete {total_symbols}/{total_symbols} failures={len(failures)}")'
t=t.replace(needle,'    refresh_return_basis_sources(symbols, end_text)\n'+needle,1)
a=t.index('    def load_tail_return(symbol: str)');b=t.index('    return_sources:',a)
t=t[:a]+'''    def load_tail_return(symbol: str) -> tuple[str, pd.Series, str] | None:
        from scripts.stock_return_pipeline import load_return_prices
        raw_path = freq_mod.resolve_cache_path(freq_mod.PRICE_DIR, freq_mod.SHARED_PRICE_DIR, symbol)
        if raw_path is None:
            return None
        frame = load_return_prices(root=ROOT, symbol=symbol, raw_path=raw_path,
                                   master=freq_mod.load_security_master(), as_of=str(target_end.date()),
                                   allow_network=False)
        close = frame.set_index("date")["close_adj"].reindex(tail_dates).ffill()
        if close.isna().any() or (close <= 0).any():
            return None
        return symbol, close.pct_change(fill_method=None).fillna(0.0), "adjusted"

'''+t[b:]
p.write_text(t,encoding='utf-8',newline='\n')
print('Daily and replay routes now share the strict basis loader.')
