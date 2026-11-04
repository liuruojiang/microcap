from pathlib import Path
import shutil
root=Path.cwd(); backup=root/'.codex_backups/ratio_pipeline_20261030';backup.mkdir(parents=True,exist_ok=True)
paths=['microcap_top100_mom16_biweekly_live_v2_0.py','fetch_wind_microcap_index.py','analyze_top100_rebalance_frequency.py']
texts={p:(root/p).read_text(encoding='utf-8') for p in paths}
for p in paths:
    if not (backup/p).exists():shutil.copy2(root/p,backup/p)
fetch='''def fetch_adjusted_price_history(symbol: str, start_date: str, end_date: str, force_refresh: bool = False) -> pd.DataFrame:
    from scripts.stock_return_pipeline import load_return_prices
    raw_path = PRICE_CACHE_DIR / f"{symbol}.csv"
    if not raw_path.exists() and SHARED_PRICE_CACHE_DIR is not None:
        raw_path = SHARED_PRICE_CACHE_DIR / f"{symbol}.csv"
    master_path = CACHE_DIR / "security_master.csv"
    if not master_path.exists() and SHARED_CACHE_DIR is not None:
        master_path = SHARED_CACHE_DIR / "security_master.csv"
    master = pd.read_csv(master_path, dtype={"symbol": str})
    frame = load_return_prices(root=ROOT, symbol=symbol, raw_path=raw_path,
                               master=master, as_of=end_date, allow_network=True)
    return frame.loc[frame.date.between(pd.Timestamp(start_date), pd.Timestamp(end_date))].copy()


'''
for p in paths[:2]:
    t=texts[p];a=t.index('def fetch_adjusted_price_history(');b=t.index('def fetch_share_change(',a)
    texts[p]=t[:a]+fetch+t[b:]
# Reuse the formal panel implementation in the standalone entry point.
t=texts[paths[0]]; a=t.index('def select_return_close(');b=t.index('def build_rebalance_dates(',a)
block=t[a:b]
block=block.replace('if raw_close.empty or return_price is None or return_price.empty:\n        return raw_close.copy()', 'if raw_close.empty:\n        return raw_close.copy()\n    if return_price is None or return_price.empty:\n        raise ValueError(f"Missing verified adjusted return prices for {symbol}")')
block=block.replace('if adjusted.empty:\n        return raw_close.copy()', 'if adjusted.empty:\n        raise ValueError(f"No adjusted return prices in range for {symbol}")')
a0=block.index('    # Equal observations introduce no adjustment.');b0=block.index('    missing_dates =',a0)
block=block[:a0]+block[b0:]
a0=block.index('    try:\n        adjusted_price = fetch_adjusted_price_history(');b0=block.index('    shares = fetch_share_change(',a0)
block=block[:a0]+'''    adjusted_price = fetch_adjusted_price_history(
        symbol=symbol, start_date=start_date, end_date=end_date, force_refresh=force_refresh,
    )
'''+block[b0:]
texts[paths[0]]=t[:a]+block+t[b:]
t=texts[paths[1]];a=t.index('def build_symbol_panel(');b=t.index('def build_rebalance_dates(',a);texts[paths[1]]=t[:a]+block+t[b:]
for p in (paths[0],paths[2]):
    t=texts[p];f=t.index('def load_symbol_cache(');a=t.index('        adjusted_price: pd.DataFrame | None = None',f);b=t.index('        shares = pd.read_csv(share_path)',a)
    t=t[:a]+'''        from scripts.stock_return_pipeline import load_return_prices
        adjusted_price = load_return_prices(
            root=ROOT, symbol=symbol, raw_path=price_path, master=load_security_master(),
            as_of=end_ts.strftime("%Y-%m-%d"), allow_network=False,
        )

'''+t[b:]
    if p==paths[2]:
        a=t.index('    except Exception:\n        return None',f)
        t=t[:a]+t[a:].replace('    except Exception:\n        return None','    except Exception as exc:\n        raise RuntimeError(f"symbol cache load failed for {symbol}: {exc}") from exc',1)
    texts[p]=t
for p,t in texts.items():(root/p).write_text(t,encoding='utf-8',newline='\n')
print('Updated three entry points; preserved pre-edit backup.')
