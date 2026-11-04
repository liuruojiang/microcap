from pathlib import Path
for name in ('microcap_top100_mom16_biweekly_live_v2_0.py','fetch_wind_microcap_index.py'):
 p=Path(name);t=p.read_text(encoding='utf-8');a=t.index('def select_return_close(');b=t.index('def build_symbol_panel(',a);s=t[a:b]
 old='    return_col = next((col for col in ("close_qfq", "close_adj", "close_raw") if col in return_price.columns), None)'
 new='''    from scripts.stock_return_basis import RETURN_BASIS_REVISION
    if return_price.attrs.get("return_basis_manifest", {}).get("return_basis_revision") != RETURN_BASIS_REVISION:
        raise ValueError(f"Unverified return-basis identity for {symbol}")
    return_col = "close_adj" if "close_adj" in return_price.columns else None'''
 assert old in s;s=s.replace(old,new);t=t[:a]+s+t[b:];p.write_text(t,encoding='utf-8',newline='\n')
p=Path('microcap_top100_mom16_biweekly_live_v2_0.py');t=p.read_text(encoding='utf-8')
t=t.replace('            "security_master_enabled": True,','            "security_master_enabled": True,\n            "stock_return_basis_revision": "sina_ratio_ex_reference_v1",',1)
t=t.replace('        core_params.get("execution_timing") == EXECUTION_TIMING','        core_params.get("stock_return_basis_revision") == "sina_ratio_ex_reference_v1"\n        and core_params.get("execution_timing") == EXECUTION_TIMING',1)
p.write_text(t,encoding='utf-8',newline='\n')
print('Strict consumer identity and proxy revision enforced.')
