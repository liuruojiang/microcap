"""Synthetic name evidence counterexamples; no prices or strategy results."""
import csv,io,json
from datetime import datetime,timezone
import pytest
from scripts import scoped_native_consumer as m
from scripts import scoped_source_consumer as s


@pytest.fixture
def publication(tmp_path):
    symbols=[f'{n:06}' for n in range(1,301)]
    def body(mapping):
        out=io.StringIO();writer=csv.DictWriter(out,fieldnames=['code','name']);writer.writeheader()
        writer.writerows(dict(code=k,name=v) for k,v in mapping.items());return out.getvalue().encode()
    old={code:'ordinary '+code for code in symbols};old.update({'300295':'*ST short','600999':'cold label'})
    prior=body(old);name='.microcap_index_cache/local_current_names/observations/'+s.sha(prior)+'.universe.csv'
    p=tmp_path/name;p.parent.mkdir(parents=True);p.write_bytes(prior)
    current=dict(old,**{'300295':'*ST complete'});universe=body(current)
    (tmp_path/'.microcap_index_cache/active_universe.csv').write_bytes(universe)
    st=b'code,name\n300295,*ST complete\n';(tmp_path/'.microcap_index_cache/current_st.csv').write_bytes(st)
    rows=[dict(code=code,name=current[code],fetched_at=datetime.now(timezone.utc).isoformat(),quote_source='tencent_batch_free')
          for code in symbols+['300295']]
    stamp=datetime.now(timezone.utc).isoformat()
    names=dict(schema='current_publication_names_v2',publication_name_scope='maintained_plus_ST_conflicts',
        target='2000-01-04',observed_at=stamp,st_observed_at=stamp,universe_observed_at=stamp,
        universe_sha256=s.sha(universe),universe_count=len(current),cache_sha256=s.sha(st),count=1,
        scoped_observation=dict(maintained_symbols=symbols,conflict_symbols=['300295'],requested_symbols=symbols+['300295'],
            quote_rows=rows,prior_universe=dict(path=name,sha256=s.sha(prior))))
    return tmp_path,names,symbols


def check(publication):
    root,names,symbols=publication
    return m.checked_publication_universe(root,names,datetime(2000,1,4,tzinfo=timezone.utc),symbols)


def test_names_transport_keeps_prior_and_exact_native_quote_scope(publication):
    assert len(check(publication))==2


@pytest.mark.parametrize('fault',['scope','full_scope','missing_scope','fake_conflict','coverage','quote_source','clock','cold','prior','role'])
def test_names_cannot_bypass_st_identity_or_witnessed_scope(publication,fault):
    root,names,symbols=publication;w=names['scoped_observation']
    if fault=='scope':names['publication_name_scope']='unknown'
    elif fault=='full_scope':names['publication_name_scope']='full_native_name_snapshot'
    elif fault=='missing_scope':names.pop('publication_name_scope')
    elif fault=='fake_conflict':
        w['conflict_symbols'].append('600999');w['requested_symbols'].append('600999')
        w['quote_rows'].append(dict(code='600999',name='cold label',fetched_at=names['observed_at'],quote_source='tencent_batch_free'))
    elif fault=='coverage':w['quote_rows'].pop()
    elif fault=='quote_source':w['quote_rows'][0]['quote_source']='unknown'
    elif fault=='clock':w['quote_rows'][0]['fetched_at']='1999-01-01T00:00:00+00:00'
    elif fault=='cold':
        p=root/'.microcap_index_cache/active_universe.csv';raw=p.read_bytes().replace(b'cold label',b'changed cold');p.write_bytes(raw);names['universe_sha256']=s.sha(raw)
    elif fault=='prior':(root/w['prior_universe']['path']).write_bytes(b'changed')
    else:w['prior_universe']['path']='outside-universe.csv'
    with pytest.raises((ValueError,AssertionError)):check(publication)
