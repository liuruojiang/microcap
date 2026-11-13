"""Actual Git producer source hashes; no signed data is altered for compatibility."""
from functools import cache
import hashlib
from pathlib import Path
import subprocess

import pytest

from scripts import scoped_source_consumer as consumer
from scripts.stock_input_contract import SOURCE_FILES, source_fingerprint

ROOT=Path(__file__).resolve().parents[1]
P2='3fe2e15ee9e3f2ee7e25bbc328479f1fd9910d36'
P1='f6970d665d5c7a39b37c866cf13dc3d4cb8fdcc5'
P3='d2d15cbaf5763333c74239b1ded3d1ac467adb3c'
P3_RECOVERY='3344e403ff01e17ef45b745a704a240e0917720b'


@cache
def actual_git_sources(ref):
    return {name:hashlib.sha256(subprocess.check_output(['git','-C',str(ROOT),'show',ref+':'+name])
        .replace(b'\r\n',b'\n')).hexdigest() for name in SOURCE_FILES}


def package(ref,values):
    return dict(runtime_sha=ref,producer_context=dict(runtime_sha=ref,native_sources=values))


@pytest.mark.parametrize('ref',[P1,P2,P3,P3_RECOVERY,'ae439a1eec4dae5729483f43356473bcb9b1dff1'])
def test_old_actual_producer_source_hashes_remain_readable_without_resigning(ref):
    before=actual_git_sources(ref)
    assert consumer.validate_source_contract(ROOT,package(ref,before))==source_fingerprint(ROOT)
    assert before==actual_git_sources(ref)


@pytest.mark.parametrize('fault',['extra_file','partial_pair','unknown_producer'])
def test_new_quote_compatibility_is_exact_and_not_a_general_source_waiver(fault):
    values=dict(actual_git_sources(P2));ref=P2
    if fault=='extra_file':values['scripts/stock_daily_inputs.py']='0'*64
    elif fault=='partial_pair':values['scripts/share_change_source.py']=source_fingerprint(ROOT)['scripts/share_change_source.py']
    else:ref='a'*40;values=source_fingerprint(ROOT)
    with pytest.raises(ValueError):consumer.validate_source_contract(ROOT,package(ref,values))
