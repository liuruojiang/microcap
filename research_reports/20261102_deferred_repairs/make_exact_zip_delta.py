"""Transport only: reconstruct the already-validated ZIP byte for byte."""
import argparse
import hashlib
import json
import struct
from pathlib import Path
import zipfile

p = argparse.ArgumentParser()
p.add_argument('base', type=Path)
p.add_argument('target', type=Path)
p.add_argument('delta', type=Path)
a = p.parse_args()
old, new = a.base.read_bytes(), a.target.read_bytes()
def blocks(path, raw):
    with zipfile.ZipFile(path) as archive:
        entries = sorted(archive.infolist(), key=lambda x: x.header_offset)
        ends = [x.header_offset for x in entries[1:]] + [archive.start_dir]
        result = {}
        for x, end in zip(entries, ends):
            name_len, extra_len = struct.unpack_from('<HH', raw, x.header_offset + 26)
            data_start = x.header_offset + 30 + name_len + extra_len
            result[x.filename] = (x.header_offset, data_start, data_start + x.compress_size, end)
        return result, archive.start_dir
before, _ = blocks(a.base, old)
after, tail = blocks(a.target, new)
# Content-addressed raw snapshot paths change when a day is appended. Reuse
# bytes from the same symbol/leaf in the approved base, never semantic values.
def transport_identity(name):
    parts = name.split('/')
    if 'return_basis' in parts and 'bundles' in parts:
        i = parts.index('bundles')
        if len(parts) == i + 4:
            parts[i + 2] = '<content-address>'
    return '/'.join(parts)
base_by_identity = {transport_identity(name): bounds for name, bounds in before.items()}
ops, literals = [], bytearray()
def literal(raw):
    if raw:
        ops.append(['literal', len(literals), len(raw)])
        literals.extend(raw)
def copy(start, size):
    if size:
        ops.append(['copy', start, size])
def segment(start, end, lo, hi):
    current = new[start:end]
    previous = old[lo:hi]
    if previous == current:
        copy(lo, len(current))
        return
    cap = min(len(previous), len(current))
    prefix = 0
    while prefix < cap and previous[prefix] == current[prefix]:
        prefix += 1
    suffix = 0
    while suffix < cap - prefix and previous[-1-suffix] == current[-1-suffix]:
        suffix += 1
    copy(lo, prefix)
    literal(current[prefix:len(current)-suffix])
    copy(hi-suffix, suffix)
for name, bounds in after.items():
    old_bounds = before.get(name, base_by_identity.get(transport_identity(name)))
    if old_bounds is None:
        literal(new[bounds[0]:bounds[-1]])
        continue
    for i in range(3):
        segment(bounds[i], bounds[i+1], old_bounds[i], old_bounds[i+1])
literal(new[tail:])
rebuilt = b''.join((old if kind == 'copy' else literals)[start:start+size] for kind, start, size in ops)
assert rebuilt == new
sha = lambda raw: hashlib.sha256(raw).hexdigest()
recipe = dict(base_sha256=sha(old), target_sha256=sha(new), target_size=len(new), ops=ops)
with zipfile.ZipFile(a.delta, 'w', compression=zipfile.ZIP_DEFLATED, compresslevel=6) as archive:
    archive.writestr('recipe.json', json.dumps(recipe, separators=(',', ':')))
    archive.writestr('literals.bin', literals)
print(json.dumps(dict(base_sha256=sha(old), target_sha256=sha(new), target_size=len(new),
                     delta_sha256=sha(a.delta.read_bytes()), delta_size=a.delta.stat().st_size,
                     exact_reconstruction=True), indent=2))
