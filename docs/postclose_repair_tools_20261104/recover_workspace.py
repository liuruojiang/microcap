"""Recover only the certified old-state workspace stopped before source mutation."""
import argparse
import hashlib
import json
from pathlib import Path, PurePosixPath
import shutil
import subprocess
import tarfile
import re
import sys

REPO = "liuruojiang/microcap"
RUN = 41831828946
HEAD = "01cfef626c98216d5ffde0b5b001cd4c9ae25b61"
OLD = "62ed737527030de1a9c5f2897b58bc8670ce1c99"
PRIOR = "e525cb8325a1c11c2dfa3540c3a679d28230986a"
ARTIFACT = 13324051173


def sha(path):
    h = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def checked_name(member, allow_git_symlink=False):
    name = PurePosixPath(member.name)
    if (name.is_absolute() or not name.parts or name.parts[0] != "microcap"
            or ".." in name.parts or ".git" in name.parts or "\\" in member.name
            or not (member.isdir() or member.isfile() or (allow_git_symlink and member.issym()))):
        raise RuntimeError("Unsafe preserved workspace member: " + member.name)
    return PurePosixPath(*name.parts[1:]).as_posix()


def validate_archive(path, root, transition=None):
    tracked = subprocess.check_output(["git", "-C", str(root), "ls-files", "--stage", "-z"])
    identities = {}
    for entry in tracked.split(b"\0"):
        if entry:
            identity, raw_name = entry.split(b"\t", 1)
            identities[raw_name.decode("utf-8")] = identity.decode().split()
    git_links = {name: values[1] for name, values in identities.items()
                 if values[0] == "120000" and values[2] == "0" and name.startswith("research_reports/")}
    actual = {}
    manifest = None
    with tarfile.open(path, "r|gz") as archive:
        for member in archive:
            relative = PurePosixPath(*PurePosixPath(member.name).parts[1:]).as_posix()
            name = checked_name(member, relative in git_links)
            if member.isdir():
                continue
            if name in actual:
                raise RuntimeError("Duplicate workspace member")
            if member.issym():
                raw = member.linkname.encode("utf-8")
                blob = hashlib.sha1(b"blob " + str(len(raw)).encode() + b"\0" + raw).hexdigest()
                if blob != git_links[name]:
                    raise RuntimeError("Preserved historical Git symlink differs: " + name)
                actual[name] = {"sha256": hashlib.sha256(raw).hexdigest(), "git_blob": blob, "existing_git_symlink_only": True}
                continue
            digest = hashlib.sha256()
            native_digest = hashlib.sha256()
            carry = b""
            blob = hashlib.sha1(b"blob " + str(member.size).encode() + b"\0")
            captured = bytearray() if name == "outputs/top100_delivery_manifest.json" else None
            with archive.extractfile(member) as stream:
                for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                    digest.update(chunk)
                    blob.update(chunk)
                    normalized = carry + chunk
                    carry = b"\r" if normalized.endswith(b"\r") else b""
                    if carry:
                        normalized = normalized[:-1]
                    native_digest.update(normalized.replace(b"\r\n", b"\n"))
                    if captured is not None:
                        captured.extend(chunk)
            native_digest.update(carry)
            # Exactly top100_delivery.input_hashes/sha: raw ST evidence;
            # all other registered inputs and final artifacts use CRLF->LF.
            native = digest.hexdigest() if name.startswith("data/st_notice_evidence/") else native_digest.hexdigest()
            actual[name] = {"sha256": digest.hexdigest(), "native_sha256": native, "git_blob": blob.hexdigest()}
            if captured is not None:
                manifest = json.loads(captured)
    if not manifest or manifest.get("status") != "complete" or manifest.get("expected_date") != "2026-11-03" or manifest.get("errors"):
        raise RuntimeError("Preserved workspace is not the completed original restored day")
    bound = {}
    allowed = set()
    if transition is not None:
        if (transition.get('ok') is not True or transition.get('old_restore_runtime') != PRIOR
                or transition.get('new_runtime') != OLD or transition.get('protected_state_unchanged') is not True):
            raise RuntimeError('Preserved source transition identity differs')
        changes = subprocess.check_output(['git','-C',str(root),'diff','--name-only',PRIOR,OLD]).decode().splitlines()
        if sorted(changes) != sorted(transition['reviewed_changed_paths']):
            raise RuntimeError('Preserved source transition scope differs')
        allowed = set(changes)
        for name, digest in transition['protected_files'].items():
            if name not in actual or actual[name]['sha256'] != digest:
                raise RuntimeError('Source-only transition protected state differs: '+name)
    for group in (manifest["inputs"], manifest["artifacts"]):
        if not group:
            raise RuntimeError("Preserved native manifest is missing a group")
        for name, digest in group.items():
            if name not in actual or actual[name].get("native_sha256") != digest:
                if name not in allowed or name.startswith(('outputs/','.microcap_index_cache/')):
                    raise RuntimeError("Preserved native manifest binding differs: " + name)
                old_raw = subprocess.check_output(['git','-C',str(root),'show',PRIOR+':'+name])
                if hashlib.sha256(old_raw.replace(b'\r\n',b'\n')).hexdigest() != digest:
                    raise RuntimeError('Original manifest source binding differs: '+name)
            bound[name] = digest
    count = 0
    for name, (mode, blob, stage) in identities.items():
        if stage != "0" or mode not in ("100644", "100755", "120000") or name not in actual:
            raise RuntimeError("Preserved tracked inventory differs: " + name)
        if mode == "120000" and not actual[name].get("existing_git_symlink_only"):
            raise RuntimeError("Preserved Git symlink type differs: " + name)
        # Original native restore replaces mutable state with the approved
        # archive. Those files must not equal Git's older seed snapshots.
        # Their registered native hashes were verified above; all remaining
        # state bytes are bound by the authenticated post-restore archive.
        mutable_state = name.startswith(("outputs/", ".microcap_index_cache/"))
        if not mutable_state or name.endswith((".py", ".pyw")):
            if actual[name]["git_blob"] != blob:
                raise RuntimeError("Preserved Git source differs: " + name)
        count += 1
    return manifest, actual, count


def extract_verified(path, root, actual):
    with tarfile.open(path, "r|gz") as archive:
        for member in archive:
            relative = PurePosixPath(*PurePosixPath(member.name).parts[1:]).as_posix()
            name = checked_name(member, actual.get(relative, {}).get("existing_git_symlink_only", False))
            if member.isdir():
                continue
            if member.issym():
                # Preserve the fresh official checkout's existing historical
                # link; never create or follow a link from the archive.
                continue
            destination = root.joinpath(*PurePosixPath(name).parts)
            if destination.is_symlink() or any(p.is_symlink() for p in destination.parents):
                raise RuntimeError("Preserved destination has a symlink")
            destination.parent.mkdir(parents=True, exist_ok=True)
            with archive.extractfile(member) as source, destination.open("wb") as target:
                shutil.copyfileobj(source, target)
            if sha(destination) != actual[name]["sha256"]:
                raise RuntimeError("Extracted workspace bytes differ: " + name)


def recover(root, artifact_dir):
    root = root.resolve()
    if subprocess.check_output(["git", "-C", str(root), "rev-parse", "HEAD"], text=True).strip() != OLD:
        raise RuntimeError("Not the certified original restore runtime")
    def gh(*args):
        return subprocess.check_output(["gh", *args], timeout=1800)
    run = json.loads(gh("api", f"repos/{REPO}/actions/runs/{RUN}"))
    if run["head_sha"] != HEAD or run["conclusion"] != "failure":
        raise RuntimeError("Original failed producer identity differs")
    jobs = json.loads(gh("api", f"repos/{REPO}/actions/runs/{RUN}/jobs"))
    steps = {s["name"]: s["conclusion"] for j in jobs["jobs"] for s in j["steps"]}
    required = {"Recover verified already-restored workspace from authenticated failed run": "success",
                "Verify exact source-only transition and unchanged restored state": "success",
                "Prepare and verify native current share evidence before whole refresh": "failure",
                "Official strict incremental whole refresh and three-version generation": "skipped"}
    if any(steps.get(name) != status for name, status in required.items()):
        raise RuntimeError("Original workspace did not stop at the verified pre-mutation stage")
    failed_log = gh("run", "view", str(RUN), "--repo", REPO, "--log-failed")
    if b"Unchanged share tail: EM new or ambiguous tail event" not in failed_log:
        raise RuntimeError("Original failure is not the reviewed 603028 event")
    completed = set(re.findall(rb'\[native-share-preparation\] verified ([0-9]{6}) 2026-11-04', failed_log))
    if len(completed) != 71 or b'603028' in completed:
        raise RuntimeError('Original successful native share inventory differs')
    metadata = json.loads(gh("api", f"repos/{REPO}/actions/artifacts/{ARTIFACT}"))
    if metadata["workflow_run"]["id"] != RUN or metadata["name"] != "microcap-postclose-repair-20261104" or metadata["expired"]:
        raise RuntimeError("Preserved artifact identity differs")
    artifact_dir.mkdir(parents=True, exist_ok=False)
    gh("run", "download", str(RUN), "--repo", REPO, "--name", metadata["name"], "--dir", str(artifact_dir))
    matches = list(artifact_dir.rglob("failed_workspace_NOT_APPROVED.tar.gz"))
    if len(matches) != 1:
        raise RuntimeError("Expected one preserved workspace")
    archive = matches[0]
    expected = archive.with_name("failed_workspace_NOT_APPROVED.sha256").read_text().split()[0]
    if sha(archive) != expected:
        raise RuntimeError("Preserved workspace archive digest differs")
    transitions = list(artifact_dir.rglob('source_transition.json'))
    if len(transitions) != 1:
        raise RuntimeError('Expected one preserved source transition')
    transition = json.loads(transitions[0].read_bytes())
    subprocess.run(['git','-C',str(root),'fetch','--no-tags','origin',PRIOR],check=True)
    manifest, actual, tracked = validate_archive(archive, root, transition)
    extract_verified(archive, root, actual)
    sys.path.insert(0,str(root))
    from scripts.share_change_source import verify_share_cache
    from scripts.realtime_state_bundle import current_notice_member_symbols, _verify_share_source_json_protocol
    symbols = current_notice_member_symbols(root)
    if len(symbols) != 102:
        raise RuntimeError('Recovered native member scope differs')
    recovered_current = set()
    for symbol in symbols:
        state = json.loads((root/'.microcap_index_cache/share_sources'/(symbol+'.json')).read_bytes())
        end = state['query_end']
        if end not in ('2026-11-03','2026-11-04'):
            raise RuntimeError('Unexpected recovered share date')
        proof = verify_share_cache(root=root,symbol=symbol,as_of=end)
        _verify_share_source_json_protocol(root,symbol,proof)
        if end == '2026-11-04': recovered_current.add(symbol.encode())
    if recovered_current != completed:
        raise RuntimeError('Recovered native successful share scope differs from actual log')
    return {"ok": True, "original_restore_run": RUN, "original_restore_head": HEAD,
            "original_runtime": OLD, "artifact_id": ARTIFACT, "archive_sha256": expected,
            "workspace_files": len(actual), "verified_tracked_files": tracked,
            "native_manifest_inputs_and_artifacts_verified": True, "expected_date": manifest["expected_date"],
            "original_run": run, "artifact_metadata": metadata,
            "native_current_share_states_preserved": len(recovered_current),
            "scope": "D3 whole state plus 71 actual verified D4 share sources; not a D4 whole approval; no manifest rewrite, SMTP or marker"}


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--root", type=Path, required=True)
    p.add_argument("--artifact-dir", type=Path, required=True)
    p.add_argument("--audit", type=Path, required=True)
    args = p.parse_args()
    result = recover(args.root, args.artifact_dir)
    args.audit.write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(json.dumps({k: v for k, v in result.items() if k not in ("original_run", "artifact_metadata")}))
