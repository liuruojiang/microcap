"""Recover only the certified old-state workspace stopped before source mutation."""
import argparse
import hashlib
import json
from pathlib import Path, PurePosixPath
import shutil
import subprocess
import tarfile

REPO = "liuruojiang/microcap"
RUN = 41824319041
HEAD = "bf033affc6b85a25a8401242493f66859cc08bf0"
OLD = "e525cb8325a1c11c2dfa3540c3a679d28230986a"
ARTIFACT = 13318752531


def sha(path):
    h = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def checked_name(member):
    name = PurePosixPath(member.name)
    if (name.is_absolute() or not name.parts or name.parts[0] != "microcap"
            or ".." in name.parts or ".git" in name.parts or "\\" in member.name
            or not (member.isdir() or member.isfile())):
        raise RuntimeError("Unsafe preserved workspace member")
    return PurePosixPath(*name.parts[1:]).as_posix()


def validate_archive(path, root):
    actual = {}
    manifest = None
    with tarfile.open(path, "r|gz") as archive:
        for member in archive:
            name = checked_name(member)
            if member.isdir():
                continue
            if name in actual:
                raise RuntimeError("Duplicate workspace member")
            digest = hashlib.sha256()
            blob = hashlib.sha1(b"blob " + str(member.size).encode() + b"\0")
            captured = bytearray() if name == "outputs/top100_delivery_manifest.json" else None
            with archive.extractfile(member) as stream:
                for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                    digest.update(chunk)
                    blob.update(chunk)
                    if captured is not None:
                        captured.extend(chunk)
            actual[name] = {"sha256": digest.hexdigest(), "git_blob": blob.hexdigest()}
            if captured is not None:
                manifest = json.loads(captured)
    if not manifest or manifest.get("status") != "complete" or manifest.get("expected_date") != "2026-11-03" or manifest.get("errors"):
        raise RuntimeError("Preserved workspace is not the completed original restored day")
    bound = {}
    for group in (manifest["inputs"], manifest["artifacts"]):
        if not group:
            raise RuntimeError("Preserved native manifest is missing a group")
        for name, digest in group.items():
            if name not in actual or actual[name]["sha256"] != digest:
                raise RuntimeError("Preserved native manifest binding differs: " + name)
            bound[name] = digest
    tracked = subprocess.check_output(["git", "-C", str(root), "ls-files", "--stage", "-z"])
    count = 0
    for entry in tracked.split(b"\0"):
        if not entry:
            continue
        identity, raw_name = entry.split(b"\t", 1)
        mode, blob, stage = identity.decode().split()
        name = raw_name.decode("utf-8")
        if stage != "0" or mode not in ("100644", "100755") or name not in actual:
            raise RuntimeError("Preserved tracked inventory differs: " + name)
        if name not in bound or name.endswith((".py", ".pyw")):
            if actual[name]["git_blob"] != blob:
                raise RuntimeError("Preserved Git source differs: " + name)
        count += 1
    return manifest, actual, count


def extract_verified(path, root, actual):
    with tarfile.open(path, "r|gz") as archive:
        for member in archive:
            name = checked_name(member)
            if member.isdir():
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
    required = {"Restore original approved archive through verified compatible transport": "success",
                "Verify exact source-only transition and unchanged restored state": "failure",
                "Prepare and verify native current share evidence before whole refresh": "skipped",
                "Official strict incremental whole refresh and three-version generation": "skipped"}
    if any(steps.get(name) != status for name, status in required.items()):
        raise RuntimeError("Original workspace did not stop at the verified pre-mutation stage")
    failed_log = gh("run", "view", str(RUN), "--repo", REPO, "--log-failed")
    if b"RuntimeError: Source transition approval changed" not in failed_log:
        raise RuntimeError("Original failure is not the pre-mutation byte check")
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
    manifest, actual, tracked = validate_archive(archive, root)
    extract_verified(archive, root, actual)
    return {"ok": True, "original_restore_run": RUN, "original_restore_head": HEAD,
            "original_runtime": OLD, "artifact_id": ARTIFACT, "archive_sha256": expected,
            "workspace_files": len(actual), "verified_tracked_files": tracked,
            "native_manifest_inputs_and_artifacts_verified": True, "expected_date": manifest["expected_date"],
            "original_run": run, "artifact_metadata": metadata,
            "scope": "verified original restore reuse only; no current data, manifest rewrite, SMTP or marker"}


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--root", type=Path, required=True)
    p.add_argument("--artifact-dir", type=Path, required=True)
    p.add_argument("--audit", type=Path, required=True)
    args = p.parse_args()
    result = recover(args.root, args.artifact_dir)
    args.audit.write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(json.dumps({k: v for k, v in result.items() if k not in ("original_run", "artifact_metadata")}))
