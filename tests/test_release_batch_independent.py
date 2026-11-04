"""Independent release-batch checks using local Git objects, never the network."""
import hashlib
import subprocess

import pytest

from scripts import top100_delivery as delivery


def git(root, *args):
    return subprocess.check_output(["git", *args], cwd=root, stderr=subprocess.PIPE)


def repository(root, files):
    git(root, "init", "-b", "main")
    git(root, "config", "core.autocrlf", "false")
    git(root, "config", "user.name", "Independent audit")
    git(root, "config", "user.email", "audit@example.invalid")
    for name, payload in files.items():
        path = root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(payload)
    git(root, "add", ".")
    git(root, "commit", "-m", "Local release")
    git(root, "remote", "add", "origin", str(root))
    return git(root, "rev-parse", "HEAD").decode().strip()


def test_real_batch_binary_payload_and_utf8_path_are_length_framed(tmp_path, monkeypatch):
    files = {
        "scripts/含空格 core.py": b"print('ok')\n",
        "data/st_notice_evidence/report.bin": (
            b"\x00\xff\r\n" + b"a" * 40 + b" blob 12\n" + b"END\x00\n"
        ),
        "scripts/last.py": b"last=1",  # No final newline is a valid Git blob.
    }
    revision = repository(tmp_path, files)
    monkeypatch.setattr(delivery, "RELEASE_FILES", list(files))
    assert delivery.verify_release(tmp_path) == revision
    (tmp_path / "data/st_notice_evidence/report.bin").write_bytes(
        files["data/st_notice_evidence/report.bin"].replace(b"\r\n", b"\n")
    )
    with pytest.raises(ValueError, match="differs from remote release"):
        delivery.verify_release(tmp_path)


def test_ref_moving_after_resolution_keeps_one_immutable_snapshot(tmp_path, monkeypatch):
    name = "scripts/core.py"
    old = repository(tmp_path, {name: b"old\n"})
    monkeypatch.setattr(delivery, "RELEASE_FILES", [name])
    original = subprocess.check_output
    moved = []

    def checked(args, **kwargs):
        result = original(args, **kwargs)
        if args[:2] == ["git", "ls-remote"] and not moved:
            (tmp_path / name).write_bytes(b"new\n")
            original(["git", "add", "."], cwd=tmp_path)
            original(["git", "commit", "-m", "Move main"], cwd=tmp_path)
            moved.append(True)
            (tmp_path / name).write_bytes(b"old\n")
        return result

    monkeypatch.setattr(subprocess, "check_output", checked)
    assert delivery.verify_release(tmp_path) == old
    with pytest.raises(ValueError, match="differs from remote release"):
        delivery.verify_release(tmp_path)


def test_missing_local_source_cannot_be_replaced_by_valid_remote(tmp_path, monkeypatch):
    name = "scripts/core.py"
    repository(tmp_path, {name: b"approved\n"})
    monkeypatch.setattr(delivery, "RELEASE_FILES", [name])
    (tmp_path / name).unlink()
    with pytest.raises(FileNotFoundError):
        delivery.verify_release(tmp_path)


@pytest.mark.parametrize("corrupt", ["second_payload", "second_header", "extra_delimiter"])
def test_multi_object_corruption_does_not_hide_after_first_success(tmp_path, monkeypatch, corrupt):
    files = {"a.py": b"first\n", "b.py": b"second\n"}
    for name, payload in files.items():
        (tmp_path / name).write_bytes(payload)
    monkeypatch.setattr(delivery, "RELEASE_FILES", list(files))
    monkeypatch.setattr(subprocess, "check_output", lambda args, **kwargs:
                        "a" * 40 + "\trefs/heads/main\n" if args[1] == "ls-remote" else b"")

    def frame(payload):
        oid = hashlib.sha1(b"blob " + str(len(payload)).encode() + b"\0" + payload).hexdigest()
        return oid.encode() + b" blob " + str(len(payload)).encode() + b"\n" + payload + b"\n"

    first, second = (frame(payload) for payload in files.values())
    if corrupt == "second_payload":
        second = second.replace(b"second", b"tamper")
    elif corrupt == "second_header":
        second = b"f" * 40 + second[40:]
    else:
        first += b"\n"
    outputs = []

    def run(args, **kwargs):
        outputs.append(kwargs["stdout"])
        kwargs["stdout"].write(first + second)
        return subprocess.CompletedProcess(args, 0)

    monkeypatch.setattr(subprocess, "run", run)
    with pytest.raises(ValueError):
        delivery.verify_release(tmp_path)
    assert outputs[0].closed
