"""Explicit additional release-reader regression; real local Git processes required.

Kept outside the offline core30 suite, whose process guard intentionally forbids
non-Python subprocesses. Run this entire file for release-reader changes.
"""
import subprocess
import pytest
from scripts import top100_delivery as delivery
from test_top100_delivery import write


def release_git(root, *args):
    return subprocess.check_output(["git", *args], cwd=root, stderr=subprocess.PIPE)


def release_repo(root, files):
    root.mkdir(parents=True, exist_ok=True)
    release_git(root, "init", "-b", "main")
    release_git(root, "config", "core.autocrlf", "false")
    release_git(root, "config", "user.name", "Release test")
    release_git(root, "config", "user.email", "release-test@example.invalid")
    for name, raw in files.items():
        path = root/name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(raw)
    release_git(root, "add", ".")
    release_git(root, "commit", "-m", "Test approved source")
    release_git(root, "remote", "add", "origin", str(root))
    return release_git(root, "rev-parse", "HEAD").decode().strip()


def test_release_batch_real_git_all_files_once_and_normalized_code(tmp_path, monkeypatch):
    files = {f"scripts/source_{i}.py": f"value = {i}\n".encode() for i in range(12)}
    files["data/st_notice_evidence/empty.pdf"] = b""
    remote = release_repo(tmp_path, files)
    monkeypatch.setattr(delivery, "RELEASE_FILES", list(files))
    (tmp_path/"scripts/source_0.py").write_bytes(b"value = 0\r\n")
    popen = subprocess.Popen
    commands = []
    def counted(args, **kwargs):
        commands.append(args)
        return popen(args, **kwargs)
    monkeypatch.setattr(subprocess, "Popen", counted)
    assert delivery.verify_release(tmp_path) == remote
    assert len(commands) == 3  # ls-remote, object presence, one batch reader
    assert commands[-1] == ["git", "cat-file", "--batch"]
    assert not any("show" in args for args in commands)


def test_release_batch_reads_live_remote_each_call(tmp_path, monkeypatch):
    name = "scripts/core.py"
    old = release_repo(tmp_path, {name: b"old\n"})
    monkeypatch.setattr(delivery, "RELEASE_FILES", [name])
    assert delivery.verify_release(tmp_path) == old
    (tmp_path/name).write_bytes(b"new\n")
    release_git(tmp_path, "add", ".")
    release_git(tmp_path, "commit", "-m", "New approved source")
    new = release_git(tmp_path, "rev-parse", "HEAD").decode().strip()
    (tmp_path/name).write_bytes(b"old\n")
    with pytest.raises(ValueError, match="differs from remote release"):
        delivery.verify_release(tmp_path)
    (tmp_path/name).write_bytes(b"new\n")
    assert delivery.verify_release(tmp_path) == new != old


def test_release_batch_fetches_missing_commit_without_checkout(tmp_path, monkeypatch):
    source, local = tmp_path/"source", tmp_path/"local"
    name = "scripts/core.py"
    old = release_repo(source, {name: b"old\n"})
    release_git(tmp_path, "clone", "--no-local", str(source), str(local))
    (source/name).write_bytes(b"new\n")
    release_git(source, "add", ".")
    release_git(source, "commit", "-m", "New remote source")
    new = release_git(source, "rev-parse", "HEAD").decode().strip()
    (local/name).write_bytes(b"new\n")
    monkeypatch.setattr(delivery, "RELEASE_FILES", [name])
    assert delivery.verify_release(local) == new
    assert release_git(local, "rev-parse", "HEAD").decode().strip() == old
    assert (local/name).read_bytes() == b"new\n"


def release_blob(raw=b"approved\n"):
    import hashlib
    identity = hashlib.sha1(b"blob " + str(len(raw)).encode() + b"\0" + raw).hexdigest().encode()
    return identity + b" blob " + str(len(raw)).encode() + b"\n" + raw + b"\n"


@pytest.mark.parametrize("response, pattern", [
    (b"", "Missing/non-blob"),
    (b"missing-path missing\n", "Missing/non-blob"),
    (b"a"*40 + b" tree 0\n\n", "Missing/non-blob"),
    (b"a"*40 + b" blob -1\n", "invalid Git"),
    (b"a"*40 + b" blob 999999999999\n", "Truncated"),
    (release_blob()[:-2], "Truncated"),
    (release_blob()[:-1] + b"x", "invalid delimiter"),
    (b"a"*40 + release_blob()[40:], "identity mismatch"),
    (release_blob() + release_blob(), "trailing"),
    (release_blob() + b"garbage", "trailing"),
])
def test_release_batch_rejects_invalid_protocol(tmp_path, monkeypatch, response, pattern):
    name = "scripts/core.py"
    write(tmp_path, name, "approved\n")
    monkeypatch.setattr(delivery, "RELEASE_FILES", [name])
    monkeypatch.setattr(subprocess, "check_output", lambda args, **kw:
                        "a"*40 + "\trefs/heads/main\n" if args[1] == "ls-remote" else b"")
    outputs = []
    def batch(args, **kwargs):
        assert args == ["git", "cat-file", "--batch"]
        assert kwargs["input"] == ("a"*40 + ":" + name + "\n").encode()
        assert kwargs["check"] is True and kwargs["timeout"] == 300
        outputs.append(kwargs["stdout"])
        kwargs["stdout"].write(response)
        return subprocess.CompletedProcess(args, 0)
    monkeypatch.setattr(subprocess, "run", batch)
    with pytest.raises(ValueError, match=pattern):
        delivery.verify_release(tmp_path)
    assert outputs[0].closed


@pytest.mark.parametrize("failure", [
    subprocess.CalledProcessError(7, ["git", "cat-file", "--batch"]),
    subprocess.TimeoutExpired(["git", "cat-file", "--batch"], 300),
])
def test_release_batch_propagates_process_failure(tmp_path, monkeypatch, failure):
    monkeypatch.setattr(delivery, "RELEASE_FILES", ["scripts/core.py"])
    monkeypatch.setattr(subprocess, "check_output", lambda args, **kw:
                        "a"*40 + "\trefs/heads/main\n" if args[1] == "ls-remote" else b"")
    outputs = []
    def batch(args, **kwargs):
        outputs.append(kwargs["stdout"])
        kwargs["stdout"].write(release_blob())
        raise failure
    monkeypatch.setattr(subprocess, "run", batch)
    with pytest.raises(type(failure)):
        delivery.verify_release(tmp_path)
    assert outputs[0].closed


def test_release_batch_rejects_missing_real_git_object(tmp_path, monkeypatch):
    release_repo(tmp_path, {"scripts/core.py": b"approved\n"})
    monkeypatch.setattr(delivery, "RELEASE_FILES", ["scripts/missing.py"])
    with pytest.raises(ValueError, match="Missing/non-blob"):
        delivery.verify_release(tmp_path)


def test_release_batch_timeout_reaps_its_real_child(tmp_path, monkeypatch):
    import sys
    monkeypatch.setattr(delivery, "RELEASE_FILES", ["scripts/core.py"])
    monkeypatch.setattr(subprocess, "check_output", lambda args, **kw:
                        "a"*40 + "\trefs/heads/main\n" if args[1] == "ls-remote" else b"")
    real_run, real_popen = subprocess.run, subprocess.Popen
    children = []
    def popen(args, **kwargs):
        child = real_popen(args, **kwargs)
        children.append(child)
        return child
    def stalled_batch(args, **kwargs):
        assert args == ["git", "cat-file", "--batch"]
        assert kwargs["timeout"] == 300
        kwargs["timeout"] = 0.2
        return real_run([sys.executable, "-B", "-c", "import time; time.sleep(60)"], **kwargs)
    monkeypatch.setattr(subprocess, "Popen", popen)
    monkeypatch.setattr(subprocess, "run", stalled_batch)
    with pytest.raises(subprocess.TimeoutExpired):
        delivery.verify_release(tmp_path)
    assert len(children) == 1 and children[0].poll() is not None


@pytest.mark.parametrize("name", ["scripts/core.py\nHEAD:other.py", "scripts/core.py\0"])
def test_release_batch_rejects_protocol_path_injection(tmp_path, monkeypatch, name):
    monkeypatch.setattr(delivery, "RELEASE_FILES", [name])
    monkeypatch.setattr(subprocess, "check_output", lambda args, **kw:
                        "a"*40 + "\trefs/heads/main\n" if args[1] == "ls-remote" else b"")
    def unexpected(*args, **kwargs):
        pytest.fail("invalid request must not start a batch process")
    monkeypatch.setattr(subprocess, "run", unexpected)
    with pytest.raises(ValueError, match="Invalid release path"):
        delivery.verify_release(tmp_path)
