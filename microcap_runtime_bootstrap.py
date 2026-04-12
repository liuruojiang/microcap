from __future__ import annotations

import importlib.util
import os
import subprocess
import sys
from pathlib import Path
from typing import Iterable


WHEELHOUSE_ENV_VAR = "MICROCAP_WHEELHOUSE"
RUNTIME_PACKAGES = ("numpy", "pandas", "requests", "akshare", "matplotlib")
DEFAULT_WHEELHOUSE_CANDIDATES = (
    "wheelhouse",
    ".vendor_libs\\wheelhouse",
    ".vendor_libs",
)


def find_missing_modules(module_names: Iterable[str] = RUNTIME_PACKAGES) -> list[str]:
    missing: list[str] = []
    for module_name in module_names:
        if importlib.util.find_spec(module_name) is None:
            missing.append(module_name)
    return missing


def build_runtime_tag(
    sys_platform: str | None = None,
    machine: str | None = None,
    implementation_name: str | None = None,
    version_info: tuple[int, int] | None = None,
) -> str:
    platform_name = (sys_platform or sys.platform).lower()
    machine_name = (machine or os.uname().machine if hasattr(os, "uname") else "").lower()
    if not machine_name:
        machine_name = os.environ.get("PROCESSOR_ARCHITECTURE", "").lower() or "unknown"
    if machine_name in {"amd64", "x64"}:
        machine_name = "x86_64"

    impl_name = (implementation_name or sys.implementation.name).lower()
    impl_tag = "cp" if impl_name == "cpython" else impl_name[:2]
    major, minor = version_info or (sys.version_info.major, sys.version_info.minor)
    return f"{platform_name}_{machine_name}_{impl_tag}{major}{minor}"


def _expand_runtime_candidate(candidate: Path, runtime_tag: str) -> list[Path]:
    return [candidate, candidate / runtime_tag]


def resolve_wheelhouse(
    repo_root: Path,
    cli_path: Path | None = None,
    env: dict[str, str] | None = None,
    sys_platform: str | None = None,
    machine: str | None = None,
    implementation_name: str | None = None,
    version_info: tuple[int, int] | None = None,
) -> Path | None:
    env_map = os.environ if env is None else env
    candidates: list[Path] = []
    runtime_tag = build_runtime_tag(
        sys_platform=sys_platform,
        machine=machine,
        implementation_name=implementation_name,
        version_info=version_info,
    )

    if cli_path is not None:
        candidates.extend(_expand_runtime_candidate(Path(cli_path), runtime_tag))

    env_value = env_map.get(WHEELHOUSE_ENV_VAR)
    if env_value:
        candidates.extend(_expand_runtime_candidate(Path(env_value), runtime_tag))

    for relative in DEFAULT_WHEELHOUSE_CANDIDATES:
        candidates.extend(_expand_runtime_candidate(repo_root / relative, runtime_tag))

    for candidate in candidates:
        resolved = candidate.expanduser().resolve()
        if resolved.is_dir() and any(resolved.glob("*.whl")):
            return resolved
    return None


def bootstrap_from_wheelhouse(
    wheelhouse: Path,
    packages: Iterable[str] = RUNTIME_PACKAGES,
    python_executable: str | None = None,
) -> subprocess.CompletedProcess[str]:
    cmd = [
        python_executable or sys.executable,
        "-m",
        "pip",
        "install",
        "--no-index",
        "--disable-pip-version-check",
        "--find-links",
        str(wheelhouse),
        *packages,
    ]
    return subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )


def format_missing_dependencies_message(missing: Iterable[str], bootstrap_requested: bool = False) -> str:
    packages = ", ".join(missing)
    lines = [f"缺少运行时核心依赖: {packages}."]
    if bootstrap_requested:
        lines.append(
            "离线安装需要可用的 wheelhouse 目录。请传入 --wheelhouse，或设置环境变量 "
            f"{WHEELHOUSE_ENV_VAR}，或在仓库内提供 ./wheelhouse / ./.vendor_libs/wheelhouse / ./.vendor_libs。"
        )
    else:
        lines.append(
            "可使用 --bootstrap-deps 配合离线 wheelhouse 安装，或先手工安装依赖后再运行。"
        )
    return "\n".join(lines)


def format_bootstrap_failure_message(wheelhouse: Path, result: subprocess.CompletedProcess[str]) -> str:
    lines = [
        f"离线依赖安装失败，wheelhouse={wheelhouse}.",
        f"pip exit code: {result.returncode}",
    ]
    if result.stdout.strip():
        lines.append("pip stdout:")
        lines.append(result.stdout.strip())
    if result.stderr.strip():
        lines.append("pip stderr:")
        lines.append(result.stderr.strip())
    return "\n".join(lines)
