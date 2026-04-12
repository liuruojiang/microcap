from __future__ import annotations

import argparse
from importlib import metadata
import subprocess
import sys
from pathlib import Path

import microcap_runtime_bootstrap as runtime_bootstrap


ROOT = Path(__file__).resolve().parent
DEFAULT_WHEELHOUSE = ROOT / "wheelhouse"
PURE_PYTHON_BUILD_PACKAGES = ("jsonpath",)
TARGET_VERSION_OVERRIDES = {
    "numpy": "2.2.6",
    "pandas": "2.3.2",
    "requests": "2.33.1",
    "akshare": "1.18.46",
    "matplotlib": "3.10.8",
}
DEPENDENCY_SPECS = (
    "python-dateutil==2.9.0.post0",
    "pytz==2026.1.post1",
    "tzdata==2025.3",
    "charset-normalizer==3.4.6",
    "idna==3.11",
    "urllib3==2.6.3",
    "certifi==2026.2.25",
    "beautifulsoup4==4.14.3",
    "lxml==6.0.2",
    "curl-cffi==0.15.0",
    "html5lib==1.1",
    "xlrd==2.0.2",
    "tqdm==4.67.3",
    "openpyxl==3.1.5",
    "tabulate==0.10.0",
    "decorator==5.2.1",
    "contourpy==1.3.2",
    "cycler==0.12.1",
    "fonttools==4.62.1",
    "kiwisolver==1.5.0",
    "packaging==26.0",
    "pillow==11.3.0",
    "pyparsing==3.3.2",
    "soupsieve==2.8.3",
    "typing-extensions==4.15.0",
    "et-xmlfile==2.0.0",
    "webencodings==0.5.1",
    "cffi==2.0.0",
    "pycparser==3.0",
    "six==1.17.0",
    "py-mini-racer==0.6.0",
    "akracer==0.0.14",
    "rich==14.3.4",
    "markdown-it-py==4.0.0",
    "mdurl==0.1.2",
    "pygments==2.20.0",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download offline wheels for the Top100 v1.0 runtime dependencies."
    )
    parser.add_argument(
        "--wheelhouse",
        type=Path,
        default=DEFAULT_WHEELHOUSE,
        help="Destination directory for downloaded wheels.",
    )
    parser.add_argument(
        "--python-version",
        default=None,
        help="Optional target Python version for cross-download, e.g. 3.11.",
    )
    parser.add_argument(
        "--platform",
        default=None,
        help="Optional target platform tag, e.g. win_amd64 or manylinux2014_x86_64.",
    )
    parser.add_argument(
        "--implementation",
        default=None,
        help="Optional interpreter tag for cross-download, e.g. cp.",
    )
    parser.add_argument(
        "--abi",
        default=None,
        help="Optional ABI tag for cross-download, e.g. cp311.",
    )
    return parser.parse_args()


def resolve_runtime_requirement_specs() -> list[str]:
    specs: list[str] = []
    for package in runtime_bootstrap.RUNTIME_PACKAGES:
        version = TARGET_VERSION_OVERRIDES.get(package, metadata.version(package))
        specs.append(f"{package}=={version}")
    return specs


def resolve_all_requirement_specs() -> list[str]:
    return [*resolve_runtime_requirement_specs(), *DEPENDENCY_SPECS]


def build_universal_wheel_command(wheelhouse: Path, package: str) -> list[str]:
    return [
        sys.executable,
        "-m",
        "pip",
        "wheel",
        "--no-deps",
        "--wheel-dir",
        str(wheelhouse),
        package,
    ]


def build_download_command(args: argparse.Namespace) -> list[str]:
    cmd = [
        sys.executable,
        "-m",
        "pip",
        "download",
        "--dest",
        str(args.wheelhouse),
        "--find-links",
        str(args.wheelhouse),
        "--no-deps",
        "--only-binary=:all:",
        *resolve_all_requirement_specs(),
    ]
    if args.python_version:
        cmd.extend(["--python-version", args.python_version])
    if args.platform:
        cmd.extend(["--platform", args.platform])
    if args.implementation:
        cmd.extend(["--implementation", args.implementation])
    if args.abi:
        cmd.extend(["--abi", args.abi])
    return cmd


def main() -> int:
    args = parse_args()
    args.wheelhouse.mkdir(parents=True, exist_ok=True)

    for package in PURE_PYTHON_BUILD_PACKAGES:
        build_cmd = build_universal_wheel_command(args.wheelhouse, package)
        build_result = subprocess.run(build_cmd)
        if build_result.returncode != 0:
            return build_result.returncode

    cmd = build_download_command(args)
    result = subprocess.run(cmd)
    if result.returncode != 0:
        return result.returncode

    wheel_count = sum(1 for _ in args.wheelhouse.glob("*.whl"))
    print(f"wheelhouse={args.wheelhouse.resolve()}")
    print(f"wheels={wheel_count}")
    print(
        "Use on the cloud runner with "
        f"--bootstrap-deps --wheelhouse {args.wheelhouse.resolve()}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
