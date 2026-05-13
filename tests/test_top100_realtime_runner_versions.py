from __future__ import annotations

import types
from pathlib import Path

import run_top100_v1_6_v1_8_realtime_signals as runner


class _SignalFrame:
    def __init__(self, row: dict[str, object]) -> None:
        self._row = row
        self.iloc = self

    def __len__(self) -> int:
        return 1

    def __getitem__(self, index: int) -> object:
        assert index == 0
        return types.SimpleNamespace(to_dict=lambda: self._row)


def _install_static_inputs(monkeypatch, tmp_path: Path) -> str:
    module_name = "fake_static_inputs"
    module = types.ModuleType(module_name)
    module.UNIVERSE_CACHE = tmp_path / "active_universe.csv"
    module.CURRENT_ST_CACHE = tmp_path / "current_st.csv"

    def write_universe(*, force_refresh: bool = False) -> None:
        Path(module.UNIVERSE_CACHE).write_text("code\n000001\n", encoding="utf-8")

    def write_st(*, force_refresh: bool = False) -> None:
        Path(module.CURRENT_ST_CACHE).write_text("code\n", encoding="utf-8")

    module.fetch_active_universe = write_universe
    module.fetch_current_st_codes = write_st
    monkeypatch.setitem(__import__("sys").modules, module_name, module)
    return module_name


def _install_strategy(monkeypatch, tmp_path: Path, version: str, calls: list[str]) -> runner.StrategySpec:
    module_name = f"fake_{version.replace('.', '_')}"
    builder_name = f"build_{version.replace('.', '_')}"
    module = types.ModuleType(module_name)
    module.REALTIME_SIGNAL_CSV = tmp_path / f"{version}.csv"

    def build() -> tuple[_SignalFrame, dict[str, object], None]:
        calls.append(version)
        row = {
            "snapshot_time": "2026-05-13 16:00:00+08:00",
            "latest_anchor_trade_date": "2026-05-12",
            "quote_trade_date": "2026-05-13",
            "current_execution_scale": 1.5,
            "next_session_actionable_scale": 1.5,
        }
        return _SignalFrame(row), {"strategy_version": version}, None

    setattr(module, builder_name, build)
    monkeypatch.setitem(__import__("sys").modules, module_name, module)
    return runner.StrategySpec(
        version=version,
        module_name=module_name,
        builder_name=builder_name,
        realtime_csv_attr="REALTIME_SIGNAL_CSV",
    )


def test_cli_versions_filter_runs_only_requested_strategy(monkeypatch, tmp_path: Path, capsys) -> None:
    static_module = _install_static_inputs(monkeypatch, tmp_path)
    calls: list[str] = []
    specs = (
        _install_strategy(monkeypatch, tmp_path, "v1.6", calls),
        _install_strategy(monkeypatch, tmp_path, "v1.8", calls),
    )

    exit_code = runner.main(["--versions", "v1.6"], specs=specs, static_inputs_module=static_module)

    captured = capsys.readouterr()
    assert exit_code == 0
    assert calls == ["v1.6"]
    assert "strategy_version: v1.6" in captured.out
    assert "strategy_version: v1.8" not in captured.out
