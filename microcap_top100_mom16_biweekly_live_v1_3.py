from __future__ import annotations

import argparse
import contextlib
import hashlib
import json
import sys
from pathlib import Path

import matplotlib
import pandas as pd

matplotlib.use("Agg")
import matplotlib.pyplot as plt

import microcap_top100_mom16_biweekly_live_v1_1 as v1_1_mod


ROOT = Path(__file__).resolve().parent
OUTPUT_DIR = ROOT / "outputs"

BASE_SUMMARY_JSON = OUTPUT_DIR / "microcap_top100_mom16_biweekly_live_v1_1_summary.json"
BASE_SIGNAL_CSV = OUTPUT_DIR / "microcap_top100_mom16_biweekly_live_v1_1_latest_signal.csv"
BASE_COSTED_NAV_CSV = OUTPUT_DIR / "microcap_top100_mom16_hedge_zz1000_0p8x_biweekly_thursday_16y_costed_nav.csv"
V1_0_SUMMARY_JSON = OUTPUT_DIR / "microcap_top100_mom16_biweekly_live_summary.json"

OUTPUT_PREFIX = "microcap_top100_mom16_biweekly_live_v1_3"
SUMMARY_JSON = OUTPUT_DIR / f"{OUTPUT_PREFIX}_summary.json"
LATEST_SIGNAL_CSV = OUTPUT_DIR / f"{OUTPUT_PREFIX}_latest_signal.csv"
NAV_CSV = OUTPUT_DIR / f"{OUTPUT_PREFIX}_nav.csv"
COSTED_NAV_CSV = OUTPUT_DIR / "microcap_top100_mom16_hedge_zz1000_0p8x_peakdd1_biweekly_thursday_16y_costed_nav.csv"
PERF_SUMMARY_CSV = OUTPUT_DIR / f"{OUTPUT_PREFIX}_performance_summary.csv"
PERF_YEARLY_CSV = OUTPUT_DIR / f"{OUTPUT_PREFIX}_performance_yearly.csv"
PERF_NAV_CSV = OUTPUT_DIR / f"{OUTPUT_PREFIX}_performance_nav.csv"
PERF_JSON = OUTPUT_DIR / f"{OUTPUT_PREFIX}_performance_summary.json"
PERF_PNG = OUTPUT_DIR / f"{OUTPUT_PREFIX}_performance_curve.png"
EXPECTED_VERSION_ROLE = "strict_stop_alternative"
EXPECTED_VERSION_NOTE_PREFIX = "Strict stop alternative."
BASE_HEDGE_RATIO = 0.8
STRICT_STOP_THRESHOLD = 0.01

LIVE_MEMBER_QUERIES = {"成分股", "进出名单", "实时进出名单"}


def _format_pct(value: float) -> str:
    return f"{float(value):+.2%}"


def _format_num(value: float) -> str:
    return f"{float(value):,.2f}"


def _render_holding(value: str) -> str:
    if value == "cash":
        return "空仓"
    if value == "long_microcap_short_zz1000":
        return "多微盘 / 空中证1000"
    return str(value)


def _render_trade_state(value: str) -> str:
    return {
        "hold": "不变",
        "open": "开仓",
        "close": "平仓",
        "switch": "切换",
    }.get(str(value), str(value))


def _is_performance_query(query: str) -> bool:
    return bool(v1_1_mod.base_mod.PERFORMANCE_PATTERN.search(query))


def _file_sha1(path: Path) -> str:
    digest = hashlib.sha1()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _build_v1_1_args(max_workers: int = 8) -> argparse.Namespace:
    base_mod = v1_1_mod.base_mod
    return argparse.Namespace(
        query_tokens=[],
        panel_path=base_mod.hedge_mod.DEFAULT_PANEL,
        index_csv=base_mod.DEFAULT_INDEX_CSV,
        costed_nav_csv=base_mod.DEFAULT_COSTED_NAV_CSV,
        output_prefix=base_mod.DEFAULT_OUTPUT_PREFIX,
        capital=None,
        max_workers=max_workers,
        realtime_cache_seconds=30,
        rebuild_index_if_missing=True,
        force_refresh=False,
        max_stale_anchor_days=base_mod.DEFAULT_MAX_STALE_ANCHOR_DAYS,
        allow_stale_realtime=False,
    )


def current_base_fingerprint() -> dict[str, object]:
    return {
        "base_version": "1.1",
        "base_costed_nav_csv": str(BASE_COSTED_NAV_CSV),
        "base_costed_nav_sha1": _file_sha1(BASE_COSTED_NAV_CSV),
        "research_stack_version": v1_1_mod.base_mod.RESEARCH_STACK_VERSION,
        "stop_type": "peak_drawdown_strict",
        "stop_loss_threshold": STRICT_STOP_THRESHOLD,
    }


def summary_matches_current_v1_3_base(summary: dict[str, object]) -> bool:
    if not isinstance(summary, dict):
        return False
    if str(summary.get("version")) != "1.3":
        return False
    if str(summary.get("version_role")) != EXPECTED_VERSION_ROLE:
        return False
    if not str(summary.get("version_note", "")).startswith(EXPECTED_VERSION_NOTE_PREFIX):
        return False
    return summary.get("base_fingerprint") == current_base_fingerprint()


def invalidate_incompatible_v1_3_outputs() -> list[Path]:
    if not SUMMARY_JSON.exists():
        return []
    try:
        summary = json.loads(SUMMARY_JSON.read_text(encoding="utf-8"))
    except Exception:
        summary = None
    if summary_matches_current_v1_3_base(summary):
        return []
    removed: list[Path] = []
    for path in [
        SUMMARY_JSON,
        LATEST_SIGNAL_CSV,
        NAV_CSV,
        COSTED_NAV_CSV,
        PERF_SUMMARY_CSV,
        PERF_YEARLY_CSV,
        PERF_NAV_CSV,
        PERF_JSON,
        PERF_PNG,
    ]:
        if path.exists():
            path.unlink(missing_ok=True)
            removed.append(path)
    return removed


@contextlib.contextmanager
def patched_live_module():
    live_mod = v1_1_mod.base_mod
    original_output_prefix = live_mod.DEFAULT_OUTPUT_PREFIX
    original_costed_nav_csv = live_mod.DEFAULT_COSTED_NAV_CSV
    original_strategy_title = getattr(live_mod, "STRATEGY_TITLE", None)
    try:
        live_mod.DEFAULT_OUTPUT_PREFIX = OUTPUT_PREFIX
        live_mod.DEFAULT_COSTED_NAV_CSV = COSTED_NAV_CSV
        live_mod.STRATEGY_TITLE = "Top100 Microcap Mom16 Biweekly v1.3 Strict Stop"
        yield live_mod
    finally:
        live_mod.DEFAULT_OUTPUT_PREFIX = original_output_prefix
        live_mod.DEFAULT_COSTED_NAV_CSV = original_costed_nav_csv
        if original_strategy_title is None:
            if hasattr(live_mod, "STRATEGY_TITLE"):
                delattr(live_mod, "STRATEGY_TITLE")
        else:
            live_mod.STRATEGY_TITLE = original_strategy_title


def _ensure_base_outputs() -> None:
    base_paths = v1_1_mod.base_mod.build_output_paths(v1_1_mod.base_mod.DEFAULT_OUTPUT_PREFIX)
    v1_1_mod.prepare_current_v1_1_outputs(paths=base_paths, costed_nav_csv=BASE_COSTED_NAV_CSV)
    has_base_turnover = base_paths["proxy_turnover"].exists()
    if has_base_turnover and BASE_COSTED_NAV_CSV.exists():
        return
    args = _build_v1_1_args()
    resolved_panel_path, target_end_date = v1_1_mod.base_mod.build_refreshed_panel_shadow(args, base_paths)
    v1_1_mod.base_mod.ensure_strategy_files(args, base_paths, resolved_panel_path, target_end_date)


def _load_reference_summary() -> dict[str, object]:
    if BASE_SUMMARY_JSON.exists():
        try:
            summary = json.loads(BASE_SUMMARY_JSON.read_text(encoding="utf-8"))
            if v1_1_mod.summary_is_current_v1_1(summary):
                return summary
        except Exception:
            pass
    if V1_0_SUMMARY_JSON.exists():
        return json.loads(V1_0_SUMMARY_JSON.read_text(encoding="utf-8"))
    raise FileNotFoundError("Neither current v1.1 summary nor v1.0 reference summary is available.")


def _build_signal_row(net_df: pd.DataFrame, reference_summary: dict[str, object]) -> pd.DataFrame:
    latest_row = net_df.iloc[-1]
    latest_signal = dict(reference_summary.get("latest_signal", {}))
    current_holding = str(latest_row.get("holding", latest_signal.get("current_holding", "cash")))
    next_holding = str(latest_row.get("next_holding", latest_signal.get("next_holding", current_holding)))
    latest_signal["current_holding"] = current_holding
    latest_signal["next_holding"] = next_holding
    latest_signal["trade_state"] = v1_1_mod.base_mod.compute_trade_state(current_holding, next_holding)
    latest_signal["momentum_trade_state"] = latest_signal["trade_state"]
    for src_col, dst_col in [
        ("microcap_close", "microcap_close"),
        ("hedge_close", "hedge_close"),
        ("microcap_mom", "microcap_mom"),
        ("hedge_mom", "hedge_mom"),
        ("momentum_gap", "momentum_gap"),
    ]:
        if src_col in latest_row:
            latest_signal[dst_col] = float(latest_row[src_col])
    latest_signal.setdefault("signal_label", next_holding)
    latest_signal["forced_stop_triggered"] = bool(latest_row.get("forced_stop_triggered", False))
    latest_signal["stop_loss_threshold"] = STRICT_STOP_THRESHOLD
    drawdown_value = latest_row.get("trade_drawdown_from_peak", 0.0)
    latest_signal["trade_drawdown_from_peak"] = 0.0 if pd.isna(drawdown_value) else float(drawdown_value)
    return pd.DataFrame([{**latest_signal, "date": pd.Timestamp(net_df.index.max())}])


def _build_base_summary(net_df: pd.DataFrame) -> tuple[dict[str, object], pd.DataFrame]:
    reference_summary = _load_reference_summary()
    signal_df = (
        pd.read_csv(BASE_SIGNAL_CSV)
        if BASE_SIGNAL_CSV.exists() and not net_df.empty and pd.Timestamp(net_df.index.max()).date() == pd.Timestamp(net_df.index.max()).date()
        else _build_signal_row(net_df, reference_summary)
    )
    signal_df = _build_signal_row(net_df, reference_summary)
    summary = dict(reference_summary)
    summary["strategy"] = "microcap_top100_mom16_biweekly_live_v1_1"
    summary["version"] = "1.1"
    summary["version_role"] = "backup_alternative"
    summary["version_note"] = (
        "Backup alternative to v1.0. Same live framework as v1.0, "
        "but fixed hedge ratio is reduced from 1.0x to 0.8x."
    )
    summary.setdefault("core_params", {})
    summary["core_params"]["fixed_hedge_ratio"] = BASE_HEDGE_RATIO
    summary["latest_trade_date"] = str(pd.Timestamp(net_df.index.max()).date())
    summary["latest_signal"] = signal_df.iloc[0].drop(labels=["date"], errors="ignore").to_dict()
    return summary, signal_df


def _load_base_v1_1_context() -> tuple[dict[str, object], pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    _ensure_base_outputs()
    args = _build_v1_1_args()
    base_paths = v1_1_mod.base_mod.build_output_paths(v1_1_mod.base_mod.DEFAULT_OUTPUT_PREFIX)
    panel_path, target_end_date = v1_1_mod.base_mod.refresh_history_anchor(args, base_paths)
    v1_1_mod.base_mod.ensure_strategy_nav_fresh(args, base_paths, panel_path, target_end_date)
    close_df = v1_1_mod.base_mod.load_close_df(panel_path, args.index_csv)
    gross = v1_1_mod.base_mod.run_signal(close_df).sort_index()
    turnover_df = pd.read_csv(base_paths["proxy_turnover"])
    if "rebalance_date" not in turnover_df.columns:
        raise KeyError(f"Column 'rebalance_date' not found in {base_paths['proxy_turnover']}.")
    turnover_df["rebalance_date"] = pd.to_datetime(turnover_df["rebalance_date"], errors="coerce")
    turnover_df = turnover_df.dropna(subset=["rebalance_date"]).sort_values("rebalance_date")
    reference_summary = _load_reference_summary()
    base_signal = _build_signal_row(v1_1_mod.base_mod.freq_mod.cost_mod.apply_cost_model(gross, turnover_df), reference_summary)
    return reference_summary, base_signal, gross, turnover_df


def summarize_returns(ret: pd.Series) -> dict[str, float | str | int]:
    ret = ret.fillna(0.0)
    nav = (1.0 + ret).cumprod()
    years = (ret.index[-1] - ret.index[0]).days / 365.25
    annual = nav.iloc[-1] ** (1.0 / years) - 1.0 if years > 0 else 0.0
    vol = ret.std(ddof=1) * (252**0.5)
    sharpe = annual / vol if vol > 0 else 0.0
    dd = nav / nav.cummax() - 1.0
    return {
        "start_date": str(pd.Timestamp(ret.index[0]).date()),
        "end_date": str(pd.Timestamp(ret.index[-1]).date()),
        "days": int(len(ret)),
        "final_nav": float(nav.iloc[-1]),
        "total_return_pct": float((nav.iloc[-1] - 1.0) * 100.0),
        "annual_pct": float(annual * 100.0),
        "max_drawdown_pct": float(dd.min() * 100.0),
        "sharpe": float(sharpe),
        "vol_pct": float(vol * 100.0),
    }


def summarize_yearly(ret: pd.Series) -> pd.DataFrame:
    rows = []
    for year, part in ret.groupby(ret.index.year):
        part = part.dropna()
        if part.empty:
            continue
        nav = (1.0 + part).cumprod()
        years = (part.index[-1] - part.index[0]).days / 365.25
        annual = nav.iloc[-1] ** (1.0 / years) - 1.0 if years > 0 else 0.0
        vol = part.std(ddof=1) * (252**0.5)
        sharpe = annual / vol if vol > 0 else 0.0
        dd = nav / nav.cummax() - 1.0
        rows.append(
            {
                "year": str(year),
                "start_date": str(pd.Timestamp(part.index[0]).date()),
                "end_date": str(pd.Timestamp(part.index[-1]).date()),
                "days": int(len(part)),
                "return_pct": float((nav.iloc[-1] - 1.0) * 100.0),
                "max_drawdown_pct": float(dd.min() * 100.0),
                "sharpe": float(sharpe),
                "annual_pct": float(annual * 100.0),
            }
        )
    return pd.DataFrame(rows)


def build_performance_payload(ret: pd.Series) -> dict[str, object]:
    summary = summarize_returns(ret)
    yearly_df = summarize_yearly(ret)
    yearly_df.to_csv(PERF_YEARLY_CSV, index=False, encoding="utf-8-sig")

    nav_df = pd.DataFrame(
        {
            "date": ret.index,
            "return_net": ret.values,
            "nav_net": (1.0 + ret.fillna(0.0)).cumprod().values,
        }
    )
    nav_df.to_csv(PERF_NAV_CSV, index=False, encoding="utf-8-sig")

    pd.DataFrame([summary]).to_csv(PERF_SUMMARY_CSV, index=False, encoding="utf-8-sig")

    plt.figure(figsize=(12, 6))
    plt.plot(nav_df["date"], nav_df["nav_net"], linewidth=2.0)
    plt.title("Top100 Microcap Mom16 Biweekly v1.3 Strict Stop")
    plt.ylabel("NAV")
    plt.grid(alpha=0.25)
    plt.tight_layout()
    plt.savefig(PERF_PNG, dpi=160)
    plt.close()

    payload = {
        "period_label": "full_sample",
        "source": "costed_v1_3",
        "start_date": summary["start_date"],
        "end_date": summary["end_date"],
        "summary": summary,
        "yearly": yearly_df.to_dict(orient="records"),
        "files": {
            "summary_csv": str(PERF_SUMMARY_CSV),
            "yearly_csv": str(PERF_YEARLY_CSV),
            "nav_csv": str(PERF_NAV_CSV),
            "chart_png": str(PERF_PNG),
        },
    }
    PERF_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return payload


def generate_v1_3_outputs() -> tuple[dict[str, object], pd.DataFrame, pd.DataFrame]:
    invalidate_incompatible_v1_3_outputs()
    reference_summary, _, gross, turnover_df = _load_base_v1_1_context()
    out = v1_1_mod.base_mod.apply_peak_drawdown_forced_stop_loss(
        gross_result=gross,
        turnover_df=turnover_df,
        stop_loss_threshold=STRICT_STOP_THRESHOLD,
    )
    out.to_csv(COSTED_NAV_CSV, encoding="utf-8-sig")
    out.reset_index().to_csv(NAV_CSV, index=False, encoding="utf-8-sig")

    signal_row = _build_signal_row(out, reference_summary)
    signal_row["version"] = "1.3"
    signal_row["base_version"] = "1.1"
    signal_row["stop_loss_type"] = "peak_drawdown_strict"
    signal_row["stop_loss_threshold"] = STRICT_STOP_THRESHOLD
    signal_row["fixed_hedge_ratio"] = BASE_HEDGE_RATIO
    LATEST_SIGNAL_CSV.write_text(signal_row.to_csv(index=False), encoding="utf-8")

    perf_payload = build_performance_payload(out["return_net"].fillna(0.0))

    summary = dict(reference_summary)
    summary["strategy"] = OUTPUT_PREFIX
    summary["version"] = "1.3"
    summary["version_role"] = EXPECTED_VERSION_ROLE
    summary["version_note"] = (
        "Strict stop alternative. Same as v1.1 (0.8x hedge), "
        "plus peak-drawdown strict 1% stop."
    )
    summary.setdefault("core_params", {})
    summary["core_params"]["fixed_hedge_ratio"] = BASE_HEDGE_RATIO
    summary["core_params"]["peak_drawdown_stop"] = {
        "type": "strict",
        "threshold": STRICT_STOP_THRESHOLD,
        "timing_rule": "T close drawdown observed, T close exit applied",
    }
    summary["latest_trade_date"] = str(pd.Timestamp(signal_row.iloc[0]["date"]).date())
    summary["latest_nav_date"] = str(pd.Timestamp(out.index.max()).date())
    summary["latest_signal"] = signal_row.iloc[0].drop(labels=["date"], errors="ignore").to_dict()
    summary["performance_snapshot"] = perf_payload["summary"]
    summary["base_fingerprint"] = current_base_fingerprint()
    SUMMARY_JSON.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    return summary, signal_row, out


def run_live_member_query(query: str) -> None:
    with patched_live_module() as live_mod:
        args = live_mod.parse_args()
        context = live_mod.build_base_context(args, include_members=True)
        paths = context["paths"]
        latest_rebalance = pd.Timestamp(context["latest_rebalance"])
        rebalance_effective_date = context.get("rebalance_effective_date")

        if query == "成分股":
            members = context["target_members"]
            members.to_csv(paths["members"], index=False, encoding="utf-8")
            print("最新成分股")
            print(f"信号日: {latest_rebalance.date()}")
            print(
                "生效日: {}".format(
                    "暂无下一交易日"
                    if rebalance_effective_date is None
                    else pd.Timestamp(rebalance_effective_date).date()
                )
            )
            print(
                live_mod.format_table(
                    members[["rank", "symbol", "name", "market_cap", "target_weight", "signal_date", "effective_date"]],
                    max_rows=live_mod.TOP_N,
                )
            )
            print(f"已保存: {paths['members'].name}")
            return

        if query == "进出名单":
            changes = context["changes_df"]
            changes.to_csv(paths["changes"], index=False, encoding="utf-8")
            print("最新进出名单")
            print(f"信号日: {latest_rebalance.date()}")
            print(
                "生效日: {}".format(
                    "暂无下一交易日"
                    if rebalance_effective_date is None
                    else pd.Timestamp(rebalance_effective_date).date()
                )
            )
            print(live_mod.format_table(changes))
            print(f"已保存: {paths['changes'].name}")
            return

        if query == "实时进出名单":
            live_mod.ensure_realtime_anchor_is_fresh(context, args)
            try:
                realtime_state = live_mod.compute_realtime_state_fast(
                    context,
                    args.realtime_cache_seconds,
                    args.capital,
                    allow_stale_anchor=args.allow_stale_realtime,
                )
            except Exception:
                realtime_state = live_mod.compute_realtime_state(
                    context,
                    args.realtime_cache_seconds,
                    args.capital,
                    allow_stale_anchor=args.allow_stale_realtime,
                )
            realtime_members = realtime_state["members"]
            changes = realtime_state["changes"]
            realtime_members.to_csv(paths["realtime_members"], index=False, encoding="utf-8")
            changes.to_csv(paths["realtime_changes"], index=False, encoding="utf-8")
            print("实时进出名单")
            print(live_mod.format_table(changes))
            print(f"已保存: {paths['realtime_changes'].name}")
            return

    raise ValueError("V1.3 当前仅支持: 成分股 / 进出名单 / 实时进出名单")


def _print_confirmed_signal_query() -> None:
    summary, signal_df, _ = generate_v1_3_outputs()
    row = signal_df.iloc[0]
    print("确认信号")
    print("策略版本: v1.3（严格止损版）")
    print("基础版本: v1.1（0.8x hedge）")
    print(f"止损规则: 峰值回撤严格 {_format_pct(STRICT_STOP_THRESHOLD)}")
    print(f"当前状态: {_render_holding(str(row['next_holding']))}")
    print(f"仓位动作: {_render_trade_state(str(row.get('trade_state', 'hold')))}")
    print(f"信号日期: {pd.Timestamp(row['date']).strftime('%Y-%m-%d')}")
    print("关键指标")
    print(f"- 微盘收盘: {_format_num(row['microcap_close'])}")
    print(f"- 对冲收盘: {_format_num(row['hedge_close'])}")
    print(f"- 微盘动量: {_format_pct(row['microcap_mom'])}")
    print(f"- 对冲动量: {_format_pct(row['hedge_mom'])}")
    print(f"- 动量差: {_format_pct(row['momentum_gap'])}")
    print(f"- 当前单笔峰值回撤: {_format_pct(row.get('trade_drawdown_from_peak', 0.0))}")
    print(f"- 当日是否触发强平: {'是' if bool(row.get('forced_stop_triggered', False)) else '否'}")
    print("调仓快照")
    print(f"- 最新调仓日: {summary['latest_rebalance_date']}")
    print(f"- 名单变动数量: 调入 {int(row.get('member_enter_count', 0))} / 调出 {int(row.get('member_exit_count', 0))}")
    print(f"已保存: {LATEST_SIGNAL_CSV.name}")
    print(f"已保存: {SUMMARY_JSON.name}")


def _print_performance_query(query: str) -> None:
    generate_v1_3_outputs()
    with patched_live_module() as live_mod:
        perf_df = pd.read_csv(COSTED_NAV_CSV, parse_dates=["date"]).sort_values("date").set_index("date")
        live_mod.build_performance_outputs(
            perf_df=perf_df,
            ret_col="return_net",
            nav_col="nav_net",
            source_label="costed_v1_3",
            query_text=query,
            paths={
                "performance_summary": PERF_SUMMARY_CSV,
                "performance_yearly": PERF_YEARLY_CSV,
                "performance_nav": PERF_NAV_CSV,
                "performance_chart": PERF_PNG,
                "performance_json": PERF_JSON,
            },
        )
        summary = pd.read_csv(PERF_SUMMARY_CSV)
        yearly = pd.read_csv(PERF_YEARLY_CSV)
        print("表现汇总")
        print(live_mod.format_table(summary))
        print("年度分解")
        print(live_mod.format_table(yearly, max_rows=30))
        print(f"已保存: {PERF_PNG.name}")
        print(f"已保存: {PERF_SUMMARY_CSV.name}")
        print(f"已保存: {PERF_YEARLY_CSV.name}")
        print(f"已保存: {PERF_NAV_CSV.name}")
        print(f"已保存: {PERF_JSON.name}")


def _handle_query(query: str) -> None:
    if query in LIVE_MEMBER_QUERIES:
        run_live_member_query(query)
        return
    if query == "信号":
        _print_confirmed_signal_query()
        return
    if _is_performance_query(query):
        _print_performance_query(query)
        return
    raise ValueError("V1.3 支持: 信号 / 成分股 / 进出名单 / 实时进出名单 / 表现 <区间>")


def main() -> None:
    query = " ".join(sys.argv[1:]).strip()
    if query:
        _handle_query(query)
        return

    generate_v1_3_outputs()
    print(str(SUMMARY_JSON))
    print(str(LATEST_SIGNAL_CSV))
    print(str(COSTED_NAV_CSV))


if __name__ == "__main__":
    main()
