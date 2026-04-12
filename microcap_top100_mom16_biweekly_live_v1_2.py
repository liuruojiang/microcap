from __future__ import annotations

import contextlib
import json
import sys
from pathlib import Path

import matplotlib
import pandas as pd

matplotlib.use("Agg")
import matplotlib.pyplot as plt

import microcap_top100_mom16_biweekly_live_v1_1 as v1_1_mod

from analyze_top100_mom16_v1_1_nav_throttle_practical import (
    PracticalThrottleConfig,
    apply_practical_throttle,
    target_scale_from_drawdown,
)


ROOT = Path(__file__).resolve().parent
OUTPUT_DIR = ROOT / "outputs"

BASE_SUMMARY_JSON = OUTPUT_DIR / "microcap_top100_mom16_biweekly_live_v1_1_summary.json"
BASE_SIGNAL_CSV = OUTPUT_DIR / "microcap_top100_mom16_biweekly_live_v1_1_latest_signal.csv"
BASE_LIVE_NAV_CSV = OUTPUT_DIR / "microcap_top100_mom16_biweekly_live_v1_1_nav.csv"
BASE_COSTED_NAV_CSV = OUTPUT_DIR / "microcap_top100_mom16_hedge_zz1000_0p8x_biweekly_thursday_16y_costed_nav.csv"

OUTPUT_PREFIX = "microcap_top100_mom16_biweekly_live_v1_2"
SUMMARY_JSON = OUTPUT_DIR / f"{OUTPUT_PREFIX}_summary.json"
LATEST_SIGNAL_CSV = OUTPUT_DIR / f"{OUTPUT_PREFIX}_latest_signal.csv"
NAV_CSV = OUTPUT_DIR / f"{OUTPUT_PREFIX}_nav.csv"
COSTED_NAV_CSV = OUTPUT_DIR / "microcap_top100_mom16_hedge_zz1000_0p8x_nav4_8_biweekly_thursday_16y_costed_nav.csv"
PERF_SUMMARY_CSV = OUTPUT_DIR / f"{OUTPUT_PREFIX}_performance_summary.csv"
PERF_YEARLY_CSV = OUTPUT_DIR / f"{OUTPUT_PREFIX}_performance_yearly.csv"
PERF_NAV_CSV = OUTPUT_DIR / f"{OUTPUT_PREFIX}_performance_nav.csv"
PERF_JSON = OUTPUT_DIR / f"{OUTPUT_PREFIX}_performance_summary.json"
PERF_PNG = OUTPUT_DIR / f"{OUTPUT_PREFIX}_performance_curve.png"

LIVE_MEMBER_QUERIES = {"成分股", "进出名单", "实时进出名单"}
SIGNAL_QUERIES = {"信号", "实时信号"}
BASE_HEDGE_RATIO = 0.8

CFG = PracticalThrottleConfig(
    dd_moderate=0.04,
    dd_severe=0.08,
    scale_moderate=0.85,
    scale_severe=0.70,
    recover_dd=0.03,
    rebal_cost_bps=2.0,
)


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


def _render_tail_risk(value: str) -> str:
    return {
        "normal": "正常",
        "caution": "注意",
        "warning": "警告",
    }.get(str(value), str(value))


def _is_performance_query(query: str) -> bool:
    return bool(v1_1_mod.base_mod.PERFORMANCE_PATTERN.search(query))


@contextlib.contextmanager
def patched_live_module():
    live_mod = v1_1_mod.base_mod
    original_output_prefix = live_mod.DEFAULT_OUTPUT_PREFIX
    original_costed_nav_csv = live_mod.DEFAULT_COSTED_NAV_CSV
    original_strategy_title = getattr(live_mod, "STRATEGY_TITLE", None)
    try:
        live_mod.DEFAULT_OUTPUT_PREFIX = OUTPUT_PREFIX
        live_mod.DEFAULT_COSTED_NAV_CSV = COSTED_NAV_CSV
        live_mod.STRATEGY_TITLE = "Top100 Microcap Mom16 Biweekly v1.2 Defensive"
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
    has_base_turnover = base_paths["proxy_turnover"].exists()
    if BASE_SUMMARY_JSON.exists() and BASE_SIGNAL_CSV.exists() and BASE_COSTED_NAV_CSV.exists():
        try:
            summary = json.loads(BASE_SUMMARY_JSON.read_text(encoding="utf-8"))
            core_params = summary.get("core_params", {}) if isinstance(summary, dict) else {}
            # v1.2 must not reuse a costed base series that cannot be rebuilt from turnover history.
            if (
                has_base_turnover
                and core_params.get("research_stack_version") == v1_1_mod.base_mod.RESEARCH_STACK_VERSION
            ):
                return
        except Exception:
            pass
    old_argv = sys.argv[:]
    try:
        sys.argv = [sys.argv[0]]
        v1_1_mod.base_mod.main()
    finally:
        sys.argv = old_argv


def _compute_nav_control_state(base_returns: pd.Series) -> dict[str, object]:
    throttle_run = apply_practical_throttle(base_returns.fillna(0.0), CFG)
    nav = (1.0 + throttle_run["return"].fillna(0.0)).cumprod()
    last_scale = float(throttle_run["scale"].iloc[-1])
    peak = float(nav.cummax().iloc[-1])
    current_dd = float(nav.iloc[-1] / peak - 1.0) if peak > 0 else 0.0
    next_scale, next_state = target_scale_from_drawdown(current_dd, CFG, last_scale)
    return {
        "throttle_run": throttle_run,
        "nav": nav,
        "last_scale": last_scale,
        "last_state": str(throttle_run["state"].iloc[-1]),
        "current_dd": current_dd,
        "next_scale": float(next_scale),
        "next_state": str(next_state),
    }


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
    plt.title("Top100 Microcap Mom16 Biweekly v1.2 Defensive")
    plt.ylabel("NAV")
    plt.grid(alpha=0.25)
    plt.tight_layout()
    plt.savefig(PERF_PNG, dpi=160)
    plt.close()

    payload = {
        "period_label": "full_sample",
        "source": "costed",
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


def generate_v1_2_outputs() -> tuple[dict[str, object], pd.DataFrame, pd.DataFrame]:
    _ensure_base_outputs()
    base_summary = json.loads(BASE_SUMMARY_JSON.read_text(encoding="utf-8"))
    base_signal = pd.read_csv(BASE_SIGNAL_CSV)
    base_nav_path = BASE_COSTED_NAV_CSV
    base_net = pd.read_csv(base_nav_path, parse_dates=["date"]).sort_values("date").set_index("date")
    if "return_net" not in base_net.columns:
        raise KeyError(f"Expected costed return column 'return_net' in {base_nav_path}.")
    base_ret_col = "return_net"

    control = _compute_nav_control_state(base_net[base_ret_col].fillna(0.0))
    throttle_run = control["throttle_run"]

    out = base_net.copy()
    out["nav_control_scale"] = throttle_run["scale"]
    out["nav_control_state"] = throttle_run["state"]
    out["nav_control_prev_drawdown"] = throttle_run["prev_drawdown"]
    out["nav_control_turnover"] = throttle_run["turnover"]
    out["nav_control_cost"] = throttle_run["cost"]
    out["return_net_v1_2"] = throttle_run["return"]
    out["nav_net_v1_2"] = (1.0 + out["return_net_v1_2"].fillna(0.0)).cumprod()
    out.to_csv(COSTED_NAV_CSV, encoding="utf-8-sig")
    out.reset_index().to_csv(NAV_CSV, index=False, encoding="utf-8-sig")

    signal_row = base_signal.iloc[[0]].copy()
    current_holding = str(signal_row.iloc[0]["current_holding"])
    next_holding = str(signal_row.iloc[0]["next_holding"])
    signal_row["version"] = "1.2"
    signal_row["base_version"] = "1.1"
    signal_row["nav_control_scale_last_applied"] = control["last_scale"]
    signal_row["nav_control_scale_next_session"] = control["next_scale"]
    signal_row["nav_control_state_last_applied"] = control["last_state"]
    signal_row["nav_control_state_next_session"] = control["next_state"]
    signal_row["nav_control_drawdown_last_close"] = control["current_dd"]
    signal_row["nav_control_config"] = "4/8 + 0.85/0.70 + recover 3%"
    signal_row["effective_hedge_ratio_last_applied"] = (
        BASE_HEDGE_RATIO * control["last_scale"] if current_holding != "cash" else 0.0
    )
    signal_row["effective_hedge_ratio_next_session"] = (
        BASE_HEDGE_RATIO * control["next_scale"] if next_holding != "cash" else 0.0
    )
    LATEST_SIGNAL_CSV.write_text(signal_row.to_csv(index=False), encoding="utf-8")

    perf_payload = build_performance_payload(out["return_net_v1_2"].fillna(0.0))

    summary = dict(base_summary)
    summary["strategy"] = OUTPUT_PREFIX
    summary["version"] = "1.2"
    summary["version_role"] = "defensive_alternative"
    summary["version_note"] = (
        "Defensive backup alternative. Same as v1.1 (0.8x hedge), "
        "plus practical NAV throttle 4/8 with 0.85x/0.70x scale and recover 3%."
    )
    summary["core_params"]["fixed_hedge_ratio"] = BASE_HEDGE_RATIO
    summary["core_params"]["nav_control"] = {
        "type": "practical_nav_throttle",
        "dd_moderate": 0.04,
        "dd_severe": 0.08,
        "scale_moderate": 0.85,
        "scale_severe": 0.70,
        "recover_dd": 0.03,
        "timing_rule": "T close drawdown observed, T+1 scale applied",
    }
    summary["latest_trade_date"] = str(pd.Timestamp(signal_row.iloc[0]["date"]).date())
    summary["latest_nav_date"] = str(pd.Timestamp(out.index.max()).date())
    summary["latest_signal"]["nav_control_scale_last_applied"] = control["last_scale"]
    summary["latest_signal"]["nav_control_scale_next_session"] = control["next_scale"]
    summary["latest_signal"]["nav_control_state_last_applied"] = control["last_state"]
    summary["latest_signal"]["nav_control_state_next_session"] = control["next_state"]
    summary["latest_signal"]["nav_control_drawdown_last_close"] = control["current_dd"]
    summary["latest_signal"]["effective_hedge_ratio_last_applied"] = (
        BASE_HEDGE_RATIO * control["last_scale"] if current_holding != "cash" else 0.0
    )
    summary["latest_signal"]["effective_hedge_ratio_next_session"] = (
        BASE_HEDGE_RATIO * control["next_scale"] if next_holding != "cash" else 0.0
    )
    summary["performance_snapshot"] = perf_payload["summary"]
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
                    members[
                        ["rank", "symbol", "name", "market_cap", "target_weight", "signal_date", "effective_date"]
                    ],
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
            quote_source = realtime_state["meta"]["quote_source"]
            snapshot_time = realtime_state["meta"].get("snapshot_time")
            cache_age_seconds = float(realtime_state.get("cache_age_seconds", 0.0))
            realtime_members.to_csv(paths["realtime_members"], index=False, encoding="utf-8")
            changes.to_csv(paths["realtime_changes"], index=False, encoding="utf-8")
            print("实时进出名单")
            print(f"基准调仓信号日: {latest_rebalance.date()}")
            print(
                "静态名单生效日: {}".format(
                    "暂无下一交易日"
                    if rebalance_effective_date is None
                    else pd.Timestamp(rebalance_effective_date).date()
                )
            )
            if snapshot_time:
                print(f"实时快照时间: {snapshot_time}")
            print(f"实时价格来源: {quote_source}")
            print(f"结果来源: {'cache' if realtime_state['from_cache'] else 'fresh'}")
            print(f"实时结果年龄: {cache_age_seconds:.1f} 秒")
            print(live_mod.format_table(changes))
            print(f"已保存: {paths['realtime_changes'].name}")
            return

    raise ValueError("V1.2 当前仅支持 成分股 / 进出名单 / 实时进出名单")


def _print_confirmed_signal_query() -> None:
    summary, signal_df, _ = generate_v1_2_outputs()
    row = signal_df.iloc[0]
    print("确认信号")
    print("策略版本: v1.2（备选防守版）")
    print("固定对冲比: 0.8x")
    print("风控附加层: NAV 节流 4/8 -> 0.85/0.70，回撤修复 3%")
    print(f"当前状态: {_render_holding(str(row['next_holding']))}")
    print(f"仓位动作（动量信号）: {_render_trade_state(str(row.get('momentum_trade_state', row.get('trade_state', 'hold'))))}")
    print(f"名单动作（双周换仓）: {str(row.get('member_rebalance_label', '名单不变'))}")
    print(f"信号日期: {pd.Timestamp(row['date']).strftime('%Y-%m-%d')}")
    print(f"当前已生效对冲比: {float(row['effective_hedge_ratio_last_applied']):.2f}x")
    print(f"下一交易日对冲比: {float(row['effective_hedge_ratio_next_session']):.2f}x")
    print(
        "NAV 节流状态: {} -> {}".format(
            row["nav_control_state_last_applied"],
            row["nav_control_state_next_session"],
        )
    )
    print(f"收盘回撤: {_format_pct(row['nav_control_drawdown_last_close'])}")
    print("关键指标")
    print(f"- 微盘收盘: {_format_num(row['microcap_close'])}")
    print(f"- 对冲收盘: {_format_num(row['hedge_close'])}")
    print(f"- 微盘动量: {_format_pct(row['microcap_mom'])}")
    print(f"- 对冲动量: {_format_pct(row['hedge_mom'])}")
    print(f"- 动量差: {_format_pct(row['momentum_gap'])}")
    print("调仓快照")
    print(f"- 最新调仓日: {summary['latest_rebalance_date']}")
    print(f"- 名单变动数量: 调入 {int(row.get('member_enter_count', 0))} / 调出 {int(row.get('member_exit_count', 0))}")
    print(f"已保存: {LATEST_SIGNAL_CSV.name}")
    print(f"已保存: {SUMMARY_JSON.name}")


def _build_v1_2_realtime_row() -> tuple[pd.Series, dict[str, object], dict[str, object], dict[str, object]]:
    with patched_live_module() as live_mod:
        args = live_mod.parse_args()
        context = live_mod.build_base_context(args, include_members=True)
        live_mod.ensure_realtime_anchor_is_fresh(context, args)
        try:
            signal_df, meta = live_mod.build_realtime_signal_fast(context)
        except Exception:
            signal_df, meta = live_mod.build_realtime_signal(context, args.realtime_cache_seconds)
        signal_df = live_mod.augment_signal_with_member_rebalance(signal_df, context.get("changes_df"))

        snapshot_time = pd.Timestamp(meta["snapshot_time"])
        rt_close_df = context["close_df"].copy()
        rt_close_df.loc[snapshot_time, ["microcap", "hedge"]] = [
            float(meta["microcap_rt_close"]),
            float(meta["hedge_rt_close"]),
        ]
        rt_close_df = rt_close_df.sort_index()
        rt_result = live_mod.run_signal(rt_close_df)
        ret_col = "return_net" if "return_net" in rt_result.columns else "return"
        control = _compute_nav_control_state(rt_result[ret_col].fillna(0.0))

        row = signal_df.iloc[0].copy()
        current_holding = str(row.get("current_holding", rt_result.iloc[-1]["holding"]))
        next_holding = str(row["next_holding"])
        row["version"] = "1.2"
        row["base_version"] = "1.1"
        row["nav_control_scale_last_applied"] = control["last_scale"]
        row["nav_control_state_last_applied"] = control["last_state"]
        row["nav_control_drawdown_last_close"] = control["current_dd"]
        row["nav_control_scale_next_session"] = control["next_scale"]
        row["nav_control_state_next_session"] = control["next_state"]
        row["nav_control_config"] = "4/8 + 0.85/0.70 + recover 3%"
        row["effective_hedge_ratio_last_applied"] = (
            BASE_HEDGE_RATIO * control["last_scale"] if current_holding != "cash" else 0.0
        )
        row["effective_hedge_ratio_next_session"] = (
            BASE_HEDGE_RATIO * control["next_scale"] if next_holding != "cash" else 0.0
        )
        return row, meta, context, control


def _print_realtime_signal_query() -> None:
    row, meta, context, _ = _build_v1_2_realtime_row()
    realtime_signal_path = context["paths"]["realtime_signal"]
    pd.DataFrame([row]).to_csv(realtime_signal_path, index=False, encoding="utf-8")
    print("实时信号")
    print("策略版本: v1.2（备选防守版）")
    print("固定对冲比: 0.8x")
    print("风控附加层: NAV 节流 4/8 -> 0.85/0.70，回撤修复 3%")
    print(f"当前状态: {_render_holding(str(row['next_holding']))}")
    print(f"仓位动作（动量信号）: {_render_trade_state(str(row.get('momentum_trade_state', row.get('trade_state', 'hold'))))}")
    print(f"名单动作（双周换仓）: {str(row.get('member_rebalance_label', '名单不变'))}")
    print(f"快照时间: {pd.Timestamp(row['date']).strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"当前会话已生效对冲比: {float(row['effective_hedge_ratio_last_applied']):.2f}x")
    print(f"若当前收盘，下个交易日预计对冲比: {float(row['effective_hedge_ratio_next_session']):.2f}x")
    print(
        "NAV 节流状态: {} -> {}".format(
            row["nav_control_state_last_applied"],
            row["nav_control_state_next_session"],
        )
    )
    print(f"当前估算收盘回撤: {_format_pct(row['nav_control_drawdown_last_close'])}")
    print("关键指标")
    print(f"- 微盘估算价格: {_format_num(row['microcap_close'])}")
    print(f"- 对冲实时价格: {_format_num(row['hedge_close'])}")
    print(f"- 微盘动量: {_format_pct(row['microcap_mom'])}")
    print(f"- 对冲动量: {_format_pct(row['hedge_mom'])}")
    print(f"- 动量差: {_format_pct(row['momentum_gap'])}")
    print(f"- 尾盘抖动风险: {_render_tail_risk(str(row.get('tail_jitter_risk', 'normal')))}")
    note = str(row.get("tail_jitter_note", "") or "").strip()
    if note:
        print(f"- 风险提示: {note}")
    print("实时数据")
    print(f"- 成分股有效报价: {int(row.get('member_price_count', 0))} / {int(row.get('member_count', 0))}")
    print(f"- 历史锚点交易日: {row['latest_anchor_trade_date']}")
    print(f"- 微盘行情来源: {meta['quote_source']}")
    print(f"- 对冲行情来源: {meta['hedge_quote_source']}")
    print("调仓快照")
    print(f"- 最新调仓日: {context['latest_rebalance']}")
    print(f"- 当前生效名单: {context['effective_rebalance']}")
    print(f"- 调仓生效日: {context['rebalance_effective_date']}")


    print(f"已保存: {realtime_signal_path.name}")


def _print_performance_query(query: str) -> None:
    generate_v1_2_outputs()
    with patched_live_module() as live_mod:
        perf_df = pd.read_csv(COSTED_NAV_CSV, parse_dates=["date"]).sort_values("date").set_index("date")
        ret_col = "return_net_v1_2" if "return_net_v1_2" in perf_df.columns else "return_net"
        nav_col = "nav_net_v1_2" if "nav_net_v1_2" in perf_df.columns else "nav_net"
        live_mod.build_performance_outputs(
            perf_df=perf_df,
            ret_col=ret_col,
            nav_col=nav_col,
            source_label="costed_v1_2",
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
    if query == "实时信号":
        _print_realtime_signal_query()
        return
    if _is_performance_query(query):
        _print_performance_query(query)
        return
    raise ValueError("V1.2 支持: 信号 / 实时信号 / 成分股 / 进出名单 / 实时进出名单 / 表现 <区间>")


def main() -> None:
    query = " ".join(sys.argv[1:]).strip()
    if query:
        _handle_query(query)
        return

    generate_v1_2_outputs()
    print(str(SUMMARY_JSON))
    print(str(LATEST_SIGNAL_CSV))
    print(str(COSTED_NAV_CSV))


if __name__ == "__main__":
    main()
