# /// script
# requires-python = ">=3.11"
# dependencies = ["numpy", "pandas"]
# ///
"""Fresh paired comparison, not a cost test or production parameter change."""
import hashlib
import importlib.util
import json
import subprocess
import sys
from pathlib import Path

import numpy as np
import pandas as pd

RUN = Path(__file__).resolve().parent
ROOT = RUN.parents[1]
HELPER = ROOT / "quant_param_scan_runs/20260904_v20_target_vol_ablation/run_target_vol_ablation.py"
spec = importlib.util.spec_from_file_location("ablation_helpers", HELPER)
helper = importlib.util.module_from_spec(spec)
spec.loader.exec_module(helper)
v2, delivery, prior = helper.v2, helper.delivery, helper.layer4.prior


def save(name, value):
    (RUN / name).write_text(json.dumps(value, ensure_ascii=False, indent=2, default=str), encoding="utf-8")


def build(gross, turnover, original):
    buffered = v2.base_mod.apply_momentum_gap_exit_buffer(gross, .003 if original else 0.)
    if original:
        overheat = v2.overlay_mod.apply_volatility_overheat_exit(buffered, turnover, overheat_window=60, overheat_threshold=.23)
        return v2.overlay_mod.apply_target_vol_scaling(overheat)
    return helper.build(v2.base_mod.apply_momentum_gap_no_peak_decay_cost_model(buffered, turnover), "off")


def main():
    status_before = subprocess.check_output(["git", "status", "--short"], cwd=ROOT, text=True)
    with (RUN / "delivery_check.json").open("w", encoding="utf-8") as handle:
        subprocess.run([sys.executable, "-X", "utf8", "scripts/top100_delivery.py", "check"], cwd=ROOT,
                       stdout=handle, check=True, timeout=180)
    check = json.loads((RUN / "delivery_check.json").read_text(encoding="utf-8"))
    target = pd.Timestamp(check["expected_date"])
    before = delivery.inspect_outputs(ROOT, str(target.date()))
    assert delivery.validate_manifest(ROOT, before)["ok"]
    save("freshness_before.json", before)
    manifest = json.loads((ROOT / "outputs/top100_delivery_manifest.json").read_text(encoding="utf-8"))
    save("delivery_manifest_snapshot.json", manifest)
    paths = [Path(__file__), HELPER, Path(helper.layer4.__file__), Path(prior.__file__), prior.LAYER1 / "run_scan.py"]
    hashes = {str(p.relative_to(ROOT)): delivery.sha(p) for p in paths}
    v2._sync_embedded_base_config()
    base = v2.base_mod
    assert base.LOOKBACK == 16 and base.FIXED_HEDGE_RATIO == .8 and not base.REQUIRE_POSITIVE_MICROCAP_MOM
    ns = v2.overlay_mod.apply_target_vol_scaling.__globals__
    params = {key: ns[key] for key in ["TARGET_VOL", "TARGET_VOL_WINDOW", "TARGET_VOL_MAX_LEVERAGE", "TARGET_VOL_SCALE_REBALANCE_THRESHOLD", "TARGET_VOL_FINANCING_RATE", "IDLE_CASH_YIELD"]}
    assert list(params.values()) == [.15, 75, 1.5, .10, .03, .02]
    all_close = base.load_close_df(ROOT / "outputs" / delivery.BASE_PANEL,
                                  ROOT / "outputs" / delivery.BASE_FILES["proxy_index"], max_date=target)
    initial = base.run_signal(all_close).sort_index()
    close = initial[["microcap_close", "hedge_close"]].rename(columns={"microcap_close": "microcap", "hedge_close": "hedge"})
    turnover = pd.read_csv(ROOT / "outputs" / delivery.BASE_FILES["proxy_turnover"], parse_dates=["rebalance_date"])
    close.to_csv(RUN / "frozen_close.csv", index_label="date")
    turnover.to_csv(RUN / "frozen_turnover.csv", index=False)
    member = prior.member_st_audit(ROOT / "outputs" / delivery.BASE_FILES["proxy_members"])
    assert member["st_violations"] == 0
    save("member_data_quality.json", member)
    with prior.layer1.lookback(16):
        gross = base.run_signal(close).sort_index()
    frames = {"original_v20": build(gross, turnover, True), "plain16_fixed1": build(gross, turnover, False)}
    official = pd.read_csv(v2.COSTED_NAV_CSV, parse_dates=["date"]).set_index("date")
    parity = prior.compare(frames["original_v20"], official, "fresh_official_v20",
                           ["return_net", "nav_net", "holding", "next_holding", "current_execution_scale", "financing_cost", "idle_cash_yield", "scale_change_cost"])
    assert frames["plain16_fixed1"].index.equals(official.index)
    freshness, rows = [], []
    native_start = official.index.min()
    for name, frame in frames.items():
        active, nxt = frame.holding.ne("cash"), frame.next_holding.ne("cash")
        assert np.isfinite(frame.return_net).all() and frame.index.max() == target
        assert np.array_equal(active.iloc[1:].to_numpy(), nxt.iloc[:-1].to_numpy())
        assert frame.return_net.loc[~active & ~nxt].eq(0).all()
        if name == "plain16_fixed1":
            assert frame.current_execution_scale.loc[active].eq(1).all()
            assert frame.financing_cost.eq(0).all() and frame.scale_change_cost.eq(0).all()
            assert np.allclose(frame.return_net, (1+frame.overlay_pre_cost_return)*(1-frame.total_cost)-1, atol=1e-12, rtol=0)
        for count in (500, len(gross)-250):
            short = build(gross.iloc[:count], turnover, name == "original_v20")
            parity += prior.compare(short, frame.loc[short.index], name+f"_prefix{count}", ["return_net", "nav_net", "holding", "next_holding", "current_execution_scale"])
        path = RUN / (name + ".csv.gz")
        frame.to_csv(path, index_label="date", compression="gzip")
        back = pd.read_csv(path, parse_dates=["date"]).set_index("date")
        parity += prior.compare(back, frame, name+"_readback", ["return_net", "nav_net", "holding", "next_holding"])
        freshness.append({"candidate": name, "start": str(back.index.min().date()), "latest_date": str(back.index.max().date()), "rows": len(back), "sha256_bytes": hashlib.sha256(path.read_bytes()).hexdigest()})
        for caliber, start in [("native_full", native_start), ("previous_research_common", pd.Timestamp("2010-07-27"))]:
            common = frame.loc[start:]
            for segment, years in helper.layer4.WINDOWS.items():
                part = common if years is None else common.loc[common.index >= target-pd.DateOffset(years=years)]
                m = base.hedge_mod.calc_metrics(part.return_net)
                rows.append({"candidate": name, "caliber": caliber, "segment": segment, "start": str(part.index.min().date()),
                             "end": str(part.index.max().date()), "rows": len(part), "ann_return": m.annual, "ann_vol": m.vol,
                             "sharpe_repo": m.sharpe, "max_dd": m.max_dd, "total_return": m.total_return,
                             "holding_day_ratio": part.holding.ne("cash").mean(), "avg_execution_scale": part.current_execution_scale.mean()})
    s = pd.DataFrame(rows)
    for (caliber, segment), group in s.groupby(["caliber", "segment"]):
        ref = group.loc[group.candidate.eq("original_v20")].iloc[0]
        s.loc[group.index, "annual_return_delta_pp"] = (group.ann_return-ref.ann_return)*100
        s.loc[group.index, "drawdown_improvement_pp"] = (group.max_dd-ref.max_dd)*100
    s.to_csv(RUN / "comparison_metrics.csv", index=False)
    pd.DataFrame(parity).to_csv(RUN / "parity_checks.csv", index=False)
    save("candidate_freshness.json", freshness)
    after = delivery.inspect_outputs(ROOT, str(target.date()))
    assert before["inputs"] == after["inputs"] and before["artifacts"] == after["artifacts"]
    assert delivery.validate_manifest(ROOT, after)["ok"]
    assert hashes == {str(p.relative_to(ROOT)): delivery.sha(p) for p in paths}
    save("freshness_after.json", after)
    save("comparison_meta.json", {"source_change_rule": "research_only_no_production_changes", "git_status_before": status_before,
         "git_status_after": subprocess.check_output(["git", "status", "--short"], cwd=ROOT, text=True),
         "source_hashes": before["inputs"], "harness_hashes": hashes, "original_target_vol_params": params,
         "native_start": native_start, "previous_research_start": "2010-07-27", "target_date": target,
         "member_caveat": member, "refresh_release_sha": manifest["release_sha"], "refresh_verified_at": manifest["verified_at"],
         "state_cash_cost_prefix_parity_pass": True, "comparison_is_not_a_cost_sweep": True})
    lines = ["# 朴素主线与完整原版v2.0：参数及同口径回测", "", "## 范围与参数", "",
             "本轮仅执行两条固定方案的实际同数据回放，不重做费用压力、不扫描新参数、不修改生产策略。原版指当前正式完整v2.0，不混用旧数据日期或早期历史版本。", "",
             "| 项目 | 原版v2.0 | 朴素研究主线 |", "| --- | --- | --- |",
             "| 股票池与换仓 | Top100等权、双周周四换仓 | 保留 |",
             "| 相对动量 | 16天，微盘简单收益率减中证1000简单收益率 | 保留 |",
             "| 基础对冲 | 微盘1对中证1000空头0.8，随执行规模同倍缩放 | 保留比例，持仓时规模固定1 |",
             "| 入场门槛 | 动量差>0 | 保留 |",
             "| 退出缓冲 | 0.003，动量差低于-0.003退出 | 归零，动量差<0退出；等于0维持原状态 |",
             "| 波动过热退出 | 60天、23%，盈利前提及等待信号重置 | 整层关闭，附属参数不再生效 |",
             "| 目标波动率 | 15%、75天、最高1.5倍、规模调整门槛0.10 | 整层关闭，持仓时固定1倍 |",
             "| R²、绝对动量、峰值衰减、净值回撤止损 | 原本未启用 | 保持关闭，不是本次删除 |", "",
             "17天零缓冲和17天/0.003仅为已保留对照，不参与本次主线选择或绩效表。无额外加杠杆不等于双边名义头寸之和≤1：朴素持仓时1微盘多头+0.8股指空头。", "",
             "## 数据及可比性", "",
             f"refresh-all及独立check通过，最新{target.date()}；刷新release={manifest['release_sha']}，verified_at={manifest['verified_at']}。",
             "公共/本地Top100 proxy，不是Wind868008.WI官方指数历史。原冻结股票复权/指数构造口径不变；底层收益价格优先qfq，缺失时保留既有raw回退，市值与执行约束沿用原始价格路径；本轮直接使用正式构造的proxy，未重新调整复权。A股交易日，Asia/Shanghai。",
             "刷新输入：面板8721行、proxy4050行、base costed4033行，均到2026-09-04；turnover431条事件，末日2026-09-03（9月4日无新换仓）；正式v2.0为4016行、v2.3/v2.5各3971行，全部到9月4日。各文件日期/行数/哈希见freshness_before.json。",
             f"两条日收益均逐文件读回{len(official)}行，{native_start.date()}—{target.date()}。先完整运行状态机，再计算窗口；不在窗口开始重置仓位。完整可比历史用于主表；另列2010-07-27起的3915行口径对应前几轮参数研究，避免将起点变化误认为策略变动。", "",
             "## 完整共同历史及五窗口", ""]
    labels = {"full": "全样本", "last_10y": "近10年", "last_5y": "近5年", "last_3y": "近3年", "last_1y": "近1年"}
    for caliber in ("native_full", "previous_research_common"):
        if caliber != "native_full":
            lines += ["", "## 前几轮共同起点口径（2010-07-27起）", ""]
        lines += ["| 窗口 | 原版年化 | 朴素年化 | 年化变化pp | 原版回撤 | 朴素回撤 | 回撤改善pp |", "| --- | --- | --- | --- | --- | --- | --- |"]
        for segment in helper.layer4.WINDOWS:
            group = s.loc[s.caliber.eq(caliber) & s.segment.eq(segment)].set_index("candidate")
            original, plain = group.loc["original_v20"], group.loc["plain16_fixed1"]
            lines.append(f"| {labels[segment]} | {original.ann_return:.2%} | {plain.ann_return:.2%} | {plain.annual_return_delta_pp:+.2f} | {-original.max_dd:.2%} | {-plain.max_dd:.2%} | {plain.drawdown_improvement_pp:+.2f} |")
    lines += ["", "## 结果解读", "",
              "完整共同历史内，朴素版降低最大回撤，但年化低于原版；近10年收益接近，近5/3/1年朴素版收益更高，但近期四个窗口回撤均较原版扩大。因此是复杂度、收益和风险分布之间的取舍，不是全时期全面胜出。前几轮按已声明模块门槛排除附加规则，不等于最终组合在所有窗口、所有风险指标均更好。",
              "上述比较同时移除了退出缓冲、过热退出、动态仓位，不能把整组差异归因于目标波动率单项。当前主线仍是研究候选，正式生产v2.0未切换。", "",
              "## 费用、执行及验证边界", "",
              "均为costed，沿用入/退出各0.003、实际日期成员换仓成本、持仓日0.00024期货拖累，没有新增成本压力情景。完整原版保留规模换手费0.001、超1倍部分融资年率3%、持仓且低于1倍时闲置资金年收益2%；朴素线无规模调整费/融资/这部分现金收益。这些差异是删除动态仓位模块的真实结果，不是把原版gross与朴素costed混比。",
              "原执行日close成本与前一决策决定当日持仓收益的时序不变，成员停牌/涨跌停/T+1约束继承原proxy。未新增实盘成交容量或冲击估计。",
              "原版重放与新鲜正式costed CSV在1e-12内一致；两条方案读盘、前缀不变、现金状态/成本身份检查通过；研究前后源文件、正式输入与导出指纹一致。",
              "历史成员按现有ST区间零交叉，但688592一条既有policy证明缺口保留，不能声称全部历史数据质量已完善。此前使用过的历史不构成新的样本外证明。",
              "全样本及各窗口的收益与回撤需同时判断，不应把近期收益更好解释为全时期、所有风险指标全面更好；本轮是三个改动合并的最终方案对照，不将所有差异单独归因于目标波动率。", "",
              "## 复现与产物", "",
              "真实链路：正式load_close_df→run_signal→缓冲→（原版过热）→实际cost engine→（原版target-vol）；指标使用仓库calc_metrics。",
              "运行：python -X utf8 scripts/top100_delivery.py refresh-all；uv run --no-project --python C:/Python314/python.exe python -X utf8 research_reports/20260904_v20_plain_vs_original/run_comparison.py。",
              "材料：同目录comparison_metrics.csv、两条完整.csv.gz、parity_checks.csv、candidate_freshness.json、freshness_before/after.json、delivery_check.json、delivery_manifest_snapshot.json、comparison_meta.json、frozen_close.csv、frozen_turnover.csv、member_data_quality.json。",
              "新建研究脚本和报告，不改任何生产源码，未做危险修改故无需回滚备份；既有脏工作区完整保留。未推送或部署，不能视为日报版本切换完成。", ""]
    report = "\n".join(lines)
    (RUN / "report.md").write_text(report, encoding="utf-8")
    (ROOT / "docs/microcap_v20_plain_vs_original_20260904.md").write_text(report, encoding="utf-8")
    print(s.loc[s.caliber.eq("native_full")].to_string(index=False))
    print("PASS: fresh formal parity; paired output readback; state/cost/prefix; source/data unchanged")


if __name__ == "__main__":
    main()
