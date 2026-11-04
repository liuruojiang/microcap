"""Read-only revision comparison from frozen real CSVs; no refresh or replay."""
import ast
import hashlib
import io
import json
import zipfile
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
OUT = Path(__file__).resolve().parent
CUTOFF = pd.Timestamp("2026-10-28")
WINDOWS = (("full", None), ("last_10y", 10), ("last_5y", 5), ("last_3y", 3), ("last_1y", 1))
LABELS = dict(zip([w for w, _ in WINDOWS], ["全样本", "近10年", "近5年", "近3年", "近1年"]))


def read_json(relative):
    return json.loads((ROOT / relative).read_text(encoding="utf-8-sig"))


def digest(data):
    return hashlib.sha256(data).hexdigest()


# Execute only the existing pure reporting functions, not strategy module imports.
metric_path = ROOT / "microcap_top100_mom16_biweekly_live_v2_3.py"
tree = ast.parse(metric_path.read_text(encoding="utf-8-sig"))
names = {"summarize_returns", "_unavailable_window_summary", "summarize_required_windows"}
nodes = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name in names]
assert len(nodes) == len(names)
namespace = {"pd": pd, "np": np, "TRADING_DAYS": 244, "REQUIRED_PERFORMANCE_WINDOWS": WINDOWS}
exec(compile(ast.Module(body=nodes, type_ignores=[]), str(metric_path), "exec"), namespace)


def frame(data):
    f = pd.read_csv(io.BytesIO(data), index_col="date", parse_dates=True)
    assert f.index.is_unique and f.index.is_monotonic_increasing
    assert np.isfinite(f.return_net).all()
    return f


def metrics(f):
    return {r["window"]: r for r in namespace["summarize_required_windows"](f.return_net)}


def episode(f):
    nav = (1 + f.return_net).cumprod()
    dd = nav / nav.cummax().clip(lower=1) - 1
    trough = dd.idxmin()
    peak = nav.loc[:trough].idxmax()
    recovered = nav.loc[trough:][nav.loc[trough:] >= nav.loc[peak]]
    return dict(peak=str(peak.date()), trough=str(trough.date()), recovery=str(recovered.index[0].date()) if len(recovered) else None)


old_manifest = read_json("research_reports/20261029_st_history_repair/old_formal_compare_20261101/input_manifest.json")
formal = read_json("docs/release_evidence_20261101/formal_three_version_readonly.json")
package = ROOT / "research_reports/20261029_st_history_repair/final_delivery_20261101/microcap-whole-delivery-20261030.zip"
sources = {"metric_source": str(metric_path.relative_to(ROOT)), "metric_source_sha256": digest(metric_path.read_bytes()), "package": str(package.relative_to(ROOT)), "streams": {}}
rows, details, old_frames, new_frames = [], {}, {}, {}


def comparison(stage, version, before, after):
    assert before.index.equals(after.index)
    a, b = metrics(before), metrics(after)
    for window, _ in WINDOWS:
        rows.append(dict(stage=stage, version=version, window=window, start_date=a[window]["start_date"], end_date=a[window]["end_date"], rows=a[window]["days"], before_annual_pct=a[window]["annual_pct"], after_annual_pct=b[window]["annual_pct"], annual_delta_pp=b[window]["annual_pct"]-a[window]["annual_pct"], before_max_drawdown_pct=a[window]["max_drawdown_pct"], after_max_drawdown_pct=b[window]["max_drawdown_pct"], drawdown_delta_pp=b[window]["max_drawdown_pct"]-a[window]["max_drawdown_pct"], before_final_nav=a[window]["final_nav"], after_final_nav=b[window]["final_nav"]))


with zipfile.ZipFile(package) as archive:
    for version in ("v2_0", "v2_3", "v2_5"):
        spec = old_manifest["inputs"][version]
        path = Path(spec["path"])
        raw_before = path.read_bytes()
        assert digest(raw_before) == spec["sha256"]
        member = "outputs/" + path.name
        raw_after = archive.read(member)
        assert digest(raw_after) == formal["streams"][version]["sha256"]
        before, full_after = frame(raw_before), frame(raw_after)
        after = full_after.loc[full_after.index <= CUTOFF]
        assert before.index[-1] == CUTOFF and before.index.equals(after.index)
        # Reproduce the already accepted migration report before aligning cutoffs.
        prepared = read_json(f"docs/release_evidence_20261101/{version}_prepared_report.json")
        for label, values in (("previous_performance", metrics(before)), ("candidate_performance", metrics(full_after))):
            for window, result in values.items():
                assert abs(result["annual_pct"] / 100 - prepared[label][window]["annualized_return"]) < 1e-12
                assert abs(result["max_drawdown_pct"] / 100 - prepared[label][window]["max_drawdown"]) < 1e-12
        old_frames[version], new_frames[version] = before, after
        comparison("shared_history_repair", version, before, after)
        changed = (before.return_net - after.return_net).abs() > 1e-12
        current_raw = (ROOT / member).read_bytes()
        current = frame(current_raw).loc[full_after.index]
        economic_differences = {c: float((current[c]-full_after[c]).abs().max()) for c in ("return_net", "nav_net", "total_cost")}
        assert max(economic_differences.values()) < 1e-12
        assert current.holding.equals(full_after.holding) and current.next_holding.equals(full_after.next_holding)
        details[version] = dict(changed_return_rows=int(changed.sum()), compared_rows=len(before), first_changed_return=str(before.index[changed][0].date()), changed_holding_days=int((before.holding != after.holding).sum()), before_max_drawdown_period=episode(before), after_max_drawdown_period=episode(after), current_prefix_economic_max_difference=economic_differences)
        sources["streams"][version] = dict(before_path=str(path.relative_to(ROOT)), before_sha256=digest(raw_before), after_zip_member=member, after_sha256=digest(raw_after), current_path=member, current_sha256=digest(current_raw), current_latest=str(frame(current_raw).index[-1].date()))

scan_root = ROOT / "quant_param_scan_runs/20261029_microcap_top100_v2_3_overheat_vol_window_trigger_recovery"
constraint_path = scan_root / "daily_outputs/w10_t0.26_r0.20.csv"
constrained = frame(constraint_path.read_bytes())
oracle = frame((scan_root / "daily_outputs/no_constraint.csv").read_bytes())
assert oracle.index.equals(old_frames["v2_3"].index)
assert float((oracle.return_net - old_frames["v2_3"].return_net).abs().max()) < 1e-12
scan_metrics = pd.read_csv(scan_root / "window_metrics.csv").set_index("candidate")
for window, result in metrics(constrained).items():
    assert abs(result["annual_pct"]/100 - scan_metrics.loc["w10_t0.26_r0.20", "ann_return_" + window]) < 1e-12
    assert abs(result["max_drawdown_pct"]/100 - scan_metrics.loc["w10_t0.26_r0.20", "max_dd_" + window]) < 1e-12
comparison("volatility_removal", "v2_3", constrained, old_frames["v2_3"])
for version in old_frames:
    comparison("combined", version, constrained if version == "v2_3" else old_frames[version], new_frames[version])
sources["v2_3_constraint_on"] = dict(path=str(constraint_path.relative_to(ROOT)), sha256=digest(constraint_path.read_bytes()))

table = pd.DataFrame(rows)
table.to_csv(OUT / "window_comparison.csv", index=False, encoding="utf-8-sig")
(OUT / "evidence.json").write_text(json.dumps(dict(scope="frozen dated revision comparison; not current strategy performance or new historical certification", cutoff=str(CUTOFF.date()), annualization_trading_days=244, source=sources, details=details), ensure_ascii=False, indent=2), encoding="utf-8")


def markdown_table(stage, windows=None):
    selected = table.loc[table.stage == stage]
    if windows is not None:
        selected = selected.loc[selected.window.isin(windows)]
    lines = ["| 版本 | 区间 | 年化：修订前 → 修订后 | 年化变化（百分点） | 最大回撤幅度：前 → 后 |", "|---|---|---:|---:|---:|"]
    for r in selected.itertuples():
        lines.append(f"| {r.version.replace('_', '.')} | {LABELS[r.window]} | {r.before_annual_pct:.2f}% → {r.after_annual_pct:.2f}% | {r.annual_delta_pp:+.2f} | {abs(r.before_max_drawdown_pct):.2f}% → {abs(r.after_max_drawdown_pct):.2f}% |")
    return "\n".join(lines)


text = """# 2026-10-29 至 2026-11-03 修订对历史回测的影响

这是冻结历史的修订比较，统一截止 **2026-10-28**，不是最新行情回测或新的历史认证。v2.0 全样本从 2010-03-01 开始，v2.3/v2.5 从 2010-05-05 开始；每版修订前后的日期、行数完全对齐。采用原有成本后 `return_net`，调用正式代码中的纯指标函数，按每年 244 个交易日年化。最大回撤用损失幅度显示，越大越差。

旧正式 CSV 与冻结清单哈希一致；修复后的 CSV 直接从 11 月 1 日已验收的正式包读取，与当时正式读回记录哈希一致。先复现原迁移报告，再把修复后多出的 10/29、10/30 两天剔除，避免把新增交易日混入修订影响。没有刷新市场数据、重新选股或运行历史策略。

整组修订的全样本影响如下。v2.3 的“前”包含原波动率约束，“后”同时包含取消约束和共享历史修复。

""" + markdown_table("combined", ["full"]) + """

## 取消 v2.3 波动率约束的独立影响

采用 10/29 已保存的同一旧数据基础，原参数为 10 日波动率、26% 触发、20% 恢复。此项只改变 v2.3，v2.0/v2.5 的历史收益不变。这是旧数据上的独立归因，不代表在修复后数据上重新测试约束的结果。

""" + markdown_table("volatility_removal") + """

## 共享历史数据与计价修复的影响

包含历史 ST 进入退出与公告分类、历史证券资格及 B 股排除、比例复权收益口径、退市持仓计价等共享修复。此处 v2.3 的修订前基线已经取消波动率约束，因此不会重复归因。

""" + markdown_table("shared_history_repair") + """

全样本年化均提高，不代表所有区间都改善：v2.5 近十年年化下降，v2.3 的全样本及近三年最大回撤扩大。共享修复是组合变更；没有逐项隔离实验，不能把全部增益归到 ST、复权或退市某一项。

历史收益实际变化范围与最大回撤时期：

| 版本 | 收益变化交易日 / 总日数 | 持仓状态变化日数 | 修复前最大回撤峰值 → 谷值 | 修复后最大回撤峰值 → 谷值 | 修复后收复日期 |
|---|---:|---:|---|---|---|
"""
for v, d in details.items():
    a, b = d["before_max_drawdown_period"], d["after_max_drawdown_period"]
    text += f"| {v.replace('_','.')} | {d['changed_return_rows']} / {d['compared_rows']} | {d['changed_holding_days']} | {a['peak']} → {a['trough']} | {b['peak']} → {b['trough']} | {b['recovery']} |\n"
text += """
收益变化日数采用绝对差大于 1e-12 统计；持仓状态变化指策略持有/空仓状态，不是全部股票成员变化。

## 11 月 2—3 日的运行及日报修订

数据源回退、历史 CSV 字节保持、时间格式、传输去重、跨午夜交易日绑定等运行修订，没有进一步改变截至 10/30 的成本后收益、净值、成本或策略持仓。已有逐行回执覆盖三版的 9 条输出流，本次另读回当前三条正式成本后 CSV，与 11/1 正式包的历史前缀对照一致。11/2 新增一行是正常日期推进，不计入历史改写。v2.5 在 10/29 的三个同义诊断得分字段只出现约 4.44e-16 的浮点变化，不影响经济结果。

## 口径与范围

- 退市日剩余价值计零并释放仓位，是用户已选择的保守模型，**不是实际卖出成交**。
- `sina_ratio_ex_reference_v1` 是不复权价乘比例因子的除权参考收益；市值按不复权价乘历史股本。它不是完整现金分红税费、配股缴款和再投资流水下的账户总回报。
- 尚未正式迁入的新公告不在本次已发布历史修订之内；本次比较不证明这类待办已经解决，也不是对全部历史来源不存在遗漏的保证。
- 本次只生成研究比较文件，未改正式历史、参数、发布状态或发送邮件。

完整逐窗口原始数值和来源哈希见同目录 `window_comparison.csv`、`evidence.json`；可运行 `python -B -X utf8 research_reports/20261103_revision_impact/analyze.py` 复现，当前正式文件发生变化时会重新比对，不沿用旧 stdout。
"""
(OUT / "report.md").write_text(text, encoding="utf-8")
print(table.loc[(table.stage == "combined") & (table.window == "full")].to_string(index=False))
print("Written:", OUT / "report.md")
