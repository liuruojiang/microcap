from __future__ import annotations

import argparse
import hashlib
import json
import math
import re
import sys
import zipfile
from datetime import datetime
from pathlib import Path

import matplotlib
import numpy as np
import pandas as pd
import requests

matplotlib.use("Agg")
import matplotlib.pyplot as plt


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts import run_microcap_v2_3_v2_5_combo50_comparison as freshness_common


IM_REPO = Path(r"D:\动量策略\IC和IM滚动套利")
if str(IM_REPO) not in sys.path:
    sys.path.insert(0, str(IM_REPO))

import im_monthly_discount_roll_v1 as im_roll
import run_im_futures_roll_tenor_timing_scan_v1 as futures_scan
import run_im_v13_r6_full_roll_timing_tenor_v1 as full_roll


REAL_IM_RUN = (
    IM_REPO
    / "quant_param_scan_runs"
    / "20260904_im_v13_r6_full_roll_timing_tenor_v2"
)
REAL_IM_DAILY = REAL_IM_RUN / "daily_candidates.csv.gz"
REAL_IM_META = REAL_IM_RUN / "scan_meta.json"
REAL_IM_PARITY = REAL_IM_RUN / "parity_checks.json"

V20_NAV = ROOT / "outputs" / "microcap_top100_mom16_plain_fixed1_v2_0_costed_nav.csv"
V23_NAV = (
    ROOT
    / "outputs"
    / "microcap_top100_mom16_lb25_hl2p5_r2off_eb0p08_vol10_oh26_recovery20_exec0p8_v2_3_costed_nav.csv"
)
RUN = (
    ROOT
    / "quant_param_scan_runs"
    / "20260905_microcap_v20_v23_real_im_hedge_replacement_v2"
)

HEDGE_RATIO = 0.8
IM_ONE_WAY_COST = 0.0001
TRADING_DAYS = 244
REAL_START = pd.Timestamp("2022-07-22")
ROLL_PATHS = ("expiry_settle_front", "next_t03_close")
CFFEX_MONTH_URLS = (
    "https://www.cffex.com.cn/sj/historysj/{ym}/zip/{ym}.zip",
    "http://www.cffex.com.cn/sj/historysj/{ym}/zip/{ym}.zip",
)
VERSIONS = {
    "v2_0": {
        "path": V20_NAV,
        "version": "2.0",
        "revision": "plain_mom16_fixed1_20260904",
    },
    "v2_3": {
        "path": V23_NAV,
        "version": "2.3",
        "revision": "plain_lb25_hl2p5_r2off_vol10_26_20_20260904",
    },
}
WINDOWS: tuple[tuple[str, int | None], ...] = (
    ("full", None),
    ("last_10y", 10),
    ("last_5y", 5),
    ("last_3y", 3),
    ("last_1y", 1),
)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def _load_official_stream(label: str, spec: dict[str, object]) -> pd.DataFrame:
    path = Path(spec["path"])
    if not path.is_file():
        raise FileNotFoundError(path)
    frame = pd.read_csv(path, parse_dates=["date"]).sort_values("date")
    required = {
        "date",
        "version",
        "strategy_revision",
        "holding",
        "next_holding",
        "microcap_ret",
        "hedge_ret",
        "futures_drag",
        "entry_exit_cost",
        "rebalance_cost",
        "total_cost",
        "return_net",
        "nav_net",
    }
    missing = required.difference(frame.columns)
    if missing:
        raise RuntimeError(f"{label} missing columns: {sorted(missing)}")
    if frame["date"].duplicated().any() or not frame["date"].is_monotonic_increasing:
        raise RuntimeError(f"{label} dates must be unique and ordered")
    if set(frame["version"].dropna().astype(str).unique()) != {str(spec["version"])}:
        raise RuntimeError(f"{label} version identity mismatch")
    if set(frame["strategy_revision"].dropna().astype(str).unique()) != {
        str(spec["revision"])
    }:
        raise RuntimeError(f"{label} strategy revision mismatch")
    for column in (
        "microcap_ret",
        "hedge_ret",
        "futures_drag",
        "entry_exit_cost",
        "rebalance_cost",
        "total_cost",
        "return_net",
        "nav_net",
    ):
        values = pd.to_numeric(frame[column], errors="coerce")
        if values.isna().any() or not np.isfinite(values.to_numpy()).all():
            raise RuntimeError(f"{label} {column} contains non-finite values")
        frame[column] = values
    rebuilt = (1.0 + frame["return_net"]).cumprod()
    nav_error = float((rebuilt - frame["nav_net"]).abs().max())
    if nav_error > 1e-10:
        raise RuntimeError(f"{label} official NAV parity failed: {nav_error}")
    return frame.set_index("date")


def _download_month_zip(month: str, raw_dir: Path) -> tuple[Path, dict[str, object]]:
    raw_dir.mkdir(parents=True, exist_ok=True)
    errors: list[str] = []
    for template in CFFEX_MONTH_URLS:
        url = template.format(ym=month)
        try:
            response = requests.get(
                url,
                headers={"User-Agent": "Mozilla/5.0 microcap-real-im-research/1.0"},
                timeout=30,
            )
            response.raise_for_status()
            path = raw_dir / f"{month}.zip"
            path.write_bytes(response.content)
            if not zipfile.is_zipfile(path):
                raise RuntimeError("response is not a ZIP archive")
            with zipfile.ZipFile(path) as archive:
                members = sorted(
                    Path(name).name
                    for name in archive.namelist()
                    if re.fullmatch(r"\d{8}_1\.csv", Path(name).name)
                )
            if not members:
                raise RuntimeError("archive has no daily futures CSV members")
            return path, {
                "url": url,
                "bytes": int(path.stat().st_size),
                "sha256": _sha256(path),
                "member_count": len(members),
                "first_member": members[0],
                "last_member": members[-1],
            }
        except Exception as exc:
            errors.append(f"{url}: {type(exc).__name__}: {exc}")
    raise RuntimeError(f"Could not refresh official CFFEX month {month}: {errors}")


def _load_real_im_paths(
    target_end: pd.Timestamp,
) -> tuple[dict[str, pd.DataFrame], dict[str, object]]:
    for path in (REAL_IM_DAILY, REAL_IM_META, REAL_IM_PARITY):
        if not path.is_file():
            raise FileNotFoundError(path)
    meta = json.loads(REAL_IM_META.read_text(encoding="utf-8"))
    parity = json.loads(REAL_IM_PARITY.read_text(encoding="utf-8"))
    if not bool(parity.get("all_pass")) or not bool(meta.get("baseline_parity", {}).get("all_pass")):
        raise RuntimeError("Frozen real-IM source parity did not pass")
    frozen_end = pd.Timestamp(meta.get("sample", [None, None])[1])
    if meta.get("sample", [None])[0] != str(REAL_START.date()):
        raise RuntimeError(f"Unexpected real-IM sample: {meta.get('sample')}")
    if target_end < frozen_end:
        raise RuntimeError(f"Requested end {target_end.date()} precedes frozen source end")

    base_quotes_path = IM_REPO / "data" / "im_monthly_roll_3m_lowest_put_v1" / "cffex_im_contracts.csv"
    base_quotes = pd.read_csv(base_quotes_path, parse_dates=["date"])
    refresh_start = frozen_end.replace(day=1)
    months = pd.date_range(refresh_start, target_end.replace(day=1), freq="MS")
    raw_dir = RUN / "cffex_raw"
    zip_paths: list[Path] = []
    download_log: list[dict[str, object]] = []
    for month_start in months:
        month = month_start.strftime("%Y%m")
        path, evidence = _download_month_zip(month, raw_dir)
        zip_paths.append(path)
        download_log.append({"month": month, **evidence})
    refreshed_quotes = im_roll.parse_cffex_im(zip_paths, target_end)
    refreshed_quotes = refreshed_quotes.loc[refreshed_quotes["date"] >= refresh_start].copy()
    if refreshed_quotes.empty or refreshed_quotes["date"].max() != target_end:
        raise RuntimeError(
            f"Official CFFEX refresh did not reach {target_end.date()}: "
            f"got {refreshed_quotes['date'].max().date() if not refreshed_quotes.empty else 'empty'}"
        )
    overlap = base_quotes.merge(
        refreshed_quotes,
        on=["date", "contract"],
        how="inner",
        suffixes=("_base", "_refresh"),
        validate="one_to_one",
    )
    numeric = ["open", "high", "low", "volume", "turnover", "open_interest", "close", "settle", "pre_settle"]
    overlap_errors = {
        column: float((overlap[f"{column}_base"] - overlap[f"{column}_refresh"]).abs().max())
        for column in numeric
    }
    if overlap.empty or any(value > 1e-10 for value in overlap_errors.values()):
        raise RuntimeError(f"Refreshed CFFEX overlap mismatch: rows={len(overlap)}, errors={overlap_errors}")
    quotes = pd.concat(
        [base_quotes.loc[base_quotes["date"] < refresh_start], refreshed_quotes],
        ignore_index=True,
    ).sort_values(["date", "contract"]).reset_index(drop=True)
    if quotes.duplicated(["date", "contract"]).any():
        raise RuntimeError("Duplicate rows in refreshed official CFFEX chain")
    counts = quotes.groupby("date").size()
    if not counts.eq(4).all():
        raise RuntimeError(f"Expected four listed IM contracts per day: {counts.loc[~counts.eq(4)].head().to_dict()}")

    cycles = im_roll.build_cycles(quotes, target_end)
    expiry_daily, _ = im_roll.build_futures_daily(quotes, cycles)
    benchmark = pd.DataFrame({
        "date": expiry_daily["date"],
        "csi1000_price_ret": 0.0,
        "csi1000_price_close": 1.0,
    })
    expiry, complete = futures_scan.expiry_maps(quotes, target_end)
    t3_definition = futures_scan.Candidate(
        "next_t03_close", "next_listed", "fixed_td", fixed_td=3
    )
    t3_daily, t3_events = full_roll.candidate_upstream(
        t3_definition, quotes, benchmark, expiry, complete
    )
    expiry_frame = pd.DataFrame({
        "date": expiry_daily["date"],
        "contract": expiry_daily["contract"],
        "base_gross_ret": expiry_daily["im_gross_ret"],
        "base_futures_cost_rate": expiry_daily["cost_rate"],
        "roll_event": expiry_daily["roll_to"].fillna("").ne(""),
    })
    t3_frame = pd.DataFrame({
        "date": t3_daily["date"],
        "contract": t3_daily["contract"],
        "base_gross_ret": t3_daily["im_gross_ret"],
        "base_futures_cost_rate": t3_daily["cost_rate"],
        "roll_event": t3_daily["roll_to"].fillna("").ne(""),
    })
    paths = {
        "expiry_settle_front": expiry_frame.set_index("date"),
        "next_t03_close": t3_frame.set_index("date"),
    }
    if not paths["expiry_settle_front"].index.equals(paths["next_t03_close"].index):
        raise RuntimeError("Refreshed real-IM candidate dates differ")
    frozen = pd.read_csv(REAL_IM_DAILY, compression="gzip", parse_dates=["date"])
    frozen_checks: dict[str, float] = {}
    for candidate, refreshed in paths.items():
        prior = frozen.loc[frozen["candidate"].eq(candidate)].set_index("date")
        common = prior.index.intersection(refreshed.index)
        error = float(
            (prior.loc[common, "base_gross_ret"] - refreshed.loc[common, "base_gross_ret"])
            .abs()
            .max()
        )
        frozen_checks[candidate] = error
        if error > 1e-12:
            raise RuntimeError(f"Refreshed {candidate} breaks frozen parity: {error}")
    return paths, {
        "meta": meta,
        "parity": parity,
        "base_quotes_path": str(base_quotes_path),
        "base_quotes_sha256": _sha256(base_quotes_path),
        "target_end": str(target_end.date()),
        "quote_rows": int(len(quotes)),
        "quote_dates": int(quotes["date"].nunique()),
        "contracts": int(quotes["contract"].nunique()),
        "overlap_rows": int(len(overlap)),
        "overlap_max_abs_errors": overlap_errors,
        "frozen_candidate_return_max_abs_errors": frozen_checks,
        "download_log": download_log,
        "t3_roll_events": int(len(t3_events)),
    }


def _metrics(
    returns: pd.Series,
    *,
    version: str,
    candidate: str,
    window: str,
    requested_years: int | None,
) -> dict[str, object]:
    end = pd.Timestamp(returns.index.max())
    if requested_years is None:
        start = pd.Timestamp(returns.index.min())
    else:
        requested_start = end - pd.DateOffset(years=requested_years)
        available_years = (end - pd.Timestamp(returns.index.min())).days / 365.25
        if available_years + 1e-9 < requested_years:
            return {
                "version": version,
                "candidate": candidate,
                "window": window,
                "available": False,
                "na_reason": (
                    f"real IM listed-history sample is {available_years:.2f} years, "
                    f"shorter than requested {requested_years} years"
                ),
                "start": "",
                "end": str(end.date()),
                "rows": 0,
                "ann_return": np.nan,
                "max_dd": np.nan,
                "ann_vol": np.nan,
                "sharpe_repo": np.nan,
            }
        start = requested_start
    part = returns.loc[(returns.index >= start) & (returns.index <= end)].astype(float)
    if part.empty:
        raise RuntimeError(f"Empty metric window: {version} {candidate} {window}")
    nav = (1.0 + part).cumprod()
    elapsed_years = (part.index[-1] - part.index[0]).days / 365.25
    ann_return = float(nav.iloc[-1] ** (1.0 / elapsed_years) - 1.0)
    ann_vol = float(part.std(ddof=1) * math.sqrt(TRADING_DAYS))
    nav_with_initial = np.r_[1.0, nav.to_numpy(dtype=float)]
    max_dd = float((nav_with_initial / np.maximum.accumulate(nav_with_initial) - 1.0).min())
    return {
        "version": version,
        "candidate": candidate,
        "window": window,
        "available": True,
        "na_reason": "",
        "start": str(part.index[0].date()),
        "end": str(part.index[-1].date()),
        "rows": int(len(part)),
        "ann_return": ann_return,
        "max_dd": max_dd,
        "ann_vol": ann_vol,
        "sharpe_repo": float(ann_return / ann_vol) if ann_vol > 0 else np.nan,
        "final_nav": float(nav.iloc[-1]),
        "total_return": float(nav.iloc[-1] - 1.0),
    }


def _build_version_daily(
    version: str,
    official: pd.DataFrame,
    im_paths: dict[str, pd.DataFrame],
) -> tuple[pd.DataFrame, dict[str, object]]:
    dates = im_paths[ROLL_PATHS[0]].index
    if not dates.equals(im_paths[ROLL_PATHS[1]].index):
        raise RuntimeError("Real-IM candidate dates differ")
    if not dates.isin(official.index).all():
        missing = dates[~dates.isin(official.index)]
        raise RuntimeError(f"{version} is missing real-IM dates: {missing[:5].tolist()}")
    base = official.loc[dates].copy()
    out = pd.DataFrame(index=dates)
    out["version"] = version
    out["holding"] = base["holding"].astype(str)
    out["next_holding"] = base["next_holding"].astype(str)
    out["active"] = out["holding"].ne("cash")
    out["next_active"] = out["next_holding"].ne("cash")
    out["microcap_ret"] = base["microcap_ret"]
    out["proxy_index_ret"] = base["hedge_ret"]
    out["proxy_fixed_drag"] = base["futures_drag"]
    out["stock_entry_exit_cost"] = base["entry_exit_cost"]
    out["stock_rebalance_cost"] = base["rebalance_cost"]
    out["stock_total_cost"] = base["total_cost"]
    out["proxy_return_net"] = base["return_net"]
    out["proxy_nav"] = (1.0 + out["proxy_return_net"]).cumprod()
    integrity: dict[str, object] = {
        "rows": int(len(out)),
        "start": str(out.index.min().date()),
        "end": str(out.index.max().date()),
        "active_days": int(out["active"].sum()),
        "entry_exit_cost_days": int(out["stock_entry_exit_cost"].gt(0).sum()),
    }
    for candidate, im in im_paths.items():
        raw = im.loc[dates]
        out[f"{candidate}_contract"] = raw["contract"].astype(str)
        out[f"{candidate}_im_gross_ret"] = raw["base_gross_ret"]
        out[f"{candidate}_roll_event"] = raw["roll_event"].astype(bool)
        transition_event = out["stock_entry_exit_cost"].gt(0)
        roll_while_held = out["active"] & out["next_active"] & out[f"{candidate}_roll_event"]
        futures_transition_cost = transition_event.astype(float) * HEDGE_RATIO * IM_ONE_WAY_COST
        futures_roll_cost = roll_while_held.astype(float) * HEDGE_RATIO * 2.0 * IM_ONE_WAY_COST
        futures_cost = futures_transition_cost + futures_roll_cost
        pre_cost = np.where(
            out["active"],
            out["microcap_ret"] - HEDGE_RATIO * out[f"{candidate}_im_gross_ret"],
            0.0,
        )
        out[f"{candidate}_pre_cost_return"] = pre_cost
        out[f"{candidate}_futures_transition_cost"] = futures_transition_cost
        out[f"{candidate}_futures_roll_cost"] = futures_roll_cost
        out[f"{candidate}_futures_total_cost"] = futures_cost
        combined_cost = out["stock_total_cost"] + futures_cost
        if combined_cost.ge(1.0).any():
            raise RuntimeError(f"{version} {candidate} has invalid combined cost")
        out[f"{candidate}_return_net"] = (1.0 + pre_cost) * (1.0 - combined_cost) - 1.0
        out[f"{candidate}_nav"] = (1.0 + out[f"{candidate}_return_net"]).cumprod()
        if not np.isfinite(out[f"{candidate}_return_net"].to_numpy()).all():
            raise RuntimeError(f"{version} {candidate} produced non-finite returns")
        integrity[candidate] = {
            "roll_events_in_source": int(out[f"{candidate}_roll_event"].sum()),
            "roll_cost_days_while_held": int(roll_while_held.sum()),
            "futures_transition_cost_sum": float(futures_transition_cost.sum()),
            "futures_roll_cost_sum": float(futures_roll_cost.sum()),
            "signal_or_holding_mismatch_rows": 0,
        }
    out.insert(0, "date", out.index)
    return out.reset_index(drop=True), integrity


def _all_metrics(daily: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for version, group in daily.groupby("version", sort=False):
        frame = group.sort_values("date").set_index("date")
        streams = {
            "index_proxy_official": frame["proxy_return_net"],
            "real_im_expiry_settle_front": frame["expiry_settle_front_return_net"],
            "real_im_next_t03_close": frame["next_t03_close_return_net"],
        }
        for window, years in WINDOWS:
            for candidate, returns in streams.items():
                rows.append(
                    _metrics(
                        returns,
                        version=str(version),
                        candidate=candidate,
                        window=window,
                        requested_years=years,
                    )
                )
    return pd.DataFrame(rows)


def _comparison(metrics: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for (version, window), group in metrics.groupby(["version", "window"], sort=False):
        lookup = group.set_index("candidate")
        baseline = lookup.loc["index_proxy_official"]
        for candidate in (
            "real_im_expiry_settle_front",
            "real_im_next_t03_close",
        ):
            real = lookup.loc[candidate]
            available = bool(real["available"])
            rows.append(
                {
                    "version": version,
                    "window": window,
                    "candidate": candidate,
                    "available": available,
                    "na_reason": real["na_reason"],
                    "start": real["start"],
                    "end": real["end"],
                    "rows": int(real["rows"]),
                    "proxy_ann_return": baseline["ann_return"],
                    "real_ann_return": real["ann_return"],
                    "ann_return_delta_pp": (
                        100.0 * (float(real["ann_return"]) - float(baseline["ann_return"]))
                        if available
                        else np.nan
                    ),
                    "proxy_max_dd": baseline["max_dd"],
                    "real_max_dd": real["max_dd"],
                    "max_dd_improvement_pp": (
                        100.0 * (float(real["max_dd"]) - float(baseline["max_dd"]))
                        if available
                        else np.nan
                    ),
                    "proxy_ann_vol": baseline["ann_vol"],
                    "real_ann_vol": real["ann_vol"],
                    "proxy_sharpe_repo": baseline["sharpe_repo"],
                    "real_sharpe_repo": real["sharpe_repo"],
                }
            )
    return pd.DataFrame(rows)


def _annual_metrics(daily: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for version, group in daily.groupby("version", sort=False):
        frame = group.sort_values("date").set_index("date")
        for year, block in frame.groupby(frame.index.year):
            for candidate, column in (
                ("index_proxy_official", "proxy_return_net"),
                ("real_im_expiry_settle_front", "expiry_settle_front_return_net"),
                ("real_im_next_t03_close", "next_t03_close_return_net"),
            ):
                returns = block[column].astype(float)
                nav = (1.0 + returns).cumprod()
                nav0 = np.r_[1.0, nav.to_numpy(dtype=float)]
                rows.append(
                    {
                        "version": version,
                        "year": int(year),
                        "candidate": candidate,
                        "rows": int(len(block)),
                        "total_return": float(nav.iloc[-1] - 1.0),
                        "max_dd": float((nav0 / np.maximum.accumulate(nav0) - 1.0).min()),
                    }
                )
    return pd.DataFrame(rows)


def _write_chart(daily: pd.DataFrame) -> None:
    fig, axes = plt.subplots(2, 1, figsize=(12, 9), sharex=True)
    for ax, (version, group) in zip(axes, daily.groupby("version", sort=False)):
        frame = group.sort_values("date")
        ax.plot(frame["date"], frame["proxy_nav"], label="CSI1000 index proxy", linewidth=1.5)
        ax.plot(
            frame["date"],
            frame["expiry_settle_front_nav"],
            label="real IM: expiry-settle front",
            linewidth=1.8,
        )
        ax.plot(
            frame["date"],
            frame["next_t03_close_nav"],
            label="real IM: next-listed T-3",
            linewidth=1.4,
            linestyle="--",
        )
        ax.set_title(f"{version}: official signals, hedge PnL replacement only")
        ax.grid(alpha=0.25)
        ax.legend()
    fig.tight_layout()
    fig.savefig(RUN / "nav_comparison.png", dpi=160)
    plt.close(fig)


def _fmt_pct(value: object) -> str:
    if value is None or pd.isna(value):
        return "N/A"
    return f"{float(value):.2%}"


def _write_record(comparison: pd.DataFrame, integrity: dict[str, object], meta: dict[str, object]) -> None:
    lines = [
        "# 微盘 v2.0 / v2.3 真实 IM 对冲收益腿替换测试",
        "",
        "## Scope",
        "",
        "- 仅替换对冲收益腿；微盘成员、信号、持仓、过热状态和股票成本保持正式输出不变。",
        "- 主结果使用真实CFFEX IM逐合约结算价，近月持有至到期结算；next-listed T-3为换月敏感性。",
        "- 状态：research-only，不修改2.0或2.3正式策略。",
        "",
        "## Data",
        "",
        (
            f"- Real IM sample: {meta['real_im_source']['sample'][0]} to "
            f"{meta['real_im_source']['sample'][1]}, "
            f"{meta['real_im_source']['rows_per_path']} trading days."
        ),
        f"- Microcap refresh proof expected date: {meta['microcap_freshness']['expected_latest_date']}.",
        "- 10Y/5Y: N/A because IM was not listed for those full windows.",
        "",
        "## Results",
        "",
        comparison.to_markdown(index=False),
        "",
        "## Costs and execution",
        "",
        "- 股票篮子正式成本原样保留。",
        "- 删除指数代理的固定0.03%/日基差拖累。",
        "- 真实IM按0.8倍名义计入；单边手续费1bp，展期双边2bp。",
        "- 未计组合资金层的保证金融资、现金收益、整数合约和盘中冲击。",
        "",
        "## Integrity",
        "",
        "```json",
        json.dumps(integrity, ensure_ascii=False, indent=2),
        "```",
    ]
    (RUN / "record.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def main(*, official_latest_close_date: str) -> None:
    freshness = freshness_common.validate_formal_freshness(
        expected_latest_date=official_latest_close_date
    )
    target_end = pd.Timestamp(official_latest_close_date)
    im_paths, im_source = _load_real_im_paths(target_end)
    daily_parts: list[pd.DataFrame] = []
    integrity: dict[str, object] = {}
    official_streams: dict[str, pd.DataFrame] = {}
    for version, spec in VERSIONS.items():
        official = _load_official_stream(version, spec)
        official_streams[version] = official
        daily, checks = _build_version_daily(version, official, im_paths)
        daily_parts.append(daily)
        integrity[version] = checks
    daily_all = pd.concat(daily_parts, ignore_index=True)
    metrics = _all_metrics(daily_all)
    comparison = _comparison(metrics)
    annual = _annual_metrics(daily_all)

    RUN.mkdir(parents=True, exist_ok=True)
    daily_all.to_csv(RUN / "daily_returns_and_nav.csv.gz", index=False, compression="gzip")
    metrics.to_csv(RUN / "window_metrics.csv", index=False)
    comparison.to_csv(RUN / "comparison_vs_index_proxy.csv", index=False)
    annual.to_csv(RUN / "annual_metrics.csv", index=False)
    _write_chart(daily_all)

    meta: dict[str, object] = {
        "run_id": RUN.name,
        "created_at": datetime.now().astimezone().isoformat(timespec="seconds"),
        "phase": "complete",
        "status": "research_only_not_production_authority",
        "entrypoint": "scripts/run_microcap_v20_v23_real_im_hedge_backtest.py",
        "command": (
            "python -X utf8 scripts/run_microcap_v20_v23_real_im_hedge_backtest.py "
            f"--official-latest-close-date {official_latest_close_date}"
        ),
        "microcap_freshness": freshness,
        "real_im_source": {
            "frozen_path": str(REAL_IM_DAILY),
            "frozen_sha256": _sha256(REAL_IM_DAILY),
            "frozen_source_meta_path": str(REAL_IM_META),
            "frozen_source_meta_sha256": _sha256(REAL_IM_META),
            "sample": [str(REAL_START.date()), str(target_end.date())],
            "rows_per_path": int(len(im_paths[ROLL_PATHS[0]])),
            "source_label": "CFFEX official IM contract chain",
            "upstream_parity": im_source["parity"],
            "refresh_evidence": {
                key: value for key, value in im_source.items() if key not in {"meta", "parity"}
            },
        },
        "microcap_inputs": {
            version: {
                "path": str(spec["path"]),
                "sha256": _sha256(Path(spec["path"])),
                "version": spec["version"],
                "revision": spec["revision"],
                "latest_date": str(stream.index.max().date()),
                "rows": int(len(stream)),
            }
            for (version, spec), stream in zip(VERSIONS.items(), official_streams.values())
        },
        "replacement": {
            "hedge_ratio": HEDGE_RATIO,
            "signal_and_holding_states": "unchanged official v2.0/v2.3 states",
            "removed": "0.8 * CSI1000 price return + 0.8 * fixed 3bp/day drag",
            "added": "0.8 * real IM settlement return + real IM transition/roll costs",
            "im_one_way_cost": IM_ONE_WAY_COST,
            "roll_round_trip_cost": 2.0 * IM_ONE_WAY_COST,
            "stock_costs": "official total_cost preserved",
            "margin_financing": "excluded",
            "cash_yield": "excluded",
            "integer_contracts": "excluded; continuous 0.8x notional",
            "intraday_impact": "excluded",
        },
        "integrity": integrity,
        "outputs": {
            "daily": str(RUN / "daily_returns_and_nav.csv.gz"),
            "metrics": str(RUN / "window_metrics.csv"),
            "comparison": str(RUN / "comparison_vs_index_proxy.csv"),
            "annual": str(RUN / "annual_metrics.csv"),
            "chart": str(RUN / "nav_comparison.png"),
            "record": str(RUN / "record.md"),
        },
    }
    (RUN / "scan_meta.json").write_text(
        json.dumps(meta, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    (RUN / "integrity_checks.json").write_text(
        json.dumps(integrity, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    _write_record(comparison, integrity, meta)
    print(f"wrote {RUN}")
    print(
        f"real_sample={REAL_START.date()}..{target_end.date()} "
        f"rows={len(im_paths[ROLL_PATHS[0]])}"
    )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--official-latest-close-date", required=True)
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    main(official_latest_close_date=args.official_latest_close_date)
