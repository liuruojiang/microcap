from __future__ import annotations

from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]

import microcap_top100_mom16_biweekly_live_v2_5 as v25  # noqa: E402


def reject_legacy_preflight(path: Path) -> None:
    if "scan_preflight_" in Path(path).name:
        raise RuntimeError(
            f"legacy static scan preflight is not an official v2.5 baseline: {path}"
        )


def load_fresh_official_v25() -> tuple[dict[str, object], pd.DataFrame]:
    summary, _signal, frame = v25.generate_v2_5_outputs()
    v25.v2_0.assert_top100_outputs_fresh(
        expected_latest_date=frame.index.max(),
        extra_daily_paths={"v2_5_costed_nav": v25.COSTED_NAV_CSV},
    )
    if not v25.summary_matches_current_v2_5_base(summary):
        raise RuntimeError("official v2.5 fingerprint mismatch")
    return summary, frame.copy().sort_index()


def base_cost_scale(
    holding: pd.Series,
    next_holding: pd.Series,
    current_scale: pd.Series,
    next_scale: pd.Series,
) -> pd.Series:
    holding = holding.fillna("cash").astype(str)
    next_holding = next_holding.fillna(holding).astype(str)
    current = pd.to_numeric(current_scale, errors="coerce").fillna(0.0)
    actionable = pd.to_numeric(next_scale, errors="coerce").fillna(current)
    scale = current.copy()
    scale.loc[holding.eq("cash") & next_holding.ne("cash")] = actionable
    scale.loc[holding.ne("cash") & next_holding.eq("cash")] = current
    scale.loc[holding.eq("cash") & next_holding.eq("cash")] = 0.0
    return scale.clip(lower=0.0)


def replay_scale_multiplier(
    frame: pd.DataFrame,
    multiplier: pd.Series,
    *,
    next_multiplier: pd.Series,
    one_side_scale_cost: float,
    label: str,
) -> pd.DataFrame:
    """Replay exposure, state, and costs after applying a close-known scale multiplier."""
    out = frame.copy().sort_index()
    required = {"holding", "next_holding", "base_pre_cost_return"}
    missing = required - set(out.columns)
    if missing:
        raise RuntimeError(f"{label} missing replay columns: {sorted(missing)}")
    base_holding = out["holding"].fillna("cash").astype(str)
    base_next_holding = out["next_holding"].fillna(base_holding).astype(str)
    base_current = pd.to_numeric(
        out.get("current_execution_scale", pd.Series(1.0, index=out.index)),
        errors="coerce",
    ).fillna(0.0)
    base_next = pd.to_numeric(
        out.get("next_session_actionable_scale", base_current.shift(-1).fillna(base_current)),
        errors="coerce",
    ).fillna(base_current)
    multiplier = pd.to_numeric(multiplier.reindex(out.index), errors="coerce").fillna(1.0).clip(lower=0.0)
    next_multiplier = pd.to_numeric(next_multiplier.reindex(out.index), errors="coerce").fillna(1.0).clip(lower=0.0)
    current = (base_current * multiplier).where(base_holding.ne("cash"), 0.0)
    actionable = current.shift(-1)
    if len(out.index):
        actionable.iloc[-1] = float(base_next.iloc[-1] * next_multiplier.iloc[-1])
    actionable = actionable.fillna(0.0).where(base_next_holding.ne("cash"), 0.0)
    holding = base_holding.where(current.gt(1e-12), "cash")
    next_holding = base_next_holding.where(actionable.gt(1e-12), "cash")

    unscaled_base_cost = pd.to_numeric(
        out.get("base_trade_cost", out.get("total_cost", pd.Series(0.0, index=out.index))),
        errors="coerce",
    ).fillna(0.0)
    trade_cost_scale = base_cost_scale(holding, next_holding, current, actionable)
    base_trade_cost_scaled = (unscaled_base_cost * trade_cost_scale).clip(lower=0.0, upper=0.99)
    same_base_holding = base_holding.eq(base_holding.shift(1)) & base_holding.ne("cash")
    scale_change_cost = current.sub(current.shift(1).fillna(0.0)).abs().where(same_base_holding, 0.0)
    scale_change_cost = (scale_change_cost * float(one_side_scale_cost)).clip(lower=0.0, upper=0.99)

    annual_cash_yield = pd.to_numeric(
        out.get("cash_day_yield_annual", pd.Series(0.0, index=out.index)),
        errors="coerce",
    ).fillna(0.0)
    base_active = base_holding.ne("cash")
    idle_cash_yield = base_active.astype(float) * current.rsub(1.0).clip(lower=0.0, upper=1.0) * annual_cash_yield / 252.0
    cash_day_yield = base_active.astype(float).rsub(1.0) * annual_cash_yield / 252.0
    gross = pd.to_numeric(out["base_pre_cost_return"], errors="coerce").fillna(0.0) * current
    gross = gross + idle_cash_yield + cash_day_yield
    ret = (1.0 + gross) * (1.0 - base_trade_cost_scaled) * (1.0 - scale_change_cost) - 1.0

    out["base_holding_state"] = base_holding
    out["base_next_holding_state"] = base_next_holding
    out["base_current_execution_scale"] = base_current
    out["current_execution_scale"] = current
    out["next_session_actionable_scale"] = actionable
    out["holding"] = holding
    out["next_holding"] = next_holding
    out["base_trade_cost_scaled_actual"] = base_trade_cost_scaled
    out["overlay_scale_change_cost"] = scale_change_cost
    out["return_net"] = ret
    out["nav_net"] = (1.0 + ret).cumprod()
    out["nav"] = out["nav_net"]
    assert_candidate_state_consistent(out, label)
    return out


def assert_candidate_state_consistent(frame: pd.DataFrame, label: str) -> None:
    current_cash = frame["holding"].fillna("cash").astype(str).eq("cash")
    next_cash = frame["next_holding"].fillna("cash").astype(str).eq("cash")
    current_zero = pd.to_numeric(frame["current_execution_scale"], errors="coerce").fillna(0.0).le(1e-12)
    next_zero = pd.to_numeric(frame["next_session_actionable_scale"], errors="coerce").fillna(0.0).le(1e-12)
    if not current_cash.equals(current_zero):
        raise RuntimeError(f"{label} holding/current_execution_scale state mismatch")
    if not next_cash.equals(next_zero):
        raise RuntimeError(f"{label} next_holding/next_session_actionable_scale state mismatch")
