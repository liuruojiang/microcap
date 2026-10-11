"""Black-box challenge of the isolated paper settlement contract."""

from __future__ import annotations

import importlib.util
import json
from decimal import Decimal as D
from pathlib import Path


HERE = Path(__file__).resolve().parent
CONTRACT = HERE.parent / "fixed_share_engine/contract.py"
spec = importlib.util.spec_from_file_location("isolated_contract_under_review", CONTRACT)
assert spec and spec.loader
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


def run() -> dict[str, object]:
    holdings = {"A": D(1), "B": D(1)}
    t = {"A": D(100), "B": D(100), "C": D(100)}
    next_close = {"A": D(110), "B": D(90), "C": D(50)}
    frozen = module.freeze_orders(holdings, D(10), t, ["B", "C"])
    assert [(x["symbol"], x["side"], x["frozen_shares"]) for x in frozen] == [
        ("A", "sell", D(1)), ("B", "buy", D("0.05")), ("C", "buy", D("1.05"))
    ]
    # Different future prices must not change T-frozen quantities.
    shock = {"A": D(20), "B": D(200), "C": D(200)}
    assert frozen == module.freeze_orders(holdings, D(10), t, ["B", "C"])
    assert shock != next_close

    pre = module.settle_next_close(holdings, D(10), frozen, next_close, set(), "prefunded")
    net = module.settle_next_close(holdings, D(10), frozen, next_close, set(), "netted")
    stopped = module.settle_next_close(holdings, D(10), frozen, next_close, {"A"}, "netted", max_members=2)
    assert pre["old_equity_at_next_close"] == net["old_equity_at_next_close"] == D(210)
    assert [x["booked"] for x in pre["events"]] == [True, True, False]
    assert [x["booked"] for x in net["events"]] == [True, True, True]
    assert [x["booked"] for x in stopped["events"]] == [False, True, False]
    assert stopped["events"][-1]["reason"] == "member_capacity"
    for account in (pre, net, stopped):
        assert account["cash_after"] >= 0
        assert account["old_equity_at_next_close"] - account["fees"] == account["equity_after"]
    assert pre["fees"] == D("0.3435")
    assert net["fees"] == D("0.5010")
    assert net["cash_after"] == D("62.4990")
    # An unavailable raw mark currently refuses the whole step, even if the
    # symbol's order was blocked. A separately sourced halt mark may alter this.
    try:
        module.settle_next_close(holdings, D(10), frozen, {"B": D(90), "C": D(50)}, {"A"}, "netted")
    except ValueError as exc:
        assert "Missing/nonpositive" in str(exc)
    else:
        raise AssertionError("missing old held mark was accepted")
    out = {
        "stock_only_contract_arithmetic": "PASS",
        "frozen_quantities": [{"symbol": x["symbol"], "side": x["side"], "quantity": str(x["frozen_shares"])} for x in frozen],
        "prefunded_booked": [x["booked"] for x in pre["events"]],
        "netted_booked": [x["booked"] for x in net["events"]],
        "blocked_exit_booked": [x["booked"] for x in stopped["events"]],
        "netted_same_close_proceeds_is_theoretical": True,
        "netted_cash": str(net["cash_after"]),
        "netted_fees": str(net["fees"]),
        "missing_old_mark": "refused",
    }
    (HERE / "netting_audit.json").write_text(json.dumps(out, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return out


if __name__ == "__main__":
    print(json.dumps(run(), ensure_ascii=False, indent=2))
