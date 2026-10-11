"""Focused adversarial checks for rights marking, cash and board regimes."""

from decimal import Decimal as D

from extend_ledger import require_ex_raw, side_limit_pct
from ledger_contract import settle_next_close


class FakeSource:
    def __init__(self, pmap):
        self.pmap = pmap

    def price_map(self, symbol):
        return self.pmap.get(symbol, {})

    def list_date(self, symbol):
        return min(self.price_map(symbol))


def main():
    item = {"symbol": "600211"}
    try:
        require_ex_raw(FakeSource({"600211": {"2010-09-06": D("10")}}), item, "2010-09-07")
        raise AssertionError("Missing ex-date raw was accepted")
    except ValueError as exc:
        assert str(exc) == "unpriced_ex_action_no_raw_close:2010-09-07:600211"
    require_ex_raw(FakeSource({"002049": {"2010-04-08": D("14.42")}}), {"symbol": "002049"}, "2010-04-08")

    fake = FakeSource({"688001": {f"2019-07-{d:02d}": D("10") for d in range(22, 28)},
        "300001": {"2010-01-04": D("10")}, "600001": {"2010-01-04": D("10")}})
    assert side_limit_pct(fake, "300001", "2020-08-21", "0") == D("0.10")
    assert side_limit_pct(fake, "300001", "2020-08-24", "1") == D("0.20")
    assert side_limit_pct(fake, "600001", "2026-07-03", "1") == D("0.05")
    assert side_limit_pct(fake, "600001", "2026-07-06", "1") == D("0.10")
    assert side_limit_pct(fake, "688001", "2019-07-26", "0") is None
    assert side_limit_pct(fake, "688001", "2019-07-27", "0") == D("0.20")

    # 101 holdings from a passive stock credit are legal book state.  An
    # active purchase of a new 102nd name must still be rejected.
    positions = {f"{n:06d}": D(1) for n in range(101)}
    out = settle_next_close(positions, D(10),
        [{"symbol": "999999", "side": "buy", "frozen_shares": D(1)}],
        {**{s: D(1) for s in positions}, "999999": D(1)}, set(), "netted")
    assert len(out["positions_after"]) == 101
    assert out["events"][0]["reason"] == "member_capacity"
    assert out["cash_after"] == D(10)
    print("PASS: missing ex-date raw rejected; date/board limits; passive 101st share capacity")


if __name__ == "__main__":
    main()
