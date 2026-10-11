"""Attack actual signal guard and settlement with delayed/blocked next session."""

import ast
import hashlib
import importlib.util
import json
import sys
from decimal import Decimal as D
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
ENGINE = ROOT / "engine"
SOURCE = ENGINE / "full_history.py"


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def load_engine():
    sys.path.insert(0, str(ENGINE))
    spec = importlib.util.spec_from_file_location("full_history_adversarial_import", SOURCE)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def extract_real_signal_pending_guard():
    tree = ast.parse(SOURCE.read_text(encoding="utf-8"))
    candidates = [node for node in ast.walk(tree) if isinstance(node, ast.If) and ast.unparse(node.test) == "pending_shares" and "pending_bonus_not_next_session" in ast.unparse(node)]
    assert len(candidates) == 1
    guard = candidates[0]
    template = ast.parse("def probe(date, dates, i, entitlements, pending_shares, positions):\n    signal_positions = dict(positions)\n    return signal_positions\n")
    function = template.body[0]
    function.body.insert(1, guard)
    compiled = compile(ast.fix_missing_locations(template), "<extracted_real_pending_guard>", "exec")
    namespace = {"D":D, "ValueError":ValueError}
    exec(compiled, namespace)
    return namespace["probe"], ast.unparse(guard)


class FakeSource:
    def __init__(self, bar, pmap):
        self._bar = bar
        self._pmap = pmap
        self.previous_market_day = {"2018-06-08":"2018-06-07"}
        self.action_exdates = set()

    def price_map(self, symbol):
        return self._pmap

    def bar(self, symbol, date):
        return self._bar


def main():
    manifest = json.loads((ENGINE/"full_history_result.json").read_text(encoding="utf-8"))
    assert sha(SOURCE) == manifest["code_sha256"], "stale_code_hash"
    engine = load_engine()
    probe, guard_source = extract_real_signal_pending_guard()
    assert "stock_credited" in guard_source and "next_date" in guard_source
    dates = ["2018-06-07","2018-06-08"]
    entitlement = {"active":True,"bonus_due":D("40"),"stock_credited":"2018-06-08","symbol":"603031"}
    accepted = probe(dates[0],dates,0,[entitlement],{"603031":D("40")},{"603031":D("100")})
    assert accepted == {"603031":D("140")}
    rejected = {}
    for name, due, pending in (("release_late","2018-06-11",D("40")),("rights_quantity_mismatch","2018-06-08",D("41"))):
        try:
            probe(dates[0],dates,0,[dict(entitlement,stock_credited=due)],{"603031":pending},{"603031":D("100")})
        except ValueError as exc:
            rejected[name] = str(exc)
        else:
            raise AssertionError(f"mutant_accepted:{name}")
    assert "pending_bonus_not_next_session" in rejected["release_late"]
    assert "pending_bonus_rights_mismatch" in rejected["rights_quantity_mismatch"]

    halted = FakeSource({"date":"2018-06-08","preclose":"15.87","close":"15.87","volume":"0","tradestatus":"0","isST":"0"},{"2018-06-07":D("15.87")})
    halt_reason = engine.check_paper_trade(halted,"603031","2018-06-08","sell",D("15.87"))
    assert halt_reason == "suspended_or_zero_volume"
    limit = FakeSource({"date":"2018-06-08","preclose":"15.87","close":"14.28","volume":"100000","tradestatus":"1","isST":"0"},{"2018-06-07":D("15.87"),"2018-06-08":D("14.28")})
    limit_reason = engine.check_paper_trade(limit,"603031","2018-06-08","sell",D("14.28"))
    assert limit_reason == "at_side_close_limit_diagnostic_ST_and_board"
    frozen = [{"symbol":"603031","side":"sell","frozen_shares":D("40")},{"symbol":"000001","side":"buy","frozen_shares":D("20")}]
    cash_blocks = {}
    for name, price, reason in (("halt",D("15.87"),halt_reason),("sell_side_limit",D("14.28"),limit_reason)):
        result = engine.settle_next_close({"603031":D("140")},D(0),frozen,{"603031":price,"000001":D("10")},{"603031"},"netted")
        assert result["positions_after"] == {"603031":D("140")}
        assert result["cash_after"] == 0
        assert [(e["side"],e["booked"],e["reason"]) for e in result["events"]] == [("sell",False,"blocked_trade"),("buy",False,"insufficient_netted_cash")]
        cash_blocks[name] = {"reason":reason,"orders":[{"side":e["side"],"booked":e["booked"],"reason":e["reason"]} for e in result["events"]]}
    output = {"status":"PASS_ACTUAL_GUARD_AND_SETTLEMENT_MUTANTS","manifest_sha256":sha(ENGINE/"full_history_result.json"),"code_sha256":sha(SOURCE),"accepted_next_session_signal_shares":str(accepted["603031"]),"guard_rejections":rejected,"blocked_execution_and_no_unrealized_netted_cash":cash_blocks,"limitations":"Synthetic injections exercise the real signal guard and settlement functions; they are not historical execution evidence or proof of real fills."}
    (HERE/"603031_signal_mutants_adversarial_audit.json").write_text(json.dumps(output,ensure_ascii=False,indent=2)+"\n",encoding="utf-8")
    print(json.dumps(output,ensure_ascii=False,indent=2))


if __name__ == "__main__":
    main()
