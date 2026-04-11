from pathlib import Path

import microcap_top100_mom16_biweekly_live as base


ROOT = Path(__file__).resolve().parent

base.OUTPUT_DIR = ROOT
base.TOP_N = 50
base.DEFAULT_INDEX_CSV = ROOT / "wind_microcap_top_50_biweekly_thursday_16y_cached.csv"
base.DEFAULT_OUTPUT_PREFIX = "microcap_top50_mom16_biweekly_live"
base.DEFAULT_COSTED_NAV_CSV = ROOT / "microcap_top50_mom16_hedge_zz1000_biweekly_thursday_16y_costed_nav.csv"
base.UNIVERSE_LABEL = "Top50"
base.STRATEGY_TITLE = "Top50 Microcap Mom16 Biweekly"
base.INDEX_CODE = "TOP50_BIWEEKLY_THURSDAY_PROXY"


if __name__ == "__main__":
    base.main()
