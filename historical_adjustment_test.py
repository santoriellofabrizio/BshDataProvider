from dateutil.utils import today
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

from analytics.adjustments import Adjuster
from analytics.adjustments.dividend import DividendComponent
from analytics.adjustments.fx_forward_carry import FxForwardCarryComponent
from analytics.adjustments.fx_spot import FxSpotComponent
from analytics.adjustments.repo import RepoComponent
from analytics.adjustments.ter import TerComponent
from core.enums.instrument_types import InstrumentType
from core.instruments.instrument_factory import InstrumentFactory
from interface.bshdata import BshData

def as_isin(ticker: str, map):
    rev_map = {v:k for k,v in map.items()}
    if isinstance(ticker, str):
        return rev_map[ticker]
    return [rev_map[t] for t in ticker]


def adjustment(start):
    etf_tickers = ["XMAW","XMAE"]
    api = BshData(autocomplete=True)

    # ============================================================
    # 4. GET FX & STATIC DATA
    # ============================================================
    prices = pd.read_parquet("data/etf_prices.parquet")
    fx_composition = pd.read_parquet("data/fx_composition.parquet")
    fx_forward_composition = pd.read_parquet("data/fx_forward.parquet")

    all_fx_currencies = set(fx_composition.columns) | set(fx_forward_composition.columns)
    fx = pd.read_parquet("data/fx_prices.parquet")

    currencies_hedged = fx_forward_composition.columns.tolist()
    fx_forward_prices = pd.read_parquet("data/fx_forward_prices.parquet")

    dividends = pd.read_parquet("data/dividends.parquet")
    ter = pd.read_parquet("data/ter.parquet")

    tickers = api.info.get_etp_fields(fields=["TICKER"], isin=prices.columns.tolist())
    tickers_map = tickers["TICKER"].to_dict()

    # ============================================================
    # 5. BUILD ADJUSTER WITH INSTRUMENTS
    # ============================================================
    adjuster = (Adjuster(prices=prices, fill_method="time")
                .add(FxSpotComponent(fx_composition,fx_prices=fx))
                .add(FxForwardCarryComponent(fx_forward_composition, fx_forward_prices, "1M", fx))
                .add(TerComponent(ter))
                .add(DividendComponent(dividends, fx_prices=fx)))

    adjustments = adjuster.calculate()
    breakdown = adjuster.get_breakdown(as_isin("HWDE", tickers_map))
    print(breakdown)
    rebased_clean = adjuster.clean_prices(
        rebase=True).rename(
        tickers_map,
        axis='columns')[etf_tickers]

    rebased_clean.plot(figsize=(15, 5))
    print(rebased_clean.iloc[-1]*100)
    plt.show()
    return adjustments


if __name__ == "__main__":
    start = "2025-08-01"
    adjustments = adjustment(start)
