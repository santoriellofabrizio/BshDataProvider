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


def adjustment(start):
    etf_tickers = ["IUSE","IUSA"]
    start = "2025-11-01"

    api = BshData(autocomplete=True)
    factory = InstrumentFactory()

    # ============================================================
    # 1. BUILD INSTRUMENTS
    # ============================================================
    etf_instruments = [
        factory.create(ticker=t, type=InstrumentType.ETP, autocomplete=True)
        for t in etf_tickers
    ]

    all_instruments = etf_instruments
    instruments_dict = {inst.id: inst for inst in all_instruments}

    # ============================================================
    # 2. GET PRICES WITH INSTRUMENTS
    # ============================================================
    prices_etf = api.market.get_daily_etf(
        ticker=etf_tickers,
        start=start)
    #
    # prices_future = api.market.get_daily_future(
    #     id=future_id,
    #     source="bloomberg",
    #     snapshot_time="16:00:00",
    #     start=start)

    # prices = pd.concat([prices_etf, prices_future], axis=1)
    prices = prices_etf


    # ============================================================
    # 3. GET REPO RATES
    # ============================================================
    instrument_currencies = list(set(inst.currency.value for inst in all_instruments))

    repo_rates_for_currency = api.market.get_daily_repo_rates(
        start=start,
        currencies=instrument_currencies,
    )

    # ============================================================
    # 4. GET FX & STATIC DATA
    # ============================================================
    fx_composition = api.info.get_fx_composition(ticker=etf_tickers, fx_fxfwrd="fx")
    fx_forward_composition = api.info.get_fx_composition(ticker=etf_tickers, fx_fxfwrd="fxfwrd")
    fx_composition.loc["IUSE"] = 0

    all_fx_currencies = set(fx_composition.columns) | set(fx_forward_composition.columns)
    fx = api.market.get_daily_currency(
        id=[f"EUR{ccy}" for ccy in all_fx_currencies],
        start=start,
    )

    currencies_hedged = fx_forward_composition.columns.tolist()
    fx_forward_prices = api.market.get_daily_fx_forward(quoted_currency=currencies_hedged, start=start)

    dividends = api.info.get_dividends(ticker=etf_tickers, start=start)
    ter = api.info.get_ter(ticker=etf_tickers)

    # ============================================================
    # 5. BUILD ADJUSTER WITH INSTRUMENTS
    # ============================================================
    adjuster = (Adjuster(prices=prices)
                .add(FxSpotComponent(fx_composition,fx_prices=fx))
                .add(FxForwardCarryComponent(fx_forward_composition, fx_forward_prices, "1M", fx))
                .add(TerComponent(ter))
                .add(DividendComponent(dividends, fx_prices=fx)))

    adjustments = adjuster.calculate()
    breakdown = adjuster.get_breakdown()
    returns = prices.pct_change()

    cleans_returns = returns.add(adjustments, fill_value=0)

    rebased_clean = ((1 + cleans_returns).cumprod() - 1)

    rebased_clean.plot(figsize=(15, 5))
    print(rebased_clean.iloc[-1]*100)
    plt.show()

    (prices/prices.iloc[0] - 1).plot(figsize=(15, 5))
    plt.show()

    return adjustments





if __name__ == "__main__":
    start = "2025-08-01"
    adjustments = adjustment(start)