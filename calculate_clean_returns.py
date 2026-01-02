from datetime import time

from matplotlib import pyplot as plt

from analytics.adjustments import Adjuster
from analytics.adjustments.dividend import DividendComponent
from analytics.adjustments.fx_forward_carry import FxForwardCarryComponent
from analytics.adjustments.fx_spot import FxSpotComponent
from analytics.adjustments.ter import TerComponent
from core.enums.currencies import CurrencyEnum
from interface.bshdata import BshData


def test_comparison_adjustment_equity(start, end, tickers, snapshot_time=time(16)):

    api = BshData()
    currencies = [c for c in CurrencyEnum.__members__]
    eur_currency_list = [f"EUR{c}" for c in currencies]

    etf_prices = api.market.get_daily_etf(start=start, end=end, ticker=tickers, snapshot_time=snapshot_time)
    fx_prices = api.market.get_daily_currency(start=start, end=end, id=eur_currency_list, snapshot_time=snapshot_time)

    dividends = api.info.get_dividends(start=start, end=end, ticker=tickers)
    ter = api.info.get_ter(ticker=tickers)/100
    fx_comp = api.info.get_fx_composition(ticker=tickers, fx_fxfwrd="fx")
    fx_forward_comp = api.info.get_fx_composition(ticker=tickers, fx_fxfwrd="fxfwrd")

    fx_forward_prices = api.market.get_fx_forward_prices(base_currency="EUR",
                                                         quoted_currency=currencies,
                                                         start=start,
                                                         end=end,
                                                         tenor="1M")

    adjuster = Adjuster(etf_prices).add(
        FxSpotComponent(fx_comp, fx_prices)).add(
        FxForwardCarryComponent(fx_forward_comp, fx_forward_prices, "1M", fx_prices)).add(
        TerComponent(ter)).add(
        DividendComponent(dividends, fx_prices))

    returns = adjuster.clean_returns()
    clean_prices = adjuster.clean_returns()

    clean_prices.plot()
    plt.show()



if __name__ == "__main__":

    start = "2025-09-01"
    end = "2025-12-31"
    tickers = ["AWSRIA","AWSRIE"]
    test_comparison_adjustment_equity(start, end, tickers)
