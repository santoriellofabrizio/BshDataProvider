import datetime

import pandas as pd
from dateutil.utils import today
from sfm_data_provider.interface.bshdata import BshData, BshDataSingleton
from sfm_utilities.addin import export_to_sfm_add_in, CustomCategory, CellsRange1D

__all__ = ['daily_etf_price', 'daily_etf_price_array']


@export_to_sfm_add_in(CustomCategory.MARKETS_TRADES_VOLUMES,
                      dict(start_date="The date starting from which the daily prices are returned",
                           end_date="The last date for which the daily prices are returned. Defaults to today.",
                           etf_id="Identifier of the ETF. Defaults to None.",
                           isin="ISIN of the ETF. Defaults to None.",
                           ticker="Ticker of the ETF. Defaults to None.",
                           fields="Field from which to extract the daily prices. Defaults to 'MID'.",
                           source="The data source from which to download the data. Defaults to 'timescale'.",
                           curr="The currency in which to extract the daily prices. Defaults to 'EUR'.",
                           snapshot_time="Time at which is the prices are screenshot. Defaults to '17:00:00'."
                           )
                      )
def daily_etf_price_array(
        start_date: datetime.datetime,
        end_date: datetime.datetime = today(),
        etf_id: CellsRange1D[str] = None,
        isin: CellsRange1D[str] = None,
        ticker: CellsRange1D[str] = None,
        fields: CellsRange1D[str] = "MID",
        source: CellsRange1D[str] = "timescale",
        curr: CellsRange1D[str] = "EUR",
        snapshot_time: CellsRange1D[str] = "17:00:00",
) -> pd.DataFrame:
    """Download daily etf prices from the desired data source."""
    data_provider = BshDataSingleton(config_path=None, add_in_mode=True)
    results = data_provider.market.get_daily_etf(
        start=start_date.date(),
        end=end_date.date(),
        id=etf_id,
        isin=isin,
        ticker=ticker,
        fields=fields,
        source=source,
        currency=curr,
        snapshot_time=snapshot_time
    )

    if isinstance(results, pd.Series):
        results = results.to_frame()

    return results


@export_to_sfm_add_in(CustomCategory.MARKETS_TRADES_VOLUMES,
                      dict(date_input="date to download",
                           etf_id="Identifier of the ETF. Defaults to None.",
                           isin="ISIN of the ETF. Defaults to None.",
                           ticker="Ticker of the ETF. Defaults to None.",
                           fields="Field from which to extract the daily prices. Defaults to 'MID'.",
                           source="The data source from which to download the data. Defaults to 'timescale'.",
                           curr="The currency in which to extract the daily prices. Defaults to 'EUR'.",
                           snapshot_time="Time at which is the prices are screenshot. Defaults to '17:00:00'."
                           )
                      )
def daily_etf_price(
        date_input: datetime.datetime,
        etf_id: CellsRange1D[str] = None,
        isin: CellsRange1D[str] = None,
        ticker: CellsRange1D[str] = None,
        fields: CellsRange1D[str] = "MID",
        source: CellsRange1D[str] = "timescale",
        curr: CellsRange1D[str] = "EUR",
        snapshot_time: CellsRange1D[str] = "17:00:00",
) -> float:
    """Download daily etf prices from the desired data source."""
    data_provider = BshDataSingleton(config_path=None, add_in_mode=True)
    results = data_provider.market.get_daily_etf(
        start=date_input.date(),
        end=date_input.date(),
        id=etf_id,
        isin=isin,
        ticker=ticker,
        fields=fields,
        source=source,
        currency=curr,
        snapshot_time=snapshot_time
    )

    return results.iloc[0]
