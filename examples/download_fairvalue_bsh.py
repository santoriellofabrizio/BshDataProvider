"""
download_fairvalue_bsh.py — Fair value download adapted to use bsh_data_provider.

This script is an adaptation of the original download_fairvalue function.
All data retrieval (Bloomberg and Timescale) is routed through BshData instead
of calling BloombergClient and sfm_datalibrary directly.

Key substitutions vs. the original:
  - BloombergClient.reference_data_request()  → bsh.info.get(source='bloomberg', fields=['QUOTED_CRNCY'])
  - BloombergClient.intraday_data_request()   → bsh.market.get(source='bloomberg', snapshot_time=...)
  - BloombergClient.historical_data_request() → bsh.market.get(source='bloomberg', frequency='1d')
  - download_instruments_currency()           → bsh.info.get(source='oracle', fields=['CURRENCY'])
  - download_daily_fairvalues()               → bsh.market.get(source='timescale', snapshot_time=...)

The bbg_session parameter is removed: bsh_data_provider manages its own Bloomberg session.
"""

from typing import Dict
from datetime import date, time, datetime
import pandas as pd

from Common.Enums import MarketName, PriceSource
from Common.functions_common import get_connection_to_config_db

# bsh_data_provider entry point (replaces BloombergClient + sfm_datalibrary)
from interface.bshdata import BshData


# ---------------------------------------------------------------------------
# Instrument type mapping: DB INSTRUMENT_TYPE  →  bsh InstrumentType string
# ---------------------------------------------------------------------------
_INSTRUMENT_TYPE_MAP: Dict[str, str] = {
    'BOND': 'BOND',
    'EQUITY': 'STOCK',
    'STOCK': 'STOCK',
    'ETF': 'ETP',
    'ETP': 'ETP',
    'FX SPOT': 'CURRENCYPAIR',
    'FX FORWARD': 'FXFWD',
    'IRS': 'SWAP',
    'ZCIS': 'SWAP',
    'PERPETUAL CDS': 'CDXINDEX',
    'FUTURE': 'FUTURE',
    'INDEX': 'INDEX',
    'GENERIC TREASURY YIELD': 'BOND',
    'ON FINANCING RATE': 'BOND',
}


def _map_instrument_type(db_type: str) -> str:
    """Map a DB INSTRUMENT_TYPE string to a bsh InstrumentType string."""
    return _INSTRUMENT_TYPE_MAP.get(db_type, 'BOND')


def _df_to_currency_series(result, tickers: list) -> pd.Series:
    """
    Normalise a bsh info.get() result (DataFrame or dict) into a
    Series with ticker index and currency values.
    """
    if result is None:
        return pd.Series(dtype=str)

    if isinstance(result, pd.DataFrame) and not result.empty:
        col = result.columns[0]
        return result[col]

    if isinstance(result, dict):
        flat = {}
        for key, val in result.items():
            if isinstance(val, dict):
                # {ticker: {'QUOTED_CRNCY': 'EUR'}} → 'EUR'
                flat[key] = next(iter(val.values()), None)
            else:
                flat[key] = val
        return pd.Series(flat, dtype=str)

    return pd.Series(dtype=str)


# ---------------------------------------------------------------------------
# Main function
# ---------------------------------------------------------------------------

def download_fairvalue(start_date: date, end_date: date, fairvalue_time: time) -> None:
    """
    Download fair value prices for all configured instruments and write them to
    the HistoricalPrices table.

    Bloomberg prices use a point-in-time snapshot at fairvalue_time (ASK or TRADE
    event type depending on the instrument configuration, with a PX_LAST fallback
    at 17:00 for instruments where intraday data is unavailable).

    Timescale prices are fetched as a daily snapshot at fairvalue_time.

    Args:
        start_date:     First date (inclusive) for which prices are downloaded.
        end_date:       Last date (inclusive) for which prices are downloaded.
        fairvalue_time: Target intraday time used to pick the representative price.
    """

    # ------------------------------------------------------------------
    # 1. Read instrument list from config DB
    # ------------------------------------------------------------------
    conn_sql = get_connection_to_config_db()

    query = '''
        Select idc.INSTRUMENT_ID, idc.PRICE_SOURCE_MARKET, idc.PRICE_SOURCE_FIELD,
               ia.BLOOMBERG_CODE, ia.ISIN_CODE, ia.INSTRUMENT_TYPE
        from   InstrumentsDataConfig idc
        inner join InstrumentsAnagraphic ia on idc.INSTRUMENT_ID = ia.INSTRUMENT_ID
        where  idc.ENABLE_DOWNLOAD = 1
          and  ia.INSTRUMENT_TYPE not in ("GENERIC TREASURY YIELD", "ON FINANCING RATE")
    '''
    result, names = conn_sql.execute_query(query)
    instruments_to_download = pd.DataFrame(result, columns=names).set_index('INSTRUMENT_ID')

    # Validate that all enum values in the DB are known
    assert set(instruments_to_download['PRICE_SOURCE_FIELD'].values).issubset(
        set(ps.name for ps in PriceSource))
    assert set(instruments_to_download['PRICE_SOURCE_MARKET'].values).issubset(
        set(m.name for m in MarketName))

    # ------------------------------------------------------------------
    # 2. Initialise bsh_data_provider (single instance, manages all sessions)
    # ------------------------------------------------------------------
    bsh = BshData()

    # ================================================================
    # BLOOMBERG INSTRUMENTS
    # ================================================================
    bbg_mask = instruments_to_download['PRICE_SOURCE_MARKET'] == 'BBG'
    bbg_instruments = instruments_to_download[bbg_mask]

    if len(bbg_instruments) > 0:
        all_bbg_ids = bbg_instruments.index.tolist()

        bbg_tickers_mapping: Dict[str, str] = {
            instr_id: instruments_to_download.loc[instr_id, 'BLOOMBERG_CODE']
            for instr_id in all_bbg_ids
        }
        bbg_tickers_reverse_mapping: Dict[str, str] = {
            v: k for k, v in bbg_tickers_mapping.items()
        }

        # Split instruments by price-source field (ASK vs TRADE)
        ask_source_tickers = [
            bbg_tickers_mapping[i] for i in all_bbg_ids
            if instruments_to_download.loc[i, 'PRICE_SOURCE_FIELD'] == 'ASK'
        ]
        trade_source_tickers = [
            bbg_tickers_mapping[i] for i in all_bbg_ids
            if instruments_to_download.loc[i, 'PRICE_SOURCE_FIELD'] == 'TRADE'
        ]

        # --------------------------------------------------------------
        # 2a. Download instrument currencies from Bloomberg
        #     (skipping instrument types that don't have a meaningful CCY)
        # --------------------------------------------------------------
        no_ccy_types = {'ZCIS', 'FX SPOT', 'FX FORWARD', 'IRS', 'PERPETUAL CDS'}
        instruments_need_ccy = [
            i for i in all_bbg_ids
            if instruments_to_download.loc[i, 'INSTRUMENT_TYPE'] not in no_ccy_types
        ]
        ticker_to_download_ccy = [bbg_tickers_mapping[i] for i in instruments_need_ccy]

        # quoted_ccy_series: index=bloomberg_ticker, value=currency_string
        quoted_ccy_series = pd.Series(dtype=str)

        if ticker_to_download_ccy:
            # Primary attempt: QUOTED_CRNCY
            # Replaces: bbg_client.reference_data_request(fields_list=['QUOTED_CRNCY'])
            ccy_result = bsh.info.get(
                ticker=ticker_to_download_ccy,
                source='bloomberg',
                fields=['QUOTED_CRNCY'],
                request_type='reference',
            )
            quoted_ccy_series = _df_to_currency_series(ccy_result, ticker_to_download_ccy)
            quoted_ccy_series = quoted_ccy_series.rename('QUOTED_CRNCY')

            missing_ccy = [t for t in ticker_to_download_ccy if t not in quoted_ccy_series.index
                           or pd.isna(quoted_ccy_series.get(t))]

            # Fallback: CRNCY (for instruments where QUOTED_CRNCY is unavailable)
            if missing_ccy:
                ccy_fallback = bsh.info.get(
                    ticker=missing_ccy,
                    source='bloomberg',
                    fields=['CRNCY'],
                    request_type='reference',
                )
                fallback_series = _df_to_currency_series(ccy_fallback, missing_ccy)
                quoted_ccy_series = pd.concat([quoted_ccy_series, fallback_series])

                still_missing = [t for t in ticker_to_download_ccy if t not in quoted_ccy_series.index
                                 or pd.isna(quoted_ccy_series.get(t))]
                if still_missing:
                    raise Exception(
                        f'Missing currency (Bloomberg) for instruments: {still_missing}'
                    )

        # --------------------------------------------------------------
        # 2b. Download Bloomberg intraday snapshot prices
        #     Replaces: bbg_client.intraday_data_request(event_type='ASK'/'TRADE')
        #
        #     bsh.market.get() with snapshot_time uses Bloomberg's IntradayBarRequest
        #     per business day, returning the bar closest to fairvalue_time.
        #     Result: DataFrame(index=date, columns=bloomberg_ticker)
        # --------------------------------------------------------------
        print('Downloading data from BBG...', end='')

        def _download_bbg_snapshot(tickers: list, event_field: str) -> pd.DataFrame | None:
            """Download intraday snapshot prices for a list of Bloomberg tickers."""
            if not tickers:
                return None
            prices = bsh.market.get(
                type='BOND',           # Bloomberg routing is ticker-based; type is a hint
                ticker=tickers,
                subscription=tickers,  # explicit Bloomberg ticker as the API security string
                start=start_date,
                end=end_date,
                source='bloomberg',
                snapshot_time=fairvalue_time,
                fields=event_field,
                frequency='1d',
            )
            if prices is None or (isinstance(prices, pd.DataFrame) and prices.empty):
                return None
            return prices

        def _apply_px_last_fallback(
            prices: pd.DataFrame | None, tickers: list
        ) -> pd.DataFrame | None:
            """
            For missing instruments at 17:00, fall back to Bloomberg PX_LAST
            (daily historical close).
            Replaces: bbg_client.historical_data_request(fields_list=['PX_LAST'])
            """
            if not tickers or fairvalue_time.hour != 17:
                return prices

            if prices is not None and not prices.empty:
                missing = [t for t in tickers
                           if t not in prices.columns or prices[t].isna().all()]
            else:
                missing = tickers

            if not missing:
                return prices

            px_last = bsh.market.get(
                type='BOND',
                ticker=missing,
                subscription=missing,
                start=start_date,
                end=end_date,
                source='bloomberg',
                fields='PX_LAST',
                frequency='1d',
            )
            if px_last is None or (isinstance(px_last, pd.DataFrame) and px_last.empty):
                return prices

            if prices is None or prices.empty:
                return px_last
            return prices.combine_first(px_last)

        ask_source_prices = _download_bbg_snapshot(ask_source_tickers, 'ASK')
        ask_source_prices = _apply_px_last_fallback(ask_source_prices, ask_source_tickers)

        trade_source_prices = _download_bbg_snapshot(trade_source_tickers, 'TRADE')
        trade_source_prices = _apply_px_last_fallback(trade_source_prices, trade_source_tickers)

        # Merge ASK and TRADE price DataFrames
        if ask_source_prices is not None and trade_source_prices is not None:
            all_bbg_prices = pd.concat([ask_source_prices, trade_source_prices], axis=1)
        elif ask_source_prices is not None:
            all_bbg_prices = ask_source_prices
        else:
            all_bbg_prices = trade_source_prices

        if all_bbg_prices is not None and not all_bbg_prices.empty:
            print('BBG Download Completed!')

            insert_query = (
                'INSERT OR REPLACE INTO HistoricalPrices '
                '(DATETIME, INSTRUMENT_ID, PRICE, CURRENCY) VALUES (?, ?, ?, ?)'
            )
            for date_idx, row in all_bbg_prices.iterrows():
                # Normalise index to a plain date
                date_val = date_idx.date() if hasattr(date_idx, 'date') else date_idx
                target_dt = datetime.combine(date_val, fairvalue_time)

                for ticker, price in row.items():
                    if pd.isna(price):
                        continue
                    instrument_id = bbg_tickers_reverse_mapping.get(ticker)
                    if instrument_id is None:
                        continue
                    currency = (
                        quoted_ccy_series.get(ticker, 'NO CURRENCY')
                        if ticker in quoted_ccy_series.index
                        else 'NO CURRENCY'
                    )
                    if pd.isna(currency):
                        currency = 'NO CURRENCY'
                    conn_sql.execute_query(insert_query, [target_dt, instrument_id, price, currency])

    # ================================================================
    # TIMESCALE INSTRUMENTS
    # ================================================================
    ts_mask = instruments_to_download['PRICE_SOURCE_MARKET'] != 'BBG'
    all_timescale_ids = instruments_to_download[ts_mask].index.tolist()

    if len(all_timescale_ids) > 0:
        print('Downloading data from Timescale...', end='')

        timescale_isin_mapping: Dict[str, str] = {
            instr_id: instruments_to_download.loc[instr_id, 'ISIN_CODE']
            for instr_id in all_timescale_ids
        }
        timescale_isin_reverse_mapping: Dict[str, str] = {
            v: k for k, v in timescale_isin_mapping.items()
        }

        all_ts_isins = list(timescale_isin_mapping.values())

        # --------------------------------------------------------------
        # 3a. Download instrument currencies via Oracle
        #     Replaces: download_instruments_currency() from sfm_datalibrary
        # --------------------------------------------------------------
        # oracle_ccy_series: index=isin, value=currency_string
        try:
            ccy_info = bsh.info.get(
                isin=all_ts_isins,
                source='oracle',
                fields=['CURRENCY'],
                request_type='reference',
            )
            oracle_ccy_series = _df_to_currency_series(ccy_info, all_ts_isins)
        except Exception:
            oracle_ccy_series = pd.Series(dtype=str)

        # Fill any ISINs missing from the oracle result with 'NO CURRENCY'
        missing_ccy_isins = [i for i in all_ts_isins
                             if i not in oracle_ccy_series.index
                             or pd.isna(oracle_ccy_series.get(i))]
        if missing_ccy_isins:
            oracle_ccy_series = pd.concat([
                oracle_ccy_series,
                pd.Series('NO CURRENCY', index=missing_ccy_isins, dtype=str),
            ])

        # --------------------------------------------------------------
        # 3b. Build per-instrument metadata lists for bsh dispatch
        # --------------------------------------------------------------
        ts_isins = []
        ts_types = []
        ts_markets = []
        ts_currencies = []

        for instr_id in all_timescale_ids:
            isin = timescale_isin_mapping[instr_id]
            db_type = instruments_to_download.loc[instr_id, 'INSTRUMENT_TYPE']
            market = instruments_to_download.loc[instr_id, 'PRICE_SOURCE_MARKET']
            ccy = oracle_ccy_series.get(isin, 'NO CURRENCY')

            ts_isins.append(isin)
            ts_types.append(_map_instrument_type(db_type))
            ts_markets.append(market)
            ts_currencies.append(None if ccy == 'NO CURRENCY' else ccy)

        # --------------------------------------------------------------
        # 3c. Download Timescale snapshot prices
        #     Replaces: download_daily_fairvalues() from sfm_datalibrary
        #
        #     bsh.market.get() with snapshot_time filters each day's timeseries
        #     to the bar nearest fairvalue_time.
        #     Result: DataFrame(index=date, columns=isin)
        # --------------------------------------------------------------
        ts_prices = bsh.market.get(
            type=ts_types,
            isin=ts_isins,
            start=start_date,
            end=end_date,
            source='timescale',
            market=ts_markets,
            snapshot_time=fairvalue_time,
            fields='MID',
            frequency='1d',
            currency=ts_currencies,
        )

        print('Timescale Download Completed!')

        if ts_prices is not None and isinstance(ts_prices, pd.DataFrame) and not ts_prices.empty:
            # Adjust timestamps so they reflect the exact fairvalue_time
            ts_prices.index = pd.to_datetime(ts_prices.index)
            ts_prices.index = [
                d.replace(hour=fairvalue_time.hour, minute=fairvalue_time.minute,
                          second=fairvalue_time.second)
                for d in ts_prices.index
            ]

            insert_query = (
                'INSERT OR REPLACE INTO HistoricalPrices '
                '(DATETIME, INSTRUMENT_ID, PRICE, CURRENCY) VALUES (?, ?, ?, ?)'
            )
            for date_idx, row in ts_prices.iterrows():
                target_dt = date_idx.to_pydatetime() if hasattr(date_idx, 'to_pydatetime') else date_idx

                for isin, price in row.items():
                    if pd.isna(price):
                        continue
                    instrument_id = timescale_isin_reverse_mapping.get(isin)
                    if instrument_id is None:
                        continue
                    ccy = oracle_ccy_series.get(isin, 'NO CURRENCY')
                    if pd.isna(ccy):
                        ccy = 'NO CURRENCY'
                    conn_sql.execute_query(insert_query, [target_dt, instrument_id, price, ccy])

    conn_sql.commit()
    conn_sql.close()
