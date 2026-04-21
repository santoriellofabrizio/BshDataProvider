"""
market_data_api.py — Unified API for dynamic market data.

This module defines the :class:`MarketDataAPI`, a high-level interface for querying
market time-series and is_intraday data (ETF, futures, FX, equities) from multiple providers
(e.g., Timescale, Bloomberg). It standardizes the construction, dispatch, and aggregation
of requests for dynamic data such as prices, NAVs, or fair values.

Responsibilities:
    - Manage both daily and is_intraday data requests
    - Normalize identifiers and query parameters
    - Handle provider dispatch and result aggregation
    - Integrate caching and auto-completion via BaseAPI

Typical usage:
    >>> from sfm_data_provider.client import BSHDataClient
    >>> client = BSHDataClient()
    >>> api = MarketDataAPI(client)
    >>> df = api.get_daily_etf("2024-01-01", "2024-02-01", isin="IE00B4L5Y983")
    >>> fx = api.get_intraday_fx(date="2024-03-01", id="EURUSD", frequency="5m")
    >>> snap = api.get_day_snapshot_future(date="2024-02-20", ticker="FESXZ4 Index")
"""

import uuid
from datetime import datetime, time
import datetime as dt

import pandas as pd
from dateutil.utils import today
from typing import Union, Optional, List, Dict, Any

from sfm_data_provider.core.decorators.respect_cache_status import respect_cache_kwarg
from sfm_data_provider.core.enums.instrument_types import InstrumentType
from sfm_data_provider.core.requests.request_builder.request_builder import RequestBuilder
from sfm_data_provider.core.utils.common import normalize_list, normalize_param
from sfm_data_provider.core.utils.merge_utils import merge_incomplete_results

from sfm_data_provider.interface.api.base_api import BaseAPI


class MarketDataAPI(BaseAPI):
    """
    High-level API for market data retrieval (historical, is_intraday, and snapshots).

    This class provides a unified interface to fetch time-series
    data from multiple sources. It manages request creation, provider dispatch,
    and aggregation into a consistent output structure.

    Responsibilities:
        - Build and send market data requests and instruments (via InstrumentFactory) through the unified data client
        - Normalize and validate parameters (dates, instruments, frequencies)
        - Support both historical (daily) and is_intraday data retrieval
        - Provide convenience wrappers for ETF, Futures, and FX instruments
        - Integrate with caching, autocomplete, and BaseAPI utilities
    """

    # ============================================================
    # INTERNAL HELPERS
    # ============================================================

    def _dispatch(
            self,
            instruments: list,
            fields: Union[str, List[str]],
            source: Union[str, List[str], Dict[str, str]],
            subscriptions: Optional[Union[str, List[str], Dict[str, str]]] = None,
            market: Optional[Union[str, List[str], Dict[str, str]]] = None,
            request_type: Optional[Union[str, List[str]]] = None,
            fallbacks: Optional[List[Dict[str, Any]]] = None,
            **kwargs,
    ):
        """
        Generic dispatcher with automatic retry via fallbacks.

        Responsibilities:
            - Normalize parameters per instrument
            - Build request objects using the RequestBuilder
            - Send requests to the unified data client
            - On partial/incomplete results, retry with fallback configs
            - Aggregate and return combined results

        Args:
            instruments (list): List of instrument objects to query.
            fields (str | list[str]): Data fields to request.
            source (str | list[str] | dict[str, str]): Data source(s) (e.g., 'timescale', 'bloomberg').
            subscriptions (str | list[str] | dict[str, str], optional): Optional subscription identifiers.
            market (str | list[str] | dict[str, str], optional): Market codes (e.g., 'ETFP', 'EUREX').
            request_type: Type of request.
            fallbacks (list[dict], optional): Alternative configs to retry on incomplete results.
            **kwargs: Additional parameters forwarded to RequestBuilder.

        Returns:
            dict | pd.DataFrame | None: Aggregated results from all attempts.
        """
        if not instruments:
            return None

        fields = [fields] if isinstance(fields, str) else fields
        market = normalize_param(market, instruments, default=None)
        source = normalize_param(source, instruments, default=None)
        subscriptions = normalize_param(subscriptions, instruments, default=None)

        requests = []

        for i, inst in enumerate(instruments):
            req = RequestBuilder.build_market_request(
                instrument=inst,
                fields=fields,
                market=market[i],
                source=source[i],
                subscription=subscriptions[i],
                request_type=request_type,
                **kwargs,
            )
            requests.append(req)

        batch_id = str(uuid.uuid4())
        self.log_request(f"[dispatch] batch={batch_id} MarketRequest n={len(requests)}")

        result = self.client.send(requests)

        # Check if we need fallbacks
        if fallbacks:
            incomplete = self.client.tracker.get_failed() + self.client.tracker.get_incomplete()
            if incomplete:
                self.log_request(
                    f"[fallback] {len(incomplete)} incomplete requests, trying {len(fallbacks)} fallback(s)"
                )
                result = self._retry_with_fallbacks(
                    incomplete_statuses=incomplete,
                    fallbacks=fallbacks,
                    current_result=result,
                    fields=fields,
                    request_type=request_type,
                    **kwargs
                )

        return result

    def _retry_with_fallbacks(
            self,
            incomplete_statuses: List[Any],
            fallbacks: List[Dict[str, Any]],
            current_result: Dict[str, Any],
            fields: List[str],
            request_type: Optional[str] = None,
            **kwargs,
    ) -> Dict[str, Any]:
        """
        Retry incomplete requests with fallback configurations.

        Args:
            incomplete_statuses: List of RequestStatus objects that are incomplete/failed.
            fallbacks: List of alternative configs to try.
            current_result: Aggregated result from first attempt.
            fields: Fields that were requested.
            request_type: Type of request.
            **kwargs: Additional parameters.

        Returns:
            dict: Merged results from all attempts.
        """
        merged_result = dict(current_result) if current_result else {}

        for fallback_idx, fallback_config in enumerate(fallbacks):
            self.log_request(
                f"[fallback {fallback_idx + 1}/{len(fallbacks)}] Retrying with config: {fallback_config}"
            )

            # Extract original instruments from incomplete statuses
            retry_instruments = [s.request.instrument for s in incomplete_statuses]
            # Override parameters from fallback config
            retry_source = normalize_param(fallback_config.get("source"), retry_instruments, default=None)
            retry_market = normalize_param(fallback_config.get("market"), retry_instruments, default=None)
            retry_subscriptions = normalize_param(fallback_config.get("subscriptions"), retry_instruments, default=None)

            # Build retry requests
            retry_requests = []
            for i, inst in enumerate(retry_instruments):
                req = RequestBuilder.build_market_request(
                    instrument=inst,
                    fields=fields,
                    market=retry_market[i],
                    source=retry_source[i],
                    subscription=retry_subscriptions[i],
                    request_type=request_type,
                    **kwargs,
                )
                retry_requests.append(req)

            # Send retry requests
            retry_result = self.client.send(retry_requests)

            # Merge intelligente: aggiorna SOLO le date/valori con NaN
            if retry_result:
                merged_result = merge_incomplete_results(
                    original_results=merged_result,
                    retry_results=retry_result,
                    incomplete_statuses=incomplete_statuses,
                )

            # Check if all incomplete are now complete
            still_incomplete = self.client.tracker.get_incomplete()
            if not still_incomplete:
                self.log_request("[fallback] All requests now complete")
                break

        return merged_result

    # ============================================================
    # MarketDataAPI - get() con overload
    # ============================================================

    @respect_cache_kwarg
    def get(
            self,
            type: str = None,
            start: Optional[Union[str, dt.date, datetime]] = None,
            end: Optional[Union[str, dt.date, datetime]] = None,
            id: Optional[Union[str, List[str]]] = None,
            isin: Optional[Union[str, List[str]]] = None,
            ticker: Optional[Union[str, List[str]]] = None,
            instruments: Optional[List] = None,  # ← NEW
            market: Optional[Union[str, List[str], Dict[str, str]]] = None,
            source: Optional[Union[str, List[str], Dict[str, str]]] = None,
            frequency: Optional[str] = "1d",
            fields: Union[str, List[str]] = "MID",
            currency: Union[str, List[str], Dict[str, str]] = "EUR",
            snapshot_time: Optional[Union[str, time]] = None,
            subscription: Optional[Union[str, List[str], Dict[str, str]]] = None,
            autocomplete: Optional[bool] = None,
            fallbacks: Optional[List[Dict[str, Any]]] = None,
            **extra_params,
    ):
        """
        Retrieve market time-series. Two modes:

        Mode 1 - Build instruments from identifiers:
            get(type='ETP', ticker='IUSA', start='2024-01-01', end='2024-12-31')

        Mode 2 - Use pre-built instruments:
            get(instruments=[etf1, etf2], start='2024-01-01', end='2024-12-31')

        Dict mode support (NEW):
            Parameters like currency, market, source, subscription can be passed as:
            - Single value: "USD" (replicated to all instruments)
            - List: ["USD", "EUR", "GBP"] (aligned with instruments)
            - Dict: {"AAPL": "USD", "MSFT": "EUR"} (mapped by instrument ID, default for others)

        Example with dict mode:
            get(
                type='ETP',
                ticker=['IUSA', 'VUSA', 'CSPX'],
                currency={"IUSA": "USD", "CSPX": "GBP"},  # VUSA gets default EUR
                source={"CSPX": "oracle"},  # IUSA, VUSA get default
                start='2024-01-01',
                end='2024-12-31'
            )
        """
        self.client.tracker.reset()

        # Mode 2: pre-built instruments
        if instruments is not None:
            return self.get_with_instruments(
                instruments=instruments,
                fields=fields,
                source=source,
                subscription=subscription,
                market=market,
                frequency=frequency,
                snapshot_time=snapshot_time,
                start=start,
                end=end,
                fallbacks=fallbacks,
                **extra_params
            )

        # Mode 1: build instruments
        auto = self.autocomplete if autocomplete is None else autocomplete
        ids, isins, tickers = self._resolve_identifiers(id, isin, ticker, autocomplete=auto)
        n = len(ids)

        # Create mock instruments with IDs for normalize_param
        class _InitInstrument:
            def __init__(self, id_): self.id = id_
        init_instruments = [_InitInstrument(id_) for id_ in ids]

        currency = normalize_param(currency, init_instruments, default="EUR")
        market = normalize_param(market, init_instruments, default=None)
        type_ = normalize_param(type, init_instruments, default=None)

        start = self._parse_datetime(start) if isinstance(start, (datetime, str)) else\
            datetime.combine(self._parse_date(start), datetime.min.time())
        end = self._parse_datetime(end) if isinstance(end, (datetime, str)) else\
            datetime.combine(self._parse_date(end), datetime.max.time())

        # Separate instrument-building params from request params
        instrument_build_params = {
            k: v for k, v in extra_params.items()
            if k not in ['fields', 'source', 'subscription', 'frequency',
                         'snapshot_time', 'start', 'end', 'fallbacks', 'request_type']
        }

        instruments = [
            self.build_instrument(
                id=ids[i],
                type=type_[i],
                ticker=tickers[i],
                isin=isins[i],
                currency=currency[i],
                market=market[i],
                autocomplete=auto,
                **instrument_build_params,
            )
            for i in range(n)
        ]

        return self.get_with_instruments(
            instruments=instruments,
            fields=fields,
            source=source,
            subscription=subscription,
            market=market,
            frequency=frequency,
            snapshot_time=snapshot_time,
            start=start,
            end=end,
            fallbacks=fallbacks,
            **extra_params
        )

    def get_with_instruments(
            self,
            instruments: List,
            fields: List[str],
            source: Union[str, List[str], Dict[str, str]],
            subscription: Union[str, List[str], Dict[str, str]],
            market: Union[str, List[str], Dict[str, str]],
            frequency: str,
            snapshot_time: Optional[time],
            start: Union[dt.date, datetime],
            end: Union[dt.date, datetime],
            fallbacks: Optional[List[Dict[str, Any]]],
            **extra_params,
    ):

        if not isinstance(fields, list):
            fields = [fields]
        fields = [f.upper() for f in fields]
        subscription = normalize_param(subscription, instruments, default=None)
        source = normalize_param(source, instruments, default=None)

        request_type = extra_params.pop("request_type", None)

        if isinstance(frequency, str) and frequency.lower() in ["1d", "daily", "weekly"]:
            start = self._parse_date(start)
            end = self._parse_date(end)
        else:
            start = self._parse_datetime(start)
            end = self._parse_datetime(end)

        snapshot_time = self._parse_time(snapshot_time)

        """Access point per chiamate con instruments già creati."""
        dispatch_params = dict(
            instruments=instruments,
            fields=fields,
            source=source,
            subscriptions=subscription,
            market=market,
            frequency=frequency,
            snapshot_time=snapshot_time,
            start=start,
            end=end,
            request_type=request_type,
            **extra_params,
        )
        result = self._dispatch(**dispatch_params, fallbacks=fallbacks)
        return self._as_datetime_index(self._aggregate(result).sort_index())

    def get_fx_forward_prices(self,
                              quoted_currency: list[str] | str,
                              start,
                              base_currency: list[str] | str = "EUR",
                              tenor: int | str = "1M",
                              end: str | datetime = today(),
                              snapshot_time: time | str = time(17)):

        if isinstance(quoted_currency, str):
            quoted_currency = [quoted_currency]
        if isinstance(base_currency, str):
            base_currency = [base_currency]
        n = len(quoted_currency)
        quoted_currency = normalize_list(quoted_currency, n)
        base_currency = normalize_list(base_currency, n)

        for b,q in zip(base_currency, quoted_currency):
            if b == q: base_currency.remove(b); quoted_currency.remove(q)

        ids = [f"{b}{q} {tenor}" for b, q in zip(base_currency, quoted_currency)]
        quoted_currency = {id: q for id, q in zip(ids, quoted_currency)}
        base_currency = {id: b for id, b in zip(ids, base_currency)}
        return self.get(type=InstrumentType.FXFWD,
                        id=ids,
                        base_currency=base_currency,
                        quoted_currency=quoted_currency,
                        start=start,
                        frequency="1d",
                        source="bloomberg",
                        end=end,
                        tenor=tenor,
                        snapshot_time=snapshot_time)

    def get_intraday(
            self,
            start: Union[dt.date, datetime, str],
            end: Union[dt.date, datetime, str] = today(),
            id: Optional[Union[str, List[str]]] = None,
            isin: Optional[Union[str, List[str]]] = None,
            ticker: Optional[Union[str, List[str]]] = None,
            type: str = "ETP",
            frequency: str = "1m",
            fields: Union[str, List[str]] = "MID",
            source: Optional[Union[str, List[str], Dict[str, str]]] = None,
            market: Optional[Union[str, List[str], Dict[str, str]]] = None,
            subscription: Optional[Union[str, List[str], Dict[str, str]]] = None,
            **extra_params,
    ):
        """
        Generic is_intraday data wrapper for any instrument (tick/bar level).

        Args:
            start (date | datetime | str): Start date/time. If date, uses 00:00:00. If datetime, uses full timestamp.
            end (date | datetime | str): End date/time. If date, uses 23:59:59. If datetime, uses full timestamp.
            id: Optional[Union[str, List[str]]], optional): Instrument identifier.
            isin: Optional[Union[str, List[str]]], optional): Instrument isin.
            ticker: Optional[Union[str, List[str]]], optional): Instrument ticker.
            type (str): Instrument type ('ETP', 'FUTURE', etc.).
            frequency (str): Data frequency (e.g., '1m', '5m', '15m', '1h').
            fields (str | list[str]): Requested data fields.
            source (str | list[str] | dict[str, str]): Data source(s).
            market (str | list[str] | dict[str, str], optional): Market name(s).
            subscription (str | list[str] | dict[str, str], optional): Subscription identifier(s).

        Returns:
            pd.Series | pd.DataFrame: Time-series data for the specified period.
        """
        # Parse start and end (handles date/datetime/str)
        start_parsed = self._parse_datetime(start) if isinstance(start, (datetime, str)) else\
            datetime.combine(self._parse_date(start), datetime.min.time())
        end_parsed = self._parse_datetime(end) if isinstance(end, (datetime, str)) else\
            datetime.combine(self._parse_date(end), datetime.max.time())

        # Convert to ISO format strings for the request
        start_str = start_parsed.isoformat()
        end_str = end_parsed.isoformat()

        return self.get(
            type=type,
            id=id,
            isin=isin,
            ticker=ticker,
            start=start_str,
            end=end_str,
            frequency=frequency,
            fields=fields,
            source=source,
            market=market,
            subscription=subscription,
            **extra_params,
        ).sort_index()

    def get_day_snapshot(
            self,
            date: Union[dt.date, str] = today(),
            id: Optional[Union[str, List[str]]] = None,
            isin: Optional[Union[str, List[str]]] = None,
            ticker: Optional[Union[str, List[str]]] = None,
            type: str = "ETP",
            fields: Union[str, List[str]] = "MID",
            source: Optional[Union[str, List[str]]] = "timescale",
            market: Optional[Union[str, List[str]]] = None,
            snapshot_time: Union[str, time] = "17:00:00",
            **extra_params,
    ):
        """
        Retrieve a daily snapshot (single point per instrument) for a given date.

        Args:
            date (date | str): Snapshot date.
            id: Optional[Union[str, List[str]]], optional): Instrument identifier.
            isin: Optional[Union[str, List[str]]], optional): Instrument isin.
            ticker: Optional[Union[str, List[str]]], optional): Instrument ticker.
            snapshot_time (str | time): Time used to filter daily data.
            type (str): Instrument type ('ETP', 'FUTURE', etc.).
            fields (str | list[str]): Requested data fields.
            source (str | list[str]): Data source(s).
            market (str | list[str], optional): Market name(s).

        Returns:
            pd.Series | pd.DataFrame: Snapshot data for the specified date.
        """
        date = self._parse_date(date)
        snapshot_time = self._parse_time(snapshot_time)
        start = end = date

        res = self.get(
            type=type,
            id=id,
            isin=isin,
            ticker=ticker,
            start=start,
            end=end,
            source=source,
            frequency="1d",
            fields=fields,
            market=market,
            snapshot_time=snapshot_time,
            **extra_params,
        )

        if isinstance(res, pd.Series):
            return res.iloc[0]
        return res

    # ============================================================
    # WRAPPER METHODS
    # ============================================================

    def get_intraday_etf(
            self,
            start: Union[dt.date, datetime, str],
            end: Optional[Union[dt.date, datetime, str]] = None,
            id: Optional[Union[str, List[str]]] = None,
            isin: Optional[Union[str, List[str]]] = None,
            ticker: Optional[Union[str, List[str]]] = None,
            frequency: str = "15m",
            fields: Union[str, List[str]] = "MID",
            source: Union[str, List[str], Dict[str, str]] = "timescale",
            market: Union[str, List[str], Dict[str, str]] = "ETFP",
            **extra_params,
    ):
        """Dati is_intraday per ETF (ETP su ETFP di default). wraps get_intraday."""
        if not end:
            end = dt.date.today()
        return self.get_intraday(
            start=start,
            end=end,
            id=id,
            isin=isin,
            ticker=ticker,
            type="ETP",
            frequency=frequency,
            fields=fields,
            source=source,
            market=market,
            **extra_params,
        )

    def get_intraday_future(
            self,
            start: Union[dt.date, datetime, str],
            end: Union[dt.date, datetime, str],
            id: Optional[Union[str, List[str]]] = None,
            isin: Optional[Union[str, List[str]]] = None,
            ticker: Optional[Union[str, List[str]]] = None,
            frequency: str = "1m",
            fields: Union[str, List[str]] = "MID",
            source: str = "timescale",
            market: Optional[str] = None,
            **extra_params,
    ):
        """Dati is_intraday per Futures (EUREX se source=timescale e market non specificato). wraps get_intraday."""
        if source == "timescale" and market is None:
            market = "EUREX"

        return self.get_intraday(
            start=start,
            end=end,
            id=id,
            isin=isin,
            ticker=ticker,
            type="FUTURE",
            frequency=frequency,
            fields=fields,
            source=source,
            market=market,
            **extra_params,
        )

    def get_intraday_fx(
            self,
            start: Union[dt.date, datetime, str],
            end: Union[dt.date, datetime, str],
            id: Optional[Union[str, List[str]]] = None,
            isin: Optional[Union[str, List[str]]] = None,
            ticker: Optional[Union[str, List[str]]] = None,
            frequency: str = "1m",
            fields: Union[str, List[str]] = "MID",
            source: str = "timescale",
            **extra_params,
    ):
        """Dati is_intraday per coppie FX. wraps get_intraday."""
        return self.get_intraday(
            start=start,
            end=end,
            id=id,
            isin=isin,
            ticker=ticker,
            type="CURRENCYPAIR",
            frequency=frequency,
            fields=fields,
            source=source,
            **extra_params,
        )

    def get_day_snapshot_etf(
            self,
            date: Union[dt.date, str] = today(),
            id: Optional[Union[str, List[str]]] = None,
            isin: Optional[Union[str, List[str]]] = None,
            ticker: Optional[Union[str, List[str]]] = None,
            fields: Union[str, List[str]] = "MID",
            source: str = "timescale",
            market: Optional[str] = "ETFP",
            snapshot_time: Union[str, time] = "17:00:00",
            **extra_params,
    ):
        """Snapshot giornaliero per ETF (default 17:00)."""
        return self.get_day_snapshot(
            date=date,
            id=id,
            isin=isin,
            ticker=ticker,
            type="ETP",
            fields=fields,
            source=source,
            market=market,
            snapshot_time=snapshot_time,
            **extra_params,
        )

    def get_day_snapshot_future(
            self,
            date: Union[dt.date, str] = today(),
            id: Optional[Union[str, List[str]]] = None,
            isin: Optional[Union[str, List[str]]] = None,
            ticker: Optional[Union[str, List[str]]] = None,
            fields: Union[str, List[str]] = "MID",
            source: str = "timescale",
            market: Optional[str] = None,
            snapshot_time: Union[str, time] = "17:00:00",
            **extra_params,
    ):
        """Snapshot giornaliero per Futures (EUREX di default su timescale)."""
        if source == "timescale" and market is None:
            market = "EUREX"

        return self.get_day_snapshot(
            date=date,
            id=id,
            isin=isin,
            ticker=ticker,
            type="FUTURE",
            fields=fields,
            source=source,
            market=market,
            snapshot_time=snapshot_time,
            **extra_params,
        )

    def get_daily_etf(
            self,
            start: Union[dt.date, str],
            end: Union[dt.date, str] = today(),
            id: Optional[Union[str, List[str]]] = None,
            isin: Optional[Union[str, List[str]]] = None,
            ticker: Optional[Union[str, List[str]]] = None,
            fields: Union[str, List[str]] = "MID",
            source: Union[str, List[str], Dict[str, str]] = "timescale",
            market: Optional[Union[str, List[str], Dict[str, str]]] = "ETFP",
            currency: Union[str, List[str], Dict[str, str]] = "EUR",
            snapshot_time: Union[str, time] = "17:00:00",
            **extra_params,
    ):
        """Serie daily per ETF (1d, snapshot_time opzionale)."""
        return self.get(
            type="ETP",
            id=id,
            isin=isin,
            ticker=ticker,
            start=start,
            end=end,
            source=source,
            frequency="1d",
            fields=fields,
            market=market,
            currency=currency,
            snapshot_time=snapshot_time,
            **extra_params,
        )

    def get_daily_stock(
            self,
            start: Union[dt.date, str],
            end: Union[dt.date, str] = today(),
            id: Optional[Union[str, List[str]]] = None,
            isin: Optional[Union[str, List[str]]] = None,
            ticker: Optional[Union[str, List[str]]] = None,
            fields: Union[str, List[str]] = "MID",
            source: str = "timescale",
            market: Optional[Union[str, List[str]]] = None,
            currency: Union[str, List[str]] = "EUR",
            snapshot_time: Union[str, time] = "17:00:00",
            **extra_params,
    ):
        """Serie daily per ETF (1d, snapshot_time opzionale)."""
        return self.get(
            type="STOCK",
            id=id,
            isin=isin,
            ticker=ticker,
            start=start,
            end=end,
            source=source,
            frequency="1d",
            fields=fields,
            market=market,
            currency=currency,
            snapshot_time=snapshot_time,
            **extra_params,
        )

    def get_daily_currency(
            self,
            start: Union[dt.date, str],
            end: Union[dt.date, str] = today(),
            id: Optional[Union[str, List[str]]] = None,
            isin: Optional[Union[str, List[str]]] = None,
            ticker: Optional[Union[str, List[str]]] = None,
            fields: Union[str, List[str]] = "MID",
            source: str = "timescale",
            **extra_params,
    ):
        """Serie daily per coppie FX."""
        return self.get(
            type="CURRENCYPAIR",
            id=id,
            isin=isin,
            ticker=ticker,
            start=start,
            end=end,
            source=source,
            frequency="1d",
            fields=fields,
            **extra_params,
        )

    def get_daily_future(
            self,
            start: Union[dt.date, str],
            end: Union[dt.date, str] = today(),
            id: Optional[Union[str, List[str]]] = None,
            isin: Optional[Union[str, List[str]]] = None,
            ticker: Optional[Union[str, List[str]]] = None,
            fields: Union[str, List[str]] = "MID",
            source: str = "timescale",
            market: Optional[Union[str, List[str]]] = None,
            suffix: Optional[str] = None,
            **extra_params,
    ):
        """Serie daily per Futures (EUREX di default su timescale)."""
        if source == "timescale" and market is None:
            market = "EUREX"

        return self.get(
            type="FUTURE",
            id=id,
            isin=isin,
            ticker=ticker,
            start=start,
            end=end,
            source=source,
            frequency="1d",
            fields=fields,
            market=market,
            suffix=suffix,
            **extra_params,
        )

    def get_daily_swap(
            self,
            start: Union[dt.date, str],
            end: Union[dt.date, str] = today(),
            id: Optional[Union[str, List[str]]] = None,
            ticker: Optional[Union[str, List[str]]] = None,
            fields: Union[str, List[str]] = "MID",
            source: str = "bloomberg",
            tenor: Optional[Union[str, List[str]]] = None,
            subscriptions: Optional[Union[str, List[str]]] = None,
            **extra_params,
    ):
        """daily market data swap, refer to Oracle anagraphic for ticker choice."""
        return self.get(
            type="SWAP",
            id=id,
            start=start,
            end=end,
            ticker=ticker,
            fields=fields,
            source=source,
            tenor=tenor,
            subscriptions=subscriptions,
            **extra_params,
        )

    def get_intraday_swap(
            self,
            start: Union[dt.date, datetime, str],
            end: Union[dt.date, datetime, str],
            id: Optional[Union[str, List[str]]] = None,
            ticker: Optional[Union[str, List[str]]] = None,
            frequency: str = "1m",
            fields: Union[str, List[str]] = "MID",
            source: str = "oracle",
            **extra_params,
    ):
        """Dati is_intraday swap. Refer to Oracle anagraphic for ticker choice (e.g., EUZCISWAP10)."""
        return self.get_intraday(
            start=start,
            end=end,
            id=id,
            ticker=ticker,
            type="SWAP",
            frequency=frequency,
            fields=fields,
            source=source,
            **extra_params)

    def get_intraday_cdx(
            self,
            start: Union[dt.date, datetime, str],
            end: Union[dt.date, datetime, str],
            id: Optional[Union[str, List[str]]] = None,
            ticker: Optional[Union[str, List[str]]] = None,
            frequency: str = "1m",
            fields: Union[str, List[str]] = "MID",
            source: str = "oracle",
            **extra_params,
    ):
        """Dati is_intraday CDX. Refer to Oracle anagraphic for ticker choice."""
        return self.get_intraday(
            start=start,
            end=end,
            id=id,
            ticker=ticker,
            type=InstrumentType.CDXINDEX,
            frequency=frequency,
            fields=fields,
            source=source,
            **extra_params)

    def get_daily_cdx(self,
                      start: Union[dt.date, str],
                      end: Union[dt.date, str] = today(),
                      id: Optional[Union[str, List[str]]] = None,
                      ticker: Optional[Union[str, List[str]]] = None,
                      fields: Union[str, List[str]] = "MID",
                      source: str = "bloomberg",
                      tenor: Optional[Union[str, List[str]]] = None,
                      subscriptions: Optional[Union[str, List[str]]] = None,
                      **extra_params,
                      ):
        """daily market data cdx, refer to Oracle anagraphic for ticker choice."""
        return self.get(
            type=InstrumentType.CDXINDEX,
            id=id,
            start=start,
            end=end,
            ticker=ticker,
            fields=fields,
            source=source,
            tenor=tenor,
            subscriptions=subscriptions,
            **extra_params,
        )

    # In MarketDataAPI, aggiungi:

    def get_daily_repo_rates(
            self,
            start: Union[dt.date, str],
            end: Union[dt.date, str] = today(),
            currencies: Optional[Union[str, List[str]]] = None,
            tenor: Optional[Union[str, List[str]]] = None,
            ticker: Optional[Union[str, List[str]]] = None,
            source: str = "bloomberg",
            **extra_params,
    ) -> pd.DataFrame:

        """Get daily repo rates. currencies=['EUR','USD'], tenor=None(overnight) or '1M'/'3M'/etc."""
        OVERNIGHT = {'EUR': 'ESTRON INDEX', 'USD': 'SOFRRATE INDEX', 'GBP': 'SONIA INDEX', 'JPY': 'TONAR INDEX',
                     'CHF': 'SARON INDEX'}

        if currencies:
            currencies = [currencies] if isinstance(currencies, str) else currencies
            n = len(currencies)
            tenor = [tenor] * n if isinstance(tenor, str) or tenor is None else tenor
            ticker = [OVERNIGHT[c] for c in currencies]  # Overnight only for now
            ccy_map = {ticker[i]: currencies[i] for i in range(n)}
            extra_params['tenor'] = tenor
        else:
            ccy_map = None

        result = self.get(type=InstrumentType.INDEX, ticker=ticker, start=start, end=end,
                          fields="PX_LAST", source=source, frequency="1d", request_type="historical", **extra_params)

        if isinstance(result, pd.DataFrame):
            result = result / 100.0  # % -> decimal
            if ccy_map:
                result = result.rename(columns={t: c for t, c in ccy_map.items() if
                                                any(t.replace(' INDEX', '') in str(col) for col in result.columns)})
        return result

    def get_daily_fx_forward(self, start: Union[dt.date, str],
                             end: Union[dt.date, str] = today(),
                             id: Optional[Union[str, List[str]]] = None,
                             base_currency: Optional[Union[str, List[str]]] = "EUR",
                             quoted_currency: Optional[Union[str, List[str]]] = None,
                             fields: Union[str, List[str]] = "MID",
                             source: str = "bloomberg",
                             tenor: Optional[Union[str, List[str]]] = "1M",
                             **extra_params):

        if not id and not quoted_currency:
            raise ValueError("Either ticker or quote_currency must be specified (base currency assumed to be EUR)")

        if isinstance(base_currency, str):  base_currency = [base_currency]
        if isinstance(quoted_currency, str): quoted_currency = quoted_currency

        base_currency = normalize_list(base_currency, len(quoted_currency))
        for b, q in zip(base_currency, quoted_currency):
            if b == q: base_currency.remove(b); quoted_currency.remove(q);

        if not id:
            id = [f"{b}{q} {tenor}" for b,q in zip(base_currency, quoted_currency)]

        quoted_currency = {i: q for i, q in zip(id, quoted_currency)}
        base_currency = {i: b for i, b in zip(id, base_currency)}

        return self.get(type=InstrumentType.FXFWD,
                        id=id,
                        start=start,
                        end=end,
                        base_currency=base_currency,
                        quoted_currency=quoted_currency,
                        fields=fields,
                        source=source,
                        frequency="1d",
                        request_type="historical",
                        tenor=tenor,
                        **extra_params)

    def _as_datetime_index(self, df: pd.DataFrame) -> pd.DataFrame:
        try:
            df.index = pd.to_datetime(df.index)
        except Exception:
            pass
        finally:
            return df


