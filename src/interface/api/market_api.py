"""
market_data_api.py — Unified API for dynamic market data.

This module defines the :class:`MarketDataAPI`, a high-level interface for querying
market time-series and intraday data (ETF, futures, FX, equities) from multiple providers
(e.g., Timescale, Bloomberg). It standardizes the construction, dispatch, and aggregation
of requests for dynamic data such as prices, NAVs, or fair values.

Responsibilities:
    - Manage both daily and intraday data requests
    - Normalize identifiers and query parameters
    - Handle provider dispatch and result aggregation
    - Integrate caching and auto-completion via BaseAPI

Typical usage:
    >>> from src import BSHDataClient
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

from core.decorators.respect_cache_status import respect_cache_kwarg
from core.enums.instrument_types import InstrumentType
from core.requests.request_builder.request_builder import RequestBuilder
from core.utils.common import normalize_list
from interface.api.base_api import BaseAPI


class MarketDataAPI(BaseAPI):
    """
    High-level API for market data retrieval (historical, intraday, and snapshots).

    This class provides a unified interface to fetch time-series
    data from multiple sources. It manages request creation, provider dispatch,
    and aggregation into a consistent output structure.

    Responsibilities:
        - Build and send market data requests and instruments (via InstrumentFactory) through the unified data client
        - Normalize and validate parameters (dates, instruments, frequencies)
        - Support both historical (daily) and intraday data retrieval
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
            source: Union[str, List[str]],
            subscriptions: Optional[Union[str, List[str]]] = None,
            market: Optional[Union[str, List[str]]] = None,
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
            source (str | list[str]): Data source(s) (e.g., 'timescale', 'bloomberg').
            subscriptions (str | list[str], optional): Optional subscription identifiers.
            market (str | list[str], optional): Market codes (e.g., 'ETFP', 'EUREX').
            request_type: Type of request.
            fallbacks (list[dict], optional): Alternative configs to retry on incomplete results.
            **kwargs: Additional parameters forwarded to RequestBuilder.

        Returns:
            dict | pd.DataFrame | None: Aggregated results from all attempts.
        """
        if not instruments:
            return None

        n = len(instruments)
        fields = [fields] if isinstance(fields, str) else fields
        market = normalize_list(market, n)
        source = normalize_list(source, n)
        subscriptions = normalize_list(subscriptions, n)

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
            n = len(retry_instruments)

            # Override parameters from fallback config
            retry_source = normalize_list(fallback_config.get("source"), n)
            retry_market = normalize_list(fallback_config.get("market"), n)
            retry_subscriptions = normalize_list(fallback_config.get("subscriptions"), n)

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

            # Merge results (retry results override original)
            if retry_result:
                merged_result.update(retry_result)

            # Check if all incomplete are now complete
            still_incomplete = self.client.tracker.get_incomplete()
            if not still_incomplete:
                self.log_request("[fallback] All requests now complete")
                break

        return merged_result

    # ============================================================
    # GENERIC GET
    # ============================================================
    @respect_cache_kwarg
    def get(
            self,
            type: str,
            start: Optional[Union[str, dt.date, datetime]],
            end: Optional[Union[str, dt.date, datetime]],
            id: Optional[Union[str, List[str]]] = None,
            isin: Optional[Union[str, List[str]]] = None,
            ticker: Optional[Union[str, List[str]]] = None,
            market: Optional[Union[str, List[str]]] = None,
            source: Optional[Union[str, List[str]]] = None,
            frequency: Optional[str] = "1d",
            fields: Union[str, List[str]] = "MID",
            currency: Union[str, List[str]] = "EUR",
            snapshot_time: Optional[Union[str, time]] = None,
            subscription: Optional[Union[str, List[str], Dict[str, str]]] = None,
            autocomplete: Optional[bool] = None,
            fallbacks: Optional[List[Dict[str, Any]]] = None,
            **extra_params,
    ):
        """
        Retrieve market time-series (historical or intraday) for ETFs, futures, FX, or equities.

        Args:
            type (str): Instrument type (e.g., 'ETP', 'FUTURE', 'CURRENCYPAIR').
            start (str | date | datetime): Start date/datetime for the query.
            end (str | date | datetime): End date/datetime for the query.
            id (str | list[str], optional): Instrument identifiers (e.g., "IHYG", "RXZ4", "EURUSD").
                If missing, inferred from `isin` or `ticker`.
            isin (str | list[str], optional): Instrument ISIN code(s).
            ticker (str | list[str], optional): Instrument ticker(s).
            market (str | list[str], optional): Market name(s) (e.g., 'ETFP', 'EUREX').
            source (str | list[str], optional): Data source(s) ('timescale', 'bloomberg', 'oracle').
            frequency (str): Data frequency. '1d' for daily, '1m'/'5m'/etc for intraday.
            fields (str | list[str]): Requested data fields (e.g., 'MID', 'VOLUME', 'BID', 'ASK').
            currency (str | list[str]): Instrument currency (default 'EUR').
            snapshot_time (str | time, optional): Time for daily snapshot filtering (e.g., '17:00').
            subscription (str | list[str] | dict, optional): Subscription identifier(s).
            autocomplete (bool, optional): Auto-complete missing instrument metadata.
            fallbacks (list[dict], optional): Alternative configurations for automatic retry.
                If the request fails or returns partial data, retry with each fallback config.
                Each dict can override: 'source', 'market', 'currency', or any other parameter.
            **extra_params: Additional provider or instrument parameters.

        Returns:
            pd.DataFrame | pd.Series | dict | None: Aggregated results from provider(s).

        Examples:
            Basic usage:
                >>> api.market.get()

            With fallbacks (automatic retry):
                >>> api.market.get()
                # Tries bloomberg first. If MID or VOLUME missing, retries with oracle.
                # If still missing, retries with timescale + XETRA market.
                # Returns merged results from all successful attempts.
        """
        auto = self.autocomplete if autocomplete is None else autocomplete

        if frequency.lower() in ["1d", "daily", "weekly"]:
            start = self._parse_date(start)
            end = self._parse_date(end)
        else:
            start = self._parse_datetime(start)
            end = self._parse_datetime(end)

        snapshot_time = self._parse_time(snapshot_time)
        ids, isins, tickers = self._resolve_identifiers(id, isin, ticker, autocomplete=auto)
        n = len(ids)

        currency = normalize_list(currency, n)
        market = normalize_list(market, n)
        source = normalize_list(source, n)
        type_ = normalize_list(type, n)
        subscription = normalize_list(subscription, n)
        if not isinstance(fields, list):
            fields = [fields]
        fields = [f.upper() for f in fields]
        request_type = extra_params.pop("request_type", None)

        instruments = [
            self._build_instrument(
                id=ids[i],
                type=type_[i],
                ticker=tickers[i],
                isin=isins[i],
                currency=currency[i],
                market=market[i],
                autocomplete=auto,
                **extra_params,
            )
            for i in range(n)
        ]

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
        return self._aggregate(result)


    # WRAPPER METHODS
    # ============================================================
    def get_intraday(
            self,
            date: Union[dt.date, str],
            id: Optional[Union[str, List[str]]] = None,
            isin: Optional[Union[str, List[str]]] = None,
            ticker: Optional[Union[str, List[str]]] = None,
            type: str = "ETP",
            frequency: str = "1m",
            fields: Union[str, List[str]] = "MID",
            start_time: Union[time, str] = "09:00:00",
            end_time: Union[time, str] = "17:00:00",
            source: Optional[Union[str, List[str]]] = None,
            market: Optional[Union[str, List[str]]] = None,
            subscription: Optional[Union[str, List[str], Dict[str, str]]] = None,
            **extra_params,
    ):
        """
        Generic intraday data wrapper for any instrument (tick/bar level).

        Args:
            date (date | str): Snapshot date.
            id: Optional[Union[str, List[str]]], optional): Instrument identifier.
            isin: Optional[Union[str, List[str]]], optional): Instrument isin.
            ticker: Optional[Union[str, List[str]]], optional): Instrument ticker.
            type (str): Instrument type ('ETP', 'FUTURE', etc.).
            fields (str | list[str]): Requested data fields.
            source (str | list[str]): Data source(s).
            market (str | list[str], optional): Market name(s).
            start_time (str | time): Snapshot start time (format as HH:MM:SS or time).
            end_time (str | time): Snapshot start time (format as HH:MM:SS or time).
            subscription: Optional[Union[str, List[str]]]: Subscription identifier(s) to be used in reqeust.

        Returns:
            pd.Series | pd.DataFrame: Snapshot data for the specified date.
        """
        date = self._parse_date(date)
        start_time = self._parse_time(start_time)
        end_time = self._parse_time(end_time)

        if not all(isinstance(t, time) for t in (start_time, end_time)):
            raise ValueError("start_time e end_time devono essere in formato HH:MM:SS")

        start = f"{date}T{start_time.strftime('%H:%M:%S')}"
        end = f"{date}T{end_time.strftime('%H:%M:%S')}"

        return self.get(
            type=type,
            id=id,
            isin=isin,
            ticker=ticker,
            start=start,
            end=end,
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
            date: Union[dt.date, str],
            id: Optional[Union[str, List[str]]] = None,
            isin: Optional[Union[str, List[str]]] = None,
            ticker: Optional[Union[str, List[str]]] = None,
            frequency: str = "15m",
            fields: Union[str, List[str]] = "MID",
            start_time: Union[time, str] = "09:00:00",
            end_time: Union[time, str] = "17:00:00",
            source: str = "timescale",
            market: str = "ETFP",
            **extra_params,
    ):
        """Dati intraday per ETF (ETP su ETFP di default). wraps get_intraday."""
        return self.get_intraday(
            date=date,
            id=id,
            isin=isin,
            ticker=ticker,
            type="ETP",
            frequency=frequency,
            fields=fields,
            start_time=start_time,
            end_time=end_time,
            source=source,
            market=market,
            **extra_params,
        )

    def get_intraday_future(
            self,
            date: Union[dt.date, str],
            id: Optional[Union[str, List[str]]] = None,
            isin: Optional[Union[str, List[str]]] = None,
            ticker: Optional[Union[str, List[str]]] = None,
            frequency: str = "1m",
            fields: Union[str, List[str]] = "MID",
            start_time: Union[time, str] = "09:00:00",
            end_time: Union[time, str] = "17:00:00",
            source: str = "timescale",
            market: Optional[str] = None,
            **extra_params,
    ):
        """Dati intraday per Futures (EUREX se source=timescale e market non specificato). wraps get_intraday."""
        if source == "timescale" and market is None:
            market = "EUREX"

        return self.get_intraday(
            date=date,
            id=id,
            isin=isin,
            ticker=ticker,
            type="FUTURE",
            frequency=frequency,
            fields=fields,
            start_time=start_time,
            end_time=end_time,
            source=source,
            market=market,
            **extra_params,
        )

    def get_intraday_fx(
            self,
            date: Union[dt.date, str],
            id: Optional[Union[str, List[str]]] = None,
            isin: Optional[Union[str, List[str]]] = None,
            ticker: Optional[Union[str, List[str]]] = None,
            frequency: str = "1m",
            fields: Union[str, List[str]] = "MID",
            start_time: Union[time, str] = "00:00:00",
            end_time: Union[time, str] = "23:59:59",
            source: str = "timescale",
            **extra_params,
    ):
        """Dati intraday per coppie FX. wraps get_intraday."""
        return self.get_intraday(
            date=date,
            id=id,
            isin=isin,
            ticker=ticker,
            type="CURRENCYPAIR",
            frequency=frequency,
            fields=fields,
            start_time=start_time,
            end_time=end_time,
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
            source: str = "timescale",
            market: Optional[Union[str, List[str]]] = "ETFP",
            currency: Union[str, List[str]] = "EUR",
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
            date: Union[dt.date, str],
            id: Optional[Union[str, List[str]]] = None,
            ticker: Optional[Union[str, List[str]]] = None,
            frequency: str = "1m",
            fields: Union[str, List[str]] = "MID",
            start_time: Union[time, str] = "09:00:00",
            end_time: Union[time, str] = "23:59:59",
            source: str = "oracle",
            **extra_params,
    ):
        """daily market data swap, refer to Oracle anagraphic for ticker choice. (es: EUZCISWAP10,..)"""
        return self.get_intraday(
            date=date,
            id=id,
            ticker=ticker,
            type="SWAP",
            frequency=frequency,
            fields=fields,
            start_time=start_time,
            end_time=end_time,
            source=source,
            **extra_params)

    def get_intraday_cdx(
            self,
            date: Union[dt.date, str],
            id: Optional[Union[str, List[str]]] = None,
            ticker: Optional[Union[str, List[str]]] = None,
            frequency: str = "1m",
            fields: Union[str, List[str]] = "MID",
            start_time: Union[time, str] = "09:00:00",
            end_time: Union[time, str] = "23:59:59",
            source: str = "oracle",
            **extra_params,
    ):
        """intraday market data cdx, refer to Oracle anagraphic for ticker choice."""
        return self.get_intraday(
            date=date,
            id=id,
            ticker=ticker,
            type=InstrumentType.CDXINDEX,
            frequency=frequency,
            fields=fields,
            start_time=start_time,
            end_time=end_time,
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
