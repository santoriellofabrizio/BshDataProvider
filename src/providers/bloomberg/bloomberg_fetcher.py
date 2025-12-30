"""
Bloomberg fetcher - Unified data retrieval interface using handlers.

This module defines :class:`BloombergFetcher`, the unified interface for retrieving
all Bloomberg data types via BLPAPI using the chain of responsibility pattern.

It delegates data retrieval to specialized handlers:
    - ReferenceDataRequest → BloombergReferenceHandler
    - HistoricalDataRequest (info) → BloombergHistoricalFieldHandler
    - BulkDataRequest → BloombergBulkFieldHandler
    - HistoricalDataRequest (market) → BloombergDailyPriceHandler
    - IntradayBarRequest → BloombergIntradayPriceHandler
    - Snapshot → BloombergSnapshotPriceHandler

Responsibilities:
    - Initialize handler chains for different request types
    - Route requests to appropriate handler chains
    - Provide unified interface for all Bloomberg data retrieval

Example:
    >>> fetcher = BloombergFetcher(session)
    >>> ref = fetcher.fetch_reference_data(["IHYG IM Equity"], ["FUND_TOTAL_EXP"], ["IHYG"])
    >>> daily = fetcher.fetch_daily(requests)
"""

import logging
from typing import List, Dict, Any, Optional
from datetime import date, datetime
from types import SimpleNamespace

import blpapi

from core.base_classes.base_fetcher import BaseFetcher
from core.requests.requests import BaseStaticRequest, BulkRequest
from core.utils.memory_provider import cache_bsh_data
from providers.bloomberg.handlers.bulk_field_handler import BloombergBulkHandler
from providers.bloomberg.handlers.historical_field_handler import BloombergHistoricalHandler
from providers.bloomberg.handlers.reference_field_handler import BloombergReferenceHandler

from providers.bloomberg.handlers.daily_price_handler import BloombergDailyPriceHandler
from providers.bloomberg.handlers.intraday_price_handler import BloombergIntradayPriceHandler
from providers.bloomberg.handlers.snapshot_price_handler import BloombergSnapshotPriceHandler

logger = logging.getLogger(__name__)


class BloombergFetcher(BaseFetcher):
    """
    Unified Bloomberg fetcher using handler chains.

    This class manages all Bloomberg data requests by delegating to specialized
    handlers using the chain of responsibility pattern.

    Args:
        session (blpapi.Session): Active Bloomberg session.
        service (blpapi.Service | None): Optional pre-opened RefData service.
        show_progress (bool): Whether to display progress information.

    Example:
        >>> fetcher = BloombergFetcher(session)
        >>> result = fetcher.fetch_reference_data(
        ...     subscriptions=["IHYG IM Equity"],
        ...     fields=["FUND_TOTAL_EXP"],
        ...     corr_ids=["IHYG"]
        ... )
    """

    SERVICE_NAME = "//blp/refdata"

    def __init__(self, session, service=None, show_progress: bool = True):
        super().__init__()
        self.session = session
        self.service = service or session.getService(self.SERVICE_NAME)
        self.show_progress = show_progress

        # Initialize handlers
        self.reference_handler = BloombergReferenceHandler()
        self.historical_handler = BloombergHistoricalHandler()
        self.bulk_handler = BloombergBulkHandler()
        self.daily_handler = BloombergDailyPriceHandler(show_progress)
        self.intraday_handler = BloombergIntradayPriceHandler(show_progress)
        self.snapshot_handler = BloombergSnapshotPriceHandler(show_progress)

        logger.debug("BloombergFetcher initialized with handler chains")

    # ============================================================
    # REFERENCE DATA (STATIC FIELDS)
    # ============================================================

    @cache_bsh_data
    def fetch_reference_data(
            self,
            requests: List[BaseStaticRequest],
    ) -> Dict[str, Dict[str, Any]]:
        """
        Fetch Bloomberg reference data (static fields).

        Args:
            requests:
            subscriptions: Bloomberg security identifiers
            fields: Bloomberg field names
            corr_ids: Correlation IDs for mapping responses

        Returns:
            Dict[corr_id, Dict[field, value]]
        """


        # Delegate to handler
        return self.reference_handler.handle(requests, self.session, self.service)

    # ============================================================
    # HISTORICAL DATA (NON-MARKET TIME SERIES)
    # ============================================================

    @cache_bsh_data
    def fetch_historical_data(
            self,
            subscriptions: List[str],
            fields: List[str],
            corr_ids: List[str],
            *,
            start: Optional[date] = None,
            end: Optional[date] = None,
            periodicity: str = "DAILY",
    ) -> Dict[str, Any]:
        """
        Fetch Bloomberg historical data (non-market time series).

        Args:
            subscriptions: Bloomberg security identifiers
            fields: Bloomberg field names
            corr_ids: Correlation IDs for mapping responses
            start: Start date (default: 1 year ago)
            end: End date (default: today)
            periodicity: Data periodicity (default: DAILY)

        Returns:
            Dict[corr_id, Dict[field, Dict[date, value]]]
        """
        if not subscriptions or not fields:
            logger.warning("Empty subscriptions or fields for HistoricalDataRequest")
            return {}

        if len(subscriptions) != len(corr_ids):
            raise ValueError("subscriptions and corr_ids must have same length")

        logger.info("Fetching Bloomberg HistoricalData: %s for %d instruments", fields, len(subscriptions))

        # Create pseudo-requests for the handler
        requests = self._create_static_requests(subscriptions, fields, corr_ids, "historical", start, end)

        # Delegate to handler
        return self.historical_handler.handle(requests, self.session, self.service)

    # ============================================================
    # BULK DATA
    # ============================================================

    @cache_bsh_data
    def fetch_bulk_data(
            self,
            requests: List[BulkRequest],
    ) -> Dict[str, Any]:
        """
        Fetch Bloomberg bulk data (tabular fields).

        Args:
            requests: list of BulkRequest objects
        Returns:
            Dict[corr_id, Dict[field, Dict[date, value]]] or {} if no data
        """

        # Delegate to handler
        return self.bulk_handler.handle(requests, self.session, self.service)

    # ============================================================
    # DAILY / HISTORICAL MARKET DATA
    # ============================================================

    @cache_bsh_data
    def fetch_daily(self, requests: List) -> dict:
        """
        Fetch daily market data for multiple instruments.

        Args:
            requests: List of market data request objects
            fields: Bloomberg field names (e.g., PX_LAST, PX_VOLUME)
            start: Start datetime
            end: End datetime

        Returns:
            Dict[instrument_id, Dict[field, Dict[date, value]]]
        """
        logger.info("Starting Bloomberg daily fetch for %d instruments", len(requests))

        # Delegate to daily handler
        return self.daily_handler.handle(requests, self.session, self.service)

    # ============================================================
    # SNAPSHOT
    # ============================================================

    @cache_bsh_data
    def fetch_snapshot(self, request) -> dict:
        """
        Fetch snapshot data for a single instrument.

        Args:
            request: Market data request object

        Returns:
            Dict[instrument_id, Dict[field, Dict[date, value]]]
        """
        logger.info("Fetching Bloomberg snapshot for %s", request.instrument.id)

        # Delegate to snapshot handler
        return self.snapshot_handler.handle([request], self.session, self.service)

    # ============================================================
    # INTRADAY
    # ============================================================

    @cache_bsh_data
    def fetch_intraday(self, request) -> dict:
        """
        Fetch intraday bar data for a single instrument.

        Args:
            request: Market data request object

        Returns:
            Dict[field, Dict[datetime, value]]
        """
        logger.info("Fetching Bloomberg intraday for %s", request.instrument.id)

        # Delegate to intraday handler
        result = self.intraday_handler.handle([request], self.session, self.service)

        # Intraday returns {instrument_id: {field: {time: value}}}
        # But the old API expected just {field: {time: value}}
        # Return the data for this instrument
        return result.get(request.instrument.id, {})

    # ============================================================
    # HELPER METHODS
    # ============================================================

    def _create_static_requests(
            self,
            subscriptions: List[str],
            fields: List[str],
            corr_ids: List[str],
            request_type: str,
            start: Optional[date] = None,
            end: Optional[date] = None
    ) -> List[Any]:
        """
        Create pseudo-request objects for handlers.

        Handlers expect request objects with certain attributes.
        This helper creates minimal request objects from raw parameters.
        """
        requests = []
        for sub, corr_id in zip(subscriptions, corr_ids):
            # Create a minimal instrument object
            instrument = SimpleNamespace(
                id=corr_id,
                isin=None,
                ticker=sub
            )

            # Create a minimal request object
            req = SimpleNamespace(
                instrument=instrument,
                subscription=sub,
                fields=fields,
                request_type=request_type,
                start=start,
                end=end
            )

            requests.append(req)

        return requests
