"""
Timescale fetcher - Unified data retrieval interface.

This module defines :class:`TimescaleFetcher`, the unified interface for retrieving
all TimescaleDB data types. It combines both market data and info data fetching
using the chain of responsibility pattern.

Responsibilities:
    - Dispatch market data requests to appropriate handlers (equity, FX, bond, etc.)
    - Dispatch info data requests to YTM handler
    - Use chain of responsibility pattern for request routing
"""

from typing import List

from core.base_classes.base_fetcher import BaseFetcher
from core.requests.requests import BaseMarketRequest, BaseStaticRequest
from providers.timescale.handlers.bond_handler import BondHandler
from providers.timescale.handlers.equity_handler import EquityHandler
from providers.timescale.handlers.fallback_handler import FallbackHandler
from providers.timescale.handlers.future_handler import FutureHandler
from providers.timescale.handlers.fx_handler import FXHandler
from providers.timescale.handlers.fxfwrd_handler import FXFwdHandler
from providers.timescale.handlers.index_handler import IndexHandler
from providers.timescale.handlers.ytm_handler import YTMHandler
from providers.timescale.query_timescale import QueryTimeScale


class TimescaleFetcher(BaseFetcher):
    """
    Unified Timescale fetcher for all data types.

    This class manages all TimescaleDB data requests using the chain of responsibility
    pattern to route requests to the appropriate handler based on instrument type.

    Args:
        query_ts (QueryTimeScale): TimescaleDB query interface
        show_progress (bool): Whether to display progress information

    Example:
        >>> fetcher = TimescaleFetcher(query_ts)
        >>> market_data = fetcher.fetch_market_data(market_requests)
        >>> info_data = fetcher.fetch_info_data(info_requests)
    """

    def __init__(self, query_ts: QueryTimeScale, show_progress=True):
        super().__init__(show_progress)
        self.query_ts = query_ts

        # Build market data handler chain
        market_chain = EquityHandler()
        market_chain.set_next(FXHandler()) \
            .set_next(FutureHandler()) \
            .set_next(BondHandler()) \
            .set_next(FXFwdHandler()) \
            .set_next(IndexHandler()) \
            .set_next(FallbackHandler())

        self.market_chain = market_chain

        # Build info data handler chain
        self.info_chain = YTMHandler()

    def fetch_market_data(self, requests: List[BaseMarketRequest]):
        """
        Fetch market data using the market handler chain.

        Args:
            requests: List of market data requests

        Returns:
            Dictionary mapping instrument IDs to their data
        """
        return self.market_chain.handle(requests, self.query_ts)

    def fetch_info_data(self, requests: List[BaseStaticRequest]):
        """
        Fetch static/info data using the info handler chain.

        Args:
            requests: List of static data requests

        Returns:
            Dictionary mapping instrument IDs to their data
        """
        return self.info_chain.handle(requests, self.query_ts)
