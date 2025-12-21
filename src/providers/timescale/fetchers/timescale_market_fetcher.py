from typing import List

from core.base_classes.base_fetcher import BaseMarketFetcher
from core.requests.requests import BaseMarketRequest
from providers.timescale.handlers.bond_handler import BondHandler
from providers.timescale.handlers.equity_handler import EquityHandler
from providers.timescale.handlers.fallback_handler import FallbackHandler
from providers.timescale.handlers.future_handler import FutureHandler
from providers.timescale.handlers.fx_handler import FXHandler
from providers.timescale.handlers.fxfwrd_handler import FXFwdHandler
from providers.timescale.handlers.index_handler import IndexHandler
from providers.timescale.query_timescale import QueryTimeScale


class TimescaleMarketFetcher(BaseMarketFetcher):

    def __init__(self, query_ts: QueryTimeScale, show_progress=True):
        super().__init__(show_progress)
        self.query_ts = query_ts

        # COSTRUISCO LA CHAIN (ordine logico)
        first = EquityHandler()
        first.set_next(FXHandler()) \
            .set_next(FutureHandler()) \
            .set_next(BondHandler()) \
            .set_next(FXFwdHandler()) \
            .set_next(IndexHandler()) \
            .set_next(FallbackHandler()) \

        self.chain = first

    def fetch(self, requests: List[BaseMarketRequest]):
        return self.chain.handle(requests, self.query_ts)
