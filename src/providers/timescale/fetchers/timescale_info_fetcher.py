from typing import List

from core.base_classes.base_fetcher import BaseMarketFetcher
from core.requests.requests import BaseStaticRequest
from providers.timescale.handlers.ytm_handler import YTMHandler
from providers.timescale.query_timescale import QueryTimeScale


class TimescaleInfoFetcher(BaseMarketFetcher):

    def __init__(self, query_ts: QueryTimeScale, show_progress=True):
        super().__init__(show_progress)
        self.query_ts = query_ts

        # COSTRUISCO LA CHAIN (ordine logico)
        first = YTMHandler()

        self.chain = first

    def fetch(self, requests: List[BaseStaticRequest]):
        return self.chain.handle(requests, self.query_ts)
