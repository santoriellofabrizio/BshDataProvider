from abc import ABC
from typing import List

from core.requests.requests import BaseMarketRequest, BaseStaticRequest


class BaseProvider(ABC):

    def fetch_market_data(self, request: BaseMarketRequest | List[BaseMarketRequest]):
        pass

    def fetch_info_data(self, request: BaseStaticRequest | List[BaseStaticRequest]):
        pass

    def healthcheck(self):
        pass
