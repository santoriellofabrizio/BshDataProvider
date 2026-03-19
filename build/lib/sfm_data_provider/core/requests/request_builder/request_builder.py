
from sfm_data_provider.core.requests.request_builder.info_request_builder import StaticRequestBuilder
from sfm_data_provider.core.requests.request_builder.market_request_builder import MarketRequestBuilder


class RequestBuilder:
    """Facade che coordina MarketRequestBuilder e StaticRequestBuilder."""

    _helper = None

    @classmethod
    def set_helper(cls, helper):
        cls._helper = helper

    @property
    def helper(self):
        return self._helper

    # Alias pubblici coerenti con la versione precedente
    build_market_request = staticmethod(MarketRequestBuilder.build)
    build_static_request = staticmethod(StaticRequestBuilder.build)

    select_market_class = staticmethod(MarketRequestBuilder.select_class)
    select_static_class = staticmethod(StaticRequestBuilder.select_class)
