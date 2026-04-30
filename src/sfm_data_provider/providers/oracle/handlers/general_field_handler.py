import logging
from typing import Any, List

from sfm_data_provider.core.requests.requests import BulkRequest
from sfm_data_provider.providers.oracle.handlers.base_handlers import GeneralHandler
from sfm_data_provider.providers.oracle.query_oracle import QueryOracle


logger = logging.getLogger(__name__)


class ETPIsinsHandler(GeneralHandler):
    """Handles ETP_ISINS field."""

    def can_handle(self, request) -> bool:
        field = request.fields
        if len(field) > 1:
            raise ValueError("can only handle one general field at a time")
        return field[0].upper() == "ETP_ISINS"

    def process(self, request, query: QueryOracle) -> Any:
        if isinstance(request, list):
            request = request[0]
        prm = request.extra_params or {}
        return query.get_etp_isins(
            segments=prm.pop("segments"),
            underlyings=prm.pop("underlying"),
            currencies=prm.pop("currency"),
            **prm
        )

    def handle(self, requests: List[BulkRequest], query: QueryOracle):
        if isinstance(requests, list):
            if len(requests) > 1:
                raise ValueError("can only handle one general field at a time")

        if self.can_handle(requests[0]):
            return self.process(requests[0], query)
        else:
            return self._next.handle(requests, query)


class ETFMarketsHandler(GeneralHandler):
    """Handles ETF_MARKETS field."""

    def can_handle(self, request) -> bool:
        if isinstance(request, list):
            request = request[0]
        return request.field.upper() == "ETF_MARKETS"

    def process(self, request, query: QueryOracle) -> Any:
        return query.get_all_markets()


class FuturesDataHandler(GeneralHandler):
    """Handles FUTURES_DATA field."""

    def can_handle(self, request) -> bool:
        return request.field.upper() == "FUTURES_DATA"

    def process(self, request, query: QueryOracle) -> Any:
        return query.get_futures_data()


class FuturesIdentifiersHandler(GeneralHandler):
    """Handles FUTURES_IDENTIFIERS field."""

    def can_handle(self, request) -> bool:
        return request.field.upper() == "FUTURES_IDENTIFIERS"

    def process(self, request, query: QueryOracle) -> Any:
        return query.get_futures_identifiers()


class FuturesRootsHandler(GeneralHandler):
    """Handles FUTURES_ROOTS field."""

    def can_handle(self, request) -> bool:
        return request.field.upper() == "FUTURES_ROOTS"

    def process(self, request, query: QueryOracle) -> Any:
        return query.get_future_field_by_roots(request.field)


class CurrenciesHandler(GeneralHandler):
    """Handles CURRENCIES field."""

    def can_handle(self, request) -> bool:
        return request.field.upper() == "CURRENCIES"

    def process(self, request, query: QueryOracle) -> Any:
        return query.get_currencies_codes()


class InstrumentTypesHandler(GeneralHandler):
    """Handles INSTRUMENT_TYPES field."""

    def can_handle(self, request) -> bool:
        return request.field.upper() == "INSTRUMENT_TYPES"

    def process(self, request, query: QueryOracle) -> Any:
        return query.get_instrument_types()


class SwapsHandler(GeneralHandler):
    """Handles SWAPS field."""

    def can_handle(self, request) -> bool:
        return request.field.upper() == "SWAPS"

    def process(self, request, query: QueryOracle) -> Any:
        return query.get_swap_data()
