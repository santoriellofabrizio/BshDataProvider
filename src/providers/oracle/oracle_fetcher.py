from typing import Type, Dict

from core.requests.requests import (
    ReferenceRequest,
    GeneralRequest, HistoricalRequest,
)
from providers.oracle.handlers.base_handlers import Handler
from providers.oracle.handlers.bulk_field_handler import PCFCompositionHandler, FXCompositionHandler, \
    MarketsInfoHandler, StockMarketsInfoHandler

# Handlers
from providers.oracle.handlers.general_field_handler import *
from providers.oracle.handlers.historical_field_handler import NAVHistoricalHandler, DividendHistoricalHandler
from providers.oracle.handlers.reference_field_handler import ISINLookupHandler, ETFStaticHandler, FutureStaticHandler, \
    StockHandler, SwapHandler, CDSIndexHandler

logger = logging.getLogger(__name__)


class OracleFetcher:
    """
    Oracle data fetcher implementing Chain-of-Responsibility with batch processing.
    """

    # ======================================================
    # CTOR
    # ======================================================

    def __init__(self, query: QueryOracle):
        self.query = query

        self._reference_chain = self._build_chain([
            ISINLookupHandler,
            ETFStaticHandler,
            FutureStaticHandler,
            StockHandler,
            SwapHandler,
            CDSIndexHandler,
        ])

        self._historical_chain = self._build_chain([
            NAVHistoricalHandler,
            DividendHistoricalHandler,
        ])

        self._bulk_chain = self._build_chain([
            PCFCompositionHandler,
            FXCompositionHandler,
            MarketsInfoHandler,
            StockMarketsInfoHandler,
        ])

        self._general_chain = self._build_chain([
            ETPIsinsHandler,
            ETFMarketsHandler,
            FuturesDataHandler,
            FuturesIdentifiersHandler,
            FuturesRootsHandler,
            CurrenciesHandler,
            InstrumentTypesHandler,
            SwapsHandler,
        ])

    # ======================================================
    # CHAIN BUILDER
    # ======================================================

    @staticmethod
    def _build_chain(handler_classes: List[Type[Handler]]) -> Handler:
        """
        Crea una chain completa da una lista di CLASSI di handler.
        """
        handlers = [cls() for cls in handler_classes]

        for current, nxt in zip(handlers, handlers[1:]):
            current.set_next(nxt)

        return handlers[0]

    # ======================================================
    # GENERIC GROUPER
    # ======================================================

    @staticmethod
    def _group_by_field(requests):
        """
        Raggruppa le request per field, indipendentemente dal tipo.
        Ritorna:
            { FIELD: [requests] }
        """
        grouped: Dict[str, List[Any]] = {}

        for req in requests:
            fields = req.fields if isinstance(req.fields, list) else [req.fields]
            for f in fields:
                grouped.setdefault(f.upper(), []).append(req)

        return grouped

    # ======================================================
    # GENERIC FETCH
    # ======================================================

    def _run_chain(self, requests, chain: Handler, group_by_field=True):
        """
        Esegue una lista di request attraverso la chain specificata.
        Se group_by_field=True:
            raggruppa per field (Reference, Historical, General).
        Se group_by_field=False:
            NON raggruppa (Bulk).
        """

        if not requests:
            return {}

        groups = (
            self._group_by_field(requests)
            if group_by_field
            else {requests[0].fields[0].upper(): requests}  # Bulk ha 1 solo field
        )

        results = {}

        for field, req_group in groups.items():
            out = chain.handle(req_group, self.query)
            if out:
                results.update(out)

        return results

    # ======================================================
    # PUBLIC API
    # ======================================================

    def fetch_reference(self, requests: List[ReferenceRequest]):
        return self._run_chain(requests, self._reference_chain, group_by_field=True)

    def fetch_historical(self, requests: List[HistoricalRequest]):
        return self._run_chain(requests, self._historical_chain, group_by_field=True)

    def fetch_bulk(self, requests: List[BulkRequest]):
        return self._run_chain(requests, self._bulk_chain, group_by_field=False)

    def fetch_general(self, requests: List[GeneralRequest]):
        if len(requests) > 1:
            raise ValueError("Can only handle one general request at a time")
        return self._run_chain(requests, self._general_chain, group_by_field=True)
