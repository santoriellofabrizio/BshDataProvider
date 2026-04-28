from datetime import date
from typing import List, Dict, Any

from sfm_data_provider.core.requests.requests import BulkRequest

from sfm_data_provider.providers.oracle.handlers.base_handlers import BulkFieldHandler
from sfm_data_provider.providers.oracle.query_oracle import QueryOracle
from sfm_data_provider.core.utils.memory_provider import cache_bsh_data

class PCFCompositionHandler(BulkFieldHandler):
    """Handles PCF (Portfolio Composition File) bulk data."""
    _HANDLED_FIELD = "PCF_COMPOSITION"

    def can_handle(self, req) -> bool:
        return self._HANDLED_FIELD in req.fields

    def process(self, requests: List[BulkRequest], query: QueryOracle) -> Dict[str, Dict[str, Any]]:
        """
        Gestisce composizione PCF.

        Returns:
            {isin: {"pcf_composition": [...]}}
            Garantisce presenza di tutti gli ISIN richiesti.
        """
        isins = [r.instrument.isin for r in requests if r.instrument]

        # Parametri dalla prima request
        first = requests[0]
        reference_date = first.extra_params.get("reference_date")
        include_cash = first.extra_params.get("include_cash", False)

        # Query Oracle (già fixata)
        result = query.get_etf_pcf(
            isin_list=isins,
            reference_date=reference_date,
            include_cash=include_cash
        )

        # result è già nel formato corretto: {isin: {"pcf_composition": [...]}}
        return result

class FXCompositionHandler(BulkFieldHandler):
    """Handles FX Composition bulk data."""
    _HANDLED_FIELD = "FX_COMPOSITION"

    def can_handle(self, req) -> bool:
        return self._HANDLED_FIELD in [f.upper() for f in req.fields]


    def process(self, requests: List[BulkRequest], query: QueryOracle) -> Dict[str, Dict[str, Any]]:

        isins = tuple(sorted(r.instrument.isin for r in requests))  # hashable
        first = requests[0]
        reference_date = first.extra_params.get("reference_date") or date.today()
        fx_fxfwrd = first.extra_params.get("fx_fxfwrd", "both")

        return self._cached_get_etf_fx(isins, reference_date, fx_fxfwrd, query)

    @cache_bsh_data
    def _cached_get_etf_fx(self, isins: tuple, day: date, fx_fxfwrd: str, query: QueryOracle):
        # Qui query è ignorato nella cache key
        return query.get_etf_fx(isin_list=list(isins), day=day, fx_fxfwrd=fx_fxfwrd)


class MarketsInfoHandler(BulkFieldHandler):
    """Handles markets information for ETF instruments."""

    _HANDLED_FIELD = "MARKETS"

    def can_handle(self, req) -> bool:
        return self._HANDLED_FIELD in req.fields

    def process(self, requests, query: QueryOracle):
        subs = [r.subscription for r in requests]
        return query.get_etf_markets(subs)


class StockMarketsInfoHandler(BulkFieldHandler):
    """Handles market info for STOCK instruments."""

    _HANDLED_FIELD = "STOCK_MARKETS_INFO"

    def can_handle(self, req) -> bool:
        return self._HANDLED_FIELD in req.fields

    def process(self, requests: List[BulkRequest], query: QueryOracle) -> Dict[str, Dict[str, Any]]:
        """
        Gestisce info mercati per stock.

        Returns:
            {isin: {"stock_markets_info": [...]}}
            Garantisce presenza di tutti gli ISIN richiesti.
        """
        isins = [r.instrument.isin for r in requests]

        # Query Oracle
        result = query.get_stock_markets_info(isins)

        # 🆕 Garantisci presenza di tutti gli ISIN
        for isin in isins:
            if isin not in result:
                result[isin] = {"stock_markets_info": []}

        return result