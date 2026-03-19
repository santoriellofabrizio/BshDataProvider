from datetime import date
from typing import Any, List, Dict

from core.requests.requests import BaseStaticRequest, HistoricalRequest
from providers.oracle.handlers.base_handlers import HistoricalFieldHandler
from providers.oracle.query_oracle import QueryOracle


class NAVHistoricalHandler(HistoricalFieldHandler):

    def can_handle(self, req) -> bool:

        return (
            "NAV" in [f.upper() for f in req.fields]
        )

    def process(self, requests: List[HistoricalRequest], query: QueryOracle) -> Dict[str, Dict[str, Any]]:
        """
        Gestisce NAV storici.

        Returns:
            {isin: {"NAV": {date: value, ...}}}
            Garantisce presenza di tutti gli ISIN richiesti.
        """
        isins = [r.instrument.isin for r in requests]

        # Estrai start/end dalla prima request (dovrebbero essere uguali per il batch)
        first = requests[0]
        start = first.start
        end = first.end or date.today()

        # 🆕 Crea correlation mapping se necessario
        corr_id_mapping = {r.instrument.isin: r.instrument.id for r in requests}

        # Query Oracle (già fixata, restituisce tutti gli ISIN)
        result = query.get_etf_nav(isins, start, end, corr_id_mapping)

        # result è già nel formato corretto: {isin: {"NAV": {...}}}
        return result


class DividendHistoricalHandler(HistoricalFieldHandler):

    def can_handle(self, req) -> bool:
        f = req.fields
        if isinstance(f, list):
            return any(x.upper() in ("DIVIDEND", "DIVIDENDS") for x in f)
        return f.upper() in ("DIVIDEND", "DIVIDENDS")

    def process(self, requests: List[BaseStaticRequest], query: QueryOracle) -> Dict[str, Dict[str, Any]]:
        """
        Gestisce dividendi storici.

        Returns:
            {isin: {"DIVIDEND_AMOUNT": {date: value, ...}}}
            Garantisce presenza di tutti gli ISIN richiesti.
        """
        isins = [r.instrument.isin for r in requests]

        # Estrai start/end dalla prima request
        first = requests[0]
        start = first.start
        end = first.end or date.today()

        # 🆕 Crea correlation mapping
        corr_id_mapping = {r.instrument.isin: r.instrument.id for r in requests}

        # Query Oracle (già fixata)
        result = query.get_etf_dividends(isins, start, end, corr_id_mapping)

        # result è già nel formato corretto
        return result