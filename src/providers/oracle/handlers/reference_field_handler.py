import logging
from typing import List, Dict, Any

from core.enums.instrument_types import InstrumentType
from core.requests.requests import BaseRequest
from providers.oracle.handlers.base_handlers import ReferenceFieldHandler
from providers.oracle.query_oracle import QueryOracle

logger = logging.getLogger(__name__)

class ISINLookupHandler(ReferenceFieldHandler):

    def can_handle(self, req) -> bool:
        return "ISIN" in req.fields

    def process(self, requests, query: QueryOracle):
        subs = [req.subscription or req.instrument.ticker for req in requests]
        types = [req.instrument.type for req in requests]

        if len(set(types)) != 1:
            raise ValueError("ISIN lookup richiede un solo instrument.type per batch")

        instrument_type = types[0]

        # Oracle → { sub: { ISIN : value } }
        return query.get_isin_by_ticker(subs, instrument_type)


class ETFStaticHandler(ReferenceFieldHandler):
    ETF_STATIC = {
        "DESCRIPTION", "TICKER", "INSTRUMENT_TYPE", "UNDERLYING_TYPE",
        "UNDERLYING_CATEGORY", "ETP_TYPE", "LEVERAGE", "CURRENCY_HEDGING",
        "FUND_CURRENCY", "PAYMENT_POLICY", "ISSUE_DATE", "PRIMARY_EXCHANGE_CODE",
        "TER", "MARKETS"
    }

    def can_handle(self, req) -> bool:
        return (
                req.instrument.type == "ETP" and
                any(f in self.ETF_STATIC for f in req.fields)
        )

    def process(
            self,
            requests: List[BaseRequest],
            query: QueryOracle
    ) -> Dict[str, Dict[str, Any]]:
        """
        Gestisce field statici ETF.

        Returns:
            {isin: {field: value, ...}}
            Garantisce presenza di tutti gli ISIN richiesti.
        """
        isins = [req.instrument.isin for req in requests]
        fields = set(f for r in requests for f in r.fields)

        # Inizializza con tutti gli ISIN
        result = {isin: {} for isin in isins}

        # TER
        if "TER" in fields:
            ter_data = query.get_etf_ter(isins)
            for isin, data in ter_data.items():
                if isin in result:
                    result[isin].update(data)  # update invece di sovrascrivere

        # MARKETS
        if "MARKETS" in fields:
            markets_data = query.get_etf_markets(isins)
            # markets_data è List[Dict], va trasformato
            markets_by_isin = {}
            for item in markets_data:
                isin = item.get("isin") or item.get("ISIN")
                if isin:
                    markets_by_isin.setdefault(isin, []).append(item)

            for isin, markets_list in markets_by_isin.items():
                if isin in result:
                    result[isin]["MARKETS"] = markets_list

        # Altri field statici
        other = fields - {"TER", "MARKETS"}
        if other:
            static_data = query.get_etf_static_field(isins, subset=list(other))
            for isin, data in static_data.items():
                if isin in result:
                    result[isin].update(data)

        # Verifica completezza: aggiungi None per field mancanti
        for isin in isins:
            missing = fields - set(result[isin].keys())
            for field in missing:
                result[isin][field] = None
                logger.debug(f"Field '{field}' not found for {isin}, set to None")

        return result

class FutureStaticHandler(ReferenceFieldHandler):
    FUTURE_FIELDS = {
        "BBG_TYPE", "EXCH_SYMBOL", "UNDERLYING", "COUNTRY",
        "GEOGRAPHICAL_AREA", "CONTRACT_SIZE", "UNDERLYING_PRICE_MULTIPLIER",
        "ECONOMY", "CFI_CODE", "DELIVERY_TYPE", "REFERENCE_MARKET",
        "CALENDAR", "VALID_FROM"
    }

    def can_handle(self, req) -> bool:
        return (
                req.instrument.type == "FUTURE" and
                any(f in self.FUTURE_FIELDS for f in req.fields)
        )

    def _can_handle_single(self, typ, field):
        return typ == "FUTURE" and field in self.FUTURE_FIELDS

    def process(self, requests: List[BaseRequest], query: QueryOracle) -> Dict[str, Dict[str, Any]]:
        """
        Gestisce field statici Future.

        Returns:
            {ticker: {field: value, ...}}
            Garantisce presenza di tutti i ticker richiesti.
        """
        tickers = [req.instrument.ticker for req in requests]
        fields_list = [f for r in requests for f in r.fields if self._can_handle_single(r.instrument.type, f)]

        if not fields_list:
            return {}

        # 🆕 Inizializza con tutti i ticker
        result = {ticker: {f.upper(): None for f in fields_list} for ticker in tickers}

        # Query Oracle
        raw_data = query.get_future_field_by_roots(fields_list, root_tickers=tickers)

        # 🆕 Popola i dati trovati
        # raw_data è List[Dict] con chiave TICKER
        for row in raw_data:
            ticker = row.get("TICKER")
            if ticker and ticker in result:
                for field in fields_list:
                    field_upper = field.upper()
                    if field_upper in row:
                        result[ticker][field_upper] = row[field_upper]

        return result


class StockHandler(ReferenceFieldHandler):
    STOCK_FIELDS = {
        "PRIMARY_TICKER", "PRIMARY_EXCHANGE_CODE", "CURRENCY",
        "EXCHANGE_CODE", "TICKER"
    }

    def can_handle(self, req) -> bool:
        return (
                req.instrument.type == "STOCK" and
                any(f in self.STOCK_FIELDS for f in req.fields)
        )

    def process(self, requests: List[BaseRequest], query: QueryOracle) -> Dict[str, Dict[str, Any]]:
        """
        Gestisce field statici Stock.

        Returns:
            {identifier: {field: value, ...}}
            Garantisce presenza di tutti gli identifier richiesti.
        """
        isins = [r.instrument.isin for r in requests]
        markets = [r.instrument.market for r in requests]
        tickers = [r.instrument.ticker for r in requests]
        fields = list(set(f for r in requests for f in r.fields))

        # 🆕 Inizializza con tutti gli identifier (usa ISIN se presente, altrimenti ticker)
        identifiers = [isin if isin else ticker for isin, ticker in zip(isins, tickers)]
        result = {identifier: {f.upper(): None for f in fields} for identifier in identifiers}

        # Query Oracle
        raw_data = query.get_equity_field(
            isin=isins,
            market=markets,
            fields=fields,
            ticker=tickers
        )

        # 🆕 Popola i dati trovati
        for identifier, data in raw_data.items():
            if identifier in result:
                result[identifier].update(data)

        return result

class SwapHandler(ReferenceFieldHandler):

    SWAP_FIELDS = {"TICKER", "TENOR", "SETTLEMENT_DAYS", "SWAP_TYPE"}

    def can_handle(self, req) -> bool:
        return (
                req.instrument.type == InstrumentType.SWAP and
                any(f in self.SWAP_FIELDS for f in req.fields)
        )

    def process(self, requests: List[BaseRequest], query: QueryOracle) -> Dict[str, Dict[str, Any]]:
        """
        Gestisce field statici Swap.

        Returns:
            {ticker: {field: value, ...}}
            Garantisce presenza di tutti i ticker richiesti.
        """
        tickers = [r.instrument.ticker for r in requests]
        fields = list(set(f for r in requests for f in r.fields))

        # 🆕 Inizializza con tutti i ticker
        result = {ticker: {f.upper(): None for f in fields} for ticker in tickers}

        # Query Oracle
        raw_data = query.get_swap_field(ticker=tickers, fields=fields)

        # 🆕 Popola i dati trovati
        for ticker, data in raw_data.items():
            if ticker in result:
                result[ticker].update(data)

        return result


class CDSIndexHandler(ReferenceFieldHandler):
    CDX_FIELDS = {
        "TICKER_ROOT", "INDEX_NAME", "TENOR", "BBG_TYPE", "DESCRIPTION",
        "DAY_COUNT_CONV", "SERIES_START_DATE", "CURRENCY", "GEOGRAPHICAL_AREA",
        "ECONOMY", "CREDIT_SCORE", "VALID_FROM"
    }

    def can_handle(self, req) -> bool:
        return (
                req.instrument.type == InstrumentType.CDXINDEX and
                any(f in self.CDX_FIELDS for f in req.fields)
        )

    def process(self, requests: List[BaseRequest], query: QueryOracle) -> Dict[str, Dict[str, Any]]:
        """
        Gestisce field statici CDX.

        Returns:
            {ticker: {field: value, ...}}
            Garantisce presenza di tutti i ticker richiesti.
        """
        tickers = [r.instrument.ticker for r in requests]
        fields = list(set(f for r in requests for f in r.fields))

        # 🆕 Inizializza con tutti i ticker
        result = {ticker: {f.upper(): None for f in fields} for ticker in tickers}

        # Query Oracle
        raw_data = query.get_cdx_fields(tickers, fields)

        # 🆕 raw_data formato: {field: {ticker: value}}
        # Dobbiamo invertire in: {ticker: {field: value}}
        for field, ticker_values in raw_data.items():
            for ticker, value in ticker_values.items():
                if ticker in result:
                    result[ticker][field.upper()] = value

        return result
