"""
bloomberg_info_market_fetcher.py — Bloomberg static and bulk data fetcher.

This module defines :class:`BloombergInfoMarketFetcher`, the unified interface
for retrieving Bloomberg *non-market* data via BLPAPI. It supports three major
Bloomberg request types:
    - **ReferenceDataRequest** → static or semi-static fields (TER, ISIN, listings, etc.)
    - **HistoricalDataRequest** → time-series of non-market data (e.g., YAS, carry, NAV history)
    - **BulkDataRequest** → tabular fields (e.g., DVD_HIST_ALL, EQY_FUND_CRNCY_HIST)

Each request uses custom correlation IDs to ensure results can be traced back
to the originating instrument. The class handles request creation, event loop
management, parsing, and normalization of Bloomberg responses.

Responsibilities:
    - Build and send Bloomberg reference, historical, and bulk requests
    - Handle event-driven BLPAPI responses and errors
    - Parse nested field structures into Python dictionaries
    - Normalize date and structure formats for dividends and other bulk data
    - Integrate with BSH cache decorators for improved performance

Example:
    >>> fetcher = BloombergInfoMarketFetcher(session)
    >>> ref = fetcher.fetch_reference_data(["IHYG IM Equity"], ["FUND_TOTAL_EXP"], ["IHYG"])
    >>> hist = fetcher.fetch_historical_data(["IHYG IM Equity"], ["FUND_NET_ASSET_VAL"], ["IHYG"])
    >>> bulk = fetcher.fetch_bulk_data(["IHYG IM Equity"], ["DVD_HIST_ALL"], ["IHYG"])
"""

import datetime
import logging
from datetime import date, timedelta
from typing import List, Dict, Any, Optional, Set

import blpapi
from dateutil.utils import today

from core.base_classes.base_fetcher import BaseMarketFetcher
from core.utils.memory_provider import cache_bsh_data

logger = logging.getLogger(__name__)


class BloombergInfoMarketFetcher(BaseMarketFetcher):
    """
    Bloomberg fetcher for *non-market* (static or bulk) data.

    This class manages all Bloomberg data requests not directly related to
    real-time or tick-based market data. It wraps the BLPAPI interface for
    sending and receiving requests of the following types:
        - ``ReferenceDataRequest`` for static fields (e.g., TER, ISIN, description)
        - ``HistoricalDataRequest`` for non-market time series (e.g., YAS, carry)
        - ``BulkDataRequest`` for tabular datasets (e.g., DVD_HIST_ALL)

    All requests use explicit correlation IDs (usually instrument identifiers)
    to maintain consistent mappings between input subscriptions and output data.

    Responsibilities:
        - Manage Bloomberg reference/historical/bulk requests
        - Map securities (tickers/ISINs) into Bloomberg-compliant codes
        - Parse BLPAPI messages and convert them to Python dicts
        - Handle and log security/field-level errors
        - Support caching via ``@cache_bsh_data`` decorator

    Args:
        session (blpapi.Session): Active Bloomberg session.
        service (blpapi.Service | None): Optional pre-opened RefData service.
        show_progress (bool): Whether to display progress information.

    Example:
        >>> fetcher = BloombergInfoMarketFetcher(session)
        >>> result = fetcher.fetch_reference_data(
        ...     subscriptions=["IHYG IM Equity"],
        ...     fields=["FUND_TOTAL_EXP"],
        ...     corr_ids=["IHYG"]
        ... )
        >>> print(result["IHYG"]["FUND_TOTAL_EXP"])
    """

    SERVICE_NAME = "//blp/refdata"

    def __init__(self, session, service=None, show_progress: bool = True):
        super().__init__()
        self.session = session
        self.service = service or session.getService(self.SERVICE_NAME)
        self.show_progress = show_progress
        logger.debug("BloombergInfoMarketFetcher initialized")

    # ============================================================
    # PUBLIC METHODS
    # ============================================================
    @cache_bsh_data
    def fetch_reference_data(
            self,
            subscriptions: List[str],
            fields: List[str],
            corr_ids: List[str],
    ) -> Dict[str, Dict[str, Any]]:
        """
        ReferenceDataRequest Bloomberg per dati statici (TER, ISIN, listing, ecc.).
        
        Invia TUTTE le richieste in batch e raccoglie tutte le risposte insieme.
        
        Returns:
            Dict[corr_id, Dict[field, value]]
        """
        if not subscriptions or not fields:
            logger.warning("Empty subscriptions or fields for ReferenceDataRequest")
            return {}

        if len(subscriptions) != len(corr_ids):
            raise ValueError("subscriptions and corr_ids must have same length")

        logger.info("Fetching Bloomberg ReferenceData: %s for %d instruments", fields, len(subscriptions))
        
        # PHASE 1: Invia TUTTE le richieste
        self._send_reference_request(subscriptions, fields, corr_ids)
        
        # PHASE 2: Raccoglie TUTTE le risposte
        return self._collect_batch_responses(
            response_type="ReferenceDataResponse",
            parser=self._parse_reference_response,
            expected_corr_ids=set(corr_ids)
        )

    @cache_bsh_data
    def fetch_historical_data(
            self,
            subscriptions: List[str],
            fields: List[str],
            corr_ids: List[str],
            *,
            start: Optional[date] = None,
            end: Optional[date] = None,
            periodicity: str = "DAILY",
    ) -> Dict[str, Any]:
        """
        HistoricalDataRequest Bloomberg per dati storici NON di mercato (es. YAS, carry, ecc.).
        
        Invia TUTTE le richieste in batch e raccoglie tutte le risposte insieme.
        
        Returns:
            Dict[corr_id, Dict[field, Dict[date, value]]]
        """
        if not subscriptions or not fields:
            logger.warning("Empty subscriptions or fields for HistoricalDataRequest")
            return {}

        if len(subscriptions) != len(corr_ids):
            raise ValueError("subscriptions and corr_ids must have same length")

        logger.info("Fetching Bloomberg HistoricalData: %s for %d instruments", fields, len(subscriptions))
        
        # PHASE 1: Invia TUTTE le richieste
        self._send_historical_request(subscriptions, fields, corr_ids, start, end, periodicity)
        
        # PHASE 2: Raccoglie TUTTE le risposte
        raw_data = self._collect_batch_responses(
            response_type="HistoricalDataResponse",
            parser=self._parse_historical_response,
            expected_corr_ids=set(corr_ids)
        )

        # Trasforma nel formato atteso: {corr_id: {field: {date: value}}}
        return {
            k: {f: {d: v for d, x in v.items() for f, v in x.items()}
                for f in {i for x in v.values() for i in x}}
            for k, v in raw_data.items()
        }

    @cache_bsh_data
    def fetch_bulk_data(
            self,
            subscriptions: List[str],
            fields: List[str],
            corr_ids: List[str],
            start: Optional[date] = None,
            end: Optional[date] = None
    ) -> Dict[str, Any]:
        """
        ReferenceDataRequest per campi BULK (es. DVD_HIST_ALL, EQY_FUND_CRNCY_HIST).
        
        Invia TUTTE le richieste in batch e raccoglie tutte le risposte insieme.
        
        Restituisce: {corr_id: {field: {date: value}}} oppure {} se il titolo non ha dati.
        """
        if not subscriptions or not fields:
            logger.warning("Empty subscriptions or fields for BulkDataRequest")
            return {}

        if len(subscriptions) != len(corr_ids):
            raise ValueError("subscriptions and corr_ids must have same length")

        logger.info("Fetching Bloomberg bulk data: %s for %d instruments", fields, len(subscriptions))
        
        # PHASE 1: Invia TUTTE le richieste
        self._send_reference_request(subscriptions, fields, corr_ids)
        
        # PHASE 2: Raccoglie TUTTE le risposte
        raw_data = self._collect_batch_responses(
            response_type="ReferenceDataResponse",
            parser=self._parse_bulk_response,
            expected_corr_ids=set(corr_ids)
        )

        # Normalizza i risultati finali
        return self.parse_bulk_raw_data(raw_data, fields, start, end)

    # ============================================================
    # REQUEST BUILDERS
    # ============================================================

    def _send_reference_request(self, subscriptions: List[str], fields: List[str], corr_ids: List[str]):
        """
        Invia una o più ReferenceDataRequest con correlationId personalizzati.
        
        Ogni richiesta viene inviata separatamente con il proprio correlationId.
        Bloomberg processerà tutte le richieste in parallelo.
        """
        for sec, cid in zip(subscriptions, corr_ids):
            request = self.service.createRequest("ReferenceDataRequest")

            # Se termina con ' ISIN', converte in /isin/
            if sec.upper().endswith(" ISIN"):
                isin = sec.split()[0]
                bb_code = f"/isin/{isin}"
            else:
                bb_code = sec

            request.append("securities", bb_code)
            for f in fields:
                request.append("fields", f)

            # Crea CorrelationId con il corr_id come stringa
            corr_id_obj = blpapi.CorrelationId(cid)
            self.session.sendRequest(request, correlationId=corr_id_obj)
            logger.debug("Sent ReferenceDataRequest: %s (corr_id=%s)", bb_code, cid)

    def _send_historical_request(
            self,
            subscriptions: List[str],
            fields: List[str],
            corr_ids: List[str],
            start: Optional[date],
            end: Optional[date],
            periodicity: str,
    ):
        """
        Invia una o più HistoricalDataRequest con correlationId personalizzati.
        
        Ogni richiesta viene inviata separatamente con il proprio correlationId.
        Bloomberg processerà tutte le richieste in parallelo.
        """
        for sec, cid in zip(subscriptions, corr_ids):
            request = self.service.createRequest("HistoricalDataRequest")
            start_date = (start or (today() - timedelta(days=365))).strftime("%Y%m%d")
            request.set("startDate", start_date)
            
            if callable(sec):
                sec = sec(current_date=start)

            if sec.upper().endswith(" ISIN"):
                isin = sec.split()[0]
                bb_code = f"/isin/{isin}"
            else:
                bb_code = sec

            request.append("securities", bb_code)
            for f in fields:
                request.append("fields", f)

            if end:
                request.set("endDate", end.strftime("%Y%m%d"))

            # Crea CorrelationId con il corr_id come stringa
            corr_id_obj = blpapi.CorrelationId(cid)
            self.session.sendRequest(request, correlationId=corr_id_obj)
            logger.debug("Sent HistoricalDataRequest: %s (corr_id=%s)", bb_code, cid)

    # ============================================================
    # RESPONSE LOOP - BATCHED VERSION
    # ============================================================

    def _collect_batch_responses(
            self,
            response_type: str,
            parser,
            expected_corr_ids: Set[str]
    ) -> Dict[str, Any]:
        """
        Raccoglie tutte le risposte Bloomberg per un batch di richieste.
        
        Questo metodo continua a leggere eventi finché:
        1. Tutte le risposte attese sono state ricevute, OPPURE
        2. Si verifica un timeout ripetuto
        
        Args:
            response_type: Tipo di risposta Bloomberg (es. "ReferenceDataResponse")
            parser: Funzione per parsare ogni messaggio
            expected_corr_ids: Set di correlation IDs che ci aspettiamo
            
        Returns:
            Dictionary con tutti i dati raccolti: {corr_id: data}
        """
        all_data: Dict[str, Any] = {}
        errors: Dict[str, str] = {}
        pending_corr_ids = expected_corr_ids.copy()
        
        timeout_count = 0
        max_timeouts = 20

        while pending_corr_ids and timeout_count < max_timeouts:
            try:
                ev = self.session.nextEvent(timeout=2000)
                ev_type = ev.eventType()

                # Handle timeout
                if ev_type == blpapi.Event.TIMEOUT:
                    timeout_count += 1
                    logger.debug("Timeout %d/%d waiting for %d responses", 
                               timeout_count, max_timeouts, len(pending_corr_ids))
                    continue

                timeout_count = 0  # Reset on valid event

                # Process messages in PARTIAL_RESPONSE or RESPONSE events
                if ev_type in (blpapi.Event.PARTIAL_RESPONSE, blpapi.Event.RESPONSE):
                    for msg in ev:
                        # Verifica il tipo di messaggio
                        if msg.messageType() != blpapi.Name(response_type):
                            continue

                        # Recupera correlation IDs dal messaggio
                        # msg.correlationIds() ritorna una sequenza di CorrelationId objects
                        msg_corr_ids = msg.correlationIds()
                        
                        # Estrai il valore dal primo CorrelationId
                        # .value() ritorna l'oggetto originale passato a CorrelationId()
                        if not msg_corr_ids:
                            logger.warning("Message without correlation ID")
                            continue
                            
                        corr_id = str(msg_corr_ids[0].value())

                        # Verifica se questo corr_id è uno che ci aspettiamo
                        if corr_id not in pending_corr_ids:
                            logger.debug("Received response for unexpected corr_id: %s", corr_id)
                            continue

                        # Rimuovi subito dalla pending list
                        pending_corr_ids.discard(corr_id)
                        logger.debug("Received response for %s (%d pending)", 
                                   corr_id, len(pending_corr_ids))

                        # Parse security data in base al tipo di response
                        try:
                            if response_type == "HistoricalDataResponse":
                                # HistoricalDataResponse ha UN SOLO securityData element
                                sec_data = msg.getElement("securityData")
                                sec = sec_data.getElementAsString("security")
                                
                                # Gestisci errori a livello security
                                if sec_data.hasElement("securityError"):
                                    err = sec_data.getElement("securityError").getElementAsString("message")
                                    errors[corr_id] = err
                                    logger.warning("Bloomberg security error for %s: %s", corr_id, err)
                                    continue

                                # Gestisci errori a livello campo
                                if sec_data.hasElement("fieldExceptions"):
                                    self._process_field_exceptions(sec_data, corr_id, errors)

                                # Parse i dati se presenti
                                if sec_data.hasElement("fieldData"):
                                    parsed = parser(msg)
                                    # Sostituisci security con corr_id
                                    if sec in parsed:
                                        all_data[corr_id] = parsed[sec]
                                    else:
                                        logger.warning("Parser did not return data for %s", sec)
                            
                            else:
                                # ReferenceDataResponse ha un ARRAY di securityData elements
                                sec_data_array = msg.getElement("securityData")
                                
                                for i in range(sec_data_array.numValues()):
                                    sec_data = sec_data_array.getValueAsElement(i)
                                    sec = sec_data.getElementAsString("security")
                                    
                                    # Gestisci errori a livello security
                                    if sec_data.hasElement("securityError"):
                                        err = sec_data.getElement("securityError").getElementAsString("message")
                                        errors[corr_id] = err
                                        logger.warning("Bloomberg security error for %s: %s", corr_id, err)
                                        continue

                                    # Gestisci errori a livello campo
                                    if sec_data.hasElement("fieldExceptions"):
                                        self._process_field_exceptions(sec_data, corr_id, errors)

                                    # Parse i dati se presenti
                                    if sec_data.hasElement("fieldData"):
                                        parsed = parser(msg)
                                        # Sostituisci security con corr_id
                                        if sec in parsed:
                                            all_data[corr_id] = parsed[sec]
                                        else:
                                            logger.warning("Parser did not return data for %s", sec)
                        
                        except Exception as e:
                            logger.error("Error processing message for %s: %s", corr_id, e, exc_info=True)

                # Exit quando ricevi RESPONSE finale e non hai più pending
                if ev_type == blpapi.Event.RESPONSE:
                    if not pending_corr_ids:
                        logger.debug("All responses collected, exiting")
                        break
                    else:
                        logger.debug("Received RESPONSE event but still waiting for %d responses", 
                                   len(pending_corr_ids))

            except Exception as e:
                logger.error("Error in response collection loop: %s", e, exc_info=True)
                break

        if pending_corr_ids:
            logger.warning("Exited with %d unfulfilled requests: %s", 
                         len(pending_corr_ids), sorted(list(pending_corr_ids))[:10])

        logger.debug("%s completed: %d instruments received, %d errors, %d missing", 
                    response_type, len(all_data), len(errors), len(pending_corr_ids))
        
        return all_data

    def _process_field_exceptions(self, sec_data, corr_id: str, errors: Dict[str, str]):
        """Helper per processare field exceptions."""
        field_excs = sec_data.getElement("fieldExceptions")
        for j in range(field_excs.numValues()):
            exc = field_excs.getValueAsElement(j)
            field_id = exc.getElementAsString("fieldId")
            err_msg = exc.getElement("errorInfo").getElementAsString("message")
            errors[f"{corr_id}:{field_id}"] = err_msg
            logger.info("Bloomberg field error for %s.%s: %s", corr_id, field_id, err_msg)

    # ============================================================
    # PARSERS (identici a prima)
    # ============================================================

    def _parse_reference_response(self, msg) -> Dict[str, Dict[str, Any]]:
        """
        Parse ReferenceDataResponse message.
        
        Returns:
            Dict[security, Dict[field, value]]
        """
        data: Dict[str, Dict[str, Any]] = {}
        sec_data_array = msg.getElement("securityData")
        
        for i in range(sec_data_array.numValues()):
            sec_data = sec_data_array.getValueAsElement(i)
            sec = sec_data.getElementAsString("security")
            
            if sec_data.hasElement("fieldData"):
                field_data = sec_data.getElement("fieldData")
                record = {
                    str(f.name()): (f.getValue() if f.isValid() else None) 
                    for f in field_data.elements()
                }
                data[sec] = record
        
        return data

    def _parse_historical_response(self, msg) -> Dict[str, Any]:
        """
        Parse HistoricalDataResponse message.
        
        Returns:
            Dict[security, Dict[date, Dict[field, value]]]
        """
        data: Dict[str, Any] = {}
        sec_data = msg.getElement("securityData")
        sec = sec_data.getElementAsString("security")
        
        if not sec_data.hasElement("fieldData"):
            return data
            
        field_data_array = sec_data.getElement("fieldData")

        history: Dict[date, Dict[str, Any]] = {}
        for j in range(field_data_array.numValues()):
            bar = field_data_array.getValueAsElement(j)
            dt_val = bar.getElementAsDatetime("date")
            if hasattr(dt_val, "date"):
                dt_val = dt_val.date()
            
            record = {
                str(f.name()): f.getValue() 
                for f in bar.elements() 
                if str(f.name()) != "date"
            }
            history[dt_val] = record
        
        data[sec] = history
        return data

    def _parse_bulk_response(self, msg) -> Dict[str, Any]:
        """
        Parse ReferenceDataResponse for bulk fields.
        
        Returns:
            Dict[security, Dict[field, List[Dict] or value]]
        """
        data: Dict[str, Any] = {}
        sec_data_array = msg.getElement("securityData")
        
        for i in range(sec_data_array.numValues()):
            sec_data = sec_data_array.getValueAsElement(i)
            sec = sec_data.getElementAsString("security")
            
            if not sec_data.hasElement("fieldData"):
                continue
                
            field_data = sec_data.getElement("fieldData")
            record = {}
            
            for field in field_data.elements():
                name = str(field.name())
                if field.isArray():
                    records = [
                        self._parse_element(field.getValueAsElement(k)) 
                        for k in range(field.numValues())
                    ]
                    record[name] = records
                else:
                    record[name] = field.getValue()
            
            data[sec] = record
        
        return data

    def _parse_element(self, element) -> Dict[str, Any]:
        """Recursively parse a complex element."""
        record = {}
        for sub in element.elements():
            name = str(sub.name())
            if sub.isArray():
                record[name] = [
                    self._parse_element(sub.getValueAsElement(k)) 
                    for k in range(sub.numValues())
                ]
            elif sub.isComplexType():
                record[name] = self._parse_element(sub)
            else:
                try:
                    record[name] = sub.getValue()
                except Exception as e:
                    logger.info("Error getting value for %s: %s", name, e)
        return record

    # ============================================================
    # BULK HELPERS
    # ============================================================

    def parse_bulk_raw_data(self, raw_data, fields, start, end):
        """Parse and normalize bulk data based on field type."""
        match fields[0].upper():
            case "DVD_HIST_ALL":
                return self.parse_dividends_data(raw_data, start, end)
            case _:
                return raw_data

    def parse_dividends_data(
            self,
            raw_data: Dict[str, Any],
            start: date = None,
            end: date = None,
    ) -> Dict[str, Dict[str, Dict[date, float]]]:
        """
        Parsa i dati di dividendo da DVD_HIST_ALL e filtra per intervallo [start, end].
        Restituisce: {instrument_id: {"DIVIDEND_AMOUNT": {ex_date: amount}}}
        """
        parsed: Dict[str, Dict[str, Dict[date, float]]] = {}
        
        if isinstance(start, datetime.datetime):
            start = start.date()
        if isinstance(end, datetime.datetime):
            end = end.date()
        
        for instrument_id, fields in raw_data.items():
            dvd_data = fields.get("DVD_HIST_ALL")
            if not dvd_data or not isinstance(dvd_data, list):
                parsed[instrument_id] = {"DIVIDEND_AMOUNT": {}}
                continue
            
            divs: Dict[date, float] = {}
            for entry in dvd_data:
                ex_date = entry.get("Ex-Date") or entry.get("Ex Date")
                amount = entry.get("Dividend Amount") or entry.get("Amount")
                
                if not ex_date or amount is None:
                    continue
                
                if isinstance(ex_date, str):
                    try:
                        ex_date = date.fromisoformat(ex_date)
                    except ValueError:
                        continue
                
                if (start and ex_date < start) or (end and ex_date > end):
                    continue

                try:
                    divs[ex_date] = float(amount)
                except (TypeError, ValueError):
                    continue

            if divs:
                parsed[instrument_id] = {
                    "DIVIDEND_AMOUNT": dict(sorted(divs.items(), key=lambda x: x[0], reverse=True))
                }
            else:
                parsed[instrument_id] = {"DIVIDEND_AMOUNT": {}}

        return parsed
