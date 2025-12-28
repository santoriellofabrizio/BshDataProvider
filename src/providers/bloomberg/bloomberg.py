"""
bloomberg_provider.py — Unified provider for Bloomberg market and static data.

This module defines the :class:`BloombergProvider`, a unified entry point to access
Bloomberg data (both market and reference) through the BLPAPI interface. It manages
the Bloomberg session lifecycle, initializes a unified fetcher for all data types,
and routes requests according to their type and frequency.

Responsibilities:
    - Start and manage Bloomberg API sessions
    - Open the Bloomberg RefData service
    - Dispatch market data requests (daily, intraday, snapshots)
    - Dispatch static data requests (reference, bulk, historical)
    - Map internal BSH field names to Bloomberg field codes and back
    - Integrate with :class:`BloombergFetcher` for all data retrieval

Example:
    >>> provider = BloombergProvider(host="localhost", port=8194)
    >>> result = provider.fetch_market_data(daily_requests)
    >>> ref = provider.fetch_info_data(reference_requests)
    >>> provider.close()
"""

# bshdata/providers/bloomberg/provider.py
import logging
import blpapi
from typing import List, Dict, Any

from core.base_classes.base_provider import BaseProvider
from core.requests.requests import BaseMarketRequest, BaseStaticRequest

from providers.bloomberg.bloomberg_fetcher import BloombergFetcher

logger = logging.getLogger(__name__)

BSH_TO_BBG = {
     "TER": "FUND_TOTAL_EXP",
     "DIVIDEND": "DVD_HIST_ALL",
     "DIVIDENDS": "DVD_HIST_ALL",
     "NAV": "FUND_NET_ASSET_VAL"
}


class BloombergProvider(BaseProvider):
    """
    Unified provider for Bloomberg data access.

    The BloombergProvider coordinates all interactions with Bloomberg via BLPAPI.
    It handles the session setup, service initialization, and routing of both market
    and static data requests to a unified fetcher.

    Responsibilities:
        - Manage Bloomberg connection and service lifecycle
        - Dispatch all request types (daily, intraday, snapshot, reference, historical, bulk)
        - Automatically map field names between internal (BSH) and Bloomberg format
        - Provide a unified interface for Bloomberg as part of the BSH data framework

    Args:
        host (str): Bloomberg host, default is "localhost".
        port (int): Bloomberg API port, default is 8194.
        show_progress (bool): Whether to display progress for large downloads.

    Example:
        >>> bb = BloombergProvider(host="localhost", port=8194)
        >>> res = bb.fetch_market_data(daily_reqs)
        >>> info = bb.fetch_info_data(reference_reqs)
        >>> bb.close()
    """

    SERVICE_NAME = "//blp/refdata"

    def __init__(self, host: str = "localhost", port: int = 8194, show_progress: bool = True):
        self.host = host
        self.port = port
        self.show_progress = show_progress
        #
        self.session = self._start_session()
        self.service = self._open_service(self.SERVICE_NAME)
        self.fetcher = BloombergFetcher(self.session, self.service, show_progress)

        logger.info("BloombergProvider initialized successfully")

    # ============================================================
    # MAIN ENTRY POINT
    # ============================================================

    def fetch_market_data(self, requests: List[BaseMarketRequest]) -> Dict:
        """
        Entry point unificato.
        Riceve sempre una lista di richieste omogenee (tutte daily / intraday / snapshot).
        """
        if not requests:
            logger.warning("Empty request list passed to BloombergProvider")
            return {}

        sample = requests[0]

        # === Caso Snapshot (con snapshot_time) ===
        if getattr(sample, "snapshot_time", None):
            logger.debug("Dispatching Bloomberg snapshot fetch")
            results = {}
            for req in requests:
                ps = self.fetcher.fetch_snapshot(req)
                results[req.instrument.id] = ps.get(req.instrument.id)
            return results

        # === Caso Daily ===
        elif "d" in str(sample.frequency).lower():
            logger.debug("Dispatching Bloomberg daily fetch")
            return self.fetcher.fetch_daily(
                requests,
                fields=sample.fields,
                start=min(r.start for r in requests),
                end=max(r.end for r in requests)
            )

        # === Caso Intraday ===
        else:
            logger.debug("Dispatching Bloomberg intraday fetch")
            results = {}
            for req in requests:
                ps = self.fetcher.fetch_intraday(req)
                results[req.instrument.id] = ps
            return results

    def fetch_info_data(self, requests: List[BaseStaticRequest]) -> None | dict | dict[Any, Any]:
        """
            Esegue richieste statiche Bloomberg (TER, YAS, DIVIDENDS o altri campi statici).
            Riconosce automaticamente se serve Reference o Historical.
            """
        if not requests:
            logger.warning("Empty static request list passed to BloombergProvider")
            return {}

        # === Preparazione batch ===
        instruments = {req.instrument.id: req.instrument for req in requests}
        instrument_list = list(instruments.keys())
        id_map = {req.subscription: req.instrument.id for req in requests if getattr(req, "subscription", None)}
        subscriptions = list(id_map.keys())
        if not instruments:
            return {}

        first_req = requests[0]
        fields = [f.upper() for f in
                  (first_req.fields if isinstance(first_req.fields, list) else [first_req.fields])]
        # === Determina tipo richiesta ===
        match first_req.request_type:
            # === Dispatch ===
            case "historical":
                return _rename_fields(self.fetcher.fetch_historical_data(
                    subscriptions, _get_bbg_field(fields), start=first_req.start or None,
                    end = first_req.end or None,
                    corr_ids=instrument_list
                ))

            case "reference":
                return _rename_fields(self.fetcher.fetch_reference_data(
                    subscriptions, _get_bbg_field(fields), corr_ids=instrument_list))

            case "bulk":
                return _rename_fields(self.fetcher.fetch_bulk_data(
                    subscriptions, _get_bbg_field(fields), corr_ids=instrument_list, start=first_req.start))




    # ============================================================
    # SESSION MANAGEMENT
    # ============================================================

    def _start_session(self):
        logger.debug(f"Starting Bloomberg session at {self.host}:{self.port}")
        opts = blpapi.SessionOptions()
        opts.setServerHost(self.host)
        opts.setServerPort(self.port)
        session = blpapi.Session(opts)
        if not session.start():
            raise ConnectionError("Failed to start Bloomberg session.")
        return session

    def _open_service(self, name: str):
        logger.debug(f"Opening Bloomberg service: {name}")
        if not self.session.openService(name):
            raise RuntimeError(f"Failed to open Bloomberg service: {name}")
        return self.session.getService(name)

    def close(self):
        if self.session and self.session.isStarted():
            logger.info("Closing Bloomberg session")
            self.session.stop()

def _get_bsh_field(names):
    if isinstance(names, str): names = [names]
    bbg_to_bsh = {k:v for v, k in BSH_TO_BBG.items()}
    return [bbg_to_bsh.get(name, name) for name in names]

def _get_bbg_field(names):
    if isinstance(names, str): names = [names]
    return [BSH_TO_BBG.get(name, name) for name in names]

def _rename_fields(res: dict) -> dict:
    """
    Rinomina i campi Bloomberg → BSH anche nel formato:
        {isin: {campo: valore_serie_o_singolo}}
    Esempio:
        {'IE00B4L5Y983': {'PX_LAST': {...}, 'CCY': 'EUR'}}
        → {'IE00B4L5Y983': {'NAV': {...}, 'NAV_CCY': 'EUR'}}
    """
    if not res:
        return res

    for isin, fields in res.items():
        if not isinstance(fields, dict):
            continue

        renamed = {}
        for key, value in fields.items():
            # Trova la chiave BSH corrispondente (se esiste)
            new_key = next((bsh for bsh, bbg in BSH_TO_BBG.items() if bbg == key), key)
            renamed[new_key] = value

        res[isin] = renamed

    return res
