"""
timescale_provider.py — Unified provider for TimescaleDB market data.

This module defines the :class:`TimescaleProvider`, the unified access layer for
retrieving time-series and intraday data from TimescaleDB. It handles connection
initialization, environment-based configuration loading, and request dispatch
through the :class:`TSTimescaleFetcher`.

Responsibilities:
    - Load TimescaleDB connection parameters from environment or YAML
    - Initialize the :class:`QueryTimeScale` connection interface
    - Dispatch and group requests by frequency, instrument type, and currency
    - Delegate market data retrieval to :class:`TSTimescaleFetcher`
    - Provide basic connection health checks

Example:
    >>> provider = TimescaleProvider(config_path="config/db.yaml")
    >>> result = provider.fetch_market_data(requests)
    >>> provider.healthcheck()
"""
import logging
from collections import defaultdict
from typing import List, Union, Dict, Tuple, Optional

from core.enums.datasources import DataSource
from core.utils.common import load_yaml
from providers.timescale.fetchers.timescale_info_fetcher import TimescaleInfoFetcher
from providers.timescale.fetchers.timescale_market_fetcher import TimescaleMarketFetcher
from providers.timescale.query_timescale import QueryTimeScale
from sfm_dbconnections.DbConnectionParameters import DbConnectionParameters, TimescaleConnectionParameters

from core.enums.instrument_types import InstrumentType
from core.requests.requests import DailyRequest, BaseMarketRequest, BaseStaticRequest
from core.base_classes.base_provider import BaseProvider

logger = logging.getLogger(__name__)


class TimescaleProvider(BaseProvider):
    """
    Unified provider for TimescaleDB market data (daily and intraday).

    The TimescaleProvider centralizes all access to time-series data stored in
    TimescaleDB, grouping requests and routing them to the appropriate fetcher.
    It is designed to be compatible with the unified BSH data client and integrates
    seamlessly with the system’s market API layer.

    Responsibilities:
        - Load connection settings from the environment or YAML configuration
        - Initialize the :class:`QueryTimeScale` client
        - Dispatch market requests by instrument type and frequency
        - Route to the proper :class:`TSTimescaleFetcher` method
        - Handle both daily (EOD) and intraday series retrieval

    Args:
        config_path (str | None): Optional path to a YAML configuration file.
            If omitted, connection parameters are loaded from environment
            or singleton connection managers.

    Example:
        >>> provider = TimescaleProvider(config_path="config/db.yaml")
        >>> reqs = [DailyRequest(source="timescale", instrument=my_etf, fields=["MID"])]
        >>> data = provider.fetch_market_data(reqs)
        >>> print(data[my_etf.id].head())
    """

    def __init__(self, config_path: Optional[str] = None):
        try:
            # ===========================================================
            # 1️⃣ Caricamento configurazione
            # ===========================================================
            cfg = self._load_config(config_path)

            if not cfg:
                logger.warning("TimescaleProvider failed to load config")
                return

            # ===========================================================
            # 2️⃣ Inizializzazione connessione Timescale
            # ===========================================================
            self.query_ts = QueryTimeScale(**cfg)
            self.source = DataSource.TIMESCALE
            self.market_fetcher = TimescaleMarketFetcher(self.query_ts)
            self.info_fetcher = TimescaleInfoFetcher(self.query_ts)
            logger.info("✅ TimescaleProvider initialized successfully")

        except Exception as e:
            logger.exception(f"❌ Failed to initialize TimescaleProvider: {e}")


    # ===========================================================
    # CONFIGURAZIONE
    # ===========================================================
    def _load_config(self, config_path: Optional[str]) -> dict:
        try:
            return self._load_from_env_singleton()
        except Exception:
            logger.debug("DB singleton not instantiated, falling back to YAML/env config.")
            return load_yaml(config_path).get("timescale_connection", {})
        finally:
            return

    @staticmethod
    def _load_from_env_singleton() -> dict:
        """Carica parametri da variabili ambiente (singleton connection)."""
        params = DbConnectionParameters()
        cfg = {
            "port": params.get_timescale_parameter(TimescaleConnectionParameters.PORT),
            "host": params.get_timescale_parameter(TimescaleConnectionParameters.HOST),
            "db_name": params.get_timescale_parameter(TimescaleConnectionParameters.DB_NAME),
            "user": params.get_timescale_parameter(TimescaleConnectionParameters.USERNAME),
            "password": params.get_timescale_parameter(TimescaleConnectionParameters.PASSWORD),
        }

        missing = [k for k, v in cfg.items() if not v]
        if missing:
            raise ValueError(f"Missing Timescale parameters: {', '.join(missing)}")

        logger.debug("Loaded Timescale parameters from environment (singleton)")
        return cfg

    # ===========================================================
    # MARKET DATA FETCH
    # ===========================================================
    def fetch_market_data(self, requests: Union[BaseMarketRequest, List[BaseMarketRequest]]) -> Dict:
        """
        Entry point principale.
        Raggruppa le richieste per (daily/intraday, instrument_type, currency)
        e le instrada verso il metodo corretto di TSTimescaleFetcher.
        """
        if isinstance(requests, BaseMarketRequest):
            requests = [requests]

        if not requests:
            logger.warning("Empty Timescale request batch.")
            return {}

        grouped: Dict[Tuple[str, InstrumentType, str], List[BaseMarketRequest]] = defaultdict(list)
        for req in requests:
            freq_type = "daily" if isinstance(req, DailyRequest) else "intraday"
            inst_type = InstrumentType.from_str(req.instrument.type)
            currency = req.instrument.currency or "UNKNOWN"
            grouped[(freq_type, inst_type, currency)].append(req)

        all_results = {}
        for (freq_type, inst_type, currency), batch in grouped.items():
            n = len(batch)
            logger.debug(f"[{freq_type.upper()}] {inst_type.name} ({currency}) — {n} instruments")
            try:
                result = self.market_fetcher.fetch(batch)
                if result:
                    all_results.update(result.items())

            except Exception as e:
                logger.error(f"Error fetching {inst_type} {currency} ({freq_type}): {e}", exc_info=True)

        logger.info(f"✅ Completed Timescale fetch: {len(all_results)}/{len(requests)} instruments fetched")
        return all_results

    def fetch_info_data(self, request: BaseStaticRequest | List[BaseStaticRequest]):
        return self.info_fetcher.fetch(request)

    def healthcheck(self) -> bool:
        if not self.query_ts:
            return False
        rows, _ = self.query_ts.create_connection().execute_query("SELECT 1")
        return bool(rows and rows[0][0] == 1)
