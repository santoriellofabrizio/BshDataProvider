"""
oracle_provider.py — Unified provider for Oracle-based static and semi-static data.

This module defines the :class:`OracleProvider`, which serves as the unique interface
between the BSH data framework and Oracle databases. It manages authentication,
connection setup, and query execution through :class:`QueryOracle` and
:class:`OracleInfoFetcher`.

Responsibilities:
    - Establish and manage a secure Oracle connection
    - Load connection parameters from environment, singleton, or YAML config
    - Handle Reference, Bulk, Historical, and General static requests
    - Integrate with the OracleInfoFetcher for higher-level data retrieval
    - Provide direct query access for debugging or validation

Example:
    >>> provider = OracleProvider(config_path="config/db.yaml")
    >>> data = provider.fetch_info_data(requests)
    >>> provider.healthcheck()
"""
import logging
from collections import defaultdict
from typing import Union, List, Dict, Optional

from core.base_classes.base_provider import BaseProvider


from core.requests.requests import BaseStaticRequest, BaseMarketRequest
from core.utils.common import load_yaml
from providers.oracle.fetchers.oracle_info_fetcher import OracleInfoFetcher
from providers.oracle.query_oracle import QueryOracle
from sfm_dbconnections.DbConnectionParameters import DbConnectionParameters, OracleConnectionParameters
from sfm_dbconnections.OracleConnection import OracleConnection

logger = logging.getLogger(__name__)


class OracleProvider(BaseProvider):
    """
    Unified provider for Oracle static and semi-static data.

    This provider centralizes access to Oracle-based datasets (TER, NAV, PCF, FX, etc.)
    and delegates data retrieval to the :class:`OracleInfoFetcher`, which groups
    queries into four logical categories:
        - ``fetch_reference()``: for static metadata (ISIN, ticker, TER, etc.)
        - ``fetch_historical()``: for time-dependent values (NAV, dividends, etc.)
        - ``fetch_bulk()``: for batch composition data (PCF, FX composition, etc.)
        - ``fetch_general()``: for general-purpose or schema-wide queries

    Responsibilities:
        - Load Oracle credentials from environment or YAML configuration
        - Create and manage an active :class:`OracleConnection`
        - Instantiate :class:`QueryOracle` and :class:`OracleInfoFetcher`
        - Route requests to the proper Oracle fetcher category
        - Expose diagnostic helpers (raw SQL execution and health check)

    Args:
        config_path (str | None): Optional path to a YAML configuration file
            containing Oracle connection details. If omitted, credentials are
            resolved from environment variables or singleton parameters.

    Example:
        >>> oracle = OracleProvider(config_path="config/db.yaml")
        >>> reqs = [ReferenceRequest(source="oracle", isin="IE00B4L5Y983", fields=["TER"])]
        >>> result = oracle.fetch_info_data(reqs)
        >>> print(result["IE00B4L5Y983"]["TER"])
    """

    # ===========================================================
    # INIT / CONFIGURAZIONE
    # ===========================================================
    def __init__(self, config_path: Optional[str] = "bshdata_config.yaml"):
        self.connection: OracleConnection | None = None
        try:
            cfg = self._load_config(config_path)

            # Connessione Oracle
            self.connection = OracleConnection(
                user=cfg["user"],
                password=cfg["password"],
                tns_name=cfg["tns_name"],
                schema=cfg.get("schema"),
                secret_key=cfg.get("secret_key", "AreaFinanza"),
                is_encrypted=cfg.get("is_encrypted", True),
                config_dir=cfg.get("config_dir"),
            )
            self.connection.connect()
            logger.info("✅ OracleConnection established successfully")

            # Query manager + fetcher
            self.query = QueryOracle(self.connection)
            self.fetcher = OracleInfoFetcher(self.query)

        except Exception as e:
            logger.exception(f"❌ Failed to initialize OracleProvider: {e}")
            raise

    # ===========================================================
    # CONFIG LOADER
    # ===========================================================
    def _load_config(self, config_path: Optional[str]) -> dict:
        try:
            return self._load_from_env_singleton()
        except Exception:
            logger.debug("Oracle singleton not instantiated, falling back to YAML/env config.")

        return load_yaml(config_path).get("oracle_connection", {})



    @staticmethod
    def _load_from_env_singleton() -> dict:
        params = DbConnectionParameters()
        cfg = {
            "user": params.get_oracle_parameter(OracleConnectionParameters.USERNAME),
            "password": params.get_oracle_parameter(OracleConnectionParameters.PASSWORD),
            "tns_name": params.get_oracle_parameter(OracleConnectionParameters.TNS_NAME),
            "schema": params.get_oracle_parameter(OracleConnectionParameters.SCHEMA),
        }
        missing = [k for k, v in cfg.items() if not v]
        if missing:
            raise ValueError(f"Missing Oracle parameters: {', '.join(missing)}")
        logger.debug("Loaded Oracle parameters from environment (singleton)")
        return cfg

    # ===========================================================
    # STATIC DATA (COMPATIBILE CON ORACLEINFOFETCHER)
    # ===========================================================
    def fetch_info_data(self, requests: Union[BaseStaticRequest, List[BaseStaticRequest]]) -> Dict:
        """Smista le richieste statiche alle 4 categorie del fetcher."""
        if isinstance(requests, BaseStaticRequest):
            requests = [requests]
        if not requests:
            logger.warning("Empty Oracle static request list.")
            return {}

        # 🔹 Raggruppa le richieste per categoria in base ai campi richiesti
        grouped: Dict[str, List[BaseStaticRequest]] = defaultdict(list)

        for req in requests:
            # se un campo è NAV o DIVIDEND → historical
            # se PCF_COMPOSITION o FX_COMPOSITION → bulk
            # se ETP_ISINS o FUTURES_DATA → general
            grouped[req.request_type].append(req)

        #  Esegue il fetch per ogni categoria
        results = {}
        for category, group in grouped.items():
            try:
                match category:
                    case "reference":
                        out = self.fetcher.fetch_reference(group)
                    case "historical":
                        out = self.fetcher.fetch_historical(group)
                    case "bulk":
                        out = self.fetcher.fetch_bulk(group)
                    case "general":
                        out = self.fetcher.fetch_general(group)
                    case _:
                        logger.warning(f"Unsupported Oracle fetch category: {category}")
                        continue

                if isinstance(out, dict):
                    results.update(out)
            except Exception as e:
                logger.exception(f"Oracle static fetch failed for category '{category}': {e}")

        return results

    # ===========================================================
    # MARKET DATA (NAV, DIVIDENDS, ETC.)
    # ===========================================================
    def fetch_market_data(self, requests: Union[BaseMarketRequest, List[BaseMarketRequest]]) -> Dict:
        """
        Le serie storiche Oracle (NAV, dividendi, carry, ecc.) sono
        coperte da OracleInfoFetcher.fetch_historical().
        """
        raise NotImplementedError

    # ===========================================================
    # RAW QUERY ACCESS
    # ===========================================================
    def get_raw_query(self, query: str, params: Optional[dict] = None):
        """Accesso diretto a Oracle per debug/testing."""
        return self.connection.execute_query(query, params)

    def healthcheck(self) -> bool:
        if not self.connection:
            return False
        rows, _ = self.connection.execute_query("SELECT 1 FROM DUAL")
        return bool(rows and rows[0][0] == 1)