import logging
import os
import sys
from client import BSHDataClient
from core.utils.common import load_yaml
from core.utils.memory_provider import enable_cache, set_cache_dir, disable_cache
from interface.api.general_data_api import GeneralDataAPI
from interface.api.info_data_api import InfoDataAPI
from interface.api.market_api import MarketDataAPI

CONFIG_PATH = "config/bshdata_config.yaml"


class BshData:
    """
    Unified facade for all data APIs: dynamic data (.market), static information (.info),
    and general utilities (.general).

    This class provides a single entry point for interacting with the BSH data layer.

    Responsibilities:
        - Centralized logging (initialized once per process)
        - Global cache management (can be enabled or disabled)
        - Initialization of the client and all submodules (market, info, general)
        - Optional autocomplete for missing data or metadata, resolved via Oracle

    Args:
        config_path (str | None): Path to the YAML configuration file.
            If not provided, the default ``CONFIG_PATH`` is used.
        cache (bool): Whether to enable global caching. Defaults to True.
        log_level (str | None): Logging level for console output.
        log_file (str | None): Path for the log file.
        log_level_file (str | None): Log level for file logging (may differ from console).
        autocomplete (bool | None): Enables automatic lookup of missing data or
            instrument metadata using Oracle. Defaults to the value in the config.
        **kwargs: Additional parameters passed to client initialization.

    Example:
        >>> bsh = BshData(config_path="config/db.yaml", cache=True)
        >>> etf_data = bsh.market.get_daily_etf("IE00B4L5Y983", "2024-01-01", "2024-02-01")

    """

    # ============================================================A
    # INIT
    # ============================================================
    def __init__(self,
                 config_path: str | None = CONFIG_PATH,
                 cache=True,
                 log_level=None,
                 log_file=None,
                 log_level_file: str = None,
                 autocomplete=None,
                 warmup=None,
                 **kwargs) -> None:

        cfg = (load_yaml(config_path) or {}).get("api", {})
        log_level = log_level or cfg.get("log_level")
        log_file = log_file or cfg.get("log_file")
        log_level_file = log_level_file or cfg.get("log_level_file")
        autocomplete = autocomplete or cfg.get("autocomplete")
        warmup = warmup or cfg.get("warmup")
        cache = cache or cfg.get("cache")
        cache_path = cfg.get("cache_path")

        self._setup_logging(log_level, log_file=log_file, log_level_file=log_level_file)
        self._setup_cache(cache, cache_path)
        self._setup_client(config_path, autocomplete, warmup, **kwargs)

    # ============================================================
    # CACHE
    # ============================================================
    def _setup_cache(self, enabled: bool, cache_path) -> None:
        """Abilita o disabilita la cache globale."""
        if enabled:
            enable_cache()
            set_cache_dir(cache_path)
            self.logger.debug("Cache abilitata. path: {}".format(cache_path))
        else:
            disable_cache()
            self.logger.debug("Cache disabilitata.")

    @staticmethod
    def enable_cache() -> None:
        enable_cache()

    @staticmethod
    def disable_cache() -> None:
        disable_cache()

    # ============================================================
    # CLIENT E API
    # ============================================================
    def _setup_client(self, config_path: str | None, autocomplete, warmup, **kwargs) -> None:
        """Crea il client dati e inizializza le API."""
        self.client = BSHDataClient(config_path=config_path)
        self.market = MarketDataAPI(self.client, autocomplete=autocomplete)
        self.info = InfoDataAPI(self.client, autocomplete=autocomplete)
        self.general = GeneralDataAPI(self.client, autocomplete=autocomplete)
        self.logger.info("BshData inizializzata con successo.")

    # ============================================================
    # LOGGING
    # ============================================================

    def _setup_logging(self, log_level: str, log_file: str | None = None, log_level_file: str | None = None) -> None:
        """Configura logging globale su console e (opzionale) su file."""

        root_logger = logging.getLogger()

        if not root_logger.handlers:
            # Formatter unico
            formatter = logging.Formatter(
                "%(asctime)s | %(processName)s | %(levelname)-8s | %(name)s | %(message)s",
                datefmt="%H:%M:%S",
            )

            # Stream handler (console)
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setFormatter(formatter)
            root_logger.addHandler(console_handler)

            # File handler (solo se richiesto)
            if log_file:
                os.makedirs(os.path.dirname(log_file), exist_ok=True)
                file_handler = logging.FileHandler(log_file, mode="a", encoding="utf-8")
                file_handler.setFormatter(formatter)
                file_handler.setLevel(log_level_file)
                root_logger.addHandler(file_handler)


            # Livello globale
            root_logger.setLevel(getattr(logging, log_level.upper(), logging.INFO))

        # Logger locale
        self.logger = logging.getLogger(__name__)

        # Silenzia librerie rumorose
        for lib in ["urllib3", "requests", "sqlalchemy", "blpapi", "pandas", "numexpr", "asyncio"]:
            logging.getLogger(lib).setLevel(logging.WARNING)

        self.logger.info("Logging inizializzato.")

    def set_log_level(self, log_level: str) -> None:
        """Cambia il livello di log a runtime."""
        level = getattr(logging, log_level.upper(), None)
        if level is None:
            raise ValueError(f"Livello log non valido: {log_level}")
        self.logger.setLevel(level)
        self.logger.info(f"Livello log impostato a {log_level}.")


