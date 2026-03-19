from pathlib import Path
from typing import Optional, Dict, Any

from sfm_utilities.addin import get_oracle_username, get_oracle_password, get_timescale_username, get_timescale_password

from sfm_data_provider.core.utils.config_manager import ConfigManager, APIConfig, ClientConfig, OracleConfig, \
    TimescaleConfig


class AddinConfigManager(ConfigManager):
    DEFAULT_CONFIG_PATHS = []

    def __init__(self):
        """Private constructor - use load() class method."""
        super().__init__()
        self._yaml = None

    @classmethod
    def load(cls, config_path: Optional[str] = None) -> 'ConfigManager':
        """Load config from config_path."""
        if cls._instance is None:
            cls._instance = cls()

        # Resolve config path
        if config_path:
            raise ValueError("When using an AddinConfigManager, config_path must be None.")

        # Load if path changed or not loaded yet
        if not cls._config:
            cls._config = cls._instance._load_conf_file()
        return cls._instance

    @staticmethod
    def _load_conf_file() -> Dict[str, str]:
        return {
            "OracleUser": get_oracle_username(),
            "OraclePassword": get_oracle_password(),
            "TimescaleUser": get_timescale_username(),
            "TimescalePassword": get_timescale_password()
        }

    def get_api_config(self, **overrides: Any) -> APIConfig:
        return APIConfig(
            cache_path="C:/SFMAddIn/.cache/data_api"
        )

    def get_client_config(self, **overrides: Any) -> ClientConfig:
        return ClientConfig()

    def get_oracle_config(self, **overrides: Any) -> OracleConfig:
        return OracleConfig(
            user=self._config["OracleUser"],
            password=self._config["OraclePassword"],
            tns_name="ORABOH",
            schema="AF_DATAMART_DBA"
        )

    def get_timescale_config(self, **overrides: Any) -> TimescaleConfig:
        return TimescaleConfig(
            host="timescaledb.af.pro",
            db_name="aidb",
            user=self._config["TimescaleUser"],
            password=self._config["TimescalePassword"],
        )
