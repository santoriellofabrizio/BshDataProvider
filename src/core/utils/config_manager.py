"""
Improved Configuration Manager for BshDataProvider.

This module provides a centralized, cached, and validated configuration system
that addresses the issues with the current multi-read, unvalidated approach.
"""

import logging
import os
from pathlib import Path
from typing import Optional, Dict, Any
from dataclasses import dataclass, field
from functools import lru_cache

from dotenv import find_dotenv
from ruamel.yaml import YAML

logger = logging.getLogger(__name__)


# ============================================================================
# CONFIG SCHEMAS
# ============================================================================

@dataclass
class APIConfig:
    """API-level configuration."""
    log_level: str = "INFO"
    log_file: Optional[str] = None
    log_level_file: str = "INFO"
    autocomplete: bool = True
    cache: bool = True
    cache_path: str = "cache"
    warmup: bool = False

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'APIConfig':
        """Create from dictionary with defaults."""
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})


@dataclass
class ClientConfig:
    """Client-level configuration."""
    activate_oracle: bool = True
    activate_timescale: bool = True
    activate_bloomberg: bool = False
    activate_mock: bool = False

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ClientConfig':
        """Create from dictionary with defaults."""
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})


@dataclass
class OracleConfig:
    """Oracle connection configuration."""
    user: str = ""
    password: str = ""
    tns_name: str = ""
    schema: Optional[str] = None
    environment: str = "PROD"
    secret_key: str = "AreaFinanza"
    is_encrypted: bool = True
    config_dir: Optional[str] = None

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'OracleConfig':
        """Create from dictionary with defaults."""
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})

    def validate(self) -> None:
        """Validate required fields."""
        required = ["user", "password", "tns_name"]
        missing = [k for k in required if not getattr(self, k)]
        if missing:
            raise ValueError(f"Missing required Oracle config fields: {missing}")


@dataclass
class TimescaleConfig:
    """TimescaleDB connection configuration."""
    host: str = ""
    port: int = 5432
    db_name: str = ""
    user: str = ""
    password: str = ""

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'TimescaleConfig':
        """Create from dictionary with defaults."""
        # Handle port as string or int
        if "port" in data and isinstance(data["port"], str):
            data["port"] = int(data["port"])
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})

    def validate(self) -> None:
        """Validate required fields."""
        required = ["host", "db_name", "user", "password"]
        missing = [k for k in required if not getattr(self, k)]
        if missing:
            raise ValueError(f"Missing required Timescale config fields: {missing}")


# ============================================================================
# CONFIG MANAGER
# ============================================================================

class ConfigManager:
    """
    Centralized configuration manager with caching and validation.
    
    Features:
    - Loads config file once and caches result
    - Supports environment variables (BSH_* prefix)
    - Supports .env files (if python-dotenv is installed)
    - Validates required fields
    - Provides typed access to config sections
    - Handles missing config files gracefully
    
    Configuration precedence (highest to lowest):
    1. Constructor arguments (explicit overrides)
    2. Environment variables (BSH_* prefix)
    3. .env file (if python-dotenv installed)
    4. YAML config file
    5. Dataclass defaults
    """
    
    _instance: Optional['ConfigManager'] = None
    _config: Dict[str, Any] = {}
    _config_path: Optional[Path] = None
    _last_modified: float = 0
    
    DEFAULT_CONFIG_PATHS = [
        Path("config/bshdata_config.yaml"),
        Path("bshdata_config.yaml"),
        Path.home() / ".bshdata" / "config.yaml",
    ]
    
    def __init__(self):
        """Private constructor - use load() class method."""
        self._yaml = YAML(typ="safe")
        self._load_dotenv()
    
    @staticmethod
    def _load_dotenv() -> None:
        """Load .env file if it exists and python-dotenv is available."""
        try:
            from dotenv import load_dotenv
            # Try to load .env from common locations
            env_paths = [
                Path.cwd() / Path(".env"),
                Path.cwd() / Path("config/.env"),
                Path.cwd() / ".bshdata" / ".env",
                Path.cwd().parent / Path(".env"),
                Path.cwd().parent.parent / Path(".env"),
                Path.cwd().parent.parent / Path(".env"),
            ]
            for env_path in env_paths:
                if env_path.exists():
                    load_dotenv(env_path, override=False)  # Don't override existing env vars
                    logger.debug(f"Loaded .env file from {env_path}")
                    return
        except ImportError:
            # python-dotenv not installed, skip .env loading
            pass
        except Exception as e:
            logger.debug(f"Failed to load .env file: {e}")
    
    @classmethod
    def load(cls, config_path: Optional[str] = None) -> 'ConfigManager':
        """
        Load configuration from file.
        
        Args:
            config_path: Path to config file. If None, tries default paths.
            
        Returns:
            ConfigManager instance (singleton)
        """
        if cls._instance is None:
            cls._instance = cls()
        
        # Resolve config path
        resolved_path = cls._resolve_config_path(config_path)
        
        # Load if path changed or not loaded yet
        if resolved_path and (resolved_path != cls._config_path or not cls._config):
            cls._config = cls._instance._load_yaml(resolved_path)
            cls._config_path = resolved_path
            cls._last_modified = resolved_path.stat().st_mtime if resolved_path.exists() else 0
        
        return cls._instance
    
    @classmethod
    def _resolve_config_path(cls, config_path: Optional[str]) -> Optional[Path]:
        """Resolve config file path with fallback to defaults."""
        if config_path:
            path = Path(config_path)
            if path.is_absolute():
                return path if path.exists() else None
            # Try relative to current working directory
            if path.exists():
                return path.resolve()
        
        # Try default paths
        for default_path in cls.DEFAULT_CONFIG_PATHS:
            if default_path.exists():
                return default_path.resolve()
        
        return None
    
    def _load_yaml(self, config_path: Path) -> Dict[str, Any]:
        """Load YAML configuration file."""
        try:
            with open(config_path, "r", encoding="utf-8") as f:
                config = self._yaml.load(f)
            
            if not config or not isinstance(config, dict):
                logger.warning(f"Config file {config_path} is empty or invalid")
                return {}
            
            logger.debug(f"Loaded config from {config_path}")
            return config
            
        except Exception as e:
            logger.exception(f"Failed to load config from {config_path}: {e}")
            return {}
    
    def _get_env_value(self, key: str, default: Any = None) -> Any:
        """Get value from environment variable with BSH_ prefix."""
        env_key = f"BSH_{key.upper().replace('.', '_')}"
        value = os.getenv(env_key)
        if value is not None:
            logger.debug(f"Using environment variable {env_key}")
            return value
        return default
    
    def _get_nested(self, data: Dict[str, Any], key: str, default: Any = None) -> Any:
        """Get nested value from dict using dot notation."""
        keys = key.split(".")
        value = data
        for k in keys:
            if isinstance(value, dict):
                value = value.get(k)
                if value is None:
                    return default
            else:
                return default
        return value
    
    # ========================================================================
    # CONFIG SECTION ACCESSORS
    # ========================================================================
    
    def get_api_config(self, **overrides: Any) -> APIConfig:
        """Get API configuration with optional overrides."""
        data = self._config.get("api", {})
        
        # Apply environment variable overrides
        for key in APIConfig.__dataclass_fields__:
            env_value = self._get_env_value(f"api.{key}")
            if env_value is not None:
                # Type conversion
                field_type = APIConfig.__dataclass_fields__[key].type
                if field_type == bool:
                    data[key] = env_value.lower() in ("true", "1", "yes")
                elif field_type == Optional[str] or field_type == str:
                    data[key] = str(env_value)
                else:
                    data[key] = env_value
        
        # Apply constructor overrides
        data.update(overrides)
        
        return APIConfig.from_dict(data)
    
    def get_client_config(self, **overrides: Any) -> ClientConfig:
        """Get client configuration with optional overrides."""
        data = self._config.get("client", {})
        
        # Apply environment variable overrides
        for key in ClientConfig.__dataclass_fields__:
            env_value = self._get_env_value(f"client.{key}")
            if env_value is not None:
                data[key] = env_value.lower() in ("true", "1", "yes")
        
        # Apply constructor overrides
        data.update(overrides)
        
        return ClientConfig.from_dict(data)
    
    def get_oracle_config(self, **overrides: Any) -> OracleConfig:
        """Get Oracle configuration with optional overrides."""
        data = self._config.get("oracle_connection", {})
        
        # Apply environment variable overrides
        for key in OracleConfig.__dataclass_fields__:
            env_value = self._get_env_value(f"oracle_connection.{key}")
            if env_value is not None:
                data[key] = env_value
        
        # Apply constructor overrides
        data.update(overrides)
        
        config = OracleConfig.from_dict(data)
        
        # Validate if all required fields are present
        if config.user or config.password or config.tns_name:
            config.validate()
        
        return config
    
    def get_timescale_config(self, **overrides: Any) -> TimescaleConfig:
        """Get TimescaleDB configuration with optional overrides."""
        data = self._config.get("timescale_connection", {})
        
        # Apply environment variable overrides
        for key in TimescaleConfig.__dataclass_fields__:
            env_value = self._get_env_value(f"timescale_connection.{key}")
            if env_value is not None:
                if key == "port":
                    data[key] = int(env_value)
                else:
                    data[key] = env_value
        
        # Apply constructor overrides
        data.update(overrides)
        
        config = TimescaleConfig.from_dict(data)
        
        # Validate if all required fields are present
        if config.host or config.db_name or config.user:
            config.validate()
        
        return config
    
    def get_raw_config(self) -> Dict[str, Any]:
        """Get raw configuration dictionary."""
        return self._config.copy()
    
    def reload_if_changed(self) -> bool:
        """Reload config if file has been modified (for development)."""
        if self._config_path and self._config_path.exists():
            mtime = self._config_path.stat().st_mtime
            if mtime > self._last_modified:
                self._config = self._load_yaml(self._config_path)
                self._last_modified = mtime
                logger.info(f"Config reloaded from {self._config_path}")
                return True
        return False
    
    @classmethod
    def reset(cls) -> None:
        """Reset singleton (useful for testing)."""
        cls._instance = None
        cls._config = {}
        cls._config_path = None
        cls._last_modified = 0


# ============================================================================
# BACKWARD COMPATIBILITY
# ============================================================================

def load_yaml(config_path: Optional[str]) -> Dict[str, Any]:
    """
    Backward-compatible function that uses ConfigManager.
    
    This maintains compatibility with existing code while using
    the improved config system under the hood.
    """
    manager = ConfigManager.load(config_path)
    return manager.get_raw_config()

