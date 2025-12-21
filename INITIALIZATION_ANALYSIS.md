# Initialization and Configuration Analysis

## Current Initialization Flow

### 1. Entry Point: `BshData.__init__()`

```python
def __init__(self, config_path: str | None = CONFIG_PATH, ...):
    # Step 1: Load YAML config
    cfg = (load_yaml(config_path) or {}).get("api", {})
    
    # Step 2: Extract parameters (constructor args override config)
    log_level = log_level or cfg.get("log_level")
    log_file = log_file or cfg.get("log_file")
    # ... etc
    
    # Step 3: Initialize components
    self._setup_logging(...)
    self._setup_cache(...)
    self._setup_client(config_path, ...)  # Passes config_path down
```

**Flow:**
1. Loads YAML config file
2. Extracts `api` section
3. Sets up logging
4. Sets up cache
5. Initializes client (passes `config_path`)

### 2. Client Initialization: `BSHDataClient.__init__()`

```python
def __init__(self, config_path=None, show_progress=True):
    # Loads config AGAIN
    cfg = (load_yaml(config_path) or {}).get("client", {})
    
    # Initialize providers based on activate_* flags
    self.providers = {
        name: self._init_lazy_provider(key, factory, cfg)
        for name, (key, factory) in provider_specs.items()
    }
```

**Flow:**
1. Loads YAML config file **again** (duplicate read)
2. Extracts `client` section
3. Initializes providers lazily based on `activate_*` flags
4. Each provider receives `config_path` and loads config **again**

### 3. Provider Initialization

**OracleProvider:**
```python
def __init__(self, config_path: Optional[str] = "bshdata_config.yaml"):
    cfg = self._load_config(config_path)  # Loads config AGAIN
    
    # Try environment singleton first
    try:
        return self._load_from_env_singleton()
    except Exception:
        # Fallback to YAML
        return load_yaml(config_path).get("oracle_connection", {})
```

**TimescaleProvider:**
```python
def __init__(self, config_path: Optional[str] = None):
    cfg = self._load_config(config_path)  # Loads config AGAIN
    
    # Same pattern: singleton → YAML fallback
    try:
        return self._load_from_env_singleton()
    except Exception:
        return load_yaml(config_path).get("timescale_connection", {})
```

## Current Issues

### 1. **Multiple Config File Reads** ⚠️

**Problem:**
- Config file is loaded **3-4 times** during initialization:
  - Once in `BshData.__init__()`
  - Once in `BSHDataClient.__init__()`
  - Once per provider (Oracle, Timescale, etc.)

**Impact:**
- Unnecessary I/O operations
- No caching of config data
- Potential for inconsistent reads if file changes

**Example:**
```python
bsh = BshData(config_path="config.yaml")
# File is read:
# 1. In BshData.__init__() → extracts "api" section
# 2. In BSHDataClient.__init__() → extracts "client" section  
# 3. In OracleProvider.__init__() → extracts "oracle_connection" section
# 4. In TimescaleProvider.__init__() → extracts "timescale_connection" section
```

### 2. **Inconsistent Error Handling** ⚠️

**Problem:**
- `load_yaml()` returns `None` or empty dict on error (silent failure)
- Some providers check for empty config, some don't
- No validation of required fields

**Examples:**
```python
# In common.py - returns None on error
def load_yaml(config_path: str) -> dict:
    try:
        # ... load config
    except Exception as e:
        logger.exception(f"Failed loading...")
        # Returns None implicitly

# In BshData - uses or {} to handle None
cfg = (load_yaml(config_path) or {}).get("api", {})

# In TimescaleProvider - checks for empty
if not cfg:
    logger.warning("TimescaleProvider failed to load config")
    return  # Silent failure, provider partially initialized
```

### 3. **No Config Validation** ⚠️

**Problem:**
- No validation of required fields
- No type checking
- No schema validation
- Missing fields cause runtime errors later

**Example:**
```python
# OracleProvider expects these fields:
cfg["user"]  # KeyError if missing
cfg["password"]  # KeyError if missing
cfg["tns_name"]  # KeyError if missing

# No validation before use!
self.connection = OracleConnection(
    user=cfg["user"],  # Crashes here if missing
    password=cfg["password"],
    ...
)
```

### 4. **Inconsistent Config Path Handling** ⚠️

**Problem:**
- Default paths are inconsistent
- Some use `None`, some use hard-coded paths
- No resolution of relative paths

**Examples:**
```python
# BshData
CONFIG_PATH = "config/bshdata_config.yaml"  # Relative path

# OracleProvider
def __init__(self, config_path: Optional[str] = "bshdata_config.yaml"):  # Different default!

# TimescaleProvider
def __init__(self, config_path: Optional[str] = None):  # No default
```

### 5. **No Environment Variable Support** ⚠️

**Problem:**
- Only supports YAML files and singleton pattern
- No support for environment variables
- No 12-factor app compliance

**Current:**
```python
# Only two sources:
1. YAML file
2. Environment singleton (DbConnectionParameters)
```

**Missing:**
- Direct environment variable support
- Config precedence (env > YAML > defaults)
- No `.env` file support

### 6. **Hard-coded Defaults** ⚠️

**Problem:**
- Defaults scattered throughout code
- No single source of truth
- Hard to change defaults

**Examples:**
```python
# In BshData
log_level = log_level or cfg.get("log_level")  # No default, could be None

# In OracleProvider
secret_key=cfg.get("secret_key", "AreaFinanza"),  # Hard-coded default
is_encrypted=cfg.get("is_encrypted", True),  # Hard-coded default
```

### 7. **No Config Caching** ⚠️

**Problem:**
- Config is loaded fresh every time
- No caching even within same process
- Multiple instances load config multiple times

### 8. **Silent Failures** ⚠️

**Problem:**
- Config loading failures are often silent
- Providers can be partially initialized
- Errors only appear when trying to use the provider

**Example:**
```python
# TimescaleProvider
if not cfg:
    logger.warning("TimescaleProvider failed to load config")
    return  # Provider exists but is broken!
    
# Later, when used:
provider.fetch_market_data(...)  # AttributeError: 'NoneType' has no attribute 'query_ts'
```

## Improvement Recommendations

### 1. **Centralized Config Manager** ✅

Create a single config manager that:
- Loads config once
- Caches the result
- Provides typed access to sections
- Validates required fields

**Implementation:**
```python
class ConfigManager:
    _instance = None
    _config = None
    _config_path = None
    
    @classmethod
    def load(cls, config_path: str | None = None) -> 'ConfigManager':
        if cls._instance is None:
            cls._instance = cls()
        if config_path and cls._config_path != config_path:
            cls._config = cls._load_yaml(config_path)
            cls._config_path = config_path
        return cls._instance
    
    def get_api_config(self) -> dict:
        return self._config.get("api", {})
    
    def get_client_config(self) -> dict:
        return self._config.get("client", {})
    
    def get_oracle_config(self) -> dict:
        return self._config.get("oracle_connection", {})
    
    def get_timescale_config(self) -> dict:
        return self._config.get("timescale_connection", {})
```

### 2. **Config Validation** ✅

Add validation with clear error messages:

```python
from typing import TypedDict, Required

class OracleConfig(TypedDict):
    user: Required[str]
    password: Required[str]
    tns_name: Required[str]
    schema: str | None
    secret_key: str
    is_encrypted: bool

def validate_oracle_config(cfg: dict) -> OracleConfig:
    required = ["user", "password", "tns_name"]
    missing = [k for k in required if k not in cfg or not cfg[k]]
    if missing:
        raise ValueError(f"Missing required Oracle config: {missing}")
    return OracleConfig(**cfg)
```

### 3. **Environment Variable Support** ✅

Add support for environment variables with precedence:

```python
import os
from typing import Any

class ConfigManager:
    def _get_value(self, key: str, default: Any = None) -> Any:
        # Precedence: env > config > default
        env_key = f"BSH_{key.upper()}"
        if env_key in os.environ:
            return os.environ[env_key]
        return self._config.get(key, default)
    
    def get_oracle_user(self) -> str:
        return self._get_value("oracle_connection.user") or \
               os.getenv("BSH_ORACLE_USER")
```

### 4. **Unified Config Path Resolution** ✅

```python
from pathlib import Path

class ConfigManager:
    DEFAULT_CONFIG_PATHS = [
        "config/bshdata_config.yaml",
        "bshdata_config.yaml",
        Path.home() / ".bshdata" / "config.yaml",
    ]
    
    @classmethod
    def resolve_config_path(cls, config_path: str | None) -> Path | None:
        if config_path:
            path = Path(config_path)
            if path.is_absolute() or path.exists():
                return path.resolve()
        
        # Try defaults
        for default in cls.DEFAULT_CONFIG_PATHS:
            path = Path(default)
            if path.exists():
                return path.resolve()
        
        return None
```

### 5. **Better Error Handling** ✅

```python
class ConfigError(Exception):
    """Base exception for config errors"""
    pass

class ConfigNotFoundError(ConfigError):
    """Config file not found"""
    pass

class ConfigValidationError(ConfigError):
    """Config validation failed"""
    pass

def load_config(config_path: str) -> dict:
    path = Path(config_path)
    if not path.exists():
        raise ConfigNotFoundError(f"Config file not found: {config_path}")
    
    try:
        # Load and validate
        config = yaml.load(path)
        validate_config(config)
        return config
    except Exception as e:
        raise ConfigError(f"Failed to load config: {e}") from e
```

### 6. **Lazy Provider Initialization with Error Handling** ✅

```python
class BSHDataClient:
    def _init_lazy_provider(self, key: str, factory, cfg: dict):
        if not cfg.get(f"activate_{key}", True):
            return None
        
        try:
            return factory()
        except ConfigError as e:
            logger.error(f"Failed to initialize {key} provider: {e}")
            return None  # Graceful degradation
        except Exception as e:
            logger.exception(f"Unexpected error initializing {key} provider: {e}")
            raise
```

### 7. **Config Schema Definition** ✅

```python
from dataclasses import dataclass
from typing import Optional

@dataclass
class APIConfig:
    log_level: str = "INFO"
    log_file: Optional[str] = None
    log_level_file: str = "INFO"
    autocomplete: bool = True
    cache: bool = True
    cache_path: str = "cache"
    
    @classmethod
    def from_dict(cls, data: dict) -> 'APIConfig':
        return cls(**{k: v for k, v in data.items() if k in cls.__annotations__})

@dataclass
class OracleConfig:
    user: str
    password: str
    tns_name: str
    schema: Optional[str] = None
    environment: str = "PROD"
    secret_key: str = "AreaFinanza"
    is_encrypted: bool = True
```

### 8. **Config Hot Reload (Optional)** ✅

For development/testing:

```python
class ConfigManager:
    _last_modified: float = 0
    
    def reload_if_changed(self):
        if self._config_path:
            mtime = self._config_path.stat().st_mtime
            if mtime > self._last_modified:
                self._config = self._load_yaml(self._config_path)
                self._last_modified = mtime
                return True
        return False
```

## Proposed Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    BshData.__init__()                    │
│  ┌───────────────────────────────────────────────────┐  │
│  │         ConfigManager.load(config_path)             │  │
│  │  - Loads YAML once                                  │  │
│  │  - Caches result                                    │  │
│  │  - Validates schema                                 │  │
│  │  - Supports env vars                                │  │
│  └───────────────────────────────────────────────────┘  │
│                          │                                │
│                          ▼                                │
│  ┌───────────────────────────────────────────────────┐  │
│  │  Extract config sections:                          │  │
│  │  - api_config = config.get_api_config()           │  │
│  │  - client_config = config.get_client_config()     │  │
│  └───────────────────────────────────────────────────┘  │
│                          │                                │
│                          ▼                                │
│  Setup: logging, cache, client                            │
└─────────────────────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────┐
│              BSHDataClient.__init__()                    │
│  - Uses cached config (no reload)                        │
│  - Initializes providers with config sections           │
└─────────────────────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────┐
│            Provider.__init__()                           │
│  - Receives config section (not path)                   │
│  - Validates required fields                            │
│  - Falls back to env singleton if needed                │
└─────────────────────────────────────────────────────────┘
```

## Migration Path

1. **Phase 1: Add ConfigManager (non-breaking)**
   - Create `ConfigManager` class
   - Keep existing `load_yaml()` for backward compatibility
   - Use `ConfigManager` in new code

2. **Phase 2: Migrate Components**
   - Update `BshData` to use `ConfigManager`
   - Update `BSHDataClient` to use cached config
   - Update providers to receive config sections

3. **Phase 3: Add Validation**
   - Add TypedDict/dataclass schemas
   - Add validation functions
   - Update error handling

4. **Phase 4: Add Features**
   - Environment variable support
   - Config hot reload
   - Better error messages

## Benefits

✅ **Performance**: Config loaded once, cached
✅ **Reliability**: Validation prevents runtime errors
✅ **Maintainability**: Single source of truth
✅ **Flexibility**: Environment variable support
✅ **Debugging**: Clear error messages
✅ **Type Safety**: TypedDict/dataclass schemas

