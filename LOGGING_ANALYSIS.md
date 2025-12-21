# Logging Analysis - BshDataProvider Project

## Overview

The BshDataProvider project uses Python's standard `logging` module with a centralized configuration approach. Logging is initialized once per process through the main `BshData` facade class and is used consistently across all modules.

## Architecture

### 1. Centralized Logging Setup

**Location:** `src/interface/bshdata.py`

The logging system is initialized in the `BshData` class constructor via the `_setup_logging()` method:

```python
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
```

**Key Features:**
- **Singleton Pattern**: Checks `if not root_logger.handlers` to ensure logging is only configured once per process
- **Dual Output**: Supports both console (stdout) and file logging
- **Separate Log Levels**: Console and file can have different log levels
- **Noise Reduction**: Automatically silences verbose third-party libraries
- **Unified Formatter**: Single formatter used for both handlers

### 2. Configuration

**Location:** `config/bshdata_config.yaml`

Logging configuration is defined in the YAML config file under the `api` section:

```yaml
api:
  log_level: INFO
  log_file: logs/bshapi.log
  log_level_file: INFO
  autocomplete: True
  cache: True
  cache_path: cache
```

**Configuration Parameters:**
- `log_level`: Console log level (default: INFO)
- `log_file`: Path to log file (optional, creates directory if needed)
- `log_level_file`: File log level (can differ from console)

**Initialization Priority:**
1. Constructor parameters (highest priority)
2. YAML config file
3. Default values

### 3. Log Format

**Format String:**
```
%(asctime)s | %(processName)s | %(levelname)-8s | %(name)s | %(message)s
```

**Example Output:**
```
14:11:03 | MainProcess | INFO     | interface.bshdata | Logging inizializzato.
14:11:05 | MainProcess | INFO     | providers.oracle.provider | ✅ OracleConnection established successfully
14:21:08 | MainProcess | ERROR    | providers.oracle.provider | ❌ Failed to initialize OracleProvider: ...
```

**Components:**
- `asctime`: Time in `HH:MM:SS` format
- `processName`: Process name (typically "MainProcess")
- `levelname`: Log level (INFO, DEBUG, WARNING, ERROR, etc.) - 8 chars wide
- `name`: Logger name (module path)
- `message`: Actual log message

## Usage Patterns

### 1. Module-Level Loggers

Most modules create a logger at the module level using `__name__`:

```python
import logging

logger = logging.getLogger(__name__)

# Usage
logger.info("Message")
logger.debug("Debug message")
logger.warning("Warning message")
logger.error("Error message")
logger.exception("Exception occurred")  # Includes traceback
```

**Examples:**
- `src/providers/oracle/provider.py`: `logger = logging.getLogger(__name__)`
- `src/providers/timescale/provider.py`: `logger = logging.getLogger(__name__)`
- `src/core/utils/common.py`: `logger = logging.getLogger(__name__)`
- `src/analytics/adjustments/*.py`: All use `logger = logging.getLogger(__name__)`

### 2. Class-Level Loggers

Some classes create loggers using the class name:

```python
class BaseAPI:
    def __init__(self, ...):
        self.logger = logging.getLogger(self.__class__.__name__)
        
    def log_request(self, msg: str):
        self.logger.debug(f"[{self.__class__.__name__}] {msg}")
```

**Location:** `src/interface/api/base_api.py`

### 3. Direct Root Logger Usage

In some cases, the root logger is used directly:

```python
import logging

# In src/client.py
logging.exception(f"Error from {provider.__class__.__name__}: {e}")
logging.error(f"Failed to initialize {provider_cls.__name__}: {e}", exc_info=True)
```

## Log Levels Used

Based on codebase analysis, the following log levels are used:

1. **DEBUG**: Detailed diagnostic information
   - Cache operations (hits/misses)
   - Configuration loading details
   - Low-level operations

2. **INFO**: General informational messages
   - Initialization success messages
   - Cache status changes
   - Provider initialization
   - Request processing

3. **WARNING**: Warning messages
   - Empty request lists
   - Unsupported operations
   - Cache errors (non-fatal)

4. **ERROR**: Error messages
   - Provider initialization failures
   - Database connection errors
   - Critical operation failures

5. **EXCEPTION**: Exception logging (includes traceback)
   - Used with `logger.exception()` to automatically include stack trace
   - Provider initialization failures
   - Database query failures

## Logging by Component

### Providers

**Oracle Provider** (`src/providers/oracle/provider.py`):
- ✅ Success: `logger.info("✅ OracleConnection established successfully")`
- ❌ Failure: `logger.exception(f"❌ Failed to initialize OracleProvider: {e}")`
- Debug: Configuration loading details
- Warning: Empty request lists, unsupported categories

**Timescale Provider** (`src/providers/timescale/provider.py`):
- ✅ Success: `logger.info("✅ TimescaleProvider initialized successfully")`
- ❌ Failure: `logger.exception(f"❌ Failed to initialize TimescaleProvider: {e}")`
- Debug: Configuration fallback messages

**Bloomberg Provider** (`src/providers/bloomberg/bloomberg.py`):
- Uses standard logger pattern
- Logs session initialization and service opening

### Cache System

**Memory Provider** (`src/core/utils/memory_provider.py`):
- Cache enable/disable: `logger.info("Cache globally enabled")`
- Cache directory: `logger.info(f"Cache directory set to: {_cache_dir}")`
- Cache hits/misses: `logger.info(f"[DISK HIT] {func.__qualname__}")`
- Cache errors: `logger.warning(f"[CACHE ERROR] {func.__qualname__}: {e}")`
- RAM cache operations: `logger.debug(f"[RAM HIT] {func.__qualname__}")`

### Client

**BSHDataClient** (`src/client.py`):
- Exception logging: `logging.exception(f"Error from {provider.__class__.__name__}: {e}")`
- Provider initialization errors: `logging.error(f"Failed to initialize {provider_cls.__name__}: {e}", exc_info=True)`

### API Layer

**BaseAPI** (`src/interface/api/base_api.py`):
- Request logging: `self.logger.debug(f"[{self.__class__.__name__}] {msg}")`

## Third-Party Library Suppression

The logging system automatically suppresses verbose output from third-party libraries:

```python
for lib in ["urllib3", "requests", "sqlalchemy", "blpapi", "pandas", "numexpr", "asyncio"]:
    logging.getLogger(lib).setLevel(logging.WARNING)
```

This ensures only WARNING and above messages from these libraries are shown, reducing noise in logs.

## Runtime Log Level Changes

The `BshData` class provides a method to change log levels at runtime:

```python
def set_log_level(self, log_level: str) -> None:
    """Cambia il livello di log a runtime."""
    level = getattr(logging, log_level.upper(), None)
    if level is None:
        raise ValueError(f"Livello log non valido: {log_level}")
    self.logger.setLevel(level)
    self.logger.info(f"Livello log impostato a {log_level}.")
```

## Log File Management

- **Location**: `logs/bshapi.log` (configurable)
- **Mode**: Append (`mode="a"`) - logs are appended, not overwritten
- **Encoding**: UTF-8
- **Directory Creation**: Automatically creates directory if it doesn't exist
- **Separate Level**: File can have different log level than console

## Statistics

Based on codebase analysis:
- **31 files** use logging
- **162 log statements** across the codebase
- **11 modules** create module-level loggers
- **Most common**: `logger.info()` and `logger.exception()`

## Best Practices Observed

1. ✅ **Consistent Naming**: All modules use `logger = logging.getLogger(__name__)`
2. ✅ **Centralized Configuration**: Single point of configuration in `BshData`
3. ✅ **Noise Reduction**: Third-party libraries are silenced
4. ✅ **Exception Logging**: Uses `logger.exception()` for automatic traceback
5. ✅ **Emoji Indicators**: Uses ✅/❌ for quick visual scanning of success/failure
6. ✅ **Structured Messages**: Clear, descriptive log messages
7. ✅ **Dual Output**: Console for development, file for production

## Potential Improvements

1. **Structured Logging**: Consider using structured logging (JSON format) for better parsing
2. **Log Rotation**: No log rotation configured - could fill disk over time
3. **Contextual Logging**: Could add request IDs or correlation IDs for tracing
4. **Performance Logging**: Could add timing information for operations
5. **Log Aggregation**: No centralized log aggregation system mentioned

## Example Log Output

```
14:11:03 | MainProcess | INFO     | interface.bshdata | Logging inizializzato.
14:11:03 | MainProcess | INFO     | core.utils.memory_provider | Cache globally enabled
14:11:03 | MainProcess | INFO     | core.utils.memory_provider | Cache directory set to: C:\AFMachineLearning\Libraries\BshDataProvider\cache
14:11:03 | MainProcess | INFO     | providers.timescale.provider | ✅ TimescaleProvider initialized successfully
14:11:05 | MainProcess | INFO     | providers.oracle.provider | ✅ OracleConnection established successfully
14:11:05 | MainProcess | INFO     | interface.bshdata | BshData inizializzata con successo.
14:11:05 | MainProcess | INFO     | core.utils.memory_provider | [DISK HIT] QueryOracle.get_etps_data
14:11:05 | MainProcess | INFO     | core.utils.memory_provider | [DISK MISS] QueryOracle.get_etf_fx
```

## Summary

The logging system in BshDataProvider is:
- **Well-structured**: Centralized configuration with consistent usage patterns
- **Flexible**: Supports both console and file output with separate levels
- **Production-ready**: Handles errors gracefully, suppresses noise, and provides clear messages
- **Developer-friendly**: Easy to configure via YAML or constructor parameters
- **Comprehensive**: Used throughout the codebase for debugging, monitoring, and error tracking

