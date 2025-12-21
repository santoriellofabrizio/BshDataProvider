# Logging Behavior When Using BshDataProvider in External Projects

## Overview

When you use BshDataProvider as a library in another project, the logging system has specific behaviors that you should understand to avoid conflicts or unexpected behavior.

## Key Behavior: Conditional Handler Setup

The critical line in `_setup_logging()` is:

```python
if not root_logger.handlers:
    # Only sets up handlers if none exist
```

This means **BshDataProvider will only configure logging handlers if your project doesn't already have logging configured**.

## Scenarios

### Scenario 1: Your Project Has NO Logging Configured

**What Happens:**
- ✅ BshDataProvider will set up logging handlers
- ✅ Adds console handler (stdout) with its custom format
- ✅ Adds file handler (if `log_file` is specified)
- ✅ Sets root logger level (default: INFO)
- ✅ Silences third-party libraries (urllib3, requests, sqlalchemy, etc.)

**Example:**
```python
# Your project (no logging setup)
from bshDataProvider import BshData

# This will configure logging for the entire application
bsh = BshData(config_path="config.yaml")
# Now ALL logging in your app uses BshDataProvider's format
```

**Result:**
- Your entire application will use BshDataProvider's log format
- All log messages from your code and BshDataProvider will appear in the same format
- Log file will be created if specified

### Scenario 2: Your Project ALREADY Has Logging Configured

**What Happens:**
- ❌ BshDataProvider will **NOT** add its handlers
- ⚠️ **BUT** it will still:
  - Set the root logger level (line 133)
  - Silence third-party libraries (lines 139-140)

**Example:**
```python
# Your project (with logging setup)
import logging

# Your logging configuration
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler('myapp.log')]
)

from bshDataProvider import BshData

# BshDataProvider sees handlers exist, so it won't add its own
bsh = BshData(config_path="config.yaml")
# BUT it will still change root logger level and silence libraries!
```

**Result:**
- Your existing handlers remain unchanged
- Your log format is preserved
- **However**, the root logger level may be changed to INFO (overriding your DEBUG)
- Third-party libraries will be silenced to WARNING level

### Scenario 3: Your Project Uses a Logging Framework (e.g., structlog, loguru)

**What Happens:**
- Depends on whether the framework adds handlers to root logger
- If handlers exist: Same as Scenario 2
- If no handlers: Same as Scenario 1

**Example with loguru:**
```python
from loguru import logger
# loguru doesn't add handlers to root logger by default

from bshDataProvider import BshData
bsh = BshData()
# BshDataProvider will add its handlers, potentially conflicting with loguru
```

## What ALWAYS Happens (Regardless of Existing Logging)

Even if handlers exist, BshDataProvider will **always**:

1. **Set Root Logger Level:**
   ```python
   root_logger.setLevel(getattr(logging, log_level.upper(), logging.INFO))
   ```
   - This can override your existing level
   - Default is INFO if not specified

2. **Silence Third-Party Libraries:**
   ```python
   for lib in ["urllib3", "requests", "sqlalchemy", "blpapi", "pandas", "numexpr", "asyncio"]:
       logging.getLogger(lib).setLevel(logging.WARNING)
   ```
   - These libraries will only show WARNING and above
   - This affects your entire application, not just BshDataProvider

## Potential Issues

### Issue 1: Log Level Override

**Problem:**
If your project sets logging to DEBUG, BshDataProvider will change it to INFO.

**Solution:**
```python
import logging

# Set up your logging first
logging.basicConfig(level=logging.DEBUG)

# Then initialize BshDataProvider with explicit log_level
from bshDataProvider import BshData
bsh = BshData(log_level="DEBUG")  # Match your level
```

### Issue 2: Format Conflicts

**Problem:**
BshDataProvider's format might not match your project's format.

**Solution:**
```python
import logging

# Configure your logging BEFORE importing BshDataProvider
logging.basicConfig(
    format='Your custom format',
    level=logging.INFO
)

# Now BshDataProvider won't add its handlers
from bshDataProvider import BshData
bsh = BshData()
```

### Issue 3: Third-Party Library Suppression

**Problem:**
BshDataProvider silences libraries you might want to see.

**Solution:**
After initialization, restore the levels:
```python
from bshDataProvider import BshData
bsh = BshData()

# Restore library logging if needed
import logging
logging.getLogger("urllib3").setLevel(logging.DEBUG)
logging.getLogger("requests").setLevel(logging.DEBUG)
```

### Issue 4: File Handler Conflicts

**Problem:**
If you specify `log_file`, BshDataProvider will add a file handler even if you already have one.

**Solution:**
```python
# Don't specify log_file if you have your own file logging
from bshDataProvider import BshData
bsh = BshData(log_file=None)  # Explicitly disable file logging
```

## Best Practices for Using in External Projects

### Option 1: Let BshDataProvider Manage Logging (Simple Projects)

**When to use:**
- Small projects without existing logging
- Quick scripts or notebooks
- You want BshDataProvider's format

**Code:**
```python
from bshDataProvider import BshData

# Let it configure everything
bsh = BshData(
    config_path="config.yaml",
    log_level="INFO",
    log_file="logs/bshapi.log"  # Optional
)
```

### Option 2: Configure Your Logging First (Recommended)

**When to use:**
- Projects with existing logging
- You want control over format and handlers
- Production applications

**Code:**
```python
import logging

# Configure YOUR logging first
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('myapp.log')
    ]
)

# Now BshDataProvider won't add handlers, but will still set levels
from bshDataProvider import BshData
bsh = BshData(
    log_level="INFO",  # Match your level to avoid override
    log_file=None      # Don't add file handler
)
```

### Option 3: Use Logger Hierarchy (Advanced)

**When to use:**
- You want to isolate BshDataProvider logs
- You use structured logging
- Complex logging requirements

**Code:**
```python
import logging

# Create a separate logger for BshDataProvider
bsh_logger = logging.getLogger('bshDataProvider')
bsh_handler = logging.StreamHandler()
bsh_handler.setFormatter(logging.Formatter('BSH: %(message)s'))
bsh_logger.addHandler(bsh_handler)
bsh_logger.setLevel(logging.INFO)

# Your main logging
logging.basicConfig(level=logging.DEBUG)

# BshDataProvider will still configure root logger, but you can filter
from bshDataProvider import BshData
bsh = BshData()
```

## Log Messages You'll See

When using BshDataProvider, you'll see these log messages:

```
14:11:03 | MainProcess | INFO     | interface.bshdata | Logging inizializzato.
14:11:03 | MainProcess | INFO     | core.utils.memory_provider | Cache globally enabled
14:11:03 | MainProcess | INFO     | core.utils.memory_provider | Cache directory set to: ...
14:11:03 | MainProcess | INFO     | providers.timescale.provider | ✅ TimescaleProvider initialized successfully
14:11:05 | MainProcess | INFO     | providers.oracle.provider | ✅ OracleConnection established successfully
14:11:05 | MainProcess | INFO     | interface.bshdata | BshData inizializzata con successo.
```

These will appear in:
- Console (stdout) - always
- Log file - if `log_file` is specified

## Controlling BshDataProvider Logging

### Disable File Logging
```python
bsh = BshData(log_file=None)
```

### Set Different Levels
```python
bsh = BshData(
    log_level="WARNING",        # Console: only warnings and errors
    log_level_file="DEBUG"      # File: everything
)
```

### Change Level at Runtime
```python
bsh = BshData()
bsh.set_log_level("DEBUG")  # Change console level
```

### Disable Logging Entirely (Not Recommended)

You can't completely disable BshDataProvider's logging setup, but you can:
1. Set level to CRITICAL (only critical errors)
2. Don't specify log_file
3. Configure your own logging first

```python
import logging
logging.basicConfig(level=logging.CRITICAL)  # Your app won't log

from bshDataProvider import BshData
bsh = BshData(log_level="CRITICAL", log_file=None)
```

## Summary

| Your Project State | Handlers Added? | Root Level Changed? | Libraries Silenced? |
|-------------------|----------------|---------------------|---------------------|
| No logging config | ✅ Yes | ✅ Yes | ✅ Yes |
| Has logging config | ❌ No | ✅ Yes | ✅ Yes |
| Uses logging framework | Depends | ✅ Yes | ✅ Yes |

**Key Takeaways:**
1. ✅ BshDataProvider respects existing handlers (won't duplicate)
2. ⚠️ It will always set root logger level and silence libraries
3. ✅ Configure your logging first if you want full control
4. ✅ Use constructor parameters to match your logging setup
5. ✅ Consider the impact on third-party library logging

## Recommendations

1. **For New Projects:** Let BshDataProvider configure logging
2. **For Existing Projects:** Configure your logging first, then initialize BshDataProvider
3. **For Production:** Use explicit log levels and file paths
4. **For Debugging:** Set `log_level="DEBUG"` to see detailed information
5. **For Integration:** Be aware that third-party libraries will be silenced

