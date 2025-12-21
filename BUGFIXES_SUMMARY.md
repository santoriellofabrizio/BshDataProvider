# Bug Fixes Summary

## Bug 1: Constructor Default Values Overriding YAML Config ✅ FIXED

### Problem
When constructor parameters used their default values (like `cache=True`, `autocomplete=None`), they were passed to `get_api_config()` as overrides, which unconditionally overrode YAML config values via `data.update(overrides)`. This broke the intended precedence where constructor args should only override when explicitly provided, not when using defaults.

**Example:**
```python
# YAML: cache: false
bsh = BshData()  # cache=True (default) would override YAML's false
```

Also, `None` defaults would override explicitly configured YAML values with `None`.

### Root Cause
1. In `BshData.__init__()`, all constructor parameters (including defaults) were passed to `get_api_config()`
2. In `get_api_config()`, `data.update(overrides)` unconditionally overwrote all values, including `None`

### Fix Applied

1. **Changed `cache` default from `True` to `None`** in `BshData.__init__()`
   - Now `None` means "use YAML config value"
   - Explicit `True`/`False` still override YAML

2. **Filter out `None` values in `BshData.__init__()`**
   - Only pass non-`None` values as overrides to `get_api_config()`
   - This ensures defaults don't override YAML

3. **Filter out `None` values in `get_api_config()`**
   - Added `filtered_overrides = {k: v for k, v in overrides.items() if v is not None}`
   - This provides defense-in-depth against `None` values overriding config

### Files Modified
- `src/interface/bshdata.py`: Changed `cache` default to `None`, only pass non-`None` overrides
- `src/core/utils/config_manager.py`: Filter out `None` values in `get_api_config()`

### Behavior After Fix
```python
# YAML: cache: false
bsh = BshData()  # Uses YAML: cache=False ✅
bsh = BshData(cache=True)  # Overrides YAML: cache=True ✅
bsh = BshData(cache=False)  # Overrides YAML: cache=False ✅

# YAML: autocomplete: true
bsh = BshData()  # Uses YAML: autocomplete=True ✅
bsh = BshData(autocomplete=False)  # Overrides YAML: autocomplete=False ✅
```

---

## Bug 2: TimescaleProvider Silent Failure ✅ FIXED

### Problem
`TimescaleProvider.__init__()` caught exceptions but failed to re-raise them, causing silent failures. When initialization encountered an error, it was logged but the exception was swallowed, leaving a partially-initialized provider object. The caller wouldn't know the provider failed and would encounter `AttributeError`s later when trying to access uninitialized attributes like `self.query_ts`.

**Example:**
```python
provider = TimescaleProvider()  # Fails silently
provider.fetch_market_data(...)  # AttributeError: 'TimescaleProvider' has no attribute 'query_ts'
```

### Root Cause
The exception handler in `TimescaleProvider.__init__()` logged the exception but didn't re-raise it:
```python
except Exception as e:
    logger.exception(f"❌ Failed to initialize TimescaleProvider: {e}")
    # Missing: raise
```

This differed from `OracleProvider` which correctly re-raised exceptions after logging.

### Fix Applied
Added `raise` statement after logging the exception:
```python
except Exception as e:
    logger.exception(f"❌ Failed to initialize TimescaleProvider: {e}")
    raise  # Re-raise exception to prevent silent failures
```

### Files Modified
- `src/providers/timescale/provider.py`: Added `raise` statement in exception handler

### Behavior After Fix
```python
try:
    provider = TimescaleProvider()  # Raises exception immediately ✅
except Exception as e:
    # Exception is properly raised, caller can handle it
    print(f"Provider initialization failed: {e}")
```

---

## Testing Recommendations

### Test Bug 1 Fix
```python
# Test 1: YAML config should be used when constructor args are None
# config.yaml: cache: false
bsh = BshData()
assert bsh._config_manager.get_api_config().cache == False

# Test 2: Explicit constructor args should override YAML
bsh = BshData(cache=True)
assert bsh._config_manager.get_api_config().cache == True

# Test 3: None values shouldn't override YAML
# config.yaml: autocomplete: true
bsh = BshData(autocomplete=None)  # Should use YAML value
assert bsh._config_manager.get_api_config().autocomplete == True
```

### Test Bug 2 Fix
```python
# Test: Provider should raise exception on initialization failure
with pytest.raises(Exception):
    provider = TimescaleProvider(config_path="nonexistent.yaml")
    # Should raise immediately, not fail silently
```

---

## Breaking Changes

⚠️ **Breaking Change**: The `cache` parameter default changed from `True` to `None`

**Impact:**
- Code that relied on `BshData()` always enabling cache will now use YAML config
- If YAML doesn't specify `cache`, it defaults to `True` (via dataclass default)

**Migration:**
```python
# Old behavior (always True)
bsh = BshData()  # cache=True

# New behavior (uses YAML)
bsh = BshData()  # Uses YAML config value

# To maintain old behavior, explicitly pass True
bsh = BshData(cache=True)  # Always True
```

---

## Summary

Both bugs have been fixed:
- ✅ **Bug 1**: Constructor defaults no longer override YAML config
- ✅ **Bug 2**: TimescaleProvider now properly raises exceptions on initialization failure

The fixes maintain backward compatibility where possible, with one intentional breaking change (`cache` default) that improves the API's behavior.

