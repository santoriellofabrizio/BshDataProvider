# Dict Mode Parameter Feature

## Overview

The InfoDataAPI now supports **dict-based parameter mapping**, allowing you to specify different parameter values for different instruments without maintaining aligned lists.

## What Changed

### New Function: `normalize_param()`

Added to `src/core/utils/common.py`:

```python
def normalize_param(value, instruments, default=None):
    """
    Normalizes parameter input to aligned list matching instruments.

    Supports three modes:
    1. Single value (str/scalar): Replicate for all instruments
    2. List: Must match instrument count (or length 1 to replicate)
    3. Dict: Map instrument IDs to values, use default for missing entries
    """
```

### Updated Type Hints

New file `src/interface/api/type_hints.py` with TypedDict classes:
- `InfoDataGetParams`
- `FXCompositionParams`
- `PCFCompositionParams`
- `DividendsParams`
- `TERParams`
- `NAVParams`
- `PricesParams`

All support `Union[str, List[str], Dict[str, str]]` for applicable parameters.

### Updated API Methods

All methods in `InfoDataAPI` now support dict mode for these parameters:
- `currency`
- `market`
- `source`
- `subscriptions`
- `request_type`

## Usage

### Before (Aligned Lists)

```python
# Had to maintain aligned lists
api.get_ter(
    ticker=["IUSA", "VUSA", "CSPX"],
    source=["bloomberg", "bloomberg", "oracle"]  # Must align with tickers!
)
```

**Problems:**
- Error-prone (easy to misalign)
- Verbose (must specify every value)
- Hard to maintain when adding/removing instruments

### After (Dict Mode)

```python
# Only specify what differs from default
api.get_ter(
    ticker=["IUSA", "VUSA", "CSPX"],
    source={
        "CSPX": "oracle"  # Only CSPX uses oracle
        # IUSA and VUSA get default (bloomberg)
    }
)
```

**Benefits:**
- ✅ Only specify exceptions
- ✅ Clear mapping by instrument ID
- ✅ Defaults for unspecified instruments
- ✅ Less error-prone
- ✅ Backward compatible!

## Examples

### Example 1: Mixed Currency Requirements

```python
# Different currencies for different instruments
api.get(
    type="ETP",
    ticker=["IUSA", "VUSA", "CSPX"],
    currency={
        "IUSA": "USD",
        "CSPX": "GBP"
        # VUSA gets default "EUR"
    },
    fields="NAV"
)
```

### Example 2: Different Data Sources

```python
# Use oracle for one instrument, bloomberg for others
api.get_fx_composition(
    ticker=["IUSA", "VUSA", "CSPX"],
    source={
        "CSPX": "oracle"
        # Others default to bloomberg
    }
)
```

### Example 3: Complex Multi-Parameter (InfoDataAPI)

```python
api.get(
    type="ETP",
    ticker=["IUSA", "VUSA", "CSPX", "VUAA"],
    currency={"IUSA": "USD", "VUAA": "USD"},  # USD for 2, default for others
    source={"CSPX": "oracle"},  # Oracle for 1, default for others
    market="XLON",  # Same market for all (single value)
    fields="TER"
)
```

### Example 4: MarketDataAPI with Dict Mode

```python
# Get daily ETF data with mixed parameters
market_api.get_daily_etf(
    ticker=["IUSA", "VUSA", "CSPX"],
    start="2024-01-01",
    end="2024-12-31",
    currency={"IUSA": "USD", "CSPX": "GBP"},  # VUSA gets default EUR
    source={"CSPX": "oracle"},  # IUSA, VUSA get default timescale
    market="ETFP"  # Same market for all
)

# Get intraday data with subscription mapping
market_api.get_intraday(
    date="2024-03-01",
    ticker=["IUSA", "VUSA", "CSPX"],
    type="ETP",
    frequency="5m",
    subscription={
        "IUSA": "ETFP_PREMIUM",
        "CSPX": "ETFP_BASIC"
        # VUSA gets default
    }
)
```

## Type Hints for IDE Support

```python
from interface.api.type_hints import InfoDataGetParams

def my_api_wrapper(**kwargs):
    # Cast to TypedDict for IDE autocomplete (no runtime overhead)
    params: InfoDataGetParams = kwargs

    # Now you get autocomplete and type checking!
    instrument_id = params.get('id')
    market = params.get('market')  # IDE knows this can be str/list/dict
    currency = params.get('currency')
```

## Backward Compatibility

All existing code continues to work! Dict mode is **additive**:

```python
# All still work:
api.get_ter(ticker="IUSA", source="bloomberg")  # Single value
api.get_ter(ticker=["IUSA", "VUSA"], source=["bloomberg", "bloomberg"])  # List
api.get_ter(ticker=["IUSA", "VUSA"], source={"VUSA": "oracle"})  # NEW: Dict
```

## Three Input Modes

| Mode | Example | Behavior |
|------|---------|----------|
| **Single value** | `"USD"` | Replicated to all instruments |
| **List** | `["USD", "EUR", "GBP"]` | Must match instrument count (or single element to replicate) |
| **Dict** | `{"AAPL": "USD", "GOOGL": "EUR"}` | Maps by instrument ID, others get default |

## Supported Methods

### InfoDataAPI

All these methods support dict mode:

- `get()` - Main entry point
- `get_with_instruments()` - Direct instrument usage
- `get_ter()` - TER data
- `get_dividends()` - Dividend data
- `get_fx_composition()` - FX composition
- `get_pcf_composition()` - PCF composition
- `get_nav()` - NAV data
- `get_etp_fields()` - Generic ETP fields

### MarketDataAPI

All these methods support dict mode:

- `get()` - Main entry point for market data
- `get_with_instruments()` - Direct instrument usage
- `get_intraday()` - Generic intraday data
- `get_day_snapshot()` - Day snapshot data
- `get_daily_etf()` - Daily ETF data
- `get_intraday_etf()` - Intraday ETF data
- All other convenience methods (get_daily_future, get_intraday_fx, etc.)

## Implementation Details

### Files Modified

1. **`src/core/utils/common.py`**
   - Added `normalize_param()` function

2. **`src/interface/api/info_data_api.py`**
   - Updated type signatures for all methods
   - Replaced `normalize_list()` with `normalize_param()` where instruments are available
   - Updated docstrings

3. **`src/interface/api/market_api.py`**
   - Updated type signatures for all methods (get, get_with_instruments, _dispatch, etc.)
   - Replaced `normalize_list()` with `normalize_param()` in _dispatch, _retry_with_fallbacks, get_with_instruments
   - Added mock instruments support in get() for dict mode when building instruments from IDs
   - Updated docstrings to document dict mode support

4. **`src/interface/api/type_hints.py`** (NEW)
   - TypedDict definitions for all API methods

### Testing

Run tests:
```bash
python test_normalize_param.py
```

View examples:
```bash
python examples/dict_mode_usage.py
```

## Migration Guide

### From Aligned Lists to Dict Mode

**Before:**
```python
currencies = ["EUR", "EUR", "USD", "EUR", "EUR"]  # Error-prone!
sources = ["bloomberg", "bloomberg", "bloomberg", "oracle", "bloomberg"]

api.get_ter(
    ticker=["A", "B", "C", "D", "E"],
    currency=currencies,
    source=sources
)
```

**After:**
```python
# Only specify exceptions from default
api.get_ter(
    ticker=["A", "B", "C", "D", "E"],
    currency={"C": "USD"},  # Only C needs USD, others default to EUR
    source={"D": "oracle"}  # Only D needs oracle, others default to bloomberg
)
```

Much cleaner and less error-prone!

## Notes

- Dict keys are **instrument IDs** (from `instrument.id`)
- Missing keys in dict → use default value
- Default value is `None` unless specified
- List mode still available for backward compatibility
- No performance overhead (pure Python, no external dependencies)
