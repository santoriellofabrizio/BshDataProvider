# Adjuster Improvements - Implementation Summary

## Overview
Complete implementation of three major improvements to the BshDataProvider Adjuster system:
1. **Intraday support** with date normalization option
2. **FX ticker normalization** (EURUSD → USD)
3. **Dividend adjustment logic** for intraday period returns

---

## 1. Intraday Support (with `intraday` parameter)

### Changes
- **File**: `adjuster.py`, `component.py`, all component files
- **New parameter**: `intraday=False` (default)

### Behavior
```python
# Daily mode (default) - normalizes timestamps to dates
adj = Adjuster(prices, fx_prices, intraday=False)
# 2024-01-15 10:30:00 → 2024-01-15 00:00:00

# Intraday mode - preserves timestamps
adj = Adjuster(prices, fx_prices, intraday=True)
# 2024-01-15 10:30:00 → 2024-01-15 10:30:00
```

### Implementation Details
- Auto-normalizes `prices.index` and `fx_prices.index` when `intraday=False`
- All Component signatures updated to accept `Union[List[date], List[datetime]]`
- `calculate()`, `get_breakdown()`, `clean_returns()` handle both date and datetime

---

## 2. FX Ticker Normalization & Validation

### Changes
- **File**: `adjuster.py`
- **New method**: `_normalize_fx_columns()`

### Problem Solved
Before:
```python
fx_prices.columns = ['EURUSD', 'EURGBP']  # Ticker format
fx_composition.columns = ['USD', 'GBP']   # Currency codes
# ❌ Mismatch: KeyError when looking up 'USD'
```

After:
```python
adj = Adjuster(prices, fx_prices, ...)
# Auto-normalizes: 'EURUSD' → 'USD', 'EURGBP' → 'GBP'
adj.fx_prices.columns = ['USD', 'GBP']
# ✓ Matches fx_composition
```

### Normalization & Validation Rules

**Case 1: EURUSD format (correct, EUR-based)**
- `EURUSD` (6 chars, starts with EUR) → `USD` (last 3 chars)
- Values: **unchanged**
- Log: Debug level

**Case 2: USDEUR format (inverted, non-EUR base)**
- `USDEUR` (6 chars, ends with EUR) → `USD` (first 3 chars)
- Values: **inverted** (1/price)
- Log: **WARNING** - "FX column 'USDEUR' is inverted. Inverting prices: 1/USDEUR → USD"

**Case 3: USD format (ambiguous, 3 chars)**
- `USD` (3 chars) → `USD` (unchanged)
- Values: **unchanged** (assumes EURUSD)
- Log: **WARNING** - "FX column 'USD' is a currency code without EUR base indication. Assuming it represents EURUSD."

**Case 4: Other formats**
- Keep as-is with warning

### Inversion Logic

```python
# Original inverted prices
fx_prices['USDEUR'] = [0.90, 0.91, 0.89]  # USD/EUR rate

# After normalization
adj.fx_prices['USD'] = [1.11, 1.10, 1.12]  # EUR/USD rate (1/original)
```

### Zero/Invalid Price Handling
- `1/0.0` → `NaN` (not `inf`)
- Prevents downstream errors in calculations

---

## 3. Dividend Adjustment for Intraday

### Changes
- **File**: `dividend.py`
- **New methods**: `_is_intraday_mode()`, `_calculate_daily()`, `_calculate_intraday()`

### Logic Overview
**Dividends are treated as occurring at midnight (date boundary)**

For period returns crossing a date boundary:
```
14-01 16:00 → 15-01 09:00  (date changes to 15-01)
  ↓
If dividend exists on 15-01:
  Apply adjustment = dividend / price_at_t1
```

### Example
```python
timestamps = [
    '2024-01-14 16:00',  # Last timestamp on 14th
    '2024-01-15 09:00',  # First timestamp on 15th (DATE CHANGE)
    '2024-01-15 14:00',  # Same date (no adjustment)
]

dividends = {
    'SPY US': [..., 0, 1.50, 0]  # $1.50 on 2024-01-15
}

# Period returns:
14-01 16:00 → 15-01 09:00:  +0.0033 adjustment (crosses midnight)
15-01 09:00 → 15-01 14:00:  no adjustment (same date)
```

### Auto-Detection
Component automatically detects mode:
- **Daily**: If timestamps have `hour=0, minute=0`
- **Intraday**: If timestamps have non-zero time components

---

## Architecture Design

### Period Returns vs Cumulative Returns

**Decision**: Adjuster works exclusively with **period returns**

Rationale:
1. Adjustment formula is additive: `clean = raw + adjustment`
2. Mathematically simpler and cleaner
3. User can cumulate after: `(1 + clean_period).cumprod() - 1`

```python
# Adjuster produces period returns
period_returns = prices.pct_change()
clean_period = adjuster.clean_returns(period_returns)

# User cumulates if needed
cumulative = (1 + clean_period).cumprod() - 1
```

### Dividend Application Point

For period return `t1 → t2`:
- Adjustment applied at **t2** (endpoint of period)
- Uses price at **t1** for normalization
- Triggered when `date(t1) != date(t2)` AND dividend exists on `date(t2)`

---

## File Changes Summary

### Modified Files
1. **adjuster.py**
   - Added `intraday` parameter
   - Added `_normalize_fx_columns()` method
   - Updated `calculate()`, `get_breakdown()`, `clean_returns()` signatures

2. **component.py**
   - Updated `calculate_batch()` signature: `Union[List[date], List[datetime]]`

3. **All component files** (ter.py, ytm.py, fx_spot.py, fx_forward_carry.py, repo.py, dividend.py)
   - Updated imports and signatures

4. **dividend.py** (major changes)
   - Added `_is_intraday_mode()`
   - Added `_calculate_daily()`
   - Added `_calculate_intraday()`
   - Removed market open time logic (simplified to midnight boundary)

### Test Files Created
1. **test_adjuster_improvements.py**
   - FX normalization tests
   - Intraday mode tests
   - DateTime handling tests

2. **test_dividend_intraday.py**
   - Date-change dividend logic
   - Daily mode backward compatibility
   - Multiple dividends

3. **test_fx_validation.py**
   - EURUSD format (unchanged)
   - USDEUR format (inverted)
   - Currency code format (warning)
   - Mixed formats
   - Zero price handling

---

## Usage Examples

### Basic Daily Usage (unchanged)
```python
from analytics.adjustments.adjuster import Adjuster
from analytics.adjustments.ter import TerComponent

dates = pd.date_range('2024-01-01', periods=100, freq='D')
prices = pd.DataFrame(...)
fx_prices = pd.DataFrame(...)

adj = Adjuster(prices, fx_prices)  # intraday=False by default
adj.add(TerComponent(ters))
clean_returns = adj.calculate()
```

### Intraday Usage
```python
# Intraday timestamps
timestamps = pd.date_range('2024-01-01', periods=500, freq='30min')
prices = pd.DataFrame(..., index=timestamps)
fx_prices = pd.DataFrame(..., index=timestamps)

# Preserve intraday timestamps
adj = Adjuster(prices, fx_prices, intraday=True)
adj.add(DividendComponent(dividends))
clean_returns = adj.calculate()

# Cumulate if needed
cumulative = (1 + clean_returns).cumprod() - 1
```

### FX Ticker Normalization
```python
# Pass FX tickers instead of currency codes
fx_prices = pd.DataFrame({
    'EURUSD': [...],
    'EURGBP': [...],
}, index=dates)

# Adjuster auto-normalizes
adj = Adjuster(prices, fx_prices)
# adj.fx_prices.columns = ['USD', 'GBP']

# Now FxSpotComponent works seamlessly
fx_composition = pd.DataFrame({
    'USD': [0.65],  # ✓ Matches normalized column
    'GBP': [0.15],
}, index=['IWDA LN'])

adj.add(FxSpotComponent(fx_composition))
```

---

## Testing

Run test suites:
```powershell
cd C:\AFMachineLearning\Libraries\BshDataProvider
.venv\Scripts\python.exe test_adjuster_improvements.py
.venv\Scripts\python.exe test_dividend_intraday.py
```

Expected output:
```
✓ FX normalization tests passed
✓ Intraday mode tests passed
✓ Dividend date-change logic passed
✓ Daily mode backward compatibility passed
✓ All tests passed
```

---

## Key Design Decisions

1. **Intraday is opt-in**: Default `intraday=False` preserves backward compatibility

2. **Midnight dividend boundary**: Simpler than market open times, sufficient accuracy

3. **Period returns only**: Cleaner architecture, user controls cumulation

4. **Auto-detection**: Component automatically detects daily vs intraday mode

5. **FX normalization is automatic**: Transparent to user, works with both formats

---

## Next Steps (Future Enhancements)

1. **Performance optimization**
   - Batch FX conversions in DividendComponent
   - Cache normalized FX columns

2. **Enhanced dividend metadata**
   - Optional: Support explicit `ex_dividend_datetime` if needed
   - Payment vs ex-dividend date tracking

3. **Multiple date changes**
   - Handle weekend gaps (multiple dates between t1 and t2)
   - Currently handles all dividends in the gap

4. **Validation**
   - Add warnings for suspicious period returns (>50% move)
   - Validate dividend magnitudes vs price

---

## Backward Compatibility

All changes are **backward compatible**:
- Default `intraday=False` maintains existing behavior
- FX normalization is transparent (works with both formats)
- Dividend component auto-detects mode
- All existing code continues to work unchanged

---

## Contact

Implementation by: Claude (Anthropic)
Date: December 21, 2025
Library: BshDataProvider
