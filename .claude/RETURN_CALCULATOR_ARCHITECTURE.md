# ReturnCalculator Architecture

## Overview

Centralized return calculation logic to support multiple return types (percentage, logarithmic, absolute).

## Design Pattern: Dependency Injection

The `ReturnCalculator` is created by `Adjuster` and injected into all components via `.add()`.

```python
# Adjuster creates calculator
adjuster = Adjuster(prices, return_type="percentage")  # or "logarithmic", "absolute"
adjuster.return_calculator  # ReturnCalculator instance

# Components receive calculator when added
adjuster.add(ter_component)  # Injects return_calculator into ter_component
ter_component.return_calculator  # Now available
```

## Files

### 1. `return_calculator.py`
**New file** - Centralizes all return logic

```python
class ReturnType(Enum):
    PERCENTAGE = "percentage"
    LOGARITHMIC = "logarithmic"
    ABSOLUTE = "absolute"

class ReturnCalculator:
    def calculate_returns(prices: pd.DataFrame) -> pd.DataFrame
    def accumulate_returns(returns: pd.DataFrame) -> pd.DataFrame
    def returns_to_prices(returns: pd.DataFrame, initial_price: pd.Series) -> pd.DataFrame
```

### 2. `component.py`
**Modified** - Added return calculator injection

```python
class Component(ABC):
    def __init__(self):
        self._return_calculator: Optional[ReturnCalculator] = None

    def set_return_calculator(self, calculator: ReturnCalculator) -> None:
        """Called by Adjuster.add()"""
        self._return_calculator = calculator

    @property
    def return_calculator(self) -> ReturnCalculator:
        """Access calculator (raises error if not set)"""
        if self._return_calculator is None:
            raise RuntimeError("Component not added to Adjuster")
        return self._return_calculator
```

### 3. `adjuster.py`
**Modified** - Creates and injects calculator

```python
class Adjuster:
    def __init__(self, prices, return_type="percentage"):
        self.return_calculator = ReturnCalculator(return_type)

    def add(self, component: Component) -> 'Adjuster':
        # Inject calculator into component
        component.set_return_calculator(self.return_calculator)
        self.components.append(component)
        return self

    def clean_returns(self) -> pd.DataFrame:
        # Use calculator for return calculation
        raw_returns = self.return_calculator.calculate_returns(self.prices)
        # ... rest of logic

    def clean_prices(self) -> pd.DataFrame:
        # Use calculator for price reconstruction
        clean_prices = self.return_calculator.returns_to_prices(returns, initial_price)
        # ... rest of logic
```

### 4. `fx_spot.py`
**Modified** - Uses calculator for FX returns

```python
class FxSpotComponent(Component):
    def calculate_adjustment(self, instruments, dates, prices):
        # Use return calculator for FX returns
        fx_returns = self.return_calculator.calculate_returns(current_fx_prices)
        # ... rest of logic
```

## Usage Examples

### Current Usage (Percentage Returns - Default)

```python
# No change required - percentage is default
adjuster = Adjuster(prices)
adjuster.add(TerComponent(ters))
adjuster.add(FxSpotComponent(composition, fx_prices))
clean_returns = adjuster.get_clean_returns()  # Percentage returns
```

### NaN Handling
```python
# Forward-fill
adjuster = Adjuster(prices, fill_method='ffill')

# Backward-fill
adjuster = Adjuster(prices, fill_method='bfill')

# Time-weighted interpolation for is_intraday data
adjuster = Adjuster(prices, fill_method='time')

# Linear interpolation
adjuster = Adjuster(prices, fill_method='linear')

# Any other pandas interpolation method
adjuster = Adjuster(prices, fill_method='polynomial')
adjuster = Adjuster(prices, fill_method='spline')
```

### Future Usage (Logarithmic Returns)

```python
# Specify return type
adjuster = Adjuster(prices, return_type="logarithmic")
adjuster.add(TerComponent(ters))
adjuster.add(FxSpotComponent(composition, fx_prices))
clean_returns = adjuster.get_clean_returns()  # Logarithmic returns

# Components will automatically adjust their calculations
# based on the return type
```

### Future Usage (Absolute Returns)

```python
adjuster = Adjuster(prices, return_type="absolute")
adjuster.add(TerComponent(ters))
clean_returns = adjuster.get_clean_returns()  # Absolute returns
```

## Return Type Formulas

### Percentage Returns (Default)
```python
# Return calculation
r_t = (P_t - P_{t-1}) / P_{t-1}

# Price reconstruction
P_t = P_0 × (1 + r_1) × (1 + r_2) × ... × (1 + r_t)
    = P_0 × ∏(1 + r_i)
```

### Logarithmic Returns
```python
# Return calculation
r_t = log(P_t / P_{t-1})

# Price reconstruction
P_t = P_0 × exp(r_1 + r_2 + ... + r_t)
    = P_0 × exp(Σr_i)
```

### Absolute Returns
```python
# Return calculation
r_t = P_t - P_{t-1}

# Price reconstruction
P_t = P_0 + r_1 + r_2 + ... + r_t
    = P_0 + Σr_i
```

## Benefits

1. **Centralized Logic**: All return math in one place
2. **Type Safety**: Enum for return types prevents typos
3. **Extensible**: Easy to add new return types
4. **Testable**: ReturnCalculator can be tested independently
5. **Consistent**: All components use same calculator
6. **Clear Errors**: Runtime error if component not added to adjuster
7. **Future-Proof**: Components automatically support new return types

## Migration Path

### Current State (v1.0)
- Only percentage returns supported
- ReturnCalculator exists but components don't need modification yet
- All tests pass with default behavior

### Future State (v2.0)
- Components will check `self.return_calculator.return_type`
- Adjust their formulas accordingly:
  - TER: `log(1 - ter)` for log returns, `-ter × price` for absolute
  - FX: Already uses calculator for FX returns
  - Dividend: `log(1 + div/price)` for log returns, `div` for absolute

### Example Component Migration (TER)
```python
class TerComponent(Component):
    def calculate_adjustment(self, instruments, dates, prices):
        # ... existing logic ...

        for inst in applicable:
            if self.return_calculator.return_type == ReturnType.PERCENTAGE:
                # Current formula
                result[inst.id] = -self.ters[inst.id] * year_fractions

            elif self.return_calculator.return_type == ReturnType.LOGARITHMIC:
                # Log return adjustment
                result[inst.id] = np.log(1 - self.ters[inst.id] * year_fractions)

            elif self.return_calculator.return_type == ReturnType.ABSOLUTE:
                # Absolute return adjustment
                result[inst.id] = -self.ters[inst.id] * year_fractions * prices.shift(1)[inst.id]

        return result
```

## Testing

Run `test_return_types.py` to verify ReturnCalculator:
```bash
python test_return_types.py
```

Run `test_updatable_components.py` to verify integration:
```bash
python test_updatable_components.py
```

## Notes

- Currently only percentage returns are fully implemented in components
- ReturnCalculator infrastructure is ready for log/absolute returns
- Components will need minor updates when log/absolute returns are needed
- All existing code continues to work without changes (percentage is default)
