"""
Compare old vs new adjuster to verify clean returns are similar.
"""
import pandas as pd
import numpy as np

# Mock instrument
class MockInstrument:
    def __init__(self, id_, currency='EUR', inst_type='ETP'):
        self.id = id_
        self.currency = currency
        self.type = inst_type

# Import both old and new
import sys
sys.path.insert(0, 'C:\\AFMachineLearning\\Libraries\\BshDataProvider\\src')

from analytics.adjustments.fx_spot import FxSpotComponent

# Import NEW adjuster
from analytics.adjustments.adjuster import Adjuster as NewAdjuster

# Import OLD adjuster
import importlib.util
spec = importlib.util.spec_from_file_location("old_adjuster",
    "C:\\AFMachineLearning\\Libraries\\BshDataProvider\\src\\analytics\\adjustments\\adjuster_old.py")
old_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(old_module)
OldAdjuster = old_module.Adjuster

print("=" * 80)
print("COMPARISON TEST: Old vs New Adjuster")
print("=" * 80)

# Test data
dates = pd.date_range('2025-01-01', periods=10, freq='D')
prices = pd.DataFrame({
    'A': np.linspace(100, 110, 10),
    'B': np.linspace(200, 210, 10)
}, index=dates)

fx_composition = pd.DataFrame({
    'USD': [0.5, 0.7],
    'GBP': [0.5, 0.3]
}, index=['A', 'B'])

fx_prices = pd.DataFrame({
    'USD': np.linspace(1.10, 1.15, 10),
    'GBP': np.linspace(1.25, 1.30, 10)
}, index=dates)

instruments = {
    'A': MockInstrument('A'),
    'B': MockInstrument('B')
}

# Setup OLD adjuster
print("\n1. Setting up OLD adjuster...")
fx_comp_old = FxSpotComponent(fx_composition, fx_prices)
old_adj = OldAdjuster(prices, instruments=instruments, intraday=False)
old_adj.add(fx_comp_old)

# Setup NEW adjuster
print("2. Setting up NEW adjuster...")
fx_comp_new = FxSpotComponent(fx_composition, fx_prices)
new_adj = NewAdjuster(prices, instruments=instruments, is_intraday=False)
new_adj.add(fx_comp_new)

# Calculate adjustments
print("\n3. Calculating adjustments...")
old_adjustments = old_adj.calculate_adjustments()
new_adjustments = new_adj.calculate_adjustments()

print(f"   OLD adjustments shape: {old_adjustments.shape}")
print(f"   NEW adjustments shape: {new_adjustments.shape}")

# Calculate clean returns
print("\n4. Calculating clean returns...")
old_clean = old_adj.get_clean_returns()
new_clean = new_adj.get_clean_returns()

print(f"   OLD clean returns shape: {old_clean.shape}")
print(f"   NEW clean returns shape: {new_clean.shape}")

# Compare
print("\n5. COMPARISON:")
print("\n   ADJUSTMENTS:")
adj_match = np.allclose(old_adjustments.values, new_adjustments.values, rtol=1e-10, equal_nan=True)
print(f"   Match: {adj_match}")
if not adj_match:
    print(f"   Max difference: {np.nanmax(np.abs(old_adjustments.values - new_adjustments.values))}")
    print("\n   OLD:")
    print(old_adjustments.head())
    print("\n   NEW:")
    print(new_adjustments.head())

print("\n   CLEAN RETURNS:")
clean_match = np.allclose(old_clean.values, new_clean.values, rtol=1e-10, equal_nan=True)
print(f"   Match: {clean_match}")
if not clean_match:
    print(f"   Max difference: {np.nanmax(np.abs(old_clean.values - new_clean.values))}")
    print("\n   OLD:")
    print(old_clean.head())
    print("\n   NEW:")
    print(new_clean.head())

# Test with temp data
print("\n6. Testing with TEMP DATA...")
new_prices = pd.Series({'A': 111, 'B': 211}, name=pd.Timestamp('2025-01-11'))
new_fx = pd.Series({'USD': 1.16, 'GBP': 1.31}, name=pd.Timestamp('2025-01-11'))

old_clean_temp = old_adj.get_clean_returns()
new_clean_temp = new_adj.get_clean_returns()

temp_match = np.allclose(old_clean_temp.values, new_clean_temp.values, rtol=1e-10, equal_nan=True)
print(f"   Temp data match: {temp_match}")
if not temp_match:
    print(f"   Max difference: {np.nanmax(np.abs(old_clean_temp.values - new_clean_temp.values))}")

print("\n" + "=" * 80)
if adj_match and clean_match and temp_match:
    print("SUCCESS: Old and new adjusters produce identical results")
else:
    print("FAIL: Results differ between old and new")
print("=" * 80)
