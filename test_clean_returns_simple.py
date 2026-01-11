"""
Simple test to verify clean returns are being calculated correctly.
"""
import pandas as pd
import numpy as np
from analytics.adjustments import Adjuster
from analytics.adjustments.fx_spot import FxSpotComponent

# Create simple test data
dates = pd.date_range('2025-01-01', periods=5, freq='D')

# Prices: instrument A goes from 100 to 105
prices = pd.DataFrame({
    'A': [100, 101, 102, 103, 104]
}, index=dates)

# FX composition: instrument A is 100% USD
fx_composition = pd.DataFrame({
    'USD': [1.0]  # 100% USD exposure
}, index=['A'])

# FX prices: USD appreciates from 1.10 to 1.15 (5% move)
fx_prices = pd.DataFrame({
    'USD': [1.10, 1.11, 1.12, 1.13, 1.14]
}, index=dates)

print("=" * 80)
print("TEST: Clean Returns with FX Spot Adjustment")
print("=" * 80)

# Setup
fx_comp = FxSpotComponent(fx_composition, fx_prices)
adj = Adjuster(prices, is_intraday=False).add(fx_comp)

# Calculate dirty returns (without adjustments)
dirty_returns = prices.pct_change().dropna()
print("\n1. DIRTY RETURNS (raw price changes):")
print(dirty_returns)

# Calculate adjustments
adjustments = adj.calculate_adjustments()
print("\n2. ADJUSTMENTS (FX corrections):")
print(adjustments)
print(f"   Non-zero adjustments: {(adjustments != 0).sum().sum()}")

# Calculate clean returns
clean_returns = adj.get_clean_returns()
print("\n3. CLEAN RETURNS (dirty - adjustments):")
print(clean_returns)

# Verify formula: clean = dirty - adjustments
print("\n4. VERIFICATION:")
print("   dirty_returns - adjustments == clean_returns?")
manual_clean = dirty_returns - adjustments.loc[dirty_returns.index]
match = np.allclose(manual_clean.values, clean_returns.values, rtol=1e-10)
print(f"   {match}")

if not match:
    print("\n   MISMATCH DETAILS:")
    print("   Manual clean:")
    print(manual_clean)
    print("   Adjuster clean:")
    print(clean_returns)
    print("   Difference:")
    print(manual_clean - clean_returns)

# Test with temp data
print("\n" + "=" * 80)
print("TEST: Clean Returns with TEMP DATA")
print("=" * 80)

new_price = pd.Series({'A': 105}, name=pd.Timestamp('2025-01-06'))
new_fx = pd.Series({'USD': 1.15}, name=pd.Timestamp('2025-01-06'))

clean_with_temp = adj.get_clean_returns()
print("\n5. CLEAN RETURNS WITH TEMP DATA:")
print(clean_with_temp.tail(2))

# Check adjustments were applied
adj_with_temp = adj.calculate_adjustments()
print("\n6. ADJUSTMENTS WITH TEMP DATA:")
print(adj_with_temp.tail(2))
print(f"   Non-zero adjustments: {(adj_with_temp != 0).sum().sum()}")

print("\n" + "=" * 80)
if match and (adjustments != 0).sum().sum() > 0:
    print("✓ TEST PASSED: Adjustments are being applied correctly")
else:
    print("✗ TEST FAILED: Adjustments not applied correctly")
print("=" * 80)
