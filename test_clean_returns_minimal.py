"""
Minimal test - bypass instrument fetching.
"""
import pandas as pd
import numpy as np
from analytics.adjustments import Adjuster
from analytics.adjustments.fx_spot import FxSpotComponent

# Mock instrument
class MockInstrument:
    def __init__(self, id_, currency='EUR', type_='ETP'):
        self.id = id_
        self.currency = currency
        self.type = type_

# Create simple test data
dates = pd.date_range('2025-01-01', periods=5, freq='D')

# Prices
prices = pd.DataFrame({'A': [100, 101, 102, 103, 104]}, index=dates)

# FX composition
fx_composition = pd.DataFrame({'USD': [1.0]}, index=['A'])

# FX prices
fx_prices = pd.DataFrame({'USD': [1.10, 1.11, 1.12, 1.13, 1.14]}, index=dates)

print("Creating components...")
fx_comp = FxSpotComponent(fx_composition, fx_prices)

print("Creating adjuster with mock instruments...")
instruments = {'A': MockInstrument('A', currency='EUR', type_='ETP')}
adj = Adjuster(prices, instruments=instruments, is_intraday=False)

print("Adding component...")
adj.add(fx_comp)

print("Calculating adjustments...")
adjustments = adj.calculate_adjustments()
print(f"Adjustments shape: {adjustments.shape}")
print(f"Non-zero: {(adjustments != 0).sum().sum()}")
print(adjustments)

print("\nCalculating clean returns...")
clean_returns = adj.get_clean_returns()
print(f"Clean returns shape: {clean_returns.shape}")
print(clean_returns)

print("\n✓ Test completed successfully!")
