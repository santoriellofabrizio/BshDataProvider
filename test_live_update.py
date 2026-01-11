"""Test that live_update includes the last row"""
import pandas as pd
import sys
sys.path.insert(0, 'C:\\AFMachineLearning\\Libraries\\BshDataProvider\\src')

from analytics.adjustments import Adjuster
from analytics.adjustments.fx_spot import FxSpotComponent

class MockInstrument:
    def __init__(self, id_, currency='EUR', inst_type='ETP'):
        self.id = id_
        self.currency = currency
        self.type = inst_type

# Historical data: 5 days
dates = pd.date_range('2025-01-01', periods=5, freq='D')
prices = pd.DataFrame({'A': [100, 101, 102, 103, 104]}, index=dates)
fx_composition = pd.DataFrame({'USD': [1.0]}, index=['A'])
fx_prices = pd.DataFrame({'USD': [1.10, 1.11, 1.12, 1.13, 1.14]}, index=dates)

instruments = {'A': MockInstrument('A')}

# Setup
fx_comp = FxSpotComponent(fx_composition, fx_prices)
adj = Adjuster(prices, instruments=instruments, is_intraday=False)
adj.add(fx_comp)

print("HISTORICAL DATA:")
print(f"Prices shape: {prices.shape}")
print(f"Last price date: {prices.index[-1]}")
print(f"Last price: {prices.iloc[-1, 0]}")

# Calculate clean returns WITHOUT live data
clean_historical = adj.get_clean_returns()
print(f"\nClean returns (historical only): {clean_historical.shape}")
print(clean_historical.tail(2))

# Live data: day 6
live_prices = pd.Series({'A': 105}, name=pd.Timestamp('2025-01-06'))
live_fx = pd.Series({'USD': 1.15}, name=pd.Timestamp('2025-01-06'))

print(f"\nLIVE DATA:")
print(f"Live price date: {live_prices.name}")
print(f"Live price: {live_prices['A']}")

# Calculate clean returns WITH live data
clean_with_live = adj.get_clean_returns()
print(f"\nClean returns (with live): {clean_with_live.shape}")
print("Last 3 rows:")
print(clean_with_live.tail(3))

# Verify last row is the live update
if len(clean_with_live) > len(clean_historical):
    print(f"\nSUCCESS: Live update included!")
    print(f"  Historical rows: {len(clean_historical)}")
    print(f"  With live rows: {len(clean_with_live)}")
    print(f"  Last date: {clean_with_live.index[-1]}")
else:
    print(f"\nFAIL: Live update NOT included")
    print(f"  Historical rows: {len(clean_historical)}")
    print(f"  With live rows: {len(clean_with_live)}")
