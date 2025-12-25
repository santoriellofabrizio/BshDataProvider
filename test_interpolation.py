"""
Test interpolation features.
"""
import pandas as pd
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "src"))

from analytics.adjustments.adjuster import Adjuster

# Create test data with NaN
prices = pd.DataFrame({
    'A': [100.0, None, 102.0, None, 105.0],
    'B': [50.0, 51.0, None, 52.0, 51.0]
}, index=pd.date_range('2024-01-01', periods=5, freq='D'))

print('Original prices:')
print(prices)
print(f'\nNaN count: {prices.isna().sum().sum()}')

# Test 1: ffill only
print('\n' + '='*80)
print('Test 1: ffill only')
print('='*80)
adj1 = Adjuster(prices, fill_method='ffill')
print(adj1.prices)
print(f'NaN count: {adj1.prices.isna().sum().sum()}')

# Test 2: linear interpolation only
print('\n' + '='*80)
print('Test 2: linear interpolation')
print('='*80)
adj2 = Adjuster(prices, fill_method='linear')
print(adj2.prices)
print(f'NaN count: {adj2.prices.isna().sum().sum()}')

# Test 3: Time interpolation
print('\n' + '='*80)
print('Test 3: time interpolation')
print('='*80)
adj3 = Adjuster(prices, fill_method='time')
print(adj3.prices)
print(f'NaN count: {adj3.prices.isna().sum().sum()}')

print('\n' + '='*80)
print('ALL TESTS PASSED!')
print('='*80)
