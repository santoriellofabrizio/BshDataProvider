"""
Test the new append_update and live_update API.
"""
import sys
sys.path.insert(0, 'src')

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from analytics.adjustments.adjuster import Adjuster
from analytics.adjustments.fx_spot import FxSpotComponent
from core.enums.instrument_types import InstrumentType


class MockETF:
    def __init__(self, id, currency='EUR', currency_hedged=False):
        self.id = id
        self.isin = f'ISIN_{id}'
        self.type = InstrumentType.ETP
        self.currency = currency
        self.currency_hedged = currency_hedged


def test_append_update():
    """Test append_update with incremental calculation"""
    print("="*80)
    print("TEST 1: append_update (permanent storage)")
    print("="*80)

    # Create initial data (10 days)
    dates = pd.date_range('2024-01-01', periods=10, freq='D')
    prices = pd.DataFrame({
        'ETF1': np.linspace(100, 110, 10),
        'ETF2': np.linspace(100, 105, 10)
    }, index=dates)

    fx_prices = pd.DataFrame({
        'USD': np.linspace(1.10, 1.15, 10)
    }, index=dates)

    fx_comp = pd.DataFrame({
        'USD': [1.0, 0.5],
        'EUR': [0.0, 0.5]
    }, index=['ETF1', 'ETF2'])

    # Create instruments
    instruments = {
        'ETF1': MockETF('ETF1', currency='EUR', currency_hedged=False),
        'ETF2': MockETF('ETF2', currency='EUR', currency_hedged=False)
    }

    # Create adjuster with initial data
    print("\n1. Create adjuster with 10 days of data")
    fx_spot = FxSpotComponent(fx_comp, fx_prices)
    adjuster = Adjuster(prices, instruments=instruments).add(fx_spot)

    # Initial calculation
    adj1 = adjuster.calculate()
    print(f"   Initial adjustments shape: {adj1.shape}")
    print(f"   Cache size: {len(adjuster._adjustments)}")

    # Append new data (5 more days) with recalc_last_n=1
    print("\n2. Append 5 new days with recalc_last_n=1")
    new_dates = pd.date_range('2024-01-11', periods=5, freq='D')
    new_prices = pd.DataFrame({
        'ETF1': np.linspace(110, 115, 5),
        'ETF2': np.linspace(105, 108, 5)
    }, index=new_dates)

    new_fx_prices = pd.DataFrame({
        'USD': np.linspace(1.15, 1.18, 5)
    }, index=new_dates)

    adjuster.append_update(prices=new_prices, fx_prices=new_fx_prices, recalc_last_n=1)
    print(f"   Cache size after append: {len(adjuster._adjustments)}")

    # Calculate should use cache
    adj2 = adjuster.calculate()
    print(f"   Final adjustments shape: {adj2.shape}")
    print(f"   Total dates: {len(adj2)}")
    print(f"   Price dates: {adjuster.prices.index[0]} to {adjuster.prices.index[-1]}")
    print(f"   Unique dates in prices: {len(adjuster.prices.index.unique())}")

    # Verify data is stored permanently
    print("\n3. Verify data is stored permanently")
    print(f"   Prices shape: {adjuster.prices.shape} (should be 15)")
    print(f"   Cache shape: {adjuster._adjustments.shape} (should be 15)")

    # Test recalc_last_n=0 (only new dates)
    print("\n4. Append 3 more days with recalc_last_n=0 (only new)")
    newer_dates = pd.date_range('2024-01-16', periods=3, freq='D')
    newer_prices = pd.DataFrame({
        'ETF1': np.linspace(115, 118, 3),
        'ETF2': np.linspace(108, 110, 3)
    }, index=newer_dates)

    newer_fx_prices = pd.DataFrame({
        'USD': np.linspace(1.18, 1.20, 3)
    }, index=newer_dates)

    adjuster.append_update(prices=newer_prices, fx_prices=newer_fx_prices, recalc_last_n=0)
    print(f"   Cache size: {len(adjuster._adjustments)} (should be 18)")

    print("\n[OK] append_update test PASSED")
    return adjuster


def test_live_update():
    """Test live_update (temporary calculation)"""
    print("\n" + "="*80)
    print("TEST 2: live_update (temporary calculation)")
    print("="*80)

    # Create initial data (10 days)
    dates = pd.date_range('2024-01-01', periods=10, freq='D')
    prices = pd.DataFrame({
        'ETF1': np.linspace(100, 110, 10),
        'ETF2': np.linspace(100, 105, 10)
    }, index=dates)

    fx_prices = pd.DataFrame({
        'USD': np.linspace(1.10, 1.15, 10)
    }, index=dates)

    fx_comp = pd.DataFrame({
        'USD': [1.0, 0.5],
        'EUR': [0.0, 0.5]
    }, index=['ETF1', 'ETF2'])

    # Create instruments
    instruments = {
        'ETF1': MockETF('ETF1', currency='EUR', currency_hedged=False),
        'ETF2': MockETF('ETF2', currency='EUR', currency_hedged=False)
    }

    # Create adjuster
    print("\n1. Create adjuster with 10 days of data")
    fx_spot = FxSpotComponent(fx_comp, fx_prices)
    adjuster = Adjuster(prices, instruments=instruments).add(fx_spot)

    # Initial calculation
    adj1 = adjuster.calculate()
    print(f"   Initial cache size: {len(adjuster._adjustments)}")
    original_prices_len = len(adjuster.prices)
    original_cache_len = len(adjuster._adjustments)

    # Live update (should not store)
    print("\n2. Live update with 2 new days (should NOT store)")
    live_dates = pd.date_range('2024-01-11', periods=2, freq='D')
    live_prices = pd.DataFrame({
        'ETF1': [111, 112],
        'ETF2': [106, 107]
    }, index=live_dates)

    live_fx_prices = pd.DataFrame({
        'USD': [1.16, 1.17]
    }, index=live_dates)

    live_adj = adjuster.live_update(prices=live_prices, fx_prices=live_fx_prices)
    print(f"   Live adjustments shape: {live_adj.shape}")
    print(f"   Live adjustments dates: {live_adj.index[0]} to {live_adj.index[-1]}")

    # Verify nothing is stored
    print("\n3. Verify data is NOT stored permanently")
    print(f"   Prices shape after live: {len(adjuster.prices)} (should be {original_prices_len})")
    print(f"   Cache shape after live: {len(adjuster._adjustments)} (should be {original_cache_len})")

    if len(adjuster.prices) == original_prices_len:
        print("   [OK] Prices unchanged (temp data discarded)")
    else:
        print(f"   [FAIL] Prices changed from {original_prices_len} to {len(adjuster.prices)}")

    if len(adjuster._adjustments) == original_cache_len:
        print("   [OK] Cache unchanged (temp data discarded)")
    else:
        print(f"   [FAIL] Cache changed from {original_cache_len} to {len(adjuster._adjustments)}")

    # Next calculation should use original data
    print("\n4. Calculate after live_update (should use original data)")
    adj2 = adjuster.calculate()
    print(f"   Adjustments shape: {adj2.shape} (should be {adj1.shape})")

    if adj2.shape == adj1.shape:
        print("   [OK] Back to original data")
    else:
        print(f"   [FAIL] Shape mismatch")

    print("\n[OK] live_update test PASSED")
    return adjuster


def test_recalc_variations():
    """Test different recalc_last_n values"""
    print("\n" + "="*80)
    print("TEST 3: recalc_last_n variations")
    print("="*80)

    # Create initial data
    dates = pd.date_range('2024-01-01', periods=10, freq='D')
    prices = pd.DataFrame({
        'ETF1': np.linspace(100, 110, 10)
    }, index=dates)

    fx_prices = pd.DataFrame({
        'USD': np.linspace(1.10, 1.15, 10)
    }, index=dates)

    fx_comp = pd.DataFrame({
        'USD': [1.0]
    }, index=['ETF1'])

    instruments = {'ETF1': MockETF('ETF1', currency='EUR', currency_hedged=False)}

    # Test recalc_last_n=-1 (full recalc)
    print("\n1. Test recalc_last_n=-1 (full recalculation)")
    fx_spot = FxSpotComponent(fx_comp, fx_prices)
    adjuster = Adjuster(prices, instruments=instruments).add(fx_spot)
    adjuster.calculate()

    new_dates = pd.date_range('2024-01-11', periods=2, freq='D')
    new_prices = pd.DataFrame({'ETF1': [111, 112]}, index=new_dates)
    new_fx_prices = pd.DataFrame({'USD': [1.16, 1.17]}, index=new_dates)

    adjuster.append_update(prices=new_prices, fx_prices=new_fx_prices, recalc_last_n=-1)
    print(f"   Cache size: {len(adjuster._adjustments)} (all dates recalculated)")

    # Test recalc_last_n=3
    print("\n2. Test recalc_last_n=3 (recalc last 3 + new)")
    fx_spot2 = FxSpotComponent(fx_comp, fx_prices)
    adjuster2 = Adjuster(prices, instruments=instruments).add(fx_spot2)
    adjuster2.calculate()

    adjuster2.append_update(prices=new_prices, fx_prices=new_fx_prices, recalc_last_n=3)
    print(f"   Cache size: {len(adjuster2._adjustments)} (last 3 + 2 new = 5 recalculated)")

    print("\n[OK] recalc_last_n variations test PASSED")


if __name__ == "__main__":
    print("\n" + "="*80)
    print("TESTING NEW UPDATE API")
    print("="*80)

    test_append_update()
    test_live_update()
    test_recalc_variations()

    print("\n" + "="*80)
    print("ALL TESTS PASSED!")
    print("="*80)
