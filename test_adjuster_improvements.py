"""
Test script for Adjuster improvements:
1. Intraday support with normalize option
2. FX ticker normalization (EURUSD → USD)
3. datetime handling throughout the pipeline
"""
import pandas as pd
from datetime import datetime, date
import numpy as np

# Test imports
try:
    from src.analytics.adjustments.adjuster import Adjuster
    from src.analytics.adjustments.ter import TerComponent
    from src.analytics.adjustments.fx_spot import FxSpotComponent
    print("✓ Imports successful")
except ImportError as e:
    print(f"✗ Import failed: {e}")
    exit(1)


def test_fx_normalization():
    """Test that FX columns are normalized from tickers to currency codes."""
    print("\n" + "="*60)
    print("TEST 1: FX Column Normalization")
    print("="*60)
    
    # Create test data with FX tickers (EURUSD, EURGBP)
    dates = pd.date_range('2024-01-01', periods=5, freq='D')
    
    prices = pd.DataFrame({
        'IWDA LN': [100, 101, 102, 101, 103],
        'VWRL LN': [50, 51, 50.5, 51.5, 52],
    }, index=dates)
    
    # FX prices with ticker format (EURUSD, EURGBP)
    fx_prices = pd.DataFrame({
        'EURUSD': [1.10, 1.11, 1.12, 1.11, 1.13],
        'EURGBP': [0.85, 0.86, 0.85, 0.87, 0.86],
        'EURJPY': [160, 161, 162, 161, 163],
    }, index=dates)
    
    # Create adjuster
    adj = Adjuster(prices, fx_prices, intraday=False)
    
    # Check that columns were normalized
    print(f"Original FX columns: {list(fx_prices.columns)}")
    print(f"Normalized FX columns: {list(adj.fx_prices.columns)}")
    
    expected = ['USD', 'GBP', 'JPY']
    assert list(adj.fx_prices.columns) == expected, f"Expected {expected}, got {list(adj.fx_prices.columns)}"
    print("✓ FX columns correctly normalized")


def test_intraday_false():
    """Test that intraday=False normalizes timestamps to dates."""
    print("\n" + "="*60)
    print("TEST 2: Intraday=False (Date Normalization)")
    print("="*60)
    
    # Create data with intraday timestamps
    timestamps = pd.to_datetime([
        '2024-01-01 09:00:00',
        '2024-01-01 14:30:00',
        '2024-01-02 10:15:00',
        '2024-01-02 16:00:00',
    ])
    
    prices = pd.DataFrame({
        'IWDA LN': [100, 100.5, 101, 101.5],
        'VWRL LN': [50, 50.2, 50.5, 50.8],
    }, index=timestamps)
    
    fx_prices = pd.DataFrame({
        'USD': [1.10, 1.105, 1.11, 1.115],
    }, index=timestamps)
    
    # Create adjuster with intraday=False
    adj = Adjuster(prices, fx_prices, intraday=False)
    
    # Check that index is normalized to dates
    print(f"Original index: {timestamps.tolist()[:2]}...")
    print(f"Normalized index: {adj.prices.index.tolist()[:2]}...")
    
    assert isinstance(adj.prices.index[0], pd.Timestamp)
    assert adj.prices.index[0].hour == 0
    assert adj.prices.index[0].minute == 0
    print("✓ Timestamps correctly normalized to dates")


def test_intraday_true():
    """Test that intraday=True preserves timestamps."""
    print("\n" + "="*60)
    print("TEST 3: Intraday=True (Timestamp Preservation)")
    print("="*60)
    
    # Create data with intraday timestamps
    timestamps = pd.to_datetime([
        '2024-01-01 09:00:00',
        '2024-01-01 14:30:00',
        '2024-01-02 10:15:00',
        '2024-01-02 16:00:00',
    ])
    
    prices = pd.DataFrame({
        'IWDA LN': [100, 100.5, 101, 101.5],
        'VWRL LN': [50, 50.2, 50.5, 50.8],
    }, index=timestamps)
    
    fx_prices = pd.DataFrame({
        'USD': [1.10, 1.105, 1.11, 1.115],
    }, index=timestamps)
    
    # Create adjuster with intraday=True
    adj = Adjuster(prices, fx_prices, intraday=True)
    
    # Check that timestamps are preserved
    print(f"Original timestamps: {timestamps.tolist()[:2]}...")
    print(f"Preserved timestamps: {adj.prices.index.tolist()[:2]}...")
    
    assert adj.prices.index[0].hour == 9
    assert adj.prices.index[1].hour == 14
    assert adj.prices.index[1].minute == 30
    print("✓ Timestamps correctly preserved")


def test_calculate_with_datetime():
    """Test that calculate() works with datetime objects."""
    print("\n" + "="*60)
    print("TEST 4: Calculate with Datetime Objects")
    print("="*60)
    
    dates = pd.date_range('2024-01-01', periods=10, freq='D')
    
    prices = pd.DataFrame({
        'IWDA LN': np.random.uniform(100, 110, 10),
        'VWRL LN': np.random.uniform(50, 55, 10),
    }, index=dates)
    
    fx_prices = pd.DataFrame({
        'USD': np.random.uniform(1.08, 1.12, 10),
    }, index=dates)
    
    # Test with intraday=True
    adj_intraday = Adjuster(prices, fx_prices, intraday=True)
    
    # Add a component
    ters = {'IWDA LN': 0.0020, 'VWRL LN': 0.0022}
    adj_intraday.add(TerComponent(ters))
    
    # Calculate with datetime subset
    calc_dates = dates[2:5].to_pydatetime().tolist()
    result = adj_intraday.calculate(calc_dates)
    
    print(f"Calculation dates: {len(calc_dates)}")
    print(f"Result shape: {result.shape}")
    assert result.shape[0] == 3, "Expected 3 rows"
    print("✓ Calculate works with datetime objects")
    
    # Test with intraday=False
    adj_daily = Adjuster(prices, fx_prices, intraday=False)
    adj_daily.add(TerComponent(ters))
    
    # Calculate with date subset
    calc_dates_as_dates = [d.date() for d in dates[2:5]]
    result_daily = adj_daily.calculate(calc_dates_as_dates)
    
    print(f"Calculation dates (as date): {len(calc_dates_as_dates)}")
    print(f"Result shape: {result_daily.shape}")
    assert result_daily.shape[0] == 3, "Expected 3 rows"
    print("✓ Calculate works with date objects")


def test_fx_spot_with_normalized_fx():
    """Test that FxSpotComponent works with normalized FX columns."""
    print("\n" + "="*60)
    print("TEST 5: FxSpotComponent with Normalized FX")
    print("="*60)
    
    dates = pd.date_range('2024-01-01', periods=5, freq='D')
    
    prices = pd.DataFrame({
        'IWDA LN': [100, 101, 102, 101, 103],
        'VWRL LN': [50, 51, 50.5, 51.5, 52],
    }, index=dates)
    
    # FX prices with ticker format
    fx_prices = pd.DataFrame({
        'EURUSD': [1.10, 1.11, 1.12, 1.11, 1.13],
        'EURGBP': [0.85, 0.86, 0.85, 0.87, 0.86],
    }, index=dates)
    
    # FX composition with currency codes (USD, GBP)
    fx_composition = pd.DataFrame({
        'USD': [0.65, 0.60],
        'GBP': [0.10, 0.15],
    }, index=['IWDA LN', 'VWRL LN'])
    
    # Create mock instruments
    from unittest.mock import MagicMock
    instruments = {}
    for inst_id in ['IWDA LN', 'VWRL LN']:
        inst = MagicMock()
        inst.id = inst_id
        inst.type = MagicMock()
        inst.type.name = 'ETP'
        inst.currency = 'EUR'
        inst.currency_hedged = False
        instruments[inst_id] = inst
    
    # Create adjuster
    adj = Adjuster(prices, fx_prices, instruments=instruments, intraday=False)
    
    # Add FX spot component
    adj.add(FxSpotComponent(fx_composition))
    
    # Calculate
    result = adj.calculate()
    
    print(f"Result shape: {result.shape}")
    print(f"Non-zero adjustments: {(result != 0).sum().sum()}")
    print("✓ FxSpotComponent works with normalized FX columns")


def main():
    """Run all tests."""
    print("\n" + "="*60)
    print("ADJUSTER IMPROVEMENTS - TEST SUITE")
    print("="*60)
    
    try:
        test_fx_normalization()
        test_intraday_false()
        test_intraday_true()
        test_calculate_with_datetime()
        test_fx_spot_with_normalized_fx()
        
        print("\n" + "="*60)
        print("✓ ALL TESTS PASSED")
        print("="*60)
        
    except AssertionError as e:
        print(f"\n✗ TEST FAILED: {e}")
        raise
    except Exception as e:
        print(f"\n✗ ERROR: {e}")
        import traceback
        traceback.print_exc()
        raise


if __name__ == "__main__":
    main()
