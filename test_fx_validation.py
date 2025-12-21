"""
Test FX price validation and inversion logic.

Tests that FX prices in various formats are correctly handled:
1. EURUSD format (correct, no change)
2. USDEUR format (inverted, should be corrected with 1/price)
3. USD format (ambiguous, warning but assumed EURUSD)
"""
import pandas as pd
import numpy as np
from datetime import datetime
import sys
sys.path.insert(0, 'src')

from analytics.adjustments.adjuster import Adjuster


def test_fx_eurusd_format():
    """
    Test that EURUSD format is handled correctly (no inversion).
    """
    print("\n" + "="*70)
    print("TEST 1: EURUSD Format (Correct)")
    print("="*70)
    
    dates = pd.date_range('2024-01-01', periods=5, freq='D')
    
    prices = pd.DataFrame({
        'IWDA LN': [100, 101, 102, 101, 103],
    }, index=dates)
    
    # Correct format: EURUSD
    fx_prices = pd.DataFrame({
        'EURUSD': [1.10, 1.11, 1.12, 1.11, 1.13],
        'EURGBP': [0.85, 0.86, 0.85, 0.87, 0.86],
    }, index=dates)
    
    adj = Adjuster(prices, fx_prices, intraday=False)
    
    print(f"\nOriginal FX columns: {list(fx_prices.columns)}")
    print(f"Normalized FX columns: {list(adj.fx_prices.columns)}")
    print(f"\nOriginal EURUSD values: {fx_prices['EURUSD'].values[:3]}")
    print(f"Normalized USD values: {adj.fx_prices['USD'].values[:3]}")
    
    # Values should be unchanged
    assert list(adj.fx_prices.columns) == ['USD', 'GBP'], "Expected ['USD', 'GBP']"
    assert np.allclose(fx_prices['EURUSD'].values, adj.fx_prices['USD'].values), \
        "USD values should be unchanged"
    
    print("\n✓ EURUSD format handled correctly (no inversion)")


def test_fx_usdeur_format():
    """
    Test that USDEUR format is detected and inverted.
    """
    print("\n" + "="*70)
    print("TEST 2: USDEUR Format (Inverted)")
    print("="*70)
    
    dates = pd.date_range('2024-01-01', periods=5, freq='D')
    
    prices = pd.DataFrame({
        'IWDA LN': [100, 101, 102, 101, 103],
    }, index=dates)
    
    # Inverted format: USDEUR (should be inverted to get EURUSD)
    fx_prices = pd.DataFrame({
        'USDEUR': [0.90, 0.91, 0.89, 0.90, 0.88],  # 1/USDEUR ≈ 1.11, 1.10, ...
        'GBPEUR': [1.18, 1.17, 1.18, 1.15, 1.16],  # 1/GBPEUR ≈ 0.85, 0.85, ...
    }, index=dates)
    
    adj = Adjuster(prices, fx_prices, intraday=False)
    
    print(f"\nOriginal FX columns: {list(fx_prices.columns)}")
    print(f"Normalized FX columns: {list(adj.fx_prices.columns)}")
    print(f"\nOriginal USDEUR values: {fx_prices['USDEUR'].values[:3]}")
    print(f"Inverted USD values (1/USDEUR): {adj.fx_prices['USD'].values[:3]}")
    print(f"Expected values (1/[0.90, 0.91, 0.89]): {1.0/fx_prices['USDEUR'].values[:3]}")
    
    # Values should be inverted
    assert list(adj.fx_prices.columns) == ['USD', 'GBP'], "Expected ['USD', 'GBP']"
    
    expected_usd = 1.0 / fx_prices['USDEUR'].values
    assert np.allclose(adj.fx_prices['USD'].values, expected_usd), \
        f"USD values should be 1/USDEUR. Got {adj.fx_prices['USD'].values[:3]}, expected {expected_usd[:3]}"
    
    expected_gbp = 1.0 / fx_prices['GBPEUR'].values
    assert np.allclose(adj.fx_prices['GBP'].values, expected_gbp), \
        "GBP values should be 1/GBPEUR"
    
    print("\n✓ USDEUR format correctly inverted")


def test_fx_currency_code_format():
    """
    Test that currency code format (USD) is handled with warning.
    """
    print("\n" + "="*70)
    print("TEST 3: Currency Code Format (USD)")
    print("="*70)
    
    dates = pd.date_range('2024-01-01', periods=5, freq='D')
    
    prices = pd.DataFrame({
        'IWDA LN': [100, 101, 102, 101, 103],
    }, index=dates)
    
    # Ambiguous format: just currency codes
    fx_prices = pd.DataFrame({
        'USD': [1.10, 1.11, 1.12, 1.11, 1.13],
        'GBP': [0.85, 0.86, 0.85, 0.87, 0.86],
    }, index=dates)
    
    adj = Adjuster(prices, fx_prices, intraday=False)
    
    print(f"\nOriginal FX columns: {list(fx_prices.columns)}")
    print(f"Normalized FX columns: {list(adj.fx_prices.columns)}")
    print(f"\nOriginal USD values: {fx_prices['USD'].values[:3]}")
    print(f"Normalized USD values: {adj.fx_prices['USD'].values[:3]}")
    
    # Values should be unchanged (assumes EURUSD)
    assert list(adj.fx_prices.columns) == ['USD', 'GBP'], "Expected ['USD', 'GBP']"
    assert np.allclose(fx_prices['USD'].values, adj.fx_prices['USD'].values), \
        "USD values should be unchanged (assumed EURUSD)"
    
    print("\n✓ Currency code format handled with warning (assumed EURUSD)")


def test_fx_mixed_formats():
    """
    Test handling of mixed FX formats in same DataFrame.
    """
    print("\n" + "="*70)
    print("TEST 4: Mixed FX Formats")
    print("="*70)
    
    dates = pd.date_range('2024-01-01', periods=5, freq='D')
    
    prices = pd.DataFrame({
        'IWDA LN': [100, 101, 102, 101, 103],
    }, index=dates)
    
    # Mixed formats
    fx_prices = pd.DataFrame({
        'EURUSD': [1.10, 1.11, 1.12, 1.11, 1.13],  # Correct
        'GBPEUR': [1.18, 1.17, 1.18, 1.15, 1.16],  # Inverted
        'JPY': [160, 161, 162, 161, 163],          # Ambiguous (assumed EURJPY)
    }, index=dates)
    
    adj = Adjuster(prices, fx_prices, intraday=False)
    
    print(f"\nOriginal FX columns: {list(fx_prices.columns)}")
    print(f"Normalized FX columns: {list(adj.fx_prices.columns)}")
    print(f"\nEURUSD (unchanged): {adj.fx_prices['USD'].values[0]:.4f}")
    print(f"GBPEUR (inverted): {adj.fx_prices['GBP'].values[0]:.4f} (was {fx_prices['GBPEUR'].values[0]:.4f})")
    print(f"JPY (unchanged, warning): {adj.fx_prices['JPY'].values[0]:.4f}")
    
    # Check normalization
    assert 'USD' in adj.fx_prices.columns, "Expected USD column"
    assert 'GBP' in adj.fx_prices.columns, "Expected GBP column"
    assert 'JPY' in adj.fx_prices.columns, "Expected JPY column"
    
    # EURUSD should be unchanged
    assert np.allclose(fx_prices['EURUSD'].values, adj.fx_prices['USD'].values)
    
    # GBPEUR should be inverted
    expected_gbp = 1.0 / fx_prices['GBPEUR'].values
    assert np.allclose(adj.fx_prices['GBP'].values, expected_gbp)
    
    # JPY should be unchanged (warning issued)
    assert np.allclose(fx_prices['JPY'].values, adj.fx_prices['JPY'].values)
    
    print("\n✓ Mixed formats handled correctly")


def test_fx_zero_handling():
    """
    Test that zero FX prices are handled (converted to NaN after inversion).
    """
    print("\n" + "="*70)
    print("TEST 5: Zero FX Price Handling")
    print("="*70)
    
    dates = pd.date_range('2024-01-01', periods=5, freq='D')
    
    prices = pd.DataFrame({
        'IWDA LN': [100, 101, 102, 101, 103],
    }, index=dates)
    
    # FX prices with a zero (invalid)
    fx_prices = pd.DataFrame({
        'USDEUR': [0.90, 0.0, 0.89, 0.90, 0.88],  # Zero on second date
    }, index=dates)
    
    adj = Adjuster(prices, fx_prices, intraday=False)
    
    print(f"\nOriginal USDEUR values: {fx_prices['USDEUR'].values}")
    print(f"Inverted USD values: {adj.fx_prices['USD'].values}")
    print(f"Note: 1/0.0 should be NaN, not inf")
    
    # Check that zero was converted to NaN (not inf)
    assert pd.isna(adj.fx_prices['USD'].values[1]), "Zero should be converted to NaN"
    assert not np.isinf(adj.fx_prices['USD'].values[1]), "Should not be inf"
    
    # Other values should be inverted correctly
    assert np.isclose(adj.fx_prices['USD'].values[0], 1.0/0.90)
    assert np.isclose(adj.fx_prices['USD'].values[2], 1.0/0.89)
    
    print("\n✓ Zero FX prices handled correctly (converted to NaN)")


def main():
    """Run all FX validation tests."""
    print("\n" + "="*70)
    print("FX PRICE VALIDATION & INVERSION TESTS")
    print("="*70)
    
    try:
        test_fx_eurusd_format()
        test_fx_usdeur_format()
        test_fx_currency_code_format()
        test_fx_mixed_formats()
        test_fx_zero_handling()
        
        print("\n" + "="*70)
        print("✓ ALL FX VALIDATION TESTS PASSED")
        print("="*70)
        
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
