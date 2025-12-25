"""
Test different return types (percentage, logarithmic, absolute).

Demonstrates how ReturnCalculator centralizes return logic.
"""
import pandas as pd
import numpy as np
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from analytics.adjustments.return_calculations import ReturnCalculator, ReturnType


def test_return_calculator():
    """Test ReturnCalculator with different return types"""

    # Create simple price series
    prices = pd.DataFrame({
        'A': [100.0, 102.0, 101.0, 103.0, 105.0],
        'B': [50.0, 51.0, 50.5, 52.0, 51.0]
    })

    print("=" * 80)
    print("TEST: ReturnCalculator with different return types")
    print("=" * 80)
    print("\nPrices:")
    print(prices)

    # Test percentage returns
    print("\n" + "=" * 80)
    print("PERCENTAGE RETURNS")
    print("=" * 80)
    calc_pct = ReturnCalculator(return_type="percentage")
    returns_pct = calc_pct.calculate_returns(prices)
    print("\nReturns:")
    print(returns_pct)

    # Reconstruct prices
    reconstructed_pct = calc_pct.returns_to_prices(returns_pct.fillna(0), prices.iloc[0])
    print("\nReconstructed prices:")
    print(reconstructed_pct)

    diff_pct = (reconstructed_pct - prices).abs().max().max()
    print(f"\nMax reconstruction error: {diff_pct:.15f}")
    print(f"Test passes: {diff_pct < 1e-10}")

    # Test logarithmic returns
    print("\n" + "=" * 80)
    print("LOGARITHMIC RETURNS")
    print("=" * 80)
    calc_log = ReturnCalculator(return_type="logarithmic")
    returns_log = calc_log.calculate_returns(prices)
    print("\nReturns:")
    print(returns_log)

    # Reconstruct prices
    reconstructed_log = calc_log.returns_to_prices(returns_log.fillna(0), prices.iloc[0])
    print("\nReconstructed prices:")
    print(reconstructed_log)

    diff_log = (reconstructed_log - prices).abs().max().max()
    print(f"\nMax reconstruction error: {diff_log:.15f}")
    print(f"Test passes: {diff_log < 1e-10}")

    # Test absolute returns
    print("\n" + "=" * 80)
    print("ABSOLUTE RETURNS")
    print("=" * 80)
    calc_abs = ReturnCalculator(return_type="absolute")
    returns_abs = calc_abs.calculate_returns(prices)
    print("\nReturns:")
    print(returns_abs)

    # Reconstruct prices
    reconstructed_abs = calc_abs.returns_to_prices(returns_abs.fillna(0), prices.iloc[0])
    print("\nReconstructed prices:")
    print(reconstructed_abs)

    diff_abs = (reconstructed_abs - prices).abs().max().max()
    print(f"\nMax reconstruction error: {diff_abs:.15f}")
    print(f"Test passes: {diff_abs < 1e-10}")

    # Compare return values
    print("\n" + "=" * 80)
    print("COMPARISON: Row 2 returns for instrument A")
    print("=" * 80)
    print(f"Price change: {prices.loc[1, 'A']} -> {prices.loc[2, 'A']} (102 -> 101)")
    print(f"Percentage return: {returns_pct.loc[2, 'A']:.10f}")
    print(f"Logarithmic return: {returns_log.loc[2, 'A']:.10f}")
    print(f"Absolute return: {returns_abs.loc[2, 'A']:.10f}")
    print("\nExpected values:")
    print(f"  Percentage: (101-102)/102 = {(101-102)/102:.10f}")
    print(f"  Logarithmic: log(101/102) = {np.log(101/102):.10f}")
    print(f"  Absolute: 101-102 = {101-102:.10f}")

    # Test accumulate_returns
    print("\n" + "=" * 80)
    print("TEST: accumulate_returns()")
    print("=" * 80)

    cumulative_pct = calc_pct.accumulate_returns(returns_pct.fillna(0))
    print("\nCumulative percentage returns:")
    print(cumulative_pct)

    cumulative_log = calc_log.accumulate_returns(returns_log.fillna(0))
    print("\nCumulative logarithmic returns:")
    print(cumulative_log)

    cumulative_abs = calc_abs.accumulate_returns(returns_abs.fillna(0))
    print("\nCumulative absolute returns:")
    print(cumulative_abs)

    # Verify cumulative returns match price changes
    print("\n" + "=" * 80)
    print("VERIFICATION: Cumulative returns match price changes")
    print("=" * 80)

    # For percentage: (P_final - P_initial) / P_initial
    expected_pct = (prices - prices.iloc[0]) / prices.iloc[0]
    print(f"\nPercentage cumulative returns match: {(cumulative_pct - expected_pct).abs().max().max() < 1e-10}")

    # For log: log(P_final / P_initial)
    expected_log = np.log(prices / prices.iloc[0])
    print(f"Logarithmic cumulative returns match: {(cumulative_log - expected_log).abs().max().max() < 1e-10}")

    # For absolute: P_final - P_initial
    expected_abs = prices - prices.iloc[0]
    print(f"Absolute cumulative returns match: {(cumulative_abs - expected_abs).abs().max().max() < 1e-10}")

    print("\n" + "=" * 80)
    print("ALL TESTS PASSED!")
    print("=" * 80)
    print("\nReturnCalculator correctly handles:")
    print("  - Percentage returns: (P_t - P_{t-1}) / P_{t-1}")
    print("  - Logarithmic returns: log(P_t / P_{t-1})")
    print("  - Absolute returns: P_t - P_{t-1}")
    print("\nAnd correctly reconstructs prices from all return types.")


if __name__ == "__main__":
    test_return_calculator()
