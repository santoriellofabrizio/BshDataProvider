"""
Test script for Dividend Component intraday logic.

Tests that dividend adjustments are correctly applied to period returns
containing the ex-dividend datetime.
"""
import pandas as pd
import numpy as np
from datetime import datetime
from unittest.mock import MagicMock

# Add src to path
import sys
sys.path.insert(0, 'src')

from analytics.adjustments.dividend import DividendComponent
from analytics.adjustments.adjuster import Adjuster


def create_mock_instrument(inst_id: str, market_suffix: str = 'US'):
    """Create a mock instrument for testing."""
    inst = MagicMock()
    inst.id = f"{inst_id} {market_suffix}"
    inst.type = MagicMock()
    inst.type.name = 'STOCK'
    inst.currency = 'USD'
    inst.fund_currency = 'USD'
    inst.primary_exchange = None
    return inst


def test_intraday_dividend_period_returns():
    """
    Test that dividend adjustment is applied when date changes.
    
    Scenario:
    - SPY pays $1.50 dividend on 2024-01-15
    - Price drops from $451.50 to $450.00 overnight
    - Adjustment applied to period crossing midnight into 15-01
    """
    print("\n" + "="*70)
    print("TEST: Intraday Dividend Adjustment (Date Change)")
    print("="*70)
    
    # Create intraday timestamps
    timestamps = pd.to_datetime([
        '2024-01-12 14:00',  # Friday afternoon
        '2024-01-12 16:00',  # Friday close
        '2024-01-14 14:00',  # Monday afternoon (no trading on weekend)
        '2024-01-14 16:00',  # Monday close (last timestamp before div)
        '2024-01-15 09:30',  # Tuesday open (FIRST timestamp with new date)
        '2024-01-15 14:00',  # Tuesday afternoon
        '2024-01-15 16:00',  # Tuesday close
    ])
    
    # Prices: drop overnight into ex-div date
    prices = pd.DataFrame({
        'SPY US': [450.00, 450.50, 451.00, 451.50, 450.00, 450.50, 451.00],
    }, index=timestamps)
    
    # FX prices (all USD)
    fx_prices = pd.DataFrame({
        'USD': [1.10] * len(timestamps),
    }, index=timestamps)
    
    # Dividend: $1.50 on 2024-01-15
    dividends = pd.DataFrame({
        'SPY US': [0, 0, 0, 0, 1.50, 0, 0],
    }, index=timestamps.normalize().unique())
    
    # Create instruments
    instruments = {
        'SPY US': create_mock_instrument('SPY', 'US')
    }
    
    # Create component
    div_component = DividendComponent(dividends)
    
    # Calculate adjustments
    adjustments = div_component.calculate_batch(
        instruments=instruments,
        dates=timestamps.tolist(),
        prices=prices,
        fx_prices=fx_prices
    )
    
    print("\nTimestamps and Adjustments:")
    print("-" * 70)
    for i, ts in enumerate(timestamps):
        adj = adjustments.loc[ts, 'SPY US']
        if i > 0:
            prev_ts = timestamps[i-1]
            date_change = "DATE CHANGE" if prev_ts.date() != ts.date() else "same date"
            period_label = f"{prev_ts.strftime('%m-%d %H:%M')} → {ts.strftime('%m-%d %H:%M')}"
            marker = " ← DIVIDEND ADJUSTMENT" if adj != 0 else ""
            print(f"{period_label:40} ({date_change:11})  Adj: {adj:+.6f}{marker}")
    
    # Verify adjustment is ONLY at first timestamp of new date (09:30 on 15th)
    print("\n" + "="*70)
    print("Verification:")
    print("="*70)
    
    # Expected: adjustment only at 09:30 (index 4, first timestamp on 15-01)
    expected_idx = 4
    expected_adjustment = 1.50 / 451.50  # div / price_at_t1 (14-01 16:00)
    
    non_zero_adjustments = adjustments[adjustments['SPY US'] != 0]
    
    print(f"Non-zero adjustments: {len(non_zero_adjustments)}")
    print(f"Expected: 1 adjustment at {timestamps[expected_idx]}")
    print(f"Actual adjustment: {adjustments.loc[timestamps[expected_idx], 'SPY US']:.6f}")
    print(f"Expected adjustment: {expected_adjustment:.6f}")
    
    # Assertions
    assert len(non_zero_adjustments) == 1, f"Expected 1 adjustment, got {len(non_zero_adjustments)}"
    
    actual_adj = adjustments.loc[timestamps[expected_idx], 'SPY US']
    assert abs(actual_adj - expected_adjustment) < 1e-6, \
        f"Adjustment mismatch: {actual_adj:.6f} vs {expected_adjustment:.6f}"
    
    print("\n✓ Test PASSED: Adjustment correctly applied on date change")
    
    # Now test period returns
    print("\n" + "="*70)
    print("Period Returns (with adjustment):")
    print("="*70)
    
    raw_returns = prices['SPY US'].pct_change()
    clean_returns = raw_returns + adjustments['SPY US']
    
    for i in range(1, len(timestamps)):
        t1 = timestamps[i-1]
        t2 = timestamps[i]
        raw_ret = raw_returns.loc[t2]
        clean_ret = clean_returns.loc[t2]
        adj = adjustments.loc[t2, 'SPY US']
        
        period_label = f"{t1.strftime('%m-%d %H:%M')} → {t2.strftime('%m-%d %H:%M')}"
        print(f"{period_label:40} Raw: {raw_ret:+.4%}  Adj: {adj:+.6f}  Clean: {clean_ret:+.4%}")
    
    # The period crossing midnight should have clean return near zero
    date_change_clean_return = clean_returns.loc[timestamps[expected_idx]]
    print(f"\nDate-change period clean return: {date_change_clean_return:.6f} (should be near 0)")
    assert abs(date_change_clean_return) < 0.01, "Date-change period should have ~0% clean return"
    
    print("\n✓ Date-change period correctly neutralized")


def test_daily_mode_unchanged():
    """
    Test that daily mode still works as before.
    """
    print("\n" + "="*70)
    print("TEST: Daily Mode (Backward Compatibility)")
    print("="*70)
    
    # Daily dates (normalized)
    dates = pd.date_range('2024-01-12', periods=5, freq='D')
    
    prices = pd.DataFrame({
        'SPY US': [450, 451, 452, 450.50, 451.50],
    }, index=dates)
    
    fx_prices = pd.DataFrame({
        'USD': [1.10] * len(dates),
    }, index=dates)
    
    # Dividend on 2024-01-15
    dividends = pd.DataFrame({
        'SPY US': [0, 0, 0, 1.50, 0],
    }, index=dates)
    
    instruments = {'SPY US': create_mock_instrument('SPY', 'US')}
    
    div_component = DividendComponent(dividends)
    
    adjustments = div_component.calculate_batch(
        instruments=instruments,
        dates=dates.tolist(),
        prices=prices,
        fx_prices=fx_prices
    )
    
    print("\nDaily Adjustments:")
    for dt, adj in adjustments['SPY US'].items():
        marker = " ← DIVIDEND" if adj != 0 else ""
        print(f"{dt.strftime('%Y-%m-%d'):20} {adj:+.6f}{marker}")
    
    # Should have adjustment on 2024-01-15
    assert adjustments.loc[dates[3], 'SPY US'] > 0, "Expected adjustment on ex-div date"
    assert (adjustments['SPY US'] != 0).sum() == 1, "Expected exactly 1 adjustment"
    
    print("\n✓ Daily mode works as expected")


def test_multiple_dividends():
    """
    Test handling of multiple dividend events in intraday data.
    """
    print("\n" + "="*70)
    print("TEST: Multiple Dividends (Intraday)")
    print("="*70)
    
    timestamps = pd.to_datetime([
        '2024-01-12 14:00',
        '2024-01-15 09:30',  # Date change to 15-01 (SPY dividend)
        '2024-01-15 16:00',
        '2024-01-20 08:00',  # Date change to 20-01 (VWRL dividend)
        '2024-01-20 16:00',
    ])
    
    prices = pd.DataFrame({
        'SPY US': [450, 449, 450, 450, 451],
        'VWRL LN': [50, 50, 50.5, 49.8, 50.2],
    }, index=timestamps)
    
    fx_prices = pd.DataFrame({
        'USD': [1.10] * len(timestamps),
        'GBP': [0.85] * len(timestamps),
    }, index=timestamps)
    
    # Two dividend events
    dividends = pd.DataFrame({
        'SPY US': [0, 1.50, 0, 0, 0],
        'VWRL LN': [0, 0, 0, 0.25, 0],
    }, index=timestamps.normalize().unique())
    
    instruments = {
        'SPY US': create_mock_instrument('SPY', 'US'),
        'VWRL LN': create_mock_instrument('VWRL', 'LN'),
    }
    
    div_component = DividendComponent(dividends)
    
    adjustments = div_component.calculate_batch(
        instruments=instruments,
        dates=timestamps.tolist(),
        prices=prices,
        fx_prices=fx_prices
    )
    
    print("\nAdjustments:")
    for ts in timestamps:
        spy_adj = adjustments.loc[ts, 'SPY US']
        vwrl_adj = adjustments.loc[ts, 'VWRL LN']
        print(f"{ts}  SPY: {spy_adj:+.6f}  VWRL: {vwrl_adj:+.6f}")
    
    # Verify SPY adjustment at first timestamp on 15th (09:30)
    assert adjustments.loc[timestamps[1], 'SPY US'] > 0, "Expected SPY adjustment"
    
    # Verify VWRL adjustment at first timestamp on 20th (08:00)
    assert adjustments.loc[timestamps[3], 'VWRL LN'] > 0, "Expected VWRL adjustment"
    
    print("\n✓ Multiple dividends handled correctly")


def main():
    """Run all tests."""
    try:
        test_intraday_dividend_period_returns()
        test_daily_mode_unchanged()
        test_multiple_dividends()
        
        print("\n" + "="*70)
        print("✓ ALL DIVIDEND INTRADAY TESTS PASSED")
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
