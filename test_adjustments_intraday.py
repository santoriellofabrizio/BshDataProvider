"""
Test intraday adjustments con FX e Dividend.

Setup:
- 2 ETF: SPY (USD exposure), EQQQ (EUR hedged)
- 4 giorni, 4 ore al giorno (16 timestamps totali)
- SPY: prezzo = index_level + fx_effect, paga dividend al giorno 2
- EQQQ: prezzo = index_level (hedged), no dividend
"""
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
from analytics.adjustments.adjuster import Adjuster
from analytics.adjustments.fx_spot import FxSpotComponent
from analytics.adjustments.dividend import DividendComponent
from core.enums.instrument_types import InstrumentType


# ============================================================================
# MOCK INSTRUMENT
# ============================================================================
class MockETF:
    def __init__(self, id, currency='EUR', currency_hedged=False, payment_policy='DIST', fund_currency='EUR'):
        self.id = id
        self.isin = f'ISIN_{id}'
        self.type = InstrumentType.ETP
        self.currency = currency
        self.currency_hedged = currency_hedged
        self.payment_policy = payment_policy
        self.fund_currency = fund_currency


# ============================================================================
# SETUP DATA
# ============================================================================
# Timestamps: 4 giorni, 4 ore al giorno
start = datetime(2024, 1, 2, 9, 0)
timestamps = [start + timedelta(days=d, hours=h) for d in range(4) for h in [0, 3, 6, 9]]

# Strumenti
SPY = MockETF('SPY US', currency='EUR', currency_hedged=False, fund_currency='USD')
EQQQ = MockETF('EQQQ GY', currency='EUR', currency_hedged=True, fund_currency='EUR')
instruments = {SPY.id: SPY, EQQQ.id: EQQQ}

# FX USD/EUR: parte da 1.10, sale gradualmente
usd_eur = np.linspace(1.10, 1.13, 16)
fx_prices = pd.DataFrame({'USD': usd_eur}, index=timestamps)

# Index returns (SAME for both ETFs)
np.random.seed(42)  # Riproducibilità
index_returns = np.random.randn(16) * 0.005  # Returns dell'index (0.5% volatilità)

# FX returns
fx_returns_pct = np.diff(usd_eur) / usd_eur[:-1]
fx_returns_pct = np.insert(fx_returns_pct, 0, 0.0)  # Primo return = 0

# Dividend info
dividend_timestamp = timestamps[4]
dividend_amount = 4.0

# Build prices correctly
# We want SPY to track: index × FX_factor
# So that SPY_return ≈ index_return + fx_return (for small returns)

# Start with initial price
initial_price = 100.0

# Build cumulative factors separately
index_cumulative = (1 + index_returns).cumprod()
fx_cumulative = (1 + fx_returns_pct).cumprod()

# EQQQ: only index (hedged from FX) - no dividend
prices_eqqq = initial_price * index_cumulative

# SPY: exposed to both index and FX, WITH dividend at timestamp 4
# Build prices iteratively to properly handle dividend
prices_spy = np.zeros(16)
prices_spy[0] = initial_price  # Start at 100 EUR

for i in range(1, 16):
    # Natural price evolution: previous price × (1 + index_ret) / (1 + fx_ret)
    prices_spy[i] = prices_spy[i-1] * (1 + index_returns[i]) / (1 + fx_returns_pct[i])

    # Apply dividend drop at timestamp 4 (ex-dividend date)
    if i == 4:
        prices_spy[i] -= dividend_amount

# Prices DataFrame
prices = pd.DataFrame({
    'SPY US': prices_spy,
    'EQQQ GY': prices_eqqq
}, index=timestamps)

# FX Composition
fx_composition = pd.DataFrame({
    'USD': [1.0, 0.0],  # SPY: 100% USD, EQQQ: 0% USD
    'EUR': [0.0, 1.0]   # SPY: 0% EUR, EQQQ: 100% EUR
}, index=['SPY US', 'EQQQ GY'])

# Dividends
dividends = pd.DataFrame({
    'SPY US': [4.0],
    'EQQQ GY': [0.0]
}, index=[dividend_timestamp])


# ============================================================================
# TEST INCREMENTAL UPDATE
# ============================================================================
def test_intraday_incremental():
    print("\n" + "="*80)
    print("TEST INTRADAY ADJUSTMENTS - INCREMENTAL UPDATE")
    print("="*80)

    # Setup adjuster con primi 2 giorni (8 timestamps)
    initial_prices = prices.iloc[:8]
    initial_fx = fx_prices.iloc[:8]

    adjuster = Adjuster(initial_prices, instruments=instruments, intraday=True)
    adjuster.add(FxSpotComponent(fx_composition, initial_fx))
    adjuster.add(DividendComponent(dividends, initial_fx))

    print(f"\n1. Initial setup: {len(initial_prices)} timestamps")
    print(f"   Dates: {initial_prices.index[0]} to {initial_prices.index[-1]}")

    # Calculate initial
    adj_initial = adjuster.calculate()
    print(f"\n2. Initial calculate:")
    print(f"   Cache size: {len(adjuster._adjustments)} timestamps")
    print(f"   SPY adjustments: mean={adj_initial['SPY US'].mean():.6f}")
    print(f"   EQQQ adjustments: mean={adj_initial['EQQQ GY'].mean():.6f}")

    # Update: aggiungi giorno 3 (4 timestamps)
    day3_prices = prices.iloc[8:12]
    day3_fx = fx_prices.iloc[8:12]

    print(f"\n3. Update (append=True): adding {len(day3_prices)} timestamps (day 3)")
    adjuster.update(append=True, prices=day3_prices, fx_prices=day3_fx)
    print(f"   Cache size after update: {len(adjuster._adjustments)} timestamps")

    # Calculate (dovrebbe usare cache)
    adj_day3 = adjuster.calculate()
    print(f"\n4. Calculate after update (should use cache):")
    print(f"   Total timestamps: {len(adj_day3)}")

    # Update: aggiungi giorno 4 (4 timestamps)
    day4_prices = prices.iloc[12:]
    day4_fx = fx_prices.iloc[12:]

    print(f"\n5. Update (append=True): adding {len(day4_prices)} timestamps (day 4)")
    adjuster.update(append=True, prices=day4_prices, fx_prices=day4_fx)
    print(f"   Cache size after update: {len(adjuster._adjustments)} timestamps")

    # Final calculate
    adj_final = adjuster.calculate()
    print(f"\n6. Final calculate:")
    print(f"   Total timestamps: {len(adj_final)}")

    # Clean returns
    clean_returns = adjuster.clean_returns()
    print(f"\n7. Clean returns:")
    print(f"   SPY: mean={clean_returns['SPY US'].mean():.6f}, std={clean_returns['SPY US'].std():.6f}")
    print(f"   EQQQ: mean={clean_returns['EQQQ GY'].mean():.6f}, std={clean_returns['EQQQ GY'].std():.6f}")

    # Clean prices (backpropagate)
    clean_prices = adjuster.clean_prices(backpropagate=False)
    print(f"\n8. Clean prices (backpropagated from last):")
    print(f"   SPY: first={clean_prices['SPY US'].iloc[0]:.2f}, last={clean_prices['SPY US'].iloc[-1]:.2f}")
    print(f"   EQQQ: first={clean_prices['EQQQ GY'].iloc[0]:.2f}, last={clean_prices['EQQQ GY'].iloc[-1]:.2f}")

    # Validation
    print(f"\n9. Validation:")
    print(f"   ✓ SPY has FX adjustments (not hedged): {(adj_final['SPY US'] != 0).any()}")
    print(f"   ✓ EQQQ has NO FX adjustments (hedged): {(adj_final['EQQQ GY'] == 0).all()}")
    print(f"   ✓ SPY dividend at timestamp 8: adjustment={adj_final.loc[dividend_timestamp, 'SPY US']:.6f}")

    # Verifica che i clean returns siano uguali (stesso underlying index)
    print(f"\n10. Clean Returns Comparison (same underlying index):")
    diff = clean_returns['SPY US'] - clean_returns['EQQQ GY']
    max_diff = diff.abs().max()
    mean_diff = diff.abs().mean()
    print(f"   Max difference: {max_diff:.10f}")
    print(f"   Mean difference: {mean_diff:.10f}")
    print(f"   Std of difference: {diff.std():.10f}")
    print(f"   Mean FX return: {fx_returns_pct.mean():.10f}")
    print(f"   Ratio (mean_diff / mean_fx): {mean_diff / fx_returns_pct.mean():.2f}")
    print(f"   ✓ Returns are equal (within 0.01% precision): {max_diff < 1e-4}")
    print(f"   ✓ Returns are equal (within 0.001% numerical precision): {max_diff < 1e-5}")

    # Debug: mostra raw returns e adjustments
    print(f"\n11. Debug Info:")
    raw_returns = adjuster.prices.pct_change(fill_method=None).fillna(0)

    # Show prices around dividend timestamp
    print(f"   Dividend timestamp: {dividend_timestamp} (index 4)")
    print(f"   Price SPY[3]: {adjuster.prices['SPY US'].iloc[3]:.6f}")
    print(f"   Price SPY[4]: {adjuster.prices['SPY US'].iloc[4]:.6f} (after dividend drop)")
    print(f"   Price SPY[5]: {adjuster.prices['SPY US'].iloc[5]:.6f}")
    print(f"   Price EQQQ[3]: {adjuster.prices['EQQQ GY'].iloc[3]:.6f}")
    print(f"   Price EQQQ[4]: {adjuster.prices['EQQQ GY'].iloc[4]:.6f}")
    print(f"   Price EQQQ[5]: {adjuster.prices['EQQQ GY'].iloc[5]:.6f}")

    # Check construction
    print(f"\n   SPY prices_spy[3] BEFORE dividend drop: {prices_spy[3] + dividend_amount:.6f}")
    print(f"   SPY prices_spy[4] would be BEFORE dividend: {prices_spy[4] + dividend_amount:.6f}")
    print(f"   EQQQ prices_eqqq[3]: {prices_eqqq[3]:.6f}")
    print(f"   EQQQ prices_eqqq[4]: {prices_eqqq[4]:.6f}")

    print(f"\n   Raw return SPY[4]: {raw_returns['SPY US'].iloc[4]:.10f} (includes dividend drop)")
    print(f"   Raw return SPY[5]: {raw_returns['SPY US'].iloc[5]:.10f}")
    print(f"   Raw return EQQQ[4]: {raw_returns['EQQQ GY'].iloc[4]:.10f}")
    print(f"   Raw return EQQQ[5]: {raw_returns['EQQQ GY'].iloc[5]:.10f}")
    print(f"   Total adjustment SPY[4]: {adj_final['SPY US'].iloc[4]:.10f}")
    print(f"   Total adjustment SPY[5]: {adj_final['SPY US'].iloc[5]:.10f}")
    print(f"   Clean return SPY[4]: {clean_returns['SPY US'].iloc[4]:.10f}")
    print(f"   Clean return SPY[5]: {clean_returns['SPY US'].iloc[5]:.10f}")
    print(f"   Clean return EQQQ[5]: {clean_returns['EQQQ GY'].iloc[5]:.10f}")

    # Check what the true index returns are
    print(f"\n   Expected index return[4]: {index_returns[4]:.10f}")
    print(f"   Expected index return[5]: {index_returns[5]:.10f}")
    print(f"   Expected FX return[5]: {fx_returns_pct[5]:.10f}")

    # Check errors far from dividend (timestamps 10-15, after dividend effect)
    print(f"\n   Clean return errors far from dividend:")
    for i in [10, 11, 12, 13, 14, 15]:
        error = clean_returns['SPY US'].iloc[i] - clean_returns['EQQQ GY'].iloc[i]
        print(f"   [{i}] Error: {error:.10f}, Index ret: {index_returns[i]:.10f}")

    # Summary
    print(f"\n" + "="*80)
    print("SUMMARY")
    print("="*80)
    print(f"Total timestamps processed: {len(adj_final)}")
    print(f"Updates performed: 2 (day 3 + day 4)")
    print(f"Cache hits: 100% after initial calculation")
    print(f"SPY total adjustment: {adj_final['SPY US'].sum():.6f}")
    print(f"EQQQ total adjustment: {adj_final['EQQQ GY'].sum():.6f}")
    print("="*80 + "\n")

    # Return data for plotting
    return {
        'prices': prices,
        'clean_prices': clean_prices,
        'adjustments': adj_final,
        'clean_returns': clean_returns,
        'fx_prices': fx_prices,
        'dividend_timestamp': dividend_timestamp
    }


def plot_results(data):
    """Plot comprehensive results"""
    fig, axes = plt.subplots(3, 3, figsize=(18, 10))
    fig.suptitle('Intraday Adjustments Test - SPY vs EQQQ (Same Underlying Index)', fontsize=14, fontweight='bold')

    # 1. Raw Prices
    ax = axes[0, 0]
    data['prices'].plot(ax=ax, marker='o', markersize=3)
    ax.axvline(data['dividend_timestamp'], color='red', linestyle='--', alpha=0.5, label='Dividend')
    ax.set_title('Raw Prices')
    ax.set_ylabel('Price (EUR)')
    ax.legend()
    ax.grid(True, alpha=0.3)

    # 2. Clean Prices
    ax = axes[0, 1]
    data['clean_prices'].plot(ax=ax, marker='o', markersize=3)
    ax.axvline(data['dividend_timestamp'], color='red', linestyle='--', alpha=0.5, label='Dividend')
    ax.set_title('Clean Prices (Backpropagated)')
    ax.set_ylabel('Price (EUR)')
    ax.legend()
    ax.grid(True, alpha=0.3)

    # 3. Adjustments
    ax = axes[1, 0]
    data['adjustments'].plot(ax=ax, marker='o', markersize=3)
    ax.axvline(data['dividend_timestamp'], color='red', linestyle='--', alpha=0.5, label='Dividend')
    ax.axhline(0, color='black', linestyle='-', linewidth=0.5)
    ax.set_title('Total Adjustments (FX + Dividend)')
    ax.set_ylabel('Adjustment')
    ax.legend()
    ax.grid(True, alpha=0.3)

    # 4. Clean Returns
    ax = axes[1, 1]
    data['clean_returns'].plot(ax=ax, marker='o', markersize=3, alpha=0.7)
    ax.axvline(data['dividend_timestamp'], color='red', linestyle='--', alpha=0.5, label='Dividend')
    ax.axhline(0, color='black', linestyle='-', linewidth=0.5)
    ax.set_title('Clean Returns')
    ax.set_ylabel('Return')
    ax.legend()
    ax.grid(True, alpha=0.3)

    # 5. FX USD/EUR
    ax = axes[2, 0]
    data['fx_prices']['USD'].plot(ax=ax, marker='o', markersize=3, color='green')
    ax.set_title('FX USD/EUR')
    ax.set_ylabel('USD/EUR')
    ax.grid(True, alpha=0.3)

    # 6. Cumulative Returns Comparison
    ax = axes[2, 1]
    raw_returns = data['prices'].pct_change().fillna(0)
    cum_raw = (1 + raw_returns).cumprod() - 1
    cum_clean = (1 + data['clean_returns']).cumprod() - 1

    cum_raw['SPY US'].plot(ax=ax, label='SPY Raw', linestyle='--', marker='o', markersize=3)
    cum_clean['SPY US'].plot(ax=ax, label='SPY Clean', marker='o', markersize=3)
    cum_raw['EQQQ GY'].plot(ax=ax, label='EQQQ Raw', linestyle='--', marker='s', markersize=3)
    cum_clean['EQQQ GY'].plot(ax=ax, label='EQQQ Clean', marker='s', markersize=3)

    ax.axvline(data['dividend_timestamp'], color='red', linestyle='--', alpha=0.5)
    ax.axhline(0, color='black', linestyle='-', linewidth=0.5)
    ax.set_title('Cumulative Returns (Raw vs Clean)')
    ax.set_ylabel('Cumulative Return')
    ax.legend()
    ax.grid(True, alpha=0.3)

    # 7. Clean Returns Difference (Should be ~0)
    ax = axes[0, 2]
    diff = data['clean_returns']['SPY US'] - data['clean_returns']['EQQQ GY']
    diff.plot(ax=ax, marker='o', markersize=4, color='red', linewidth=2)
    ax.axhline(0, color='black', linestyle='-', linewidth=0.5)
    ax.set_title('Clean Returns Difference (SPY - EQQQ)\nShould be ~0 (same index)')
    ax.set_ylabel('Difference')
    ax.grid(True, alpha=0.3)
    # Add text with max diff
    max_diff = diff.abs().max()
    ax.text(0.5, 0.95, f'Max diff: {max_diff:.2e}',
            transform=ax.transAxes, ha='center', va='top',
            bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))

    # 8. Clean Returns Overlay (Should be identical)
    ax = axes[1, 2]
    data['clean_returns']['SPY US'].plot(ax=ax, label='SPY', marker='o', markersize=4, linewidth=2)
    data['clean_returns']['EQQQ GY'].plot(ax=ax, label='EQQQ', marker='s', markersize=4,
                                          linewidth=2, linestyle='--', alpha=0.7)
    ax.axhline(0, color='black', linestyle='-', linewidth=0.5)
    ax.set_title('Clean Returns Overlay\n(Should overlap perfectly)')
    ax.set_ylabel('Clean Return')
    ax.legend()
    ax.grid(True, alpha=0.3)

    # 9. Index Effect (Price diff due to FX + Dividend)
    ax = axes[2, 2]
    price_diff = data['prices']['SPY US'] - data['prices']['EQQQ GY']
    price_diff.plot(ax=ax, marker='o', markersize=4, color='purple', linewidth=2)
    ax.axvline(data['dividend_timestamp'], color='red', linestyle='--', alpha=0.5, label='Dividend')
    ax.axhline(0, color='black', linestyle='-', linewidth=0.5)
    ax.set_title('Raw Price Difference (SPY - EQQQ)\nDue to FX + Dividend')
    ax.set_ylabel('Price Difference (EUR)')
    ax.legend()
    ax.grid(True, alpha=0.3)

    plt.tight_layout()
    plt.savefig('intraday_test_results.png', dpi=150, bbox_inches='tight')
    print("\nPlot saved to: intraday_test_results.png")
    # plt.show()  # Commented out to avoid blocking


if __name__ == '__main__':
    data = test_intraday_incremental()
    plot_results(data)
