"""
Test to verify adjustments are actually having an impact on clean prices.
"""
import sys
sys.path.insert(0, 'src')

import pandas as pd
import numpy as np
from pathlib import Path
from test_real_data import load_real_data, MockETF
from analytics.adjustments.adjuster import Adjuster
from analytics.adjustments.ter import TerComponent
from analytics.adjustments.fx_spot import FxSpotComponent
from analytics.adjustments.fx_forward_carry import FxForwardCarryComponent
from analytics.adjustments.dividend import DividendComponent

print("="*80)
print("TESTING ADJUSTMENT IMPACT ON CLEAN PRICES")
print("="*80)

# Load data
data = load_real_data()

# Use ALL data (not split)
all_prices = data['prices']
all_fx = data['fx_prices']
all_fx_forward = data['fx_forward_points']

print(f"\n1. Creating adjuster with ALL data: {len(all_prices)} timestamps")

# Create instruments
instruments = {
    inst_id: MockETF(inst_id)
    for inst_id in all_prices.columns
}

# Create adjuster
adjuster = (
    Adjuster(all_prices, instruments=instruments, intraday=True)
    .add(TerComponent(data['ters']))
    .add(FxSpotComponent(data['fx_composition'], all_fx))
    .add(FxForwardCarryComponent(
        fwd_composition=data['fx_forward_composition'],
        fx_forward_prices=all_fx_forward,
        tenor='1M',
        fx_spot_prices=all_fx,
        settlement_days=2
    ))
    .add(DividendComponent(data['dividends'], data['fx_prices']))
)

print(f"\n2. Calculating clean prices (forward propagation, no rebase)...")
clean_prices = adjuster.clean_prices(backpropagate=False, rebase=False)

print(f"\n3. Comparing last day raw vs clean prices...")

# Get last day
last_day = all_prices.index[-1].normalize()
mask_last_day = all_prices.index.normalize() == last_day

last_day_raw = all_prices[mask_last_day]
last_day_clean = clean_prices[mask_last_day]

print(f"\n   Last day: {last_day.date()} ({len(last_day_raw)} timestamps)")

# Calculate difference
diff = last_day_clean - last_day_raw

print(f"\n4. Price differences (Clean - Raw):")
print(f"\n   IUSA:")
print(f"     First: Raw={last_day_raw['IUSA'].iloc[0]:.6f}, Clean={last_day_clean['IUSA'].iloc[0]:.6f}, Diff={diff['IUSA'].iloc[0]:.6f}")
print(f"     Last:  Raw={last_day_raw['IUSA'].iloc[-1]:.6f}, Clean={last_day_clean['IUSA'].iloc[-1]:.6f}, Diff={diff['IUSA'].iloc[-1]:.6f}")
print(f"     Mean difference: {diff['IUSA'].mean():.6f}")
print(f"     Std difference: {diff['IUSA'].std():.6f}")

print(f"\n   IUSE:")
print(f"     First: Raw={last_day_raw['IUSE'].iloc[0]:.6f}, Clean={last_day_clean['IUSE'].iloc[0]:.6f}, Diff={diff['IUSE'].iloc[0]:.6f}")
print(f"     Last:  Raw={last_day_raw['IUSE'].iloc[-1]:.6f}, Clean={last_day_clean['IUSE'].iloc[-1]:.6f}, Diff={diff['IUSE'].iloc[-1]:.6f}")
print(f"     Mean difference: {diff['IUSE'].mean():.6f}")
print(f"     Std difference: {diff['IUSE'].std():.6f}")

# Get adjustment breakdown
print(f"\n5. Adjustment breakdown for last day:")
breakdown = adjuster.get_breakdown(dates=last_day_raw.index.tolist())

for comp_name, comp_adj in breakdown.items():
    print(f"\n   {comp_name}:")
    mean_iusa = comp_adj['IUSA'].mean()
    mean_iuse = comp_adj['IUSE'].mean()
    sum_iusa = comp_adj['IUSA'].sum()
    sum_iuse = comp_adj['IUSE'].sum()
    print(f"     IUSA: mean={mean_iusa:.8f}, sum={sum_iusa:.8f}")
    print(f"     IUSE: mean={mean_iuse:.8f}, sum={sum_iuse:.8f}")

# Get total adjustments
total_adj = adjuster.calculate(dates=last_day_raw.index.tolist())
print(f"\n6. Total adjustments for last day:")
print(f"     IUSA: mean={total_adj['IUSA'].mean():.8f}, sum={total_adj['IUSA'].sum():.8f}")
print(f"     IUSE: mean={total_adj['IUSE'].mean():.8f}, sum={total_adj['IUSE'].sum():.8f}")

# Show percentage impact
print(f"\n7. Percentage impact on prices:")
pct_diff_iusa = (diff['IUSA'] / last_day_raw['IUSA'] * 100).mean()
pct_diff_iuse = (diff['IUSE'] / last_day_raw['IUSE'] * 100).mean()
print(f"     IUSA: {pct_diff_iusa:.4f}% average difference")
print(f"     IUSE: {pct_diff_iuse:.4f}% average difference")

print(f"\n" + "="*80)
print("CONCLUSION:")
if abs(diff['IUSA'].mean()) > 0.001 or abs(diff['IUSE'].mean()) > 0.001:
    print("✓ Adjustments ARE making a difference in clean prices!")
    print(f"  IUSA: Average difference of {diff['IUSA'].mean():.4f} EUR")
    print(f"  IUSE: Average difference of {diff['IUSE'].mean():.4f} EUR")
else:
    print("✗ Adjustments appear too small to have significant impact")
    print("  This might be expected for intraday data where adjustments are tiny")
print("="*80)
