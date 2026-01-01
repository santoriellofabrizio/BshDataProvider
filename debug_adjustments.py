"""
Debug script to investigate why adjustments don't appear to be making a difference.
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

import pandas as pd
import numpy as np
from pathlib import Path
from analytics.adjustments.adjuster import Adjuster
from analytics.adjustments.ter import TerComponent
from analytics.adjustments.fx_spot import FxSpotComponent
from analytics.adjustments.fx_forward_carry import FxForwardCarryComponent
from analytics.adjustments.dividend import DividendComponent
from core.enums.instrument_types import InstrumentType

# Data directory
DATA_DIR = Path("real_data")

class MockETF:
    """Mock ETF instrument"""
    def __init__(self, instrument_id):
        self.id = instrument_id
        self.isin = f"ISIN_{instrument_id}"
        self.type = InstrumentType.ETP
        self.currency = "EUR"
        self.underlying_type = "EQUITY"
        self.payment_policy = "DIST"
        self.fund_currency = "USD"
        self.currency_hedged = False

def load_real_data():
    """Load all real data from parquet files"""
    data = {}

    # Load prices
    prices = pd.read_parquet(DATA_DIR / "prices.parquet")

    # Load FX prices
    fx_prices = pd.read_parquet(DATA_DIR / "fx_prices.parquet")

    # Load FX composition
    fx_composition = pd.read_parquet(DATA_DIR / "fx_composition.parquet")

    # Load FX forward composition
    fx_forward_composition = pd.read_parquet(DATA_DIR / "fx_forward_composition.parquet")

    # Load FX forward points
    fx_forward_points = pd.read_parquet(DATA_DIR / "fx_forward_points.parquet")

    # Load dividends
    dividends = pd.read_parquet(DATA_DIR / "dividends.parquet")

    # Load TERs
    ters = pd.read_parquet(DATA_DIR / "ters.parquet")

    data = {
        'prices': prices,
        'fx_prices': fx_prices,
        'fx_composition': fx_composition,
        'ters': ters,
        'dividends': dividends,
        'fx_forward_points': fx_forward_points,
        'fx_forward_composition': fx_forward_composition
    }

    return data

print("="*80)
print("DEBUGGING ADJUSTMENTS")
print("="*80)

# Load data
print("\n1. Loading real data...")
data = load_real_data()
print(f"   Loaded {len(data['prices'])} timestamps from {data['prices'].index[0]} to {data['prices'].index[-1]}")

# Split data by last day
all_dates = data['prices'].index
last_day = all_dates[-1].normalize()
mask_before_last_day = all_dates.normalize() < last_day

historical_prices = data['prices'][mask_before_last_day]
historical_fx = data['fx_prices'][mask_before_last_day]
historical_fx_forward = data['fx_forward_points'][mask_before_last_day]

last_day_prices = data['prices'][~mask_before_last_day]
last_day_fx = data['fx_prices'][~mask_before_last_day]
last_day_fx_forward = data['fx_forward_points'][~mask_before_last_day]

print(f"   Historical: {len(historical_prices)} timestamps")
print(f"   Last day: {len(last_day_prices)} timestamps on {last_day.date()}")

# Create adjuster with all historical data
print("\n2. Creating adjuster with historical data...")

# Create mock instruments
instruments = {
    inst_id: MockETF(inst_id)
    for inst_id in historical_prices.columns
}

# Create components
print(f"   Historical FX: {len(historical_fx)} timestamps, {historical_fx.index[0]} to {historical_fx.index[-1]}")
print(f"   Historical FX forward: {len(historical_fx_forward)} timestamps, {historical_fx_forward.index[0]} to {historical_fx_forward.index[-1]}")

ter_comp = TerComponent(data['ters'])
fx_spot_comp = FxSpotComponent(data['fx_composition'], historical_fx)
fx_forward_comp = FxForwardCarryComponent(
    fwd_composition=data['fx_forward_composition'],
    fx_forward_prices=historical_fx_forward,
    tenor='1M',
    fx_spot_prices=historical_fx,
    settlement_days=2
)
div_comp = DividendComponent(data['dividends'], data['fx_prices'])

# Create adjuster
adjuster = (
    Adjuster(historical_prices, instruments=instruments, intraday=True)
    .add(ter_comp)
    .add(fx_spot_comp)
    .add(fx_forward_comp)
    .add(div_comp)
)

print("   Components added: TER, FXSpot, FXForwardCarry, Dividend")

# Now let's add just the FIRST timestamp of the last day and examine everything
print("\n3. Adding FIRST timestamp of last day...")
first_timestamp = last_day_prices.iloc[0:1]
first_fx = last_day_fx.iloc[0:1]
first_fx_forward = last_day_fx_forward.iloc[0:1]

print(f"   Timestamp: {first_timestamp.index[0]}")
print(f"   Raw prices: IUSA={first_timestamp['IUSA'].values[0]:.4f}, IUSE={first_timestamp['IUSE'].values[0]:.4f}")
print(f"   FX: USD={first_fx['USD'].values[0]:.6f}")

adjuster.append_update(
    prices=first_timestamp,
    fx_prices=first_fx,
    fx_forward_prices=first_fx_forward,
    recalc_last_n=1
)

# Get breakdown of adjustments for the last 2 timestamps
print("\n4. Examining adjustments for LAST 2 timestamps...")
all_timestamps = adjuster.prices.index
last_2_timestamps = all_timestamps[-2:]

breakdown = adjuster.get_breakdown(dates=last_2_timestamps.tolist())

print(f"\n   Timestamps being analyzed:")
for ts in last_2_timestamps:
    print(f"   - {ts}")

print("\n   Adjustment breakdown:")
for comp_name, comp_adj in breakdown.items():
    print(f"\n   {comp_name}:")
    print(comp_adj.to_string())
    print(f"   Mean: IUSA={comp_adj['IUSA'].mean():.8f}, IUSE={comp_adj['IUSE'].mean():.8f}")

# Get total adjustments
print("\n5. Total adjustments...")
total_adjustments = adjuster.calculate(dates=last_2_timestamps.tolist())
print(total_adjustments.to_string())
print(f"   Mean: IUSA={total_adjustments['IUSA'].mean():.8f}, IUSE={total_adjustments['IUSE'].mean():.8f}")

# Get raw returns
print("\n6. Raw returns...")
raw_returns = adjuster.return_calculator.calculate_returns(adjuster.prices)
raw_returns_last_2 = raw_returns.loc[last_2_timestamps]
print(raw_returns_last_2.to_string())
print(f"   Mean: IUSA={raw_returns_last_2['IUSA'].mean():.8f}, IUSE={raw_returns_last_2['IUSE'].mean():.8f}")

# Get clean returns
print("\n7. Clean returns (raw + adjustments)...")
clean_returns = adjuster.clean_returns(dates=last_2_timestamps.tolist())
print(clean_returns.to_string())
print(f"   Mean: IUSA={clean_returns['IUSA'].mean():.8f}, IUSE={clean_returns['IUSE'].mean():.8f}")

# Get raw prices for these timestamps
print("\n8. Raw prices for these timestamps...")
raw_prices_last_2 = adjuster.prices.loc[last_2_timestamps]
print(raw_prices_last_2.to_string())

# Get clean prices (backpropagate=False to go forward)
print("\n9. Clean prices (forward propagation, no rebase)...")
clean_prices = adjuster.clean_prices(backpropagate=False, rebase=False)
clean_prices_last_2 = clean_prices.loc[last_2_timestamps]
print(clean_prices_last_2.to_string())

# Calculate the difference
print("\n10. Difference (Clean - Raw)...")
diff = clean_prices_last_2 - raw_prices_last_2
print(diff.to_string())
print(f"   Mean: IUSA={diff['IUSA'].mean():.8f}, IUSE={diff['IUSE'].mean():.8f}")

# Now let's add the SECOND timestamp and see how things change
print("\n" + "="*80)
print("11. Adding SECOND timestamp of last day...")
second_timestamp = last_day_prices.iloc[1:2]
second_fx = last_day_fx.iloc[1:2]
second_fx_forward = last_day_fx_forward.iloc[1:2]

print(f"   Timestamp: {second_timestamp.index[0]}")
print(f"   Raw prices: IUSA={second_timestamp['IUSA'].values[0]:.4f}, IUSE={second_timestamp['IUSE'].values[0]:.4f}")

adjuster.append_update(
    prices=second_timestamp,
    fx_prices=second_fx,
    fx_forward_prices=second_fx_forward,
    recalc_last_n=1
)

# Get last 3 timestamps now
all_timestamps = adjuster.prices.index
last_3_timestamps = all_timestamps[-3:]

print("\n12. Examining adjustments for LAST 3 timestamps...")
breakdown = adjuster.get_breakdown(dates=last_3_timestamps.tolist())

print(f"\n   Timestamps being analyzed:")
for ts in last_3_timestamps:
    print(f"   - {ts}")

print("\n   Adjustment breakdown:")
for comp_name, comp_adj in breakdown.items():
    print(f"\n   {comp_name}:")
    print(comp_adj.to_string())

# Get total adjustments
print("\n13. Total adjustments...")
total_adjustments = adjuster.calculate(dates=last_3_timestamps.tolist())
print(total_adjustments.to_string())

# Get raw returns
print("\n14. Raw returns...")
raw_returns_last_3 = raw_returns.loc[last_3_timestamps]
print(raw_returns_last_3.to_string())

# Get clean returns
print("\n15. Clean returns (raw + adjustments)...")
clean_returns_last_3 = adjuster.clean_returns(dates=last_3_timestamps.tolist())
print(clean_returns_last_3.to_string())

# Get clean prices
print("\n16. Clean prices (forward propagation, no rebase)...")
clean_prices = adjuster.clean_prices(backpropagate=False, rebase=False)
clean_prices_last_3 = clean_prices.loc[last_3_timestamps]
print(clean_prices_last_3.to_string())

# Get raw prices
print("\n17. Raw prices for these timestamps...")
raw_prices_last_3 = adjuster.prices.loc[last_3_timestamps]
print(raw_prices_last_3.to_string())

# Calculate the difference
print("\n18. Difference (Clean - Raw)...")
diff = clean_prices_last_3 - raw_prices_last_3
print(diff.to_string())

print("\n" + "="*80)
print("SUMMARY")
print("="*80)
print("\nKey observations:")
print("1. Adjustment magnitudes are very small (~1e-5 to 1e-8)")
print("2. Raw returns are much larger (~1e-3 to 1e-2)")
print("3. Clean prices should differ from raw prices by cumulative effect of adjustments")
print("4. For intraday data (15min), adjustments might need different scaling")
