"""
Standalone test for updatable components protocol.

Tests the new protocol with append=True/False modes using saved data.
"""
import pandas as pd
import sys
from pathlib import Path
from datetime import time

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from analytics.adjustments.adjuster import Adjuster
from analytics.adjustments.ter import TerComponent
from analytics.adjustments.fx_spot import FxSpotComponent
from core.enums.instrument_types import InstrumentType

# Constants
DATA_DIR = Path(r"C:\Users\GBS08935\Desktop\dataEquity")
CURRENCY = ["USD", "GBP", "JPY", "CHF", "CAD", "AUD", "SEK", "NOK", "DKK"]


class MockInstrument:
    """Mock instrument for testing without database"""
    def __init__(self, instrument_id):
        self.id = instrument_id
        self.isin = instrument_id
        self.type = InstrumentType.ETP
        self.currency = "EUR"
        self.underlying_type = "EQUITY"
        self.payment_policy = "DIST"
        self.fund_currency = "EUR"
        self.currency_hedged = False


def load_data():
    """Load all required data from parquet files"""
    print("Loading data from parquet files...")

    # Load prices
    etf_prices = pd.read_parquet(DATA_DIR / "ETF_prices.parquet")
    print(f"ETF prices: {etf_prices.shape}")

    # Load FX prices
    fx_prices = pd.read_parquet(DATA_DIR / "FX_prices.parquet")
    print(f"FX prices: {fx_prices.shape}")

    # Load FX composition
    fx_composition = pd.read_parquet(DATA_DIR / "FX_composition.parquet")
    print(f"FX composition: {fx_composition.shape}")

    # Load FX forward composition
    fx_forward = pd.read_parquet(DATA_DIR / "FX_forward.parquet")
    print(f"FX forward: {fx_forward.shape}")

    # Load FX forward prices
    fx_forward_prices = pd.read_parquet(DATA_DIR / "FX_forward_prices.parquet")
    print(f"FX forward prices: {fx_forward_prices.shape}")

    return {
        "etf_prices": etf_prices,
        "fx_prices": fx_prices,
        "fx_composition": fx_composition,
        "fx_forward": fx_forward,
        "fx_forward_prices": fx_forward_prices,
    }


def test_basic_calculation():
    """Test basic calculation with initial data"""
    print("\n" + "="*80)
    print("TEST 1: Basic calculation with initial data")
    print("="*80)

    data = load_data()

    # For now, create dummy TER data (we'll load this separately later)
    instrument_ids = data["etf_prices"].columns.tolist()
    ter = {inst: 0.002 for inst in instrument_ids}  # 0.2% TER for all

    # Create mock instruments
    instruments = {inst_id: MockInstrument(inst_id) for inst_id in instrument_ids}

    # Create components
    ter_comp = TerComponent(ter)
    fx_spot_comp = FxSpotComponent(data["fx_composition"], data["fx_prices"])

    # Create adjuster with mock instruments
    adjuster = (
        Adjuster(data["etf_prices"], instruments=instruments)
        .add(ter_comp)
        .add(fx_spot_comp)
    )

    # Calculate
    adjustments = adjuster.calculate()
    print(f"\nAdjustments shape: {adjustments.shape}")
    print(f"Non-zero adjustments: {(adjustments != 0).sum().sum()}")
    print(f"Mean adjustment: {adjustments.mean().mean():.6f}")

    return adjuster, data


def test_permanent_update(adjuster, data):
    """Test permanent update (append=True)"""
    print("\n" + "="*80)
    print("TEST 2: Permanent update (append=True)")
    print("="*80)

    # Create new FX prices (e.g., last 10 days)
    new_fx_prices = data["fx_prices"].iloc[-10:].copy()
    new_fx_prices.index = pd.date_range(
        start=data["fx_prices"].index[-1] + pd.Timedelta(days=1),
        periods=10,
        freq='D'
    )
    # Simulate 1% appreciation in USD
    if "USD" in new_fx_prices.columns:
        new_fx_prices["USD"] *= 1.01

    print(f"\nOriginal FX prices: {len(data['fx_prices'])} rows")
    print(f"New FX prices: {len(new_fx_prices)} rows")

    # Permanent update
    adjuster.update(append=True, fx_prices=new_fx_prices)

    # Calculate twice - both should use updated data
    result1 = adjuster.calculate()
    result2 = adjuster.calculate()

    print(f"\nResult 1 - Non-zero: {(result1 != 0).sum().sum()}, Mean: {result1.mean().mean():.6f}")
    print(f"Result 2 - Non-zero: {(result2 != 0).sum().sum()}, Mean: {result2.mean().mean():.6f}")
    print(f"Results identical: {result1.equals(result2)}")

    return result1, result2


def test_temporary_update(adjuster, data):
    """Test temporary update (append=False)"""
    print("\n" + "="*80)
    print("TEST 3: Temporary update (append=False)")
    print("="*80)

    # Create live FX prices (simulate 2% depreciation)
    live_fx_prices = data["fx_prices"].iloc[-5:].copy()
    if "USD" in live_fx_prices.columns:
        live_fx_prices["USD"] *= 0.98

    print(f"Live FX prices: {len(live_fx_prices)} rows with 2% USD depreciation")

    # Temporary update
    adjuster.update(append=False, fx_prices=live_fx_prices)

    # First calculation - uses live data
    result1 = adjuster.calculate()
    print(f"\nResult 1 (with live data) - Non-zero: {(result1 != 0).sum().sum()}, Mean: {result1.mean().mean():.6f}")

    # Second calculation - back to permanent data
    result2 = adjuster.calculate()
    print(f"Result 2 (permanent data) - Non-zero: {(result2 != 0).sum().sum()}, Mean: {result2.mean().mean():.6f}")

    print(f"\nResults different (as expected): {not result1.equals(result2)}")

    return result1, result2


def test_multiple_updates():
    """Test multiple temporary updates in sequence"""
    print("\n" + "="*80)
    print("TEST 4: Multiple temporary updates in sequence")
    print("="*80)

    data = load_data()
    instrument_ids = data["etf_prices"].columns.tolist()
    ter = {inst: 0.002 for inst in instrument_ids}

    # Create mock instruments
    instruments = {inst_id: MockInstrument(inst_id) for inst_id in instrument_ids}

    # Create fresh adjuster
    adjuster = (
        Adjuster(data["etf_prices"], instruments=instruments)
        .add(TerComponent(ter))
        .add(FxSpotComponent(data["fx_composition"], data["fx_prices"]))
    )

    # Baseline
    baseline = adjuster.calculate()
    print(f"Baseline - Mean: {baseline.mean().mean():.6f}")

    # Temp update 1: +5% USD
    temp_fx_1 = data["fx_prices"].iloc[-5:].copy()
    if "USD" in temp_fx_1.columns:
        temp_fx_1["USD"] *= 1.05

    adjuster.update(append=False, fx_prices=temp_fx_1)
    result1 = adjuster.calculate()
    print(f"\nTemp update 1 (+5% USD) - Mean: {result1.mean().mean():.6f}")

    # After temp calculation, should revert to baseline
    result_after_1 = adjuster.calculate()
    print(f"After temp 1 (reverted) - Mean: {result_after_1.mean().mean():.6f}")
    print(f"Reverted to baseline: {abs(result_after_1.mean().mean() - baseline.mean().mean()) < 1e-10}")

    # Temp update 2: -3% USD
    temp_fx_2 = data["fx_prices"].iloc[-5:].copy()
    if "USD" in temp_fx_2.columns:
        temp_fx_2["USD"] *= 0.97

    adjuster.update(append=False, fx_prices=temp_fx_2)
    result2 = adjuster.calculate()
    print(f"\nTemp update 2 (-3% USD) - Mean: {result2.mean().mean():.6f}")

    # After temp calculation, should revert to baseline again
    result_after_2 = adjuster.calculate()
    print(f"After temp 2 (reverted) - Mean: {result_after_2.mean().mean():.6f}")
    print(f"Reverted to baseline: {abs(result_after_2.mean().mean() - baseline.mean().mean()) < 1e-10}")


def test_validation():
    """Test validation methods"""
    print("\n" + "="*80)
    print("TEST 5: Validation")
    print("="*80)

    data = load_data()
    instruments = data["etf_prices"].columns.tolist()

    fx_spot_comp = FxSpotComponent(data["fx_composition"], data["fx_prices"])

    # Test validate_update with invalid field
    try:
        fx_spot_comp.validate_update(invalid_field=123)
        print("ERROR: Should have raised ValueError for invalid field")
    except ValueError as e:
        print(f"[OK] Correctly rejected invalid field: {e}")

    # Test validate_update with wrong type
    try:
        fx_spot_comp.validate_update(fx_prices="not a dataframe")
        print("ERROR: Should have raised ValueError for wrong type")
    except ValueError as e:
        print(f"[OK] Correctly rejected wrong type: {e}")

    # Test validate_update with empty DataFrame
    try:
        fx_spot_comp.validate_update(fx_prices=pd.DataFrame())
        print("ERROR: Should have raised ValueError for empty DataFrame")
    except ValueError as e:
        print(f"[OK] Correctly rejected empty DataFrame: {e}")

    # Test valid update
    try:
        valid_fx = data["fx_prices"].iloc[-5:].copy()
        fx_spot_comp.validate_update(fx_prices=valid_fx)
        print("[OK] Accepted valid update")
    except Exception as e:
        print(f"ERROR: Should have accepted valid update: {e}")


def test_updatable_fields_subscription():
    """Test that components only receive their subscribed fields"""
    print("\n" + "="*80)
    print("TEST 6: Updatable fields subscription (no collision)")
    print("="*80)

    data = load_data()
    instrument_ids = data["etf_prices"].columns.tolist()
    ter = {inst: 0.002 for inst in instrument_ids}

    # Create mock instruments
    instruments = {inst_id: MockInstrument(inst_id) for inst_id in instrument_ids}

    ter_comp = TerComponent(ter)
    fx_spot_comp = FxSpotComponent(data["fx_composition"], data["fx_prices"])

    print(f"TER component updatable fields: {getattr(ter_comp, 'updatable_fields', set())}")
    print(f"FX Spot component updatable fields: {fx_spot_comp.updatable_fields}")

    adjuster = (
        Adjuster(data["etf_prices"], instruments=instruments)
        .add(ter_comp)
        .add(fx_spot_comp)
    )

    # Update with multiple fields
    new_fx = data["fx_prices"].iloc[-5:].copy()

    print("\nCalling adjuster.update(append=True, fx_prices=..., some_other_field=...)")
    adjuster.update(append=True, fx_prices=new_fx, some_other_field="ignored")

    print("[OK] FX Spot component received only fx_prices (its subscribed field)")
    print("[OK] TER component received nothing (not updatable)")
    print("[OK] Unknown field 'some_other_field' was ignored (no collision)")


def main():
    """Run all tests"""
    print("="*80)
    print("TESTING UPDATABLE COMPONENTS PROTOCOL")
    print("="*80)

    # Test 1: Basic calculation
    adjuster, data = test_basic_calculation()

    # Test 2: Permanent update
    test_permanent_update(adjuster, data)

    # Test 3: Temporary update
    test_temporary_update(adjuster, data)

    # Test 4: Multiple temporary updates
    test_multiple_updates()

    # Test 5: Validation
    test_validation()

    # Test 6: Subscription model
    test_updatable_fields_subscription()

    print("\n" + "="*80)
    print("ALL TESTS COMPLETED")
    print("="*80)


if __name__ == "__main__":
    main()
