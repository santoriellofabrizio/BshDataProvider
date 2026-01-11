"""Test simulate_live_returns initialization step by step"""
import sys
sys.path.insert(0, 'C:\\AFMachineLearning\\Libraries\\BshDataProvider\\src')

print("1. Importing modules...")
from simulate_live_returns import load_data, EtfInstrument
from analytics.adjustments import Adjuster
from analytics.adjustments.ter import TerComponent
from analytics.adjustments.fx_spot import FxSpotComponent
from analytics.adjustments.fx_forward_carry import FxForwardCarryComponent
from analytics.adjustments.dividend import DividendComponent

print("2. Loading data...")
data, tickers = load_data()
print(f"   Loaded {len(data['etf_prices'].columns)} ETFs")

SUBSET_TICKERS = ['IUSA', 'IUSE']
ticker_to_isin = {v: k for k, v in tickers.items()}
subset_isins = [ticker_to_isin[t] for t in SUBSET_TICKERS]

etf_prices = data["etf_prices"][subset_isins]
fx_prices = data["fx_prices"]
instruments = {isin: EtfInstrument(isin) for isin in subset_isins}

print("3. Creating Adjuster...")
adjuster = Adjuster(etf_prices, instruments=instruments, is_intraday=False)
print("   Adjuster created")

print("4. Adding TerComponent...")
adjuster.add(TerComponent(data["ter"]))
print("   TerComponent added")

print("5. Adding FxSpotComponent...")
adjuster.add(FxSpotComponent(data["fx_composition"], fx_prices))
print("   FxSpotComponent added")

print("6. Adding FxForwardCarryComponent...")
adjuster.add(FxForwardCarryComponent(
    data["fx_forward_composition"],
    data["fx_forward_prices"],
    "1M",
    fx_prices,
))
print("   FxForwardCarryComponent added")

print("7. Adding DividendComponent...")
adjuster.add(DividendComponent(data["dividends"], fx_prices))
print("   DividendComponent added")

print("\n8. Calculating once...")
result = adjuster.calculate_adjustments()
print(f"   Result shape: {result.shape}")

print("\n SUCCESS - All initialization steps completed!")
