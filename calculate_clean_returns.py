"""
Script minimale per calcolare clean returns usando l'adjuster.
"""
import pandas as pd
import sys
from pathlib import Path
import yfinance as yf
from matplotlib import pyplot as plt

from analytics.adjustments.dividend import DividendComponent
from analytics.adjustments.fx_forward_carry import FxForwardCarryComponent
from interface.bshdata import BshData

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from analytics.adjustments.adjuster import Adjuster
from analytics.adjustments.ter import TerComponent
from analytics.adjustments.fx_spot import FxSpotComponent
from core.enums.instrument_types import InstrumentType

# Data directory
DATA_DIR = Path(r"C:\Users\GBS08935\Desktop\dataEquity")


class MockInstrument:
    """Mock instrument per test senza database"""
    def __init__(self, instrument_id):
        self.id = instrument_id
        self.isin = instrument_id
        self.type = InstrumentType.ETP
        self.currency = "EUR"
        self.underlying_type = "EQUITY"
        self.payment_policy = "DIST"
        self.fund_currency = "EUR"
        self.currency_hedged = False


# Carica dati
print("Caricamento dati...")
etf_prices = pd.read_parquet(DATA_DIR / "ETF_prices.parquet")
fx_prices = pd.read_parquet(DATA_DIR / "FX_prices.parquet")
fx_composition = pd.read_parquet(DATA_DIR / "FX_composition.parquet")
fx_forward_composition = pd.read_parquet(DATA_DIR / "FX_forward.parquet")
fx_forward_prices = pd.read_parquet(DATA_DIR / "FX_forward_prices.parquet")
ter = pd.read_csv('ter.csv').iloc[:,-1]/100
dividends = pd.read_csv('dividends.csv').set_index('Date')

print(f"ETF prices: {etf_prices.shape}")
print(f"FX prices: {fx_prices.shape}")
print(f"FX composition: {fx_composition.shape}")

# Crea TER dummy (0.2% per tutti)
instrument_ids = etf_prices.columns.tolist()
tickers = pd.read_csv("tickers.csv").set_index("Unnamed: 0")["TICKER"].to_dict()
subs = {isin: f"{ticker}.MI" for isin, ticker in tickers.items()}

data = {}

etf_prices.drop("2025-12-19", inplace=True)
# Crea mock instruments
instruments = {inst_id: MockInstrument(inst_id) for inst_id in instrument_ids}

# Crea adjuster
print("Creazione adjuster...")
adjuster = (
    Adjuster(etf_prices, instruments=instruments)
    .add(TerComponent(ter))
    .add(FxSpotComponent(fx_composition, fx_prices))
    .add(FxForwardCarryComponent(fx_forward_composition, fx_forward_prices, "1M", fx_prices))
    .add(DividendComponent(dividends, fx_prices))
)

# Calcola adjustments
print("\nCalcolo adjustments...")
adjustments = adjuster.calculate()

print(f"\nAdjustments:")
print(f"  Shape: {adjustments.shape}")
print(f"  Non-zero: {(adjustments != 0).sum().sum()}")
print(f"  Mean: {adjustments.mean().mean():.6f}")
print(f"  Min: {adjustments.min().min():.6f}")
print(f"  Max: {adjustments.max().max():.6f}")

# Calcola raw returns
print("\nCalcolo raw returns...")
raw_returns = etf_prices.pct_change(fill_method=None)


# Calcola clean returns
print("Calcolo clean returns...")
clean_returns = adjuster.clean_returns()
clean_prices = adjuster.clean_prices()
clean_prices /= clean_prices.iloc[0]

clean_prices.plot()
plt.show()

print(f"\nRaw returns:")
print(f"  Mean: {raw_returns.mean().mean():.6f}")
print(f"  Std: {raw_returns.std().mean():.6f}")

print(f"\nClean returns:")
print(f"  Mean: {clean_returns.mean().mean():.6f}")
print(f"  Std: {clean_returns.std().mean():.6f}")

print(f"\nDifferenza (clean - raw):")
diff = clean_returns - raw_returns
print(f"  Mean: {diff.mean().mean():.6f}")
print(f"  Max: {diff.max().max():.6f}")

clean_returns.rename(tickers,axis=1, inplace=True)

print(adjuster.get_breakdown())

subset = ["IUSA","IUSE","CSSPX"]

rets = clean_returns[subset]

compare = ((1+rets).cumprod() - 1)
print(compare*10000)
compare.plot()
plt.show()

print("\nFatto!")
