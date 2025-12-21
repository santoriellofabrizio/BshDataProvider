"""Test minimo FX components"""
import datetime as dt
import pandas as pd
from analytics.adjustments import InstrumentAdjuster
from core.instruments import InstrumentFactory
from client import BSHDataClient

# Setup
client = BSHDataClient(r"C:\AFMachineLearning\Libraries\BshDataProvider\config\bshdata_config.yaml")
factory = InstrumentFactory(client)
etf = factory.create(ticker="IHYG", autocomplete=True)

dates = [dt.date(2024, 12, 1) + dt.timedelta(days=i) for i in range(5)]

# Mock data esterni
fx_prices = pd.DataFrame({'USD': [1.10, 1.11, 1.10, 1.12, 1.11]}, index=dates)
instrument_prices = pd.DataFrame({etf.id: [85.0, 85.5, 86.0, 85.5, 86.5]}, index=dates)

# Setup adjuster

adjuster = InstrumentAdjuster(etf)
adjuster.setup(client, dates, fx_prices=fx_prices, instrument_prices=instrument_prices)

# Download e calcola
adjuster.download_data()
adjustments = adjuster.get_adjustments()

print("Adjustments:")
print(adjustments)
print(f"\nMean adjustment: {adjustments.mean():.6f}")