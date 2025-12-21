# bshdata/providers/mock.py
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

from core.base_classes.base_provider import BaseProvider
from core.requests.requests import BaseMarketRequest


class MockProvider(BaseProvider):
    """
    Provider di test che genera dati casuali invece di interfacciarsi a un DB o API.
    Utile per unit test e sviluppo offline.
    """

    def fetch_market_data(self, request: BaseMarketRequest):
        """
        Genera dati fake per testare il flusso end-to-end.

        Args:
            request (BaseMarketRequest): richiesta di mercato (DailyRequest, IntradayRequest, ecc.)

        Returns:
            MarketTimeSeries: dati randomici associati allo strumento richiesto
        """
        start = request.start or datetime.now() - timedelta(days=5)
        end = request.end or datetime.now()

        # Se daily, generiamo un punto al giorno
        if request.frequency == "1d":
            date_range = pd.date_range(start, end, freq="D")
        else:
            # Frequenza intraday → default 1m
            step = "1min" if request.frequency in ["1m", "tick"] else request.frequency
            date_range = pd.date_range(start, end, freq=step)

        data = pd.DataFrame({
            "timestamp": date_range,
            "mid_price": np.random.uniform(90, 110, len(date_range)),
            "bid_price": np.random.uniform(89, 109, len(date_range)),
            "ask_price": np.random.uniform(91, 111, len(date_range)),
            "volume": np.random.randint(1000, 5000, len(date_range)),
        })

        return data