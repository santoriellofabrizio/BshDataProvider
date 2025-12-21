# stock_classifier.py
import re

import pandas as pd
from .base_classifier import BaseClassifier


class StockClassifier(BaseClassifier):

    ISIN_RE = re.compile(r"^[A-Z]{2}[A-Z0-9]{9}[0-9]$")

    def _load(self, isin=[], ticker=[]):
        if self._df is None:
            if not self.oracle:
                raise RuntimeError("StockClassifier: manca QueryOracle")
            data, cols = self.oracle.get_equity_data()
            self._df = pd.DataFrame(data, columns=cols)
            self._build_maps()
        return self._df

    # ------------------------------------------------------------
    def _build_maps(self):
        df = self._df

        # ISIN → TICKER
        self.isin_to_ticker = df.set_index("ISIN")["TICKER"].to_dict()

        # TICKER → ISIN
        self.ticker_to_isin = df.set_index("TICKER")["ISIN"].to_dict()

        self.isins = set(self.isin_to_ticker.keys())
        self.tickers = set(self.ticker_to_isin.keys())

    # ------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------
    def matches(self, identifier: str) -> bool:
        self._load()
        return identifier in self.isins or identifier in self.tickers

    def as_ticker(self, isin: str):
        self._load()
        return self.isin_to_ticker.get(isin)

    def as_isin(self, ticker: str):
        self._load()
        return self.ticker_to_isin.get(ticker)

    def get_ccy(self, isin: str, market: str):
        self._load()
        result = self._df.loc[(self._df["ISIN"] == isin) & (self._df["EXCHANGE_CODE"] == market), "CURRENCY"]
        return result.iloc[0] if len(result) > 0 else None
