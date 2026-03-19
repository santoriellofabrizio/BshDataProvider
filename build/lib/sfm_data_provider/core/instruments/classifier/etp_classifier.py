# etp_classifier.py
import re
from typing import Optional

import pandas as pd

from sfm_data_provider.providers.oracle.query_oracle import QueryOracle
from .base_classifier import BaseClassifier
from ...enums.issuers import IssuerGroup, normalize_issuer


class ETPClassifier(BaseClassifier):

    ISIN_RE = re.compile(r"^[A-Z]{2}[A-Z0-9]{9}[0-9]$")

    def __init__(self, oracle: Optional[QueryOracle] = None):
        super().__init__(oracle)
        self._isins = None
        self._tickers = []

    def _load(self):
        if self._df is None:
            if not self.oracle:
                raise RuntimeError("ETPClassifier: manca QueryOracle")
            data = self.oracle.get_etps_data()
            self._df = pd.DataFrame(data)
            self._build_maps()
        return self._df

    # ------------------------------------------------------------
    def _build_maps(self):
        self._load()
        df = self._df

        self.isin_to_ticker = (
            df.dropna(subset=["ISIN", "TICKER"])
              .drop_duplicates(subset=["ISIN"])
              .set_index("ISIN")["TICKER"].astype(str)
              .to_dict()
        )

        self.ticker_to_isin = {v: k for k, v in self.isin_to_ticker.items()}

    # ------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------

    @property
    def tickers(self):
        if not self._tickers:
            self._load()
            self._tickers = self.ticker_to_isin.keys()
        return self._tickers

    @property
    def isins(self):
        if not self._isins:
            self._load()
            self._isins = self.isin_to_ticker.keys()
        return self._isins

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

    def get_undelying_type(self, id_):
        self._load()
        result = self._df.loc[(self._df["ISIN"] == id_), "UNDERLYING_TYPE"]
        return result.iloc[0] if len(result) > 0 else None

    def get_fund_currency(self, id_):
        self._load()
        result = self._df.loc[(self._df["ISIN"] == id_), "FUND_CURRENCY"]
        return result.iloc[0] if len(result) > 0 else None

    def get_payment_policy(self, id_):
        self._load()
        result = self._df.loc[(self._df["ISIN"] == id_), "PAYMENT_POLICY"]
        return result.iloc[0] if len(result) > 0 else None

    def get_issue_date(self, id_):
        self._load()
        result = self._df.loc[(self._df["ISIN"] == id_), "ISSUE_DATE"]
        return result.iloc[0] if len(result) > 0 else None

    def get_issuer(self, id_) -> IssuerGroup:
        self._load()
        result = self._df.loc[(self._df["ISIN"] == id_), "SHORT_NAME"]
        return normalize_issuer(result.iloc[0] if len(result) > 0 else None)
