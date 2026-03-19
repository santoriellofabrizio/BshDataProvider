# base_classifier.py
from typing import Optional

import pandas as pd

from providers.oracle.query_oracle import QueryOracle


class BaseClassifier:
    def __init__(self, oracle: Optional[QueryOracle] = None):
        self.oracle = oracle
        self._df = None  # Lazy-loaded dataframe
        self._warmup = False

    def _load(self) -> pd.DataFrame:
        raise NotImplementedError

    def matches(self, identifier: str) -> bool:
        return False

    def warmup(self):
        self._warmup = True
