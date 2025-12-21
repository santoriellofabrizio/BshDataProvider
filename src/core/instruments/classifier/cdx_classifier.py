# cdx_classifier.py
import pandas as pd
from .base_classifier import BaseClassifier


class CDSClassifier(BaseClassifier):

    def _load(self):
        if self._df is None:
            if not self.oracle:
                raise RuntimeError("CDSClassifier: manca QueryOracle")
            self._df = pd.DataFrame(self.oracle.get_cdx_data()).set_index("TICKER_ROOT")
        return self._df

    # ------------------------------------------------------------
    def matches(self, identifier: str) -> bool:
        identifier = identifier.upper().strip()
        identifier = identifier.replace('ITRAXXMAIN', 'ITXEB5')
        identifier = identifier.replace('ITRAXXXOVER', 'ITXEX5')
        identifier = identifier.replace('ITRAXXSUBFIN', 'ITXEU5')
        identifier = identifier.replace('ITRAXXSNRFIN', 'ITXES5')
        df = self._load()
        idu = identifier.upper()
        return idu in df.index or idu in df.get("INDEX_NAME").values.tolist()

    def get_field(self, ticker_root: str, field: str):
        df = self._load()
        try:
            return df.at[ticker_root.upper(), field.upper()]
        except Exception:
            return None

    def roots(self):
        df = self._load()
        return df.index.values.tolist()

    def get_ccy(self, isin, market):
        raise NotImplementedError
