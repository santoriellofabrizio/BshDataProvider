# swap_classifier.py
import pandas as pd
from .base_classifier import BaseClassifier


class SwapClassifier(BaseClassifier):

    def _load(self):
        if self._df is None:
            if not self.oracle:
                raise RuntimeError("SwapClassifier: manca QueryOracle")
            self._df = pd.DataFrame(self.oracle.get_swap_data())
        return self._df

    # ------------------------------------------------------------
    def matches(self, identifier: str) -> bool:
        idu = identifier.upper()
        df = self._load()

        # Dataset ufficiale
        if idu in df["TICKER"].str.upper().values:
            return True

        # Tenor interno (EUZCISWAP5)
        if self.extract_tenor(idu) is not None:
            return True

        # Prefissi noti
        prefixes = ("EUSWI", "ILSWI", "USSWIT", "EUSW", "USOSFRC")
        if idu.startswith(prefixes):
            return True

        # Euristica Bloomberg
        if "SWI" in idu:
            return True

        return False

    def extract_tenor(self, idu: str) -> str | None:
        df = self._load()

        # Dataset
        tenor = df.loc[df["TICKER"] == idu, "TENOR"]
        if not tenor.empty:
            return tenor.iloc[0].upper()

        # Pattern interni
        for root in ("EUZCISWAP", "USZCISWAP"):
            if idu.startswith(root):
                t = idu[len(root):]
                if t.isdigit():
                    return f"{t}Y"

        return None

    def get_ccy(self, isin, market):
        raise NotImplementedError