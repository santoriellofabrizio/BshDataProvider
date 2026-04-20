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
    def matches(self, identifier: str) -> str:
        idu = identifier.upper()
        df = self._load()

        # Dataset ufficiale
        if idu in df["TICKER"].str.upper().values:
            return idu

        # Prefissi noti
        prefixes = ("EUSWI", "ILSWI", "USSWIT", "EUSW", "USOSFRC")
        if idu.startswith(prefixes):
            return True

        # Euristica Bloomberg
        if "SWI" in idu:
            return True

        return False

    def extract_ticker(self, symbol: str) -> str | None:
        symbol = symbol.upper()
        df = self._load()
        if 'USZCISWAP' in symbol:
            symbol = symbol.replace('USZCISWAP', 'USSWIT')
        elif 'EUZCISWAP' in symbol:
            symbol = symbol.replace('EUZCISWAP', 'EUSWI')
        elif 'ESTR3M' in symbol:
            symbol = symbol.replace('ESTR3M', 'EESWEC')
        elif 'SOFR3M' in symbol:
            symbol = symbol.replace('SOFR3M', 'USOSFRC')
        # Dataset
        ticker = df.loc[df["TICKER"] == symbol, "TICKER"]
        if not ticker.empty:
            return ticker.iloc[0].upper()

    def extract_tenor(self, idu: str) -> str | None:
        df = self._load()
        symbol = self.extract_ticker(idu)
        if symbol is not None:
            tenor = df.loc[df["TICKER"] == symbol, "TENOR"]
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