import logging
import re
import warnings
import pandas as pd
from typing import Optional

from .base_classifier import BaseClassifier

logger = logging.getLogger(__name__)


class IndexClassifier(BaseClassifier):

    # ---------------------------------------------
    # Data loading
    # ---------------------------------------------
    def _load(self):
        if self._df is None:
            if not self.oracle:
                raise RuntimeError("IndexClassifier: manca QueryOracle")
            self._rate_df = pd.DataFrame(self.oracle.get_rates_index_data())
        return self._rate_df

    # ---------------------------------------------
    # LOOKUP
    # ---------------------------------------------
    def lookup_by_ticker(self, ticker: str) -> Optional[dict]:
        df = self._load()
        t = (ticker or "").upper()
        mask = df["TICKER"].str.upper() == t
        if mask.any():
            return df.loc[mask].iloc[0].to_dict()
        return None

    # ---------------------------------------------
    # FAMILY
    # ---------------------------------------------
    def get_family(self, identifier: str) -> Optional[str]:
        """
        Cerca la famiglia partendo da:
        - TICKER (EUR003M -> EURIBOR)
        - FAMILY diretto (EURIBOR)
        """
        df = self._load()
        idu = (identifier or "").upper()

        # se è già una family
        if idu in df["FAMILY"].str.upper().unique():
            return idu

        # se è un ticker
        row = self.lookup_by_ticker(idu)
        if row:
            return row["FAMILY"]

        return None

    # ---------------------------------------------
    # TENOR
    # ---------------------------------------------
    def get_tenor(self, identifier: str, family: Optional[str]) -> Optional[str]:
        """
        Regole:
        - Se identifier è un ticker noto -> usa dataset
        - Se explicit tenor non c'è -> fallback per family
        """
        df = self._load()
        idu = (identifier or "").upper()

        # 1) se è un ticker conosciuto
        row = self.lookup_by_ticker(idu)
        if row:
            return row["TENOR"]

        # 2) se non c’è tenor ma c’è una family
        if family:
            fam_u = family.upper()
            # default: overnight
            logger.info(f"Tenor mancante, uso default TENOR='1D' per family {fam_u} o provo estrazione")
            return self.extract_tenor(idu) or "1D"
        else:
            self.extract_tenor(idu)

    # ---------------------------------------------
    # TICKER derivato da FAMILY + TENOR
    # ---------------------------------------------
    def get_ticker(self, family: str, tenor: str) -> Optional[str]:
        df = self._load()

        fam_u = (family or "").upper()
        ten_u = (tenor or "").upper()

        mask = (df["FAMILY"].str.upper() == fam_u) & (df["TENOR"].str.upper() == ten_u)
        if mask.any():
            return df.loc[mask, "TICKER"].iloc[0]

        return None

    # ---------------------------------------------
    # CURRENCY dal FAMILY
    # ---------------------------------------------
    def get_currency_from_family(self, family: str) -> Optional[str]:
        df = self._load()
        fam_u = (family or "").upper()
        mask = df["FAMILY"].str.upper() == fam_u

        if mask.any():
            return df.loc[mask, "CURRENCY"].iloc[0]

        return None

    def matches(self, identifier: str) -> bool:
        """
        Determina se `identifier` rappresenta un Interest Rate Index.
        Matcha su:
          - TICKER (EUR003M, ESTRON, SOFRRATE)
          - FAMILY (EURIBOR, ESTR, SOFR)
          - pattern noti per indici dei tassi
        """
        idu = (identifier or "").upper()
        if not idu:
            return False

        df = self._load()

        if "TICKER" in df.columns:
            if idu in df["TICKER"].str.upper().values:
                return True

        if "FAMILY" in df.columns:
            if idu in df["FAMILY"].str.upper().values:
                return True

        if re.match(r"^[A-Z]{3,5}\d{1,3}[MD]$", idu):
            return True

        if idu.endswith("ON"):
            return True

        if idu.endswith("RATE"):
            return True

        if "ON" in idu or "RATE" in idu or 'INDEX':
            return True

        return False

    def get_ticker_by_id(self, identifier: str, tenor: Optional[str]) -> Optional[str]:
        """
        Dato un identificatore che può essere:

            - un TICKER (EUR003M, ESTRON, SOFRRATE)
            - una FAMILY (EURIBOR, ESTR, SOFR)

        restituisce sempre un TICKER valido.

        Se identifier è una family, richiede un tenor.
        Se tenor è None, assume "1D" con warning.
        """
        if not identifier:
            return None

        idu = identifier.upper()
        df = self._load()

        if idu in df["TICKER"].str.upper().values:
            return idu
        # --------------------------------------------------------------
        if idu in df["FAMILY"].str.upper().values:
            # Tenor mancante -> warning + default 1D
            if not tenor:
                logger.info(f"Tenor missing for Index Family {idu}. Using default tenor '1D'.")
                tenor = "1D"

            ten_u = tenor.upper()

            mask = (
                           df["FAMILY"].str.upper() == idu
                   ) & (
                           df["TENOR"].str.upper() == ten_u
                   )

            if mask.any():
                return df.loc[mask, "TICKER"].iloc[0]
            logger.warning(f"No ticker found for family={idu} tenor={ten_u}")
            return None
        else:
            if self.has_family(idu):
                family = self.has_family(idu)
                tenor = self.extract_tenor(idu)
                ticker = self.get_ticker(family, tenor)
                return ticker
            return None

    def has_family(self, idu: str) -> Optional[str]:
        df = self._load()
        for tnr in df["TENOR"].unique():
            if tnr in idu:
                tenor = tnr
                try_family = idu.replace(tnr, "")
                if try_family in df["FAMILY"].unique():
                    return try_family
        return None

    def extract_tenor(self, idu: str) -> Optional[str]:
        df = self._load()
        for tnr in df["TENOR"].unique():
            if tnr in idu:
                return tnr
        return None
