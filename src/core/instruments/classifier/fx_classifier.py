# fx_classifier.py
from typing import Literal, Optional
import pandas as pd

from core.enums.currencies import CurrencyEnum
from .base_classifier import BaseClassifier


class FXClassifier(BaseClassifier):
    """
    Classifier for FX currency pairs and individual currencies.

    Handles:
    - CurrencyInstrument pair validation (EURUSD, GBPJPY, etc.)
    - Individual currency validation and metadata
    - Subunit currencies (GBX, GBp, etc.)
    """

    _df: Optional[pd.DataFrame] = None

    def _load(self):
        """Load currency data from Oracle."""
        if self._df is None:
            data = self.oracle.get_currency_data()
            self._df = pd.DataFrame(data)

            # Normalize column names to uppercase (match Oracle query)
            self._df.columns = [col.upper() for col in self._df.columns]

    def matches_pair(self, identifier: str) -> bool:
        """
        Check if identifier is a valid currency pair.

        Args:
            identifier: String to check (e.g., 'EURUSD', 'EUR USD')

        Returns:
            True if valid currency pair (6 chars, both parts valid currencies)

        Example:
            >>> fx.matches_pair('EURUSD')
            True
            >>> fx.matches_pair('EUR USD')
            True
            >>> fx.matches_pair('USD')  # Single currency
            False
        """
        s = identifier.replace(" ", "")

        if len(s) != 6:
            return False

        base_ccy = s[:3]
        quote_ccy = s[3:]

        return CurrencyEnum.exists(base_ccy) and CurrencyEnum.exists(quote_ccy)

    def matches(self, identifier: str) -> bool:
        """
        Check if identifier is a valid single currency.

        Args:
            identifier: String to check (e.g., 'USD', 'GBX')

        Returns:
            True if valid single currency code (3 chars)

        Example:
            >>> fx.matches('USD')
            True
            >>> fx.matches('EURUSD')  # Pair, not single
            False
        """
        s = identifier.strip()

        if len(s) != 3:
            return False

        return CurrencyEnum.exists(s)

    def get_currency_type(self, identifier: str) -> Optional[str]:
        """
        Get currency type from database.

        Args:
            identifier: CurrencyInstrument code

        Returns:
            'STANDARD', 'SUBUNIT', 'FUNDS CODE', or None

        Example:
            >>> fx.get_currency_type('GBX')
            'SUBUNIT'
            >>> fx.get_currency_type('USD')
            'STANDARD'
        """
        self._load()

        code = identifier.strip()
        result = self._df.loc[self._df["CURRENCY_CODE"] == code, "CURRENCY_TYPE"]

        return result.iloc[0] if len(result) > 0 and not pd.isna(result.iloc[0]) else None

    def get_currency_multiplier(self, identifier: str) -> Optional[float]:
        """
        Get currency multiplier for subunit currencies.

        Args:
            identifier: CurrencyInstrument code (e.g., 'GBX')

        Returns:
            Multiplier (e.g., 100.0 for GBX) or None if not a subunit

        Example:
            >>> fx.get_currency_multiplier('GBX')
            100.0
            >>> fx.get_currency_multiplier('USD')
            None
        """
        self._load()

        code = identifier.strip()
        result = self._df.loc[self._df["CURRENCY_CODE"] == code, "CURRENCY_MULTIPLIER"]

        if len(result) > 0 and not pd.isna(result.iloc[0]):
            return float(result.iloc[0])

        return None

    def get_reference_currency(self, identifier: str) -> Optional[str]:
        """
        Get reference (principal) currency for subunit currencies.

        Args:
            identifier: Subunit currency code (e.g., 'GBX')

        Returns:
            Principal currency code string (e.g., 'GBP') or None

        Example:
            >>> fx.get_reference_currency('GBX')
            'GBP'
            >>> fx.get_reference_currency('USD')
            None
        """
        self._load()

        code = identifier.strip()
        result = self._df.loc[self._df["CURRENCY_CODE"] == code, "CURRENCY_CODE_PRINCIPAL"]

        if len(result) > 0 and not pd.isna(result.iloc[0]):
            return result.iloc[0]

        return None

    def get_base_currency(self, pair: str) -> str:
        """
        Extract base currency from pair.

        Args:
            pair: CurrencyInstrument pair (e.g., 'EURUSD', 'EUR USD')

        Returns:
            Base currency code (e.g., 'EUR')
        """
        s = pair.replace(" ", "")

        if not self.matches_pair(s):
            raise ValueError(f"Invalid currency pair: {pair}")

        return s[:3]

    def get_quote_currency(self, pair: str) -> str:
        """
        Extract quote currency from pair.

        Args:
            pair: CurrencyInstrument pair (e.g., 'EURUSD')

        Returns:
            Quote currency code (e.g., 'USD')
        """
        s = pair.replace(" ", "")

        if not self.matches_pair(s):
            raise ValueError(f"Invalid currency pair: {pair}")

        return s[3:]