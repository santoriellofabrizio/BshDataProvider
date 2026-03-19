import re
from typing import Optional

from core.instruments.classifier.base_classifier import BaseClassifier

TENOR_PATTERN = re.compile(
    r"^[A-Z]{6}(ON|TN|SN|[0-9]+[DWMY])(?:\s+BGN)?\s+CURN?CY$"
)


class FXForwardClassifier(BaseClassifier):

    def _load(self):
        return None

    def matches(self, identifier: str) -> bool:
        idu = identifier.upper().strip()
        return bool(TENOR_PATTERN.match(idu))

    def get_tenor(self, identifier: str) -> Optional[str]:
        idu = identifier.upper().strip()
        m = TENOR_PATTERN.match(idu)
        if not m:
            return None
        return m.group(1)

    def get_base_currency(self, identifier: str) -> Optional[str]:
        """
        Restituisce la base currency (prime 3 lettere del ticker FX).
        """
        idu = identifier.upper().strip()
        if len(idu) < 3:
            return None
        return idu[:3]  # ES: EURUSD → EUR, USDJPY → USD

    def get_quoted_currency(self, identifier: str) -> Optional[str]:
        """
        Restituisce la quoted currency (lettere 4-6 del ticker FX).
        """
        idu = identifier.upper().strip()
        if len(idu) < 6:
            return None
        return idu[3:6]  # ES: EURUSD → USD, USDJPY → JPY
