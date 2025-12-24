"""
Type definitions and protocols for adjustments system.
"""
from typing import Protocol, runtime_checkable
from datetime import date, datetime
from typing import Union, List
import pandas as pd


@runtime_checkable
class InstrumentProtocol(Protocol):
    """Protocol for instrument objects"""

    @property
    def id(self) -> str:
        """Instrument identifier"""
        ...

    @property
    def isin(self) -> str | None:
        """ISIN code"""
        ...

    @property
    def type(self):
        """Instrument type (InstrumentType enum)"""
        ...

    @property
    def currency(self) -> str:
        """Trading currency"""
        ...


@runtime_checkable
class EtfInstrumentProtocol(InstrumentProtocol, Protocol):
    """Protocol for ETF-specific attributes"""

    @property
    def underlying_type(self) -> str | None:
        """Underlying asset type (EQUITY, FIXED INCOME, etc)"""
        ...

    @property
    def payment_policy(self) -> str | None:
        """Payment policy (DIST, ACC, INC)"""
        ...

    @property
    def fund_currency(self) -> str | None:
        """Fund currency (dividend currency)"""
        ...


class ComponentProtocol(Protocol):
    """Protocol for adjustment components"""

    def is_applicable(self, instrument: InstrumentProtocol) -> bool:
        """Check if component applies to instrument (domain logic only)"""
        ...

    def should_apply(self, instrument: InstrumentProtocol) -> bool:
        """Check if component should apply (domain + target filter)"""
        ...

    def validate_input(self, instruments: dict[str, InstrumentProtocol], dates: Union[List[date], List[datetime]], prices: pd.DataFrame) -> None:
        """Validate input data, raise ValueError if invalid"""
        ...

    def validate_output(self, result: pd.DataFrame) -> None:
        """Validate output data, raise ValueError if invalid"""
        ...

    def calculate_adjustment(
        self,
        instruments: dict[str, InstrumentProtocol],
        dates: Union[List[date], List[datetime]],
        prices: pd.DataFrame,
    ) -> pd.DataFrame:
        """
        Calculate adjustments for multiple instruments.

        Returns:
            DataFrame(dates A- instruments)
        """
        ...
