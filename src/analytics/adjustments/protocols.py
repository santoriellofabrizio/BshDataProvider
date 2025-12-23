"""
Type definitions and protocols for adjustments system.
"""
from typing import Protocol, runtime_checkable
from datetime import date
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
    
    def is_applicable(self, instrument_id: str, instrument: InstrumentProtocol) -> bool:
        """Check if component applies to instrument"""
        ...
    
    def calculate_adjustment(
        self,
        instrument_ids: list[str],
        instruments: dict[str, InstrumentProtocol],
        dates: list[date],
        prices: pd.DataFrame,
        fx_prices: pd.DataFrame,
    ) -> pd.DataFrame:
        """
        Calculate adjustments for multiple instruments.
        
        Returns:
            DataFrame(dates × instruments)
        """
        ...

    # In protocols.py
    class UpdatableComponent(Protocol):
        """Component that can receive live data updates"""

        def update_data(self, **kwargs) -> None:
            """Update component data with new live values"""
            ...
