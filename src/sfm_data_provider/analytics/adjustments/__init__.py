"""
Return adjustments for financial instruments.

Public API:
    - Adjuster: Main orchestrator
    - Component: Base class for components
    - ETF components: TerComponent, YtmComponent, DividendComponent, etc.
    
Usage:
    from bshdata.analytics.adjustments import Adjuster
    from bshdata.analytics.adjustments.components.etf import TerComponent, YtmComponent
    
    intraday_adjuster = (
        Adjuster(prices, fx_prices_intraday, instruments)
        .add(TerComponent(ters))
        .add(YtmComponent(ytms))
    )
    
    adjustments = intraday_adjuster.calculate()
"""

__version__ = '0.1.0'

__all__ = [
    # Core
    'Adjuster',
    'Component',
    'BondAccruedInterestComponent',
    'SpecialtyEtfCarryComponent',

]

from sfm_data_provider.analytics.adjustments.adjuster import Adjuster
from sfm_data_provider.analytics.adjustments.component import Component
from sfm_data_provider.analytics.adjustments.bond import BondAccruedInterestComponent
from sfm_data_provider.analytics.adjustments.specialty_ytm import SpecialtyEtfCarryComponent
