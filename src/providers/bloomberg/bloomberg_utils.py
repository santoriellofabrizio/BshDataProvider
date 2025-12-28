"""
Bloomberg utility functions.

This module contains common utility functions used across Bloomberg fetchers.
These are stateless helper functions that don't depend on session state.
"""

import logging
from datetime import datetime
from typing import Union, List, Dict, Any

import blpapi

from core.enums.frequency import Frequency

logger = logging.getLogger(__name__)


# ============================================================
# BLOOMBERG REQUEST HELPERS
# ============================================================

def append_values(req, element: str, values: List[str]) -> None:
    """
    Append multiple values to a Bloomberg request element.

    Args:
        req: Bloomberg request object
        element: Element name (e.g., "securities", "fields")
        values: List of values to append
    """
    el = req.getElement(element)
    for v in values:
        el.appendValue(v)


def parse_interval(interval: Union[str, int, Frequency]) -> int:
    """
    Parse interval specification into minutes.

    Args:
        interval: Interval as Frequency enum, integer (minutes), or string ("5m", "1h")

    Returns:
        Interval in minutes

    Raises:
        ValueError: If interval format is not recognized

    Examples:
        >>> parse_interval(Frequency.MIN_5)
        5
        >>> parse_interval("15m")
        15
        >>> parse_interval("1h")
        60
    """
    if isinstance(interval, Frequency):
        interval = interval.value
    if isinstance(interval, int):
        return interval
    s = str(interval).strip().lower()
    if s.endswith("m"):
        return int(s[:-1])
    if s.endswith("h"):
        return int(s[:-1]) * 60
    raise ValueError(f"Unsupported interval: {interval}")


# ============================================================
# BLOOMBERG MESSAGE PARSERS
# ============================================================

def parse_intraday_bars_from_message(msg, ohlc_field: str = "close") -> Dict[datetime, float]:
    """
    Parse intraday bars from a Bloomberg IntradayBarResponse message.

    Args:
        msg: Bloomberg message object
        ohlc_field: Which OHLC field to extract ("open", "high", "low", "close")

    Returns:
        Dictionary mapping bar timestamps to values: {datetime: value}
    """
    bars = {}

    try:
        if not msg.hasElement("barData"):
            return bars

        bar_data = msg.getElement("barData")

        if not bar_data.hasElement("barTickData"):
            return bars

        bar_tick_data = bar_data.getElement("barTickData")

        for i in range(bar_tick_data.numValues()):
            bar_element = bar_tick_data.getValueAsElement(i)

            if bar_element.hasElement("time") and bar_element.hasElement(ohlc_field):
                bar_time = bar_element.getElementAsDatetime("time")
                bar_value = bar_element.getElementAsFloat(ohlc_field)
                bars[bar_time] = bar_value

    except Exception as e:
        logger.warning("Error parsing intraday bars from message: %s", e)

    return bars


def parse_element(element) -> Dict[str, Any]:
    """
    Recursively parse a complex Bloomberg element.

    Args:
        element: Bloomberg element object

    Returns:
        Dictionary representation of the element
    """
    record = {}
    for sub in element.elements():
        name = str(sub.name())
        if sub.isArray():
            record[name] = [
                parse_element(sub.getValueAsElement(k))
                for k in range(sub.numValues())
            ]
        elif sub.isComplexType():
            record[name] = parse_element(sub)
        else:
            try:
                record[name] = sub.getValue()
            except Exception as e:
                logger.info("Error getting value for %s: %s", name, e)
    return record


# ============================================================
# BLOOMBERG SECURITY CONVERTERS
# ============================================================

def convert_to_bloomberg_code(security: str) -> str:
    """
    Convert security identifier to Bloomberg format.

    Handles ISIN conversion to Bloomberg /isin/ format.

    Args:
        security: Security identifier (ticker or ISIN)

    Returns:
        Bloomberg-formatted security code

    Examples:
        >>> convert_to_bloomberg_code("US0378331005 ISIN")
        "/isin/US0378331005"
        >>> convert_to_bloomberg_code("AAPL US Equity")
        "AAPL US Equity"
    """
    if security.upper().endswith(" ISIN"):
        isin = security.split()[0]
        return f"/isin/{isin}"
    return security
