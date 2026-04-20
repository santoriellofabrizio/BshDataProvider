from datetime import date, timedelta, datetime
from functools import lru_cache
from typing import Optional
import pandas as pd
import os


class HolidayManager:
    _instance: Optional['HolidayManager'] = None
    _initialized: bool = False

    def __new__(cls, config_path=None):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, config_path=None):
        # Evita re-inizializzazione se già fatto
        if HolidayManager._initialized:
            return

        if config_path is None:
            config_path = os.path.join(os.path.dirname(__file__), "holidays.yaml")

        from ruamel.yaml import YAML
        yaml = YAML(typ="safe")

        with open(config_path, "r", encoding="utf-8") as f:
            self.config = yaml.load(f) or {}

        if not isinstance(self.config, dict):
            raise ValueError(
                f"Invalid holidays.yaml format at {config_path}: expected mapping, got {type(self.config)}")

        HolidayManager._initialized = True

    @classmethod
    def get_instance(cls, config_path=None) -> 'HolidayManager':
        """Metodo alternativo per ottenere l'istanza singleton."""
        if cls._instance is None:
            cls(config_path)
        return cls._instance

    @lru_cache(maxsize=None)
    def is_holiday(self, dt: date, market: str = "default") -> bool:
        """
        Returns True if the date is a weekend or holiday for the given market.
        """
        market_cfg = self.config["markets"].get(market, {})
        default_cfg = self.config["default"]

        # Check weekend
        if dt.strftime("%A") in default_cfg["weekends"]:
            return True

        # Check fixed holidays
        fixed_days = set(default_cfg.get("fixed"))
        fixed_days.update(market_cfg.get("fixed", {}))
        if dt.strftime("%m-%d") in fixed_days:
            return True

        elif dt in market_cfg.get("other_days",[]):
            return True

        return False

    def is_business_day(self, dt: date, market: str = "default") -> bool:
        """Returns True if the date is a business day (not a weekend or holiday)."""
        return not self.is_holiday(dt, market)

    def next_business_day(self, dt: date, market: str = "default") -> date:
        """Return the next business day after dt."""
        dt += timedelta(days=1)
        while self.is_holiday(dt, market):
            dt += timedelta(days=1)
        return dt

    def previous_business_day(self, dt: date, market: str = "default") -> date:
        """Return the last business day before dt."""
        dt -= timedelta(days=1)
        while self.is_holiday(dt, market):
            dt -= timedelta(days=1)
        return dt

    def get_next_business_day_or_self(self, dt: date, market: str = "default") -> date:
        """
        If dt is a business day, return it. Otherwise return the next business day.
        """
        if self.is_business_day(dt, market):
            return dt
        return self.next_business_day(dt, market)

    def get_previous_business_day_or_self(self, dt: date, market: str = "default") -> date:
        """
        If dt is a business day, return it. Otherwise return the previous business day.
        """
        if self.is_business_day(dt, market):
            return dt
        return self.previous_business_day(dt, market)

    def add_business_days(self, dt: date, n: int, market: str = "default") -> date:
        """
        Add n business days to the given date.
        If n is negative, subtracts business days.
        """
        if n == 0:
            return dt

        direction = 1 if n > 0 else -1
        remaining = abs(n)

        current = dt
        while remaining > 0:
            current += timedelta(days=direction)
            if self.is_business_day(current, market):
                remaining -= 1

        return current

    def subtract_business_days(self, dt: date, n: int, market: str = "default") -> date:
        """Subtract n business days from the given date."""
        return self.add_business_days(dt, -n, market)

    def business_days_between(self, start: date, end: date, market: str = "default", inclusive: str = "both") -> int:
        """
        Count business days between start and end.

        Args:
            start: Start date
            end: End date
            market: Market identifier
            inclusive: "both", "neither", "left", "right"

        Returns:
            Number of business days
        """
        if start > end:
            start, end = end, start

        count = 0
        current = start

        # Gestisci inclusive
        if inclusive in ("neither", "right"):
            current += timedelta(days=1)

        end_inclusive = end
        if inclusive in ("neither", "left"):
            end_inclusive -= timedelta(days=1)

        while current <= end_inclusive:
            if self.is_business_day(current, market):
                count += 1
            current += timedelta(days=1)

        return count

    def get_business_days(self, start: date, end: date, market: str = "default") -> pd.DatetimeIndex:
        """Return all business days between start and end (inclusive)."""
        days = pd.date_range(start, end, freq="D")
        return days[~days.to_series().apply(lambda x: self.is_holiday(x.date(), market))]

    def get_business_days_list(self, start: date, end: date, market: str = "default") -> list[date]:
        """Return all business days between start and end as a list of date objects."""
        business_days = []
        current = start
        while current <= end:
            if self.is_business_day(current, market):
                business_days.append(current)
            current += timedelta(days=1)
        return business_days

    def get_last_n_business_days(
            self,
            n: int,
            end_date: Optional[date] = None,
            market: str = "default",
            inclusive: bool = True
    ) -> list[date]:
        """
        Return the last n business days up to (and optionally including) end_date.

        Args:
            n: Number of business days to return
            end_date: Reference date (default: today)
            market: Market identifier
            inclusive: If True, include end_date if it's a business day

        Returns:
            List of the last n business days in chronological order

        Examples:
            >>> # Get last 5 business days including today
            >>> hm.get_last_n_business_days(5)

            >>> # Get last 10 business days before a specific date (excluding that date)
            >>> hm.get_last_n_business_days(10, date(2025, 12, 20), inclusive=False)
        """
        if end_date is None:
            end_date = date.today()

        if n <= 0:
            raise ValueError(f"n must be positive, got {n}")

        business_days = []
        current = end_date

        # Se inclusive=True e end_date è business day, includilo
        if inclusive and self.is_business_day(current, market):
            business_days.append(current)
            n -= 1

        # Vai indietro fino a trovare n business days
        while n > 0:
            current -= timedelta(days=1)
            if self.is_business_day(current, market):
                business_days.append(current)
                n -= 1

        # Ritorna in ordine cronologico (dal più vecchio al più recente)
        return list(reversed(business_days))

    def get_last_n_business_days_index(
            self,
            n: int,
            end_date: Optional[date] = None,
            market: str = "default",
            inclusive: bool = True
    ) -> pd.DatetimeIndex:
        """
        Return the last n business days as a DatetimeIndex.

        Args:
            n: Number of business days to return
            end_date: Reference date (default: today)
            market: Market identifier
            inclusive: If True, include end_date if it's a business day

        Returns:
            DatetimeIndex of the last n business days in chronological order

        Examples:
            >>> # Get last 5 business days as DatetimeIndex
            >>> hm.get_last_n_business_days_index(5)
        """
        business_days_list = self.get_last_n_business_days(n, end_date, market, inclusive)
        return pd.DatetimeIndex(business_days_list).date