from enum import Enum

class Frequency(str, Enum):
    """Standardized frequency units used in providers."""

    TICK = "tick"
    ONE_MIN = "1m"
    TWO_MIN = "2m"
    FIVE_MIN = "5m"
    TEN_MIN = "10m"
    FIFTEEN_MIN = "15m"
    THIRTY_MIN = "30m"
    HOURLY = "1h"
    DAILY = "1d"

    @classmethod
    def from_str(cls, value: str | int) -> "Frequency":
        # Se è un intero, converti in stringa con unità
        if isinstance(value, int):
            int_map = {
                1: "1m",
                10: "10m",
                5: "5m",
                15: "15m",
                30: "30m",
                60: "1h",
                1440: "1d",
            }
            normalized = int_map.get(value)
            if normalized:
                return cls(normalized)
            else:
                raise ValueError(f"Unsupported frequency (int): {value}")

        # Se è una stringa, normalizza
        val = value.strip().lower()
        aliases = {
            "tick": "tick",
            "1min": "1m",
            "5min": "5m",
            "15min": "15m",
            "30min": "30m",
            "hourly": "1h",
            "1h": "1h",
            "daily": "1d",
            "1day": "1d",
        }
        normalized = aliases.get(val, val)
        try:
            return cls(normalized)
        except ValueError:
            raise ValueError(f"Unsupported frequency: {value}")