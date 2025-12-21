import logging
from datetime import date, datetime, time
from typing import List, Union, Optional, Type

from core.enums.datasources import DataSource
from core.enums.instrument_types import InstrumentType
from core.enums.markets import normalize_market, Market
from core.requests.requests import DailyRequest, IntradayRequest
from core.requests.subscriptions import SubscriptionBuilder

logger = logging.getLogger(__name__)


class MarketRequestBuilder:
    """Costruttore di MarketRequest (DailyRequest o IntradayRequest)."""

    # ============================================================
    # CLASS SELECTION
    # ============================================================
    @staticmethod
    def select_class(frequency: Optional[str]) -> Type[DailyRequest | IntradayRequest]:
        if not frequency:
            return DailyRequest
        return DailyRequest if frequency.lower() in ("1d", "daily") else IntradayRequest

    # ============================================================
    # PARAMETER COMPOSITION
    # ============================================================
    @staticmethod
    def compose_params(
            type_: str,
            start: Union[str, date, datetime],
            end: Optional[Union[str, date, datetime]],
            market: Optional[str],
            source: Optional[str],
            frequency: Optional[str],
            fields: Union[str, List[str]],
            snapshot_time: Optional[time],
            subscription: Optional[str] = None,
            **extra_params,
    ) -> dict:
        if isinstance(fields, str):
            fields = [fields]

        src_enum = DataSource(source)
        logical_market = market
        mkt = normalize_market(logical_market, src_enum) if type_ != InstrumentType.CURRENCYPAIR else "FX"

        if end and isinstance(start, (date, datetime)) and start > end:
            raise ValueError(f"Invalid time range: start {start} > end {end}")

        params = {
            "fields": fields,
            "start": start,
            "end": end,
            "market": mkt,
            "source": src_enum,
            "frequency": frequency,
            "subscription": subscription,
            "extra_params": extra_params,
        }

        if frequency in ("1d", "daily"):
            params["snapshot_time"] = snapshot_time

        MarketRequestBuilder._validate_params({**params, "logical_market": logical_market})
        return params

    # ============================================================
    # VALIDATION
    # ============================================================
    @staticmethod
    def _validate_params(params: dict):
        required = ["fields", "start", "source","frequency"]
        missing = [p for p in required if not params.get(p)]
        if missing:
            raise ValueError(f"Missing required market params: {missing}")

        if not isinstance(params["fields"], list):
            raise TypeError("fields must be a list")
        if not isinstance(params["source"], DataSource):
            raise TypeError("source must be a DataSource enum")

        match params["source"]:
            case DataSource.TIMESCALE:
                mkt = params["market"]
                if mkt and mkt not in ["EUREX", "EURONEXT", "FX"] + list(Market.get_timescale_segments().keys()):
                    logger.warning(f"{params['logical_market']} not implemented for TimescaleRequest. "
                                   f"EURONEXT/EUREX + CurrencyEnum will be used")

            case DataSource.ORACLE:
                raise NotImplementedError("market request for oracle provider are not implemented")

    # ============================================================
    # BUILD REQUEST
    # ============================================================
    @staticmethod
    def build(
            instrument,
            start,
            end,
            market,
            source,
            frequency,
            fields,
            snapshot_time,
            subscription=None,
            **kwargs,
    ):
        if instrument is None:
            raise ValueError("instrument cannot be None")

        req_cls = MarketRequestBuilder.select_class(frequency)
        params = MarketRequestBuilder.compose_params(
            type_=instrument.type, start=start, end=end, market=market,
            source=source, frequency=frequency, fields=fields,
            snapshot_time=snapshot_time, subscription=subscription, **kwargs
        )

        req = req_cls(instrument=instrument, **params)
        req.subscription = subscription or SubscriptionBuilder.build(req)
        return req
