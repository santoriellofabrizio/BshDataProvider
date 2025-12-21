from datetime import date, datetime
from typing import List, Union, Optional, Type

from core.enums.datasources import DataSource
from core.enums.fields import StaticField
from core.instruments.instruments import Instrument
from core.requests.requests import (
    BaseStaticRequest, ReferenceRequest, HistoricalRequest, BulkRequest, GeneralRequest
)
from core.requests.subscriptions import SubscriptionBuilder


class StaticRequestBuilder:
    """Costruttore di StaticRequest (Reference, Historical o Bulk)."""

    @staticmethod
    def select_class(fields: List[str], explicit_type: Optional[str] = None) -> Type[BaseStaticRequest]:
        categories = set(StaticField.category(f.upper()) for f in fields)
        if explicit_type:
            explicit_type = explicit_type.lower()
        if len(categories) > 1:
            raise ValueError("cannot mix reference/bulk/historical requests")
        match explicit_type or next(iter(categories)):
                case "historical": return HistoricalRequest
                case "reference": return ReferenceRequest
                case "bulk": return BulkRequest
                case "general": return GeneralRequest
                case _: return ReferenceRequest

    @staticmethod
    def compose_params(
            fields: Union[str, List[str]],
            source: str,
            market: Optional[str] = None,
            start: Optional[Union[str, date, datetime]] = None,
            end: Optional[Union[str, date, datetime]] = None,
            subscriptions: Optional[Union[str, List[str]]] = None,
            **extra_params,
    ) -> dict:
        if isinstance(fields, str):
            fields = [fields]
        params = {
            "fields": [StaticField.from_str(f, source) for f in fields],
            "source": DataSource(source),
            "market": market,
            "start": start,
            "end": end,
            "subscription": subscriptions,
            "extra_params": extra_params,
        }
        StaticRequestBuilder._validate_params(params)
        return params

    @staticmethod
    def _validate_params(params: dict):
        if not params.get("fields"):
            raise ValueError("At least one static field must be provided.")
        if not isinstance(params["source"], DataSource):
            raise TypeError("source must be a DataSource enum")
        if params.get("start") and params.get("end"):
            s, e = params["start"], params["end"]
            if isinstance(s, (date, datetime)) and isinstance(e, (date, datetime)) and s > e:
                raise ValueError("start date cannot be after end date")

    @staticmethod
    def build(
            fields: Union[str, List[str]],
            source: Optional[str],
            instrument: Optional[Instrument] = None,
            market: Optional[str] = None,
            start: Optional[Union[str, date, datetime]] = None,
            end: Optional[Union[str, date, datetime]] = None,
            request_type: Optional[str] = None,
            subscriptions: Optional[str] = None,
            **extra_params,
    ):

        if source is None:
            raise ValueError("source cannot be None")

        fields_list = [fields] if isinstance(fields, str) else fields
        params = StaticRequestBuilder.compose_params(
            fields=fields_list, source=source, market=market,
            start=start, end=end, subscriptions=subscriptions, **extra_params
        )
        if instrument is None:
            request_type = "general"
        req_cls = StaticRequestBuilder.select_class(params["fields"], request_type)
        if request_type == "general":
            return GeneralRequest(**params)
        req = req_cls(instrument=instrument, **params)
        if req.instrument:
            req.subscription = subscriptions or SubscriptionBuilder.build(req)
        return req
