import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any, Callable, Union

import blpapi

from core.base_classes.base_fetcher import BaseFetcher
from core.enums.frequency import Frequency
from core.requests.requests import BaseRequest
from core.utils.memory_provider import cache_bsh_data
from providers.bloomberg.handlers.base_handlers import IntradayPriceHandler

logger = logging.getLogger(__name__)


class BloombergIntradayPriceHandler(IntradayPriceHandler, BaseFetcher):
    """
    Handler for Bloomberg intraday price data.

    Uses IntradayBarRequest to fetch intraday bar data.
    Returns format: {instrument_id: {field: {datetime: value}}}
    """

    def __init__(self, show_progress: bool = True):
        super().__init__()
        self.show_progress = show_progress

    def can_handle(self, req: BaseRequest) -> bool:
        """
        This handler handles intraday market data requests (not snapshot).
        """
        # Check if it's NOT a daily frequency and NOT a snapshot
        freq = str(getattr(req, "frequency", "")).lower()
        has_snapshot_time = getattr(req, "snapshot_time", None) is not None
        return "d" not in freq and not has_snapshot_time

    @cache_bsh_data
    def process(
            self,
            requests: List[BaseRequest],
            session: blpapi.Session,
            service: blpapi.Service
    ) -> Dict[str, Dict[str, Dict[datetime, float]]]:
        """
        Process Bloomberg intraday price requests.

        Args:
            requests: List of intraday market requests
            session: Bloomberg session
            service: Bloomberg refdata service

        Returns:
            Dict[instrument_id, Dict[field, Dict[datetime, value]]]
        """
        if not requests:
            return {}

        results = {}

        # Process each request individually (intraday is not easily batchable)
        for req in requests:
            corr_id = req.instrument.id
            interval = self._parse_interval(req.frequency)
            ohlc_field = req.extra_params.get("ohlc_field", "close")
            result_fields: Dict[str, Dict[datetime, float]] = {}

            # Skip if holiday
            if self.holidays.is_holiday(req.start.date(), req.market):
                logger.info("Skipping intraday fetch: %s is a holiday for %s", req.start.date(), req.market)
                continue

            # Get subscription
            sub = (req.subscription(current_date=req.start)
                   if isinstance(req.subscription, Callable)
                   else req.subscription)

            # Process each field
            for field in req.fields:
                field_upper = field.upper()
                try:
                    if field_upper in ["MID", "SPREAD", "SPREAD_PCT"]:
                        bid_bars = self._parse_intraday_response(
                            session,
                            self._make_intraday_bar_request(service, sub, "BID", interval, req.start, req.end),
                            ohlc_field,
                        )
                        ask_bars = self._parse_intraday_response(
                            session,
                            self._make_intraday_bar_request(service, sub, "ASK", interval, req.start, req.end),
                            ohlc_field,
                        )
                        common = bid_bars.keys() & ask_bars.keys()
                        match field_upper:
                            case "MID":
                                result_fields[field] = {t: (bid_bars[t] + ask_bars[t]) / 2 for t in common}
                            case "SPREAD":
                                result_fields[field] = {t: (ask_bars[t] - bid_bars[t]) / 2 for t in common}
                            case "SPREAD_PCT":
                                result_fields[field] = {
                                    t: (ask_bars[t] - bid_bars[t]) / ((ask_bars[t] + bid_bars[t]) / 2)
                                    for t in common
                                    if (ask_bars[t] + bid_bars[t]) != 0
                                }
                    else:
                        req_obj = self._make_intraday_bar_request(service, sub, field_upper, interval, req.start, req.end)
                        result_fields[field_upper] = self._parse_intraday_response(session, req_obj, ohlc_field)
                except Exception as e:
                    logger.warning("Intraday fetch failed for %s (%s): %s", corr_id, field_upper, e)

            results[corr_id] = result_fields

        return results

    def _make_intraday_bar_request(
            self,
            service: blpapi.Service,
            security: str,
            event_type: str,
            interval: int,
            start: datetime,
            end: datetime
    ):
        """Create Bloomberg IntradayBarRequest."""
        req = service.createRequest("IntradayBarRequest")
        req.set("security", security)
        req.set("eventType", event_type.upper())
        req.set("interval", interval)
        req.set("gapFillInitialBar", "true")
        req.set("startDateTime", start.strftime("%Y-%m-%dT%H:%M:%S"))
        req.set("endDateTime", end.strftime("%Y-%m-%dT%H:%M:%S"))
        return req

    def _parse_intraday_response(
            self,
            session: blpapi.Session,
            req,
            ohlc_field: str = "close"
    ) -> Dict[datetime, float]:
        """Execute IntradayBarRequest and return {time: value}."""
        session.sendRequest(req)
        values = {}
        while True:
            ev = session.nextEvent()
            for msg in ev:
                if msg.messageType() == blpapi.Name("IntradayBarResponse") and msg.hasElement("barData"):
                    bar_data = msg.getElement("barData").getElement("barTickData")
                    for i in range(bar_data.numValues()):
                        el = bar_data.getValueAsElement(i)
                        t = el.getElementAsDatetime("time")
                        v = el.getElementAsFloat(ohlc_field)
                        values[t] = v
            if ev.eventType() == blpapi.Event.RESPONSE:
                break
        return values

    @staticmethod
    def _parse_interval(interval: Union[str, int, Frequency]) -> int:
        """Parse interval into minutes."""
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
