import logging
from datetime import datetime, timedelta, date
from typing import List, Dict, Union

import blpapi

from core.base_classes.base_fetcher import BaseFetcher
from core.enums.frequency import Frequency
from core.requests.requests import BaseRequest
from core.utils.memory_provider import cache_bsh_data
from providers.bloomberg.handlers.base_handlers import IntradayPriceHandler

logger = logging.getLogger(__name__)


class BloombergSnapshotPriceHandler(IntradayPriceHandler, BaseFetcher):
    """
    Handler for Bloomberg snapshot price data.

    Uses IntradayBarRequest to fetch data around a specific snapshot time.
    Returns format: {instrument_id: {field: {date: value}}}
    """

    def __init__(self, show_progress: bool = True):
        super().__init__()
        self.show_progress = show_progress

    def can_handle(self, req: BaseRequest) -> bool:
        """
        This handler handles snapshot requests (requests with snapshot_time).
        """
        return getattr(req, "snapshot_time", None) is not None

    @cache_bsh_data
    def process(
            self,
            requests: List[BaseRequest],
            session: blpapi.Session,
            service: blpapi.Service
    ) -> Dict[str, Dict[str, Dict[date, float]]]:
        """
        Process Bloomberg snapshot price requests.

        Args:
            requests: List of snapshot market requests
            session: Bloomberg session
            service: Bloomberg refdata service

        Returns:
            Dict[instrument_id, Dict[field, Dict[date, value]]]
        """
        if not requests:
            return {}

        results = {}

        # Process each request individually
        for req in requests:
            corr_id = req.instrument.id
            interval_window = req.extra_params.get("interval_window_snapshot", 15)
            result_fields = {}

            logger.info("Fetching Bloomberg snapshot for %s (%s)", corr_id, req.fields)

            for field in req.fields:
                try:
                    if field.upper() == "MID":
                        bid = self._fetch_snapshot_single(session, service, req, "BID", interval_window)
                        ask = self._fetch_snapshot_single(session, service, req, "ASK", interval_window)
                        common_days = bid.keys() & ask.keys()
                        result_fields[field] = {d: (bid[d] + ask[d]) / 2 for d in common_days}
                    else:
                        result_fields[field] = self._fetch_snapshot_single(session, service, req, field, interval_window)
                except Exception as e:
                    logger.warning("Snapshot fetch failed for %s (%s): %s", corr_id, field, e)

            results[corr_id] = result_fields

        return results

    def _fetch_snapshot_single(
            self,
            session: blpapi.Session,
            service: blpapi.Service,
            request: BaseRequest,
            field: str,
            interval_window: int
    ) -> Dict[date, float]:
        """
        Fetch snapshot data for a single field across multiple days using batched requests.

        Args:
            session: Bloomberg session
            service: Bloomberg service
            request: Market data request object
            field: Bloomberg field name (e.g., "BID", "ASK", "LAST")
            interval_window: Time window in minutes around snapshot_time

        Returns:
            Dictionary mapping dates to field values: {date: value}
        """
        values: Dict[date, float] = {}
        bin_interval = self._parse_interval(request.extra_params.get("bin_interval", 5))
        ohlc_field = request.extra_params.get("event", "close")
        corr_id_base = request.instrument.id

        # Map correlation IDs to their corresponding days
        correlation_map: Dict[tuple, tuple] = {}

        # Get all business days in range
        business_days = self.holidays.get_business_days(
            request.start,
            request.end,
            request.market
        )

        if business_days.empty:
            logger.debug("No business days found for %s between %s and %s",
                        corr_id_base, request.start, request.end)
            return values

        # PHASE 1: SEND ALL REQUESTS (Non-blocking batch submission)
        for day in business_days:
            if self.holidays.is_holiday(day, request.market):
                continue

            # Build subscription for this specific day
            sub = (request.subscription(day)
                   if callable(request.subscription)
                   else request.subscription)

            # Calculate time window around snapshot time
            snap_dt = datetime.combine(day, request.snapshot_time)
            start_dt = snap_dt - timedelta(minutes=interval_window)
            end_dt = snap_dt + timedelta(minutes=interval_window)

            # Create unique correlation ID as tuple
            corr_id = (corr_id_base, day.strftime('%Y%m%d'), field)

            try:
                # Create and send request
                req = self._make_intraday_bar_request(
                    service=service,
                    security=sub,
                    event_type=field,
                    interval=bin_interval,
                    start=start_dt,
                    end=end_dt
                )
                session.sendRequest(req, correlationId=blpapi.CorrelationId(corr_id))

                # Store mapping for response matching
                correlation_map[corr_id] = (day, snap_dt)

            except Exception as e:
                logger.warning("Failed to send request for %s %s on %s: %s",
                              corr_id_base, field, day, e)

        if not correlation_map:
            logger.debug("No valid requests sent for %s %s", corr_id_base, field)
            return values

        # PHASE 2: COLLECT ALL RESPONSES (Event-driven collection)
        max_timeout_count = 3
        timeout_count = 0
        max_total_wait = 30000  # 30 seconds total
        total_wait = 0

        while correlation_map and timeout_count < max_timeout_count:
            try:
                ev = session.nextEvent(timeout=5000)

                # Handle timeout events
                if ev.eventType() == blpapi.Event.TIMEOUT:
                    timeout_count += 1
                    total_wait += 5000
                    if total_wait >= max_total_wait:
                        logger.warning("Max total wait time reached for %s %s",
                                      corr_id_base, field)
                        break
                    continue

                timeout_count = 0  # Reset on valid event

                for msg in ev:
                    # Check if this is an IntradayBarResponse
                    if msg.messageType() != blpapi.Name("IntradayBarResponse"):
                        continue

                    # Get correlation ID
                    msg_corr_id = msg.correlationId().value()

                    if msg_corr_id not in correlation_map:
                        logger.debug("Received response for unknown correlation ID: %s", msg_corr_id)
                        continue

                    # Get the day and target time for this response
                    day, snap_dt = correlation_map[msg_corr_id]

                    try:
                        # Parse intraday bars from message
                        bars = self._parse_intraday_bars_from_message(msg, ohlc_field)

                        if bars:
                            # Find bar closest to snapshot time
                            nearest_time = min(bars.keys(), key=lambda t: abs(t - snap_dt))
                            values[day.date()] = bars[nearest_time]
                        else:
                            logger.debug("No bars returned for %s on %s", msg_corr_id, day)

                    except Exception as e:
                        logger.warning("Failed to parse response for %s on %s: %s",
                                      msg_corr_id, day, e)

                    # Remove from pending map
                    del correlation_map[msg_corr_id]

                # Check if event is final response
                if ev.eventType() == blpapi.Event.RESPONSE:
                    logger.debug("Received final RESPONSE event, exiting collection loop")

            except Exception as e:
                logger.error("Error while collecting responses for %s %s: %s",
                            corr_id_base, field, e)
                break

        if correlation_map:
            logger.warning("Exited response loop with %d unfulfilled requests for %s %s",
                          len(correlation_map), corr_id_base, field)

        return values

    @staticmethod
    def _parse_intraday_bars_from_message(msg, ohlc_field: str = "close") -> Dict[datetime, float]:
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
