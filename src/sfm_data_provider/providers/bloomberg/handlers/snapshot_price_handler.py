import logging
from datetime import datetime, timedelta, date
from typing import List, Dict, Union
from collections import defaultdict

import blpapi

from sfm_data_provider.core.base_classes.base_fetcher import BaseFetcher
from sfm_data_provider.core.enums.frequency import Frequency
from sfm_data_provider.core.requests.requests import BaseRequest
from sfm_data_provider.providers.bloomberg.handlers.base_handlers import IntradayPriceHandler

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

    def process(
            self,
            requests: List[BaseRequest],
            session: blpapi.Session,
            service: blpapi.Service
    ) -> Dict[str, Dict[str, Dict[date, float]]]:
        """
        Process Bloomberg snapshot price requests with batched fetching.

        Args:
            requests: List of snapshot market requests
            session: Bloomberg session
            service: Bloomberg refdata service

        Returns:
            Dict[instrument_id, Dict[field, Dict[date, value]]]
        """
        if not requests:
            return {}

        # Group all requests by common parameters to enable batching
        batch_groups = self._group_requests_for_batching(requests)

        # Fetch all data in batches
        all_results = {}
        for batch_key, batch_requests in batch_groups.items():
            batch_results = self._fetch_batch(session, service, batch_requests)
            all_results.update(batch_results)

        return all_results

    def _group_requests_for_batching(self, requests: List[BaseRequest]) -> Dict[tuple, List[BaseRequest]]:
        """
        Group requests that can be batched together.
        Groups by: interval_window, bin_interval, event type, start, end
        """
        groups = defaultdict(list)

        for req in requests:
            interval_window = req.extra_params.get("interval_window_snapshot", 15)
            bin_interval = req.extra_params.get("bin_interval", 5)
            event = req.extra_params.get("event", "close")

            # Group key includes all parameters that must match for batching
            key = (
                interval_window,
                bin_interval,
                event,
                req.start,
                req.end,
                req.market,
                req.snapshot_time.isoformat()
            )
            groups[key].append(req)

        return groups

    def _fetch_batch(
            self,
            session: blpapi.Session,
            service: blpapi.Service,
            requests: List[BaseRequest]
    ) -> Dict[str, Dict[str, Dict[date, float]]]:
        """
        Fetch snapshot data for a batch of requests with the same parameters.
        """
        if not requests:
            return {}

        # Extract common parameters
        first_req = requests[0]
        interval_window = first_req.extra_params.get("interval_window_snapshot", 15)
        bin_interval = self._parse_interval(first_req.extra_params.get("bin_interval", 5))
        ohlc_field = first_req.extra_params.get("event", "close")

        # Get business days (same for all requests in batch)
        business_days = self.holidays.get_business_days(
            first_req.start,
            first_req.end,
            first_req.market
        )

        if business_days.empty:
            logger.debug("No business days found in range")
            return {}

        # Build complete request map: {corr_id: (instrument_id, field, day, snap_dt)}
        correlation_map = {}
        results = defaultdict(lambda: defaultdict(dict))

        # PHASE 1: Send ALL requests for ALL instruments, fields, and days
        total_requests = 0
        for req in requests:
            instrument_id = req.instrument.id

            # Determine which fields to fetch
            fields_to_fetch = []
            for field in req.fields:
                if field.upper() == "MID":
                    fields_to_fetch.extend(["BID", "ASK"])
                else:
                    fields_to_fetch.append(field)

            # Send request for each field and day
            for field in fields_to_fetch:
                for day in business_days:
                    if self.holidays.is_holiday(day, req.market):
                        continue

                    sub = (req.subscription(day)
                           if callable(req.subscription)
                           else req.subscription)

                    snap_dt = datetime.combine(day, req.snapshot_time)
                    start_dt = snap_dt - timedelta(minutes=interval_window)
                    end_dt = snap_dt + timedelta(minutes=interval_window)

                    # Unique correlation ID
                    corr_id = (instrument_id, field, day.strftime('%Y%m%d'))

                    try:
                        req_obj = self._make_intraday_bar_request(
                            service=service,
                            security=sub,
                            event_type=field,
                            interval=bin_interval,
                            start=start_dt,
                            end=end_dt
                        )
                        session.sendRequest(req_obj, correlationId=blpapi.CorrelationId(corr_id))

                        correlation_map[corr_id] = (instrument_id, field, day, snap_dt)
                        total_requests += 1

                    except Exception as e:
                        logger.warning("Failed to send request for %s %s on %s: %s",
                                       instrument_id, field, day, e)

        if not correlation_map:
            logger.debug("No valid requests sent")
            return {}

        logger.info("Sent %d snapshot requests, waiting for responses...", total_requests)

        # PHASE 2: Collect ALL responses
        timeout_count = 0
        max_timeout_count = 5
        max_total_wait = 60000  # 60 seconds
        total_wait = 0
        responses_received = 0

        while correlation_map and timeout_count < max_timeout_count:
            try:
                ev = session.nextEvent(timeout=5000)

                if ev.eventType() == blpapi.Event.TIMEOUT:
                    timeout_count += 1
                    total_wait += 5000
                    if total_wait >= max_total_wait:
                        logger.warning("Max total wait time reached. Received %d/%d responses",
                                       responses_received, total_requests)
                        break
                    continue

                timeout_count = 0

                for msg in ev:
                    if msg.messageType() != blpapi.Name("IntradayBarResponse"):
                        continue

                    msg_corr_id = msg.correlationId().value()
                    if msg_corr_id not in correlation_map:
                        continue

                    instrument_id, field, day, snap_dt = correlation_map[msg_corr_id]

                    try:
                        bars = self._parse_intraday_bars_from_message(msg, ohlc_field)

                        if bars:
                            nearest_time = min(bars.keys(), key=lambda t: abs(t - snap_dt))
                            value = bars[nearest_time]

                            # Store intermediate result
                            results[instrument_id][f"_{field}"][day.date()] = value
                            responses_received += 1

                    except Exception as e:
                        logger.warning("Failed to parse response for %s %s on %s: %s",
                                       instrument_id, field, day, e)

                    del correlation_map[msg_corr_id]

                # NON uscire su RESPONSE - continua finché correlation_map non è vuota
                # RESPONSE indica solo la fine di UNA richiesta, non di tutte

                # Opzionale: log progress periodico
                if responses_received % 100 == 0:
                    logger.debug("Progress: %d/%d responses received, %d pending",
                                 responses_received, total_requests, len(correlation_map))

            except Exception as e:
                logger.error("Error collecting responses: %s", e)
                break

        if correlation_map:
            logger.warning("Exited with %d/%d unfulfilled requests",
                           len(correlation_map), total_requests)

        if correlation_map:
            logger.warning("Exited with %d/%d unfulfilled requests",
                           len(correlation_map), total_requests)

        # PHASE 3: Post-process results (compute MID, organize output)
        final_results = {}
        for req in requests:
            instrument_id = req.instrument.id
            final_results[instrument_id] = {}

            for field in req.fields:
                if field.upper() == "MID":
                    # Compute MID from BID and ASK
                    bid_data = results[instrument_id].get("_BID", {})
                    ask_data = results[instrument_id].get("_ASK", {})
                    common_days = bid_data.keys() & ask_data.keys()
                    final_results[instrument_id][field] = {
                        d: (bid_data[d] + ask_data[d]) / 2
                        for d in common_days
                    }
                else:
                    final_results[instrument_id][field] = results[instrument_id].get(f"_{field}", {})

        logger.info("Snapshot fetch complete: %d/%d responses received",
                    responses_received, total_requests)

        return final_results

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