import logging
from datetime import date, timedelta
from typing import List, Dict, Any, Set

import blpapi
from dateutil.utils import today

from sfm_data_provider.core.requests.requests import BaseRequest
from sfm_data_provider.providers.bloomberg.handlers.base_handlers import HistoricalFieldHandler

logger = logging.getLogger(__name__)


class BloombergHistoricalHandler(HistoricalFieldHandler):
    """
    Handler for Bloomberg HistoricalDataRequest.

    Processes time-series data using Bloomberg's HistoricalDataRequest API.
    Returns format: {subscription: {field: {date: value}}}
    """

    def can_handle(self, req: BaseRequest) -> bool:
        """
        This handler can handle any historical request.
        """
        return req.request_type == "historical"

    def process(
            self,
            requests: List[BaseRequest],
            session: blpapi.Session,
            service: blpapi.Service
    ) -> Dict[str, Dict[str, Dict[date, Any]]]:
        """
        Process Bloomberg HistoricalDataRequest.

        Args:
            requests: List of historical requests
            session: Bloomberg session
            service: Bloomberg refdata service

        Returns:
            Dict[subscription, Dict[field, Dict[date, value]]]
        """
        if not requests:
            return {}

        # Extract subscriptions and correlation IDs
        subscriptions = []
        corr_ids = []

        for req in requests:
            sub = req.subscription or req.instrument.isin or req.instrument.ticker
            subscriptions.append(sub)
            corr_ids.append(req.instrument.id)

        # Collect all unique fields
        fields = list({f.upper() for r in requests for f in r.fields})

        # Get date range from first request (should be same for all)
        first_req = requests[0]
        start = first_req.start
        end = first_req.end
        periodicity = getattr(first_req, "periodicity", "DAILY")

        logger.info("Processing Bloomberg HistoricalDataRequest: %d instruments, fields=%s, start=%s, end=%s",
                   len(subscriptions), fields, start, end)

        # Send all requests
        self._send_historical_requests(
            service, session, subscriptions, fields, corr_ids, start, end, periodicity
        )

        # Collect all responses
        raw_data = self._collect_batch_responses(
            session=session,
            response_type="HistoricalDataResponse",
            expected_corr_ids=set(corr_ids)
        )

        # Convert from {corr_id: {date: {field: value}}} to {subscription: {field: {date: value}}}
        result = {}
        for sub, corr_id in zip(subscriptions, corr_ids):
            if corr_id in raw_data:
                # Transform from {date: {field: value}} to {field: {date: value}}
                date_data = raw_data[corr_id]
                field_data = {}
                for dt, fields_dict in date_data.items():
                    for field, value in fields_dict.items():
                        field_data.setdefault(field.upper(), {})[dt] = value
                result[sub] = field_data

        return result

    def _send_historical_requests(
            self,
            service: blpapi.Service,
            session: blpapi.Session,
            subscriptions: List[str],
            fields: List[str],
            corr_ids: List[str],
            start: date,
            end: date,
            periodicity: str
    ):
        """Send Bloomberg HistoricalDataRequest for each subscription."""
        for sub, cid in zip(subscriptions, corr_ids):
            request = service.createRequest("HistoricalDataRequest")

            start_date = (start or (today() - timedelta(days=365))).strftime("%Y%m%d")
            request.set("startDate", start_date)

            # Handle callable subscriptions
            if callable(sub):
                sub = sub(current_date=start)

            # Handle ISIN format
            if sub.upper().endswith(" ISIN"):
                isin = sub.split()[0]
                bb_code = f"/isin/{isin}"
            else:
                bb_code = sub

            request.append("securities", bb_code)
            for f in fields:
                request.append("fields", f)

            if end:
                request.set("endDate", end.strftime("%Y%m%d"))

            request.set("periodicitySelection", periodicity)

            corr_id_obj = blpapi.CorrelationId(cid)
            session.sendRequest(request, correlationId=corr_id_obj)
            logger.debug("Sent HistoricalDataRequest: %s (corr_id=%s)", bb_code, cid)

    def _collect_batch_responses(
            self,
            session: blpapi.Session,
            response_type: str,
            expected_corr_ids: Set[str]
    ) -> Dict[str, Dict[date, Dict[str, Any]]]:
        """
        Collect all Bloomberg HistoricalDataResponse messages.

        Args:
            session: Bloomberg session
            response_type: Type of Bloomberg response
            expected_corr_ids: Set of correlation IDs we're expecting

        Returns:
            Dictionary: {corr_id: {date: {field: value}}}
        """
        all_data: Dict[str, Dict[date, Dict[str, Any]]] = {}
        errors: Dict[str, str] = {}
        pending_corr_ids = expected_corr_ids.copy()

        timeout_count = 0
        max_timeouts = 20

        while pending_corr_ids and timeout_count < max_timeouts:
            try:
                ev = session.nextEvent(timeout=2000)
                ev_type = ev.eventType()

                # Handle timeout
                if ev_type == blpapi.Event.TIMEOUT:
                    timeout_count += 1
                    logger.debug("Timeout %d/%d waiting for %d responses",
                               timeout_count, max_timeouts, len(pending_corr_ids))
                    continue

                timeout_count = 0  # Reset on valid event

                # Process messages
                if ev_type in (blpapi.Event.PARTIAL_RESPONSE, blpapi.Event.RESPONSE):
                    for msg in ev:
                        if msg.messageType() != blpapi.Name(response_type):
                            continue

                        # Get correlation ID
                        msg_corr_ids = msg.correlationIds()
                        if not msg_corr_ids:
                            logger.warning("Message without correlation ID")
                            continue

                        corr_id = str(msg_corr_ids[0].value())

                        if corr_id not in pending_corr_ids:
                            logger.debug("Received response for unexpected corr_id: %s", corr_id)
                            continue

                        # Remove from pending list
                        pending_corr_ids.discard(corr_id)
                        logger.debug("Received response for %s (%d pending)",
                                   corr_id, len(pending_corr_ids))

                        # Parse historical data
                        try:
                            sec_data = msg.getElement("securityData")
                            sec = sec_data.getElementAsString("security")

                            # Handle security errors
                            if sec_data.hasElement("securityError"):
                                err = sec_data.getElement("securityError").getElementAsString("message")
                                errors[corr_id] = err
                                logger.warning("Bloomberg security error for %s: %s", corr_id, err)
                                continue

                            # Handle field exceptions
                            if sec_data.hasElement("fieldExceptions"):
                                self._process_field_exceptions(sec_data, corr_id, errors)

                            # Parse field data
                            if sec_data.hasElement("fieldData"):
                                field_data_array = sec_data.getElement("fieldData")
                                history: Dict[date, Dict[str, Any]] = {}

                                for j in range(field_data_array.numValues()):
                                    bar = field_data_array.getValueAsElement(j)
                                    dt_val = bar.getElementAsDatetime("date")
                                    if hasattr(dt_val, "date"):
                                        dt_val = dt_val.date()

                                    record = {
                                        str(f.name()): f.getValue()
                                        for f in bar.elements()
                                        if str(f.name()) != "date"
                                    }
                                    history[dt_val] = record

                                all_data[corr_id] = history

                        except Exception as e:
                            logger.error("Error processing message for %s: %s", corr_id, e, exc_info=True)

                # Exit when received RESPONSE finale
                if ev_type == blpapi.Event.RESPONSE:
                    if not pending_corr_ids:
                        logger.debug("All responses collected, exiting")
                        break
                    else:
                        logger.debug("Received RESPONSE event but still waiting for %d responses",
                                   len(pending_corr_ids))

            except Exception as e:
                logger.error("Error in response collection loop: %s", e, exc_info=True)
                break

        if pending_corr_ids:
            logger.warning("Exited with %d unfulfilled requests: %s",
                         len(pending_corr_ids), sorted(list(pending_corr_ids))[:10])

        logger.debug("%s completed: %d instruments received, %d errors, %d missing",
                    response_type, len(all_data), len(errors), len(pending_corr_ids))

        return all_data

    def _process_field_exceptions(self, sec_data, corr_id: str, errors: Dict[str, str]):
        """Process field exceptions from Bloomberg response."""
        field_excs = sec_data.getElement("fieldExceptions")
        for j in range(field_excs.numValues()):
            exc = field_excs.getValueAsElement(j)
            field_id = exc.getElementAsString("fieldId")
            err_msg = exc.getElement("errorInfo").getElementAsString("message")
            errors[f"{corr_id}:{field_id}"] = err_msg
            logger.info("Bloomberg field error for %s.%s: %s", corr_id, field_id, err_msg)
