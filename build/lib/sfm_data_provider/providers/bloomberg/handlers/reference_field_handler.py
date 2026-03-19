import logging
from typing import List, Dict, Any, Set

import blpapi

from sfm_data_provider.core.requests.requests import BaseRequest
from sfm_data_provider.providers.bloomberg.handlers.base_handlers import ReferenceFieldHandler

logger = logging.getLogger(__name__)


class BloombergReferenceHandler(ReferenceFieldHandler):
    """
    Handler for Bloomberg ReferenceDataRequest.

    Processes static/semi-static fields using Bloomberg's ReferenceDataRequest API.
    """

    def can_handle(self, req: BaseRequest) -> bool:
        """
        This handler can handle any reference request.
        It's typically used as a catch-all for reference data.
        """
        return req.request_type == "reference"

    def process(
            self,
            requests: List[BaseRequest],
            session: blpapi.Session,
            service: blpapi.Service
    ) -> Dict[str, Dict[str, Any]]:
        """
        Process Bloomberg ReferenceDataRequest.

        Args:
            requests: List of reference requests (each with instrument, fields, subscription)
            session: Bloomberg session
            service: Bloomberg refdata service

        Returns:
            Dict[instrument_id, Dict[field, value]]
        """
        if not requests:
            return {}

        logger.info("Processing Bloomberg ReferenceDataRequest: %d instruments",
                   len(requests))

        # Send requests directly from request objects (no spacchettazione)
        expected_corr_ids = self._send_reference_requests_from_objects(
            service, session, requests
        )

        # Collect all responses
        data = self._collect_batch_responses(
            session=session,
            response_type="ReferenceDataResponse",
            expected_corr_ids=expected_corr_ids
        )

        return data

    def _send_reference_requests_from_objects(
            self,
            service: blpapi.Service,
            session: blpapi.Session,
            requests: List[BaseRequest]
    ) -> Set[str]:
        """
        Send Bloomberg ReferenceDataRequest directly from request objects.

        Each request is sent independently with its own fields.
        No unpacking/repacking of subscriptions or fields.

        Args:
            service: Bloomberg refdata service
            session: Bloomberg session
            requests: List of BaseStaticRequest objects

        Returns:
            Set of correlation IDs (instrument IDs) expected in responses
        """
        expected_corr_ids = set()

        for req in requests:
            # Extract subscription (security ID) from request
            sub = req.subscription or req.instrument.isin or req.instrument.ticker
            corr_id = req.instrument.id

            # Create Bloomberg request
            bb_request = service.createRequest("ReferenceDataRequest")

            # Handle ISIN format
            if sub.upper().endswith(" ISIN"):
                isin = sub.split()[0]
                bb_code = f"/isin/{isin}"
            else:
                bb_code = sub

            bb_request.append("securities", bb_code)

            # Add fields from this specific request
            for field in req.fields:
                field_str = str(field).upper()
                bb_request.append("fields", field_str)

            # Send request
            corr_id_obj = blpapi.CorrelationId(corr_id)
            session.sendRequest(bb_request, correlationId=corr_id_obj)
            expected_corr_ids.add(corr_id)

            logger.debug("Sent ReferenceDataRequest: %s, fields=%s (corr_id=%s)",
                        bb_code, req.fields, corr_id)

        return expected_corr_ids

    def _collect_batch_responses(
            self,
            session: blpapi.Session,
            response_type: str,
            expected_corr_ids: Set[str]
    ) -> Dict[str, Dict[str, Any]]:
        """
        Collect all Bloomberg responses for a batch of requests.

        Args:
            session: Bloomberg session
            response_type: Type of Bloomberg response (e.g., "ReferenceDataResponse")
            expected_corr_ids: Set of correlation IDs we're expecting

        Returns:
            Dictionary with all collected data: {corr_id: {field: value}}
        """
        all_data: Dict[str, Dict[str, Any]] = {}
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

                # Process messages in PARTIAL_RESPONSE or RESPONSE events
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

                        # Parse security data
                        try:
                            sec_data_array = msg.getElement("securityData")

                            for i in range(sec_data_array.numValues()):
                                sec_data = sec_data_array.getValueAsElement(i)
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
                                    field_data = sec_data.getElement("fieldData")
                                    record = {
                                        str(f.name()): (f.getValue() if f.isValid() else None)
                                        for f in field_data.elements()
                                    }
                                    all_data[corr_id] = record

                        except Exception as e:
                            logger.error("Error processing message for %s: %s", corr_id, e, exc_info=True)

                # Exit when received RESPONSE finale and no more pending
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