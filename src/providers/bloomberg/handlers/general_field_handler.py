import logging
from typing import List, Dict, Any

import blpapi

from core.requests.requests import BaseRequest
from providers.bloomberg.handlers.base_handlers import GeneralHandler

logger = logging.getLogger(__name__)


# Placeholder for future general handlers
# Bloomberg doesn't have "general" requests like Oracle's lookup tables
# This is kept for consistency with the Oracle structure

class BloombergGeneralPlaceholderHandler(GeneralHandler):
    """
    Placeholder handler for general (non-instrument-specific) Bloomberg requests.

    Currently not used, but kept for consistency with Oracle structure.
    Future implementations might include:
    - Currency lookup tables
    - Exchange code mappings
    - Other global reference data
    """

    def can_handle(self, req: BaseRequest) -> bool:
        """
        Currently returns False as no general requests are supported.
        """
        return False

    def process(
            self,
            requests: List[BaseRequest],
            session: blpapi.Session,
            service: blpapi.Service
    ) -> Dict[str, Any]:
        """
        Placeholder process method.

        Args:
            requests: List of general requests
            session: Bloomberg session
            service: Bloomberg refdata service

        Returns:
            Empty dict
        """
        logger.warning("BloombergGeneralPlaceholderHandler.process() called but no implementation exists")
        return {}
