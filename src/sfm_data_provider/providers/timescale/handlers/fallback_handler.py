import logging


from sfm_data_provider.providers.timescale.handlers.base_handlers import Handler
logger = logging.getLogger(__name__)


class FallbackHandler(Handler):

    def can_handle(self, req):
        return True  # ultima spiaggia

    def process(self, requests, query):
        logger.warning("Unhandled requests: %s", [r.instrument.id for r in requests])
        return {r.instrument.id: {f: {} for f in r.fields} for r in requests}
