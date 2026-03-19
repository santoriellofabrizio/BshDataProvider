
from sfm_data_provider.providers.timescale.handlers.base_handlers import Handler

class FXFwdHandler(Handler):
    def can_handle(self, req):
        return req.instrument.type.upper() == "FXFWD"

    def process(self, requests, query):
        raise NotImplemented # TODO implement
