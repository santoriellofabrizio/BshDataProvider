from sfm_data_provider.providers.timescale.handlers.base_handlers import Handler

class BondHandler(Handler):
    def can_handle(self, req):
        return req.instrument.type.upper() == "BOND"

    def process(self, requests, query):
        raise NotImplementedError  # TODO implement
