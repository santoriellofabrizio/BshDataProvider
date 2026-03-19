from sfm_data_provider.interface.bshdata import BshData

api = BshData().market

api.get_daily_currency(start="2025-12-12", id="EURPKR")