"""
Example: basic BshData API usage.

Run directly:
    python tests/examples/api_usage.py
"""

from sfm_data_provider.interface.bshdata import BshData

api = BshData(r"C:\AFMachineLearning\Libraries\SFMDataProvider\config\bshdata_config.yaml")

api.info.get_nav(start="2026-03-12", ticker='IHYG', source='bloomberg', fallbacks=[{'source': 'bloomberg'}])
