"""
Example: Using dict mode for API parameters

This example demonstrates the new dict-based parameter mapping feature
that allows you to specify different values for different instruments
without maintaining aligned lists.
"""
import sys
from pathlib import Path

# Add src to path (if running as standalone script)
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from interface.api.info_data_api import InfoDataAPI
from interface.api.type_hints import InfoDataGetParams

# Example usage (pseudocode - requires actual API client setup)

def example_old_way():
    """OLD WAY: Using aligned lists"""
    print("=" * 80)
    print("OLD WAY: Using aligned lists")
    print("=" * 80)

    # If you have 3 instruments and want different currencies
    instruments = ["AAPL", "MSFT", "GOOGL"]  # Simplified for example

    # You need to maintain aligned lists
    currencies = ["USD", "EUR", "GBP"]  # Must match order and count!
    sources = ["bloomberg", "bloomberg", "oracle"]

    # If you only want to specify currency for one instrument,
    # you still need to fill the whole list
    currencies_partial = ["USD", "EUR", "EUR"]  # EUR as default for others

    print(f"Instruments: {instruments}")
    print(f"Currencies (aligned): {currencies}")
    print(f"Sources (aligned): {sources}")
    print("\nProblem: Must maintain alignment and specify all values")


def example_new_way():
    """NEW WAY: Using dict mode"""
    print("\n" + "=" * 80)
    print("NEW WAY: Using dict mode")
    print("=" * 80)

    # If you have 3 instruments and want different currencies
    # You can use a dict to specify only what you need

    # Example 1: Partial mapping with default
    currencies_dict = {
        "AAPL": "USD",
        "GOOGL": "GBP"
        # MSFT will get the default (e.g., "EUR")
    }

    # Example 2: Specific sources for specific instruments
    sources_dict = {
        "GOOGL": "oracle",
        # Others will get default (e.g., "bloomberg")
    }

    print(f"Currencies (dict): {currencies_dict}")
    print(f"Sources (dict): {sources_dict}")
    print("\nBenefit: Only specify what differs from default!")


def example_api_usage():
    """Example API calls with dict mode"""
    print("\n" + "=" * 80)
    print("API USAGE EXAMPLES")
    print("=" * 80)

    # Note: These are pseudocode examples showing the API signature
    # Actual usage requires proper API client setup

    print("\n1. Single value (replicated to all):")
    print("""
    api.get_ter(
        ticker=["IUSA", "VUSA", "CSPX"],
        source="bloomberg"  # Same source for all
    )
    """)

    print("\n2. Aligned list (traditional way):")
    print("""
    api.get_ter(
        ticker=["IUSA", "VUSA", "CSPX"],
        source=["bloomberg", "bloomberg", "oracle"]  # Must align!
    )
    """)

    print("\n3. Dict mode (NEW!):")
    print("""
    api.get_ter(
        ticker=["IUSA", "VUSA", "CSPX"],
        source={
            "IUSA": "bloomberg",
            "CSPX": "oracle"
            # VUSA gets default (bloomberg)
        }
    )
    """)

    print("\n4. Multiple dict parameters:")
    print("""
    api.get(
        type="ETP",
        ticker=["IUSA", "VUSA", "CSPX"],
        currency={
            "IUSA": "USD",
            "CSPX": "GBP"
            # VUSA gets default
        },
        source={
            "CSPX": "oracle"
            # Others get default
        },
        market={
            "IUSA": "XLON",
            "VUSA": "XLON",
            "CSPX": "XLON"
        }
    )
    """)


def example_with_type_hints():
    """Example using TypedDict for IDE autocomplete"""
    print("\n" + "=" * 80)
    print("TYPE HINTS FOR IDE SUPPORT")
    print("=" * 80)

    print("""
    from interface.api.type_hints import InfoDataGetParams

    def my_function(**kwargs):
        # Cast to TypedDict for IDE autocomplete (no runtime overhead)
        params: InfoDataGetParams = kwargs

        # Now you get autocomplete for:
        # - id, isin, ticker
        # - market, currency, source (all support str/list/dict!)
        # - fields, start_date, end_date, etc.

        instrument_id = params.get('id')
        market = params.get('market')  # Can be str, list, or dict!
        currency = params.get('currency')  # Can be str, list, or dict!
    """)


def main():
    """Run all examples"""
    example_old_way()
    example_new_way()
    example_api_usage()
    example_with_type_hints()

    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print("""
Dict mode advantages:
  1. Specify values only for specific instruments
  2. No need to maintain aligned lists
  3. Clear mapping: {instrument_id: value}
  4. Others get default value automatically
  5. Same API, backward compatible!

Supported parameters (dict mode):
  - currency
  - market
  - source
  - subscriptions
  - request_type

All convenience methods support dict mode:
  - get_ter()
  - get_dividends()
  - get_fx_composition()
  - get_pcf_composition()
  - get_nav()
  - get_etp_fields()
    """)


if __name__ == "__main__":
    main()
