"""
Simple test to verify Bloomberg handler structure.

This test checks that:
1. All handlers can be imported correctly
2. Handler chains can be built
3. Basic structure is working

Note: This does NOT test actual Bloomberg API calls (requires active connection)
"""

import sys
import os
import logging

# Add src directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_imports():
    """Test that all handler modules can be imported."""
    logger.info("Testing handler imports...")

    try:
        from providers.bloomberg.handlers.base_handlers import (
            Handler,
            ReferenceFieldHandler,
            HistoricalFieldHandler,
            BulkFieldHandler,
            GeneralHandler,
            DailyPriceHandler,
            IntradayPriceHandler
        )
        logger.info("✓ Base handlers imported successfully")
    except Exception as e:
        logger.error("✗ Failed to import base handlers: %s", e)
        return False

    try:
        from providers.bloomberg.handlers.reference_field_handler import BloombergReferenceHandler
        logger.info("✓ Reference handler imported successfully")
    except Exception as e:
        logger.error("✗ Failed to import reference handler: %s", e)
        return False

    try:
        from providers.bloomberg.handlers.historical_field_handler import BloombergHistoricalHandler
        logger.info("✓ Historical handler imported successfully")
    except Exception as e:
        logger.error("✗ Failed to import historical handler: %s", e)
        return False

    try:
        from providers.bloomberg.handlers.bulk_field_handler import BloombergBulkHandler
        logger.info("✓ Bulk handler imported successfully")
    except Exception as e:
        logger.error("✗ Failed to import bulk handler: %s", e)
        return False

    try:
        from providers.bloomberg.handlers.general_field_handler import BloombergGeneralPlaceholderHandler
        logger.info("✓ General handler imported successfully")
    except Exception as e:
        logger.error("✗ Failed to import general handler: %s", e)
        return False

    try:
        from providers.bloomberg.handlers.daily_price_handler import BloombergDailyPriceHandler
        logger.info("✓ Daily price handler imported successfully")
    except Exception as e:
        logger.error("✗ Failed to import daily price handler: %s", e)
        return False

    try:
        from providers.bloomberg.handlers.intraday_price_handler import BloombergIntradayPriceHandler
        logger.info("✓ Intraday price handler imported successfully")
    except Exception as e:
        logger.error("✗ Failed to import intraday price handler: %s", e)
        return False

    try:
        from providers.bloomberg.handlers.snapshot_price_handler import BloombergSnapshotPriceHandler
        logger.info("✓ Snapshot price handler imported successfully")
    except Exception as e:
        logger.error("✗ Failed to import snapshot price handler: %s", e)
        return False

    return True

def test_fetcher_imports():
    """Test that fetchers can be imported."""
    logger.info("\nTesting fetcher imports...")

    try:
        from providers.bloomberg.fetchers.bloomberg_info_fetcher import BloombergInfoFetcher
        logger.info("✓ Info fetcher imported successfully")
    except Exception as e:
        logger.error("✗ Failed to import info fetcher: %s", e)
        return False

    try:
        from providers.bloomberg.fetchers.bloomberg_market_fetcher import BloombergMarketFetcher
        logger.info("✓ Market fetcher imported successfully")
    except Exception as e:
        logger.error("✗ Failed to import market fetcher: %s", e)
        return False

    return True

def test_provider_import():
    """Test that provider can be imported."""
    logger.info("\nTesting provider import...")

    try:
        from providers.bloomberg.bloomberg import BloombergProvider
        logger.info("✓ Bloomberg provider imported successfully")
    except Exception as e:
        logger.error("✗ Failed to import Bloomberg provider: %s", e)
        return False

    return True

def test_handler_chain_structure():
    """Test that handler chains can be built."""
    logger.info("\nTesting handler chain structure...")

    try:
        from providers.bloomberg.handlers.reference_field_handler import BloombergReferenceHandler
        from providers.bloomberg.handlers.historical_field_handler import BloombergHistoricalHandler

        # Create handlers
        ref_handler = BloombergReferenceHandler()
        hist_handler = BloombergHistoricalHandler()

        # Build chain
        ref_handler.set_next(hist_handler)

        logger.info("✓ Handler chain built successfully")
        return True
    except Exception as e:
        logger.error("✗ Failed to build handler chain: %s", e)
        return False

def main():
    """Run all tests."""
    logger.info("=" * 60)
    logger.info("Bloomberg Handler Structure Test")
    logger.info("=" * 60)

    all_passed = True

    # Test imports
    if not test_imports():
        all_passed = False

    if not test_fetcher_imports():
        all_passed = False

    if not test_provider_import():
        all_passed = False

    if not test_handler_chain_structure():
        all_passed = False

    # Summary
    logger.info("\n" + "=" * 60)
    if all_passed:
        logger.info("✓ All tests PASSED")
        logger.info("=" * 60)
        return 0
    else:
        logger.error("✗ Some tests FAILED")
        logger.info("=" * 60)
        return 1

if __name__ == "__main__":
    sys.exit(main())
