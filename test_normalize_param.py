"""
Test normalize_param function with different input modes.
"""
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from core.utils.common import normalize_param


# Mock instrument class for testing
class MockInstrument:
    def __init__(self, id: str):
        self.id = id

    def __repr__(self):
        return f"Instrument({self.id})"


def test_normalize_param():
    """Test normalize_param with different input modes"""

    # Create test instruments
    instruments = [
        MockInstrument("AAPL"),
        MockInstrument("MSFT"),
        MockInstrument("GOOGL"),
    ]

    print("=" * 80)
    print("TEST: normalize_param function")
    print("=" * 80)
    print(f"\nInstruments: {[inst.id for inst in instruments]}")

    # Test 1: Single value mode
    print("\n" + "=" * 80)
    print("Test 1: Single value mode")
    print("=" * 80)
    result = normalize_param("USD", instruments)
    print(f"Input: 'USD'")
    print(f"Output: {result}")
    assert result == ["USD", "USD", "USD"], f"Expected ['USD', 'USD', 'USD'], got {result}"
    print("[PASSED]")

    # Test 2: None with default
    print("\n" + "=" * 80)
    print("Test 2: None with default")
    print("=" * 80)
    result = normalize_param(None, instruments, default="EUR")
    print(f"Input: None, default='EUR'")
    print(f"Output: {result}")
    assert result == ["EUR", "EUR", "EUR"], f"Expected ['EUR', 'EUR', 'EUR'], got {result}"
    print("[PASSED]")

    # Test 3: List mode - aligned
    print("\n" + "=" * 80)
    print("Test 3: List mode - aligned")
    print("=" * 80)
    result = normalize_param(["USD", "EUR", "GBP"], instruments)
    print(f"Input: ['USD', 'EUR', 'GBP']")
    print(f"Output: {result}")
    assert result == ["USD", "EUR", "GBP"], f"Expected ['USD', 'EUR', 'GBP'], got {result}"
    print("[PASSED]")

    # Test 4: List mode - single element replication
    print("\n" + "=" * 80)
    print("Test 4: List mode - single element replication")
    print("=" * 80)
    result = normalize_param(["USD"], instruments)
    print(f"Input: ['USD']")
    print(f"Output: {result}")
    assert result == ["USD", "USD", "USD"], f"Expected ['USD', 'USD', 'USD'], got {result}"
    print("[PASSED]")

    # Test 5: Dict mode - partial mapping
    print("\n" + "=" * 80)
    print("Test 5: Dict mode - partial mapping with default")
    print("=" * 80)
    result = normalize_param({"AAPL": "USD", "GOOGL": "EUR"}, instruments, default="GBP")
    print(f"Input: {{'AAPL': 'USD', 'GOOGL': 'EUR'}}, default='GBP'")
    print(f"Output: {result}")
    assert result == ["USD", "GBP", "EUR"], f"Expected ['USD', 'GBP', 'EUR'], got {result}"
    print("[PASSED]")

    # Test 6: Dict mode - complete mapping
    print("\n" + "=" * 80)
    print("Test 6: Dict mode - complete mapping")
    print("=" * 80)
    result = normalize_param({"AAPL": "USD", "MSFT": "EUR", "GOOGL": "GBP"}, instruments)
    print(f"Input: {{'AAPL': 'USD', 'MSFT': 'EUR', 'GOOGL': 'GBP'}}")
    print(f"Output: {result}")
    assert result == ["USD", "EUR", "GBP"], f"Expected ['USD', 'EUR', 'GBP'], got {result}"
    print("[PASSED]")

    # Test 7: Dict mode - no matches (all defaults)
    print("\n" + "=" * 80)
    print("Test 7: Dict mode - no matches (all defaults)")
    print("=" * 80)
    result = normalize_param({"XYZ": "JPY", "ABC": "CHF"}, instruments, default="USD")
    print(f"Input: {{'XYZ': 'JPY', 'ABC': 'CHF'}}, default='USD'")
    print(f"Output: {result}")
    assert result == ["USD", "USD", "USD"], f"Expected ['USD', 'USD', 'USD'], got {result}"
    print("[PASSED]")

    # Test 8: Error - mismatched list length
    print("\n" + "=" * 80)
    print("Test 8: Error - mismatched list length")
    print("=" * 80)
    try:
        result = normalize_param(["USD", "EUR"], instruments)
        print("[FAILED] - Should have raised ValueError")
        assert False, "Should have raised ValueError"
    except ValueError as e:
        print(f"Input: ['USD', 'EUR']")
        print(f"Raised ValueError: {e}")
        print("[PASSED]")

    print("\n" + "=" * 80)
    print("ALL TESTS PASSED!")
    print("=" * 80)
    print("\nSummary:")
    print("  [OK] Single value mode works")
    print("  [OK] None with default works")
    print("  [OK] List mode (aligned) works")
    print("  [OK] List mode (single element replication) works")
    print("  [OK] Dict mode (partial mapping) works")
    print("  [OK] Dict mode (complete mapping) works")
    print("  [OK] Dict mode (no matches) works")
    print("  [OK] Error handling works")


if __name__ == "__main__":
    test_normalize_param()
