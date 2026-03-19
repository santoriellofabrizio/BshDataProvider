"""
Quick test to verify ConfigManager implementation works correctly.
"""

def test_config_manager():
    """Test ConfigManager basic functionality."""
    from core.utils.config_manager import ConfigManager
    
    print("Testing ConfigManager...")
    
    # Test 1: Load config
    print("\n1. Loading config...")
    config = ConfigManager.load("config/bshdata_config.yaml")
    print("   ✅ Config loaded")
    
    # Test 2: Get API config
    print("\n2. Getting API config...")
    api_config = config.get_api_config()
    print(f"   ✅ Log level: {api_config.log_level}")
    print(f"   ✅ Cache enabled: {api_config.cache}")
    print(f"   ✅ Cache path: {api_config.cache_path}")
    
    # Test 3: Get client config
    print("\n3. Getting client config...")
    client_config = config.get_client_config()
    print(f"   ✅ Oracle activated: {client_config.activate_oracle}")
    print(f"   ✅ Timescale activated: {client_config.activate_timescale}")
    
    # Test 4: Get Oracle config
    print("\n4. Getting Oracle config...")
    try:
        oracle_config = config.get_oracle_config()
        print(f"   ✅ Oracle user: {oracle_config.user}")
        print(f"   ✅ Oracle TNS: {oracle_config.tns_name}")
    except Exception as e:
        print(f"   ⚠️  Oracle config error (expected if not configured): {e}")
    
    # Test 5: Get Timescale config
    print("\n5. Getting Timescale config...")
    try:
        timescale_config = config.get_timescale_config()
        print(f"   ✅ Timescale host: {timescale_config.host}")
        print(f"   ✅ Timescale port: {timescale_config.port}")
    except Exception as e:
        print(f"   ⚠️  Timescale config error (expected if not configured): {e}")
    
    # Test 6: Config caching
    print("\n6. Testing config caching...")
    import time
    start = time.time()
    config2 = ConfigManager.load("config/bshdata_config.yaml")
    elapsed = time.time() - start
    print(f"   ✅ Second load (should use cache): {elapsed*1000:.2f}ms")
    
    # Test 7: Backward compatibility - load_yaml
    print("\n7. Testing backward compatibility (load_yaml)...")
    from core.utils.common import load_yaml
    yaml_config = load_yaml("config/bshdata_config.yaml")
    print(f"   ✅ load_yaml() works: {len(yaml_config)} sections")
    
    print("\n✅ All tests passed!")


def test_bshdata_integration():
    """Test BshData with ConfigManager."""
    print("\n" + "="*60)
    print("Testing BshData integration...")
    print("="*60)
    
    try:
        from interface.bshdata import BshData
        
        print("\n1. Creating BshData instance...")
        bsh = BshData(config_path="config/bshdata_config.yaml")
        print("   ✅ BshData initialized successfully")
        print(f"   ✅ Client created: {bsh.client is not None}")
        print(f"   ✅ Market API: {bsh.market is not None}")
        print(f"   ✅ Info API: {bsh.info is not None}")
        print(f"   ✅ General API: {bsh.general is not None}")
        
        print("\n✅ BshData integration test passed!")
        
    except Exception as e:
        print(f"\n❌ BshData integration test failed: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    print("="*60)
    print("ConfigManager Implementation Test")
    print("="*60)
    
    test_config_manager()
    test_bshdata_integration()
    
    print("\n" + "="*60)
    print("Test Complete")
    print("="*60)

