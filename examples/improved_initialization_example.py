"""
Example demonstrating improved initialization with ConfigManager.

This shows how the new config system would work compared to the current approach.
"""

# ============================================================================
# CURRENT APPROACH (Issues)
# ============================================================================

def current_approach():
    """Current initialization - multiple config reads, no validation."""
    from bshDataProvider import BshData
    
    # Config file is read 4+ times:
    # 1. BshData.__init__() → reads config
    # 2. BSHDataClient.__init__() → reads config again
    # 3. OracleProvider.__init__() → reads config again
    # 4. TimescaleProvider.__init__() → reads config again
    
    bsh = BshData(config_path="config/bshdata_config.yaml")
    
    # Issues:
    # - No validation (crashes at runtime if config is wrong)
    # - Multiple file reads
    # - No environment variable support
    # - Silent failures


# ============================================================================
# IMPROVED APPROACH (With ConfigManager)
# ============================================================================

def improved_approach():
    """Improved initialization - single read, validation, env support."""
    from core.utils.config_manager import ConfigManager
    
    # Step 1: Load config once (cached)
    config = ConfigManager.load("config/bshdata_config.yaml")
    
    # Step 2: Get typed, validated config sections
    api_config = config.get_api_config()
    client_config = config.get_client_config()
    oracle_config = config.get_oracle_config()
    timescale_config = config.get_timescale_config()
    
    # Benefits:
    # ✅ Config loaded once, cached
    # ✅ Typed config objects (autocomplete, type checking)
    # ✅ Validation (errors caught early)
    # ✅ Environment variable support
    
    print(f"Log level: {api_config.log_level}")
    print(f"Cache enabled: {api_config.cache}")
    print(f"Oracle user: {oracle_config.user}")
    
    # Validation happens automatically
    try:
        oracle_config.validate()  # Raises ValueError if required fields missing
    except ValueError as e:
        print(f"Config error: {e}")


# ============================================================================
# ENVIRONMENT VARIABLE SUPPORT
# ============================================================================

def env_var_example():
    """Example using environment variables."""
    import os
    
    # Set environment variables (12-factor app style)
    os.environ["BSH_API_LOG_LEVEL"] = "DEBUG"
    os.environ["BSH_API_CACHE"] = "false"
    os.environ["BSH_ORACLE_CONNECTION_USER"] = "myuser"
    
    config = ConfigManager.load("config/bshdata_config.yaml")
    
    # Environment variables override YAML config
    api_config = config.get_api_config()
    print(f"Log level from env: {api_config.log_level}")  # "DEBUG"
    print(f"Cache from env: {api_config.cache}")  # False
    
    oracle_config = config.get_oracle_config()
    print(f"Oracle user from env: {oracle_config.user}")  # "myuser"


# ============================================================================
# CONFIGURATION OVERRIDES
# ============================================================================

def override_example():
    """Example of overriding config values."""
    config = ConfigManager.load("config/bshdata_config.yaml")
    
    # Override specific values
    api_config = config.get_api_config(
        log_level="DEBUG",
        cache=False
    )
    
    # Or override Oracle config
    oracle_config = config.get_oracle_config(
        user="override_user",
        tns_name="override_tns"
    )
    
    print(f"Overridden log level: {api_config.log_level}")
    print(f"Overridden cache: {api_config.cache}")


# ============================================================================
# VALIDATION EXAMPLE
# ============================================================================

def validation_example():
    """Example of config validation."""
    config = ConfigManager.load("config/bshdata_config.yaml")
    
    # Get Oracle config
    oracle_config = config.get_oracle_config()
    
    # Validation happens automatically when required fields are present
    try:
        oracle_config.validate()
        print("✅ Oracle config is valid")
    except ValueError as e:
        print(f"❌ Config validation failed: {e}")
        # Handle error appropriately


# ============================================================================
# HOT RELOAD (Development)
# ============================================================================

def hot_reload_example():
    """Example of config hot reload for development."""
    config = ConfigManager.load("config/bshdata_config.yaml")
    
    # In development loop
    while True:
        # Check if config changed
        if config.reload_if_changed():
            print("Config reloaded!")
            # Re-initialize components with new config
            api_config = config.get_api_config()
            # ... update components
        
        # Do work...
        break  # Example only


# ============================================================================
# INTEGRATION WITH BshData
# ============================================================================

def improved_bshdata_integration():
    """
    How BshData.__init__ would use ConfigManager.
    
    This is a conceptual example of how the improved initialization
    would work in the actual BshData class.
    """
    from core.utils.config_manager import ConfigManager
    
    # In BshData.__init__():
    config = ConfigManager.load(config_path)
    
    # Get API config with overrides
    api_config = config.get_api_config(
        log_level=log_level,
        log_file=log_file,
        cache=cache,
        # ... other overrides
    )
    
    # Setup logging
    self._setup_logging(
        api_config.log_level,
        log_file=api_config.log_file,
        log_level_file=api_config.log_level_file
    )
    
    # Setup cache
    self._setup_cache(api_config.cache, api_config.cache_path)
    
    # Setup client (passes config manager, not path)
    self._setup_client(config, api_config.autocomplete, **kwargs)


# ============================================================================
# MAIN
# ============================================================================

if __name__ == "__main__":
    print("=" * 60)
    print("Improved Initialization Examples")
    print("=" * 60)
    
    print("\n1. Improved Approach:")
    improved_approach()
    
    print("\n2. Environment Variables:")
    env_var_example()
    
    print("\n3. Configuration Overrides:")
    override_example()
    
    print("\n4. Validation:")
    validation_example()
    
    print("\n5. Hot Reload:")
    hot_reload_example()

