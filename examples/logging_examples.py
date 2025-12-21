"""
Examples demonstrating how BshDataProvider logging behaves in different scenarios.
Run these examples to see the actual behavior.
"""

# ============================================================================
# Example 1: No Existing Logging (BshDataProvider Configures Everything)
# ============================================================================

def example1_no_existing_logging():
    """BshDataProvider will set up all logging."""
    print("\n" + "="*60)
    print("Example 1: No Existing Logging")
    print("="*60)
    
    # No logging configuration in your project
    from bshDataProvider import BshData
    
    # BshDataProvider will configure logging
    bsh = BshData(
        config_path="config/bshdata_config.yaml",
        log_level="INFO",
        log_file="logs/example1.log"
    )
    
    # Now all logging uses BshDataProvider's format
    import logging
    logger = logging.getLogger(__name__)
    logger.info("This message will use BshDataProvider's format")
    
    print("Check logs/example1.log to see the log file")


# ============================================================================
# Example 2: Existing Logging (BshDataProvider Respects It)
# ============================================================================

def example2_existing_logging():
    """Your logging is configured first, BshDataProvider respects it."""
    print("\n" + "="*60)
    print("Example 2: Existing Logging Configuration")
    print("="*60)
    
    import logging
    
    # Configure YOUR logging first
    logging.basicConfig(
        level=logging.DEBUG,
        format='[MYAPP] %(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('logs/myapp.log')
        ]
    )
    
    logger = logging.getLogger(__name__)
    logger.info("This uses MY format")
    
    # Now import BshDataProvider
    from bshDataProvider import BshData
    
    # BshDataProvider sees handlers exist, won't add its own
    bsh = BshData(
        config_path="config/bshdata_config.yaml",
        log_level="DEBUG",  # Match your level
        log_file=None       # Don't add file handler
    )
    
    logger.info("This still uses MY format")
    
    # BshDataProvider logs will use YOUR format
    bsh.logger.info("BshDataProvider message uses YOUR format")


# ============================================================================
# Example 3: Controlling Log Levels
# ============================================================================

def example3_controlling_levels():
    """Demonstrates how to control log levels."""
    print("\n" + "="*60)
    print("Example 3: Controlling Log Levels")
    print("="*60)
    
    from bshDataProvider import BshData
    
    # Different levels for console and file
    bsh = BshData(
        config_path="config/bshdata_config.yaml",
        log_level="WARNING",        # Console: only warnings/errors
        log_file="logs/example3.log",
        log_level_file="DEBUG"      # File: everything
    )
    
    # Change level at runtime
    bsh.set_log_level("DEBUG")
    
    print("Console shows WARNING+, file shows DEBUG+")


# ============================================================================
# Example 4: Isolating BshDataProvider Logs
# ============================================================================

def example4_isolating_logs():
    """Isolate BshDataProvider logs from your application logs."""
    print("\n" + "="*60)
    print("Example 4: Isolating BshDataProvider Logs")
    print("="*60)
    
    import logging
    
    # Your application logging
    app_logger = logging.getLogger("myapp")
    app_handler = logging.StreamHandler()
    app_handler.setFormatter(logging.Formatter('[APP] %(message)s'))
    app_logger.addHandler(app_handler)
    app_logger.setLevel(logging.INFO)
    
    # BshDataProvider logging (will use root logger)
    from bshDataProvider import BshData
    bsh = BshData(
        config_path="config/bshdata_config.yaml",
        log_level="INFO"
    )
    
    # Your logs
    app_logger.info("Application message")
    
    # BshDataProvider logs (different format)
    bsh.logger.info("BshDataProvider message")
    
    print("Notice the different formats")


# ============================================================================
# Example 5: Third-Party Library Suppression
# ============================================================================

def example5_library_suppression():
    """Demonstrates third-party library suppression."""
    print("\n" + "="*60)
    print("Example 5: Third-Party Library Suppression")
    print("="*60)
    
    import logging
    
    # Before BshDataProvider
    urllib_logger = logging.getLogger("urllib3")
    urllib_logger.setLevel(logging.DEBUG)
    urllib_logger.debug("This would show before BshDataProvider")
    
    # Initialize BshDataProvider (silences urllib3)
    from bshDataProvider import BshData
    bsh = BshData(config_path="config/bshdata_config.yaml")
    
    urllib_logger.debug("This won't show (suppressed to WARNING)")
    urllib_logger.warning("This WILL show (WARNING level)")
    
    # Restore if needed
    urllib_logger.setLevel(logging.DEBUG)
    urllib_logger.debug("Now this shows again")


# ============================================================================
# Example 6: Production Setup
# ============================================================================

def example6_production_setup():
    """Recommended setup for production applications."""
    print("\n" + "="*60)
    print("Example 6: Production Setup")
    print("="*60)
    
    import logging
    from logging.handlers import RotatingFileHandler
    
    # Production logging configuration
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
        handlers=[
            logging.StreamHandler(),
            RotatingFileHandler(
                'logs/app.log',
                maxBytes=10*1024*1024,  # 10MB
                backupCount=5
            )
        ]
    )
    
    # Initialize BshDataProvider (won't add handlers, but will set levels)
    from bshDataProvider import BshData
    bsh = BshData(
        config_path="config/bshdata_config.yaml",
        log_level="INFO",      # Match production level
        log_file=None          # Use your file handler
    )
    
    logger = logging.getLogger(__name__)
    logger.info("Production application started")
    bsh.logger.info("BshDataProvider initialized")
    
    print("All logs go to your production log file with your format")


# ============================================================================
# Main
# ============================================================================

if __name__ == "__main__":
    print("\n" + "="*60)
    print("BshDataProvider Logging Examples")
    print("="*60)
    print("\nChoose an example to run:")
    print("1. No existing logging")
    print("2. Existing logging configuration")
    print("3. Controlling log levels")
    print("4. Isolating BshDataProvider logs")
    print("5. Third-party library suppression")
    print("6. Production setup")
    print("\nUncomment the example you want to run:")
    
    # Uncomment the example you want to test:
    # example1_no_existing_logging()
    # example2_existing_logging()
    # example3_controlling_levels()
    # example4_isolating_logs()
    # example5_library_suppression()
    # example6_production_setup()

