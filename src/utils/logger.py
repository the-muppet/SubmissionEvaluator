import logging
import logging.config
from logging.handlers import RotatingFileHandler
from src.utils.config_manager import ConfigManager


def setup_logger():
    """
    Sets up the application logger based on configurations specified in the settings.ini file.
    """
    config = ConfigManager()
    log_level = config.get("Logging", "LEVEL", fallback="debug")
    log_format = config.get(
        "Logging",
        "FORMAT",
        fallback="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    log_datefmt = config.get("Logging", "DATEFMT", fallback="%Y-%m-%d %H:%M:%S")
    log_file = config.get("Logging", "FILE", fallback="app.log")  # Path to log file
    max_bytes = int(config.get("Logging", "MAX_BYTES", fallback=10485760))  # 10 MB
    backup_count = int(
        config.get("Logging", "BACKUP_COUNT", fallback=5)
    )  # Keep 5 backup logs

    # Set up file handler
    log_handler = RotatingFileHandler(
        log_file, maxBytes=max_bytes, backupCount=backup_count
    )
    log_handler.setLevel(log_level)
    log_handler.setFormatter(logging.Formatter(log_format, datefmt=log_datefmt))

    # Set up console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    console_handler.setFormatter(logging.Formatter(log_format, datefmt=log_datefmt))

    # Get logger
    logger = logging.getLogger(__name__)

    # Clear existing handlers to avoid duplicate logs
    logger.handlers.clear()

    # Add file and console handlers to logger
    logger.addHandler(log_handler)
    logger.addHandler(console_handler)

    # Set logger level
    logger.setLevel(log_level)

    return logger
