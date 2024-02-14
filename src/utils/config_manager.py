from pathlib import Path
import configparser


class ConfigManager:
    def __init__(
        self, config_file_path="../config/settings.ini"
    ):  # Adjusted relative path
        # Determine the directory in which this script (config_manager.py) resides
        self.script_dir = Path(__file__).parent

        # Construct an absolute path to the configuration file
        # Note: Using .resolve() to ensure we get an absolute path, resolving any symlinks
        self.config_file_path = (self.script_dir / config_file_path).resolve()

        # Create a configparser object
        self.config = configparser.ConfigParser()
        self.load_config()

    def load_config(self):
        """
        Loads the configuration file specified by self.config_file_path.
        """
        if not self.config_file_path.exists():
            raise FileNotFoundError(
                f"Configuration file not found at: {self.config_file_path}"
            )
        self.config.read(self.config_file_path)

    def get(self, section, option, fallback=None):
        """
        Retrieves a configuration value.
        """
        return self.config.get(section, option, fallback=fallback)
