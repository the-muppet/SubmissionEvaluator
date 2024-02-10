import os
import configparser


class ConfigManager:
    def __init__(self, config_file_path="config/settings.ini"):
        self.config = configparser.ConfigParser()
        self.config_file_path = config_file_path
        self.load_config()

    def load_config(self):
        """
        Loads the configuration file specified by self.config_file.
        """
        if not os.path.exists(self.config_file_path):
            raise FileNotFoundError(
                f"Configuration file not found at: {self.config_file_path}"
            )
        self.config.read(self.config_file_path)

    def get(self, section, option, fallback=None):
        """
        Retrieves a configuration value.

        Args:
            section (str): The section in the configuration file.
            option (str): The option within the section to retrieve.
            fallback (any): The default value to return if the option is not found.

        Returns:
            The value of the configuration option if found, else the fallback value.
        """
        return self.config.get(section, option, fallback=fallback)
