from typing import Dict
import pandas as pd
from pathlib import Path
from src.utils.logger import setup_logger
from src.utils.config_manager import ConfigManager

logger = setup_logger()
config_manager = ConfigManager()
COLUMN_TYPES = {0: str, 4: str, 5: str, 15: str}


class DataLoader:
    def __init__(self, config_manager):
        self.config = config_manager
        self.sources = self.load_sources_config()

    def load_sources_config(self):
        """
        Loads data sources configuration from settings.ini.
        """
        sources = {
            "pullsheet": self.config.get(
                "DataLoader", "PULLSHEET", fallback="pullsheet.csv"
            ),
            "pullorder": self.config.get(
                "DataLoader", "PULLORDER", fallback="pullorder.csv"
            ),
            "catalog": self.config.get("DataLoader", "CATALOG", fallback="catalog.csv"),
        }
        return sources

    def read_csv_file(self, file_path):
        """
        Reads a CSV file assuming UTF-8 encoding.
        Args:
            file_path (str): Path to the CSV file.
        Returns:
            pd.DataFrame: DataFrame containing the loaded data.
        """
        # Ensure the file path is valid
        if not Path(file_path).is_file():
            logger.error(f"File not found: {file_path}")
            raise FileNotFoundError(f"File not found: {file_path}")

        try:
            return pd.read_csv(file_path, encoding="utf-8", dtype=COLUMN_TYPES)
        except Exception as e:
            logger.error(f"Failed to read {file_path} due to: {e}")
            raise
 
    def load_data(self) -> Dict[str, pd.DataFrame]:
        """
        Loads all data sources specified in the configuration into DataFrames.
        Returns:
            dict: A dictionary of DataFrames keyed by their respective source names.
        """
        data_frames = {}
        for name, path in self.sources.items():
            try:
                logger.info(f"Loading data for source: {name} from {path}")
                data_frames[name] = self.read_csv_file(path)
            except FileNotFoundError as e:
                logger.error(f"Error loading data from {path}: {e}")
