"""
Data loader module.
"""

import pandas as pd
from typing import Dict
from pathlib import Path
from app.utils.logger import setup_logger
from app.utils.config_manager import ConfigManager

logger = setup_logger()
config_manager = ConfigManager()
COLUMN_TYPES = {0: str, 4: str, 5: str, 15: str}


class DataLoader:
    def __init__(self, config_manager):
        self.config = config_manager
        self.sources = self.load_sources_config()

    def load_sources_config(self) -> Dict[str, str]:
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
        if not Path(file_path).is_file():  # Ensure the file path is valid
            logger.error(f"File not found: {file_path}")
            raise FileNotFoundError(f"File not found: {file_path}")

        try:  # Read the CSV file and return the DataFrame
            return pd.read_csv(file_path, encoding="utf-8", dtype=COLUMN_TYPES)
        except Exception as e:
            logger.error(f"Failed to read {file_path} due to: {e}")
            raise

    def load_required_data(self) -> Dict[str, pd.DataFrame]:
        """
        Loads required data from CSV files.

        Args:
            None
        Returns:
            dict[str, df]: containing the loaded dataframes
        """
        return {
            "catalog_df": self.read_csv_file(self.sources["catalog"]),
            "pullsheet_df": self.read_csv_file(self.sources["pullsheet"]),
            "pullorder_df": self.read_csv_file(self.sources["pullorder"]),
        }
