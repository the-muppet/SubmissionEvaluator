import json
import pandas as pd
import numpy as np
from utils.logger import setup_logger
from data_loader import DataLoader
from utils.config_manager import ConfigManager
from utils.masks import is_valid_tcgplayer_id, is_positive_integer
from dataclasses import dataclass, field
from typing import Any, Callable, Dict

logger = setup_logger()


@dataclass
class Evaluator:
    config_manager: ConfigManager = field(init=True)
    data_loader: DataLoader = field(init=False)
    eval_frame: pd.DataFrame = field(default_factory=pd.DataFrame)
    source_timestamps: Dict = field(default_factory=dict)
    clean_rules: Dict[str, Callable[[Any], bool]] = field(
        default_factory=lambda: {
            "TCGplayer Id": is_valid_tcgplayer_id,
            "Add to Quantity": is_positive_integer,
        }
    )
    _acv_threshold: float = 0.0
    _match_rate: float = 0.0
    _total_value: float = 0.0
    _total_quantity: int = 0
    _total_adjusted_qty: int = 0
    _acv: float = 0.0
    _status: str = None

    def __post_init__(self):
        logger.info("Initializing Evaluator")
        try:
            self.data_loader = DataLoader(self.config_manager)
            self._acv_threshold = self.data_loader.get("Threshold", "HIGHER")
            self.load_and_prep_frames()
        except Exception as e:
            logger.error(f"Could not initialize evaluator: {e}")

    def load_and_prep_frames(self):
        try:
            logger.info("Loading and preparing frames")
            data_frames = self.data_loader.load_required_data()
            self.eval_frame = self._merge_source_frames(data_frames)
        except Exception as e:
            logger.error(f"Could not load and prepare frames: {e}")

    def _calculate_metrics(self):
        """
        Calculates various metrics based on the current state of the evaluator.

        This method calculates the total value, total quantity, match rate, ACV,
        and updates the status of the evaluator.

        """
        logger.info("Calculating metrics")
        try:
            self._total_value = self._calculate_total_value()
            self._total_quantity = self._calculate_total_quantity()
            self._match_rate = self._calculate_match_rate()
            self._acv = self._recalculate_acv()
            self._update_status()
        except Exception as e:
            logger.error(f"Could not calculate metrics: {e}")

    def _calculate_total_value(self) -> float:
        """
        Calculates overall total submission value.

        Returns:
            float: Submission value
        """
        try:
            if (
                "add_to_quantity" in self.eval_frame.columns
                and "tcg_market_price" in self.eval_frame.columns
            ):
                total_value = (
                    self.eval_frame["add_to_quantity"]
                    * self.eval_frame["tcg_market_price"]
                ).sum()
                logger.info("Total value calculated")
                return float(total_value)
            else:
                logger.warning("Columns needed to calulate total value missing.")
                return 0.0
        except Exception as e:
            logger.error(f"Error calculating total value: {e}")
            return 0.0

    def _calculate_total_quantity(self) -> int:
        """
        Calculates total submission quantity.

        Returns:
            int: Submission quantity
        """
        try:
            if "add_to_quantity" in self.eval_frame.columns:
                total_quantity = self.eval_frame["add_to_quantity"].sum()
                logger.info("Total quantity calculated")
                return int(total_quantity)
        except Exception as e:
            logger.error(f"Error calculating total quantity: {e}")
            return 0

    def _calculate_total_adjusted_quantity(self) -> int:
        """
        Calculates the total adjusted quantity considering the max_qty constraint per SKU.\n
        Assumes eval_frame is preprocessed to handle missing max_qty values.

        Returns:
            int: Total adjusted quantity.
        """
        try:
            if (
                "add_to_quantity" in self.eval_frame.columns
                and "max_qty" in self.eval_frame.columns
            ):
                adjusted_quantities = np.minimum(
                    self.eval_frame["add_to_quantity"], self.eval_frame["max_qty"]
                ).sum()
                logger.info("Total adjusted quantity calculated")
                return int(adjusted_quantities)
        except Exception as e:
            logger.error(f"Error calculating total adjusted quantity: {e}")
            return 0

    def _calculate_match_rate(self) -> float:
        """
        Calculates the match rate of the submission.

        Returns:
            float: Match rate, or 0.0 if it cannot be calculated.
        """
        total_adjusted_quantity = self._calculate_total_adjusted_quantity()
        total_submission_quantity = self._calculate_total_quantity()

        # Avoid division by zero
        if total_submission_quantity == 0:
            logger.warning(
                "Could not calculate match rate because total submission quantity is 0."
            )
            return 0.0

        # Calculate match rate
        match_rate = (total_adjusted_quantity / total_submission_quantity) * 100
        logger.info("Match rate calculated")
        return float(match_rate)

    def _calculate_acv(self) -> float:
        """
        Calculates the average card value (ACV) of the submission, and updates status

        Returns:
            float: Average card value (ACV)
        """
        try:
            if self.total_quantity == 0:
                logger.warning("Could not calculate ACV because total quantity is 0.")
                return 0.0
            else:
                acv = self.total_value / self.total_quantity
                logger.info("ACV successfully calculated.")
                return float(acv)
        except Exception as e:
            logger.error(f"Error calculating ACV: {e}")
            return 0.0

    def update_status(self):
        """
        Updates the status of the submission to 'Accepted' if\n
        - ACV is above the calculated acv_threshold,\n
        - the total submission quantity is above quantity threshold\n
        otherwise 'Rejected'.
        """
        quantity_threshold = self.data_loader.get("Threshold", "QTY")
        self._status = (
            "Accepted"
            if self._total_quantity >= quantity_threshold
            and self._acv >= self._acv_threshold
            else "Rejected"
        )

    # Getter/setters for automatic variable recalculation
    @property
    def match_rate(self):
        return self._match_rate

    @match_rate.setter
    def match_rate(self, value):
        self._match_rate = value
        self._acv_threshold = (
            self.data_loader.get("Threshold", "LOWER")
            if self._match_rate >= 51
            else self.data_loader.get("Threshold", "UPPER")
        )
        self._update_status()

    @property
    def total_value(self):
        return self._total_value

    @total_value.setter
    def total_value(self, new_value):
        self._total_value = new_value
        self._calculate_acv()

    @property
    def total_quantity(self):
        return self._total_quantity

    @total_quantity.setter
    def total_quantity(self, new_value):
        self._total_quantity = new_value
        self._calculate_acv()

    @property
    def status(self):
        return self._status

    def to_dict(self):
        return {
            "acv": self._acv,
            "match_rate": self.match_rate,
            "status": self.status,
            "threshold": self._acv_threshold,
            "total_value": self.total_value,
            "total_quantity": self.total_quantity,
            "total_adjusted_quantity": self._total_adjusted_qty,
        }

    def to_json(self):
        return json.dumps(self.to_dict())

    def _merge_source_frames(
        self, data_frames: Dict[str, pd.DataFrame]
    ) -> pd.DataFrame:
        """
        Specialized merging of dataframes to create needed eval_frame.

        Args:
            data_frames(Dict[str, pd.DataFrame]): dict containing needed source frames
        Returns:
            pd.DataFrame: merged dataframe
        """
        catalog_df, pullsheet_df, pullorder_df = data_frames.values()
        # Drop add_to_quantity from catalog_df to avoid _x _y post-merge.
        catalog_df = catalog_df.drop(columns=["add_to_quantity"], errors="ignore")
        # Join max_qty into catalog_df on tcgplayer_id
        catalog_df = catalog_df.merge(
            pullsheet_df[["tcgplayer_id", "max_qty"]], on="tcgplayer_id", how="left"
        )
        # Join sheet_order into catalog_df on set_name
        catalog_df = catalog_df.merge(
            pullorder_df[["set_name", "sheet_order"]], on="set_name", how="left"
        )
        return catalog_df

    @staticmethod
    def _format_headers(
        df: pd.DataFrame, format_rules: Dict[str, Callable[[str], str]]
    ) -> pd.DataFrame:
        """
        Formats the headers of a DataFrame based on specified rules.

        Args:
            dataframe: The DataFrame whose headers need formatting.
            format_rules: A dictionary with original header names as keys and formatting functions as values.

        Returns:
            A DataFrame with formatted headers.
        """
        # Format each header according to the provided rules
        df = df.rename(columns=lambda col: format_rules.get(col, lambda x: x)(col))
        return df

    @staticmethod
    def _apply_header_formats(df: pd.DataFrame) -> pd.DataFrame:
        """
        Applies generic formatting rules to all dataframe headers:\n
        - strip
        - lower
        - replace ' ' with '_'

        Args:
            df: The DataFrame to format

        Returns:
            formatted DataFrame
        """
        df.columns = [col.strip().lower().replace(" ", "_") for col in df.columns]
        return df

    @staticmethod
    def _clean_data(
        df: pd.DataFrame,
        clean_rules: Dict[str, Callable[[Any], bool]],
        invalid_file_path: str = None,
    ) -> pd.DataFrame:
        """
        Cleans a DataFrame based on specified rules for each column, saves invalid rows to CSV file

        Args:
            df: DataFrame to be cleaned.
            clean_rules: Dict with column names as keys and cleaning functions for values.
            returns True for valid data, False for invalid.
            invalid_file_path: where to save invalid rows.

        Returns:
            cleaned DataFrame
        """
        invalid_rows = []
        for column, clean_func in clean_rules.items():
            if column in df.columns:
                # Identify valid rows
                valid_mask = df[column].apply(clean_func)
                # Accumulate invalid rows with invalidity reason
                invalid_rows.extend(
                    df.loc[~valid_mask]
                    .assign(invalid_reason=f"invalid {column}")
                    .to_dict("records")
                )
                # Keep only valid rows in the dataframe
                df = df[valid_mask]

        # Save invalid rows to CSV file
        if invalid_rows and invalid_file_path:
            pd.DataFrame(invalid_rows).to_csv(invalid_file_path, index=False)
            logger.info(f"Invalid rows saved to {invalid_file_path}")

        return df
