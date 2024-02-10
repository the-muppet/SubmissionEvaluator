import numpy as np
import pandas as pd
from models import Submission
from data_loader import DataLoader
from typing import Any, Callable, Dict
from dataclasses import dataclass, field
from utils.config_manager import ConfigManager


config = ConfigManager()


# EvalFrame class
class EvalFrame:
    def __init__(self, submission: Submission, data_loader: DataLoader):
        # Initialize data frames using the DataLoader
        self.catalog_df = data_loader.load_data()
        self.pullsheet_df = data_loader.load_data()
        self.pullorder_df = data_loader.load_data()
        self.submission_df = submission.dataframe
        self.evaluation_df = self.merge_dataframes()

    @staticmethod
    def merge_frames(
        df1: pd.DataFrame,
        df2: pd.DataFrame,
        on: str,
        columns_to_join: list,
    ) -> pd.DataFrame:
        # Select specified columns to join from df2 along with the merge column
        df2_selected = df2[[on] + columns_to_join]
        return df1.merge(df2_selected, on=on, how="left")

    def merge_dataframes(self):
        # Merge catalog with pullsheet
        merged_df = self.merge_frames(
            self.catalog_df,
            self.pullsheet_df,
            on="tcgplayer_id",
            columns_to_join=["max_qty"],
        )
        # Further merge with pullorder
        merged_df = self.merge_frames(
            merged_df,
            self.pullorder_df,
            on="set_name",
            columns_to_join=["shelf_order"],
        )
        # Assuming you need to remove 'add_to_quantity' from merged_df before the final merge
        merged_df = merged_df.drop(
            columns=["add_to_quantity"]
        )  # Correct syntax to drop a column
        # Finally, merge the submission_df with the previously merged data
        evaluation_df = self.merge_frames(
            self.submission_df,
            merged_df,
            on="tcgplayer_id",
            columns_to_join=["max_qty", "shelf_order"],
        )
        return evaluation_df


# Evaluator class
@dataclass
class Evaluator:
    _dataframe: pd.DataFrame = field(default_factory=pd.DataFrame)
    _min_quantity: int = field(init=False, repr=False)
    _threshold: float = 3.0
    _match_rate: float = field(init=False, default=0.0, repr=False)
    _total_value: float = field(init=False, default=0.0, repr=False)
    _total_quantity: int = field(init=False, default=0, repr=False)
    _total_adjusted_qty: int = field(init=False, default=0, repr=False)
    _acv: float = field(init=False, default=0.0, repr=False)
    _status: bool = field(init=False, default=False, repr=False)

    @property
    def dataframe(self):
        return self._dataframe

    @dataframe.setter
    def dataframe(self, value):
        self._dataframe = value
        self.recalculate_metrics()

    def recalculate_metrics(self):
        self._calculate_total_value()
        self._calculate_total_quantity()
        self._calculate_total_adjusted_quantity()
        self._calculate_match_rate()
        self._recalculate_acv()
        self._update_status()

    def _calculate_total_value(self):
        self._total_value = (
            self.dataframe["add_to_quantity"] * self.dataframe["tcg_market_price"]
        ).sum()

    def _calculate_total_quantity(self):
        self._total_quantity = self.dataframe["add_to_quantity"].sum()

    def _calculate_total_adjusted_quantity(self):
        if (
            "add_to_quantity" in self.dataframe.columns
            and "max_qty" in self.dataframe.columns
        ):
            self.dataframe["max_qty"].fillna(0, inplace=True)
            self.dataframe["adjusted_quantity"] = np.minimum(
                self.dataframe["add_to_quantity"], self.dataframe["max_qty"]
            )
            self._total_adjusted_qty = self.dataframe["adjusted_quantity"].sum()

    def _calculate_match_rate(self):
        if self._total_quantity > 0:
            self._match_rate = (self._total_adjusted_qty / self._total_quantity) * 100

    def _recalculate_acv(self):
        if self._total_quantity > 0:
            self._acv = self._total_value / self._total_quantity
            self._update_status()

    def _update_status(self):
        if self._match_rate >= 51:
            self._threshold = 2
        else:
            self._threshold = 3
        self._status = (
            self._total_quantity >= self._min_quantity and self._acv >= self._threshold
        )

    @property
    def match_rate(self):
        return self._match_rate

    @property
    def total_value(self):
        return self._total_value

    @property
    def total_quantity(self):
        return self._total_quantity

    @property
    def acv(self):
        return self._acv

    @property
    def status(self):
        return self._status

    def __post_init__(self):
        self._min_quantity = int(config.get("Metrics", "MINQUANTITY", fallback="500"))
        self.recalculate_metrics()

    def evaluate(self):
        """
        Evaluates the submission based on calculated metrics and returns a comprehensive summary.
        """
        # Ensure metrics are up-to-date
        self.recalculate_metrics()

        # Construct a summary of the evaluation
        evaluation_results = {
            "status": "Accepted" if self._status else "Rejected",
            "total_value": f"${self._total_value:.2f}",
            "total_quantity": self._total_quantity,
            "adjusted_total_quantity": self._total_adjusted_qty,
            "match_rate": f"{self._match_rate:.2f}%",
            "average_cost_value": f"${self._acv:.2f}",
        }
        return evaluation_results


# TODO I KNOW, I KNOW - On the fence about 'god-obj/class' or splitting them.. gah. TODO
@dataclass
class SubmissionEvaluator:
    """
    A class for evaluating TCG submissions, including data cleaning, calculating metrics,
    and determining submission status based on predefined criteria.
    Attributes:
        dataframe (pd.DataFrame): The submissions data.
        threshold (float): Threshold value for Average Card Value (ACV) to determine acceptance.
        _acv (float): Average card value calculated from total value and quantity.
        _match_rate (float): Calculated match rate of submissions. Not directly settable.
        _total_value (float): Total value of all submissions. Not directly settable.
        _total_quantity (int): Total quantity of submissions. Not directly settable.
        _total_adjusted_qty (int): Total of adjusted quantities based on quantity constraints. Not directly settable.
        _status (bool): Status of submission, accepted or rejected based on criteria.
    """

    clean_rules: Dict[str, Callable[[Any], bool]] = field(
        default_factory=lambda: {
            "tcgplayer_id": SubmissionEvaluator.is_valid_tcgplayer_id,
            "quantity": SubmissionEvaluator.is_positive_integer,
        }
    )
    dataframe: pd.DataFrame = field(default_factory=pd.DataFrame)
    threshold: float = field(default=3.0, init=False, repr=False)
    _match_rate: float = field(default=0.0, init=False, repr=False)
    _total_value: float = field(default=0.0, init=False, repr=False)
    _total_quantity: int = field(default=0, init=False, repr=False)
    _total_adjusted_qty: int = field(default=0, init=False, repr=False)
    _acv: float = field(default=0.0, init=False, repr=False)
    _status: bool = field(default=False, init=False, repr=False)

    def __post_init__(self):
        """
        Post-initialization to clean the data and calculate initial metrics.
        """
        self.init_or_update()

    def init_or_update(self):
        self.dataframe = SubmissionEvaluator._clean_data(
            self.dataframe, self.clean_rules
        )
        self._calculate_metrics()

    def _calculate_metrics(self):
        self._total_value = self._calculate_total_value()
        self._total_quantity = self._calculate_total_quantity()
        self._match_rate = self._calculate_match_rate()
        self._acv = self._recalculate_acv()
        self._update_status()

    @staticmethod
    def is_not_empty(value: str) -> bool:
        """
        Checks if a given string value is not empty.
        Args:
            value (str): The value to check.
        Returns:
            bool: True if the value is not empty, False otherwise.
        """
        return value != ""

    @staticmethod
    def is_numeric(value: Any) -> bool:
        """
        Attempts to convert a string to a float and checks if it is a positive number.
        Args:
            value (Any): The value to convert and check.
        Returns:
            bool: True if conversion is successful and value is positive, False otherwise.
        """
        try:
            val = float(value)
            if val > 0:
                return True
            else:
                return False
        except ValueError:
            return False

    @staticmethod
    def is_positive_integer(value: Any) -> bool:
        """
        Checks if the input value can be converted to a positive integer.
        """
        try:
            val = int(value)
            return val > 0
        except (ValueError, TypeError):
            return False

    @staticmethod
    def is_valid_tcgplayer_id(value: str) -> bool:
        """
        Validates a TCGplayer ID by checking if it is numeric and not empty.
        Args:
            value (str): The TCGplayer ID to validate.
        Returns:
            bool: True if the ID is valid, False otherwise.
        """
        return SubmissionEvaluator.is_numeric(
            value
        ) is not None and SubmissionEvaluator.is_not_empty(value)

    @staticmethod
    def _clean_data(
        dataframe: pd.DataFrame,
        clean_rules: Dict[str, Callable[[Any], bool]],
        invalid_file_path: str = "invalid_rows.csv",
    ) -> pd.DataFrame:
        """Cleans a DataFrame based on specified rules for each column and accumulates invalid rows in DataFrame."""
        invalid_accumulator = pd.DataFrame()

        for column, clean_func in clean_rules.items():
            if column in dataframe.columns:
                # Identify valid and invalid rows
                valid_mask = dataframe[column].apply(clean_func)
                invalid = dataframe[~valid_mask].copy()

                # Add a column to indicate the reason for invalidity
                invalid["invalid_reason"] = f"Invalid {column}"

                # Accumulate invalid rows
                invalid_accumulator = pd.concat(
                    [invalid_accumulator, invalid], ignore_index=True
                )

                # Keep only valid rows
                dataframe = dataframe[valid_mask]

        # Deduplicate the invalid_accumulator based on index
        invalid_accumulator.drop_duplicates(
            subset=dataframe.index.name, keep="first", inplace=True
        )

        # Write non-valid rows to a CSV file if they exist
        if not invalid_accumulator.empty:
            invalid_accumulator.to_csv(invalid_file_path, index=False)
            print(f"Non-valid rows written to {invalid_file_path}")

        return dataframe

    def _calculate_total_value(self) -> float:
        """Calculates overall total submission value.
        Multiplies add_to_quantity and market value using vectorized methods for efficiency
        Returns:
            float: Submission value
        """
        return (
            self.dataframe["add_to_quantity"] * self.dataframe["tcg_market_price"]
        ).sum()

    def _calculate_total_quantity(self) -> int:
        """Sums total submission quantity.
        Returns:
            int: Submission quantity
        """
        return self.dataframe["add_to_quantity"].sum()

    def _calculate_total_adjusted_quantity(self) -> int:
        """Calculates the adjusted quantity (which takes into account the max_qty constraint per SKU in the pullsheet)
        Returns:
            int: Total Adjusted quantity
        """
        if "add_to_quantity" in self.dataframe.columns:
            self.dataframe["max_qty"].fillna(0, inplace=True)
            self.dataframe["adjusted_quantity"] = np.minimum(
                self.dataframe["add_to_quantity"], self.dataframe["max_qty"]
            )
            return self.dataframe["adjusted_quantity"].sum()
        return 0

    def _calculate_match_rate(self) -> float:
        """Calculates the match rate of the submission.
        Returns:
            float: Match rate
        """
        total_adjusted_quantity = self._calculate_total_adjusted_quantity()
        total_submission_quantity = self._calculate_total_quantity()
        return (
            (total_adjusted_quantity / total_submission_quantity) * 100
            if total_submission_quantity > 0
            else 0
        )

    def _recalculate_acv(self):
        """Recalculates the average card value of the submission.
        Returns:
            float: Average card value (ACV)
        """
        self._acv = (
            self._total_value / self._total_quantity if self._total_quantity > 0 else 0
        )
        self._update_status()

    def _update_status(self):
        """
        Updates the status of the submission based on the criteria.
        Returns:
            None: (but also pseudo-boolean via 'Accepted/Rejected' indicator)
        """
        self._status = (
            False if self._total_quantity < 500 else self.acv >= self.threshold
        )

    # Getter/setters for automatic variable recalculation
    @property
    def match_rate(self):
        return self._match_rate

    @match_rate.setter
    def match_rate(self, value):
        self._match_rate = value
        self.threshold = 2.00 if self._match_rate >= 51 else 3.00
        self._update_status()

    @property
    def total_value(self):
        return self._total_value

    @total_value.setter
    def total_value(self, new_value):
        self._total_value = new_value
        self._recalculate_acv()

    @property
    def total_quantity(self):
        return self._total_quantity

    @total_quantity.setter
    def total_quantity(self, new_value):
        self._total_quantity = new_value
        self._recalculate_acv()

    @property
    def status(self):
        return self._status
