from utils.masks import is_valid_tcgplayer_id, is_positive_integer
from dataclasses import dataclass, field
from typing import Any, Callable, Dict
import json
import pandas as pd
import numpy as np


@dataclass
class SubmissionEvaluator:
    source_file_paths: Dict[str, str]
    clean_rules: Dict[str, Callable[[Any], bool]] = field(
        default_factory=lambda: {
            "TCGplayer Id": lambda x: is_valid_tcgplayer_id(x),
            "Add to Quantity": lambda x: is_positive_integer(x),
        }
    )
    format_rules: Dict[str, Callable[[Any], Any]] = field(
        default_factory=lambda: {
            "TCGplayer Id": lambda x: x.strip().lower().replace(" ", "_"),
            "Set Name": lambda x: x.strip().lower().replace(" ", "_"),
            "Add to Quantity": lambda x: x.strip().lower().replace(" ", "_"),
            "Max QTY": lambda x: x.strip().lower().replace(" ", "_"),
        }
    )
    threshold: float = 3.0
    catalog_df: pd.DataFrame = field(init=False)
    pullsheet_df: pd.DataFrame = field(init=False)
    pullorder_df: pd.DataFrame = field(init=False)
    combined_data: pd.DataFrame = field(init=False)
    _match_rate: float = 0.0
    _total_value: float = 0.0
    _total_quantity: int = 0
    _total_adjusted_qty: int = 0
    _acv: float = 0.0
    _status: bool = False

    def __post_init__(self):
        """
        Post-initialization to clean the data and calculate initial metrics.
        """
        self.load_source_data()
        self.combined_data = self.merge_frames()

    def load_source_data(self):
        self.catalog_df = pd.read_csv(self.source_file_paths["catalog"])
        self.pullsheet_df = pd.read_csv(self.source_file_paths["pullsheet"])
        self.pullorder_df = pd.read_csv(self.source_file_paths["pullorder"])

    def init_or_update(self):
        self.dataframe = self._clean_data(self.dataframe, self.clean_rules)
        self._calculate_metrics()

    def _calculate_metrics(self):
        self._total_value = self._calculate_total_value()
        self._total_quantity = self._calculate_total_quantity()
        self._match_rate = self._calculate_match_rate()
        self._acv = self._recalculate_acv()
        self._update_status()



    def _calculate_total_value(self) -> float:
        """Calculates overall total submission value.
        Multiplies add_to_quantity and market value using vectorized methods for efficiency
        Returns:
            float: Submission value
        """
        return (
            self.dataframe["add_to_quantity"] * self.dataframe["tcg_market_price"]
        ).sum()
    
    def evaluate_submission(self, submission_df: pd.DataFrame) -> pd.DataFrame:
            # Prepare submission DataFrame for evaluation
            self.submission_df = submission_df
            self.evaluate()

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

    def __dict__(self):
        return {
            "acv": self.acv,
            "match_rate": self.match_rate,
            "status": self.status,
            "threshold": self.threshold,
            "total_value": self.total_value,
            "total_quantity": self.total_quantity,
            "total_adjusted_quantity": self.total_adjusted_quantity,
        }

    def __str__(self):
        return json.dumps(self.__dict__(), indent=4)

    def __repr__(self):
        return self

    @staticmethod
    def merge_frames(
        df1: pd.DataFrame, df2: pd.DataFrame, on: str, columns_to_join: list
    ) -> pd.DataFrame:
        # Select specified columns to join from df2 along with the merge column
        df2_selected = df2[[on] + list(columns_to_join)]
        return df1.merge(df2_selected, on=on, how="left")

    @staticmethod
    def _clean_data(
        dataframe: pd.DataFrame,
        clean_rules: Dict[str, Callable[[Any], bool]],
        format_rules: Dict[str, Callable[[Any], Any]] = None,
        invalid_file_path: str = "invalid_rows.csv",
    ) -> pd.DataFrame:
        """Cleans a DataFrame based on specified rules for each column and accumulates invalid rows in DataFrame."""
        invalid_accumulator = pd.DataFrame()

        for column, clean_func in clean_rules.items() and format_rules.items():
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
