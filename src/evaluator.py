from src.data_loader import DataLoader
from src.utils.config_manager import ConfigManager
from src.utils.masks import is_valid_tcgplayer_id, is_positive_integer
from dataclasses import dataclass, field, InitVar
from typing import Any, Callable, Dict
import pandas as pd

@dataclass
class SubmissionEvaluator:
    eval_frame: pd.DataFrame = field(default_factory=pd.DataFrame)
    source_timestamps: Dict = field(default_factory=dict)
    config_manager: InitVar[ConfigManager]
    data_loader: InitVar[Any]  # Marked as InitVar because it's used in __post_init__
    clean_rules: Dict[str, Callable[[Any], bool]] = field(
        default_factory=lambda: {
            "TCGplayer Id": lambda x: is_valid_tcgplayer_id(x),
            "Add to Quantity": lambda x: is_positive_integer(x),
        }
    )
    acv_threshold: float = 0.0  # Initialize with a default value or 0.0
    _match_rate: float = 0.0
    _total_value: float = 0.0
    _total_quantity: int = 0
    _total_adjusted_qty: int = 0
    _acv: float = 0.0
    _status: bool = False

    def __post_init__(self, config_manager: ConfigManager):
        self.data_loader = DataLoader(config_manager)
        self.acv_threshold = self.data_loader.get("Metrics", "THRESHOLD")
        self.load_and_prep_frames()

    def load_and_prep_frames(self):
        data_frames = self.data_loader.load_required_data()
        catalog_df = data_frames["catalog_df"]
        pullsheet_df = data_frames["pullsheet_df"]
        pullorder_df = data_frames["pullorder_df"]

        self.eval_frame = self.merge_source_frames(
            catalog_df, pullsheet_df, pullorder_df
        )


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

    def _calculate_acv(self):
        """Calculates the average card value of the submission. and updates status
        Returns:
            float: Average card value (ACV)
        """
        self._acv = (
            self._total_value / self._total_quantity if self._total_quantity > 0 else 0
        )

    def _update_status(self):
        """
        Updates the status of the submission based on the criteria.
        Returns:
            None: (but also pseudo-boolean via 'Accepted/Rejected' indicator)
        """
        self._status = (
            False
            if self._total_quantity < self.data_loader.get("Threshold", "QTY")
            else self.acv >= self.threshold
        )

    # Getter/setters for automatic variable recalculation
    @property
    def match_rate(self):
        return self._match_rate

    @match_rate.setter
    def match_rate(self, value):
        self._match_rate = value
        self.acv_threshold = (
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

    def merge_source_frames(self, catalog_df, pullsheet_df, pullorder_df):
        """
        - Drop "add_to_quantity" from catalog_df to avoid _x _y post-merge.
        - Join "max_qty" to catalog_df from pullsheet_df using "tcgplayer_id"
        - Join "shelf_order" to catalog_df from pullorder_df using "set_name"
        """
        # Drop
        catalog_df = catalog_df.drop["add_to_quantity"]
        # Join 1
        catalog_df = pd.merge(
            catalog_df,
            pullsheet_df[["tcgplayer_id", "max_qty"]],
            on="tcgplayer_id",
            how="left",
        )
        # Join 2
        catalog_df = pd.merge(
            catalog_df,
            pullorder_df[["set_name", "sheet_order"]],
            on="set_name",
            how="left",
        )
        return catalog_df

    @staticmethod
    def _format_headers(
        dataframe: pd.DataFrame, format_rules: Dict[str, Callable[[str], str]]
    ) -> pd.DataFrame:
        """
        Formats the headers of a DataFrame based on specified rules.

        Args:
            dataframe (pd.DataFrame): The DataFrame whose headers need formatting.
            format_rules (Dict[str, Callable[[str], str]]): A dictionary where keys represent the original
                header names, and values are functions that take and return a string, applying the desired
                formatting to the header name.

        Returns:
            pd.DataFrame: A DataFrame with formatted headers.
        """
        # Format each header according to the provided rules
        new_columns = {
            col: format_rules.get(col, lambda x: x)(col) for col in dataframe.columns
        }
        # Apply the new formatted column names to the DataFrame
        dataframe.rename(columns=new_columns, inplace=True)
        
        return dataframe

    @staticmethod
    def _apply_header_formats(dataframe: pd.DataFrame) -> pd.DataFrame:
        """
        Applies generic formatting rules to all headers in the DataFrame.

        Args:
            dataframe (pd.DataFrame): The DataFrame whose headers need formatting.

        Returns:
            pd.DataFrame: The DataFrame with formatted headers.
        """
        # Define a generic formatting function, e.g., strip, lower, replace spaces with underscores
        format_func = lambda header: header.strip().lower().replace(" ", "_")
        formatted_headers = {col: format_func(col) for col in dataframe.columns}

        # Apply the formatted headers to the DataFrame
        dataframe.rename(columns=formatted_headers, inplace=True)
        return dataframe

    @staticmethod
    def _clean_data(
        dataframe: pd.DataFrame,
        clean_rules: Dict[str, Callable[[Any], bool]],
        invalid_file_path: str = "invalid_rows.csv",
    ) -> pd.DataFrame:
        """Cleans a DataFrame based on specified rules for each column and accumulates invalid rows in DataFrame."""
        invalid_rows = pd.DataFrame()

        for column, clean_func in clean_rules.items():
            if column in dataframe.columns:
                # Identify valid and invalid rows
                valid_mask = dataframe[column].apply(clean_func)
                invalid = dataframe[~valid_mask].copy()

                # Add a column to indicate the reason for invalidity
                invalid["invalid_reason"] = f"Invalid {column}"

                # Accumulate invalid rows
                invalid_rows = pd.concat([invalid_rows, invalid], ignore_index=True)
                # Keep only valid rows
                dataframe = dataframe[valid_mask]

        # Deduplicate the invalid_rows based on index
        invalid_rows.drop_duplicates(
            subset=dataframe.index.name, keep="first", inplace=True
        )

        # Write non-valid rows to a CSV file if they exist
        if not invalid_rows.empty:
            invalid_rows.to_csv(invalid_file_path, index=False)
            print(f"Non-valid rows written to {invalid_file_path}")

        return dataframe
