import pandas as pd
import numpy as np
from dataclasses import dataclass, field
from src.data_loader import DataLoader
from src.utils.config_manager import ConfigManager
from src.utils.logger import setup_logger
from src.models import Submission

logger = setup_logger()
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

    def results_to_dict(self):
        """
        Collects evaluation metrics into a dictionary suitable for JSON responses.
        """
        return {
            "status": "Accepted" if self._status else "Rejected",
            "total_value": self._total_value,
            "total_quantity": self._total_quantity,
            "match_rate": self._match_rate,
            "acv": self._acv,
        }
