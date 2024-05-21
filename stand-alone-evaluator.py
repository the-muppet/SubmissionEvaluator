import re
import os
import csv
import logging
import argparse
import sys
import numpy as np
import pandas as pd
from pathlib import Path
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Tuple
import configparser

config = configparser.ConfigParser()
config.read('config.ini')

# Setup logger
def get_logger() -> logging.Logger:
    """
    Initializes and returns a logger with INFO level for application-wide logging.
    Returns:
        logging.Logger: A logger configured with INFO level for the current module.
    """
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler(
        sys.stdout
    )
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    handler.setFormatter(formatter)
    logger.addHandler(
        handler
    )
    return logger


logger = get_logger()


def is_not_empty(value: str) -> bool:
    """
    Checks if a given string value is not empty.
    Args:
        value (str): The value to check.
    Returns:
        bool: True if the value is not empty, False otherwise.
    """
    return value != ""


def is_numeric(value: str) -> float | None:
    """
    Attempts to convert a string to a float and checks if it is a positive number.
    Args:
        value (str): The string to convert and check.
    Returns:
        float: The numeric value if conversion is successful and value is positive, None otherwise.
    """
    try:
        val = float(value)
        return val if val > 0 else None
    except ValueError:
        return None


def is_positive_integer(value: str) -> int | None:
    """
    Attempts to convert a string to an integer and checks if it is positive.
    Args:
        value (str): The string to convert and check.
    Returns:
        int: The integer value if conversion is successful and value is positive, None otherwise.
    """
    try:
        val = int(value)
        return val if val > 0 else None
    except ValueError:
        return None


def is_valid_tcgplayer_id(value: str) -> bool:
    """
    Validates a TCGplayer ID by checking if it is numeric and not empty.
    Args:
        value (str): The TCGplayer ID to validate.
    Returns:
        bool: True if the ID is valid, False otherwise.
    """
    return is_numeric(value) is not None and is_not_empty(value)


clean_rules = {
    "TCGplayer Id": is_valid_tcgplayer_id,
    "Add to Quantity": is_positive_integer,
}


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
        _pullsheet_missing_rate (float): Percentage of submissions not found on the pullsheet. Not directly settable.
        _catalog_on_pullsheet_rate (float): Percentage of catalog items found on the pullsheet. Not directly settable.
        _total_rejected_quantity (int): Total quantity of cards rejected based on criteria. Not directly settable.
    """

    dataframe: pd.DataFrame = field(default_factory=pd.DataFrame)
    _match_rate: float = field(default=0.0, init=False, repr=False)
    _total_value: float = field(default=0.0, init=False, repr=False)
    _total_quantity: int = field(default=0, init=False, repr=False)
    _total_adjusted_qty: int = field(default=0, init=False, repr=False)
    threshold: float = field(default=3.0, init=False, repr=False)
    _acv: float = field(default=0.0, init=False, repr=False)
    _status: bool = field(default=False, init=False, repr=False)
    _pullsheet_missing_rate: float = field(default=0.0, init=False, repr=False)
    _catalog_on_pullsheet_rate: float = field(default=0.0, init=False, repr=False)
    _total_rejected_quantity: int = field(default=0, init=False, repr=False)

    def __post_init__(self):
        """
        Post-initialization to clean the data and calculate initial metrics.
        """
        self.init_or_update()

    def init_or_update(self):
        self.dataframe = SubmissionEvaluator._clean_data(self.dataframe, clean_rules)
        self._calculate_metrics()

    def _calculate_metrics(self):
        self._total_value = self._calculate_total_value()
        self._total_quantity = self._calculate_total_quantity()
        self._match_rate = self._calculate_match_rate()
        self._acv = self._recalculate_acv()
        self._update_status()
        self._pullsheet_missing_rate = self._calculate_pullsheet_missing_rate()
        self._catalog_on_pullsheet_rate = self._calculate_catalog_on_pullsheet_rate()
        self._total_rejected_quantity = self._calculate_total_rejected_quantity()

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
                valid_mask = dataframe[column].apply(clean_func)
                invalid = dataframe[~valid_mask].copy()

                invalid["invalid_reason"] = f"Invalid {column}"

                invalid_accumulator = pd.concat(
                    [invalid_accumulator, invalid], ignore_index=True
                )

                dataframe = dataframe[valid_mask]

        invalid_accumulator.drop_duplicates(
            subset=dataframe.index.name, keep="first", inplace=True
        )

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

    def _calculate_pullsheet_missing_rate(self) -> float:
        """
        Calculates the percentage of submitted cards that are not found on the pullsheet.
        Returns:
            float: The pullsheet missing rate.
        """
        total_submitted_quantity = self._calculate_total_quantity()
        missing_quantity = (
            self.dataframe["add_to_quantity"][self.dataframe["max_qty"] == 0]
            .sum()
            .astype(int)
        )
        return (missing_quantity / total_submitted_quantity) * 100

    def _calculate_catalog_on_pullsheet_rate(self) -> float:
        """
        Calculates the percentage of catalog items that are found on the pullsheet.
        Returns:
            float: The catalog on pullsheet rate.
        """
        total_catalog_items = len(self.dataframe["tcgplayer_id"].unique())
        items_on_pullsheet = len(self.dataframe["tcgplayer_id"][self.dataframe["max_qty"] > 0].unique())
        return (items_on_pullsheet / total_catalog_items) * 100

    def _calculate_total_rejected_quantity(self) -> int:
        """
        Calculates the total quantity of cards rejected based on the criteria.
        Returns:
            int: Total rejected quantity
        """
        total_rejected_quantity = self._total_quantity - self._total_adjusted_qty
        return total_rejected_quantity

    def _recalculate_acv(self):
        """Recalculates the average card value of the submission.
        Returns:
            float: Average card value (ACV)
        """
        self.acv = (
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

    @property
    def pullsheet_missing_rate(self):
        return self._pullsheet_missing_rate

    @property
    def catalog_on_pullsheet_rate(self):
        return self._catalog_on_pullsheet_rate

    @property
    def total_rejected_quantity(self):
        return self._total_rejected_quantity

    def _reduce_acv_impact(
        self, df: pd.DataFrame, min_total_quantity: int = 500
    ) -> Tuple[pd.DataFrame, int]:
        """
        Reduces ACV impact by removing items from the end of the DataFrame post sorting.
        Returns the modified DataFrame and the number of rows removed.
        Args:
            df (pd.DataFrame): The DataFrame containing the items.
            min_total_quantity (int, optional): The minimum quantity threshold. Defaults to 500.
        Returns:
            Tuple[pd.DataFrame: The modified DataFrame, int: The number of rows removed.]
        """
        original_length = len(df)
        df = df.sort_values(by="tcg_market_price", ascending=False)
        while len(df) > min_total_quantity:
            df = df[:-1]
            self.dataframe = df
            self.init_or_update()
            if self.status:
                break
        removed_rows = original_length - len(df)
        return df, removed_rows

    def curate_submission(self, min_total_quantity: int = 500) -> pd.DataFrame:
        """
        Curates the submission to try to meet the ACV threshold while maintaining a minimum quantity.
        Returns the curated dataframe and logs the number of rows removed if the status changes to accepted.
        """
        curated_df = self.dataframe.copy()
        initial_status = self.status

        curated_df, removed_rows = self._reduce_acv_impact(
            curated_df, min_total_quantity
        )

        if not initial_status and self.status:
            logger.info(f"Status changed to Accepted by removing {removed_rows} rows.")

        return curated_df

    def export_lazy_curated(self, output_dir: str) -> str:
        """
        Compares the curated DataFrame with the original and exports either the curated DataFrame or
        the DataFrame of removed items as a CSV, whichever is smaller.
        Args: output_dir: Directory to save the CSV file.
        Returns: The path to the exported CSV file.
        """
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        original_df = self.dataframe
        curated_df = self.curate_submission()

        # Identifying removed items
        removed_items_df = original_df.merge(
            curated_df, how="outer", indicator=True
        ).loc[lambda x: x["_merge"] == "left_only"]
        removed_items_df.drop(columns=["_merge"], inplace=True)

        df_to_export = (
            curated_df if len(curated_df) < len(removed_items_df) else removed_items_df
        )
        file_name = (
            "curated_submission.csv"
            if len(curated_df) < len(removed_items_df)
            else "removed_items.csv"
        )
        file_path = os.path.join(output_dir, file_name)

        df_to_export.to_csv(file_path, index=False)
        return file_path

    def pretty_print(self, curated=False):
        """Prints the evaluation results to the console."""
        status_text = "Accepted" if self.status else "Rejected"
        print("Submission Evaluation" + (" - Curated" if curated else ""))
        print("------------------")
        print(f"Match Rate: {round(self.match_rate, 2)}%")
        print(f"Total Value: ${self.total_value:.2f}")
        print(f"Total Quantity: {int(self.total_quantity)}")
        print(
            f"Pullsheet Missing Rate: {round(self.pullsheet_missing_rate, 2)}%"
        ) 
        print(f"Total Rejected Quantity: {int(self.total_rejected_quantity)}")
        print(f"ACV: ${self.acv:.2f}")
        print(f"Status: {status_text}")


def load_file(
    source_path: Path, encodings: List[str], column_types: Dict[int, str]
) -> pd.DataFrame:
    """
    Attempts to load a CSV file with various encodings until successful.
    Args:
        source_path (Path): The path to the CSV file.
        encodings (List[str]): A list of encodings to try.
        column_types (Dict[int, str]): A dictionary specifying the dtype for certain columns.
    Returns:
        pd.DataFrame: The loaded DataFrame.
    Raises:
        ValueError: If the file cannot be loaded with any of the provided encodings.
    """
    for encoding in encodings:
        try:
            logger.info(
                f"Attempting to load file {source_path} with encoding {encoding}"
            )
            return pd.read_csv(
                source_path, encoding=encoding, dtype=column_types, on_bad_lines="skip"
            )
        except UnicodeDecodeError:
            logger.warning(
                f"Failed to load {source_path} with encoding {encoding}; trying next."
            )
        except Exception as e:
            logger.error(f"Error loading {source_path} with encoding {encoding}: {e}")
            continue
    raise ValueError(
        f"Failed to load {source_path} with any of the provided encodings."
    )


def normalize_headers(*dataframes: pd.DataFrame) -> Tuple[pd.DataFrame, ...]:
    """
    Normalizes column headers of passed DataFrames by removing whitespace and converting to lowercase.
    Args:
        *dataframes: Variable number of DataFrame objects to be normalized.
    Returns:
        A tuple containing all input DataFrames with normalized headers.
    """
    normalized_dataframes = []
    for dataframe in dataframes:
        dataframe.columns = (
            dataframe.columns.str.strip().str.replace(" ", "_").str.lower()
        )
        normalized_dataframes.append(dataframe)
    return tuple(normalized_dataframes)


def load_source_files(sources: Dict[str, Path]) -> Tuple[pd.DataFrame, ...]:
    """
    Loads multiple CSV files specified in a dictionary, attempting various encodings.
    Args:
        sources (Dict[str, Path]): A dictionary where keys are descriptive names and values are file paths.
    Returns:
        Tuple[pd.DataFrame, ...]: A tuple containing loaded DataFrames in the order of sources.
    Raises:
        ValueError: If any file fails to load.
    """
    encodings = ["utf-8", "ISO-8859-1", "utf-16"]
    column_types = {0: "str", 4: "str", 5: "str", 15: "str"}

    loaded_dataframes = []
    for key, path in sources.items():
        try:
            df = load_file(path, encodings, column_types)
            (df,) = normalize_headers(df)
            loaded_dataframes.append(df)
        except ValueError as e:
            logger.error(f"Error in loading {key} from {path}: {e}")
            raise e  # Rethrow the exception to handle it outside

    return tuple(loaded_dataframes)


def merge_frames(
    df1: pd.DataFrame,
    df2: pd.DataFrame,
    on: str,
    columns_to_join: list,
) -> pd.DataFrame:
    """
    Merges two DataFrames on a specified column, including only the specified columns from the second DataFrame.
    Args:
        df1 (pd.DataFrame): The first DataFrame.
        df2 (pd.DataFrame): The second DataFrame to join to the first.
        on (str): The column name to merge on.
        columns_to_join (list): The list of column names from df2 to include in the merge.
    Returns:
        pd.DataFrame: The merged DataFrame.
    """
    df2_selected = df2[[on] + columns_to_join]

    return df1.merge(df2_selected, on=on, how="left")


def generate_final_report_summary(
    submission: SubmissionEvaluator, report_file_name: str
) -> None:
    """
    Generates a final report summarizing the evaluation of a submission and saves it to a CSV file.
    Args:
        submission (SubmissionEvaluator): The submission evaluator object containing evaluation metrics.
        report_file_name (str): The file path to save the report summary CSV.
    Returns:
        None: This function does not return a value but writes directly to a file.
    """
    final_report_data = {
        "acv": submission.acv,
        "match_rate": submission.match_rate,
        "status": submission.status,
        "threshold": submission.threshold,
        "total_quantity": submission.total_quantity,
        "total_value": submission.total_value,
        "pullsheet_missing_rate": submission.pullsheet_missing_rate, 
        "total_rejected_quantity": submission.total_rejected_quantity,
    }
    with open(report_file_name, "w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=final_report_data.keys())
        writer.writeheader()
        writer.writerow(final_report_data)


def select_file():
    """
    Prompts the user for a file path through the command line and validates its existence.
    Returns:
        str: The validated file path input by the user.
    Exits:
        Terminates the script if no file path is provided or if the provided file path does not exist.
    """
    file_path = input("Please enter the path to the submission CSV file: ").strip()
    if not file_path:
        print("No file path provided. Exiting.")
        sys.exit(1)
    if not os.path.exists(file_path):
        print(f"The file {file_path} does not exist. Exiting.")
        sys.exit(1)
    return file_path


def main():
    """
    Main function to execute the submission file processing workflow.
    This function handles argument parsing, file loading, submission evaluation, and report generation.
    """
    parser = argparse.ArgumentParser(description="Process submission file.")
    parser.add_argument(
        "-f", "--file", help="Path to the submission CSV file", default=None
    )
    args = parser.parse_args()

    submission_file = args.file
    if not submission_file:
        submission_file = select_file()
        if not submission_file:
            print("No file selected. Exiting.")
            return

    try:
        sources = {
            "pullsheet": config['FILES']['pullsheet'],
            "pullorder": config['FILES']['pullorder'],
            "catalog": config['FILES']['catalog'],
            "submission": submission_file,
        }

        pullsheet_df, pullorder_df, catalog_df, submission_df = load_source_files(
            sources
        )

        if "add_to_quantity" in catalog_df.columns:
            catalog_df = catalog_df.drop("add_to_quantity", axis=1)

        merged_df = merge_frames(
            pullsheet_df,
            pullorder_df,
            on="set_name",
            columns_to_join=["shelf_order"],
        )
        catalog_plus_df = merge_frames(
            catalog_df,
            merged_df,
            on="tcgplayer_id",
            columns_to_join=["max_qty", "shelf_order"],
        )
        final_df = merge_frames(
            catalog_plus_df,
            submission_df,
            on="tcgplayer_id",
            columns_to_join=["add_to_quantity"],
        )
        submission = SubmissionEvaluator(final_df)
        generate_final_report_summary(submission, config['FILES']['report_file'])
        submission.pretty_print()

    except Exception as e:
        logger.error(f"Error in main function: {e}")
        return


if __name__ == "__main__":
    main()
