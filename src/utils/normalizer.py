import pandas as pd


def normalize_headers(dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Standardizes dataframe headers to lowercase and replaces spaces with underscores.

    Args:
        dataframe (pd.DataFrame): The dataframe to normalize headers for.

    Returns:
        pd.DataFrame: The dataframe with normalized headers.
    """
    dataframe.columns = dataframe.columns.str.strip().str.lower().str.replace(" ", "_")
    return dataframe


def handle_missing_data(
    dataframe: pd.DataFrame, strategy: str = "drop", fill_value=None
) -> pd.DataFrame:
    """
    Handles missing data in a dataframe according to the specified strategy.

    Args:
        dataframe (pd.DataFrame): The dataframe to process.
        strategy (str): Strategy for handling missing data ('drop', 'fill', 'none').
        fill_value (any): Value to use for filling missing data if strategy is 'fill'.

    Returns:
        pd.DataFrame: The processed dataframe.
    """
    if strategy == "drop":
        dataframe = dataframe.dropna()
    elif strategy == "fill":
        dataframe = dataframe.fillna(fill_value)
    # 'none' strategy leaves the dataframe as is
    return dataframe
