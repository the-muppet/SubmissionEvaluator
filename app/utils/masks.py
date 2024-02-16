import pandas as pd


def is_not_empty(series: pd.Series) -> pd.Series:
    """Checks if each value in the Series is not empty."""
    return series != ""


def is_positive_numeric(series: pd.Series) -> pd.Series:
    """Checks if each value in the Series is a positive number."""
    numeric_series = pd.to_numeric(series, errors="coerce")
    return numeric_series > 0


def is_valid_tcgplayer_id(series: pd.Series) -> pd.Series:
    """Validates TCGplayer ID by checking if it is numeric and not empty."""
    return is_positive_numeric(series) & is_not_empty(series)


def is_positive_integer(series: pd.Series) -> pd.Series:
    """Checks if each value in the Series can be converted to a positive integer."""
    integer_series = pd.to_numeric(series, errors="coerce").fillna(0).astype(int)
    return integer_series > 0
