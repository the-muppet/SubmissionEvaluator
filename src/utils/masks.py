@staticmethod
def is_not_empty(value: str | int | float) -> bool:
    """
    Checks if a given string value is not empty.
    Args:
        value (str): The value to check.
    Returns:
        bool: True if the value is not empty, False otherwise.
    """
    return value != ""


@staticmethod
def is_numeric(value: int | float) -> bool:
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
def is_positive_integer(value: int | float) -> bool:
    """
    Checks if the input value can be converted to a positive integer.
    """
    try:
        val = int(value)
        return val > 0
    except (ValueError, TypeError):
        return False


@staticmethod
def is_valid_tcgplayer_id(value: int | str) -> bool:
    """
    Validates a TCGplayer ID by checking if it is numeric and not empty.
    Args:
        value (str): The TCGplayer ID to validate.
    Returns:
        bool: True if the ID is valid, False otherwise.
    """
    if is_numeric(value) and is_not_empty(value):
        return True
