import ast
import pandas as pd
from datetime import datetime
from dateutil import parser

def parse_dict(value):
    """
    Converts a string representation of a dictionary into a Python dictionary object.

    If the input value is a string that can be safely evaluated as a dictionary,
    it returns the corresponding Python dictionary. If the evaluation fails due to
    a ValueError or SyntaxError, or if the input is not a string, the original value
    is returned unchanged.

    Args:
        value (str or any): The value to be parsed.

    Returns:
        dict or any: The parsed dictionary if successful, otherwise the original value.
    """
    """Convert stringified dicts into Python objects."""
    if isinstance(value, str):
        try:
            return ast.literal_eval(value)
        except (ValueError, SyntaxError):
            return value
    return value

def parse_list(value: str) -> list[str]:
    """
    Converts a comma-separated string into a list of individual items.

    Args:
        value (str): A string containing items separated by commas.

    Returns:
        list[str]: A list of items with leading and trailing whitespace removed. Returns an empty list if input is not a string.
    """
    """Convert comma-separated string into list of items."""
    if isinstance(value, str):
        return [item.strip() for item in value.split(",") if item.strip()]
    return []

def parse_date(value) -> datetime | None:
    """
    Converts a value representing a date in mixed formats to a `datetime.datetime` object.

    Parameters:
        value: The input value to be parsed. Can be a string, `datetime`, `pandas.Timestamp`, or other types.

    Returns:
        datetime | None: A `datetime.datetime` object if parsing is successful, or `None` if the value is invalid or cannot be parsed.

    Notes:
        - Handles `NaN` values by returning `None`.
        - Accepts `datetime` and `pandas.Timestamp` objects directly.
        - Attempts to parse strings and other types using dateutil's parser with fuzzy matching.
    """
    """Convert mixed date formats to datetime.datetime, or None if invalid."""
    if pd.isna(value):
        return None
    if isinstance(value, (datetime, pd.Timestamp)):
        return value if isinstance(value, datetime) else value.to_pydatetime()
    try:
        dt = parser.parse(str(value), fuzzy=True)
        return dt
    except Exception:
        return None