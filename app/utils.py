import ast
import math
import re
import pandas as pd
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


def parse_year(value) -> int | None:
    """
    Parses a value and attempts to extract a 4-digit year.
    Args:
        value: The input value to parse, which can be a string, number, or other type.
    Returns:
        int | None: The extracted year as an integer if successful, otherwise None.
    Notes:
        - Returns None if the value is NaN, empty, or cannot be parsed as a year.
        - Tries to match a 4-digit year directly for efficiency.
        - Falls back to fuzzy date parsing if direct match fails.
    """
    if pd.isna(value):
        return None
    
    strvalue = str(value).strip()
    if not strvalue:
        return None
    
    # Try to match a 4-digit year first for efficiency
    match = re.fullmatch(r"\d{4}", strvalue)
    if match:
        return int(strvalue)
    
    try:
        return parser.parse(strvalue, fuzzy=True).year
    except (ValueError, OverflowError, TypeError):
        return None


def get_total_chunks(csv_path, chunksize):
    """
    Calculates the total number of chunks required to process a CSV file in batches.

    Args:
        csv_path (str): The file path to the CSV file.
        chunksize (int): The number of rows per chunk.

    Returns:
        int: The total number of chunks needed to process the file. Returns 0 if an error occurs.

    Notes:
        Assumes the first row of the CSV file is a header and excludes it from the row count.
    """
    try:
        with open(csv_path, "rb") as f:
            total_rows = sum(1 for _ in f) - 1  # subtract header row
        return math.ceil(total_rows / chunksize)
    except Exception as e:
        print(f"Error occurred while getting total chunks: {e}")
        return 0