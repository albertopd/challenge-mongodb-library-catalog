import pandas as pd
from app.utils.parse_helpers import parse_dict, parse_list, parse_year


def __drop_unnecessary_columns(chunk: pd.DataFrame) -> pd.DataFrame:
    """
    Removes unnecessary columns from the given DataFrame chunk.

    Parameters:
        chunk (pd.DataFrame): The input DataFrame from which specified columns will be dropped.

    Returns:
        pd.DataFrame: A DataFrame with the specified columns removed if they exist.
    """
    drop_cols = [
        "bookId",
        "firstPublishDate",
        "coverImg",
        "bbeScore",
        "bbeVotes",
        "price",
    ]
    return chunk.drop(columns=[c for c in drop_cols if c in chunk.columns])


def __transform_dict_columns(chunk: pd.DataFrame) -> pd.DataFrame:
    """
    Transforms specified columns in a DataFrame by applying the `parse_dict` function.

    This function iterates over a predefined list of column names and, if the column exists in the input DataFrame,
    applies the `parse_dict` function to each element in that column. The transformation is performed in-place.

    Args:
        chunk (pd.DataFrame): The input DataFrame containing the columns to be transformed.

    Returns:
        pd.DataFrame: The DataFrame with the specified columns transformed using `parse_dict`.
    """
    dict_columns = ["genres", "characters", "awards", "ratingsByStars", "setting"]
    for col in dict_columns:
        if col in chunk.columns:
            chunk[col] = chunk[col].apply(parse_dict)
    return chunk


def __transform_ratings_by_stars(chunk: pd.DataFrame) -> pd.DataFrame:
    """
    Transforms the 'ratingsByStars' column in the given DataFrame chunk.

    If the 'ratingsByStars' column exists, each entry is converted to a list of integers.
    If an entry is not a list, it is replaced with [0, 0, 0, 0, 0].

    Args:
        chunk (pd.DataFrame): The DataFrame chunk to transform.

    Returns:
        pd.DataFrame: The transformed DataFrame chunk with normalized 'ratingsByStars' values.
    """
    if "ratingsByStars" in chunk.columns:
        chunk["ratingsByStars"] = chunk["ratingsByStars"].apply(
            lambda lst: (
                [int(x) for x in lst] if isinstance(lst, list) else [0, 0, 0, 0, 0]
            )
        )
    return chunk


def __transform_authors(chunk: pd.DataFrame) -> pd.DataFrame:
    """
    Transforms the 'author' column in the given DataFrame chunk by cleaning and parsing author names.

    If the 'author' column exists:
        - Removes the substring '(Goodreads Author)' from author names.
        - Parses the author string into a list using the `parse_list` function.
        - Strips whitespace from each author name and removes any entries equal to 'more…' (case-insensitive).
        - Stores the result in a new 'authors' column and drops the original 'author' column.

    Args:
        chunk (pd.DataFrame): A pandas DataFrame containing an 'author' column to be transformed.

    Returns:
        pd.DataFrame: The transformed DataFrame with the 'authors' column.
    """
    if "author" in chunk.columns:
        chunk["authors"] = (
            chunk["author"]
            .str.replace(r"\s*\(Goodreads Author\)", "", regex=True)
            .apply(parse_list)
            .apply(lambda lst: [a.strip() for a in lst if a.strip().lower() != "more…"])
        )
        chunk.drop(columns=["author"], inplace=True)
    return chunk


def __transform_publish_year(chunk: pd.DataFrame) -> pd.DataFrame:
    """
    Extracts the publication year from the 'publishDate' column and adds it as a new 'publishYear' column.

    Parameters:
        chunk (pd.DataFrame): Input DataFrame containing a 'publishDate' column.

    Returns:
        pd.DataFrame: DataFrame with an additional 'publishYear' column containing parsed years as integers.
    """
    if "publishDate" in chunk.columns:
        chunk["publishYear"] = chunk["publishDate"].apply(parse_year).astype("Int64")
    return chunk


def transform_chunk(chunk: pd.DataFrame) -> pd.DataFrame:
    """
    Transforms a pandas DataFrame chunk by sequentially applying a series of transformation functions.

    The following transformations are applied in order:
        1. Drops unnecessary columns.
        2. Transforms columns containing dictionary-like data.
        3. Converts ratings to a star-based format.
        4. Processes author information.
        5. Transforms the publish year column.

    Args:
        chunk (pd.DataFrame): The input DataFrame chunk to be transformed.

    Returns:
        pd.DataFrame: The transformed DataFrame chunk.
    """
    for func in [
        __drop_unnecessary_columns,
        __transform_dict_columns,
        __transform_ratings_by_stars,
        __transform_authors,
        __transform_publish_year,
    ]:
        chunk = func(chunk)
    return chunk
