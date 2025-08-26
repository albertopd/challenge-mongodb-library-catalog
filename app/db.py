import os
import pandas as pd
from pymongo import MongoClient
from pymongo.database import Database
from app.utils import parse_dict, parse_list, parse_date


def setup_database(limit: int | None = None) -> Database:
    """
    Initializes and returns a MongoDB database connection.
    This function connects to a MongoDB instance using environment variables for host, port, and database name.
    If the 'books' collection is empty, it populates the database with book records from a CSV file specified by an environment variable.
    Prints status messages during setup and population.
    Args:
        limit (int, optional): Maximum number of book records to populate from the CSV file.
    Returns:
        Database: A pymongo Database instance connected to the specified database.
    """
    print("Setting up database...")

    db_host = os.getenv("MONGO_DB_HOST", "localhost")
    db_port = int(os.getenv("MONGO_DB_PORT", "27017"))
    db_name = os.getenv("MONGO_DB_NAME", "becode")
    csv_path = os.getenv("CSV_DATASET_PATH", "data/books.csv")

    client = MongoClient(host=db_host, port=db_port)
    db = client[db_name]

    # If DB is empty, populate it with books from CSV dataset
    if db.books.count_documents({}) == 0:
        print(f"Populating database with books from CSV file: {csv_path} ...")
        df = pd.read_csv(csv_path)
        populate_db_from_df(df, db, limit)

    print("Database setup complete.\n")
    return db

def populate_db_from_df(df: pd.DataFrame, db: Database, limit: int | None = None) -> None:
    """
    Populates the MongoDB database with book documents from a pandas DataFrame.
    This function processes the DataFrame by parsing complex columns, transforming author information,
    dropping unwanted columns, and converting date fields. It then inserts up to `limit` book documents
    into the `books` collection of the provided database.
    Args:
        df (pd.DataFrame): The DataFrame containing book data.
        db (Database): The MongoDB database instance where books will be inserted.
        limit (int, optional): The maximum number of books to insert.
    Returns:
        None
    """
    # Columns that need parsing as dict
    complex_columns = ["genres", "characters", "awards", "ratingsByStars", "setting"]
    for col in complex_columns:
        if col in df.columns:
            df[col] = df[col].apply(parse_dict)

    # Transform authors
    if "author" in df.columns:
        df["authors"] = df["author"].apply(parse_list)
        df.drop(columns=["author"], inplace=True)

    # Drop unwanted columns
    drop_cols = [
        "bookId",
        "firstPublishDate",
        "coverImg",
        "bbeScore",
        "bbeVotes",
        "price",
    ]
    df = df.drop(columns=[c for c in drop_cols if c in df.columns])

    # Convert publishDate
    if "publishDate" in df.columns:
        df["publishDate"] = df["publishDate"].apply(parse_date)

    # Build documents
    if limit:
        books = [row.to_dict() for _, row in df.head(limit).iterrows()]
    else:
        books = [row.to_dict() for _, row in df.iterrows()]

    if books:
        db.books.insert_many(books)
        print(f"Database populated with {len(books)} books.")
    else:
        print("No books found for populating the database.")