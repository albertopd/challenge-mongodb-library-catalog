from bson import ObjectId
import pandas as pd
from datetime import datetime
from pymongo import MongoClient, ReturnDocument
from pymongo.cursor import Cursor
from app.utils import parse_dict, parse_list, parse_date


class MongoDbLibraryCatalog:
    """
    MongoDbLibraryCatalog manages a MongoDB library catalog, providing methods for setup, data import, querying, and updating book records.
    """

    def __init__(self, host: str, port: int, db_name: str):
        """
        Initialize the MongoDbLibraryCatalog.
        Args:
            host (str): MongoDB host address.
            port (int): MongoDB port number.
            db_name (str): Name of the database to use.
        Raises:
            ValueError: If host, port, or db_name are invalid.
        """
        if not host or host.strip() == "":
            raise ValueError("Host must be provided.")
        if port <= 0:
            raise ValueError("Port must be a positive integer.")
        if not db_name or db_name.strip() == "":
            raise ValueError("Database name must be provided.")

        self._client = MongoClient(host=host, port=port)
        self._db = self._client[db_name]

    def close(self):
        """
        Closes the MongoDB connection.
        """
        self._client.close()

    def __enter__(self):
        """
        Enter the runtime context related to this object.
        Returns:
            MongoDbLibraryCatalog: The catalog instance itself.
        """
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """
        Exit the runtime context and close the MongoDB connection.
        """
        self.close()

    def count_books(self) -> int:
        """
        Get the total number of books in the 'books' collection.
        Returns:
            int: Number of books in the collection.
        """
        return self._db.books.count_documents({})

    def is_empty(self) -> bool:
        """
        Check if the 'books' collection is empty.
        Returns:
            bool: True if empty, False otherwise.
        """
        return self.count_books() == 0

    def populate_from_csv(
        self,
        csv_path: str,
        limit: int | None = None,
        chunksize: int = 5000,
    ) -> int:
        """
        Populate the MongoDB database with book documents from a CSV file in chunks.
        Args:
            csv_path (str): Path to the CSV file.
            limit (int | None, optional): Maximum number of records to insert. If None, insert all.
            chunksize (int, optional): Number of rows per chunk. Default is 5000.
        Returns:
            int: Total number of inserted documents.
        """
        total_inserted = 0
        reader = pd.read_csv(csv_path, chunksize=chunksize)

        for chunk in reader:
            # Respect the limit if provided
            if limit is not None:
                remaining = limit - total_inserted
                if remaining <= 0:
                    break
                chunk = chunk.head(remaining)

            # Transform dict columns
            complex_columns = [
                "genres",
                "characters",
                "awards",
                "ratingsByStars",
                "setting",
            ]
            for col in complex_columns:
                if col in chunk.columns:
                    chunk[col] = chunk[col].apply(parse_dict)

            # Convert ratingsByStars values to integers
            if "ratingsByStars" in chunk.columns:

                def to_int_list(lst):
                    if isinstance(lst, list):
                        return [int(x) for x in lst]
                    return [0, 0, 0, 0, 0]  # fallback if not a list

                chunk["ratingsByStars"] = chunk["ratingsByStars"].apply(to_int_list)

            # Transform authors
            if "author" in chunk.columns:
                chunk["authors"] = (
                    chunk["author"]
                    .str.replace(r"\s*\(Goodreads Author\)", "", regex=True)
                    .apply(parse_list)
                    .apply(
                        lambda lst: [
                            a.strip() for a in lst if a.strip().lower() != "more…"
                        ]
                    )
                )
                chunk.drop(columns=["author"], inplace=True)

            # Drop unnecessary columns
            drop_cols = [
                "bookId",
                "firstPublishDate",
                "coverImg",
                "bbeScore",
                "bbeVotes",
                "price",
            ]
            chunk = chunk.drop(columns=[c for c in drop_cols if c in chunk.columns])

            # Transform publishDate and ensure only Python datetime or None
            if "publishDate" in chunk.columns:
                chunk["publishDate"] = chunk["publishDate"].apply(parse_date)

            # Convert to list of dicts (faster than iterrows)
            books = chunk.to_dict(orient="records")

            # Aggressive sanitization: remove any non-datetime, pd.NaT, pd.Timestamp, numpy.datetime64
            import numpy as np

            for book in books:
                if "publishDate" in book:
                    val = book["publishDate"]
                    if val is None or isinstance(val, datetime):
                        continue
                    # Remove pandas NaT
                    if (hasattr(pd, "NaT") and val is pd.NaT) or str(
                        type(val)
                    ).endswith("NaTType'>"):
                        book["publishDate"] = None
                        continue
                    # Remove pd.Timestamp
                    if isinstance(val, pd.Timestamp):
                        book["publishDate"] = None
                        continue
                    # Remove numpy.datetime64
                    if isinstance(val, np.datetime64):
                        book["publishDate"] = None
                        continue
                    # Remove anything else
                    book["publishDate"] = None

            if books:
                self._db.books.insert_many(books)
                total_inserted += len(books)

        return total_inserted

    def get_book(self, query: dict) -> dict | None:
        """
        Retrieves a single book document from the database that matches the given query.

        Args:
            query (dict): A dictionary specifying the search criteria for the book.

        Returns:
            dict | None: The book document if found, otherwise None.
        """
        if "_id" in query and isinstance(query["_id"], str):
            query["_id"] = ObjectId(query["_id"])

        return self._db.books.find_one(query)

    def get_books_by_author(
        self, author: str, language: str | None = "English", limit: int | None = None
    ) -> Cursor:
        """
        Retrieve books written by a specified author.
        Args:
            author (str): Author name to search for.
            language (str | None, optional): Language of the books to retrieve.
            limit (int | None, optional): Maximum number of books to return. If None, return all.
        Returns:
            Cursor: MongoDB cursor for the query results.
        Raises:
            ValueError: If author is not provided or empty.
        """
        if author is None or author.strip() == "":
            raise ValueError("Author name must be provided and cannot be empty.")

        cursor = self._db.books.find({"authors": author, "language": language}).sort(
            "publishDate"
        )

        if limit:
            cursor = cursor.limit(limit)

        return cursor

    def get_rated_books_by_genre(self, genre: str, limit: int | None = None) -> Cursor:
        """
        Retrieve top-rated books for a specified genre.
        Args:
            genre (str): Genre to search for.
            limit (int | None, optional): Maximum number of books to return. If None, return all.
        Returns:
            Cursor: MongoDB cursor for the query results.
        Raises:
            ValueError: If genre is not provided or empty.
        """
        if genre is None or genre.strip() == "":
            raise ValueError("Genre must be provided and cannot be empty.")

        cursor = self._db.books.find({"genres": genre}).sort("rating", -1)

        if limit:
            cursor = cursor.limit(limit)

        return cursor

    def get_rated_books_by_year(self, year: int, limit: int | None = None) -> Cursor:
        """
        Retrieve top-rated books published in a specific year.
        Args:
            year (int): Year to search for.
            limit (int | None, optional): Maximum number of books to return. If None, return all.
        Returns:
            Cursor: MongoDB cursor for the query results.
        Raises:
            ValueError: If year is not provided.
        """
        if year is None:
            raise ValueError("Year must be provided and cannot be empty.")

        cursor = self._db.books.find(
            {"publishDate": {"$eq": datetime(year, 1, 1)}}
        ).sort("rating", -1)

        if limit:
            cursor = cursor.limit(limit)

        return cursor

    def get_genre_counts(self, limit: int | None = None) -> list[dict]:
        """
        Retrieve the count of books per genre.
        Args:
            limit (int | None, optional): Maximum number of genres to return. If None, return all.
        Returns:
            list[dict]: List of genres and their book counts.
        """
        pipeline = [
            {"$unwind": "$genres"},
            {"$group": {"_id": "$genres", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
        ]

        if limit:
            pipeline.append({"$limit": limit})

        genre_counts = self._db.books.aggregate(pipeline)
        return list(genre_counts)

    def get_author_counts(self, limit: int | None = None) -> list[dict]:
        """
        Retrieve a list of authors and the count of books associated with each author.
        Args:
            limit (int | None, optional): Maximum number of authors to return. If None, return all.
        Returns:
            list[dict]: List of authors and their book counts.
        """
        pipeline = [
            {"$unwind": "$authors"},
            {"$group": {"_id": "$authors", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
        ]

        if limit:
            pipeline.append({"$limit": limit})

        author_counts = self._db.books.aggregate(pipeline)
        return list(author_counts)

    def insert_book(self, book: dict) -> str:
        """
        Inserts a new book document into the 'books' collection in the database.

        Args:
            book (dict): A dictionary containing the book details to be inserted.

        Returns:
            str: The string representation of the inserted book's ObjectId.
        """
        result = self._db.books.insert_one(book)
        return str(result.inserted_id)

    def update_book(
        self, book_id: str, updates: dict, upsert: bool = False
    ) -> dict | None:
        """
        Update a book document in the database with the specified fields.
        Args:
            book_id (str): Unique identifier of the book to update.
            updates (dict): Dictionary of fields and values to update.
            upsert (bool, optional): If True, create a new document if no match is found. Default is False.
        Returns:
            dict | None: The updated book document if found, otherwise None.
        Raises:
            ValueError: If the updates dictionary is empty.
        """
        if not updates:
            raise ValueError("Updates cannot be empty.")

        updated_doc = self._db.books.find_one_and_update(
            {"_id": ObjectId(book_id)},
            {"$set": updates},
            return_document=ReturnDocument.AFTER,
            upsert=upsert,
        )
        return updated_doc

    def rate_book(self, book_id: str, stars: int) -> dict | None:
        if stars < 1 or stars > 5:
            raise ValueError("Rating stars must be between 1 and 5.")

        book = self.get_book({"_id": book_id})
        if not book:
            return None

        # Increase or add the star rating
        ratingsByStars = (
            [0, 0, 0, 0, 0]
            if not book.get("ratingsByStars")
            else book["ratingsByStars"]
        )
        ratingsByStars[5 - stars] += 1

        # Calculate total number of ratings
        numRatings = sum(ratingsByStars)

        # Calculate average rating
        averageRating = round(
            sum((5 - i) * count for i, count in enumerate(ratingsByStars))
            / numRatings,
            2
        )

        # Calculate liked percentage
        likedPercent = int(
            round(sum(ratingsByStars[0:3]) / numRatings * 100, 0)
        )

        # Update book ratings
        updated_data = {
            "ratingsByStars": ratingsByStars,
            "numRatings": numRatings,
            "rating": averageRating,
            "likedPercent": likedPercent,
        }
        return self.update_book(book_id, updated_data)

    def delete_book(self, book_id: str) -> bool:
        """
        Deletes a book from the database by its ID.

        Args:
            book_id (str): The unique identifier of the book to delete.

        Returns:
            bool: True if the book was successfully deleted, False otherwise.
        """
        result = self._db.books.delete_one({"_id": ObjectId(book_id)})
        return result.deleted_count > 0
