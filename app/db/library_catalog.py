import math
import pandas as pd
from bson import ObjectId
from pymongo import MongoClient, ReturnDocument
from pymongo.cursor import Cursor
from app.pipelines.transform import transform_chunk


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

    def _get_total_chunks(
        self, 
        csv_path: str, 
        chunksize: int
    ) -> int:
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
        total_chunks = self._get_total_chunks(csv_path, chunksize)
        total_inserted = 0

        reader = pd.read_csv(csv_path, chunksize=chunksize)

        for i, chunk in enumerate(reader):
            print(f"Processing chunk {i + 1} out of {total_chunks} : {chunk.shape[0]} rows")

            # Respect the limit if provided
            if limit is not None:
                remaining = limit - total_inserted
                if remaining <= 0:
                    break
                chunk = chunk.head(remaining)

            # Apply transformation pipeline
            chunk = transform_chunk(chunk)

            # Insert into MondgoDB
            books = chunk.to_dict(orient="records")
            if books:
                self._db.books.insert_many(books)
                total_inserted += len(books)

        if total_inserted > 0:
            self._setup_indexes()

        return total_inserted

    def _setup_indexes(self):
        """
        Set up indexes for the books collection to improve query performance.
        """
        # Multikey indexes 
        self._db.books.create_index("authors")
        self._db.books.create_index("genres")

        # Single key indexes
        self._db.books.create_index("publishYear")

        # Compound for common queries
        self._db.books.create_index([("authors", 1), ("publishYear", 1)])
        self._db.books.create_index([("genres", 1), ("publishYear", 1)])

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
        self, 
        author: str, 
        language: str | None = "English", 
        limit: int | None = None
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

    def get_top_rated_books_by_genre(
        self, 
        genre: str, 
        limit: int | None = None
    ) -> Cursor:
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

    def get_top_rated_books_by_year(
        self, 
        year: int, 
        limit: int | None = None
    ) -> Cursor:
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

        cursor = self._db.books.find({"publishYear": {"$eq": year}}).sort("rating", -1)

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

    def upsert_book(
        self, 
        book_id: str, 
        updates: dict, 
        upsert: bool = False
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

    def rate_book(
        self, 
        book_id: str, 
        stars: int
    ) -> dict | None:
        """
        Rates a book by its ID with a given number of stars (1 to 5).
        Updates the book's rating statistics, including:
            - ratingsByStars: List of counts for each star rating (5 to 1).
            - numRatings: Total number of ratings.
            - rating: Average rating, rounded to 2 decimal places.
            - likedPercent: Percentage of ratings that are 3 stars or higher.
        Args:
            book_id (str): The unique identifier of the book to rate.
            stars (int): The number of stars to rate the book (must be between 1 and 5).
        Returns:
            dict | None: The updated book data if the book exists, otherwise None.
        Raises:
            ValueError: If the stars value is not between 1 and 5.
        """
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
            sum((5 - i) * count for i, count in enumerate(ratingsByStars)) / numRatings,
            2,
        )

        # Calculate liked percentage
        likedPercent = int(round(sum(ratingsByStars[0:3]) / numRatings * 100, 0))

        # Update book ratings
        updated_data = {
            "ratingsByStars": ratingsByStars,
            "numRatings": numRatings,
            "rating": averageRating,
            "likedPercent": likedPercent,
        }
        return self.upsert_book(book_id, updated_data)

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
