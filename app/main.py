import os
from datetime import datetime
from app.library_catalog import MongoDbLibraryCatalog


def main():
    try:
        db_host = os.getenv("MONGO_DB_HOST", "localhost")
        db_port = int(os.getenv("MONGO_DB_PORT", "27017"))
        db_name = os.getenv("MONGO_DB_NAME", "becode")
        csv_path = os.getenv("CSV_DATASET_PATH", "data/books.csv")

        print("Setting up catalog...")

        with MongoDbLibraryCatalog(host=db_host, port=db_port, db_name=db_name) as catalog:
            # If catalog is empty, populate it with books from CSV dataset
            if catalog.is_empty():
                print(f"Populating catalog with books from CSV file: {csv_path} ...")
                books_count = catalog.populate_from_csv(csv_path, limit=100)
                if books_count:
                    print(f"Catalog populated with {books_count} books.")
                else:
                    print("No books found for populating the catalog.")

            print("Catalog setup complete.\n")

            # Find books by a specific author
            author = "J.K. Rowling"
            books_by_author = catalog.get_books_by_author(author)
            print(f"Books by {author}:")
            for book in books_by_author:
                print(f"- '{book['title']}' published on {book['publishDate'].year}")

            # Find top 10 rated books in a given genre
            genre = "Fantasy"
            top_rated_books_by_genre = catalog.get_rated_books_by_genre(genre, 10)
            print(f"\nTop 10 rated books in the {genre} genre:")
            for book in top_rated_books_by_genre:
                print(f"- '{book['title']}' by {book['authors']} : {book['rating']}")

            # Find top 10 rated books published on an specific year
            year = 2000
            top_rated_books_by_year = catalog.get_rated_books_by_year(year, 10)
            print(f"\nTop 10 rated books published on {year}:")
            for book in top_rated_books_by_year:
                print(f"- '{book['title']}' by {book['authors']} : {book['rating']}")

            # Top 10 genres by number of books
            print("\nTop 10 genres by number of books:")
            genre_counts = catalog.get_genre_counts(10)
            for genre in genre_counts:
                print(f"- {genre['_id']}: {genre['count']}")

            # Top 10 authors by number of books
            print("\nTop 10 authors by number of books:")
            author_counts = catalog.get_author_counts(10)
            for author in author_counts:
                print(f"- {author['_id']}: {author['count']}")

            # Inserting a new book
            new_book_data = {
                "title": "A Book",
                "authors": ["An Author"],
                "publishDate": datetime(2025, 8, 26),
                "genres": ["Fantasy"],
            }
            book_id = catalog.insert_book(new_book_data)
            print("\nInserted book with ID:", book_id)

            new_book = catalog.get_book({"_id": book_id})
            if new_book:
                print(f"\nRetrieved book with ID {book_id}: {new_book}")
            else:
                print(f"\nBook not found with ID: {book_id}")

            # Deleting newly created book
            catalog.delete_book(book_id)
            print(f"\nDeleted book with ID: {book_id}")

            # Add 5-star rating to existing book
            book = catalog.get_book({"title": "The Da Vinci Code"})
            if book:
                print(f"\nFound book: {book['title']}")
                print(f"Ratings by stars (5, 4, 3, 2, 1): {[0, 0, 0, 0, 0] if book.get('ratingsByStars') is None else book['ratingsByStars']}")
                print(f"Number of ratings {0 if not book.get('numRatings') else book['numRatings']}")
                print(f"Average rating: {0 if not book.get('rating') else book['rating']}")
                print(f"Liked percentage: {0 if not book.get('likedPercent') else book['likedPercent']}")

                updated_book = catalog.rate_book(book["_id"], 5)
                if updated_book:
                    print(f"\nUpdated book: {updated_book['title']}")
                    print(f"Ratings by stars (5, 4, 3, 2, 1): {updated_book['ratingsByStars']}")
                    print(f"Number of ratings: {updated_book['numRatings']}")
                    print(f"Average rating: {updated_book['rating']}")
                    print(f"Liked percentage: {updated_book['likedPercent']}")

    except Exception as e:
        print(f"Error occurred: {e}")


if __name__ == "__main__":
    main()
