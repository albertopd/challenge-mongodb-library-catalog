
from app.db import (
    setup_database,
    get_books_by_author, 
    get_top_rated_books_by_genre, 
    get_top_rated_books_by_year
)


def main():
    try:
        db = setup_database()

        # Find books by a specific author
        author = "J.K. Rowling"
        print(f"Books by {author}:")
        for book in get_books_by_author(db, author):
            print(f"- '{book['title']}' published on {book['publishDate'].year}")

        # Find top 10 rated books in a given genre
        genre = "Fantasy"
        print(f"\nTop 10 rated books in the {genre} genre:")
        for book in get_top_rated_books_by_genre(db, genre, 10):
            print(f"- '{book['title']}' by {book['authors']} : {book['rating']}")

        # Find top 10 rated books published on an specific year
        year = 2000
        print(f"\nTop 10 rated books published on {year}:")
        for book in get_top_rated_books_by_year(db, year, 10):
            print(f"- '{book['title']}' by {book['authors']} : {book['rating']}")

    except Exception as e:
        print(f"Error occurred: {e}")


if __name__ == "__main__":
    main()
