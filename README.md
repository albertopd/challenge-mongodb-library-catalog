# Challenge: MongoDB Library Catalog

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE) [![Python](https://img.shields.io/badge/Python-3.13-blue.svg)](https://www.python.org/) [![MongoDB](https://img.shields.io/badge/MongoDB-8.0-green.svg)](https://www.mongodb.com/)

This project is a MongoDB-powered library catalog system built with Python. It allows you to import, store, and query a large dataset of books, supporting advanced search and analytics features such as top-rated books, genre and author statistics, and dynamic updates.

## ✨ Features

- Import books from CSV files into MongoDB in efficient chunks.
- Query books by author, genre, or publication year.
- Retrieve top-rated books by genre or year.
- Get statistics for genres and authors (top 10, etc.).
- Add, update, and delete book records.
- Calculate ratings and liked percentages dynamically.
- Context manager support for safe database connections.

## 📂 Project Structure

```
challenge-mongodb-library-catalog/
├── app/
│   ├── library_catalog.py  # Main catalog logic
│   ├── main.py             # CLI entry point
│   ├── utils.py            # Data parsing helpers
├── data/
│   ├── books.csv           # Example dataset
│   └── README.md
├── requirements.txt        # Python dependencies
├── LICENSE
└── README.md
```

## 📋 Requirements

- Python 3.13 or later
- MongoDB server (local or remote)
- Python packages listed in [requirements.txt](requirements.txt)

## 📦 Installation

1. Clone this repository:
	```sh
	git clone https://github.com/albertopd/challenge-mongodb-library-catalog.git
	cd challenge-mongodb-library-catalog
	```
2. Install dependencies:
	```sh
	pip install -r requirements.txt
	```
3. Ensure MongoDB is running and accessible (default: localhost:27017).

## ⚙️ Configuration

Configuration is managed via the `.env` file. Example:

```env
MONGO_DB_HOST=localhost
MONGO_DB_PORT=27017
MONGO_DB_NAME=library_catalog
CSV_DATASET_PATH=data/books.csv
```

Adjust these values to match your environment.

## 🚀 Usage

1. Place your CSV dataset in the `data/` folder (default: `data/books.csv`).
2. Run the main application:
	```sh
	python -m app.main
	```
3. The catalog will be populated and you can interact with queries and updates as shown in the CLI output.

## 📝 Example Output

```sh
Setting up catalog...
Populating catalog with books from CSV file: data/books.csv ...
Processing chunk 1 out of 13 : 5000 rows
Processing chunk 2 out of 13 : 5000 rows
Processing chunk 3 out of 13 : 5000 rows
Processing chunk 4 out of 13 : 5000 rows
Processing chunk 5 out of 13 : 5000 rows
Processing chunk 6 out of 13 : 5000 rows
Processing chunk 7 out of 13 : 5000 rows
Processing chunk 8 out of 13 : 5000 rows
Processing chunk 9 out of 13 : 5000 rows
Processing chunk 10 out of 13 : 5000 rows
Processing chunk 11 out of 13 : 2478 rows
Catalog populated with 52478 books.
Catalog setup complete.

Books by J.K. Rowling:
- "Harry Potter and the Prisoner of Azkaban" published on 2004
- "Harry Potter and the Chamber of Secrets" published on 1999
- "Harry Potter and the Deathly Hallows" published on 2007
- "Harry Potter and the Half-Blood Prince" published on 2006
- "Harry Potter and the Goblet of Fire" published on 2002
- "Harry Potter and the Order of the Phoenix" published on 2004
- "Harry Potter Series Box Set" published on 2007
- "Harry Potter and the Sorcerer's Stone" published on 2003
- "The Tales of Beedle the Bard" published on 2008
- "The Harry Potter trilogy" published on 1999
- "Harry Potter and the Order of the Phoenix (Harry Potter, #5, Part 1)" published on 2003
- "Very Good Lives: The Fringe Benefits of Failure and the Importance of Imagination" published on 2015
- "Harry Potter and the Cursed Child: Parts One and Two" published on 2016
- "Harry Potter: The Prequel" published on 2008
- "Quidditch Through the Ages" published on 2001
- "Fantastic Beasts and Where to Find Them" published on 2001
- "Fantastic Beasts - The Crimes of Grindelwald: The Original Screenplay" published on 2018
- "Fantastic Beasts and Where to Find Them: The Original Screenplay" published on 2016
- "The Harry Potter Collection 1-4" published on 1999
- "Harry Potter Schoolbooks Box Set: Two Classic Books from the Library of Hogwarts School of Witchcraft and Wizardry" published on 2001
- "The Hogwarts Library" published on 2012
- "Harry Potter: A History of Magic" published on 2017
- "Harry Potter Collection" published on 2005
- "Harry Potter Boxed Set, Books 1-5 (Harry Potter, #1-5)" published on 2004
- "The Casual Vacancy" published on 2012
- "Hogwarts: An Incomplete and Unreliable Guide" published on 2016
- "Short Stories from Hogwarts of Heroism, Hardship and Dangerous Hobbies" published on 2016
- "Short Stories from Hogwarts of Power, Politics and Pesky Poltergeists" published on 2016

Top 10 rated books in the Fantasy genre:
- "Bertie's Book of Spooky Wonders" by Ocelot Emerson : 5.0
- "Kiss Me, I'm Irish" by John Blandly : 5.0
- "Battle for Erthia" by Laurie Forest : 5.0
- "Maze of Existence" by Tina M. Randolph : 5.0
- "16 Myths" by Aim Ruivivar : 5.0
- "The King Tingaling Painting" by Elias Zapple : 4.95
- "Orion: The Fight for Vox" by Ruth Watson-Morris : 4.93
- "The Present" by Kenneth Thomas : 4.92
- "Elfquest: The Original Quest Gallery Edition" by Wendy Pini (Illustrations), Richard Pini : 4.9
- "Jellybean the Dragon" by Elias Zapple : 4.89

Top 10 rated books published on 2000:
- "The Bridge to Eternal Life" by Jozef Rulof : 5.0
- "The Song of Ribhu: Translated from the Original Tamil Version of the Ribhu Gita" by H. Ramamoorthy : 4.84
- "حصن المسلم: من أذكار الكتاب والسنة" by سعيد بن علي بن وهف القحطاني : 4.7
- "The Sibley Guide to Birds" by David Allen Sibley, National Audubon Society : 4.67
- "The Battlefield Where the Moon Says I Love You" by Frank Stanford : 4.67
- "Knowing How to Know: A Practical Philosophy in the Sufi Tradition" by Idries Shah : 4.67
- "Weed / Yoshihiro Takahashi" by Yoshihiro Takahashi : 4.67
- "Naked" by Mike Leigh : 4.65
- "Steps to Christ" by Ellen G. White : 4.65
- "Tehlikeli Oyunlar" by Oğuz Atay : 4.61

Top 10 genres by number of books:
- Fiction: 31638
- Romance: 15495
- Fantasy: 15046
- Young Adult: 11869
- Contemporary: 10520
- Nonfiction: 8251
- Adult: 8246
- Novels: 7805
- Mystery: 7702
- Historical Fiction: 7665

Top 10 authors by number of books:
- Stephen King: 104
- Nora Roberts: 103
- James Patterson: 102
- Agatha Christie: 88
- Anonymous: 79
- Erin Hunter: 74
- Terry Pratchett: 67
- Meg Cabot: 65
- Mercedes Lackey: 62
- J.D. Robb: 59

Inserted book with ID: 68aed2dd6f8e01c794c5e017

Retrieved book with ID 68aed2dd6f8e01c794c5e017: {'_id': ObjectId('68aed2dd6f8e01c794c5e017'), 'title': 'A Book', 'authors': ['An Author'], 'publishYear': 2025, 'genres': ['Fantasy']}

Deleted book with ID: 68aed2dd6f8e01c794c5e017

Found book: The Da Vinci Code
- Total ratings: 1933446
- Ratings by stars:
  ***** : 645308
  ****  : 667657
  ***   : 399278
  **    : 142103
  *     : 79100
- Average rating: 3.86
- Liked percentage: 89%

Updated book ratings after adding a 5-star rating:
- Total ratings: 1933447
- Ratings by stars:
  ***** : 645309
  ****  : 667657
  ***   : 399278
  **    : 142103
  *     : 79100
- Average rating: 3.86
- Liked percentage: 89%
```

## 📜 License

This project is licensed under the [MIT License](LICENSE).

## 👤 Author

[Alberto Pérez Dávila](https://github.com/albertopd)