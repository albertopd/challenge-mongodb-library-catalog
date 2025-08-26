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
MONGO_DB_NAME=becode
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

## 📜 License

This project is licensed under the [MIT License](LICENSE).

## 👤 Author

[Alberto Pérez Dávila](https://github.com/albertopd)