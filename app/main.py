from app.db import setup_database


def main():
    try:
        db = setup_database(100)

    except Exception as e:
        print(f"Error occurred: {e}")


if __name__ == "__main__":
    main()
