import pandas as pd
import pymongo.results
import pymongo.collection
import pymongo.cursor
import json
from pymongo.errors import PyMongoError
import os


def create_connection(database: str, coll: str) -> pymongo.collection.Collection:
    """
    Establishes a connection to a MongoDB collection within a specified database.

    Args:
        database (str): The name of the MongoDB database to connect to.
        coll (str): The name of the collection within the database.

    Returns:
        pymongo.collection.Collection: A collection object representing the MongoDB collection.

    Raises:
        PyMongoError: If there is an error connecting to MongoDB.
    """
    try:
        client = pymongo.MongoClient()
        db = client[database]
        return db[coll]
    except PyMongoError as e:
        print(f"Failed to connect to MongoDB. Error: {e}")
        raise


def transform_to_json(dataframe: pd.DataFrame) -> list[dict]:
    """
    Converts a pandas DataFrame into a list of dictionaries, formatted as JSON for MongoDB insertion.

    Args:
        dataframe (pd.DataFrame): The DataFrame to convert.

    Returns:
        list[dict]: A list of records in dictionary format, ready for MongoDB insertion.

    Raises:
        ValueError: If the DataFrame is empty.
    """
    if dataframe.empty:
        raise ValueError("The DataFrame is empty. Cannot transform to JSON.")

    result = dataframe.to_json(orient="records")
    return json.loads(result)


def delete_from_coll(coll: pymongo.collection.Collection) -> None:
    """
    Deletes all documents from the specified MongoDB collection.

    Args:
        coll (pymongo.collection.Collection): The collection from which to delete documents.

    Returns:
        None

    Raises:
        PyMongoError: If the deletion operation fails.
    """
    try:
        operation: pymongo.results.DeleteResult = coll.delete_many({})
        acknowledgement: bool = operation.acknowledged
        deleted_records: int = operation.deleted_count
        print(f"Delete status: {acknowledgement}. Records deleted: {deleted_records}")
    except PyMongoError as e:
        print(f"Failed to delete records from MongoDB collection. Error: {e}")
        raise


def insert_to_coll(data: list, coll: pymongo.collection.Collection) -> None:
    """
    Inserts a list of dictionaries (records) into the specified MongoDB collection.

    Args:
        data (list): A list of dictionaries representing the data to be inserted.
        coll (pymongo.collection.Collection): The MongoDB collection to insert data into.

    Returns:
        None

    Raises:
        ValueError: If the data list is empty.
        PyMongoError: If the insertion operation fails.
    """
    if not data:
        raise ValueError(
            "The data list is empty. Cannot insert empty data into MongoDB."
        )

    try:
        operation: pymongo.results.InsertManyResult = coll.insert_many(data)
        acknowledgement: bool = operation.acknowledged
        inserted_records: int = len(operation.inserted_ids)
        print(
            f"Inserted status: {acknowledgement}. Records inserted into {coll.name}: {inserted_records}"
        )
    except PyMongoError as e:
        print(f"Failed to insert data into MongoDB collection. Error: {e}")
        raise


def list_json_files(directory: str) -> list[str]:
    """
    Lists all JSON files in the specified directory.

    Args:
        directory (str): The directory path to search for JSON files.

    Returns:
        list[str]: A list of JSON file names found in the directory.

    Raises:
        FileNotFoundError: If the directory does not exist.
    """
    if not os.path.exists(directory):
        raise FileNotFoundError(f"The directory {directory} does not exist.")

    files = [f for f in os.listdir(directory) if f.endswith(".json")]
    return files


def choose_file(files: list[str]) -> str:
    """
    Prompts the user to select a file from the list of available JSON files.

    Args:
        files (list[str]): A list of JSON file names to choose from.

    Returns:
        str: The name of the chosen file.

    Raises:
        IndexError: If the user selects an invalid file number.
        ValueError: If the input is not a valid integer.
    """
    print("Available JSON files:")
    for i, file_name in enumerate(files, start=1):
        print(f"{i}. {file_name}")
    try:
        file_choice = int(input("Choose a file number: ")) - 1
        if file_choice < 0 or file_choice >= len(files):
            raise IndexError("Invalid file number selected.")
        return files[file_choice]
    except ValueError:
        raise ValueError(
            "Invalid input. Please enter a number corresponding to the file."
        )
    except IndexError as e:
        raise IndexError(f"{e}")


def load_json_file(filepath: str) -> list[dict]:
    """
    Loads JSON data from the specified file path.

    Args:
        filepath (str): The full path to the JSON file.

    Returns:
        list[dict]: The JSON data loaded from the file.

    Raises:
        FileNotFoundError: If the file does not exist.
        json.JSONDecodeError: If the file contains invalid JSON.
    """
    if not os.path.isfile(filepath):
        raise FileNotFoundError(f"The file {filepath} does not exist.")
    try:
        with open(filepath, "r") as f:
            data = json.load(f)
        return data
    except json.JSONDecodeError as e:
        raise json.JSONDecodeError(
            f"Error decoding JSON from file {filepath}: {e}", filepath, e.pos
        )


def preview_data(data: list[dict], num_rows: int = 5) -> None:
    """
    Displays a preview of the data by showing the first few rows.

    Args:
        data (list[dict]): The data to preview.
        num_rows (int, optional): The number of rows to display. Defaults to 5.

    Returns:
        None

    Raises:
        ValueError: If the data list is empty.
    """
    if not data:
        raise ValueError("Data is empty. Cannot preview empty data.")
    df_preview = pd.DataFrame(data)
    print("\nPreview of the data:")
    print(df_preview.head(num_rows))


def get_database_and_collection_names() -> tuple[str, str]:
    """
    Prompts the user to input the database and collection names.

    Returns:
        tuple[str, str]: A tuple containing the database name and collection name.

    Raises:
        ValueError: If the user inputs empty strings.
    """
    database_name = input("Enter MongoDB database name: ").strip()
    collection_name = input("Enter MongoDB collection name: ").strip()
    if not database_name:
        raise ValueError("Database name cannot be empty.")
    if not collection_name:
        raise ValueError("Collection name cannot be empty.")
    return database_name, collection_name


def prompt_delete_collection() -> bool:
    """
    Asks the user whether to delete all documents in the collection.

    Returns:
        bool: True if the user wants to delete the collection, False otherwise.
    """
    delete_option = (
        input(
            "Do you want to delete all documents in the collection before inserting new data? (yes/no): "
        )
        .strip()
        .lower()
    )
    return delete_option == "yes"


def import_json_data_to_mongodb() -> None:
    """
    Orchestrates the import of JSON data into MongoDB.

    Steps:
        1. Lists JSON files in the specified directory.
        2. Prompts the user to select a file.
        3. Loads and previews the data.
        4. Gets the database and collection names.
        5. Connects to the MongoDB collection.
        6. Prompts to delete existing documents in the collection.
        7. Inserts data into the MongoDB collection.

    Raises:
        FileNotFoundError: If the directory or selected file does not exist.
        ValueError: For invalid user inputs.
        PyMongoError: For MongoDB-related errors.
        json.JSONDecodeError: If the JSON file contains invalid JSON.
    """
    raw_data_dir = (
        r"C:\\Users\\izzaz\\Documents\\2 Areas\\GitHub\\marketing-science\\data\\raw"
    )

    try:
        # Step 1: List available JSON files
        files = list_json_files(raw_data_dir)
        if not files:
            raise FileNotFoundError("No JSON files found in the specified directory.")

        # Step 2: Choose a file
        chosen_file = choose_file(files)
        chosen_file_path = os.path.join(raw_data_dir, chosen_file)

        # Step 3: Load and preview data
        data = load_json_file(chosen_file_path)
        preview_data(data)

        # Step 4: Get database and collection names
        database_name, collection_name = get_database_and_collection_names()

        # Step 5: Connect to the collection
        coll = create_connection(database_name, collection_name)

        # Step 6: Ask about deletion
        if prompt_delete_collection():
            delete_from_coll(coll)
            print(f"All documents in collection '{collection_name}' have been deleted.")
        else:
            print("Proceeding without deleting existing documents.")

        # Step 7: Insert data into MongoDB
        insert_to_coll(data, coll)
        print(
            f"Data from {chosen_file} successfully imported to MongoDB collection '{collection_name}' in database '{database_name}'."
        )
    except (FileNotFoundError, ValueError, PyMongoError, json.JSONDecodeError) as e:
        print(f"An error occurred: {e}")
    except Exception as e:
        # Catch-all for any other exceptions
        print(f"An unexpected error occurred: {e}")
        raise


if __name__ == "__main__":
    import_json_data_to_mongodb()
