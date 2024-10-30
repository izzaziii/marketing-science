import pandas as pd
import pymongo.results
import pymongo.collection
import pymongo.cursor
import json
from pymongo.errors import PyMongoError
import os
from datasets.boreport import read_boreport


def create_connection(database: str, coll: str) -> pymongo.collection.Collection:
    """
    Establishes a connection to a MongoDB collection within a specified database.

    Args:
        database (str): Name of the MongoDB database.
        coll (str): Name of the collection or view in the MongoDB database.

    Returns:
        pymongo.collection.Collection: A pymongo Collection object to interact with the database.

    Raises:
        PyMongoError: If the connection to the MongoDB database or collection fails.
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
        None. Prints the deletion status and number of deleted records.

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
        None. Prints the insertion status and number of records inserted.

    Raises:
        PyMongoError: If the insertion operation fails.
    """
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


def import_json_data_to_mongodb() -> None:
    """
    Lists JSON files in a specified directory, previews data, and imports the chosen file to MongoDB.

    Steps:
    1. Lists files in the raw data directory and prompts user to select one.
    2. Checks if the file is a JSON file.
    3. Loads and previews the first 5 rows for user confirmation.
    4. Asks for the MongoDB database and collection name.
    5. Inserts data into MongoDB using insert_to_coll.

    Raises:
        FileNotFoundError: If no JSON files are found in the directory.
        PyMongoError: For MongoDB-related errors.
    """
    # Step 1: List available JSON files
    raw_data_dir = (
        r"C:\\Users\\izzaz\\Documents\\2 Areas\\GitHub\\marketing-science\\data\\raw"
    )
    files = [f for f in os.listdir(raw_data_dir) if f.endswith(".json")]

    if not files:
        raise FileNotFoundError("No JSON files found in the specified directory.")

    print("Available JSON files:")
    for i, file_name in enumerate(files, start=1):
        print(f"{i}. {file_name}")

    file_choice = int(input("Choose a file number: ")) - 1
    chosen_file = os.path.join(raw_data_dir, files[file_choice])

    # Step 2: Confirm JSON format
    if not chosen_file.endswith(".json"):
        print("The chosen file is not a JSON file.")
        return

    # Step 3: Load and preview data
    with open(chosen_file, "r") as f:
        data = json.load(f)

    df_preview = pd.DataFrame(data)
    print("\nPreview of the data:")
    print(df_preview.head())

    # Step 4: Get database and collection names from the user
    database_name = input("Enter MongoDB database name: ")
    collection_name = input("Enter MongoDB collection name: ")

    # Step 5: Insert data into MongoDB
    try:
        coll = create_connection(database_name, collection_name)
        insert_to_coll(data, coll)
        print(
            f"Data from {files[file_choice]} successfully imported to MongoDB collection '{collection_name}' in database '{database_name}'."
        )
    except PyMongoError as e:
        print(f"Failed to insert data into MongoDB. Error: {e}")


def import_boreport_to_mongodb(
    report_type: str, database: str, collection: str
) -> None:
    """
    Main function to insert sales data from an Excel file into a MongoDB collection.

    Steps:
    1. Reads the Excel file into a pandas DataFrame.
    2. Transforms the DataFrame into JSON format (list of dictionaries).
    3. Establishes a connection to MongoDB.
    4. Deletes any existing records in the collection.
    5. Inserts the transformed data into the MongoDB collection.

    Args:
        file_path (str): The file path of the Excel file containing sales data.
        database (str): The MongoDB database name.
        collection (str): The MongoDB collection name.

    Returns:
        None. Performs deletion and insertion operations and prints the result.
    """
    try:
        # Step 1: Read the Excel file
        df = read_boreport(report_type=report_type)

        # Step 2: Transform to JSON format
        json_data = transform_to_json(df)

        # Step 3: Create MongoDB connection
        coll = create_connection(database, collection)

        # Step 4: Delete existing records from collection
        delete_from_coll(coll)

        # Step 5: Insert new data into collection
        insert_to_coll(json_data, coll)

    except Exception as e:
        print(f"An error occurred during the process: {e}")


# Example usage
if __name__ == "__main__":
    import_json_data_to_mongodb()
