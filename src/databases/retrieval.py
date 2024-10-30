from databases.connection import create_connection
import pandas as pd
from pymongo.errors import PyMongoError


def retrieve_data_as_dataframe(
    coll_name: str, database: str, query: dict = {}, projection: dict = None
) -> pd.DataFrame:
    """
    Retrieves data from a MongoDB collection and converts it into a pandas DataFrame.

    Args:
        coll_name (str): MongoDB collection name.
        database (str): MongoDB database name.
        query (dict, optional): MongoDB query to filter data. Defaults to {} (retrieve all).
        projection (dict, optional): Fields to include or exclude. Defaults to None (include all).

    Returns:
        pd.DataFrame: DataFrame containing the retrieved data.
    """
    try:
        coll = create_connection(database, coll_name)
        cursor = coll.find(query, projection)
        return pd.DataFrame(cursor)
    except PyMongoError as e:
        print(f"Failed to retrieve data from MongoDB. Error: {e}")
        raise


def convert_column_to_datetime(df: pd.DataFrame, column: str) -> pd.DataFrame:
    """
    Converts the specified column in the DataFrame from UNIX timestamp format to datetime.

    Args:
        df (pd.DataFrame): The DataFrame containing the column to convert.
        column (str): The name of the column to convert to datetime.

    Returns:
        pd.DataFrame: The DataFrame with the specified column converted to datetime.
    """
    try:
        df[column] = pd.to_datetime(
            df[column], unit="ms"
        )  # Convert UNIX timestamp to datetime
        return df
    except KeyError:
        print(f"Column {column} does not exist in the DataFrame.")
        return df
    except Exception as e:
        print(f"An error occurred while converting {column} to datetime: {e}")
        return df


def generate_dataframe_from_database(
    database: str, collection: str, date_column: str
) -> pd.DataFrame:
    """
    Retrieves data from a specified MongoDB collection, converts a given date column to datetime format,
    and returns a pandas DataFrame for further analysis.

    This function:
    1. Connects to MongoDB and retrieves data from the specified database and collection.
    2. Converts the specified date column from a UNIX timestamp format to datetime format for compatibility with pandas.
    3. Returns the resulting DataFrame for downstream processing or analysis.

    Args:
        database (str): The name of the MongoDB database.
        collection (str): The MongoDB collection name within the specified database.
        date_column (str): The column containing date values in UNIX timestamp format, which will be converted to datetime.

    Returns:
        pd.DataFrame: A pandas DataFrame containing the processed data from the specified MongoDB collection.

    Raises:
        Exception: Prints an error message if there is an issue with data retrieval or conversion.

    Example:
        >>> df = generate_dataframe_from_database("deep-diver", "boreport", "Probability 90% Date")
        >>> print(df.head())

    """
    try:
        # Retrieve the data from MongoDB
        df = retrieve_data_as_dataframe(database=database, coll_name=collection)

        # Convert the specified date column to datetime
        new_df = convert_column_to_datetime(df=df, column=date_column)

        return new_df
    except Exception as e:
        print(f"An error occurred in the main function: {e}")


# Run the main function if the script is executed
if __name__ == "__main__":
    database = "deep-diver"
    collection = "boreport"
    date_column = "Probability 90% Date"
    df = generate_dataframe_from_database(database, collection, date_column)
    df.info()
