import os
from datetime import datetime, timezone
import json

import pandas as pd
import pymongo.collection
from pymongo import MongoClient
from pymongo.errors import PyMongoError


def read_boreportfull(
    filepath: str = r"Z:\FUNNEL with PROBABILITY TRACKING.xlsx",
):
    return pd.read_excel(filepath, usecols="B:BI", skiprows=2)


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


def create_connection(
    database: str = "deep-diver", coll: str = "boreportfull"
) -> pymongo.collection.Collection:
    """Gets collection object from pyMongo

    Args:
        database (str, optional): _description_. Defaults to "deep-diver".
        coll (str, optional): _description_. Defaults to 'boreportfull'.

    Returns:
        pymongo.collection.Collection: _description_
    """
    try:
        client = MongoClient()
        db = client[database]
        return db[coll]
    except Exception as e:
        print(f"Failed to find specified collection. ErrorType: {e.__name__}.")


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


def create_df_with_aggregation(
    coll: pymongo.collection.Collection, pipeline: list
) -> pd.DataFrame:
    """Creates dataframe from MongoDB Collection.

    Args:
        coll (pymongo.collection.Collection): _description_
        pipeline (list): _description_

    Returns:
        pd.DataFrame: _description_
    """
    cursor = coll.aggregate(pipeline)
    return pd.DataFrame(cursor)


# Locate only New Sales and FTTH
def filter_df(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Filters dataframe by New Sales, and FTTH Product.

    Args:
        dataframe (pd.DataFrame): _description_

    Returns:
        pd.DataFrame: _description_
    """
    return (
        dataframe.loc[
            (
                (dataframe["Funnel Type"] == "New Sales")
                & (dataframe["Funnel Productname"] == "Time B.Band-FTTH")
            )
            & (dataframe["Funn Status"] != "Lost")
        ]
    )[
        [
            "Funnel Create Date",
            "Funnel SO No",
            "Email",
            "Mobile",
            "Package",
            "Channel",
            "Blk Name",
            "Bld Name",
            "Blk Cluster",
            "Blk State",
        ]
    ]


def clean_df(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Transforms Mobile Numbers into required `+60` format.

    Args:
        dataframe (pd.DataFrame): _description_

    Returns:
        pd.DataFrame: _description_
    """
    return (
        dataframe.dropna(subset="Funnel SO No")
        .dropna(subset="Email")
        .dropna(subset="Mobile")
        .assign(
            Mobile=dataframe.Mobile.str.replace("-", "")
            .str.replace(" ", "")
            .str.strip()
            .apply(
                lambda x: "'+6" + str(x)
                if str(x).startswith("0")
                else "'+" + str(x)
                if str(x).startswith("6")
                else "'" + str(x)
            )
        )
    )


# Getting Building LatLongs
def get_building_info(file_path: str = "Y:\\FTTH_Penetration2.xlsx") -> pd.DataFrame:
    """Gets the building list info and outputs as DataFrame.

    Args:
        file_path (_type_, optional): _description_. Defaults to "Y:\FTTH_Penetration2.xlsx".

    Returns:
        pd.DataFrame: _description_
    """
    return pd.read_excel(
        file_path, skiprows=[0], usecols="B:Q", sheet_name="FTTH Details"
    )


# Join the two tables
def merge_dfs(
    left_df: pd.DataFrame,
    right_df: pd.DataFrame,
    method: str = "left",
    on: str = "Bld Name",
) -> pd.DataFrame:
    """Merges the two dfs together.

    Args:
        left_df (pd.DataFrame): BO Report DataFrame
        right_df (pd.DataFrame): Building info DataFrame
        method (str, optional): _description_. Defaults to 'left'.
        on (str, optional): _description_. Defaults to 'Bld Name'.

    Returns:
        pd.DataFrame: _description_
    """
    return pd.merge(left_df, right_df, "left", "Bld Name")


def export_merged_df_to_csv(
    dataframe: pd.DataFrame,
    output_folder: str = "C:\\Users\\izzaz\\Desktop",
    file_name: str = "customer_info",
) -> None:
    """Outputs as csv to default folder (desktop)

    Args:
        dataframe (pd.DataFrame): _description_
        output_folder (_type_, optional): _description_. Defaults to "C:\\Users\\izzaz\\Desktop".
        file_name (str, optional): _description_. Defaults to "customer_info".
    """
    time = datetime.today().strftime(format="%Y%m%d")
    fullpath: str = os.path.join(output_folder, file_name) + "_" + time
    dataframe.to_csv(f"{fullpath}.csv")
    print(f"File output to {fullpath}.csv success!")


def extract_customer_info():
    """Main Function."""
    pipeline: list[dict] = [
        {"$set": {"Funnel Create Date": {"$toDate": "$Funnel Create Date"}}},
        {
            "$match": {
                "Funnel Create Date": {
                    "$gt": datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
                }
            }
        },
    ]
    coll: pymongo.collection.Collection = create_connection()
    df: pd.DataFrame = create_df_with_aggregation(coll, pipeline)
    bldg_df: pd.DataFrame = get_building_info()
    filtered_df: pd.DataFrame = filter_df(df)
    cleaned_df: pd.DataFrame = clean_df(filtered_df)
    merged_df: pd.DataFrame = merge_dfs(cleaned_df, bldg_df)
    export_merged_df_to_csv(merged_df)


def main():
    extract_customer_info()


if __name__ == "__main__":
    df = read_boreportfull()
    df.columns = df.iloc[0]
    df = df[1:].reset_index(drop=True)
    json_data = transform_to_json(df)
    coll = create_connection()
    delete_from_coll(coll)
    insert_to_coll(json_data, coll)
    extract_customer_info()
