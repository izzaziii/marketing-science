import json
import os
from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd
from databases.connection import transform_to_json

# Global configuration for paths
CONFIG = {
    "bo_reports": {
        "ftth": r"Z:\FUNNEL with PROBABILITY TRACKING_Teefa.xlsx",
        "ftto": r"Z:\FUNNEL with PROBABILITY TRACKING_Teefa (FTTO).xlsx",
    },
    "output_folder": r"C:\Users\izzaz\Documents\2 Areas\GitHub\marketing-science\data\raw",
}


def read_boreport(report_type: str) -> pd.DataFrame:
    """
    Reads the BO Report Excel file for the specified report type (FTTH or FTTO).

    Parameters:
        report_type (str): Type of report to read. Accepts "ftth" or "ftto".

    Returns:
        pd.DataFrame: DataFrame containing the data from the specified BO Report Excel file.

    Raises:
        ValueError: If an invalid report type is provided.
    """
    try:
        file_path = CONFIG["bo_reports"][report_type.lower()]
        df = pd.read_excel(file_path)
        print(f"Successfully loaded {report_type.upper()} report.")
        return df
    except KeyError:
        raise ValueError("Invalid report type. Please choose 'ftth' or 'ftto'.")
    except Exception as e:
        print(f"Error reading the Excel file: {e}")
        return pd.DataFrame()


def inspect_dataframe(df: pd.DataFrame) -> None:
    """
    Inspects the DataFrame by printing its information and the first few rows.

    Parameters:
        df (pd.DataFrame): The DataFrame to inspect.
    """
    print("DataFrame Info:")
    df.info()
    print("\nDataFrame Head:")
    print(df.head())


def prompt_inspect_dataframe(df: pd.DataFrame) -> None:
    """
    Asks the user if they want to inspect the DataFrame and runs inspect_dataframe if yes.

    Parameters:
        df (pd.DataFrame): The DataFrame to potentially inspect.
    """
    choice = (
        input("Would you like to inspect the DataFrame? (yes/no): ").strip().lower()
    )
    if choice == "yes":
        inspect_dataframe(df)
    else:
        print("Skipping DataFrame inspection.")


def print_dataframe(records: List[Dict[str, Any]]) -> None:
    """
    Prints a snapshot of the DataFrame created from the records.
    """
    df = pd.DataFrame(records)
    if df.empty:
        print("DataFrame is empty!")
    else:
        print(df.head(50))


def export_to_csv(
    records: List[Dict[str, Any]], folder_path: str = CONFIG["output_folder"]
) -> None:
    """
    Exports records to a CSV file with a user-defined filename.

    Args:
        records (List[Dict[str, Any]]): Data to export to CSV.
        folder_path (str): Directory where the CSV will be saved. Defaults to CONFIG output path.
    """
    try:
        df = pd.DataFrame(records)
        if not df.empty:
            if not os.path.exists(folder_path):
                os.makedirs(folder_path)
            time = datetime.today().strftime("%Y%m%d")
            filename = input("\nEnter filename for export: \n")
            full_filename = f"{filename}_{time}.csv"
            fullpath = os.path.join(folder_path, full_filename)
            df.to_csv(fullpath, index=False)
            print(f"Exported data to path: {fullpath}")
    except Exception as e:
        print(f"Unable to export to CSV. Error details: {type(e).__name__}")


def export_to_json(
    data: List[Dict[str, Any]], folder_path: str = CONFIG["output_folder"]
) -> Optional[str]:
    """
    Exports insights data to a JSON file with a timestamped filename.

    Args:
        data (List[Dict[str, Any]]): Data to be exported as JSON.
        folder_path (str): Directory where the JSON file will be saved. Defaults to CONFIG output path.

    Returns:
        Optional[str]: File path of the saved JSON file if successful, otherwise None.

    Raises:
        IOError: If there is an error writing the file.
    """
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    filename = f"{timestamp}_boreport.json"
    full_path = os.path.join(folder_path, filename)

    try:
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)
        with open(full_path, "w") as f:
            json.dump(data, f, indent=4)
        print(f"Data exported to {full_path}")
        return full_path
    except IOError as e:
        print(f"Failed to write to JSON file at {full_path}. Error: {e}")
        return None


def handle_option_selection(selection: str, records: List[Dict[str, Any]]) -> None:
    """
    Processes the user's selection for handling data export and display.

    Parameters:
        selection (str): User-selected option.
        records (List[Dict[str, Any]]): List of data records for export or display.
    """
    options = {
        "1": lambda: [print(json.dumps(record, indent=4)) for record in records],
        "2": lambda: print_dataframe(records),
        "3": lambda: export_to_csv(records),
        "4": lambda: export_to_json(records),
    }

    if selection in options:
        options[selection]()
    elif selection == "5":
        print("Exiting Menu.")
    else:
        print("Invalid choice. Please select an option between 1-5.")


def process_response(records: List[Dict[str, Any]]) -> None:
    """
    Prompts user to select an option for processing data and calls the appropriate function.
    """
    while True:
        selection = input(
            f"Row Count: {len(records)}.\nSelect an option to process data:\n"
            "1. Print JSON\n2. Print DataFrame (data might be nested)\n3. Export to CSV (data might be nested)\n4. Export to JSON\n"
            "5. Quit Menu\n"
        )
        if selection == "5":
            print("Exiting Menu.")
            break
        handle_option_selection(selection, records)


if __name__ == "__main__":
    report_type = input("Enter report type (ftth/ftto): ").strip().lower()
    try:
        df = read_boreport(report_type)
        if not df.empty:
            prompt_inspect_dataframe(df)
            records = transform_to_json(df)
            process_response(records)
    except ValueError as e:
        print(e)
