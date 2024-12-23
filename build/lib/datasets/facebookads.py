import os
import json
import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.api import FacebookAdsApi
from facebook_business.exceptions import FacebookRequestError


def get_configs(
    path_to_config_file: str = "C:\\Users\\izzaz\\Documents\\2 Areas\\GitHub\\marketing-science\\src\\auth\\config\\meta_secrets.json",
) -> Optional[Dict[str, Any]]:
    """
    Loads configuration data from a JSON file.
    """
    if not path_to_config_file.endswith(".json"):
        print("Error: The configuration file must be a JSON file.")
        return None

    try:
        with open(path_to_config_file, "r") as file:
            return json.load(file)
    except FileNotFoundError:
        print(
            f"Path '{path_to_config_file}' not found. Error: {FileNotFoundError.__name__}"
        )
    except json.JSONDecodeError:
        print("Error: The file is not valid JSON.")


def initialize_api(access_token: str) -> None:
    """
    Initializes the Facebook Ads API with a given access token.
    """
    FacebookAdsApi.init(access_token=access_token)


def validate_date_format(date_str: str) -> bool:
    """
    Validates if a date string is in the 'yyyy-mm-dd' format.
    """
    return bool(re.match(r"^\d{4}-\d{2}-\d{2}$", date_str))


def validate_dates(start_date: str, end_date: str) -> bool:
    """
    Validates that start_date is before end_date and both dates are in 'yyyy-mm-dd' format.
    """
    if not (validate_date_format(start_date) and validate_date_format(end_date)):
        raise ValueError("Dates must be in 'yyyy-mm-dd' format.")
    if datetime.strptime(start_date, "%Y-%m-%d") >= datetime.strptime(
        end_date, "%Y-%m-%d"
    ):
        raise ValueError("End date must be after start date.")
    return True


def fetch_insights(
    account_id: str, fields: List[str], start_date: str, end_date: str
) -> Optional[List[Dict[str, Any]]]:
    """
    Fetches insights data from a specified Facebook Ads account within a given date range.
    """
    validate_dates(start_date, end_date)
    params = {
        "time_range": {"since": start_date, "until": end_date},
        "level": "ad",
        "time_increment": 1,
    }
    try:
        account = AdAccount(account_id)
        insights = account.get_insights(fields=fields, params=params)
        return [insight.export_all_data() for insight in insights]
    except FacebookRequestError as e:
        print(
            f"FacebookRequestError: {e.api_error_message()} (Code: {e.api_error_code()})"
        )
        return None


def export_to_json(data: List[Dict[str, Any]], start_date: str, end_date: str) -> None:
    """
    Exports insights data to a JSON file with a timestamped filename.
    """
    try:
        timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        filename = f"C:\\Users\\izzaz\\Documents\\2 Areas\\GitHub\\marketing-science\\data\\raw\\{timestamp}_facebookads_{start_date}_{end_date}.json"
        with open(filename, "w") as f:
            json.dump(data, f, indent=4)
        print(f"Data exported to {filename}")
    except IOError as e:
        print(f"Failed to write to JSON file. Error: {e}")


def export_to_csv(
    records: List[Dict[str, Any]], folder_path: str = "C:\\Users\\izzaz\\Desktop"
) -> None:
    """
    Exports records to a CSV file with a user-defined filename.
    """
    try:
        df = pd.DataFrame(records)
        if not df.empty:
            time = datetime.today().strftime("%Y%m%d")
            filename = input("\nEnter filename for export: \n")
            full_filename = f"{filename}_{time}.csv"
            fullpath = os.path.join(folder_path, full_filename)
            df.to_csv(fullpath, index=False)
            print(f"Exported data to path: {fullpath}")
    except Exception as e:
        print(f"Unable to export to CSV. Error details: {type(e).__name__}")


def print_dataframe(records: List[Dict[str, Any]]) -> None:
    """
    Prints a snapshot of the DataFrame created from the records.
    """
    df = pd.DataFrame(records)
    if df.empty:
        print("DataFrame is empty!")
    else:
        print(df.head(50))


def process_response(
    records: List[Dict[str, Any]], start_date: str, end_date: str
) -> None:
    """
    Processes accumulated records based on user selection.
    """
    selection = input(
        f"Row Count: {len(records)}.\nSelect an option to process data:\n"
        "1. Print JSON\n2. Print DataFrame (data might be nested)\n3. Export to CSV (data might be nested)\n4. Export to JSON\n"
        "5. Quit Menu\n"
    )

    if selection.isnumeric():
        if selection == "1":
            for record in records:
                print(json.dumps(record, indent=4))
        elif selection == "2":
            print_dataframe(records)
        elif selection == "3":
            export_to_csv(records)
        elif selection == "4":
            export_to_json(records, start_date, end_date)
        elif selection == "5":
            print("Exiting Menu.")
        else:
            print("Invalid number! Please choose a valid option 1-5.")
    else:
        print("Invalid character! Please choose a valid number 1-5.")


def get_date_ranges() -> Tuple[str, str]:
    """
    Prompts the user for start and end dates and validates their format.
    """
    start_date = input("Enter the start date (yyyy-mm-dd): \n")
    end_date = input("Enter the end date (yyyy-mm-dd): \n")
    validate_dates(start_date, end_date)
    return start_date, end_date


def main(
    access_token: str,
    account_id: str = "act_342562064029688",
) -> None:
    """
    Main function to initialize the Facebook Ads API, retrieve insights data, and process the response.
    """
    try:
        initialize_api(access_token)
        fields = [
            "campaign_name",
            "objective",
            "spend",
            "impressions",
            "clicks",
            "adset_name",
            "ad_name",
            "actions",
        ]
        start_date, end_date = get_date_ranges()
        insights_data = fetch_insights(account_id, fields, start_date, end_date)

        if insights_data:
            process_response(insights_data, start_date, end_date)
        else:
            print("No data retrieved from Facebook API.")
    except Exception as e:
        print(f"An error occurred in main: {e}")


# Example usage
if __name__ == "__main__":
    configs = get_configs()
    if configs:
        access_token = configs.get("access_token")
        if access_token:
            main(access_token)
        else:
            print("Access token not found in configuration.")
