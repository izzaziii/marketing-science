import json
import re
from datetime import datetime
from typing import Any, Dict, List, Optional

from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.api import FacebookAdsApi
from facebook_business.exceptions import FacebookRequestError


def get_configs(
    path_to_config_file: str = "C:\\Users\\izzaz\\Documents\\2 Areas\\GitHub\\marketing-science\\src\\auth\\config\\meta_secrets.json",
) -> Optional[Dict[str, Any]]:
    """
    Loads configuration data from a JSON file.

    This function attempts to load a configuration file in JSON format. It first checks
    if the provided file path ends with '.json' and then tries to load and parse the file.
    If the file does not exist or is not a valid JSON format, appropriate error messages
    are printed, and the function returns None.

    Args:
        path_to_config_file (str): The file path to the JSON configuration file.
                                   Defaults to '../auth/config/meta_secrets.json'.

    Returns:
        Optional[Dict[str, Any]]: A dictionary containing configuration data if successful;
                                  otherwise, None.

    Raises:
        FileNotFoundError: If the specified file path does not exist.
        JSONDecodeError: If the file is not valid JSON.
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

    This function sets up the Facebook Ads API for use in making API calls. It requires a valid
    access token to authenticate the session. Once initialized, the API is ready for use with
    other functions within the script that rely on Facebook Ads data.

    Args:
        access_token (str): The access token used to authenticate with the Facebook Ads API.

    Returns:
        None
    """
    FacebookAdsApi.init(access_token=access_token)


def validate_date_format(date_str: str) -> bool:
    """
    Validates if a date string is in the 'yyyy-mm-dd' format.

    This function checks if the provided date string follows the 'yyyy-mm-dd' format.
    It returns `True` if the date matches the specified format; otherwise, it returns `False`.

    Args:
        date_str (str): The date string to validate.

    Returns:
        bool: `True` if the date string matches the 'yyyy-mm-dd' format, else `False`.
    """
    return bool(re.match(r"^\d{4}-\d{2}-\d{2}$", date_str))


def validate_dates(start_date: str, end_date: str) -> bool:
    """
    Validates that start_date is before end_date and both dates are in 'yyyy-mm-dd' format.

    This function first checks if both dates are in the 'yyyy-mm-dd' format using
    `validate_date_format`. It then ensures that `start_date` is chronologically before `end_date`.
    If either check fails, a `ValueError` is raised.

    Args:
        start_date (str): The start date in 'yyyy-mm-dd' format.
        end_date (str): The end date in 'yyyy-mm-dd' format.

    Returns:
        bool: `True` if dates are valid and `start_date` is before `end_date`.

    Raises:
        ValueError: If the dates are not in the correct format or `end_date` is not after `start_date`.
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

    This function retrieves insights for a Facebook Ads account based on the provided date range and fields.
    It validates the date format and range, constructs API request parameters, and retrieves insights data.
    If an API request error occurs, it logs the error and returns None.

    Args:
        account_id (str): The Facebook Ads account ID.
        fields (List[str]): List of fields to retrieve in insights data.
        start_date (str): Start date in 'yyyy-mm-dd' format.
        end_date (str): End date in 'yyyy-mm-dd' format.

    Returns:
        Optional[List[Dict[str, Any]]]: A list of insights data dictionaries if successful; otherwise, None.

    Raises:
        FacebookRequestError: If the Facebook API request encounters an error.
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

    This function creates a filename based on the current timestamp, start date, and end date,
    then writes the insights data to the specified JSON file with formatted indentation for readability.

    Args:
        data (List[Dict[str, Any]]): The insights data to export.
        start_date (str): The start date in 'yyyy-mm-dd' format.
        end_date (str): The end date in 'yyyy-mm-dd' format.

    Returns:
        None
    """
    timestamp = datetime.now().strftime("%Y%m%d-%H%M")
    filename = f"C:\\Users\\izzaz\\Documents\\2 Areas\\GitHub\\marketing-science\\data\\raw\\{timestamp}_facebookads_{start_date}_{end_date}.json"
    with open(filename, "w") as f:
        json.dump(data, f, indent=4)
    print(f"Data exported to {filename}")


def main(
    access_token: str,
    start_date: str,
    end_date: str,
    account_id: str = "act_342562064029688",
) -> None:
    """
    Main function to initialize the Facebook Ads API, download insights data, and export it as a JSON file.

    This function initializes the Facebook Ads API using the provided access token, then fetches
    insights data for the specified account ID and date range. If data retrieval is successful, the
    data is exported to a JSON file for storage or analysis.

    Args:
        access_token (str): The access token to authenticate with the Facebook Ads API.
        start_date (str): The start date for data retrieval in 'yyyy-mm-dd' format.
        end_date (str): The end date for data retrieval in 'yyyy-mm-dd' format.
        account_id (str, optional): The Facebook Ads account ID. Defaults to "act_342562064029688".

    Returns:
        None
    """
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
    insights_data = fetch_insights(account_id, fields, start_date, end_date)
    if insights_data:
        export_to_json(insights_data, start_date, end_date)


# Example usage
if __name__ == "__main__":
    access_token = get_configs()["access_token"]
    start_date = "2024-10-21"
    end_date = "2024-10-27"
    main(access_token, start_date, end_date)
