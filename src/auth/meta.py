import requests
from typing import Optional, Dict, Any
import json
from datetime import datetime
from facebook_business.api import FacebookAdsApi


# Configuration loading function
def get_configs(
    path_to_config_file: str = r"C:\Users\izzaz\Documents\2 Areas\GitHub\marketing-science\src\auth\config\meta_secrets.json",
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
        print(f"Path '{path_to_config_file}' not found.")
    except json.JSONDecodeError:
        print("Error: The file is not valid JSON.")
    return None


# Function to save updated configuration data
def save_configs(
    config_data: Dict[str, Any],
    path_to_config_file: str = r"C:\Users\izzaz\Documents\2 Areas\GitHub\marketing-science\src\auth\config\meta_secrets.json",
) -> None:
    """
    Saves updated configuration data to a JSON file.
    """
    try:
        with open(path_to_config_file, "w") as file:
            json.dump(config_data, file, indent=4)
        print(f"Configuration updated in '{path_to_config_file}'.")
    except Exception as e:
        print(f"Failed to save configuration: {e}")


# Function to get long-lived access token
def get_long_lived_token(
    app_id: str, app_secret: str, short_lived_token: str
) -> Optional[str]:
    """
    Exchanges a short-lived token for a long-lived token.
    """
    url = "https://graph.facebook.com/v21.0/oauth/access_token"
    params = {
        "grant_type": "fb_exchange_token",
        "client_id": app_id,
        "client_secret": app_secret,
        "fb_exchange_token": short_lived_token,
    }
    response = requests.get(url, params=params)
    data = response.json()
    return data.get("access_token")


# Initialize SDK with API version
def initialize_api(access_token: str) -> None:
    """
    Initializes the Facebook Ads API with version 21.0.
    """
    FacebookAdsApi.init(access_token=access_token, api_version="v21.0")


# Main function to fetch and save the long-lived token
def main(short_lived_token: str):
    config_path = r"C:\Users\izzaz\Documents\2 Areas\GitHub\marketing-science\src\auth\config\meta_secrets.json"
    configs = get_configs(config_path)
    if configs is None:
        print("Failed to load configuration.")
        return

    app_id = configs.get("app_id")
    app_secret = configs.get("app_secret")

    if not all([app_id, app_secret]):
        print("Missing required configuration values.")
        return

    # Initialize the API
    initialize_api(short_lived_token)

    # Fetch the long-lived token
    long_lived_token = get_long_lived_token(app_id, app_secret, short_lived_token)
    if long_lived_token:
        print("Long-lived token fetched successfully.")

        # Save the long-lived token and last updated timestamp to the configuration file
        configs["access_token"] = long_lived_token
        configs["last_updated"] = datetime.now().strftime("%Y-%m-%d %H:%M")

        save_configs(configs, config_path)
    else:
        print("Failed to retrieve long-lived token.")


if __name__ == "__main__":
    # Obtain short-lived token from the Graph API Explorer https://developers.facebook.com/tools/explorer/
    short_lived_token = input("Please enter your short-lived access token: ")
    main(short_lived_token)
