from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.exceptions import FacebookRequestError
import json
from datetime import datetime
import re
from typing import List, Optional


def initialize_api(access_token: str) -> None:
    """Initialize the Facebook Ads API with the provided access token."""
    FacebookAdsApi.init(access_token=access_token)


def validate_date_format(date_str: str) -> bool:
    """Check if the date is in 'yyyy-mm-dd' format."""
    return bool(re.match(r"^\d{4}-\d{2}-\d{2}$", date_str))


def validate_dates(start_date: str, end_date: str) -> bool:
    """
    Ensure start_date is before end_date and both are in 'yyyy-mm-dd' format.
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
) -> Optional[List[dict]]:
    """Fetch insights from the Facebook Ads account within the specified date range."""
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


def export_to_json(data: List[dict], start_date: str, end_date: str) -> None:
    """Exports insights data to a JSON file with a timestamped filename."""
    timestamp = datetime.now().strftime("%Y%m%d-%H%M")
    filename = f"../../data/raw/{timestamp}_facebookads_{start_date}_{end_date}.json"
    with open(filename, "w") as f:
        json.dump(data, f, indent=4)
    print(f"Data exported to {filename}")


def main(
    access_token: str,
    start_date: str,
    end_date: str,
    account_id: str = "act_342562064029688",
) -> None:
    """Main function to download and export Facebook Ads data as JSON."""
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
    access_token = "EAAJ5etmCAx8BOyKsj26L4yC2lkvZBn82Uqc4vz5qH0LuU4NVcZB5QSl460V8ZAEb56jCpOoYHRjjNLN4tLYnIryLxdsZAfTtfc3cxATZCY6GGtbPbTeVHN2bL8O1ZBVZBoc6Gy3CCjITYXZByDbAztOF4L1suapiRmghhAZBnOzxHFQR5Ph57rAKkE16xJ92T2AMtcnja0AlAXMj9UvwbQQZDZD"
    start_date = "2024-10-01"
    end_date = "2024-10-05"
    main(access_token, start_date, end_date)
