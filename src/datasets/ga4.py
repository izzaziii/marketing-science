import json
import os
from datetime import datetime
from typing import List

import google.analytics.data_v1beta.types as t
import pandas as pd
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.oauth2 import service_account


def generate_client(path_to_service_account_key_file: str) -> BetaAnalyticsDataClient:
    """
    Generates a BetaAnalyticsDataClient object for interacting with the GA4 API.

    Args:
        path_to_service_account_key_file (str): Path to the service account JSON key file.

    Returns:
        BetaAnalyticsDataClient: Authenticated client object.

    Raises:
        FileNotFoundError: If the service account key file is not found.
        ValueError: If the credentials file is invalid.
    """
    if not os.path.exists(path_to_service_account_key_file):
        raise FileNotFoundError(
            f"Service account key file not found: {path_to_service_account_key_file}"
        )

    try:
        credentials = service_account.Credentials.from_service_account_file(
            path_to_service_account_key_file,
            scopes=["https://www.googleapis.com/auth/analytics.readonly"],
        )
        return BetaAnalyticsDataClient(credentials=credentials)
    except Exception as e:
        raise ValueError(f"Error generating client: {e}")


def generate_report_request(
    property_id: int,
    dimensions: List[str],
    metrics: List[str],
    start_date: str,
    end_date: str,
    filter_expressions=None,
) -> t.RunReportRequest:
    """
    Constructs a RunReportRequest object for querying the GA4 API.

    Args:
        property_id (int): GA4 property ID.
        dimensions (List[str]): List of dimensions to break metrics by. Get this from: https://developers.google.com/analytics/devguides/reporting/data/v1/api-schema
        metrics (List[str]): List of metrics to retrieve. Get this from: https://developers.google.com/analytics/devguides/reporting/data/v1/api-schema#metrics
        start_date (str): Start date in yyyy-mm-dd format.
        end_date (str): End date in yyyy-mm-dd format.

    Returns:
        t.RunReportRequest: Configured RunReportRequest object.
    """
    try:
        return t.RunReportRequest(
            property=f"properties/{property_id}",
            dimensions=[t.Dimension(name=dim) for dim in dimensions],
            metrics=[t.Metric(name=metric) for metric in metrics],
            date_ranges=[t.DateRange(start_date=start_date, end_date=end_date)],
            dimension_filter=filter_expressions,
            limit=250000,
        )
    except Exception as e:
        raise ValueError(f"Error creating report request: {e}")


def fetch_report(
    dimensions: List[str],
    start_date: str,
    end_date: str,
    property_id: int,
    path_to_service_account_key_file: str,
    metrics: List[str] = ["totalUsers"],
    filter_expressions=None,
) -> t.RunReportResponse:
    """
    Fetches the report from GA4 API based on the provided parameters.

    Args:
        dimensions (List[str]): Dimensions to break metrics by.
        start_date (str): Start date in yyyy-mm-dd format.
        end_date (str): End date in yyyy-mm-dd format.
        property_id (int): GA4 property ID.
        path_to_service_account_key_file (str): Path to the service account JSON key file.
        metrics (List[str], optional): List of metrics to retrieve. Defaults to ["totalUsers"].

    Returns:
        t.RunReportResponse: API response containing the report data.

    Raises:
        RuntimeError: If the API call fails or returns no data.
    """
    try:
        client = generate_client(path_to_service_account_key_file)
        request = generate_report_request(
            property_id, dimensions, metrics, start_date, end_date, filter_expressions
        )
        response = client.run_report(request)

        if not response.rows:
            raise RuntimeError("No data returned from the API.")
        return response
    except Exception as e:
        raise RuntimeError(f"Error fetching report: {e}")


def process_response(response: t.RunReportResponse) -> pd.DataFrame:
    """
    Processes the GA4 API response into a pandas DataFrame.

    Args:
        response (t.RunReportResponse): GA4 API response.

    Returns:
        pd.DataFrame: DataFrame containing the report data.
    """
    try:
        # Extract headers
        dimension_headers = [dim.name for dim in response.dimension_headers]
        metric_headers = [metric.name for metric in response.metric_headers]
        column_names = dimension_headers + metric_headers

        # Extract rows
        dim_rows = [
            [value.value for value in row.dimension_values] for row in response.rows
        ]
        metric_rows = [
            [value.value for value in row.metric_values] for row in response.rows
        ]
        rows = [dim + metric for dim, metric in zip(dim_rows, metric_rows)]

        return pd.DataFrame(rows, columns=column_names)
    except Exception as e:
        raise ValueError(f"Error processing response: {e}")


def groupby_dataframe(df: pd.DataFrame, col: List[str]) -> pd.DataFrame:
    """
    Groups the DataFrame for analysis (e.g., typecasting, grouping).

    Args:
        df (pd.DataFrame): DataFrame to process.

    Returns:
        pd.DataFrame: Processed DataFrame.
    """
    if df.empty:
        raise ValueError("The DataFrame is empty. Cannot process further.")

    try:
        # Ensure the necessary column exists
        if "totalUsers" not in df.columns:
            raise KeyError("Expected column 'totalUsers' not found in the DataFrame.")

        # Convert totalUsers to integer and group by dimension
        df["totalUsers"] = df["totalUsers"].astype("int64")
        return df.groupby(col).totalUsers.sum().sort_values(ascending=False)
    except Exception as e:
        raise RuntimeError(f"Error processing DataFrame: {e}")


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


def export_to_json(
    data: list[dict],
    file_description: str = "unlabelled",
    folder_path: str = "C:\\Users\\izzaz\\Documents\\2 Areas\\GitHub\\marketing-science\\data\\raw",
):
    """
    Exports insights data to a JSON file with a timestamped filename.

    Args:
        data (list[dict]): Data to be exported as JSON.
        folder_path (str): Directory where the JSON file will be saved. Defaults to data/raw output path.

    Returns:
        Optional[str]: File path of the saved JSON file if successful, otherwise None.

    Raises:
        IOError: If there is an error writing the file.
    """
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    filename = f"{timestamp}_{file_description}_ga4.json"
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


def dimensions_handler() -> list[str]:
    """
    Handles user input for dimensions and allows them to add multiple dimensions.

    Returns:
        list[str]: A list of dimensions entered by the user.
    """
    dimensions: list[str] = []

    while True:
        user_input = input("Enter a dimension to pull: \n").strip()

        if user_input:
            dimensions.append(user_input)
            print(f"Dimension '{user_input}' added.")
        else:
            print("Invalid input. Please enter a valid dimension.")

        add_more = input("Add another? (y/n): ").strip().lower()
        if add_more not in ["yes", "y"]:
            print("Finished adding dimensions.")
            break

    return dimensions


def date_handler() -> dict[str]:
    """
    Handles user input for start and end dates, ensuring valid input and logical date order.

    Returns:
        dict[str]: Dictionary containing 'start_date' and 'end_date' as strings.

    Raises:
        None: Handles errors gracefully and prompts the user for correct input.
    """
    while True:
        try:
            # Get user input
            start_date_input = input("Add start date in yyyy-mm-dd format:\n").strip()
            end_date_input = input("Add end date in yyyy-mm-dd format:\n").strip()

            # Parse dates
            start_date = datetime.strptime(start_date_input, "%Y-%m-%d")
            end_date = datetime.strptime(end_date_input, "%Y-%m-%d")

            # Validate date order
            if end_date >= start_date:
                return {"start_date": start_date_input, "end_date": end_date_input}
            else:
                print(
                    "End date must be later than or equal to the start date. Please try again."
                )

        except ValueError as e:
            print(
                f"Invalid date format or value. Please use yyyy-mm-dd format. Error: {e}"
            )


def metrics_handler() -> list[str]:
    """
    Handles user input for metrics and allows them to add multiple metrics.

    Returns:
        list[str]: A list of dimensions entered by the user.
    """
    metrics: list[str] = []

    while True:
        user_input = input("Enter a metric to pull: \n").strip()

        if user_input:
            metrics.append(user_input)
            print(f"Dimension '{user_input}' added.")
        else:
            print("Invalid input. Please enter a valid metric.")

        add_more = input("Add another? (y/n): ").strip().lower()
        if add_more not in ["yes", "y"]:
            print("Finished adding metrics.")
            break

    return metrics


def main():
    """
    Main function to pull the GA4 report fetching and processing workflow.
    """
    dimensions: list[str] = dimensions_handler()
    metrics: list[str] = metrics_handler()
    dates: dict = date_handler()
    start_date: str = dates["start_date"]
    end_date: str = dates["end_date"]
    property_id: int = 307329293  # Property ID of time GA4
    path_to_service_account_key_file: str = "C:\\Users\\izzaz\\Documents\\2 Areas\\GitHub\\marketing-science\\deep-diver.json"

    try:
        # Fetch the report
        response: t.RunReportResponse = fetch_report(
            dimensions,
            start_date,
            end_date,
            property_id,
            path_to_service_account_key_file,
            metrics,
        )

        # Process the response into a DataFrame
        df: pd.DataFrame = process_response(response)

        # Export the df
        transformed_df: pd.DataFrame = transform_to_json(df)
        export_to_json(transformed_df)
    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    main()
