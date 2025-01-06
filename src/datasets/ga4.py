import json
import os
from datetime import datetime

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


def get_row_count(response: t.RunReportResponse) -> int:
    """Returns the row count of the response object

    Args:
        response (t.RunReportResponse): Response object from the Data API

    Returns:
        int: Number of rows within said response
    """
    try:
        return response.row_count
    except Exception as e:
        print(f"Error: {e}")


def get_offsets_list(row_count: int, limit: int) -> list[int]:
    """Return list of offsets for pagination handling of RunReportResponse

    Args:
        row_count (int): Number of rows. Taken from response.row_count
        limit (int): Limit of rows to pull from RunReportResponse

    Returns:
        list[int]: List of integers for the offset
    """
    if row_count < 0:
        raise ValueError("Row Count should be greater than 0.")

    if limit < 1:
        raise ValueError("Limit should be 1 or more.")

    pages: int = (
        (row_count // limit) + 1 if row_count % limit != 0 else row_count // limit
    )
    return [limit * i for i in range(pages)]


def generate_report_request(
    property_id: int,
    dimensions: list[str],
    metrics: list[str],
    start_date: str,
    end_date: str,
    offset: int = 0,
    limit: int = 100000,
    filter_expressions=None,
) -> t.RunReportRequest:
    """
    Construct a RunReportRequest object for the Google Analytics 4 Data API.

    Description: This function configures and returns a RunReportRequest for GA4. It sets up:
    - The GA4 property (by property ID).
    - Dimensions and metrics to retrieve.
    - Date ranges for the report.
    - Pagination parameters (limit and offset).
    - An optional filter expression for more granular queries.

    Args:
        property_id (int): GA4 property ID.
        dimensions (list[str]): list of dimensions to break metrics by. Get this from: https://developers.google.com/analytics/devguides/reporting/data/v1/api-schema
        metrics (list[str]): List of metrics to retrieve. Get this from: https://developers.google.com/analytics/devguides/reporting/data/v1/api-schema#metrics
        start_date (str): Start date in yyyy-mm-dd format.
        end_date (str): End date in yyyy-mm-dd format.
        offset (int): Used in pagination, when fetching rows greater than the limit that is being called. Typically Data API has a maximum limit of 250,000 that you can specify, but the default is 10,000.
        limit (int): The limit of rows to fetch.
        filter_expressions (None): Used when filtering the query. Future iterations will have an Expressions builder.
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
            limit=limit,
            offset=offset,
        )
    except Exception as e:
        print(f"Error creating report request: {e}")


def fetch_report(
    dimensions: list[str],
    start_date: str,
    end_date: str,
    property_id: int,
    path_to_service_account_key_file: str,
    metrics: list[str] = ["totalUsers"],
    offset: int = 0,
    limit: int = 100000,
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
        offset (int): Used in pagination.
        limit (int): Sets the limit of rows to be pulled in the query.

    Returns:
        t.RunReportResponse: API response containing the report data.

    Raises:
        RuntimeError: If the API call fails or returns no data.
    """
    try:
        client = generate_client(path_to_service_account_key_file)
        request = generate_report_request(
            property_id,
            dimensions,
            metrics,
            start_date,
            end_date,
            offset,
            limit,
            filter_expressions,
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


def groupby_dataframe(df: pd.DataFrame, dimensions: list[str]) -> pd.DataFrame:
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
        return (
            df.groupby(dimensions)
            .totalUsers.sum()
            .sort_values(ascending=False)
            .reset_index()
        )
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
        list[str]: A list of metrics entered by the user.
    """
    metrics: list[str] = []

    while True:
        user_input = input("Enter a metric to pull: \n").strip()

        if user_input:
            metrics.append(user_input)
            print(f"Metric '{user_input}' added.")
        else:
            print("Invalid input. Please enter a valid metric.")

        add_more = input("Add another? (y/n): ").strip().lower()
        if add_more not in ["yes", "y"]:
            print("Finished adding metrics.")
            break

    return metrics


def gather_parameters(
    limit: int = 100000,
) -> tuple[list[str], list[str], str, str, int, str, int]:
    """Collects input from the user to pass to the main function"""
    dimensions: list[str] = dimensions_handler()
    metrics: list[str] = metrics_handler()
    dates: dict = date_handler()
    start_date: str = dates["start_date"]
    end_date: str = dates["end_date"]
    property_id: int = 307329293  # Should come from a config file
    path_to_service_account_key_file: str = "C:\\Users\\izzaz\\Documents\\2 Areas\\GitHub\\marketing-science\\deep-diver.json"  # Should come from config file
    limit: int = limit
    return (
        dimensions,
        metrics,
        start_date,
        end_date,
        property_id,
        path_to_service_account_key_file,
        limit,
    )


def run_initial_report(
    property_id,
    dimensions,
    metrics,
    start_date,
    end_date,
    limit,
    path_to_service_account_key_file,
) -> t.RunReportResponse:
    "Generates an initial RunReportRequest, executes it, and returns the response."
    request: t.RunReportRequest = generate_report_request(
        property_id,
        dimensions,
        metrics,
        start_date,
        end_date,
        limit,
    )
    return generate_client(path_to_service_account_key_file).run_report(request)


def get_pagination_info(response: t.RunReportResponse, limit) -> tuple[int, int]:
    """Gets the pagination info for pagination logic"""
    row_count: int = get_row_count(response)
    pages: int = len(get_offsets_list(row_count, limit))
    return (row_count, pages)


def main():
    """
    Main function to pull the GA4 report fetching and processing workflow.
    """
    # Gather the variables
    limit: int = 100000
    (
        dimensions,
        metrics,
        start_date,
        end_date,
        property_id,
        path_to_service_account_key_file,
        limit,
    ) = gather_parameters()

    # Create the initial request and response
    initial_response: t.RunReportResponse = run_initial_report(
        property_id,
        dimensions,
        metrics,
        start_date,
        end_date,
        limit,
        path_to_service_account_key_file,
    )

    # Get info for the pagination logic
    row_count, pages = get_pagination_info(initial_response, limit)

    # Ask if would like to continue
    user_input: str = (
        input(
            f"Number of rows to process: {row_count}.\nPages to process with limit of {limit}: {pages}\nProceed? (y/n)\n"
        )
        .strip()
        .lower()
    )

    # Handle pagination
    if user_input != "n":
        # Run checks to see if row_count > limit
        pagination_status: bool = True if row_count > limit else False

        # Run conditionals
        if pagination_status:
            # Get offsets list
            offsets = get_offsets_list(row_count, limit)

            # Loop through offsets
            data: list[pd.DataFrame] = []
            for offset in offsets:
                # Create the request and get the response of the API
                response: t.RunReportResponse = fetch_report(
                    dimensions,
                    start_date,
                    end_date,
                    property_id,
                    path_to_service_account_key_file,
                    metrics,
                    offset,
                    limit,
                )
                # Process the rows
                df: pd.DataFrame = groupby_dataframe(
                    process_response(response), dimensions
                )
                data.append(df)
            results: pd.DataFrame = pd.concat(data)

            transformed_df: list[dict] = transform_to_json(results)
            export_to_json(transformed_df, "ga4test")
        else:
            # Proceed with processing the rows
            results: pd.DataFrame = groupby_dataframe(
                process_response(initial_response), dimensions
            )
            transformed_df: list[dict] = transform_to_json(results)
            export_to_json(transformed_df, "ga4test")
    else:
        print("Aborted processing.")


if __name__ == "__main__":
    main()
