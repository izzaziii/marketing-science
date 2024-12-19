import google.analytics.data_v1beta.types as t
import pandas as pd
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.oauth2 import service_account


def generate_client(path_to_service_account_key_file: str) -> BetaAnalyticsDataClient:
    credentials = service_account.Credentials.from_service_account_file(
        path_to_service_account_key_file,
        scopes=["https://www.googleapis.com/auth/analytics.readonly"],
    )
    return BetaAnalyticsDataClient(credentials=credentials)


def generate_report_request(
    property_id: int,
    dimensions: list[str],
    metrics: list[str],
    start_date: str,
    end_date: str,
) -> t.RunReportRequest:
    return t.RunReportRequest(
        property=f"properties/{property_id}",
        dimensions=[t.Dimension(name=dim) for dim in dimensions],
        metrics=[t.Metric(name=metric) for metric in metrics],
        date_ranges=[t.DateRange(start_date=start_date, end_date=end_date)],
    )


def generate_response(
    dimensions: list[str],
    start_date: str,
    end_date: str,
    property_id: int = 307329293,
    path_to_service_account_key_file: str = "C:\\Users\\izzaz\\Documents\\2 Areas\\GitHub\\marketing-science\\deep-diver.json",
    metrics: list[str] = ["totalUsers"],
) -> t.RunReportResponse:
    client = generate_client(path_to_service_account_key_file)
    request = generate_report_request(
        property_id, dimensions, metrics, start_date, end_date
    )
    return client.run_report(request)


dimensions = ["sessionDefaultChannelGrouping"]
start_date = "2024-11-01"
end_date = "2024-11-07"
response = generate_response(dimensions, start_date, end_date)

# Get headers
dimension_headers = [dim.name for dim in response.dimension_headers]
metric_headers = [dim.name for dim in response.metric_headers]
column_names = dimension_headers + metric_headers

# Get rows
dim_rows = [[value.value for value in row.dimension_values] for row in response.rows]
metric_rows = [[value.value for value in row.metric_values] for row in response.rows]
rows = [dim + metric for dim, metric in zip(dim_rows, metric_rows)]

# Transform to dataframe
df = pd.DataFrame(rows, columns=column_names)

# Process dataframe
processed_df = df.assign(
    totalUsers=df.totalUsers.astype("int64"),
)

processed_df.groupby("sessionDefaultChannelGrouping").totalUsers.sum().sort_values(
    ascending=False
)
