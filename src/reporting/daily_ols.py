import pandas as pd
from google.analytics.data_v1beta.types import (
    RunReportResponse,
    Filter,
    FilterExpression,
)

from datasets.ga4 import (
    date_handler,
    export_to_json,
    fetch_report,
    process_response,
    transform_to_json,
)


def main():
    dimensions: list[str] = [
        "date",
        "sessionDefaultChannelGrouping",
        "sessionCampaignName",
        "eventName",
        "customEvent:event_category",
        "customEvent:event_action",
        "customEvent:event_label",
    ]
    metrics: list[str] = ["totalUsers", "eventCount"]
    dates: dict = date_handler()
    start_date: str = dates["start_date"]
    end_date: str = dates["end_date"]
    property_id: int = 307329293  # Property ID of time GA4
    path_to_service_account_key_file: str = "C:\\Users\\izzaz\\Documents\\2 Areas\\GitHub\\marketing-science\\deep-diver.json"
    filter_expressions = FilterExpression(
        filter=Filter(
            field_name="customEvent:event_category",
            string_filter=Filter.StringFilter(value="For Home"),
        )
    )

    try:
        response: RunReportResponse = fetch_report(
            dimensions,
            start_date,
            end_date,
            property_id,
            path_to_service_account_key_file,
            metrics,
            filter_expressions,
        )
        df: pd.DataFrame = process_response(response)
        transformed_df: pd.DataFrame = transform_to_json(df)
        export_to_json(transformed_df, "ols")
    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    main()
