import os
from datetime import datetime
import json
import google.analytics.data_v1beta.types as t
import pandas as pd
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from pymongo import MongoClient


class GA4_DataHandler:
    def __init__(self, property_id: int = 307329293) -> None:
        """Initializes the GA4_DataHandler Object."""
        self.property_id = property_id
        self.client = self.create_client()

    def create_client(self) -> BetaAnalyticsDataClient:
        """Generates the BetaAnalyticsDataClient from the Data API."""
        return BetaAnalyticsDataClient()

    def get_date_ranges(self) -> tuple[str, str]:
        """Get start and end dates from input."""
        start_date = input("Enter the start date (yyyy-mm-dd): \n")
        end_date = input("Enter the end date (yyyy-mm-dd): \n")
        return start_date, end_date

    def request_traffic(
        self, start_date: str, end_date: str, dimensions: list[str], metrics: list[str]
    ) -> t.RunReportRequest:
        """Generate daily traffic, excluding pagePath that begins with '/payment-method'."""
        dimension_objects: list = [t.Dimension(name=dim) for dim in dimensions]
        metrics_objects: list = [t.Metric(name=metric) for metric in metrics]
        not_expression1 = t.FilterExpression(
            not_expression=t.FilterExpression(
                filter=t.Filter(
                    field_name="pagePath",
                    string_filter=t.Filter.StringFilter(
                        match_type=t.Filter.StringFilter.MatchType.BEGINS_WITH,
                        value="/payment-method",
                    ),
                )
            )
        )
        not_expression2 = t.FilterExpression(
            not_expression=t.FilterExpression(
                filter=t.Filter(
                    field_name="eventName",
                    in_list_filter=t.Filter.InListFilter(
                        values=[
                            "user_engagement",
                            "session_start",
                            "page_load_time",
                            "scroll",
                            "scroll_to_section",
                            "click",
                            "first_visit",
                            "login_begin",
                        ]
                    ),
                )
            )
        )
        combined_filter = t.FilterExpression(
            and_group=t.FilterExpressionList(
                expressions=[not_expression1, not_expression2]
            )
        )

        return t.RunReportRequest(
            property=f"properties/{str(self.property_id)}",
            dimensions=dimension_objects,
            metrics=metrics_objects,
            date_ranges=[t.DateRange(start_date=start_date, end_date=end_date)],
            dimension_filter=combined_filter,
            order_bys=[
                t.OrderBy(
                    dimension=t.OrderBy.DimensionOrderBy(
                        dimension_name="date",
                        order_type=t.OrderBy.DimensionOrderBy.OrderType.NUMERIC,
                    ),
                    desc=False,
                )
            ],
            limit=100000,
        )

    def get_response(
        self, run_report_request: t.RunReportRequest
    ) -> t.RunReportResponse:
        """Retrieves the requested report via Data API."""
        try:
            return self.client.run_report(request=run_report_request)
        except Exception as e:
            print(f"Failed to get a response. Error type: '{type(e).__name__}'")
            return None

    def create_json_records(
        self,
        dim_names: list[str],
        metric_names: list[str],
        response: t.RunReportResponse,
    ) -> list[dict]:
        """Returns list of dicts for JSON or dataframe processing."""
        records = []
        for row in response.rows:
            record = {}
            for i, dim in enumerate(dim_names):
                record[dim] = row.dimension_values[i].value
            for j, metric in enumerate(metric_names):
                record[metric] = row.metric_values[j].value
            records.append(record)
        return records

    def print_dataframe(self, records: list[dict]) -> pd.DataFrame:
        """Prints dataframe in terminal."""
        df = pd.DataFrame(records)
        if df.empty:
            print("DataFrame is empty!")
        else:
            print(df.head(50))

    def export_to_csv(
        self, records: list[dict], folder_path: str = "C:\\Users\\izzaz\\Desktop"
    ) -> None:
        """Exports records to CSV."""
        df = pd.DataFrame(records)
        if not df.empty:
            try:
                time = datetime.today().strftime("%Y%m%d")
                filename = input("\nEnter filename for export: \n")
                full_filename = f"{filename}_{time}.csv"
                fullpath = os.path.join(folder_path, full_filename)
                df.to_csv(fullpath, index=False)
                print(f"Exported data to path: {fullpath}")
            except Exception as e:
                print(f"Unable to export. Error details: {type(e).__name__}")

    def export_to_json(
        self,
        records: list[dict],
        folder_path: str = "C:\\Users\\izzaz\\Documents\\2 Areas\\GitHub\\marketing-science\\data\\raw",
    ) -> None:
        """Exports records to JSON."""
        df = pd.DataFrame(records)
        if not df.empty:
            try:
                time = datetime.today().strftime("%Y%m%d")
                filename = input("\nEnter filename for export: \n")
                full_filename = f"{filename}_{time}.json"
                fullpath = os.path.join(folder_path, full_filename)
                df.to_json(fullpath, index=False, orient="records")
                print(f"Exported data to path: {fullpath}")
            except Exception as e:
                print(f"Unable to export. Error details: {type(e).__name__}")

    def insert_to_db(
        self,
        records: list[dict],
        database: str = "deep-diver-v2",
        collection: str = "ga4-basic",
    ) -> None:
        """Inserts records into MongoDB."""
        client = MongoClient()
        coll = client[database][collection]
        result = coll.insert_many(records)
        print(f"Insertion status: {result.acknowledged}.")
        print(f"Inserted records: {len(result.inserted_ids)} record(s).")

    def process_response(
        self, records: list[dict], dimensions: list[str], metrics: list[str]
    ):
        """Processes accumulated records based on user selection."""
        selection = input(
            f"Row Count: {len(records)}.\nSelect an option to process data:\n"
            "1. Print JSON\n2. Print DataFrame\n3. Export to csv\n4. Export to JSON\n"
            "5. Insert to MongoDB\n6. Quit Menu\n"
        )

        if selection.isnumeric():
            if selection == "1":
                for record in records:
                    print(json.dumps(record, indent=4))
            elif selection == "2":
                self.print_dataframe(records)
            elif selection == "3":
                self.export_to_csv(records)
            elif selection == "4":
                self.export_to_json(records)
            elif selection == "5":
                self.insert_to_db(records)
            elif selection == "6":
                print("Exiting Menu.")
            else:
                print("Invalid number! Please choose a valid option 1-5.")
        else:
            print("Invalid character! Please choose a valid number 1-5.")


def main():
    dimensions = [
        "date",
        "sessionDefaultChannelGroup",
        "sessionCampaignName",
        "sessionManualAdContent",
        "pagePath",
        "eventName",
        "customEvent:event_category",
        "customEvent:event_label",
    ]
    metrics = ["totalUsers"]

    ga = GA4_DataHandler()
    start_date, end_date = ga.get_date_ranges()

    all_records = []
    offset = 0
    limit = 100000

    while True:
        request = ga.request_traffic(start_date, end_date, dimensions, metrics)
        request.limit = limit
        request.offset = offset

        response = ga.get_response(request)

        if response is None:
            print("No response received. Exiting pagination loop.")
            break

        records = ga.create_json_records(dimensions, metrics, response)
        all_records.extend(records)

        if len(records) < limit:
            print("Reached the end of available data.")
            break

        offset += limit
        print(f"Processed {offset} rows so far...")

    ga.process_response(all_records, dimensions, metrics)


if __name__ == "__main__":
    main()
