import pandas as pd
from typing import Optional, List, Tuple


class BOReport:
    """
    A class for processing and analyzing BO Report data for weekly sales.

    Methods:
        - prepare_sales_data_direct_channels: Prepares data filtered by direct sales channels.
        - prepare_sales_data_all_channels: Prepares data for all sales channels.
        - resample_weekly_sales: Resamples weekly sales orders by specific channels for a given year.
        - resample_weekly_sales_by_columns: Resamples weekly sales data based on specified columns.
    """

    def __init__(self, data: pd.DataFrame) -> None:
        """
        Initializes the BOReport class with a DataFrame.

        Args:
            data (pd.DataFrame): The raw DataFrame containing BO Report data.

        Raises:
            ValueError: If the provided data is empty or not in DataFrame format.
        """
        if data.empty or not isinstance(data, pd.DataFrame):
            raise ValueError("Data must be a non-empty pandas DataFrame.")
        self.data = data

    def prepare_sales_data_direct_channels(self) -> None:
        """
        Filters and prepares the sales data (direct channels) for weekly analysis.
        Updates `self.data` with the filtered DataFrame.

        The filtering process:
        - Renames ' Channel' column to 'Channel'.
        - Filters for 'Active' funnel status, 'ONLINE' and 'INSIDE SALES' channels, 'New Sales' funnel type,
          and 'Time B.Band-FTTH' product.
        - Converts 'Age' column to integer, filling non-numeric values with 0.
        - Converts 'Blk Cluster', 'Blk State', and 'Bld Name' columns to categorical type for memory optimization.

        Raises:
            KeyError: If any specified column is missing in the data.
        """
        try:
            df = self.data.rename(columns={" Channel": "Channel"})

            self.data = df.loc[
                (df["Funn Status"] == "Active")
                & (df["Channel"].isin(["ONLINE", "INSIDE SALES"]))
                & (df["Funnel Type"] == "New Sales")
                & (df["Funnel Productname"] == "Time B.Band-FTTH")
            ].copy()

            self.data["Age"] = (
                pd.to_numeric(self.data["Age"], errors="coerce").fillna(0).astype(int)
            )
            self.data["Blk Cluster"] = self.data["Blk Cluster"].astype("category")
            self.data["Blk State"] = self.data["Blk State"].astype("category")
            self.data["Bld Name"] = self.data["Bld Name"].astype("category")

        except KeyError as e:
            print(f"Error: Missing column {e} in data.")
            raise

    def prepare_sales_data_all_channels(self) -> None:
        """
        Filters and prepares the sales data (all channels) for weekly analysis.
        Updates `self.data` with the filtered DataFrame.

        The filtering process:
        - Renames ' Channel' column to 'Channel'.
        - Filters for 'Active' funnel status, 'New Sales' funnel type, and 'Time B.Band-FTTH' product.
        - Converts 'Age' column to integer, filling non-numeric values with 0.
        - Converts 'Blk Cluster', 'Blk State', and 'Bld Name' columns to categorical type for memory optimization.

        Raises:
            KeyError: If any specified column is missing in the data.
        """
        try:
            df = self.data.rename(columns={" Channel": "Channel"})

            self.data = df.loc[
                (df["Funn Status"] == "Active")
                & (df["Funnel Type"] == "New Sales")
                & (df["Funnel Productname"] == "Time B.Band-FTTH")
            ].copy()

            self.data["Age"] = (
                pd.to_numeric(self.data["Age"], errors="coerce").fillna(0).astype(int)
            )
            self.data["Blk Cluster"] = self.data["Blk Cluster"].astype("category")
            self.data["Blk State"] = self.data["Blk State"].astype("category")
            self.data["Bld Name"] = self.data["Bld Name"].astype("category")

        except KeyError as e:
            print(f"Error: Missing column {e} in data.")
            raise

    def resample_weekly_sales(
        self,
        year: str,
        channel_column: str = "Channel",
        date_column: str = "Probability 90% Date",
        target_column: str = "Funnel SO No",
        channels: Optional[List[str]] = None,
    ) -> pd.DataFrame:
        """
        Resamples weekly sales orders by specific channels for a given year, returning counts for each week.

        Args:
            year (str): The year to filter sales data (e.g., "2023" or "2024").
            channel_column (str): Column name for sales channels. Defaults to "Channel".
            date_column (str): Column containing dates for resampling. Defaults to "Probability 90% Date".
            target_column (str): Column with target metric for counting. Defaults to "Funnel SO No".
            channels (List[str], optional): List of channels to include. Defaults to ["ONLINE", "INSIDE SALES"].

        Returns:
            pd.DataFrame: DataFrame with weekly counts of sales orders per channel.

        Raises:
            KeyError: If any specified column is missing in the data.
        """
        if channels is None:
            channels = ["ONLINE", "INSIDE SALES"]

        try:
            result = (
                self.data.dropna(subset=[date_column])
                .set_index(date_column)
                .loc[year]
                .groupby(channel_column)
                .resample("W-SUN")[target_column]
                .count()
                .unstack()
                .T.fillna(0)
            )[channels]

            return result

        except KeyError as e:
            print(
                f"Error: {e}. Please check if all specified columns exist in the DataFrame."
            )
            raise

    def resample_weekly_sales_by_columns(
        self,
        list_of_columns: List[str] = [
            "Channel",
            "Funnel Bandwidth",
            "Blk State",
            "Funn Monthcontractperiod",
        ],
    ) -> Tuple[pd.DataFrame, ...]:
        """
        Resamples sales data on a weekly basis based on specified columns and returns a tuple of DataFrames.

        Args:
            list_of_columns (List[str]): List of column names to group and resample by.

        Returns:
            Tuple[pd.DataFrame, ...]: A tuple of DataFrames resampled by each specified column.

        Raises:
            KeyError: If any specified column is missing in the data.
        """
        result_dfs = []

        for col in list_of_columns:
            try:
                resampled_df = (
                    self.data.dropna(subset=["Probability 90% Date"])
                    .set_index("Probability 90% Date")
                    .groupby(col)
                    .resample("W-SUN")["Funnel SO No"]
                    .count()
                    .unstack()
                    .T.fillna(0)
                )
                result_dfs.append(resampled_df)
            except KeyError as e:
                print(
                    f"Error: {e}. Please ensure the column '{col}' exists in the DataFrame."
                )
                raise

        return tuple(result_dfs)
