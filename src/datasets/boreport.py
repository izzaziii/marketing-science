import pandas as pd
from typing import Optional


def read_excel_file(file_path: str) -> pd.DataFrame:
    """
    Reads an Excel file and returns a DataFrame.

    Parameters:
        file_path (str): The path to the Excel file.

    Returns:
        pd.DataFrame: DataFrame containing the data from the Excel file.
    """
    try:
        df = pd.read_excel(file_path)
        return df
    except Exception as e:
        print(f"Error reading the Excel file: {e}")
        return pd.DataFrame()


def inspect_dataframe(df: pd.DataFrame) -> None:
    """
    Inspects the DataFrame by printing its information and the first few rows.

    Parameters:
        df (pd.DataFrame): The DataFrame to inspect.
    """
    print("DataFrame Info:")
    df.info()
    print("\nDataFrame Head:")
    print(df.head())


def filter_by_columns(df: pd.DataFrame, columns: Optional[list] = None) -> pd.DataFrame:
    """
    Filters the DataFrame by the specified columns. If no columns are specified, returns the full DataFrame.

    Parameters:
        df (pd.DataFrame): The original DataFrame.
        columns (list, optional): List of column names to filter. Defaults to None.

    Returns:
        pd.DataFrame: Filtered DataFrame.
    """
    if columns:
        return df[columns]
    return df


def prepare_sales_data_direct_channels(df: pd.DataFrame) -> pd.DataFrame:
    """
    Prepares the sales data (direct channels) for weekly analysis and management updates by performing data cleaning, transformations,
    and filtering for new acquisitions (sales).

    Steps include:
    1. Renaming the ' Channel' column to 'Channel'.
    2. Filtering the data for:
       - Active funnel status (removing lost customers).
       - Sales channels: 'ONLINE' and 'INSIDE SALES'.
       - Funnel type: 'New Sales'.
       - Funnel product name: 'Time B.Band-FTTH'.
    3. Converting the 'Age' column to integer, replacing non-numeric values with 0.
    4. Casting 'Blk Cluster', 'Blk State', and 'Bld Name' columns to categorical type for optimization.

    Parameters:
        df (pd.DataFrame): The input DataFrame containing sales data.

    Returns:
        pd.DataFrame: The cleaned, transformed, and filtered DataFrame ready for analysis.
    """

    df = df.rename(columns={" Channel": "Channel"})

    df_filtered = df.loc[
        (df["Funn Status"] == "Active")
        & (df["Channel"].isin(["ONLINE", "INSIDE SALES"]))
        & (df["Funnel Type"] == "New Sales")
        & (df["Funnel Productname"] == "Time B.Band-FTTH").copy()
    ]

    df_filtered.loc[:, "Age"] = (
        pd.to_numeric(df_filtered["Age"], errors="coerce").fillna(0).astype(int)
    )

    df_filtered.loc[:, "Blk Cluster"] = df_filtered["Blk Cluster"].astype("category")
    df_filtered.loc[:, "Blk State"] = df_filtered["Blk State"].astype("category")
    df_filtered.loc[:, "Bld Name"] = df_filtered["Bld Name"].astype("category")

    return df_filtered


def prepare_sales_data_all_channels(df: pd.DataFrame) -> pd.DataFrame:
    """
    Prepares the sales data (all channels) for weekly analysis and management updates by performing data cleaning, transformations,
    and filtering for new acquisitions (sales).

    Steps include:
    1. Renaming the ' Channel' column to 'Channel'.
    2. Filtering the data for:
       - Active funnel status (removing lost customers).
       - Sales channels: 'ONLINE' and 'INSIDE SALES'.
       - Funnel type: 'New Sales'.
       - Funnel product name: 'Time B.Band-FTTH'.
    3. Converting the 'Age' column to integer, replacing non-numeric values with 0.
    4. Casting 'Blk Cluster', 'Blk State', and 'Bld Name' columns to categorical type for optimization.

    Parameters:
        df (pd.DataFrame): The input DataFrame containing sales data.

    Returns:
        pd.DataFrame: The cleaned, transformed, and filtered DataFrame ready for analysis.
    """

    df = df.rename(columns={" Channel": "Channel"})

    df_filtered = df.loc[
        (df["Funn Status"] == "Active")
        & (df["Funnel Type"] == "New Sales")
        & (df["Funnel Productname"] == "Time B.Band-FTTH")
    ].copy()  # Ensure this is a copy, not a view

    # Convert 'Age' column to integer, replacing non-numeric values with 0
    df_filtered.loc[:, "Age"] = (
        pd.to_numeric(df_filtered["Age"], errors="coerce").fillna(0).astype(int)
    )

    # Cast specific columns to categorical
    df_filtered.loc[:, "Blk Cluster"] = df_filtered["Blk Cluster"].astype("category")
    df_filtered.loc[:, "Blk State"] = df_filtered["Blk State"].astype("category")
    df_filtered.loc[:, "Bld Name"] = df_filtered["Bld Name"].astype("category")

    return df_filtered


def resample_weekly_boreport(
    df: pd.DataFrame,
    list_of_columns: list[str] = [
        "Channel",
        "Funnel Bandwidth",
        "Blk State",
        "Funn Monthcontractperiod",
    ],
) -> tuple:
    """
    Resamples the sales data on a weekly basis based on the Probability 90% Date for specified columns.

    The function processes the data with the following steps:
    1. Drop rows with missing values in 'Probability 90% Date'.
    2. Set the 'Probability 90% Date' column as the index to enable resampling based on date.
    3. Group the data by the specified column (defaults to 'Channel', 'Funnel Bandwidth', 'Blk State', 'Funn Monthcontractperiod').
    4. Resample the data on a weekly basis, with weeks ending on Sunday ('W-SUN').
    5. Count the occurrences of 'Funnel SO No' per group and week.
    6. Unstack the result to reshape the groups as columns and weeks as rows.
    7. Transpose the DataFrame for easier readability (weeks as columns, groups as rows).
    8. Fill any missing values with 0 (indicating no sales for that group and week).

    ### Example usage
    >>> file_path = r"Z:\FUNNEL with PROBABILITY TRACKING_Teefa.xlsx"
    >>> df = read_excel_file(filepath)
    >>> prepped_df = prepare_sales_data_direct_channels(df)
    >>> df_channel, df_bandwidth, df_state, df_contract = resample_weekly_boreport(prepped_df)

    Parameters:
        df (pd.DataFrame): The input DataFrame containing sales data.

    Returns:
        tuple: A tuple containing four DataFrames resampled by 'Channel', 'Funnel Bandwidth',
               'Blk State', and 'Funn Monthcontractperiod'.
    """
    result_dfs = []

    for col in list_of_columns:
        resampled_df = (
            df.dropna(subset=["Probability 90% Date"])
            .set_index("Probability 90% Date")
            .groupby(col)
            .resample("W-SUN")["Funnel SO No"]
            .count()
            .unstack()
            .T.fillna(0)
        )
        result_dfs.append(resampled_df)

    return tuple(result_dfs)


def main(file_path: str) -> None:
    """
    Main function to prepare the sales data for analysis and generate resampled weekly reports.

    Steps:
    1. Load the Excel file containing sales data.
    2. Prepare the sales data by filtering for direct sales channels (ONLINE and INSIDE SALES)
       and relevant columns.
    3. Resample the prepared data on a weekly basis, grouped by 'Channel', 'Funnel Bandwidth',
       'Blk State', and 'Funn Monthcontractperiod'.
    4. Return and store the resampled data for further analysis or visualization.

    Parameters:
        file_path (str): The file path of the Excel file containing sales order data.

    Returns:
        None
    """
    # Step 1: Load the Excel file
    df = pd.read_excel(file_path)

    # Step 2: Prepare the data by filtering and cleaning for direct sales channels
    prepped_df = prepare_sales_data_direct_channels(df)

    # Step 3: Resample the data on a weekly basis by various columns
    df_channel, df_bandwidth, df_state, df_contract = resample_weekly_boreport(
        prepped_df
    )

    # Step 4: Store the resampled data (for now, we just print to indicate success)
    print("Resampled Data by Channel:\n", df_channel.head())
    print("Resampled Data by Funnel Bandwidth:\n", df_bandwidth.head())
    print("Resampled Data by Blk State:\n", df_state.head())
    print("Resampled Data by Contract Period:\n", df_contract.head())


# Example usage
if __name__ == "__main__":
    file_path = r"Z:\FUNNEL with PROBABILITY TRACKING_Teefa.xlsx"
    main(file_path)
