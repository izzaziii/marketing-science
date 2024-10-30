import pandas as pd


def read_boreport(report_type: str) -> pd.DataFrame:
    """
    Reads the BO Report Excel file for the specified report type (FTTH or FTTO).

    Parameters:
        report_type (str): Type of report to read. Accepts "ftth" or "ftto".

    Returns:
        pd.DataFrame: DataFrame containing the data from the specified BO Report Excel file.

    Raises:
        ValueError: If an invalid report type is provided.
    """
    file_paths = {
        "ftth": r"Z:\FUNNEL with PROBABILITY TRACKING_Teefa.xlsx",
        "ftto": r"Z:\FUNNEL with PROBABILITY TRACKING_Teefa (FTTO).xlsx",
    }

    # Check if report_type is valid and read the corresponding file
    try:
        file_path = file_paths[report_type.lower()]
    except KeyError:
        raise ValueError("Invalid report type. Please choose 'ftth' or 'ftto'.")

    try:
        df = pd.read_excel(file_path)
        print(f"Successfully loaded {report_type.upper()} report.")
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


def prompt_inspect_dataframe(df: pd.DataFrame) -> None:
    """
    Asks the user if they want to inspect the DataFrame and runs inspect_dataframe if yes.

    Parameters:
        df (pd.DataFrame): The DataFrame to potentially inspect.
    """
    choice = (
        input("Would you like to inspect the DataFrame? (yes/no): ").strip().lower()
    )
    if choice == "yes":
        inspect_dataframe(df)
    else:
        print("Skipping DataFrame inspection.")


# Example usage
if __name__ == "__main__":
    report_type = input("Enter report type (ftth/ftto): ").strip().lower()
    try:
        df = read_boreport(report_type)
        if not df.empty:
            prompt_inspect_dataframe(df)
    except ValueError as e:
        print(e)
